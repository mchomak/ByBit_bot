"""
Strategy Engine for the Trading Pipeline.

Consumes CandleUpdate events, maintains per-symbol rolling statistics,
evaluates entry/exit conditions, and emits signals.

Strategy Rules:
- ENTRY: volume > max_5d_volume OR close >= open * 3 (either condition triggers)
- EXIT: price crosses MA14 from above (close <= ma14 OR low <= ma14)
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.pipeline_events import (
    CandleUpdate,
    DBWriteTask,
    PipelineMetrics,
    SignalType,
    SymbolState,
    TradingSignal,
)


class StrategyEngine:
    """
    Stateful strategy engine that processes candle updates.

    Maintains per-symbol state for efficient rolling calculations
    without querying the database on each tick.
    """

    def __init__(
        self,
        marketdata_queue: asyncio.Queue,
        db_write_queue: asyncio.Queue,
        signal_queue: asyncio.Queue,
        *,
        volume_window_minutes: int = 7200,  # 5 days
        ma_period: int = 14,
        price_acceleration_factor: float = 3.0,  # close >= open * factor
        stop_loss_pct: float = 7.0,  # Stop-loss at 7%
        skip_red_candle_entry: bool = False,  # Skip entry on red candles
        volume_only_green_candles: bool = True,  # Only use green candle volumes
        metrics: Optional[PipelineMetrics] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize the strategy engine.

        Args:
            marketdata_queue: Input queue for CandleUpdate events
            db_write_queue: Output queue for DBWriteTask events
            signal_queue: Output queue for TradingSignal events
            volume_window_minutes: Rolling window for max volume (5 days = 7200 min)
            ma_period: Moving average period for exit signal
            price_acceleration_factor: Multiplier for price acceleration check
            stop_loss_pct: Stop-loss percentage (e.g., 7.0 for 7%)
            skip_red_candle_entry: If True, skip entry on red candles
            volume_only_green_candles: If True, only use volume from green candles
            metrics: Optional shared metrics object
            logger: Optional logger instance
        """
        self._input_queue = marketdata_queue
        self._db_queue = db_write_queue
        self._signal_queue = signal_queue

        self._volume_window = int(volume_window_minutes)
        self._ma_period = int(ma_period)
        self._price_factor = float(price_acceleration_factor)
        self._stop_loss_pct = float(stop_loss_pct)
        self._skip_red_candle = bool(skip_red_candle_entry)
        self._volume_only_green = bool(volume_only_green_candles)

        self._metrics = metrics or PipelineMetrics()
        self._log = logger or logging.getLogger(self.__class__.__name__)

        # Per-symbol state
        self._states: Dict[str, SymbolState] = defaultdict(lambda: SymbolState(symbol=""))

        # Track open positions (will be synced from execution engine)
        self._open_positions: Set[str] = set()

        # Task handle
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def set_open_positions(self, symbols: Set[str]) -> None:
        """
        Update the set of symbols with open positions.

        Called by execution engine to sync state.
        """
        self._open_positions = set(symbols)
        # Update symbol states
        for symbol in symbols:
            if symbol in self._states:
                self._states[symbol].has_open_position = True

    def mark_position_opened(self, symbol: str, entry_price: Optional[float] = None) -> None:
        """Mark that a position was opened for a symbol."""
        self._open_positions.add(symbol)
        if symbol in self._states:
            self._states[symbol].has_open_position = True
            self._states[symbol].entry_price = entry_price

    def mark_position_closed(self, symbol: str) -> None:
        """Mark that a position was closed for a symbol."""
        self._open_positions.discard(symbol)
        if symbol in self._states:
            self._states[symbol].has_open_position = False
            self._states[symbol].entry_price = None

    async def start(self) -> None:
        """Start the strategy engine consumer loop."""
        if self._task is not None:
            self._log.warning("Engine already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="strategy-engine")
        self._log.info("Strategy engine started")

    async def stop(self) -> None:
        """Stop the strategy engine gracefully."""
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._log.info("Strategy engine stopped")

    async def _run(self) -> None:
        """Main consumer loop."""
        while not self._stop_event.is_set():
            try:
                # Wait for candle update with timeout
                try:
                    update: CandleUpdate = await asyncio.wait_for(
                        self._input_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Process the update
                await self._process_update(update)
                self._input_queue.task_done()
                self._metrics.candle_updates_processed += 1

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error processing candle update: %s", e)
                self._metrics.errors += 1

    async def _process_update(self, update: CandleUpdate) -> None:
        """
        Process a single candle update.

        1. Emit DB write task
        2. Update symbol state
        3. Evaluate entry/exit conditions
        4. Emit signals if conditions met
        """
        symbol = update.symbol

        # 1. Emit DB write task (always, for both open and confirmed candles)
        db_task = DBWriteTask(
            symbol=symbol,
            timestamp=update.timestamp,
            open=update.open,
            high=update.high,
            low=update.low,
            close=update.close,
            volume=update.volume,
            turnover=update.turnover,
            is_confirmed=update.confirm,
        )

        try:
            self._db_queue.put_nowait(db_task)
            self._metrics.db_writes_queued += 1
        except asyncio.QueueFull:
            self._log.warning("DB write queue full, dropping write for %s", symbol)

        # 2. Get/create symbol state
        state = self._states[symbol]
        if not state.symbol:
            state.symbol = symbol

        # 3. Update rolling statistics only on confirmed candles
        # This ensures we don't double-count volume or use incomplete data
        if update.confirm:
            # IMPORTANT: Capture max volume BEFORE adding current candle
            # Otherwise we can never detect a new volume spike
            previous_max_volume = state.max_volume_5d

            # Check if this is a green candle (close >= open)
            is_green_candle = update.close >= update.open

            # Update volume history (optionally filtering to green candles only)
            state.update_volume_history(
                update.timestamp,
                update.volume,
                self._volume_window,
                is_green_candle=is_green_candle,
                only_green_candles=self._volume_only_green,
            )
            ma14 = state.update_ma14(update.close)

            # Sync position state
            state.has_open_position = symbol in self._open_positions

            # 4. Evaluate conditions (using previous_max_volume for entry check)
            signals = self._evaluate_conditions(
                update, state, ma14, previous_max_volume, is_green_candle
            )

            # 5. Emit signals
            for signal in signals:
                try:
                    self._signal_queue.put_nowait(signal)
                    self._metrics.signals_generated += 1
                    self._log.info(
                        "Signal: %s %s @ %.6f (vol_ratio=%.2f, ma14=%.6f)",
                        signal.signal_type.value,
                        signal.symbol,
                        signal.price,
                        signal.volume_ratio or 0,
                        signal.ma14_value or 0,
                    )
                except asyncio.QueueFull:
                    self._log.warning("Signal queue full, dropping signal for %s", symbol)

    def _evaluate_conditions(
        self,
        update: CandleUpdate,
        state: SymbolState,
        ma14: Optional[float],
        previous_max_volume: float = 0.0,
        is_green_candle: bool = True,
    ) -> List[TradingSignal]:
        """
        Evaluate entry and exit conditions.

        Args:
            update: Current candle update
            state: Symbol state
            ma14: Current MA14 value
            previous_max_volume: Max volume from history BEFORE current candle was added
                                 (used to detect volume spikes)
            is_green_candle: Whether the current candle is green (close >= open)

        Returns list of signals (0, 1, or 2 if both entry and exit somehow trigger).
        """
        signals: List[TradingSignal] = []

        # --- EXIT CHECK (before entry, so we don't enter and exit same candle) ---
        if state.has_open_position:
            exit_triggered = False
            exit_reason = None

            # Check 1: Stop-loss - price dropped by stop_loss_pct from entry
            if state.entry_price is not None and self._stop_loss_pct > 0:
                stop_loss_price = state.entry_price * (1 - self._stop_loss_pct / 100)
                if update.close <= stop_loss_price or update.low <= stop_loss_price:
                    exit_triggered = True
                    exit_reason = "stop_loss"
                    self._log.warning(
                        "STOP-LOSS triggered for %s: price %.6f <= stop %.6f (entry=%.6f, loss=%.1f%%)",
                        update.symbol, update.close, stop_loss_price,
                        state.entry_price, self._stop_loss_pct
                    )

            # Check 2: MA14 crossover (only if stop-loss didn't trigger)
            if not exit_triggered and ma14 is not None:
                # Exit if price crosses MA14 from above
                # We check: close <= ma14 OR low <= ma14
                if update.close <= ma14 or update.low <= ma14:
                    exit_triggered = True
                    exit_reason = "ma14_crossover"

            if exit_triggered:
                exit_signal = TradingSignal(
                    signal_type=SignalType.EXIT,
                    symbol=update.symbol,
                    timestamp=update.timestamp,
                    price=update.close,
                    volume=update.volume,
                    ma14_value=ma14,
                )
                signals.append(exit_signal)
                # Note: execution engine will confirm the close

        # --- ENTRY CHECK ---
        if not state.has_open_position:
            # Check entry conditions (OR logic - either triggers entry):
            # 1. Volume spike: current volume > max 5-day volume (BEFORE this candle)
            # 2. Price acceleration: close >= open * 3
            #
            # IMPORTANT: We compare against previous_max_volume (history BEFORE current candle)
            # because if we include current candle in max, a spike can never exceed itself!

            # Skip entry on red candles if configured
            if self._skip_red_candle and not is_green_candle:
                logging.debug(
                    "Skipping entry for %s: red candle (close=%.6f < open=%.6f)",
                    update.symbol, update.close, update.open
                )
                return signals

            volume_condition = False
            volume_ratio = 0.0
            if previous_max_volume > 0:
                volume_ratio = update.volume / previous_max_volume
                volume_condition = update.volume > previous_max_volume

            price_acceleration = update.close >= update.open * self._price_factor
            price_change_pct = ((update.close - update.open) / update.open * 100) if update.open > 0 else 0

            logging.debug(
                "Evaluating ENTRY for %s: vol=%.2f (prev_max5d=%.2f, ratio=%.8f), price_accel=%s, green=%s",
                update.symbol,
                update.volume,
                previous_max_volume,
                volume_ratio,
                price_acceleration,
                is_green_candle,
            )

            # Check deduplication: don't enter same minute twice
            already_entered = state.last_entry_minute == update.timestamp

            # Entry condition: volume spike OR price acceleration (not both required)
            entry_condition = (volume_condition or price_acceleration) and not already_entered

            if entry_condition:
                entry_signal = TradingSignal(
                    signal_type=SignalType.ENTRY,
                    symbol=update.symbol,
                    timestamp=update.timestamp,
                    price=update.close,
                    volume=update.volume,
                    volume_ratio=volume_ratio,
                    price_change_pct=price_change_pct,
                    ma14_value=ma14,
                )
                signals.append(entry_signal)
                # Mark this minute as entered (execution engine will confirm)
                state.last_entry_minute = update.timestamp

        return signals

    def load_historical_data(
        self,
        symbol: str,
        candles: List[Dict[str, Any]],
    ) -> None:
        """
        Bootstrap symbol state from historical candle data.

        Called at startup to initialize rolling statistics.

        Args:
            symbol: The symbol to load
            candles: List of candle dicts with keys: timestamp, close, volume
        """
        state = self._states[symbol]
        if not state.symbol:
            state.symbol = symbol

        # Sort by timestamp ascending
        sorted_candles = sorted(candles, key=lambda c: c["timestamp"])

        for candle in sorted_candles:
            ts = candle["timestamp"]
            close = float(candle["close"])
            volume = float(candle["volume"])

            state.update_volume_history(ts, volume, self._volume_window)
            state.update_ma14(close)

        self._log.debug(
            "Loaded %d candles for %s: max_vol=%.2f, ma14_samples=%d",
            len(sorted_candles),
            symbol,
            state.max_volume_5d,
            len(state.ma14_closes),
        )