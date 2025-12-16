"""StrategyService: Signal detection and trading logic.

Entry Conditions (BUY signal):
1. Abnormal volume: current volume > volume_spike_multiplier * max(5-day volume)
2. Price acceleration: (close - open) / open * 100 >= min_price_change_pct
3. Risk check: current positions < max_positions
4. No duplicate: not already in position for this symbol

Exit Conditions (SELL signal):
1. MA14 crossover: price crosses below MA14 (14-period moving average)
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import numpy as np
from loguru import logger

from ..config.settings import Settings
from ..db.repository import Repository
from ..db.models import Candle1m, Position, Signal, PositionStatus, SignalType
from ..queues.event_bus import (
    MarketDataQueue,
    TradeSignalQueue,
    NotificationQueue,
    CandleEvent,
    EventType,
)


@dataclass
class SymbolState:
    """State tracking for a single symbol."""

    symbol: str
    candles: List[Dict[str, Any]]  # Recent candles for MA calculation
    max_volume_5d: float = 0.0  # Maximum volume in 5-day history
    ma14: float = 0.0  # Current MA14 value
    last_close: float = 0.0
    has_position: bool = False
    position_id: Optional[int] = None


class StrategyService:
    """
    Strategy service for detecting entry and exit signals.

    Consumes candle events from MarketDataQueue and produces
    trade signals to TradeSignalQueue.
    """

    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        market_data_queue: MarketDataQueue,
        trade_signal_queue: TradeSignalQueue,
        notification_queue: NotificationQueue,
    ):
        """
        Initialize StrategyService.

        Args:
            settings: Application settings
            repository: Database repository
            market_data_queue: Queue to receive candle events from
            trade_signal_queue: Queue to send trade signals to
            notification_queue: Queue to send notifications to
        """
        self.settings = settings
        self.repository = repository
        self.market_queue = market_data_queue
        self.trade_queue = trade_signal_queue
        self.notification_queue = notification_queue
        self.log = logger.bind(component="StrategyService")

        # Symbol state tracking
        self._symbol_states: Dict[str, SymbolState] = {}

        # Cache of open positions
        self._open_positions: Dict[str, Position] = {}  # symbol -> position

        # Background tasks
        self._consumer_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the strategy service."""
        self.log.info("Starting StrategyService")
        self._running = True

        # Load existing open positions
        await self._load_open_positions()

        # Start candle consumer
        self._consumer_task = asyncio.create_task(self._consume_candles())

        self.log.info("StrategyService started")

    async def stop(self) -> None:
        """Stop the strategy service."""
        self._running = False

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        self.log.info("StrategyService stopped")

    async def _load_open_positions(self) -> None:
        """Load existing open positions from database."""
        positions = await self.repository.get_all(
            Position,
            filters={"status": PositionStatus.OPEN},
        )

        for pos in positions:
            self._open_positions[pos.symbol] = pos

        self.log.info(f"Loaded {len(positions)} open positions")

    async def _consume_candles(self) -> None:
        """
        Consume candle events from market data queue.

        Main processing loop that:
        1. Receives candle updates
        2. Updates symbol state
        3. Checks entry/exit conditions
        4. Generates signals
        """
        self.log.info("Starting candle consumer")

        while self._running:
            try:
                # Get candle event from queue
                event: CandleEvent = await self.market_queue.get()

                try:
                    await self._process_candle(event)
                except Exception as e:
                    self.log.error(f"Error processing candle: {e}")
                finally:
                    self.market_queue.task_done()

            except asyncio.CancelledError:
                break

    async def _process_candle(self, event: CandleEvent) -> None:
        """
        Process a single candle event.

        Args:
            event: Candle event from market data service
        """
        symbol = event.symbol

        # Get or create symbol state
        if symbol not in self._symbol_states:
            self._symbol_states[symbol] = SymbolState(
                symbol=symbol,
                candles=[],
            )

        state = self._symbol_states[symbol]

        # Update state with new candle
        await self._update_symbol_state(state, event)

        # Only check signals on confirmed candles (minute boundary)
        if not event.is_confirmed:
            return

        # Check if we have a position
        state.has_position = symbol in self._open_positions
        if state.has_position:
            state.position_id = self._open_positions[symbol].id

        # Check entry/exit conditions
        if state.has_position:
            await self._check_exit_conditions(state, event)
        else:
            await self._check_entry_conditions(state, event)

    async def _update_symbol_state(
        self, state: SymbolState, event: CandleEvent
    ) -> None:
        """
        Update symbol state with new candle data.

        Args:
            state: Current symbol state
            event: New candle event
        """
        # Update last close price
        state.last_close = event.close

        # Add candle to history (only confirmed candles)
        if event.is_confirmed:
            candle_dict = {
                "timestamp": event.candle_timestamp,
                "open": event.open,
                "high": event.high,
                "low": event.low,
                "close": event.close,
                "volume": event.volume,
            }

            state.candles.append(candle_dict)

            # Keep only last N candles for MA calculation + volume history
            # 5 days * 24 hours * 60 minutes = 7200 candles
            max_candles = self.settings.candle_history_days * 24 * 60
            if len(state.candles) > max_candles:
                state.candles = state.candles[-max_candles:]

            # Update max volume (5-day)
            if len(state.candles) >= 2:
                volumes = [c["volume"] for c in state.candles]
                state.max_volume_5d = max(volumes[:-1])  # Exclude current

            # Update MA14
            state.ma14 = self._calculate_ma(state.candles, self.settings.ma_exit_period)

    def _calculate_ma(self, candles: List[Dict], period: int) -> float:
        """
        Calculate simple moving average.

        Args:
            candles: List of candle dicts with 'close' prices
            period: MA period

        Returns:
            MA value or 0.0 if not enough data
        """
        if len(candles) < period:
            return 0.0

        closes = [c["close"] for c in candles[-period:]]
        return sum(closes) / len(closes)

    # =========================================================================
    # Entry Logic
    # =========================================================================

    async def _check_entry_conditions(
        self, state: SymbolState, event: CandleEvent
    ) -> None:
        """
        Check if entry conditions are met.

        Entry conditions:
        1. Volume spike: current volume > multiplier * max historical volume
        2. Price acceleration: (close - open) / open >= threshold

        Args:
            state: Symbol state
            event: Current candle event
        """
        symbol = state.symbol

        # Check position limit
        if len(self._open_positions) >= self.settings.max_positions:
            self.log.debug(f"Skip {symbol}: max positions reached")
            return

        # Need enough history for volume comparison
        if state.max_volume_5d <= 0:
            self.log.debug(f"Skip {symbol}: no volume history")
            return

        # Check volume spike condition
        volume_ratio = event.volume / state.max_volume_5d
        volume_threshold = self.settings.volume_spike_multiplier

        if volume_ratio < volume_threshold:
            # No volume spike
            return

        # Check price acceleration condition
        if event.open <= 0:
            return

        price_change_pct = (event.close - event.open) / event.open * 100

        if price_change_pct < self.settings.min_price_change_pct:
            # No significant price move
            return

        # All conditions met - generate entry signal!
        self.log.info(
            f"ENTRY SIGNAL: {symbol} | "
            f"Volume: {volume_ratio:.2f}x max | "
            f"Price change: {price_change_pct:.2f}%"
        )

        # Calculate position size
        quantity = await self._calculate_position_size(symbol, event.close)

        if quantity <= 0:
            self.log.warning(f"Skip {symbol}: position size too small")
            return

        # Send signal to trade queue
        await self.trade_queue.put_entry_signal(
            symbol=symbol,
            price=event.close,
            quantity=quantity,
            reason=f"volume_spike:{volume_ratio:.2f}x,price_change:{price_change_pct:.2f}%",
        )

        # Log signal to database
        await self._log_signal(
            symbol=symbol,
            signal_type=SignalType.ENTRY,
            price=event.close,
            volume=event.volume,
            volume_ratio=volume_ratio,
            price_change_pct=price_change_pct,
            executed=True,
        )

        # Send notification
        await self.notification_queue.put_notification(
            message=(
                f"<b>BUY Signal</b>\n"
                f"Symbol: {symbol}\n"
                f"Price: {event.close:.8f}\n"
                f"Volume: {volume_ratio:.2f}x average\n"
                f"Change: {price_change_pct:.2f}%"
            ),
            source="StrategyService",
            priority="high",
        )

    async def _calculate_position_size(self, symbol: str, price: float) -> float:
        """
        Calculate position size based on risk parameters.

        Args:
            symbol: Trading symbol
            price: Current price

        Returns:
            Quantity to buy in base asset
        """
        # TODO: Implement actual position sizing
        # This is a stub that should:
        # 1. Get available USDT balance from TradingEngine
        # 2. Apply risk_per_trade_pct
        # 3. Check minimum trade amount
        # 4. Return quantity in base asset
        #
        # available_usdt = await self.trading_engine.get_available_balance("USDT")
        # trade_value = available_usdt * (self.settings.risk_per_trade_pct / 100)
        #
        # if trade_value < self.settings.min_trade_amount_usdt:
        #     return 0.0
        #
        # return trade_value / price

        # Placeholder: return a small fixed quantity
        return 0.0

    # =========================================================================
    # Exit Logic
    # =========================================================================

    async def _check_exit_conditions(
        self, state: SymbolState, event: CandleEvent
    ) -> None:
        """
        Check if exit conditions are met.

        Exit condition: Price crosses below MA14

        Args:
            state: Symbol state
            event: Current candle event
        """
        symbol = state.symbol

        # Need MA14 calculated
        if state.ma14 <= 0:
            return

        # Check MA crossover
        # Exit when close price falls below MA14
        if event.close >= state.ma14:
            # Still above MA, hold position
            return

        # Price crossed below MA14 - generate exit signal!
        self.log.info(
            f"EXIT SIGNAL: {symbol} | "
            f"Price: {event.close:.8f} < MA14: {state.ma14:.8f}"
        )

        # Send signal to trade queue
        await self.trade_queue.put_exit_signal(
            symbol=symbol,
            price=event.close,
            position_id=state.position_id,
            reason=f"ma14_crossover:price={event.close:.8f},ma={state.ma14:.8f}",
        )

        # Log signal to database
        await self._log_signal(
            symbol=symbol,
            signal_type=SignalType.EXIT,
            price=event.close,
            volume=event.volume,
            ma14_value=state.ma14,
            executed=True,
        )

        # Send notification
        await self.notification_queue.put_notification(
            message=(
                f"<b>SELL Signal</b>\n"
                f"Symbol: {symbol}\n"
                f"Price: {event.close:.8f}\n"
                f"MA14: {state.ma14:.8f}\n"
                f"Reason: Price crossed below MA14"
            ),
            source="StrategyService",
            priority="high",
        )

    # =========================================================================
    # Position Tracking
    # =========================================================================

    def on_position_opened(self, position: Position) -> None:
        """
        Called when a new position is opened.

        Args:
            position: The opened position
        """
        self._open_positions[position.symbol] = position
        self.log.info(f"Position opened: {position.symbol}")

    def on_position_closed(self, symbol: str) -> None:
        """
        Called when a position is closed.

        Args:
            symbol: Symbol of closed position
        """
        self._open_positions.pop(symbol, None)
        self.log.info(f"Position closed: {symbol}")

    def get_open_positions_count(self) -> int:
        """Get number of currently open positions."""
        return len(self._open_positions)

    # =========================================================================
    # Signal Logging
    # =========================================================================

    async def _log_signal(
        self,
        symbol: str,
        signal_type: SignalType,
        price: float,
        volume: Optional[float] = None,
        volume_ratio: Optional[float] = None,
        price_change_pct: Optional[float] = None,
        ma14_value: Optional[float] = None,
        executed: bool = True,
        execution_reason: Optional[str] = None,
    ) -> None:
        """
        Log a signal to the database for analysis.

        Args:
            symbol: Trading symbol
            signal_type: Entry or exit
            price: Signal price
            volume: Current volume
            volume_ratio: Volume / max historical volume
            price_change_pct: Price change percentage
            ma14_value: MA14 value (for exit signals)
            executed: Whether signal was acted upon
            execution_reason: Why not executed, if applicable
        """
        await self.repository.insert(
            Signal,
            {
                "symbol": symbol,
                "signal_type": signal_type,
                "timestamp": datetime.now(timezone.utc),
                "price": price,
                "volume": volume,
                "volume_ratio": volume_ratio,
                "price_change_pct": price_change_pct,
                "ma14_value": ma14_value,
                "executed": executed,
                "execution_reason": execution_reason,
            },
        )
