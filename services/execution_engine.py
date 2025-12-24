"""
Execution Engine for the Trading Pipeline.

Consumes TradingSignal events, performs risk checks, calculates
position sizing, places orders, and persists results.

Gate Checks:
- Global max positions limit
- No duplicate open position per symbol
- Per-minute entry deduplication
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Set
import os
import sys
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.pipeline_events import PipelineMetrics, SignalType, TradingSignal


class ExecutionEngine:
    """
    Execution engine that processes trading signals.

    Handles risk management, position sizing, and order placement.
    """

    def __init__(
        self,
        signal_queue: asyncio.Queue,
        session_factory: sessionmaker,
        position_model: Any,
        order_model: Any,
        signal_model: Any,
        *,
        strategy_engine: Optional[Any] = None,  # For position state sync
        max_positions: int = 5,
        risk_per_trade_pct: float = 5.0,  # 5% of balance
        min_trade_usdt: float = 10.0,
        get_balance_fn: Optional[Callable[[], float]] = None,
        place_order_fn: Optional[Callable[[str, str, float, float], Dict[str, Any]]] = None,
        metrics: Optional[PipelineMetrics] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize the execution engine.

        Args:
            signal_queue: Queue to consume TradingSignal events from
            session_factory: SQLAlchemy async session factory
            position_model: Position SQLAlchemy model
            order_model: Order SQLAlchemy model
            signal_model: Signal SQLAlchemy model
            strategy_engine: Reference to strategy engine for state sync
            max_positions: Maximum simultaneous open positions
            risk_per_trade_pct: Percentage of balance to risk per trade
            min_trade_usdt: Minimum trade size in USDT
            get_balance_fn: Async function to get available USDT balance
            place_order_fn: Async function to place orders
            metrics: Optional shared metrics object
            logger: Optional logger instance
        """
        self._queue = signal_queue
        self._session_factory = session_factory
        self._position_model = position_model
        self._order_model = order_model
        self._signal_model = signal_model
        self._strategy_engine = strategy_engine

        self._max_positions = int(max_positions)
        self._risk_pct = float(risk_per_trade_pct) / 100.0
        self._min_trade = float(min_trade_usdt)

        self._get_balance = get_balance_fn
        self._place_order = place_order_fn

        self._metrics = metrics or PipelineMetrics()
        self._log = logger or logging.getLogger(self.__class__.__name__)

        # Track open positions in memory (synced from DB at startup)
        self._open_positions: Dict[str, int] = {}  # symbol -> position_id
        self._entry_minutes: Set[tuple] = set()  # (symbol, minute_ts) dedup

        # Task handle
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        # Simulated balance for testing (when no real balance function)
        self._simulated_balance = 10000.0

    async def start(self) -> None:
        """Start the execution engine."""
        if self._task is not None:
            self._log.warning("Engine already running")
            return

        self._stop_event.clear()

        # Sync open positions from DB
        await self._sync_open_positions()

        # Sync with strategy engine
        if self._strategy_engine:
            self._strategy_engine.set_open_positions(set(self._open_positions.keys()))

        self._task = asyncio.create_task(self._run(), name="execution-engine")
        self._log.info(
            "Execution engine started (max_pos=%d, risk=%.1f%%, min=%.2f USDT)",
            self._max_positions,
            self._risk_pct * 100,
            self._min_trade,
        )

    async def stop(self) -> None:
        """Stop the execution engine gracefully."""
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._log.info("Execution engine stopped")

    async def _sync_open_positions(self) -> None:
        """Load open positions from database."""
        try:
            async with self._session_factory() as session:
                from db.models import PositionStatus

                stmt = select(self._position_model).where(
                    self._position_model.status == PositionStatus.OPEN
                )
                result = await session.execute(stmt)
                positions = result.scalars().all()

            self._open_positions = {p.symbol: p.id for p in positions}
            self._log.info("Synced %d open positions from DB", len(self._open_positions))

        except Exception as e:
            self._log.exception("Failed to sync positions: %s", e)

    async def _run(self) -> None:
        """Main consumer loop."""
        while not self._stop_event.is_set():
            try:
                try:
                    signal: TradingSignal = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                await self._process_signal(signal)
                self._queue.task_done()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error processing signal: %s", e)
                self._metrics.errors += 1

    async def _process_signal(self, signal: TradingSignal) -> None:
        """Process a single trading signal."""
        self._log.info(
            "Processing signal: %s %s @ %.6f",
            signal.signal_type.value,
            signal.symbol,
            signal.price,
        )

        # Persist signal to DB
        await self._persist_signal(signal)

        if signal.signal_type == SignalType.ENTRY:
            await self._handle_entry(signal)
        elif signal.signal_type == SignalType.EXIT:
            await self._handle_exit(signal)

    async def _handle_entry(self, signal: TradingSignal) -> None:
        """Handle an entry signal."""
        symbol = signal.symbol

        # Gate check 1: Already have position for this symbol?
        if symbol in self._open_positions:
            self._log.info("Skipping entry for %s: already have open position", symbol)
            await self._update_signal_execution(signal, False, "Already have open position")
            return

        # Gate check 2: Max positions reached?
        if len(self._open_positions) >= self._max_positions:
            self._log.info(
                "Skipping entry for %s: max positions reached (%d/%d)",
                symbol, len(self._open_positions), self._max_positions
            )
            await self._update_signal_execution(signal, False, "Max positions reached")
            return

        # Gate check 3: Already entered this minute?
        minute_key = (symbol, signal.timestamp)
        if minute_key in self._entry_minutes:
            self._log.info("Skipping entry for %s: already entered this minute", symbol)
            await self._update_signal_execution(signal, False, "Duplicate entry this minute")
            return

        # Calculate position size
        balance = await self._get_available_balance()
        position_usdt = balance * self._risk_pct

        if position_usdt < self._min_trade:
            self._log.info(
                "Skipping entry for %s: position size %.2f < min %.2f",
                symbol, position_usdt, self._min_trade
            )
            await self._update_signal_execution(signal, False, "Position size below minimum")
            return

        quantity = position_usdt / signal.price

        # Place order with context for notifications
        order_result = await self._execute_buy(
            symbol, quantity, signal.price,
            volume_ratio=signal.volume_ratio,
            price_change_pct=signal.price_change_pct,
        )

        if order_result.get("success"):
            # Record entry
            self._entry_minutes.add(minute_key)

            # Create position
            position_id = await self._create_position(
                symbol=symbol,
                entry_price=signal.price,
                entry_amount=quantity,
                entry_value=position_usdt,
                order_id=order_result.get("order_id"),
            )

            if position_id:
                self._open_positions[symbol] = position_id

                # Sync with strategy engine
                if self._strategy_engine:
                    self._strategy_engine.mark_position_opened(symbol)

                self._metrics.signals_executed += 1
                await self._update_signal_execution(signal, True, None)

                self._log.info(
                    "ENTRY executed: %s qty=%.6f value=%.2f USDT",
                    symbol, quantity, position_usdt
                )
        else:
            await self._update_signal_execution(
                signal, False, order_result.get("error", "Order failed")
            )

    async def _handle_exit(self, signal: TradingSignal) -> None:
        """Handle an exit signal."""
        symbol = signal.symbol

        # Must have open position
        if symbol not in self._open_positions:
            self._log.info("Skipping exit for %s: no open position", symbol)
            await self._update_signal_execution(signal, False, "No open position")
            return

        position_id = self._open_positions[symbol]

        # Get position details
        position = await self._get_position(position_id)
        if not position:
            self._log.warning("Position %d not found for %s", position_id, symbol)
            return

        # Calculate expected P&L for context
        exit_value = position.entry_amount * signal.price
        expected_profit_usdt = exit_value - position.entry_value_usdt
        expected_profit_pct = (expected_profit_usdt / position.entry_value_usdt) * 100

        # Place sell order with context for notifications
        order_result = await self._execute_sell(
            symbol, position.entry_amount, signal.price,
            position_id=position_id,
            entry_price=position.entry_price,
            expected_pnl_usdt=expected_profit_usdt,
            expected_pnl_pct=expected_profit_pct,
        )

        if order_result.get("success"):
            # Calculate P&L
            exit_value = position.entry_amount * signal.price
            profit_usdt = exit_value - position.entry_value_usdt
            profit_pct = (profit_usdt / position.entry_value_usdt) * 100

            # Close position
            await self._close_position(
                position_id=position_id,
                exit_price=signal.price,
                exit_amount=position.entry_amount,
                exit_value=exit_value,
                exit_reason="ma14_crossover",
                order_id=order_result.get("order_id"),
                profit_usdt=profit_usdt,
                profit_pct=profit_pct,
            )

            # Remove from open positions
            del self._open_positions[symbol]

            # Sync with strategy engine
            if self._strategy_engine:
                self._strategy_engine.mark_position_closed(symbol)

            self._metrics.signals_executed += 1
            await self._update_signal_execution(signal, True, None)

            self._log.info(
                "EXIT executed: %s P&L=%.2f USDT (%.2f%%)",
                symbol, profit_usdt, profit_pct
            )
        else:
            await self._update_signal_execution(
                signal, False, order_result.get("error", "Order failed")
            )

    async def _get_available_balance(self) -> float:
        """Get available USDT balance."""
        if self._get_balance:
            try:
                return await self._get_balance()
            except Exception as e:
                self._log.warning("Failed to get balance: %s, using simulated", e)

        return self._simulated_balance

    async def _execute_buy(
        self, symbol: str, quantity: float, price: float, **context
    ) -> Dict[str, Any]:
        """Execute a market buy order with optional context for notifications."""
        if self._place_order:
            try:
                # Use market order (price=None) for immediate execution
                return await self._place_order(
                    symbol, "Buy", quantity, None,  # None = market order
                    signal_type="entry",
                    volume_ratio=context.get("volume_ratio"),
                    price_change_pct=context.get("price_change_pct"),
                )
            except Exception as e:
                self._log.exception("Buy order failed: %s", e)
                return {"success": False, "error": str(e)}

        # Stub: simulate successful order
        self._log.info("[STUB] Buy order: %s qty=%.6f @ %.6f", symbol, quantity, price)
        return {
            "success": True,
            "order_id": f"stub-{datetime.now().timestamp()}",
        }

    async def _execute_sell(
        self, symbol: str, quantity: float, price: float, **context
    ) -> Dict[str, Any]:
        """Execute a market sell order with optional context for notifications."""
        if self._place_order:
            try:
                # Use market order (price=None) for immediate execution
                return await self._place_order(
                    symbol, "Sell", quantity, None,  # None = market order
                    signal_type="exit",
                    position_id=context.get("position_id"),
                    entry_price=context.get("entry_price"),
                    expected_pnl_usdt=context.get("expected_pnl_usdt"),
                    expected_pnl_pct=context.get("expected_pnl_pct"),
                )
            except Exception as e:
                self._log.exception("Sell order failed: %s", e)
                return {"success": False, "error": str(e)}

        # Stub: simulate successful order
        self._log.info("[STUB] Sell order: %s qty=%.6f @ %.6f", symbol, quantity, price)
        return {
            "success": True,
            "order_id": f"stub-{datetime.now().timestamp()}",
        }

    async def _create_position(
        self,
        symbol: str,
        entry_price: float,
        entry_amount: float,
        entry_value: float,
        order_id: Optional[str] = None,
    ) -> Optional[int]:
        """Create a new position in the database."""
        try:
            from db.models import PositionStatus

            async with self._session_factory() as session:
                position = self._position_model(
                    symbol=symbol,
                    status=PositionStatus.OPEN,
                    entry_price=entry_price,
                    entry_amount=entry_amount,
                    entry_value_usdt=entry_value,
                    entry_time=datetime.now(timezone.utc),
                    entry_order_id=order_id,
                )
                session.add(position)
                await session.commit()
                await session.refresh(position)
                return position.id

        except Exception as e:
            self._log.exception("Failed to create position: %s", e)
            return None

    async def _get_position(self, position_id: int) -> Optional[Any]:
        """Get a position by ID."""
        try:
            async with self._session_factory() as session:
                result = await session.get(self._position_model, position_id)
                return result
        except Exception as e:
            self._log.exception("Failed to get position: %s", e)
            return None

    async def _close_position(
        self,
        position_id: int,
        exit_price: float,
        exit_amount: float,
        exit_value: float,
        exit_reason: str,
        order_id: Optional[str] = None,
        profit_usdt: float = 0,
        profit_pct: float = 0,
    ) -> bool:
        """Close a position in the database."""
        try:
            from db.models import PositionStatus
            from sqlalchemy import update

            async with self._session_factory() as session:
                stmt = (
                    update(self._position_model)
                    .where(self._position_model.id == position_id)
                    .values(
                        status=PositionStatus.CLOSED,
                        exit_price=exit_price,
                        exit_amount=exit_amount,
                        exit_value_usdt=exit_value,
                        exit_time=datetime.now(timezone.utc),
                        exit_order_id=order_id,
                        exit_reason=exit_reason,
                        profit_usdt=profit_usdt,
                        profit_pct=profit_pct,
                    )
                )
                await session.execute(stmt)
                await session.commit()
                return True

        except Exception as e:
            self._log.exception("Failed to close position: %s", e)
            return False

    async def _persist_signal(self, signal: TradingSignal) -> None:
        """Persist a signal to the database for analysis."""
        try:
            from db.models import SignalType as DBSignalType

            async with self._session_factory() as session:
                db_signal = self._signal_model(
                    symbol=signal.symbol,
                    signal_type=DBSignalType(signal.signal_type.value),
                    timestamp=signal.timestamp,
                    price=signal.price,
                    volume=signal.volume,
                    volume_ratio=signal.volume_ratio,
                    price_change_pct=signal.price_change_pct,
                    ma14_value=signal.ma14_value,
                    executed=False,  # Updated later
                )
                session.add(db_signal)
                await session.commit()

        except Exception as e:
            self._log.exception("Failed to persist signal: %s", e)

    async def _update_signal_execution(
        self, signal: TradingSignal, executed: bool, reason: Optional[str]
    ) -> None:
        """Update signal execution status in database."""
        try:
            from sqlalchemy import update

            async with self._session_factory() as session:
                stmt = (
                    update(self._signal_model)
                    .where(self._signal_model.symbol == signal.symbol)
                    .where(self._signal_model.timestamp == signal.timestamp)
                    .values(executed=executed, execution_reason=reason)
                )
                await session.execute(stmt)
                await session.commit()

        except Exception as e:
            self._log.debug("Failed to update signal execution: %s", e)