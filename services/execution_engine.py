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
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, Optional, Set
import os
import sys
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.models import Token, AllToken
from services.pipeline_events import PipelineMetrics, SignalType, TradingSignal


@dataclass
class ExitFailureInfo:
    """Tracks repeated exit failures for a symbol."""
    symbol: str
    position_id: int
    failure_count: int = 0
    last_failure_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_error: str = ""
    notified_user: bool = False  # Whether we've sent a "position stuck" notification


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
        telegram_queue: Optional[asyncio.Queue] = None,
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
            telegram_queue: Optional queue for Telegram notifications
            metrics: Optional shared metrics object
            logger: Optional logger instance
        """
        self._queue = signal_queue
        self._session_factory = session_factory
        self._position_model = position_model
        self._order_model = order_model
        self._signal_model = signal_model
        self._strategy_engine = strategy_engine
        self._telegram_queue = telegram_queue

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

        # Track exit failures to prevent spam and handle stuck positions
        self._exit_failures: Dict[str, ExitFailureInfo] = {}
        self._exit_cooldown_minutes = 5  # Wait 5 minutes between exit retry attempts
        self._max_exit_failures = 3  # After 3 failures, mark position as stuck

        # Loss threshold for disabling tokens (stored in database as BigLoss)
        self._disable_loss_threshold_pct = -1.0  # Disable if loss exceeds 1%

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
        """Load open positions from database and check for invalid/blacklisted symbols."""
        try:
            async with self._session_factory() as session:
                from db.models import PositionStatus, BlacklistedToken

                # Get open positions
                stmt = select(self._position_model).where(
                    self._position_model.status == PositionStatus.OPEN
                )
                result = await session.execute(stmt)
                positions = result.scalars().all()

                # Get blacklisted tokens
                blacklist_stmt = select(BlacklistedToken)
                blacklist_result = await session.execute(blacklist_stmt)
                blacklisted = {t.symbol.upper() for t in blacklist_result.scalars().all()}

            # Process positions
            valid_positions = {}
            positions_to_close = []

            for p in positions:
                symbol = p.symbol or ""
                coin = symbol.replace("USDT", "").replace("USDC", "")

                # Check for invalid symbol
                if not symbol or not coin or len(coin) < 2:
                    self._log.warning(
                        "Found position with invalid symbol: id=%d, symbol='%s'",
                        p.id, symbol
                    )
                    positions_to_close.append((p.id, symbol, "invalid_symbol"))
                    continue

                # Check if symbol is blacklisted
                if coin.upper() in blacklisted:
                    self._log.warning(
                        "Found open position for blacklisted token: %s (id=%d)",
                        symbol, p.id
                    )
                    positions_to_close.append((p.id, symbol, f"blacklisted_token: {coin}"))
                    continue

                valid_positions[symbol] = p.id

            # Close invalid/blacklisted positions
            for pos_id, symbol, reason in positions_to_close:
                await self._close_position_as_failed(
                    position_id=pos_id,
                    symbol=symbol,
                    reason=reason,
                )

            self._open_positions = valid_positions

            if positions_to_close:
                self._log.info(
                    "Synced %d open positions from DB, closed %d invalid/blacklisted",
                    len(valid_positions), len(positions_to_close)
                )
            else:
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

        # Gate check 1: Token not in tokens table or is_active=False?
        # Reasons: StalePrice, BigLoss, or removed during daily sync
        if not await self._is_token_active(symbol):
            self._log.info(
                "Skipping entry for %s: token inactive in database",
                symbol
            )
            await self._update_signal_execution(signal, False, "Token inactive in database")
            return

        # Gate check 2: Already have position for this symbol?
        if symbol in self._open_positions:
            self._log.info("Skipping entry for %s: already have open position", symbol)
            await self._update_signal_execution(signal, False, "Already have open position")
            return

        # Gate check 3: Max positions reached?
        if len(self._open_positions) >= self._max_positions:
            self._log.info(
                "Skipping entry for %s: max positions reached (%d/%d)",
                symbol, len(self._open_positions), self._max_positions
            )
            await self._update_signal_execution(signal, False, "Max positions reached")
            return

        # Gate check 4: Already entered this minute?
        minute_key = (symbol, signal.timestamp)
        if minute_key in self._entry_minutes:
            self._log.info("Skipping entry for %s: already entered this minute", symbol)
            await self._update_signal_execution(signal, False, "Duplicate entry this minute")
            return

        # Gate check 5: Price must be above MA14 for entry
        if signal.ma14_value is not None and signal.price < signal.ma14_value:
            self._log.info(
                "Skipping entry for %s: price %.6f <= MA14 %.6f",
                symbol, signal.price, signal.ma14_value
            )
            await self._update_signal_execution(signal, False, "Price below MA14")
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

            # Use actual filled values from batch result
            filled_qty = order_result.get("filled_qty", quantity)
            avg_price = order_result.get("avg_price", signal.price)
            actual_value = filled_qty * avg_price

            # Get order IDs (may be multiple for split orders)
            order_ids = order_result.get("order_ids", [])
            order_id_str = ",".join(order_ids) if order_ids else order_result.get("order_id")

            # Create position with actual filled values
            position_id = await self._create_position(
                symbol=symbol,
                entry_price=avg_price,  # Use average price from batch
                entry_amount=filled_qty,  # Use actual filled quantity
                entry_value=actual_value,
                order_id=order_id_str,
            )

            if position_id:
                self._open_positions[symbol] = position_id

                # Sync with strategy engine (pass entry price for stop-loss tracking)
                if self._strategy_engine:
                    self._strategy_engine.mark_position_opened(symbol, entry_price=avg_price)

                self._metrics.signals_executed += 1
                await self._update_signal_execution(signal, True, None)

                # Log with batch info
                orders_info = ""
                if order_result.get("orders_completed", 1) > 1:
                    orders_info = f" ({order_result['orders_completed']} orders)"

                self._log.info(
                    "ENTRY executed: %s qty=%.6f @ %.6f value=%.2f USDT%s",
                    symbol, filled_qty, avg_price, actual_value, orders_info
                )

                # Log partial success if any orders failed
                if order_result.get("partial"):
                    self._log.warning(
                        "ENTRY partial: %s failed_qty=%.6f errors=%s",
                        symbol, order_result.get("failed_qty", 0),
                        order_result.get("errors", [])
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

        # Check if this symbol has repeated exit failures (cooldown)
        if symbol in self._exit_failures:
            failure_info = self._exit_failures[symbol]
            time_since_failure = datetime.now(timezone.utc) - failure_info.last_failure_time
            cooldown = timedelta(minutes=self._exit_cooldown_minutes)

            # If we've hit max failures, close position with error and stop trying
            if failure_info.failure_count >= self._max_exit_failures:
                if not failure_info.notified_user:
                    self._log.error(
                        "Position %s stuck after %d failed sell attempts. "
                        "Last error: %s. Closing position as FAILED.",
                        symbol, failure_info.failure_count, failure_info.last_error
                    )
                    # Close position with error reason
                    await self._close_position_as_failed(
                        position_id=position_id,
                        symbol=symbol,
                        reason=f"sell_failed_{failure_info.failure_count}x: {failure_info.last_error}"
                    )
                    failure_info.notified_user = True
                return

            # Still in cooldown period
            if time_since_failure < cooldown:
                self._log.debug(
                    "Skipping exit for %s: in cooldown (failed %d times, retry in %d min)",
                    symbol, failure_info.failure_count,
                    int((cooldown - time_since_failure).total_seconds() / 60)
                )
                return

        # Get position details
        position = await self._get_position(position_id)
        if not position:
            self._log.warning("Position %d not found for %s", position_id, symbol)
            return

        # Validate symbol before attempting sell
        coin = symbol.replace("USDT", "").replace("USDC", "")
        if not coin or len(coin) < 2:
            self._log.error(
                "Invalid symbol %s (coin=%s) - closing position as failed", symbol, coin
            )
            await self._close_position_as_failed(
                position_id=position_id,
                symbol=symbol,
                reason=f"invalid_symbol: {symbol}"
            )
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
            # Clear failure tracking on success
            if symbol in self._exit_failures:
                del self._exit_failures[symbol]

            # Use actual filled values from batch result
            filled_qty = order_result.get("filled_qty", position.entry_amount)
            avg_exit_price = order_result.get("avg_price", signal.price)

            # Calculate P&L with actual values
            exit_value = filled_qty * avg_exit_price
            # Use proportional entry value if partial fill
            if filled_qty < position.entry_amount:
                proportional_entry_value = (filled_qty / position.entry_amount) * position.entry_value_usdt
            else:
                proportional_entry_value = position.entry_value_usdt

            profit_usdt = exit_value - proportional_entry_value
            profit_pct = (profit_usdt / proportional_entry_value) * 100 if proportional_entry_value > 0 else 0

            # Get order IDs (may be multiple for split orders)
            order_ids = order_result.get("order_ids", [])
            order_id_str = ",".join(order_ids) if order_ids else order_result.get("order_id")

            # Close position
            await self._close_position(
                position_id=position_id,
                exit_price=avg_exit_price,  # Use average price from batch
                exit_amount=filled_qty,  # Use actual filled quantity
                exit_value=exit_value,
                exit_reason="ma14_crossover",
                order_id=order_id_str,
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

            # Log with batch info
            orders_info = ""
            if order_result.get("orders_completed", 1) > 1:
                orders_info = f" ({order_result['orders_completed']} orders)"

            self._log.info(
                "EXIT executed: %s @ %.6f P&L=%.2f USDT (%.2f%%)%s",
                symbol, avg_exit_price, profit_usdt, profit_pct, orders_info
            )

            # Log partial success if any orders failed
            if order_result.get("partial"):
                self._log.warning(
                    "EXIT partial: %s failed_qty=%.6f errors=%s",
                    symbol, order_result.get("failed_qty", 0),
                    order_result.get("errors", [])
                )

            # Check if loss exceeds threshold - disable token for trading
            if profit_pct < self._disable_loss_threshold_pct:
                await self.disable_token_for_loss(symbol, profit_pct)
        else:
            # Track failure
            error_msg = order_result.get("error", "Order failed")
            if symbol in self._exit_failures:
                self._exit_failures[symbol].failure_count += 1
                self._exit_failures[symbol].last_failure_time = datetime.now(timezone.utc)
                self._exit_failures[symbol].last_error = error_msg
            else:
                self._exit_failures[symbol] = ExitFailureInfo(
                    symbol=symbol,
                    position_id=position_id,
                    failure_count=1,
                    last_failure_time=datetime.now(timezone.utc),
                    last_error=error_msg,
                )

            self._log.warning(
                "Exit failed for %s (attempt %d/%d): %s. Next retry in %d minutes.",
                symbol,
                self._exit_failures[symbol].failure_count,
                self._max_exit_failures,
                error_msg,
                self._exit_cooldown_minutes,
            )

            await self._update_signal_execution(signal, False, error_msg)

    async def _is_token_active(self, symbol: str) -> bool:
        """
        Check if token is active in the database.

        Checks the Token table for is_active=True.
        Tokens with stale prices are set to is_active=False by StalePriceChecker.

        Args:
            symbol: The trading pair symbol (e.g., "BTCUSDT")

        Returns:
            True if token is active, False if inactive or not found
        """
        try:
            async with self._session_factory() as session:
                result = await session.execute(
                    select(Token.is_active).where(Token.bybit_symbol == symbol)
                )
                row = result.scalar_one_or_none()

                if row is None:
                    # Token not found in database - don't trade unknown tokens
                    self._log.warning(
                        "Token %s not found in database, treating as inactive",
                        symbol
                    )
                    return False

                return bool(row)

        except Exception as e:
            self._log.exception("Failed to check token active status: %s", e)
            # On error, default to allowing trade (don't block due to DB issues)
            return True

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

    async def _close_position_as_failed(
        self,
        position_id: int,
        symbol: str,
        reason: str,
    ) -> bool:
        """
        Close a position as FAILED when sell cannot be completed.

        This happens when:
        - Symbol is invalid/delisted
        - No balance available
        - Max sell retries exceeded

        The position is marked as closed with exit_reason containing the error.
        """
        try:
            from db.models import PositionStatus
            from sqlalchemy import update

            self._log.warning(
                "Closing position %s (id=%d) as FAILED: %s",
                symbol, position_id, reason
            )

            async with self._session_factory() as session:
                stmt = (
                    update(self._position_model)
                    .where(self._position_model.id == position_id)
                    .values(
                        status=PositionStatus.CLOSED,
                        exit_time=datetime.now(timezone.utc),
                        exit_reason=reason,
                        exit_price=0,
                        exit_amount=0,
                        exit_value_usdt=0,
                        profit_usdt=0,
                        profit_pct=-100.0,  # Mark as total loss
                    )
                )
                await session.execute(stmt)
                await session.commit()

            # Remove from open positions tracking
            if symbol in self._open_positions:
                del self._open_positions[symbol]

            # Clear failure tracking
            if symbol in self._exit_failures:
                del self._exit_failures[symbol]

            # Sync with strategy engine
            if self._strategy_engine:
                self._strategy_engine.mark_position_closed(symbol)

            return True

        except Exception as e:
            self._log.exception("Failed to close position as failed: %s", e)
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

    async def disable_token_for_loss(self, symbol: str, loss_pct: float) -> bool:
        """
        Disable a token due to significant trading loss.

        Removes token from 'tokens' table and marks it in 'all_tokens'
        with deactivation_reason='BigLoss'. Token will be re-enabled
        during the next daily sync if it passes all filters.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            loss_pct: Percentage loss (negative value)

        Returns:
            True if token was disabled, False on error
        """
        try:
            # Extract base symbol (e.g., "BTC" from "BTCUSDT")
            base_symbol = symbol.replace("USDT", "").replace("USDC", "")

            async with self._session_factory() as session:
                from sqlalchemy import delete, update

                # 1. Delete from tokens table
                delete_stmt = delete(Token).where(Token.bybit_symbol == symbol)
                await session.execute(delete_stmt)

                # 2. Update all_tokens with BigLoss reason
                update_stmt = (
                    update(AllToken)
                    .where(AllToken.bybit_symbol == symbol)
                    .values(
                        is_active=False,
                        deactivation_reason="BigLoss",
                    )
                )
                await session.execute(update_stmt)
                await session.commit()

            self._log.warning(
                "Token %s DISABLED for trading (loss: %.2f%%, threshold: %.2f%%). "
                "Will be re-enabled on next daily sync.",
                symbol, loss_pct, self._disable_loss_threshold_pct
            )

            # Send Telegram notification
            await self._send_token_disabled_notification(symbol, loss_pct)

            return True

        except Exception as e:
            self._log.exception("Failed to disable token %s: %s", symbol, e)
            return False

    async def _send_token_disabled_notification(
        self, symbol: str, loss_pct: float
    ) -> None:
        """Send Telegram notification when a token is disabled due to loss."""
        if not self._telegram_queue:
            return

        try:
            coin = symbol.replace("USDT", "").replace("USDC", "")
            msg = (
                f"<b>⛔ Токен отключён</b>\n\n"
                f"Монета: {coin}\n"
                f"Причина: убыток {loss_pct:.1f}%\n"
                f"Порог: {self._disable_loss_threshold_pct:.1f}%\n\n"
                f"<i>Токен вернётся в торговлю при следующей синхронизации (каждые 6ч)</i>"
            )
            self._telegram_queue.put_nowait({"text": msg, "parse_mode": "HTML"})
        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, token disabled notification dropped")
        except Exception as e:
            self._log.error("Failed to send token disabled notification: %s", e)