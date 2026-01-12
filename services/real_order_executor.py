"""
Real Order Executor Service.

Integrates the OrderQueue from trade_client.py with the trading pipeline.
Executes real orders on Bybit, logs to database, and sends Telegram notifications.

Supports order splitting when quantity exceeds exchange maxOrderQty limit.
Learns max quantities from error messages and stores them in the database.
"""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Awaitable

from loguru import logger
from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from trade.trade_client import OrderQueue, QueuedOrder, OrderStatus, Category, truncate_to_step
from config.config import settings


def parse_max_qty_from_error(error_message: str) -> Optional[float]:
    """
    Parse max quantity limit from Bybit error message.

    Example error: "The quantity of a single market order must be less than
                   the maximum allowed per order: 1100000MEE."

    Returns the numeric limit (1100000) or None if not found.
    """
    if not error_message:
        return None

    # Pattern: "maximum allowed per order: <number><token_symbol>"
    # The number can have commas or be plain digits
    patterns = [
        r'maximum allowed per order:\s*([\d,]+)',  # "maximum allowed per order: 1100000"
        r'less than.*?:\s*([\d,]+)',  # Fallback: "less than X: 1100000"
        r'must be less than\s*([\d,]+)',  # Another variant
    ]

    for pattern in patterns:
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            # Remove commas and convert to float
            qty_str = match.group(1).replace(',', '')
            try:
                return float(qty_str)
            except ValueError:
                continue

    return None


def format_price(price: float, min_significant: int = 3) -> str:
    """
    Format price with enough decimal places to show significant digits.

    For very small prices (e.g., 0.000000345), shows all zeros plus
    at least min_significant digits.

    Args:
        price: The price to format
        min_significant: Minimum significant digits to show (default 3)

    Returns:
        Formatted price string

    Examples:
        format_price(25.81) -> "25.810000"
        format_price(0.000000345) -> "0.000000345"
        format_price(0.00000123) -> "0.00000123"
    """
    if price == 0:
        return "0.000000"

    if price >= 0.000001:
        # For normal prices, use 6 decimal places
        formatted = f"{price:.6f}"
        # But check if all decimals are zeros after the dot
        if float(formatted) == 0:
            # Need more decimals
            pass
        else:
            return formatted

    # For very small prices, find how many decimals we need
    # to show at least min_significant digits
    abs_price = abs(price)

    # Find the position of the first significant digit
    if abs_price >= 1:
        return f"{price:.6f}"

    # Calculate decimals needed: -log10(price) + min_significant - 1
    decimals_needed = int(-math.log10(abs_price)) + min_significant
    decimals_needed = max(6, min(decimals_needed, 15))  # Clamp between 6 and 15

    return f"{price:.{decimals_needed}f}"


@dataclass
class BatchOrderResult:
    """Result of a batch order execution."""
    success: bool
    total_quantity: float = 0.0
    filled_quantity: float = 0.0
    failed_quantity: float = 0.0
    avg_price: float = 0.0
    order_ids: List[str] = field(default_factory=list)
    orders_completed: int = 0
    orders_failed: int = 0
    errors: List[str] = field(default_factory=list)
    first_order_time: Optional[datetime] = None


class RealOrderExecutorService:
    """
    Real order executor that uses OrderQueue from trade_client.py.

    Features:
    - Executes real orders on Bybit via OrderQueue
    - Splits large orders into multiple smaller ones when exceeding maxOrderQty
    - Sends Telegram notifications only after all orders in batch complete
    - Logs each order to database individually
    - Queues order execution for sequential processing
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        session_factory: sessionmaker,
        order_model: Any,
        *,
        telegram_queue: Optional[asyncio.Queue] = None,
        demo: bool = False,
        max_concurrent: int = 1,
        retry_count: int = 3,
        category: str = "spot",
    ) -> None:
        """
        Initialize the real order executor.

        Args:
            api_key: Bybit API key
            api_secret: Bybit API secret
            session_factory: SQLAlchemy async session factory
            order_model: Order SQLAlchemy model
            telegram_queue: Optional queue for Telegram notifications
            demo: Use Bybit demo if True
            max_concurrent: Maximum concurrent order executions
            retry_count: Number of retries for failed orders
            category: Bybit category (spot, linear, inverse)
        """
        self._session_factory = session_factory
        self._order_model = order_model
        self._telegram_queue = telegram_queue
        self._category = Category(category)

        # Initialize OrderQueue from trade_client
        self._order_queue = OrderQueue(
            api_key=api_key,
            api_secret=api_secret,
            demo=demo,
            max_concurrent=max_concurrent,
            retry_count=retry_count,
        )

        # Don't use callbacks - we handle notifications after batch completes
        self._order_queue.on_completed = None
        self._order_queue.on_failed = None

        # Pending order requests awaiting completion
        self._pending_requests: Dict[str, Dict[str, Any]] = {}

        self._log = logger.bind(stream="trading", component="RealOrderExecutor")
        self._running = False

        # Cache for max_market_qty learned from errors (symbol -> max_qty)
        self._max_qty_cache: Dict[str, float] = {}

        # Statistics
        self._stats = {
            "orders_placed": 0,
            "orders_completed": 0,
            "orders_failed": 0,
            "batches_executed": 0,
        }

    async def start(self) -> None:
        """Start the order executor."""
        if self._running:
            self._log.warning("Order executor already running")
            return

        await self._order_queue.start()
        self._running = True

        mode = "TESTNET" if self._order_queue.demo else "MAINNET"
        self._log.info("Real order executor started ({} mode)", mode)

    async def stop(self) -> None:
        """Stop the order executor gracefully."""
        if not self._running:
            return

        await self._order_queue.stop(wait=True)
        self._running = False

        self._log.info(
            "Real order executor stopped | placed={} completed={} failed={} batches={}",
            self._stats["orders_placed"],
            self._stats["orders_completed"],
            self._stats["orders_failed"],
            self._stats["batches_executed"],
        )

    async def get_balance(self, coin: str = "USDT") -> float:
        """Get available balance for a coin."""
        try:
            balances = await self._order_queue.get_balance(coin)
            return balances.get(coin, 0.0)
        except Exception as e:
            self._log.error("Failed to get balance for {}: {}", coin, e)
            await self._send_error_notification(f"Ошибка получения баланса: {e}")
            return 0.0

    async def get_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol."""
        try:
            return await self._order_queue.get_price(symbol, self._category)
        except Exception as e:
            self._log.error("Failed to get price for {}: {}", symbol, e)
            return None

    async def _get_stored_max_qty(self, symbol: str) -> Optional[float]:
        """
        Get stored max_market_qty from database for a symbol.

        First checks in-memory cache, then queries database.
        """
        # Check cache first
        if symbol in self._max_qty_cache:
            return self._max_qty_cache[symbol]

        try:
            from db.models import Token

            async with self._session_factory() as session:
                stmt = select(Token.max_market_qty).where(Token.bybit_symbol == symbol)
                result = await session.execute(stmt)
                max_qty = result.scalar_one_or_none()

                if max_qty:
                    self._max_qty_cache[symbol] = max_qty
                    self._log.debug("Loaded stored max_market_qty for {}: {}", symbol, max_qty)

                return max_qty

        except Exception as e:
            self._log.error("Failed to get stored max_qty for {}: {}", symbol, e)
            return None

    async def _save_max_qty(self, symbol: str, max_qty: float) -> bool:
        """
        Save learned max_market_qty to database.

        Updates the token record and refreshes the cache.
        """
        try:
            from db.models import Token

            async with self._session_factory() as session:
                stmt = (
                    update(Token)
                    .where(Token.bybit_symbol == symbol)
                    .values(max_market_qty=max_qty)
                )
                result = await session.execute(stmt)
                await session.commit()

                if result.rowcount > 0:
                    self._max_qty_cache[symbol] = max_qty
                    self._log.info(
                        "Saved max_market_qty for {}: {} (learned from error)",
                        symbol, max_qty
                    )
                    return True
                else:
                    self._log.warning("Token {} not found in DB, cannot save max_qty", symbol)
                    return False

        except Exception as e:
            self._log.error("Failed to save max_qty for {}: {}", symbol, e)
            return False

    def _calculate_order_chunks(
        self, total_quantity: float, max_qty: float, qty_step: str
    ) -> List[float]:
        """
        Split total quantity into chunks not exceeding max_qty.

        Distributes quantity evenly across minimum number of orders.
        """
        if total_quantity <= max_qty:
            return [total_quantity]

        # Calculate minimum number of orders needed
        num_orders = math.ceil(total_quantity / max_qty)

        # Distribute evenly
        qty_per_order = total_quantity / num_orders

        # Truncate each to step precision
        chunks = []
        remaining = total_quantity

        for i in range(num_orders):
            if i == num_orders - 1:
                # Last chunk gets the remainder
                chunk = remaining
            else:
                chunk = qty_per_order

            # Truncate to step
            chunk_str = truncate_to_step(chunk, qty_step)
            chunk_val = float(chunk_str)

            if chunk_val > 0:
                chunks.append(chunk_val)
                remaining -= chunk_val

        self._log.info(
            "Split order: total={} max_qty={} -> {} orders: {}",
            total_quantity, max_qty, len(chunks), chunks
        )

        return chunks

    async def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: Optional[float] = None,
        *,
        signal_type: str = "",
        position_id: Optional[int] = None,
        entry_price: Optional[float] = None,
        expected_pnl_usdt: Optional[float] = None,
        expected_pnl_pct: Optional[float] = None,
        volume_ratio: Optional[float] = None,
        price_change_pct: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Place an order on Bybit. Splits into multiple orders if quantity exceeds max.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            side: "Buy" or "Sell"
            quantity: Order quantity
            price: Limit price (None for market order)
            signal_type: "entry" or "exit"
            position_id: Associated position ID
            entry_price: Entry price (for exit orders)
            expected_pnl_usdt: Expected P&L in USDT (for exit orders)
            expected_pnl_pct: Expected P&L percentage (for exit orders)
            volume_ratio: Volume ratio (for entry orders)
            price_change_pct: Price change percentage (for entry orders)

        Returns:
            Dict with success status and order details
        """
        try:
            # Get qty_step and max_qty for proper truncation and limits
            min_info = await self._order_queue.get_min_order(symbol, self._category)
            qty_step = min_info.get("qty_step", "0.000001")

            # IMPORTANT: Bybit has DIFFERENT limits for market vs limit orders!
            # - maxOrderQty: Maximum for LIMIT orders
            # - maxMktOrderQty: Maximum for MARKET orders (usually lower)
            max_limit_qty_str = min_info.get("max_qty")  # For limit orders
            max_mkt_qty_str = min_info.get("max_mkt_qty")  # For market orders

            max_limit_qty = float(max_limit_qty_str) if max_limit_qty_str else None
            max_mkt_qty = float(max_mkt_qty_str) if max_mkt_qty_str else None

            # Choose the correct limit based on order type
            is_market_order = price is None
            if is_market_order:
                max_qty = max_mkt_qty  # Use market order limit
            else:
                max_qty = max_limit_qty  # Use limit order limit

            # If API doesn't provide max_qty, check our stored limit from previous errors
            stored_max_qty = await self._get_stored_max_qty(symbol)
            if max_qty is None and stored_max_qty:
                max_qty = stored_max_qty
                self._log.info(
                    "Using stored max_market_qty for {}: {} (learned from previous error)",
                    symbol, max_qty
                )
            elif max_qty is not None and stored_max_qty is not None:
                # Use the smaller of the two to be safe
                if stored_max_qty < max_qty:
                    self._log.info(
                        "Using stored max_qty {} instead of API max_qty {} for {}",
                        stored_max_qty, max_qty, symbol
                    )
                    max_qty = stored_max_qty

            self._log.debug(
                "Order limits for {}: qty_step={}, max_limit_qty={}, max_mkt_qty={}, stored={}, using={} ({})",
                symbol, qty_step, max_limit_qty, max_mkt_qty, stored_max_qty, max_qty,
                "MARKET" if is_market_order else "LIMIT"
            )

            # Warn if we couldn't get max_qty - orders might fail
            if max_qty is None:
                self._log.warning(
                    "Could not get {} max_qty for {} - order splitting disabled! min_info={}",
                    "market" if is_market_order else "limit", symbol, min_info
                )

            # Context for notifications and DB
            context = {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "signal_type": signal_type,
                "position_id": position_id,
                "entry_price": entry_price,
                "expected_pnl_usdt": expected_pnl_usdt,
                "expected_pnl_pct": expected_pnl_pct,
                "volume_ratio": volume_ratio,
                "price_change_pct": price_change_pct,
                "timestamp": datetime.now(timezone.utc),
            }

            # Check if we need to split the order
            if max_qty and quantity > max_qty:
                self._log.info(
                    "Order {} {}: qty={} exceeds max_qty={}, splitting...",
                    side, symbol, quantity, max_qty
                )
                chunks = self._calculate_order_chunks(quantity, max_qty, qty_step)
                result = await self._execute_batch_order(
                    symbol, side, chunks, price, qty_step, context
                )
            else:
                # Single order
                chunks = [quantity]
                result = await self._execute_batch_order(
                    symbol, side, chunks, price, qty_step, context
                )

            # If we learned a new max_qty and have failed quantity, retry with splitting
            learned_max_qty = result.get("learned_max_qty")
            failed_qty = result.get("failed_qty", 0)

            if learned_max_qty and failed_qty > 0:
                self._log.info(
                    "Retrying failed order for {} with learned max_qty={}: failed_qty={}",
                    symbol, learned_max_qty, failed_qty
                )

                # Split the failed quantity using the learned limit
                retry_chunks = self._calculate_order_chunks(failed_qty, learned_max_qty, qty_step)

                if retry_chunks:
                    # Update context for retry (don't send duplicate notifications for partial success)
                    retry_context = context.copy()
                    retry_context["is_retry"] = True

                    retry_result = await self._execute_batch_order(
                        symbol, side, retry_chunks, price, qty_step, retry_context
                    )

                    # Merge results
                    result["retry_result"] = retry_result
                    result["filled_qty"] = result.get("filled_qty", 0) + retry_result.get("filled_qty", 0)
                    result["failed_qty"] = retry_result.get("failed_qty", 0)
                    result["orders_completed"] = result.get("orders_completed", 0) + retry_result.get("orders_completed", 0)
                    result["orders_failed"] = retry_result.get("orders_failed", 0)
                    result["order_ids"].extend(retry_result.get("order_ids", []))

                    # Update success status
                    if retry_result.get("success"):
                        result["success"] = True
                        # Recalculate average price
                        total_filled = result["filled_qty"]
                        if total_filled > 0:
                            orig_value = (result.get("filled_qty", 0) - retry_result.get("filled_qty", 0)) * result.get("avg_price", 0)
                            retry_value = retry_result.get("filled_qty", 0) * retry_result.get("avg_price", 0)
                            result["avg_price"] = (orig_value + retry_value) / total_filled

                    self._log.info(
                        "Retry complete for {}: filled={}/{}, orders={}",
                        symbol, result["filled_qty"], quantity, result["orders_completed"]
                    )

                    # Send consolidated notification after retry
                    await self._send_retry_notification(result, context)

            return result

        except Exception as e:
            self._log.exception("Error placing order: {}", e)
            await self._send_error_notification(f"Ошибка ордера для {symbol}: {e}")
            return {"success": False, "error": str(e)}

    async def _execute_batch_order(
        self,
        symbol: str,
        side: str,
        chunks: List[float],
        price: Optional[float],
        qty_step: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute a batch of orders (possibly just one).

        - Saves each order to DB individually
        - Sends Telegram notification only after all complete
        - Returns aggregated result
        """
        self._stats["batches_executed"] += 1

        batch_result = BatchOrderResult(
            success=False,
            total_quantity=sum(chunks),
            first_order_time=datetime.now(timezone.utc),
        )

        # Track if we learned a new max_qty from errors
        learned_max_qty: Optional[float] = None

        price_str = f"{price:.8f}".rstrip('0').rstrip('.') if price else None

        # For market orders, get current price for calculations
        current_price = price
        if not current_price:
            current_price = await self.get_price(symbol)
            if not current_price:
                return {"success": False, "error": "Could not get current price"}

        # Track prices for averaging
        filled_prices: List[tuple] = []  # (quantity, price)

        self._log.info(
            "Executing batch: {} {} {} in {} order(s)",
            side, sum(chunks), symbol, len(chunks)
        )

        for i, chunk_qty in enumerate(chunks):
            self._stats["orders_placed"] += 1

            qty_str = truncate_to_step(chunk_qty, qty_step)

            self._log.debug(
                "Batch order {}/{}: {} {} {} qty={}",
                i + 1, len(chunks), side, symbol, qty_str, chunk_qty
            )

            try:
                # Place order via OrderQueue
                if side.lower() == "buy":
                    if price:
                        # Limit order - use token quantity
                        order_id = await self._order_queue.buy(
                            symbol=symbol,
                            amount=qty_str,
                            price=price_str,
                        )
                    else:
                        # Market order - use token quantity (baseCoin) for precise control
                        # This ensures we don't exceed maxOrderQty due to price fluctuation
                        order_id = await self._order_queue.buy(
                            symbol=symbol,
                            amount=qty_str,
                            use_quote_coin=False,  # Use baseCoin = exact token quantity
                        )
                else:
                    # Sell order
                    if len(chunks) == 1 and price_str is None:
                        # Single market sell - use "all" for convenience
                        sell_amount = "all"
                    else:
                        sell_amount = qty_str

                    order_id = await self._order_queue.sell(
                        symbol=symbol,
                        amount=sell_amount,
                        price=price_str,
                    )

                # Wait for order completion
                completed_order = await self._order_queue.wait(order_id, timeout=60)

                if completed_order and completed_order.status == OrderStatus.COMPLETED:
                    self._stats["orders_completed"] += 1
                    batch_result.orders_completed += 1
                    batch_result.filled_quantity += chunk_qty
                    batch_result.order_ids.append(
                        completed_order.result.order_id if completed_order.result else order_id
                    )

                    # Track price for averaging
                    fill_price = float(completed_order.price) if completed_order.price else current_price
                    filled_prices.append((chunk_qty, fill_price))

                    # Save order to DB
                    await self._save_order_to_db(
                        completed_order, context, success=True,
                        position_id=context.get("position_id")
                    )

                    self._log.info(
                        "Batch order {}/{} completed: {} {} {}",
                        i + 1, len(chunks), side, qty_str, symbol
                    )
                else:
                    self._stats["orders_failed"] += 1
                    batch_result.orders_failed += 1
                    batch_result.failed_quantity += chunk_qty

                    error_msg = "Unknown error"
                    if completed_order and completed_order.result:
                        error_msg = completed_order.result.message
                    batch_result.errors.append(f"Order {i+1}: {error_msg}")

                    # Try to parse max_qty from error message and save to DB
                    parsed_max_qty = parse_max_qty_from_error(error_msg)
                    if parsed_max_qty:
                        learned_max_qty = parsed_max_qty
                        self._log.info(
                            "Learned max_market_qty from error for {}: {}",
                            symbol, parsed_max_qty
                        )
                        await self._save_max_qty(symbol, parsed_max_qty)

                    # Save failed order to DB
                    if completed_order:
                        await self._save_order_to_db(
                            completed_order, context, success=False,
                            position_id=context.get("position_id")
                        )

                    self._log.error(
                        "Batch order {}/{} failed: {} {} {} - {}",
                        i + 1, len(chunks), side, qty_str, symbol, error_msg
                    )

            except Exception as e:
                self._stats["orders_failed"] += 1
                batch_result.orders_failed += 1
                batch_result.failed_quantity += chunk_qty
                batch_result.errors.append(f"Order {i+1}: {str(e)}")
                self._log.exception("Batch order {}/{} exception: {}", i + 1, len(chunks), e)

                # Try to parse max_qty from exception message too
                parsed_max_qty = parse_max_qty_from_error(str(e))
                if parsed_max_qty:
                    learned_max_qty = parsed_max_qty
                    self._log.info(
                        "Learned max_market_qty from exception for {}: {}",
                        symbol, parsed_max_qty
                    )
                    await self._save_max_qty(symbol, parsed_max_qty)

        # Calculate average fill price
        if filled_prices:
            total_value = sum(qty * px for qty, px in filled_prices)
            total_qty = sum(qty for qty, _ in filled_prices)
            batch_result.avg_price = total_value / total_qty if total_qty > 0 else 0

        # Determine overall success
        batch_result.success = batch_result.orders_completed > 0

        # Send consolidated Telegram notification
        # Skip if we learned a max_qty - caller will retry and send final notification
        if not learned_max_qty:
            await self._send_batch_notification(batch_result, context)

        # Log summary
        self._log.info(
            "Batch complete: {} {} {} | filled={}/{} | avg_price={:.6f} | orders={}/{}",
            side, batch_result.filled_quantity, symbol,
            batch_result.filled_quantity, batch_result.total_quantity,
            batch_result.avg_price,
            batch_result.orders_completed, len(chunks)
        )

        return {
            "success": batch_result.success,
            "order_id": batch_result.order_ids[0] if batch_result.order_ids else None,
            "order_ids": batch_result.order_ids,
            "filled_qty": batch_result.filled_quantity,
            "failed_qty": batch_result.failed_quantity,
            "avg_price": batch_result.avg_price,
            "orders_completed": batch_result.orders_completed,
            "orders_failed": batch_result.orders_failed,
            "errors": batch_result.errors,
            "partial": batch_result.orders_failed > 0 and batch_result.orders_completed > 0,
            "learned_max_qty": learned_max_qty,  # If set, we learned a new limit from errors
        }

    async def _save_order_to_db(
        self,
        order: QueuedOrder,
        context: Dict[str, Any],
        success: bool,
        position_id: Optional[int] = None,
    ) -> Optional[int]:
        """Save order to database. Returns order ID."""
        try:
            from db.models import OrderSide as DBOrderSide, OrderStatus as DBOrderStatus

            async with self._session_factory() as session:
                db_order = self._order_model(
                    bybit_order_id=order.result.order_id if order.result else None,
                    symbol=order.symbol,
                    side=DBOrderSide.BUY if order.side.value == "Buy" else DBOrderSide.SELL,
                    status=DBOrderStatus.FILLED if success else DBOrderStatus.REJECTED,
                    price=float(order.price) if order.price else None,
                    quantity=float(order.qty),
                    filled_quantity=float(order.qty) if success else 0.0,
                    avg_fill_price=float(order.price) if order.price and success else None,
                    position_id=position_id,
                )
                session.add(db_order)
                await session.commit()
                await session.refresh(db_order)
                return db_order.id

        except Exception as e:
            self._log.error("Failed to save order to database: {}", e)
            return None

    async def _send_batch_notification(
        self,
        batch: BatchOrderResult,
        context: Dict[str, Any],
    ) -> None:
        """Send consolidated Telegram notification after batch completes."""
        if not self._telegram_queue:
            return

        # Skip notifications for retry batches - caller will send consolidated notification
        if context.get("is_retry"):
            return

        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            symbol = context.get("symbol", "")
            side = context.get("side", "")
            coin = symbol.replace("USDT", "").replace("USDC", "")

            # All orders successful
            if batch.orders_failed == 0 and batch.orders_completed > 0:
                price_formatted = format_price(batch.avg_price)

                if side.lower() == "buy":
                    # Entry notification
                    position_size = batch.filled_quantity * batch.avg_price

                    if batch.orders_completed > 1:
                        # Multiple orders
                        msg = (
                            f"🟢 <b>Вход в позицию</b>\n\n"
                            f"Монета: <b>{coin}</b>\n"
                            f"Цена: {price_formatted} (средняя)\n"
                            f"Количество: {batch.filled_quantity:.4f}\n"
                            f"Размер: ${position_size:.2f}\n"
                            f"Ордеров: {batch.orders_completed}\n"
                            f"Время: {current_time}"
                        )
                    else:
                        msg = settings.telegram_entry_template.format(
                            symbol=coin,
                            price=price_formatted,
                            position_size=f"{position_size:.2f}",
                            time=current_time,
                        )
                else:
                    # Exit notification
                    exit_value = batch.filled_quantity * batch.avg_price
                    pnl_pct = context.get("expected_pnl_pct") or 0
                    profit_sign = "+" if pnl_pct >= 0 else ""

                    if batch.orders_completed > 1:
                        msg = (
                            f"🔴 <b>Выход из позиции</b>\n\n"
                            f"Монета: <b>{coin}</b>\n"
                            f"Цена: {price_formatted} (средняя)\n"
                            f"Количество: {batch.filled_quantity:.4f}\n"
                            f"Сумма: ${exit_value:.2f}\n"
                            f"Результат: {profit_sign}{pnl_pct:.1f}%\n"
                            f"Ордеров: {batch.orders_completed}\n"
                            f"Время: {current_time}"
                        )
                    else:
                        msg = settings.telegram_exit_template.format(
                            symbol=coin,
                            exit_price=price_formatted,
                            exit_value=f"{exit_value:.2f}",
                            profit_sign=profit_sign,
                            profit_pct=f"{pnl_pct:.1f}",
                            time=current_time,
                        )

            # Partial success
            elif batch.orders_completed > 0 and batch.orders_failed > 0:
                action = "Покупка" if side.lower() == "buy" else "Продажа"
                filled_value = batch.filled_quantity * batch.avg_price
                failed_value = batch.failed_quantity * batch.avg_price
                price_formatted = format_price(batch.avg_price)

                msg = (
                    f"⚠️ <b>{action} выполнена частично</b>\n\n"
                    f"Монета: <b>{coin}</b>\n"
                    f"Выполнено: {batch.filled_quantity:.4f} (${filled_value:.2f})\n"
                    f"Не выполнено: {batch.failed_quantity:.4f} (${failed_value:.2f})\n"
                    f"Средняя цена: {price_formatted}\n"
                    f"Ордеров: {batch.orders_completed}/{batch.orders_completed + batch.orders_failed}\n"
                    f"Ошибки: {'; '.join(batch.errors[:2])}\n"
                    f"Время: {current_time}"
                )

            # All failed
            else:
                action = "Покупка" if side.lower() == "buy" else "Продажа"
                msg = (
                    f"❌ <b>{action} не выполнена</b>\n\n"
                    f"Монета: <b>{coin}</b>\n"
                    f"Количество: {batch.total_quantity:.4f}\n"
                    f"Ордеров: 0/{batch.orders_failed}\n"
                    f"Ошибки: {'; '.join(batch.errors[:3])}\n"
                    f"Время: {current_time}"
                )

            # Broadcast trade notifications to all users (not errors)
            should_broadcast = batch.orders_completed > 0

            # For personalized broadcasts, include trade data
            notification_data = {
                "text": msg,
                "parse_mode": "HTML",
                "broadcast": should_broadcast,
            }

            if should_broadcast:
                # Add trade data for personalized messages
                notification_data["trade_data"] = {
                    "type": "entry" if side.lower() == "buy" else "exit",
                    "coin": coin,
                    "price": batch.avg_price,
                    "quantity": batch.filled_quantity,
                    "position_size_usdt": batch.filled_quantity * batch.avg_price,
                    "pnl_pct": context.get("expected_pnl_pct") or 0,
                    "orders_count": batch.orders_completed,
                    "time": current_time,
                }

            self._telegram_queue.put_nowait(notification_data)

        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, notification dropped")
        except Exception as e:
            self._log.error("Failed to send batch notification: {}", e)

    async def _send_error_notification(self, error_msg: str) -> None:
        """Send error notification to Telegram."""
        if not self._telegram_queue:
            return

        try:
            msg = f"<b>⚠️ Ошибка торговли</b>\n\n{error_msg}"
            self._telegram_queue.put_nowait({"text": msg, "parse_mode": "HTML"})
        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, error notification dropped")
        except Exception as e:
            self._log.error("Failed to send error notification: {}", e)

    async def _send_retry_notification(
        self,
        result: Dict[str, Any],
        context: Dict[str, Any],
    ) -> None:
        """Send notification after retry with learned max_qty."""
        if not self._telegram_queue:
            return

        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            symbol = context.get("symbol", "")
            side = context.get("side", "")
            coin = symbol.replace("USDT", "").replace("USDC", "")

            filled_qty = result.get("filled_qty", 0)
            failed_qty = result.get("failed_qty", 0)
            avg_price = result.get("avg_price", 0)
            orders_completed = result.get("orders_completed", 0)
            orders_failed = result.get("orders_failed", 0)
            learned_max_qty = result.get("learned_max_qty", 0)

            price_formatted = format_price(avg_price) if avg_price else "N/A"

            if result.get("success") and filled_qty > 0:
                # Successful after retry
                position_size = filled_qty * avg_price

                if side.lower() == "buy":
                    msg = (
                        f"🟢 <b>Вход в позицию</b> (после повтора)\n\n"
                        f"Монета: <b>{coin}</b>\n"
                        f"Цена: {price_formatted}\n"
                        f"Количество: {filled_qty:.4f}\n"
                        f"Размер: ${position_size:.2f}\n"
                        f"Ордеров: {orders_completed}\n"
                        f"Лимит: {learned_max_qty:.0f}\n"
                        f"Время: {current_time}"
                    )
                else:
                    pnl_pct = context.get("expected_pnl_pct") or 0
                    profit_sign = "+" if pnl_pct >= 0 else ""
                    exit_value = filled_qty * avg_price
                    msg = (
                        f"🔴 <b>Выход из позиции</b> (после повтора)\n\n"
                        f"Монета: <b>{coin}</b>\n"
                        f"Цена: {price_formatted}\n"
                        f"Сумма: ${exit_value:.2f}\n"
                        f"Результат: {profit_sign}{pnl_pct:.1f}%\n"
                        f"Ордеров: {orders_completed}\n"
                        f"Время: {current_time}"
                    )

                should_broadcast = True
            elif filled_qty > 0:
                # Partial success
                action = "Покупка" if side.lower() == "buy" else "Продажа"
                filled_value = filled_qty * avg_price
                msg = (
                    f"⚠️ <b>{action} выполнена частично</b> (после повтора)\n\n"
                    f"Монета: <b>{coin}</b>\n"
                    f"Выполнено: {filled_qty:.4f} (${filled_value:.2f})\n"
                    f"Не выполнено: {failed_qty:.4f}\n"
                    f"Ордеров: {orders_completed}/{orders_completed + orders_failed}\n"
                    f"Лимит: {learned_max_qty:.0f}\n"
                    f"Время: {current_time}"
                )
                should_broadcast = True
            else:
                # Complete failure even after retry
                action = "Покупка" if side.lower() == "buy" else "Продажа"
                total_qty = context.get("quantity", 0)
                errors = result.get("errors", [])
                msg = (
                    f"❌ <b>{action} не выполнена</b> (после повтора)\n\n"
                    f"Монета: <b>{coin}</b>\n"
                    f"Количество: {total_qty:.4f}\n"
                    f"Лимит: {learned_max_qty:.0f}\n"
                    f"Ошибки: {'; '.join(errors[:2])}\n"
                    f"Время: {current_time}"
                )
                should_broadcast = False

            # For personalized broadcasts, include trade data
            notification_data = {
                "text": msg,
                "parse_mode": "HTML",
                "broadcast": should_broadcast,
            }

            if should_broadcast and filled_qty > 0:
                notification_data["trade_data"] = {
                    "type": "entry" if side.lower() == "buy" else "exit",
                    "coin": coin,
                    "price": avg_price,
                    "quantity": filled_qty,
                    "position_size_usdt": filled_qty * avg_price,
                    "pnl_pct": context.get("expected_pnl_pct") or 0,
                    "orders_count": orders_completed,
                    "time": current_time,
                    "is_retry": True,
                }

            self._telegram_queue.put_nowait(notification_data)

        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, retry notification dropped")
        except Exception as e:
            self._log.error("Failed to send retry notification: {}", e)

    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        queue_stats = self._order_queue.stats
        return {
            **self._stats,
            "queue_size": queue_stats.get("queue", 0),
            "pending_orders": queue_stats.get("pending", 0),
            "running": self._running,
        }


def create_place_order_function(
    executor: RealOrderExecutorService,
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """
    Create a place_order function for ExecutionEngine.

    Returns an async function with signature: (symbol, side, quantity, price, **context) -> Dict
    Supports additional context parameters for notifications.
    """
    async def place_order(
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        **context
    ) -> Dict[str, Any]:
        return await executor.place_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            signal_type=context.get("signal_type", ""),
            position_id=context.get("position_id"),
            entry_price=context.get("entry_price"),
            expected_pnl_usdt=context.get("expected_pnl_usdt"),
            expected_pnl_pct=context.get("expected_pnl_pct"),
            volume_ratio=context.get("volume_ratio"),
            price_change_pct=context.get("price_change_pct"),
        )

    return place_order


def create_get_balance_function(
    executor: RealOrderExecutorService,
) -> Callable[[], Awaitable[float]]:
    """
    Create a get_balance function for ExecutionEngine.

    Returns an async function that returns USDT balance.
    """
    async def get_balance() -> float:
        return await executor.get_balance("USDT")

    return get_balance