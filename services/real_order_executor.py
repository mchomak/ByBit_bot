"""
Real Order Executor Service.

Integrates the OrderQueue from trade_client.py with the trading pipeline.
Executes real orders on Bybit, logs to database, and sends Telegram notifications.

Supports order splitting when quantity exceeds exchange maxOrderQty limit.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Awaitable

from loguru import logger
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from trade.trade_client import OrderQueue, QueuedOrder, OrderStatus, Category, truncate_to_step
from config.config import settings


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
            max_qty_str = min_info.get("max_qty")
            max_qty = float(max_qty_str) if max_qty_str else None

            self._log.debug(
                "Order limits for {}: qty_step={}, max_qty={}, min_info={}",
                symbol, qty_step, max_qty, min_info
            )

            # Warn if we couldn't get max_qty - orders might fail
            if max_qty is None:
                self._log.warning(
                    "Could not get max_qty for {} - order splitting disabled! min_info={}",
                    symbol, min_info
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
                return await self._execute_batch_order(
                    symbol, side, chunks, price, qty_step, context
                )
            else:
                # Single order
                chunks = [quantity]
                return await self._execute_batch_order(
                    symbol, side, chunks, price, qty_step, context
                )

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

        # Calculate average fill price
        if filled_prices:
            total_value = sum(qty * px for qty, px in filled_prices)
            total_qty = sum(qty for qty, _ in filled_prices)
            batch_result.avg_price = total_value / total_qty if total_qty > 0 else 0

        # Determine overall success
        batch_result.success = batch_result.orders_completed > 0

        # Send consolidated Telegram notification
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
            self._telegram_queue.put_nowait({
                "text": msg,
                "parse_mode": "HTML",
                "broadcast": should_broadcast
            })

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