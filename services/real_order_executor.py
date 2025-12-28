"""
Real Order Executor Service.

Integrates the OrderQueue from trade_client.py with the trading pipeline.
Executes real orders on Bybit, logs to database, and sends Telegram notifications.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Awaitable

from loguru import logger
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from trade.trade_client import OrderQueue, QueuedOrder, OrderStatus, Category, truncate_to_step
from config.config import settings


class RealOrderExecutorService:
    """
    Real order executor that uses OrderQueue from trade_client.py.

    Features:
    - Executes real orders on Bybit via OrderQueue
    - Sends Telegram notifications on order completion/failure
    - Logs orders to database
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

        # Set up callbacks
        self._order_queue.on_completed = self._on_order_completed
        self._order_queue.on_failed = self._on_order_failed

        # Pending order requests awaiting completion
        self._pending_requests: Dict[str, Dict[str, Any]] = {}

        self._log = logger.bind(stream="trading", component="RealOrderExecutor")
        self._running = False

        # Statistics
        self._stats = {
            "orders_placed": 0,
            "orders_completed": 0,
            "orders_failed": 0,
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
            "Real order executor stopped | placed={} completed={} failed={}",
            self._stats["orders_placed"],
            self._stats["orders_completed"],
            self._stats["orders_failed"],
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
        Place an order on Bybit.

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
            self._stats["orders_placed"] += 1

            # Get qty_step and max_qty for proper truncation and limits
            min_info = await self._order_queue.get_min_order(symbol, self._category)
            qty_step = min_info.get("qty_step", "0.000001")
            max_qty_str = min_info.get("max_qty")
            max_qty = float(max_qty_str) if max_qty_str else None

            # Limit quantity to maxOrderQty if it exceeds the limit
            original_qty = quantity
            if max_qty and quantity > max_qty:
                quantity = max_qty
                self._log.warning(
                    "Order {} {}: qty={} exceeds max_qty={}, limiting to max",
                    side, symbol, original_qty, max_qty
                )

            # Convert quantity to string with proper precision
            qty_str = truncate_to_step(quantity, qty_step)
            price_str = f"{price:.8f}".rstrip('0').rstrip('.') if price else None

            self._log.debug(
                "Order {} {}: qty={} -> truncated={} (step={}, max={})",
                side, symbol, original_qty, qty_str, qty_step, max_qty
            )

            # Prepare context for callbacks
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

            # Place order via OrderQueue
            if side.lower() == "buy":
                # For market buy, use USDT amount (quantity * price)
                if price:
                    order_id = await self._order_queue.buy(
                        symbol=symbol,
                        amount=qty_str,
                        price=price_str,
                    )
                else:
                    # Market order - calculate USDT amount
                    current_price = await self.get_price(symbol)
                    if current_price:
                        usdt_amount = quantity * current_price
                        order_id = await self._order_queue.buy(
                            symbol=symbol,
                            amount=f"{usdt_amount:.2f}",
                        )
                    else:
                        return {"success": False, "error": "Could not get current price"}
            else:
                # Use "all" for market sell orders to avoid insufficient balance errors
                # (trading fees reduce actual balance vs stored entry_amount)
                sell_amount = "all" if price_str is None else qty_str
                order_id = await self._order_queue.sell(
                    symbol=symbol,
                    amount=sell_amount,
                    price=price_str,
                )

            # Store context for callback
            self._pending_requests[order_id] = context

            # Wait for order completion
            completed_order = await self._order_queue.wait(order_id, timeout=60)

            if completed_order and completed_order.status == OrderStatus.COMPLETED:
                return {
                    "success": True,
                    "order_id": completed_order.result.order_id if completed_order.result else order_id,
                    "filled_qty": quantity,
                    "filled_price": price,
                }
            elif completed_order and completed_order.status == OrderStatus.FAILED:
                error_msg = completed_order.result.message if completed_order.result else "Unknown error"
                return {"success": False, "error": error_msg}
            else:
                return {"success": False, "error": "Order timeout or unknown status"}

        except Exception as e:
            self._log.exception("Error placing order: {}", e)
            await self._send_error_notification(f"Ошибка ордера для {symbol}: {e}")
            return {"success": False, "error": str(e)}

    async def _on_order_completed(self, order: QueuedOrder) -> None:
        """Callback when order is completed."""
        self._stats["orders_completed"] += 1

        context = self._pending_requests.pop(order.id, {})

        # Log to database
        await self._save_order_to_db(order, context, success=True)

        # Send Telegram notification
        await self._send_order_notification(order, context, success=True)

        self._log.info(
            "Order completed: {} {} {} @ {}",
            order.side.value,
            order.qty,
            order.symbol,
            order.result.order_id if order.result else "N/A",
        )

    async def _on_order_failed(self, order: QueuedOrder) -> None:
        """Callback when order fails."""
        self._stats["orders_failed"] += 1

        context = self._pending_requests.pop(order.id, {})

        # Log to database
        await self._save_order_to_db(order, context, success=False)

        # Send error notification
        await self._send_order_notification(order, context, success=False)

        error_msg = order.result.message if order.result else "Unknown error"
        self._log.error(
            "Order failed: {} {} {} - {}",
            order.side.value,
            order.qty,
            order.symbol,
            error_msg,
        )

    async def _save_order_to_db(
        self,
        order: QueuedOrder,
        context: Dict[str, Any],
        success: bool,
    ) -> None:
        """Save order to database."""
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
                    position_id=context.get("position_id"),
                )
                session.add(db_order)
                await session.commit()

        except Exception as e:
            self._log.error("Failed to save order to database: {}", e)

    async def _send_order_notification(
        self,
        order: QueuedOrder,
        context: Dict[str, Any],
        success: bool,
    ) -> None:
        """Send Telegram notification for order using templates from config."""
        if not self._telegram_queue:
            return

        try:
            from datetime import datetime

            current_time = datetime.now().strftime("%H:%M:%S")

            if success:
                if order.side.value == "Buy":
                    # Entry signal template
                    coin = order.symbol.replace("USDT", "").replace("USDC", "")

                    # For market orders, order.price is None
                    # Get price from context or try to estimate
                    price = None
                    if order.price:
                        price = float(order.price)
                    elif context.get("price"):
                        price = float(context["price"])

                    # For market buy with quoteCoin, order.qty is USDT amount
                    # context["quantity"] has the token quantity
                    token_qty = context.get("quantity", 0)
                    usdt_amount = float(order.qty) if order.market_unit == "quoteCoin" else 0

                    # Calculate position size
                    if usdt_amount > 0:
                        position_size = usdt_amount
                    elif price and token_qty:
                        position_size = token_qty * price
                    else:
                        position_size = 0

                    # Get price for display (use approximate if market order)
                    display_price = price if price else (position_size / token_qty if token_qty > 0 else 0)

                    msg = settings.telegram_entry_template.format(
                        symbol=coin,
                        price=f"{display_price:.6f}" if display_price else "MARKET",
                        position_size=f"{position_size:.2f}",
                        time=current_time,
                    )
                else:
                    # Exit signal template
                    coin = order.symbol.replace("USDT", "").replace("USDC", "")

                    # For market sell, order.price is None
                    exit_price = None
                    if order.price:
                        exit_price = float(order.price)
                    elif context.get("price"):
                        exit_price = float(context["price"])
                    elif context.get("entry_price"):
                        # Use entry price as estimate for display
                        exit_price = float(context["entry_price"])

                    quantity = float(order.qty) if order.qty else 0
                    exit_value = quantity * exit_price if exit_price and quantity else 0
                    pnl_pct = context.get("expected_pnl_pct") or 0
                    profit_sign = "+" if pnl_pct >= 0 else ""

                    msg = settings.telegram_exit_template.format(
                        symbol=coin,
                        exit_price=f"{exit_price:.6f}" if exit_price else "MARKET",
                        exit_value=f"{exit_value:.2f}",
                        profit_sign=profit_sign,
                        profit_pct=f"{pnl_pct:.1f}",
                        time=current_time,
                    )
            else:
                error_msg = order.result.message if order.result else "Неизвестная ошибка"
                msg = (
                    f"❌ Ордер не выполнен\n\n"
                    f"Символ: {order.symbol}\n"
                    f"Сторона: {order.side.value}\n"
                    f"Количество: {order.qty}\n"
                    f"Ошибка: {error_msg}\n"
                    f"Время: {current_time}"
                )

            self._telegram_queue.put_nowait({"text": msg, "parse_mode": "HTML"})

        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, notification dropped")
        except Exception as e:
            self._log.error("Failed to send order notification: {}", e)

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