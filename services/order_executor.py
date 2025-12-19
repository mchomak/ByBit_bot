"""
Order Executor Service - Stub Implementation.

Consumes OrderRequest events from a queue and logs them.
This is a stub implementation that does not execute real orders.

When ready for live trading, this service will be extended to:
- Validate orders against account balance
- Place orders via Bybit API
- Track order status and fills
- Send Telegram notifications
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from loguru import logger


class OrderSide(str, Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Order type."""
    MARKET = "market"
    LIMIT = "limit"


@dataclass
class OrderRequest:
    """
    Order request to be executed.

    Sent from execution engine to order executor.
    """
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float]  # None for market orders
    timestamp: datetime

    # Context for logging/notifications
    signal_type: str  # "entry" or "exit"
    volume_ratio: Optional[float] = None
    price_change_pct: Optional[float] = None
    ma14_value: Optional[float] = None
    position_id: Optional[int] = None

    # P&L for exit orders
    entry_price: Optional[float] = None
    expected_pnl_usdt: Optional[float] = None
    expected_pnl_pct: Optional[float] = None


@dataclass
class OrderResult:
    """Result of order execution."""
    success: bool
    order_id: Optional[str] = None
    filled_qty: Optional[float] = None
    filled_price: Optional[float] = None
    error: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class OrderExecutorService:
    """
    Order executor that consumes order requests and logs them.

    This is a STUB implementation - no real orders are placed.
    All orders are logged with detailed information for monitoring.
    """

    def __init__(
        self,
        order_queue: asyncio.Queue,
        telegram_queue: Optional[asyncio.Queue] = None,
        *,
        dry_run: bool = True,
    ) -> None:
        """
        Initialize the order executor.

        Args:
            order_queue: Queue to consume OrderRequest events from
            telegram_queue: Optional queue for Telegram notifications
            dry_run: If True, only log orders without executing
        """
        self._queue = order_queue
        self._telegram_queue = telegram_queue
        self._dry_run = dry_run

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

        # Trading log with dedicated stream
        self._log = logger.bind(stream="trading", component="OrderExecutor")

        # Statistics
        self._orders_received = 0
        self._orders_logged = 0

    async def start(self) -> None:
        """Start the order executor."""
        if self._task is not None:
            self._log.warning("Order executor already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="order-executor")

        mode = "DRY-RUN" if self._dry_run else "LIVE"
        self._log.info("Order executor started in {} mode", mode)

    async def stop(self) -> None:
        """Stop the order executor gracefully."""
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._log.info(
            "Order executor stopped | orders_received={} orders_logged={}",
            self._orders_received,
            self._orders_logged
        )

    async def _run(self) -> None:
        """Main consumer loop."""
        while not self._stop_event.is_set():
            try:
                try:
                    order: OrderRequest = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                self._orders_received += 1
                await self._process_order(order)
                self._queue.task_done()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error processing order: {}", e)

    async def _process_order(self, order: OrderRequest) -> OrderResult:
        """
        Process an order request.

        In dry-run mode, logs the order without executing.
        """
        # Log detailed order information
        if order.side == OrderSide.BUY:
            await self._log_buy_order(order)
        else:
            await self._log_sell_order(order)

        self._orders_logged += 1

        # Send Telegram notification
        await self._send_notification(order)

        # Return stub result
        return OrderResult(
            success=True,
            order_id=f"STUB-{datetime.now().timestamp():.0f}",
            filled_qty=order.quantity,
            filled_price=order.price,
        )

    async def _log_buy_order(self, order: OrderRequest) -> None:
        """Log a buy order with full context."""
        self._log.info(
            "=" * 60
        )
        self._log.info(
            "ORDER REQUEST: BUY {}",
            order.symbol
        )
        self._log.info("-" * 60)
        self._log.info("  Type:           {} ({})", order.signal_type.upper(), order.order_type.value)
        self._log.info("  Quantity:       {:.8f}", order.quantity)
        self._log.info("  Price:          {:.8f}", order.price or 0)
        self._log.info("  Value USDT:     {:.2f}", (order.quantity * (order.price or 0)))
        self._log.info("  Timestamp:      {}", order.timestamp.isoformat())

        if order.volume_ratio is not None:
            self._log.info("  Volume Ratio:   {:.2f}x (vs 5d max)", order.volume_ratio)
        if order.price_change_pct is not None:
            self._log.info("  Price Change:   {:.2f}%", order.price_change_pct)

        self._log.info("-" * 60)
        self._log.info("  [DRY-RUN] Order NOT executed - stub mode")
        self._log.info("=" * 60)

    async def _log_sell_order(self, order: OrderRequest) -> None:
        """Log a sell order with full context."""
        self._log.info(
            "=" * 60
        )
        self._log.info(
            "ORDER REQUEST: SELL {}",
            order.symbol
        )
        self._log.info("-" * 60)
        self._log.info("  Type:           {} ({})", order.signal_type.upper(), order.order_type.value)
        self._log.info("  Quantity:       {:.8f}", order.quantity)
        self._log.info("  Price:          {:.8f}", order.price or 0)
        self._log.info("  Value USDT:     {:.2f}", (order.quantity * (order.price or 0)))
        self._log.info("  Timestamp:      {}", order.timestamp.isoformat())

        if order.entry_price is not None:
            self._log.info("  Entry Price:    {:.8f}", order.entry_price)
        if order.ma14_value is not None:
            self._log.info("  MA14 Value:     {:.8f}", order.ma14_value)
        if order.expected_pnl_usdt is not None:
            emoji = "+" if order.expected_pnl_usdt >= 0 else ""
            self._log.info(
                "  Expected P&L:   {}{:.2f} USDT ({}{:.2f}%)",
                emoji, order.expected_pnl_usdt,
                emoji, order.expected_pnl_pct or 0
            )

        self._log.info("-" * 60)
        self._log.info("  [DRY-RUN] Order NOT executed - stub mode")
        self._log.info("=" * 60)

    async def _send_notification(self, order: OrderRequest) -> None:
        """Send Telegram notification for the order."""
        if self._telegram_queue is None:
            return

        try:
            if order.side == OrderSide.BUY:
                msg = (
                    f"<b>🟢 BUY Signal</b> [DRY-RUN]\n\n"
                    f"<b>Symbol:</b> {order.symbol}\n"
                    f"<b>Price:</b> {order.price:.8f}\n"
                    f"<b>Qty:</b> {order.quantity:.8f}\n"
                    f"<b>Value:</b> {(order.quantity * (order.price or 0)):.2f} USDT\n"
                )
                if order.volume_ratio:
                    msg += f"<b>Volume:</b> {order.volume_ratio:.2f}x (vs 5d max)\n"
                if order.price_change_pct:
                    msg += f"<b>Price Δ:</b> {order.price_change_pct:.2f}%\n"
            else:
                emoji = "🟢" if (order.expected_pnl_usdt or 0) >= 0 else "🔴"
                pnl_emoji = "+" if (order.expected_pnl_usdt or 0) >= 0 else ""
                msg = (
                    f"<b>{emoji} SELL Signal</b> [DRY-RUN]\n\n"
                    f"<b>Symbol:</b> {order.symbol}\n"
                    f"<b>Exit Price:</b> {order.price:.8f}\n"
                    f"<b>Entry Price:</b> {order.entry_price:.8f}\n"
                    f"<b>MA14:</b> {order.ma14_value:.8f}\n"
                    f"<b>P&L:</b> {pnl_emoji}{order.expected_pnl_usdt:.2f} USDT "
                    f"({pnl_emoji}{order.expected_pnl_pct:.2f}%)\n"
                )

            self._telegram_queue.put_nowait({"text": msg, "parse_mode": "HTML"})
        except asyncio.QueueFull:
            self._log.warning("Telegram queue full, notification dropped")
        except Exception as e:
            self._log.warning("Failed to send notification: {}", e)

    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        return {
            "orders_received": self._orders_received,
            "orders_logged": self._orders_logged,
            "dry_run": self._dry_run,
        }