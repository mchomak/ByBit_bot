"""TradingEngine: Order execution and position management.

Responsibilities:
1. Consume trade signals from TradeSignalQueue
2. Execute market orders via Bybit REST API
3. Track order status via Bybit private WebSocket
4. Manage positions (open/close)
5. Record orders and trades in database
6. Send notifications for important events
"""

from __future__ import annotations

import asyncio
import hmac
import hashlib
import time
import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import httpx
import websockets
from loguru import logger

from ...config.config import Settings
from ..db.repository import Repository
from ..db.models import Order, Position, OrderSide, OrderStatus, PositionStatus
from ..queues.event_bus import (
    TradeSignalQueue,
    NotificationQueue,
    TradeSignalEvent,
    EventType,
)


class BybitClient:
    """
    Bybit API client for spot trading.

    Handles authentication and order execution.
    """

    # API endpoints
    BASE_URL = "https://api.bybit.com"
    BASE_URL_TESTNET = "https://api-testnet.bybit.com"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = self.BASE_URL_TESTNET if testnet else self.BASE_URL
        self.log = logger.bind(component="BybitClient")

    def _generate_signature(self, params: Dict[str, Any], timestamp: int) -> str:
        """
        Generate HMAC SHA256 signature for Bybit API.

        Args:
            params: Request parameters
            timestamp: Unix timestamp in milliseconds

        Returns:
            Signature string
        """
        param_str = str(timestamp) + self.api_key + "5000"  # recv_window
        for key in sorted(params.keys()):
            param_str += f"{key}={params[key]}"

        return hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _get_auth_headers(self, params: Dict[str, Any]) -> Dict[str, str]:
        """Get authentication headers for signed requests."""
        timestamp = int(time.time() * 1000)
        signature = self._generate_signature(params, timestamp)

        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type": "application/json",
        }

    async def place_market_order(
        self,
        symbol: str,
        side: str,  # "Buy" or "Sell"
        qty: float,
    ) -> Optional[Dict[str, Any]]:
        """
        Place a market order.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            side: "Buy" or "Sell"
            qty: Quantity in base asset

        Returns:
            Order response or None if failed
        """
        # TODO: Implement actual API call
        # This is a stub that should call Bybit's order endpoint
        #
        # params = {
        #     "category": "spot",
        #     "symbol": symbol,
        #     "side": side,
        #     "orderType": "Market",
        #     "qty": str(qty),
        # }
        #
        # async with httpx.AsyncClient() as client:
        #     response = await client.post(
        #         f"{self.base_url}/v5/order/create",
        #         json=params,
        #         headers=self._get_auth_headers(params),
        #     )
        #     data = response.json()
        #     if data.get("retCode") == 0:
        #         return data.get("result")
        #     else:
        #         self.log.error(f"Order failed: {data}")
        #         return None

        self.log.debug(f"Placing market order (stub): {side} {qty} {symbol}")
        return None

    async def get_order_status(self, order_id: str, symbol: str) -> Optional[Dict]:
        """
        Get order status.

        Args:
            order_id: Bybit order ID
            symbol: Trading pair

        Returns:
            Order data or None
        """
        # TODO: Implement actual API call
        self.log.debug(f"Getting order status (stub): {order_id}")
        return None

    async def get_wallet_balance(self, coin: str = "USDT") -> float:
        """
        Get wallet balance for a coin.

        Args:
            coin: Coin symbol (default USDT)

        Returns:
            Available balance
        """
        # TODO: Implement actual API call
        self.log.debug(f"Getting wallet balance (stub): {coin}")
        return 0.0

    async def get_position_info(self, symbol: str) -> Optional[Dict]:
        """
        Get current position for a symbol (for spot, this is holdings).

        Args:
            symbol: Trading pair

        Returns:
            Position info or None
        """
        # TODO: Implement actual API call
        self.log.debug(f"Getting position info (stub): {symbol}")
        return None


class TradingEngine:
    """
    Trading engine for executing trades and managing positions.

    Consumes signals from TradeSignalQueue and executes via Bybit API.
    """

    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        trade_signal_queue: TradeSignalQueue,
        notification_queue: NotificationQueue,
        on_position_opened: Optional[Callable[[Position], None]] = None,
        on_position_closed: Optional[Callable[[str], None]] = None,
    ):
        """
        Initialize TradingEngine.

        Args:
            settings: Application settings
            repository: Database repository
            trade_signal_queue: Queue to receive trade signals from
            notification_queue: Queue to send notifications to
            on_position_opened: Callback when position opens
            on_position_closed: Callback when position closes
        """
        self.settings = settings
        self.repository = repository
        self.signal_queue = trade_signal_queue
        self.notification_queue = notification_queue
        self.on_position_opened = on_position_opened
        self.on_position_closed = on_position_closed
        self.log = logger.bind(component="TradingEngine")

        # Bybit client
        self.client = BybitClient(
            api_key=settings.bybit_api_key,
            api_secret=settings.bybit_api_secret,
            testnet=settings.bybit_testnet,
        )

        # Pending orders being tracked
        self._pending_orders: Dict[str, Order] = {}  # order_id -> Order

        # Background tasks
        self._signal_consumer_task: Optional[asyncio.Task] = None
        self._order_tracker_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the trading engine."""
        self.log.info("Starting TradingEngine")
        self._running = True

        # Start signal consumer
        self._signal_consumer_task = asyncio.create_task(self._consume_signals())

        # Start order status tracker
        self._order_tracker_task = asyncio.create_task(self._track_orders())

        self.log.info("TradingEngine started")

    async def stop(self) -> None:
        """Stop the trading engine."""
        self._running = False

        if self._signal_consumer_task:
            self._signal_consumer_task.cancel()
            try:
                await self._signal_consumer_task
            except asyncio.CancelledError:
                pass

        if self._order_tracker_task:
            self._order_tracker_task.cancel()
            try:
                await self._order_tracker_task
            except asyncio.CancelledError:
                pass

        self.log.info("TradingEngine stopped")

    # =========================================================================
    # Signal Processing
    # =========================================================================

    async def _consume_signals(self) -> None:
        """Consume trade signals from queue."""
        self.log.info("Starting signal consumer")

        while self._running:
            try:
                signal: TradeSignalEvent = await self.signal_queue.get()

                try:
                    if signal.event_type == EventType.ENTRY_SIGNAL:
                        await self._execute_entry(signal)
                    elif signal.event_type == EventType.EXIT_SIGNAL:
                        await self._execute_exit(signal)
                except Exception as e:
                    self.log.error(f"Error executing signal: {e}")
                    await self.notification_queue.put_error(
                        f"Trade execution failed: {signal.symbol} - {e}",
                        source="TradingEngine",
                    )
                finally:
                    self.signal_queue.task_done()

            except asyncio.CancelledError:
                break

    async def _execute_entry(self, signal: TradeSignalEvent) -> None:
        """
        Execute an entry (buy) signal.

        Args:
            signal: Entry signal event
        """
        symbol = signal.symbol
        quantity = signal.quantity
        price = signal.price

        self.log.info(f"Executing ENTRY: {symbol} qty={quantity} @ ~{price}")

        # Check if we can afford this trade
        trade_value = quantity * price
        if trade_value < self.settings.min_trade_amount_usdt:
            self.log.warning(f"Trade value too small: {trade_value} USDT")
            return

        # Place market buy order
        order_result = await self.client.place_market_order(
            symbol=symbol,
            side="Buy",
            qty=quantity,
        )

        if not order_result:
            self.log.error(f"Failed to place buy order for {symbol}")
            return

        # Create order record
        order = await self._create_order_record(
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            bybit_order_id=order_result.get("orderId"),
        )

        # Create position record
        position = await self._create_position(
            symbol=symbol,
            entry_price=price,  # Will be updated with actual fill price
            entry_amount=quantity,
            order_id=order_result.get("orderId"),
        )

        # Track order for status updates
        if order_result.get("orderId"):
            self._pending_orders[order_result["orderId"]] = order

        # Notify strategy
        if self.on_position_opened:
            self.on_position_opened(position)

        # Send notification
        await self.notification_queue.put_notification(
            message=(
                f"<b>BUY Order Placed</b>\n"
                f"Symbol: {symbol}\n"
                f"Quantity: {quantity}\n"
                f"~Price: {price}\n"
                f"Reason: {signal.reason}"
            ),
            source="TradingEngine",
            priority="high",
        )

    async def _execute_exit(self, signal: TradeSignalEvent) -> None:
        """
        Execute an exit (sell) signal.

        Args:
            signal: Exit signal event
        """
        symbol = signal.symbol
        position_id = signal.position_id

        self.log.info(f"Executing EXIT: {symbol} position_id={position_id}")

        # Get position from database
        position = await self.repository.get_by_id(Position, position_id)
        if not position:
            self.log.error(f"Position not found: {position_id}")
            return

        if position.status != PositionStatus.OPEN:
            self.log.warning(f"Position already closed: {position_id}")
            return

        # Sell entire position
        quantity = position.entry_amount

        # Place market sell order
        order_result = await self.client.place_market_order(
            symbol=symbol,
            side="Sell",
            qty=quantity,
        )

        if not order_result:
            self.log.error(f"Failed to place sell order for {symbol}")
            return

        # Create order record
        order = await self._create_order_record(
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=quantity,
            bybit_order_id=order_result.get("orderId"),
            position_id=position_id,
        )

        # Update position with exit info
        await self._close_position(
            position=position,
            exit_price=signal.price,  # Will be updated with actual fill
            exit_amount=quantity,
            exit_reason=signal.reason,
            order_id=order_result.get("orderId"),
        )

        # Track order
        if order_result.get("orderId"):
            self._pending_orders[order_result["orderId"]] = order

        # Notify strategy
        if self.on_position_closed:
            self.on_position_closed(symbol)

        # Send notification
        await self.notification_queue.put_notification(
            message=(
                f"<b>SELL Order Placed</b>\n"
                f"Symbol: {symbol}\n"
                f"Quantity: {quantity}\n"
                f"~Price: {signal.price}\n"
                f"Reason: {signal.reason}"
            ),
            source="TradingEngine",
            priority="high",
        )

    # =========================================================================
    # Order Tracking
    # =========================================================================

    async def _track_orders(self) -> None:
        """
        Background task to track pending orders.

        Polls order status and updates database when filled.
        """
        while self._running:
            try:
                await asyncio.sleep(1)  # Poll every second

                for order_id in list(self._pending_orders.keys()):
                    order = self._pending_orders[order_id]
                    await self._check_order_status(order_id, order)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Error tracking orders: {e}")

    async def _check_order_status(self, order_id: str, order: Order) -> None:
        """
        Check and update order status.

        Args:
            order_id: Bybit order ID
            order: Local order record
        """
        # TODO: Implement actual order status check
        # This should:
        # 1. Query Bybit for order status
        # 2. Update local order record
        # 3. If filled, update position with actual fill price
        # 4. Remove from pending orders
        #
        # status = await self.client.get_order_status(order_id, order.symbol)
        # if status and status.get("orderStatus") == "Filled":
        #     await self._on_order_filled(order, status)
        #     del self._pending_orders[order_id]

        pass

    async def _on_order_filled(self, order: Order, status: Dict) -> None:
        """
        Handle order fill event.

        Args:
            order: Local order record
            status: Bybit order status response
        """
        # TODO: Implement fill handling
        # Update order record with fill details
        # Update position with actual entry/exit price
        # Calculate P&L for exits
        pass

    # =========================================================================
    # Database Operations
    # =========================================================================

    async def _create_order_record(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        bybit_order_id: Optional[str] = None,
        position_id: Optional[int] = None,
    ) -> Order:
        """Create an order record in database."""
        order_data = {
            "symbol": symbol,
            "side": side,
            "status": OrderStatus.PENDING,
            "quantity": quantity,
            "bybit_order_id": bybit_order_id,
            "position_id": position_id,
        }

        await self.repository.insert(Order, order_data)
        self.log.debug(f"Created order record: {symbol} {side}")

        # Return a mock order object for now
        order = Order()
        order.symbol = symbol
        order.side = side
        order.quantity = quantity
        order.bybit_order_id = bybit_order_id
        return order

    async def _create_position(
        self,
        symbol: str,
        entry_price: float,
        entry_amount: float,
        order_id: Optional[str] = None,
    ) -> Position:
        """Create a new position record."""
        position_data = {
            "symbol": symbol,
            "status": PositionStatus.OPEN,
            "entry_price": entry_price,
            "entry_amount": entry_amount,
            "entry_value_usdt": entry_price * entry_amount,
            "entry_time": datetime.now(timezone.utc),
            "entry_order_id": order_id,
        }

        await self.repository.insert(Position, position_data)
        self.log.debug(f"Created position: {symbol}")

        # Return mock position
        position = Position()
        position.symbol = symbol
        position.entry_price = entry_price
        position.entry_amount = entry_amount
        return position

    async def _close_position(
        self,
        position: Position,
        exit_price: float,
        exit_amount: float,
        exit_reason: str,
        order_id: Optional[str] = None,
    ) -> None:
        """Close an existing position."""
        exit_time = datetime.now(timezone.utc)
        exit_value = exit_price * exit_amount

        # Calculate P&L
        profit_usdt = exit_value - position.entry_value_usdt
        profit_pct = (
            (profit_usdt / position.entry_value_usdt) * 100
            if position.entry_value_usdt > 0
            else 0.0
        )

        await self.repository.update(
            Position,
            filter_by={"id": position.id},
            update_values={
                "status": PositionStatus.CLOSED,
                "exit_price": exit_price,
                "exit_amount": exit_amount,
                "exit_value_usdt": exit_value,
                "exit_time": exit_time,
                "exit_order_id": order_id,
                "exit_reason": exit_reason,
                "profit_usdt": profit_usdt,
                "profit_pct": profit_pct,
            },
        )

        self.log.info(
            f"Closed position: {position.symbol} "
            f"P&L: {profit_usdt:.2f} USDT ({profit_pct:.2f}%)"
        )

    # =========================================================================
    # Public API
    # =========================================================================

    async def get_available_balance(self, coin: str = "USDT") -> float:
        """
        Get available balance for trading.

        Args:
            coin: Coin symbol

        Returns:
            Available balance
        """
        return await self.client.get_wallet_balance(coin)

    async def get_open_positions(self) -> List[Position]:
        """Get all open positions."""
        return await self.repository.get_all(
            Position, filters={"status": PositionStatus.OPEN}
        )

    async def cancel_pending_orders(self, symbol: Optional[str] = None) -> int:
        """
        Cancel all pending orders, optionally for a specific symbol.

        Args:
            symbol: Optional symbol filter

        Returns:
            Number of orders cancelled
        """
        # TODO: Implement order cancellation
        self.log.warning("Order cancellation not implemented")
        return 0
