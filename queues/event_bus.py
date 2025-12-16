"""Event bus and queue system for inter-service communication.

Queues:
- MarketDataQueue: Candle stream -> StrategyService
- TradeSignalQueue: Trade signals -> TradingEngine
- NotificationQueue: Events -> TelegramService
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from loguru import logger


class EventType(str, Enum):
    """Event types flowing through the system."""

    # Market data events
    CANDLE_UPDATE = "candle_update"  # New/updated candle
    CANDLE_CLOSED = "candle_closed"  # Candle finalized

    # Strategy events
    ENTRY_SIGNAL = "entry_signal"  # Buy signal detected
    EXIT_SIGNAL = "exit_signal"  # Sell signal detected

    # Trading events
    ORDER_CREATED = "order_created"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"

    # System events
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Event:
    """Base event class for all system events."""

    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    source: str = ""  # Component that generated the event


@dataclass
class CandleEvent(Event):
    """Event for candle updates."""

    symbol: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    candle_timestamp: Optional[datetime] = None
    is_confirmed: bool = False


@dataclass
class TradeSignalEvent(Event):
    """Event for trade signals from strategy."""

    symbol: str = ""
    side: str = ""  # "buy" or "sell"
    price: float = 0.0
    quantity: float = 0.0
    reason: str = ""  # e.g., "volume_spike", "ma14_crossover"
    position_id: Optional[int] = None  # For exit signals


@dataclass
class NotificationEvent(Event):
    """Event for Telegram notifications."""

    message: str = ""
    parse_mode: str = "HTML"
    priority: str = "normal"  # "low", "normal", "high"


class AsyncQueue:
    """Typed async queue wrapper with optional size limit."""

    def __init__(self, name: str, maxsize: int = 0):
        self.name = name
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.log = logger.bind(component=f"Queue:{name}")

    async def put(self, item: Any) -> None:
        """Put an item into the queue."""
        await self._queue.put(item)
        self.log.debug(f"Enqueued item, queue size: {self._queue.qsize()}")

    async def get(self) -> Any:
        """Get an item from the queue."""
        item = await self._queue.get()
        return item

    def task_done(self) -> None:
        """Mark a task as done."""
        self._queue.task_done()

    async def join(self) -> None:
        """Wait until all items have been processed."""
        await self._queue.join()

    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Return True if the queue is empty."""
        return self._queue.empty()


class MarketDataQueue(AsyncQueue):
    """
    Queue for candle updates from BybitMarketDataService to StrategyService.

    Events:
    - CANDLE_UPDATE: Every second update of current candle
    - CANDLE_CLOSED: When a minute candle is finalized
    """

    def __init__(self, maxsize: int = 10000):
        super().__init__("MarketData", maxsize)

    async def put_candle(
        self,
        symbol: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        candle_timestamp: datetime,
        is_confirmed: bool = False,
    ) -> None:
        """Convenience method to put a candle event."""
        event = CandleEvent(
            event_type=EventType.CANDLE_CLOSED if is_confirmed else EventType.CANDLE_UPDATE,
            timestamp=datetime.utcnow(),
            source="BybitMarketDataService",
            symbol=symbol,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            candle_timestamp=candle_timestamp,
            is_confirmed=is_confirmed,
        )
        await self.put(event)


class TradeSignalQueue(AsyncQueue):
    """
    Queue for trade signals from StrategyService to TradingEngine.

    Events:
    - ENTRY_SIGNAL: Buy signal (volume spike + price acceleration)
    - EXIT_SIGNAL: Sell signal (MA14 crossover)
    """

    def __init__(self, maxsize: int = 100):
        super().__init__("TradeSignal", maxsize)

    async def put_entry_signal(
        self,
        symbol: str,
        price: float,
        quantity: float,
        reason: str,
    ) -> None:
        """Convenience method to put an entry signal."""
        event = TradeSignalEvent(
            event_type=EventType.ENTRY_SIGNAL,
            timestamp=datetime.utcnow(),
            source="StrategyService",
            symbol=symbol,
            side="buy",
            price=price,
            quantity=quantity,
            reason=reason,
        )
        await self.put(event)

    async def put_exit_signal(
        self,
        symbol: str,
        price: float,
        position_id: int,
        reason: str,
    ) -> None:
        """Convenience method to put an exit signal."""
        event = TradeSignalEvent(
            event_type=EventType.EXIT_SIGNAL,
            timestamp=datetime.utcnow(),
            source="StrategyService",
            symbol=symbol,
            side="sell",
            price=price,
            quantity=0.0,  # Sell entire position
            reason=reason,
            position_id=position_id,
        )
        await self.put(event)


class NotificationQueue(AsyncQueue):
    """
    Queue for notifications from various services to TelegramService.

    Events:
    - Entry/exit notifications
    - Error notifications
    - System status updates
    """

    def __init__(self, maxsize: int = 100):
        super().__init__("Notification", maxsize)

    async def put_notification(
        self,
        message: str,
        source: str = "",
        priority: str = "normal",
        parse_mode: str = "HTML",
    ) -> None:
        """Convenience method to put a notification."""
        event = NotificationEvent(
            event_type=EventType.INFO,
            timestamp=datetime.utcnow(),
            source=source,
            message=message,
            parse_mode=parse_mode,
            priority=priority,
        )
        await self.put(event)

    async def put_error(self, message: str, source: str = "") -> None:
        """Convenience method to put an error notification."""
        event = NotificationEvent(
            event_type=EventType.ERROR,
            timestamp=datetime.utcnow(),
            source=source,
            message=f"<b>ERROR</b>: {message}",
            parse_mode="HTML",
            priority="high",
        )
        await self.put(event)


class EventBus:
    """
    Central event bus for the trading bot.

    Manages all queues and provides pub/sub functionality.
    """

    def __init__(self):
        self.market_data_queue = MarketDataQueue()
        self.trade_signal_queue = TradeSignalQueue()
        self.notification_queue = NotificationQueue()

        self._subscribers: Dict[EventType, List[Callable]] = {}
        self.log = logger.bind(component="EventBus")

    def subscribe(self, event_type: EventType, callback: Callable) -> None:
        """
        Subscribe to a specific event type.

        Args:
            event_type: Type of event to subscribe to
            callback: Async callback function to call when event occurs
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)
        self.log.debug(f"Subscribed to {event_type.value}")

    async def publish(self, event: Event) -> None:
        """
        Publish an event to all subscribers.

        Args:
            event: Event to publish
        """
        callbacks = self._subscribers.get(event.event_type, [])
        for callback in callbacks:
            try:
                await callback(event)
            except Exception as e:
                self.log.error(f"Error in event callback: {e}")

    def get_queue_stats(self) -> Dict[str, int]:
        """Get current queue sizes for monitoring."""
        return {
            "market_data": self.market_data_queue.qsize(),
            "trade_signal": self.trade_signal_queue.qsize(),
            "notification": self.notification_queue.qsize(),
        }
