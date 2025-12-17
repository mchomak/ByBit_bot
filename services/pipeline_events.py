"""
Shared event/data models for the trading pipeline.

These dataclasses define the messages passed between pipeline components
via asyncio queues, ensuring type safety and clean interfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class SignalType(str, Enum):
    """Type of trading signal."""
    ENTRY = "entry"
    EXIT = "exit"


@dataclass(frozen=True, slots=True)
class CandleUpdate:
    """
    Normalized candle update from WebSocket.

    Immutable event representing a single kline update.
    The `confirm` flag indicates whether the candle is finalized.
    """
    symbol: str
    timestamp: datetime  # Start of the minute (UTC)
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: Optional[float]
    confirm: bool  # True = candle is closed/finalized

    @property
    def minute_key(self) -> tuple[str, datetime]:
        """Unique key for deduplication: (symbol, minute_timestamp)."""
        return (self.symbol, self.timestamp)


@dataclass(frozen=True, slots=True)
class DBWriteTask:
    """
    Task to write/upsert a candle to the database.

    Passed from strategy engine to DB writer via queue.
    """
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: Optional[float]
    is_confirmed: bool


@dataclass(slots=True)
class TradingSignal:
    """
    Trading signal emitted by strategy engine.

    Contains all context needed for the execution engine to decide
    whether to act on the signal.
    """
    signal_type: SignalType
    symbol: str
    timestamp: datetime  # Candle timestamp that triggered the signal
    price: float  # Current price (close)
    volume: float
    volume_ratio: Optional[float] = None  # current_vol / max_5d_vol
    price_change_pct: Optional[float] = None  # (close - open) / open * 100
    ma14_value: Optional[float] = None


@dataclass
class SymbolState:
    """
    Per-symbol state maintained by the strategy engine.

    Tracks rolling statistics efficiently without querying DB on each tick.
    """
    symbol: str

    # Rolling 5-day max volume tracking
    # We store recent volumes in a bounded structure and track the max
    max_volume_5d: float = 0.0
    volume_history: list = field(default_factory=list)  # (timestamp, volume) tuples

    # MA14 calculation using rolling sum
    ma14_closes: list = field(default_factory=list)  # deque of last 14 closes
    ma14_sum: float = 0.0

    # Entry deduplication: last minute we entered
    last_entry_minute: Optional[datetime] = None

    # Track if we have an open position
    has_open_position: bool = False

    def update_volume_history(self, ts: datetime, volume: float, window_minutes: int = 7200) -> None:
        """
        Update volume history and recalculate max.

        Args:
            ts: Candle timestamp
            volume: Candle volume
            window_minutes: 5 days = 5 * 24 * 60 = 7200 minutes
        """
        # Add new volume
        self.volume_history.append((ts, volume))

        # Remove old entries outside window
        from datetime import timedelta
        cutoff = ts - timedelta(minutes=window_minutes)
        self.volume_history = [(t, v) for t, v in self.volume_history if t >= cutoff]

        # Recalculate max (could optimize with a heap, but this is simpler)
        self.max_volume_5d = max((v for _, v in self.volume_history), default=0.0)

    def update_ma14(self, close: float) -> Optional[float]:
        """
        Update MA14 with new close price.

        Returns the current MA14 value, or None if not enough data.
        """
        MA_PERIOD = 14

        self.ma14_closes.append(close)
        self.ma14_sum += close

        # Keep only last 14 closes
        if len(self.ma14_closes) > MA_PERIOD:
            removed = self.ma14_closes.pop(0)
            self.ma14_sum -= removed

        # Return MA if we have enough data
        if len(self.ma14_closes) >= MA_PERIOD:
            return self.ma14_sum / MA_PERIOD
        return None


@dataclass
class PipelineMetrics:
    """Metrics for monitoring pipeline health."""
    ws_messages_received: int = 0
    candle_updates_processed: int = 0
    db_writes_queued: int = 0
    db_writes_completed: int = 0
    signals_generated: int = 0
    signals_executed: int = 0
    errors: int = 0

    def snapshot(self) -> dict:
        """Return a dict snapshot of current metrics."""
        return {
            "ws_messages_received": self.ws_messages_received,
            "candle_updates_processed": self.candle_updates_processed,
            "db_writes_queued": self.db_writes_queued,
            "db_writes_completed": self.db_writes_completed,
            "signals_generated": self.signals_generated,
            "signals_executed": self.signals_executed,
            "errors": self.errors,
        }