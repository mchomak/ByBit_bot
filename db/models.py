"""Database models for Bybit Trading Bot.

Tables:
- tokens: List of tradeable coins/pairs with market cap and status
- candles_1m: 1-minute OHLCV candle history (5-day rolling window)
- positions: Active and closed trading positions
- orders: Order history
- users: Telegram users with their settings
- logs: System and error logs
- signals: Strategy signal log for analysis
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Column,
    String,
    Float,
    DateTime,
    Integer,
    Boolean,
    BigInteger,
    Text,
    Index,
    UniqueConstraint,
    Enum as SQLEnum,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


# =============================================================================
# Enums
# =============================================================================


class PositionStatus(str, Enum):
    """Position status enum."""

    OPEN = "open"
    CLOSED = "closed"


class OrderSide(str, Enum):
    """Order side enum."""

    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Order status enum."""

    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class SignalType(str, Enum):
    """Signal type enum."""

    ENTRY = "entry"  # Volume spike + price acceleration
    EXIT = "exit"  # MA14 crossover


class LogLevel(str, Enum):
    """Log level enum."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# =============================================================================
# Models
# =============================================================================


class Token(Base):
    """
    List of tradable tokens that passed all filters.

    Only contains tokens that are ready for trading.
    Populated from AllToken after filtering.

    is_active: controlled by StalePrice checker only (every 50 min)
    """

    __tablename__ = "tokens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True, index=True)  # e.g., "BTC"
    bybit_symbol = Column(String(30), nullable=False, unique=True)  # e.g., "BTCUSDT"
    name = Column(String(100), nullable=True)  # e.g., "Bitcoin"
    market_cap_usd = Column(BigInteger, nullable=True)
    bybit_categories = Column(String(50), nullable=True)  # Comma-separated: "spot,linear"
    is_active = Column(Boolean, default=True)  # StalePrice checker sets this
    max_market_qty = Column(Float, nullable=True)  # Max quantity for market orders (learned from errors)
    last_updated = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_tokens_active", "is_active"),)


class AllToken(Base):
    """
    Complete list of all tokens (tradable and non-tradable).

    Stores all tokens with their deactivation reasons.
    Used as source for the tokens table after filtering.

    Deactivation reasons:
    - Blacklist: Token in manual blacklist
    - ST: Special Treatment / high-risk token
    - LowMcap: Low market cap or not available on Bybit
    - LowVolume: 24h trading volume < $700k
    - New: Token appeared on exchange < 24h ago
    - StalePrice: Inactive price (checked every 50 min)
    - BigLoss: Token had significant trading loss (re-enabled on daily sync)
    """

    __tablename__ = "all_tokens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True, index=True)  # e.g., "BTC"
    bybit_symbol = Column(String(30), nullable=False, unique=True)  # e.g., "BTCUSDT"
    name = Column(String(100), nullable=True)  # e.g., "Bitcoin"
    market_cap_usd = Column(BigInteger, nullable=True)
    bybit_categories = Column(String(50), nullable=True)  # Comma-separated: "spot,linear"
    is_active = Column(Boolean, default=True)  # Whether token passed main 4 filters
    deactivation_reason = Column(String(50), nullable=True)  # Reason for deactivation
    max_market_qty = Column(Float, nullable=True)  # Max quantity for market orders
    last_updated = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_all_tokens_active", "is_active"),)


class BlacklistedToken(Base):
    """
    Tokens that should be excluded from trading.

    Manually added tokens that will be removed from the active token list
    during sync and startup.
    """

    __tablename__ = "blacklisted_tokens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, unique=True, index=True)  # e.g., "BTC"
    bybit_symbol = Column(String(30), nullable=True)  # e.g., "BTCUSDT" (optional)
    reason = Column(String(200), nullable=True)  # Why this token is blacklisted
    added_by = Column(String(100), nullable=True)  # Who added it (telegram username)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Candle1m(Base):
    """
    1-minute OHLCV candles for all monitored symbols.

    Updated every second via WebSocket (upsert current candle).
    At minute boundary, new candle is created.
    Old candles (> 5 days) are cleaned up periodically.
    """

    __tablename__ = "candles_1m"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(30), nullable=False, index=True)  # e.g., "BTCUSDT"
    timestamp = Column(
        DateTime(timezone=True), nullable=False
    )  # Start of the minute (truncated)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)  # Base asset volume
    turnover = Column(Float, nullable=True)  # Quote asset volume (USDT)
    is_confirmed = Column(Boolean, default=False)  # True when candle is finalized

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp", name="uix_candles_symbol_ts"),
        Index("ix_candles_symbol_ts", "symbol", "timestamp"),
    )


class Position(Base):
    """
    Trading positions (active and historical).

    Created on BUY signal, updated on SELL.
    """

    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(30), nullable=False, index=True)
    status = Column(
        SQLEnum(PositionStatus), default=PositionStatus.OPEN, nullable=False
    )

    # Entry info
    entry_price = Column(Float, nullable=False)
    entry_amount = Column(Float, nullable=False)  # Quantity of base asset
    entry_value_usdt = Column(Float, nullable=False)  # USDT value at entry
    entry_time = Column(DateTime(timezone=True), nullable=False)
    entry_order_id = Column(String(100), nullable=True)

    # Exit info (filled when closed)
    exit_price = Column(Float, nullable=True)
    exit_amount = Column(Float, nullable=True)
    exit_value_usdt = Column(Float, nullable=True)
    exit_time = Column(DateTime(timezone=True), nullable=True)
    exit_order_id = Column(String(100), nullable=True)
    exit_reason = Column(String(50), nullable=True)  # e.g., "ma14_crossover"

    # P&L
    profit_usdt = Column(Float, nullable=True)
    profit_pct = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_positions_status", "status"),)


class Order(Base):
    """
    Order log for all buy/sell orders.

    Tracks order lifecycle from creation to fill/cancel.
    """

    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bybit_order_id = Column(String(100), nullable=True, unique=True)
    symbol = Column(String(30), nullable=False, index=True)
    side = Column(SQLEnum(OrderSide), nullable=False)
    status = Column(SQLEnum(OrderStatus), default=OrderStatus.PENDING, nullable=False)

    # Order details
    price = Column(Float, nullable=True)  # None for market orders
    quantity = Column(Float, nullable=False)
    filled_quantity = Column(Float, default=0.0)
    avg_fill_price = Column(Float, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Link to position
    position_id = Column(Integer, nullable=True)

    __table_args__ = (Index("ix_orders_symbol_status", "symbol", "status"),)


class User(Base):
    """
    Telegram users who interact with the bot.

    Stores user-specific settings like position limits and deposit info.
    """

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, nullable=False, unique=True, index=True)
    username = Column(String(100), nullable=True)
    is_admin = Column(Boolean, default=False)

    # User settings
    limit_positions = Column(Integer, default=5)  # Max simultaneous positions
    notifications_enabled = Column(Boolean, default=True)

    # Deposit tracking
    deposit = Column(Float, default=0.0)  # User's virtual deposit in USDT
    share_pct = Column(Float, default=0.0)  # User's share of bot portfolio (%)
    total_profit_usdt = Column(Float, default=0.0)  # Total accumulated profit in USDT

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_active = Column(DateTime(timezone=True), server_default=func.now())


class Signal(Base):
    """
    Strategy signal log for analysis and debugging.

    Records every entry/exit signal generated by the strategy.
    """

    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(30), nullable=False, index=True)
    signal_type = Column(SQLEnum(SignalType), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)

    # Signal metrics
    price = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    volume_ratio = Column(Float, nullable=True)  # Current vol / max historical vol
    price_change_pct = Column(Float, nullable=True)  # (close - open) / open * 100
    ma14_value = Column(Float, nullable=True)

    # Was the signal acted upon?
    executed = Column(Boolean, default=False)
    execution_reason = Column(String(200), nullable=True)  # Why not executed, if any

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (Index("ix_signals_symbol_ts", "symbol", "timestamp"),)


class Log(Base):
    """
    System and error logs stored in database.

    For critical errors, API failures, reconnects, etc.
    """

    __tablename__ = "logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    level = Column(SQLEnum(LogLevel), nullable=False, index=True)
    component = Column(String(50), nullable=False)  # e.g., "TradingEngine", "WebSocket"
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)  # JSON or stack trace
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (Index("ix_logs_level_ts", "level", "timestamp"),)