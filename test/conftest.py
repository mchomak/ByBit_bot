"""Pytest configuration and shared fixtures for Bybit Trading Bot tests."""

import asyncio
import os
import sys
from datetime import datetime, timezone
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import Settings
from db.models import (
    Base,
    Position,
    Order,
    Signal,
    Token,
    PositionStatus,
    OrderSide,
    OrderStatus,
    SignalType,
)
from services.pipeline_events import TradingSignal, SignalType as PipelineSignalType, SymbolState, CandleUpdate


# =============================================================================
# Configuration Fixtures
# =============================================================================


@pytest.fixture
def test_settings() -> Settings:
    """Create test settings with safe defaults."""
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    os.environ["DEMO"] = "true"
    os.environ["DRY_RUN"] = "true"
    os.environ["BYBIT_API_KEY"] = ""
    os.environ["BYBIT_API_SECRET"] = ""
    os.environ["TELEGRAM_BOT_TOKEN"] = ""
    os.environ["TELEGRAM_CHAT_ID"] = ""

    return Settings()


@pytest.fixture
def mock_settings() -> MagicMock:
    """Create a mock settings object for isolated unit tests."""
    settings = MagicMock()
    settings.database_url = "sqlite+aiosqlite:///:memory:"
    settings.bybit_demo = True
    settings.dry_run = True
    settings.bybit_api_key = ""
    settings.bybit_api_secret = ""
    settings.bybit_category = "spot"
    settings.risk_per_trade_pct = 5.0
    settings.max_positions = 5
    settings.min_trade_amount_usdt = 10.0
    settings.stop_loss_pct = 7.0
    settings.skip_red_candle_entry = False
    settings.volume_only_green_candles = True
    settings.is_telegram_configured = MagicMock(return_value=False)
    return settings


# =============================================================================
# Database Fixtures
# =============================================================================


@pytest.fixture
async def mock_session_factory() -> AsyncMock:
    """Create a mock async session factory."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    factory = MagicMock()
    factory.return_value = session
    return factory


# =============================================================================
# Model Fixtures
# =============================================================================


@pytest.fixture
def sample_position() -> Position:
    """Create a sample position for testing."""
    return Position(
        id=1,
        symbol="BTCUSDT",
        status=PositionStatus.OPEN,
        entry_price=50000.0,
        entry_amount=0.01,
        entry_value_usdt=500.0,
        entry_time=datetime.now(timezone.utc),
        entry_order_id="test-order-123",
    )


@pytest.fixture
def sample_closed_position() -> Position:
    """Create a sample closed position for testing."""
    entry_time = datetime.now(timezone.utc)
    return Position(
        id=2,
        symbol="ETHUSDT",
        status=PositionStatus.CLOSED,
        entry_price=3000.0,
        entry_amount=1.0,
        entry_value_usdt=3000.0,
        entry_time=entry_time,
        entry_order_id="entry-order-456",
        exit_price=3300.0,
        exit_amount=1.0,
        exit_value_usdt=3300.0,
        exit_time=entry_time,
        exit_order_id="exit-order-789",
        exit_reason="ma14_crossover",
        profit_usdt=300.0,
        profit_pct=10.0,
    )


@pytest.fixture
def sample_order() -> Order:
    """Create a sample order for testing."""
    return Order(
        id=1,
        bybit_order_id="bybit-123456",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        status=OrderStatus.FILLED,
        price=50000.0,
        quantity=0.01,
        filled_quantity=0.01,
        avg_fill_price=50000.0,
        position_id=1,
    )


@pytest.fixture
def sample_signal() -> Signal:
    """Create a sample signal for testing."""
    return Signal(
        id=1,
        symbol="BTCUSDT",
        signal_type=SignalType.ENTRY,
        timestamp=datetime.now(timezone.utc),
        price=50000.0,
        volume=1000.0,
        volume_ratio=2.5,
        price_change_pct=350.0,
        executed=False,
    )


@pytest.fixture
def sample_token() -> Token:
    """Create a sample token for testing."""
    return Token(
        id=1,
        symbol="BTC",
        bybit_symbol="BTCUSDT",
        name="Bitcoin",
        market_cap_usd=1000000000000,
        bybit_categories="spot,linear",
        is_active=True,
    )


# =============================================================================
# Trading Signal Fixtures
# =============================================================================


@pytest.fixture
def sample_entry_signal() -> TradingSignal:
    """Create a sample entry trading signal."""
    return TradingSignal(
        symbol="BTCUSDT",
        signal_type=PipelineSignalType.ENTRY,
        timestamp=datetime.now(timezone.utc),
        price=50000.0,
        volume=1000.0,
        volume_ratio=2.5,
        price_change_pct=350.0,
    )


@pytest.fixture
def sample_exit_signal() -> TradingSignal:
    """Create a sample exit trading signal."""
    return TradingSignal(
        symbol="BTCUSDT",
        signal_type=PipelineSignalType.EXIT,
        timestamp=datetime.now(timezone.utc),
        price=48000.0,
        volume=500.0,
        ma14_value=49000.0,
    )


# =============================================================================
# Mock API Fixtures
# =============================================================================


@pytest.fixture
def mock_bybit_client() -> AsyncMock:
    """Create a mock Bybit API client."""
    client = AsyncMock()

    # Mock common API responses
    client.get_price = AsyncMock(return_value=50000.0)
    client.get_balance = AsyncMock(return_value={"USDT": 10000.0})
    client.get_min_order = AsyncMock(return_value={
        "min_amt": 10.0,
        "min_qty": 0.0001,
        "qty_step": 0.0001,
        "price_step": 0.01,
    })
    client.place_order = AsyncMock(return_value={
        "orderId": "test-order-id",
        "status": "Filled",
    })
    client.cancel_order = AsyncMock(return_value=True)
    client.get_open_orders = AsyncMock(return_value=[])

    return client


@pytest.fixture
def mock_telegram_queue() -> asyncio.Queue:
    """Create a mock Telegram message queue."""
    return asyncio.Queue(maxsize=100)


# =============================================================================
# Utility Fixtures
# =============================================================================


@pytest.fixture
def freeze_time():
    """Fixture to freeze time for deterministic testing."""
    import unittest.mock as mock

    frozen_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = frozen_time
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        yield frozen_time


# =============================================================================
# Async Utilities
# =============================================================================


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
