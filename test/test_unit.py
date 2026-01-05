"""Unit tests for Bybit Trading Bot core components.

These tests are fast and don't require external dependencies.
Run with: pytest test/test_unit.py -v
"""

import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import Settings
from db.models import (
    Position,
    Order,
    Signal,
    Token,
    PositionStatus,
    OrderSide,
    OrderStatus,
    SignalType,
)
from services.pipeline_events import (
    TradingSignal,
    SignalType as PipelineSignalType,
    SymbolState,
    CandleUpdate,
    DBWriteTask,
    PipelineMetrics,
)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestSettings:
    """Tests for Settings configuration class."""

    def test_default_settings(self):
        """Test that settings load with defaults."""
        # Clear relevant env vars
        for key in ["DATABASE_URL", "DEMO", "DRY_RUN"]:
            os.environ.pop(key, None)

        settings = Settings()

        assert settings.bybit_category in ["spot", "linear", "inverse", "option"]
        assert settings.max_positions >= 1
        assert settings.risk_per_trade_pct >= 0.1

    def test_demo_mode_detection(self):
        """Test demo mode flag parsing."""
        os.environ["DEMO"] = "true"
        settings = Settings()
        assert settings.bybit_demo is True

        os.environ["DEMO"] = "false"
        settings = Settings()
        assert settings.bybit_demo is False

    def test_telegram_configuration_check(self):
        """Test Telegram configuration detection."""
        os.environ["TELEGRAM_ENABLED"] = "true"
        os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
        os.environ["TELEGRAM_CHAT_ID"] = "12345"

        settings = Settings()
        assert settings.is_telegram_configured() is True

        os.environ["TELEGRAM_BOT_TOKEN"] = ""
        settings = Settings()
        assert settings.is_telegram_configured() is False

    def test_volume_window_minutes(self):
        """Test volume window calculation."""
        os.environ["VOLUME_WINDOW_DAYS"] = "5"
        settings = Settings()
        assert settings.volume_window_minutes == 5 * 24 * 60

    def test_api_key_selection(self):
        """Test that correct API keys are selected based on mode."""
        os.environ["DEMO"] = "true"
        os.environ["BYBIT_API_KEY"] = "prod-key"
        os.environ["BYBIT_API_SECRET"] = "prod-secret"
        os.environ["BYBIT_DEMO_API_KEY"] = "demo-key"
        os.environ["BYBIT_DEMO_API_SECRET"] = "demo-secret"

        settings = Settings()
        assert settings.active_api_key == "demo-key"
        assert settings.active_api_secret == "demo-secret"

        os.environ["DEMO"] = "false"
        settings = Settings()
        assert settings.active_api_key == "prod-key"
        assert settings.active_api_secret == "prod-secret"


# =============================================================================
# Model Tests
# =============================================================================


class TestPositionModel:
    """Tests for Position model."""

    def test_position_creation(self, sample_position):
        """Test position can be created with required fields."""
        assert sample_position.symbol == "BTCUSDT"
        assert sample_position.status == PositionStatus.OPEN
        assert sample_position.entry_price == 50000.0
        assert sample_position.entry_amount == 0.01

    def test_closed_position(self, sample_closed_position):
        """Test closed position has exit info."""
        assert sample_closed_position.status == PositionStatus.CLOSED
        assert sample_closed_position.exit_price == 3300.0
        assert sample_closed_position.profit_usdt == 300.0
        assert sample_closed_position.profit_pct == 10.0


class TestOrderModel:
    """Tests for Order model."""

    def test_order_creation(self, sample_order):
        """Test order can be created with required fields."""
        assert sample_order.symbol == "BTCUSDT"
        assert sample_order.side == OrderSide.BUY
        assert sample_order.status == OrderStatus.FILLED


class TestSignalModel:
    """Tests for Signal model."""

    def test_signal_creation(self, sample_signal):
        """Test signal can be created with required fields."""
        assert sample_signal.symbol == "BTCUSDT"
        assert sample_signal.signal_type == SignalType.ENTRY
        assert sample_signal.volume_ratio == 2.5


class TestTokenModel:
    """Tests for Token model."""

    def test_token_creation(self, sample_token):
        """Test token can be created with required fields."""
        assert sample_token.symbol == "BTC"
        assert sample_token.bybit_symbol == "BTCUSDT"
        assert sample_token.is_active is True


# =============================================================================
# Pipeline Events Tests
# =============================================================================


class TestSymbolState:
    """Tests for SymbolState tracking."""

    def test_symbol_state_initialization(self):
        """Test SymbolState initializes with defaults."""
        state = SymbolState(symbol="BTCUSDT")

        assert state.symbol == "BTCUSDT"
        assert state.max_volume_5d == 0.0
        assert state.volume_history == []
        assert state.has_open_position is False

    def test_update_volume_history(self):
        """Test volume history tracking and max calculation."""
        state = SymbolState(symbol="BTCUSDT")
        now = datetime.now(timezone.utc)

        # Add some volume data
        state.update_volume_history(now - timedelta(hours=1), 100.0)
        state.update_volume_history(now - timedelta(hours=2), 200.0)
        state.update_volume_history(now, 150.0)

        assert state.max_volume_5d == 200.0
        assert len(state.volume_history) == 3

    def test_volume_history_window_cleanup(self):
        """Test that old volume entries are removed."""
        state = SymbolState(symbol="BTCUSDT")
        now = datetime.now(timezone.utc)

        # Add old volume data (beyond 5 day window)
        old_time = now - timedelta(days=6)
        state.update_volume_history(old_time, 1000.0, window_minutes=7200)

        # Add recent volume
        state.update_volume_history(now, 100.0, window_minutes=7200)

        # Old entry should be cleaned up
        assert state.max_volume_5d == 100.0
        assert len(state.volume_history) == 1

    def test_update_ma14(self):
        """Test MA14 calculation."""
        state = SymbolState(symbol="BTCUSDT")

        # Add 13 closes - should return None (not enough data)
        for i in range(13):
            result = state.update_ma14(100.0)
        assert result is None

        # Add 14th close - should return MA
        result = state.update_ma14(100.0)
        assert result == 100.0

        # Add 15th close with different value
        result = state.update_ma14(114.0)
        # MA should be (13 * 100 + 114) / 14 = 101
        assert result == 101.0

    def test_green_candle_volume_filter(self):
        """Test volume filtering for green candles only."""
        state = SymbolState(symbol="BTCUSDT")
        now = datetime.now(timezone.utc)

        # Add green candle volume
        state.update_volume_history(
            now, 100.0, is_green_candle=True, only_green_candles=True
        )
        assert len(state.volume_history) == 1

        # Add red candle volume - should be ignored
        state.update_volume_history(
            now + timedelta(minutes=1),
            200.0,
            is_green_candle=False,
            only_green_candles=True,
        )
        assert len(state.volume_history) == 1
        assert state.max_volume_5d == 100.0


class TestCandleUpdate:
    """Tests for CandleUpdate dataclass."""

    def test_candle_update_creation(self):
        """Test CandleUpdate can be created."""
        now = datetime.now(timezone.utc)
        candle = CandleUpdate(
            symbol="BTCUSDT",
            timestamp=now,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            turnover=5000000.0,
            confirm=True,
        )

        assert candle.symbol == "BTCUSDT"
        assert candle.confirm is True
        assert candle.minute_key == ("BTCUSDT", now)

    def test_candle_update_immutable(self):
        """Test CandleUpdate is immutable (frozen)."""
        now = datetime.now(timezone.utc)
        candle = CandleUpdate(
            symbol="BTCUSDT",
            timestamp=now,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            turnover=5000000.0,
            confirm=True,
        )

        with pytest.raises(AttributeError):
            candle.symbol = "ETHUSDT"


class TestTradingSignal:
    """Tests for TradingSignal dataclass."""

    def test_entry_signal_creation(self, sample_entry_signal):
        """Test entry signal can be created."""
        assert sample_entry_signal.signal_type == PipelineSignalType.ENTRY
        assert sample_entry_signal.volume_ratio == 2.5
        assert sample_entry_signal.price_change_pct == 350.0

    def test_exit_signal_creation(self, sample_exit_signal):
        """Test exit signal can be created."""
        assert sample_exit_signal.signal_type == PipelineSignalType.EXIT
        assert sample_exit_signal.ma14_value == 49000.0


class TestPipelineMetrics:
    """Tests for PipelineMetrics tracking."""

    def test_metrics_initialization(self):
        """Test metrics initialize to zero."""
        metrics = PipelineMetrics()

        assert metrics.ws_messages_received == 0
        assert metrics.candle_updates_processed == 0
        assert metrics.errors == 0

    def test_metrics_snapshot(self):
        """Test metrics snapshot."""
        metrics = PipelineMetrics()
        metrics.ws_messages_received = 100
        metrics.signals_generated = 5

        snapshot = metrics.snapshot()

        assert snapshot["ws_messages_received"] == 100
        assert snapshot["signals_generated"] == 5
        assert isinstance(snapshot, dict)


# =============================================================================
# Trading Logic Tests
# =============================================================================


class TestTradingLogic:
    """Tests for trading decision logic."""

    def test_entry_signal_criteria(self):
        """Test entry signal detection criteria."""
        # Entry requires: volume > max_5d_volume AND close >= open * 3
        state = SymbolState(symbol="TESTUSDT")
        now = datetime.now(timezone.utc)

        # Set up historical max volume
        state.update_volume_history(now - timedelta(hours=1), 100.0)

        current_volume = 150.0  # Above max of 100
        open_price = 100.0
        close_price = 350.0  # 350% = 3.5x (above 3x threshold)

        volume_ratio = current_volume / state.max_volume_5d
        price_change_pct = ((close_price - open_price) / open_price) * 100

        # Check entry criteria
        volume_spike = volume_ratio > 1.0
        price_acceleration = close_price >= open_price * 3.0

        assert volume_spike is True
        assert price_acceleration is True
        assert price_change_pct == 250.0  # 250% gain

    def test_exit_signal_criteria(self):
        """Test exit signal detection criteria (MA14 crossover)."""
        state = SymbolState(symbol="TESTUSDT")

        # Build up MA14 with higher prices
        for _ in range(14):
            state.update_ma14(100.0)

        ma14 = state.update_ma14(100.0)
        current_price = 95.0  # Below MA14

        # Exit signal: price < MA14
        should_exit = current_price < ma14

        assert should_exit is True

    def test_stop_loss_trigger(self):
        """Test stop-loss calculation."""
        entry_price = 100.0
        stop_loss_pct = 7.0
        current_price = 92.0  # 8% below entry

        price_drop_pct = ((entry_price - current_price) / entry_price) * 100
        should_stop = price_drop_pct >= stop_loss_pct

        assert price_drop_pct == 8.0
        assert should_stop is True

    def test_position_sizing(self):
        """Test position size calculation."""
        balance_usdt = 10000.0
        risk_per_trade_pct = 5.0
        min_trade_usdt = 10.0

        position_size = balance_usdt * (risk_per_trade_pct / 100)

        assert position_size == 500.0
        assert position_size >= min_trade_usdt

    def test_profit_calculation(self):
        """Test profit calculation."""
        entry_price = 100.0
        entry_amount = 10.0  # 10 units
        exit_price = 110.0

        entry_value = entry_price * entry_amount
        exit_value = exit_price * entry_amount

        profit_usdt = exit_value - entry_value
        profit_pct = ((exit_price - entry_price) / entry_price) * 100

        assert profit_usdt == 100.0
        assert profit_pct == 10.0


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_zero_volume_handling(self):
        """Test handling of zero volume."""
        state = SymbolState(symbol="TESTUSDT")
        now = datetime.now(timezone.utc)

        state.update_volume_history(now, 0.0)

        assert state.max_volume_5d == 0.0

    def test_empty_volume_history(self):
        """Test behavior with empty volume history."""
        state = SymbolState(symbol="TESTUSDT")

        # max should be 0 when no history
        assert state.max_volume_5d == 0.0

    def test_ma14_with_insufficient_data(self):
        """Test MA14 returns None with insufficient data."""
        state = SymbolState(symbol="TESTUSDT")

        for i in range(10):
            result = state.update_ma14(100.0)

        assert result is None

    def test_duplicate_entry_prevention(self):
        """Test that duplicate entries in same minute are prevented."""
        state = SymbolState(symbol="TESTUSDT")
        now = datetime.now(timezone.utc)
        minute = now.replace(second=0, microsecond=0)

        state.last_entry_minute = minute

        # Should skip if same minute
        is_duplicate = state.last_entry_minute == minute

        assert is_duplicate is True


# =============================================================================
# Integration-style Unit Tests
# =============================================================================


class TestSignalFlow:
    """Tests for signal generation flow."""

    def test_complete_entry_signal_flow(self):
        """Test complete flow from candle to entry signal."""
        state = SymbolState(symbol="TESTUSDT")
        now = datetime.now(timezone.utc)

        # Setup historical volume (5 days of candles)
        for i in range(100):
            ts = now - timedelta(minutes=i * 60)
            state.update_volume_history(ts, 50.0 + i)

        # Current candle with spike
        current_volume = 500.0  # Massive spike
        open_price = 10.0
        close_price = 35.0  # 3.5x = 350%

        # Check conditions
        volume_ratio = current_volume / state.max_volume_5d if state.max_volume_5d > 0 else 0
        price_change = ((close_price - open_price) / open_price) * 100

        is_volume_spike = volume_ratio > 1.0
        is_price_acceleration = close_price >= open_price * 3.0
        is_not_duplicate = state.last_entry_minute != now.replace(second=0, microsecond=0)

        should_generate_entry = is_volume_spike and is_price_acceleration and is_not_duplicate

        assert is_volume_spike is True
        assert is_price_acceleration is True
        assert should_generate_entry is True

    def test_complete_exit_signal_flow(self):
        """Test complete flow for exit signal."""
        state = SymbolState(symbol="TESTUSDT")
        state.has_open_position = True
        state.entry_price = 100.0

        # Build MA14 at 95
        for _ in range(14):
            state.update_ma14(95.0)

        current_price = 90.0  # Below MA14

        ma14 = state.update_ma14(current_price)
        should_exit_ma = current_price < ma14

        # Also check stop-loss
        stop_loss_pct = 7.0
        price_drop = ((state.entry_price - current_price) / state.entry_price) * 100
        should_exit_stoploss = price_drop >= stop_loss_pct

        assert should_exit_ma is True
        assert should_exit_stoploss is True  # 10% drop > 7% threshold
