"""Configuration settings for Bybit Trading Bot."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """Application settings loaded from environment variables."""

    # ==========================================================================
    # Database
    # ==========================================================================
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/bybit_bot"
        )
    )

    # ==========================================================================
    # Bybit API
    # ==========================================================================
    bybit_api_key: str = field(default_factory=lambda: os.getenv("BYBIT_API_KEY", ""))
    bybit_api_secret: str = field(
        default_factory=lambda: os.getenv("BYBIT_API_SECRET", "")
    )
    bybit_testnet: bool = field(
        default_factory=lambda: os.getenv("BYBIT_TESTNET", "false").lower() == "true"
    )

    # ==========================================================================
    # CoinMarketCap API (for market cap filtering)
    # ==========================================================================
    cmc_api_key: str = field(default_factory=lambda: os.getenv("CMC_API_KEY", ""))

    # ==========================================================================
    # Telegram
    # ==========================================================================
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )

    # ==========================================================================
    # Market Universe Settings
    # ==========================================================================
    # Minimum market cap in USD to include a coin
    min_market_cap_usd: float = field(
        default_factory=lambda: float(os.getenv("MIN_MARKET_CAP_USD", "1000000"))
    )
    # How often to refresh the coin list (in seconds)
    universe_refresh_interval: int = field(
        default_factory=lambda: int(os.getenv("UNIVERSE_REFRESH_INTERVAL", "3600"))
    )

    # ==========================================================================
    # Market Data Settings
    # ==========================================================================
    # How many days of candle history to keep
    candle_history_days: int = field(
        default_factory=lambda: int(os.getenv("CANDLE_HISTORY_DAYS", "5"))
    )
    # How often to clean old candles (in minutes)
    candle_cleanup_interval_minutes: int = field(
        default_factory=lambda: int(os.getenv("CANDLE_CLEANUP_INTERVAL", "60"))
    )
    # WebSocket reconnect delay (in seconds)
    ws_reconnect_delay: float = field(
        default_factory=lambda: float(os.getenv("WS_RECONNECT_DELAY", "5.0"))
    )

    # ==========================================================================
    # Strategy Settings
    # ==========================================================================
    # Volume spike threshold multiplier (e.g., 2.0 = 200% of max historical volume)
    volume_spike_multiplier: float = field(
        default_factory=lambda: float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "1.5"))
    )
    # Minimum price change percentage (close vs open) to trigger signal
    min_price_change_pct: float = field(
        default_factory=lambda: float(os.getenv("MIN_PRICE_CHANGE_PCT", "1.0"))
    )
    # MA period for exit signal
    ma_exit_period: int = field(
        default_factory=lambda: int(os.getenv("MA_EXIT_PERIOD", "14"))
    )

    # ==========================================================================
    # Risk Management
    # ==========================================================================
    # Percentage of available balance per trade
    risk_per_trade_pct: float = field(
        default_factory=lambda: float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
    )
    # Maximum number of simultaneous open positions
    max_positions: int = field(
        default_factory=lambda: int(os.getenv("MAX_POSITIONS", "5"))
    )
    # Minimum trade amount in USDT
    min_trade_amount_usdt: float = field(
        default_factory=lambda: float(os.getenv("MIN_TRADE_AMOUNT_USDT", "10.0"))
    )

    # ==========================================================================
    # Logging
    # ==========================================================================
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_path: str = field(default_factory=lambda: os.getenv("LOG_PATH", "./logs"))


# Global settings instance
settings = Settings()
