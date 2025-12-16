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
    # Bybit API (Trading / Private)
    # ==========================================================================
    bybit_api_key: str = field(default_factory=lambda: os.getenv("BYBIT_API_KEY", ""))
    bybit_api_secret: str = field(default_factory=lambda: os.getenv("BYBIT_API_SECRET", ""))
    bybit_testnet: bool = field(
        default_factory=lambda: os.getenv("BYBIT_TESTNET", "false").lower() == "true"
    )

    # ==========================================================================
    # Bybit Market Data (Public REST + Public WS)
    # ==========================================================================
    # Category for market data (candles/instruments): spot | linear | inverse | option
    bybit_category: str = field(default_factory=lambda: os.getenv("BYBIT_CATEGORY", "linear").lower())

    # Optional explicit overrides. If empty -> derived from bybit_testnet in __post_init__.
    bybit_rest_base_url: str = field(default_factory=lambda: os.getenv("BYBIT_REST_BASE_URL", "").strip())
    bybit_ws_domain: str = field(default_factory=lambda: os.getenv("BYBIT_WS_DOMAIN", "").strip())

    # HTTP client tuning for public REST
    bybit_http_timeout_s: int = field(default_factory=lambda: int(os.getenv("BYBIT_HTTP_TIMEOUT_S", "30")))
    bybit_http_max_retries: int = field(default_factory=lambda: int(os.getenv("BYBIT_HTTP_MAX_RETRIES", "3")))

    # WebSocket tuning for public WS
    bybit_ws_ping_interval_s: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_WS_PING_INTERVAL_S", "20"))
    )
    # Max topics per subscribe batch (to avoid oversize subscribe frames)
    bybit_ws_subscribe_chunk_size: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_WS_SUBSCRIBE_CHUNK_SIZE", "50"))
    )

    # Kline settings
    # Bybit V5 kline interval for 1-minute candles is "1"
    bybit_kline_interval: str = field(default_factory=lambda: os.getenv("BYBIT_KLINE_INTERVAL", "1").strip())

    # Bootstrap/history settings for candles ingestion
    seed_days: int = field(default_factory=lambda: int(os.getenv("SEED_DAYS", "5")))
    seed_concurrency: int = field(default_factory=lambda: int(os.getenv("SEED_CONCURRENCY", "8")))
    keep_days: int = field(default_factory=lambda: int(os.getenv("KEEP_DAYS", "5")))
    flush_interval_s: float = field(default_factory=lambda: float(os.getenv("FLUSH_INTERVAL_S", "1")))
    max_symbols: int = field(default_factory=lambda: int(os.getenv("MAX_SYMBOLS", "0")))

    # ==========================================================================
    # CoinMarketCap API (for market cap filtering)
    # ==========================================================================
    cmc_api_key: str = field(default_factory=lambda: os.getenv("CMC_API_KEY", ""))

    # ==========================================================================
    # Telegram
    # ==========================================================================
    telegram_bot_token: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))

    # ==========================================================================
    # Market Universe Settings
    # ==========================================================================
    # Minimum market cap in USD to include a coin
    min_market_cap_usd: float = field(default_factory=lambda: float(os.getenv("MIN_MARKET_CAP_USD", "1000000")))
    # How often to refresh the coin list (in seconds)
    universe_refresh_interval: int = field(default_factory=lambda: int(os.getenv("UNIVERSE_REFRESH_INTERVAL", "3600")))

    # ==========================================================================
    # Market Data Settings (legacy / generic)
    # ==========================================================================
    # How many days of candle history to keep (legacy, оставлено для совместимости)
    candle_history_days: int = field(default_factory=lambda: int(os.getenv("CANDLE_HISTORY_DAYS", "5")))
    # How often to clean old candles (in minutes)
    candle_cleanup_interval_minutes: int = field(default_factory=lambda: int(os.getenv("CANDLE_CLEANUP_INTERVAL", "60")))
    # WebSocket reconnect delay (in seconds)
    ws_reconnect_delay: float = field(default_factory=lambda: float(os.getenv("WS_RECONNECT_DELAY", "5.0")))

    # ==========================================================================
    # Strategy Settings
    # ==========================================================================
    volume_spike_multiplier: float = field(default_factory=lambda: float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "1.5")))
    min_price_change_pct: float = field(default_factory=lambda: float(os.getenv("MIN_PRICE_CHANGE_PCT", "1.0")))
    ma_exit_period: int = field(default_factory=lambda: int(os.getenv("MA_EXIT_PERIOD", "14")))

    # ==========================================================================
    # Risk Management
    # ==========================================================================
    risk_per_trade_pct: float = field(default_factory=lambda: float(os.getenv("RISK_PER_TRADE_PCT", "1.0")))
    max_positions: int = field(default_factory=lambda: int(os.getenv("MAX_POSITIONS", "5")))
    min_trade_amount_usdt: float = field(default_factory=lambda: float(os.getenv("MIN_TRADE_AMOUNT_USDT", "10.0")))

    # ==========================================================================
    # Logging
    # ==========================================================================
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_path: str = field(default_factory=lambda: os.getenv("LOG_PATH", "./logs"))

    def __post_init__(self) -> None:
        # Normalize category
        self.bybit_category = (self.bybit_category or "linear").lower().strip()
        if self.bybit_category not in {"spot", "linear", "inverse", "option"}:
            raise ValueError("BYBIT_CATEGORY must be one of: spot, linear, inverse, option")

        # Derive REST base url if not explicitly set
        if not self.bybit_rest_base_url:
            self.bybit_rest_base_url = "https://api-testnet.bybit.com" if self.bybit_testnet else "https://api.bybit.com"

        # Derive WS domain if not explicitly set
        if not self.bybit_ws_domain:
            self.bybit_ws_domain = "stream-testnet.bybit.com" if self.bybit_testnet else "stream.bybit.com"

        # Safety clamps
        self.bybit_http_timeout_s = max(5, int(self.bybit_http_timeout_s))
        self.bybit_http_max_retries = max(0, int(self.bybit_http_max_retries))
        self.bybit_ws_ping_interval_s = max(5, int(self.bybit_ws_ping_interval_s))
        self.bybit_ws_subscribe_chunk_size = max(1, int(self.bybit_ws_subscribe_chunk_size))
        self.seed_days = max(1, int(self.seed_days))
        self.seed_concurrency = max(1, int(self.seed_concurrency))
        self.keep_days = max(1, int(self.keep_days))
        self.flush_interval_s = max(0.2, float(self.flush_interval_s))
        self.max_symbols = max(0, int(self.max_symbols))

        # Interval sanity
        self.bybit_kline_interval = (self.bybit_kline_interval or "1").strip()

    # Convenience URLs used by our public market-data client
    @property
    def bybit_public_ws_url(self) -> str:
        return f"wss://{self.bybit_ws_domain}/v5/public/{self.bybit_category}"

    @property
    def bybit_private_ws_url(self) -> str:
        # For future: private stream endpoint is /v5/private on the same domain
        return f"wss://{self.bybit_ws_domain}/v5/private"


# Global settings instance
settings = Settings()
print(settings.database_url)
