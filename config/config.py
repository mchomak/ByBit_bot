"""Configuration settings for Bybit Trading Bot."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional
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
    bybit_category: str = field(
        default_factory=lambda: os.getenv("BYBIT_CATEGORY", "spot").lower()
    )
    bybit_rest_base_url: str = field(
        default_factory=lambda: os.getenv("BYBIT_REST_BASE_URL", "").strip()
    )
    bybit_ws_domain: str = field(
        default_factory=lambda: os.getenv("BYBIT_WS_DOMAIN", "").strip()
    )

    # HTTP client tuning
    bybit_http_timeout_s: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_HTTP_TIMEOUT_S", "30"))
    )
    bybit_http_max_retries: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_HTTP_MAX_RETRIES", "3"))
    )

    # WebSocket tuning
    bybit_ws_ping_interval_s: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_WS_PING_INTERVAL_S", "20"))
    )
    bybit_ws_subscribe_chunk_size: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_WS_SUBSCRIBE_CHUNK_SIZE", "50"))
    )

    # Kline settings
    bybit_kline_interval: str = field(
        default_factory=lambda: os.getenv("BYBIT_KLINE_INTERVAL", "1").strip()
    )

    # Bootstrap/history settings
    seed_days: int = field(default_factory=lambda: int(os.getenv("SEED_DAYS", "5")))
    seed_concurrency: int = field(default_factory=lambda: int(os.getenv("SEED_CONCURRENCY", "8")))
    keep_days: int = field(default_factory=lambda: int(os.getenv("KEEP_DAYS", "5")))
    flush_interval_s: float = field(default_factory=lambda: float(os.getenv("FLUSH_INTERVAL_S", "1")))
    max_symbols: int = field(default_factory=lambda: int(os.getenv("MAX_SYMBOLS", "0")))

    # ==========================================================================
    # Token Sync Settings
    # ==========================================================================
    min_market_cap_usd: float = field(
        default_factory=lambda: float(os.getenv("MIN_MARKET_CAP_USD", "100000000"))
    )
    token_sync_categories: str = field(
        default_factory=lambda: os.getenv("TOKEN_SYNC_CATEGORIES", "spot,linear")
    )
    symbol_aliases: str = field(
        default_factory=lambda: os.getenv("SYMBOL_ALIASES", "")
    )
    token_sync_time: str = field(
        default_factory=lambda: os.getenv("TOKEN_SYNC_TIME", "00:00")
    )
    timezone: str = field(
        default_factory=lambda: os.getenv("TZ_NAME", "UTC")
    )

    # ==========================================================================
    # Strategy Settings
    # ==========================================================================
    # Volume spike: current volume must exceed max 5-day volume
    volume_window_days: int = field(
        default_factory=lambda: int(os.getenv("VOLUME_WINDOW_DAYS", "5"))
    )
    # Price acceleration: close >= open * this factor (3.0 = 300% gain)
    price_acceleration_factor: float = field(
        default_factory=lambda: float(os.getenv("PRICE_ACCELERATION_FACTOR", "3.0"))
    )
    # MA period for exit signal
    ma_exit_period: int = field(
        default_factory=lambda: int(os.getenv("MA_EXIT_PERIOD", "14"))
    )

    # ==========================================================================
    # Risk Management
    # ==========================================================================
    risk_per_trade_pct: float = field(
        default_factory=lambda: float(os.getenv("RISK_PER_TRADE_PCT", "5.0"))
    )
    max_positions: int = field(
        default_factory=lambda: int(os.getenv("MAX_POSITIONS", "5"))
    )
    min_trade_amount_usdt: float = field(
        default_factory=lambda: float(os.getenv("MIN_TRADE_AMOUNT_USDT", "10.0"))
    )

    # ==========================================================================
    # Order Execution
    # ==========================================================================
    # Set to False to enable real trading (True = dry-run/paper trading)
    dry_run: bool = field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").lower() == "true"
    )
    # Maximum concurrent order executions
    order_max_concurrent: int = field(
        default_factory=lambda: int(os.getenv("ORDER_MAX_CONCURRENT", "1"))
    )
    # Number of retries for failed orders
    order_retry_count: int = field(
        default_factory=lambda: int(os.getenv("ORDER_RETRY_COUNT", "3"))
    )

    # ==========================================================================
    # Telegram Notifications
    # ==========================================================================
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
    telegram_enabled: bool = field(
        default_factory=lambda: os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
    )
    # Notification types: trade, signal, error, sync, status
    telegram_notify_types: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_NOTIFY_TYPES", "trade,error,sync")
    )
    telegram_rate_limit_s: float = field(
        default_factory=lambda: float(os.getenv("TELEGRAM_RATE_LIMIT_S", "1.0"))
    )

    # Telegram notification templates
    telegram_entry_template: str = field(
        default_factory=lambda: os.getenv(
            "TELEGRAM_ENTRY_TEMPLATE",
            "🟢 Purchase\nCoin: {symbol}\nEntry price: {price}\nPosition Size: {position_size} USDT\nTime: {time}"
        )
    )
    telegram_exit_template: str = field(
        default_factory=lambda: os.getenv(
            "TELEGRAM_EXIT_TEMPLATE",
            "🔴 Sale\nCoin: {symbol}\nExit Price: {exit_price}\nTotal withdrawal amount: {exit_value} USDT\nProfit: {profit_sign}{profit_pct}%\nTime: {time}"
        )
    )

    # ==========================================================================
    # Logging
    # ==========================================================================
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_dir: str = field(default_factory=lambda: os.getenv("LOG_DIR", "./logs"))

    # ==========================================================================
    # Candle Cleanup
    # ==========================================================================
    candle_cleanup_interval_minutes: int = field(
        default_factory=lambda: int(os.getenv("CANDLE_CLEANUP_INTERVAL", "60"))
    )

    def __post_init__(self) -> None:
        self.bybit_category = (self.bybit_category or "spot").lower().strip()
        if self.bybit_category not in {"spot", "linear", "inverse", "option"}:
            raise ValueError("BYBIT_CATEGORY must be one of: spot, linear, inverse, option")

        if not self.bybit_rest_base_url:
            self.bybit_rest_base_url = (
                "https://api-testnet.bybit.com" if self.bybit_testnet
                else "https://api.bybit.com"
            )

        if not self.bybit_ws_domain:
            self.bybit_ws_domain = (
                "stream-testnet.bybit.com" if self.bybit_testnet
                else "stream.bybit.com"
            )

        # Safety clamps
        self.bybit_http_timeout_s = max(5, int(self.bybit_http_timeout_s))
        self.bybit_http_max_retries = max(0, int(self.bybit_http_max_retries))
        self.bybit_ws_ping_interval_s = max(5, int(self.bybit_ws_ping_interval_s))
        self.seed_days = max(1, int(self.seed_days))
        self.keep_days = max(1, int(self.keep_days))
        self.flush_interval_s = max(0.2, float(self.flush_interval_s))
        self.max_positions = max(1, int(self.max_positions))
        self.risk_per_trade_pct = max(0.1, min(100.0, float(self.risk_per_trade_pct)))
        self.min_trade_amount_usdt = max(1.0, float(self.min_trade_amount_usdt))
        self.bybit_kline_interval = (self.bybit_kline_interval or "1").strip()

    @property
    def bybit_public_ws_url(self) -> str:
        return f"wss://{self.bybit_ws_domain}/v5/public/{self.bybit_category}"

    @property
    def bybit_private_ws_url(self) -> str:
        return f"wss://{self.bybit_ws_domain}/v5/private"

    @property
    def token_sync_categories_list(self) -> List[str]:
        return [c.strip().lower() for c in self.token_sync_categories.split(",") if c.strip()]

    @property
    def symbol_aliases_dict(self) -> dict:
        if not self.symbol_aliases:
            return {}
        result = {}
        for pair in self.symbol_aliases.split(","):
            if "=" in pair:
                old, new = pair.split("=", 1)
                result[old.strip().upper()] = new.strip().upper()
        return result

    @property
    def telegram_notify_types_set(self) -> set:
        return {t.strip().lower() for t in self.telegram_notify_types.split(",") if t.strip()}

    @property
    def volume_window_minutes(self) -> int:
        return self.volume_window_days * 24 * 60

    def is_telegram_configured(self) -> bool:
        return bool(self.telegram_enabled and self.telegram_bot_token and self.telegram_chat_id)


# Global settings instance
settings = Settings()