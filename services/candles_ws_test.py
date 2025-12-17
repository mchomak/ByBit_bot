"""
WebSocket Candle Ingestion Test Runner.

Runs the Bybit V5 WebSocket kline stream for a configurable duration (default 1 hour)
and reports metrics on messages received and DB upserts.

Usage:
    python candles_ws_test.py

Environment variables:
    BYBIT_CATEGORY: Market category (spot, linear, inverse, option). Default: spot
    BYBIT_WS_DOMAIN: WebSocket domain. Default: stream.bybit.com
    TEST_DURATION_MINUTES: How long to run the test. Default: 60 (1 hour)
    MAX_SYMBOLS: Limit number of symbols (0 = unlimited). Default: 0
    LOG_LEVEL: Logging level. Default: INFO
    FLUSH_INTERVAL_S: Buffer flush interval. Default: 1.0
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.bybit_client import BybitClient, CandleBufferMetrics
from config.config import settings
from db.models import Candle1m, Token
from db.repository import Repository


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("candles_ws_test")


async def load_symbols(
    repo: Repository,
    limit: int | None = None,
    category: str | None = None,
) -> list[str]:
    """
    Load symbols from DB, optionally filtered by Bybit category.

    Args:
        repo: Repository instance
        limit: Max number of symbols to return
        category: Filter by Bybit category (spot, linear, etc.)
    """
    filters = {"is_active": True}
    tokens = await repo.get_all(Token, filters=filters, limit=limit)

    result = []
    for t in tokens:
        if not t.bybit_symbol:
            continue
        # If category filter is specified, check if token is available in that category
        if category and t.bybit_categories:
            available_cats = [c.strip().lower() for c in t.bybit_categories.split(",")]
            if category.strip().lower() not in available_cats:
                continue
        result.append(t.bybit_symbol)

    return result


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def print_metrics(metrics: CandleBufferMetrics, duration_s: float, symbols_count: int) -> None:
    """Print metrics summary."""
    log.info("=" * 60)
    log.info("TEST COMPLETED - METRICS SUMMARY")
    log.info("=" * 60)
    log.info("Duration: %s", format_duration(duration_s))
    log.info("Symbols monitored: %d", symbols_count)
    log.info("-" * 60)
    log.info("Messages received: %d", metrics.messages_received)
    log.info("Rows upserted to DB: %d", metrics.rows_upserted)
    log.info("Flush operations: %d", metrics.flush_count)
    log.info("Errors: %d", metrics.errors)
    log.info("-" * 60)
    if duration_s > 0:
        log.info("Avg messages/second: %.2f", metrics.messages_received / duration_s)
        log.info("Avg upserts/second: %.2f", metrics.rows_upserted / duration_s)
        if metrics.flush_count > 0:
            log.info("Avg rows/flush: %.1f", metrics.rows_upserted / metrics.flush_count)
    log.info("=" * 60)


async def main() -> None:
    """Main test runner."""
    # Configuration
    database_url = settings.database_url
    bybit_category = os.getenv("BYBIT_CATEGORY", "spot").strip().lower()
    ws_domain = os.getenv("BYBIT_WS_DOMAIN", "stream.bybit.com").strip()
    test_duration_minutes = int(os.getenv("TEST_DURATION_MINUTES", "60"))
    test_duration_s = test_duration_minutes * 60
    max_symbols = int(os.getenv("MAX_SYMBOLS", "0")) or None
    flush_interval_s = float(os.getenv("FLUSH_INTERVAL_S", "1.0"))

    log.info("=" * 60)
    log.info("BYBIT WEBSOCKET CANDLE INGESTION TEST")
    log.info("=" * 60)
    log.info("Category: %s", bybit_category)
    log.info("WS Domain: %s", ws_domain)
    log.info("Test duration: %d minutes", test_duration_minutes)
    log.info("Max symbols: %s", max_symbols or "unlimited")
    log.info("Flush interval: %.1fs", flush_interval_s)
    log.info("=" * 60)

    # Database setup
    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    repo = Repository(session_factory)

    # Load symbols
    symbols = await load_symbols(repo, limit=max_symbols, category=bybit_category)
    if not symbols:
        log.error(
            "No active symbols found for category '%s'. "
            "Run token_sync.py first to populate tokens.",
            bybit_category,
        )
        return

    log.info("Loaded %d symbols for category '%s'", len(symbols), bybit_category)

    # Setup graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        log.info("Received signal %s, initiating graceful shutdown...", sig)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start test
    start_time = datetime.now(timezone.utc)
    log.info("Starting WebSocket stream at %s", start_time.isoformat())

    metrics = CandleBufferMetrics()

    async with BybitClient(ws_domain=ws_domain) as bybit:
        try:
            metrics = await bybit.stream_kline_1m_to_db(
                category=bybit_category,
                symbols=symbols,
                session_factory=session_factory,
                candle_model=Candle1m,
                flush_interval_s=flush_interval_s,
                duration_s=test_duration_s,
            )
        except asyncio.CancelledError:
            log.info("Stream cancelled")

    # Calculate actual duration
    end_time = datetime.now(timezone.utc)
    actual_duration_s = (end_time - start_time).total_seconds()

    # Print results
    print_metrics(metrics, actual_duration_s, len(symbols))

    # Cleanup
    await engine.dispose()
    log.info("Test completed. Database connections closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Test interrupted by user")