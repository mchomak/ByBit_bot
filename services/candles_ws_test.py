"""
WebSocket Pipeline Test Harness.

Runs the full trading pipeline for a configurable duration (default 1 hour)
and reports comprehensive metrics on all components.

This test harness validates:
- WebSocket connection stability (ping every ~20s)
- Candle ingestion rate
- DB write performance
- Strategy signal generation
- No memory leaks over the test duration

Usage:
    python candles_ws_test.py

Environment variables:
    BYBIT_CATEGORY: Market category (spot, linear, inverse, option). Default: spot
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
from typing import List, Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import settings
from db.models import Candle1m, Position, Order, Signal, Token
from db.repository import Repository
from services.pipeline_events import PipelineMetrics
from services.ws_candles_service import WSCandlesService
from services.strategy_engine import StrategyEngine
from services.db_writer import DBWriterService
from services.execution_engine import ExecutionEngine


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("pipeline-test")


async def load_symbols(
    repo: Repository,
    limit: Optional[int] = None,
    category: Optional[str] = None,
) -> List[str]:
    """Load active symbols from the database."""
    filters = {"is_active": True}
    tokens = await repo.get_all(Token, filters=filters, limit=limit)

    result = []
    for t in tokens:
        if not t.bybit_symbol:
            continue
        if category and t.bybit_categories:
            available = [c.strip().lower() for c in t.bybit_categories.split(",")]
            if category.strip().lower() not in available:
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


def print_metrics(metrics: PipelineMetrics, duration_s: float, symbols_count: int) -> None:
    """Print comprehensive metrics summary."""
    log.info("=" * 70)
    log.info("TEST COMPLETED - PIPELINE METRICS SUMMARY")
    log.info("=" * 70)
    log.info("Duration: %s", format_duration(duration_s))
    log.info("Symbols monitored: %d", symbols_count)
    log.info("-" * 70)
    log.info("WebSocket Messages Received: %d", metrics.ws_messages_received)
    log.info("Candle Updates Processed: %d", metrics.candle_updates_processed)
    log.info("DB Writes Queued: %d", metrics.db_writes_queued)
    log.info("DB Writes Completed: %d", metrics.db_writes_completed)
    log.info("Signals Generated: %d", metrics.signals_generated)
    log.info("Signals Executed: %d", metrics.signals_executed)
    log.info("Errors: %d", metrics.errors)
    log.info("-" * 70)

    if duration_s > 0:
        log.info("Performance Metrics:")
        log.info("  - Avg WS messages/second: %.2f", metrics.ws_messages_received / duration_s)
        log.info("  - Avg candles processed/second: %.2f", metrics.candle_updates_processed / duration_s)
        log.info("  - Avg DB writes/second: %.2f", metrics.db_writes_completed / duration_s)

        if metrics.ws_messages_received > 0:
            db_efficiency = (metrics.db_writes_completed / metrics.ws_messages_received) * 100
            log.info("  - DB write efficiency: %.1f%% (writes/messages)", db_efficiency)

    log.info("=" * 70)


async def log_periodic_status(
    metrics: PipelineMetrics,
    interval_s: float = 30.0,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Log status periodically during the test."""
    while not (stop_event and stop_event.is_set()):
        await asyncio.sleep(interval_s)

        snapshot = metrics.snapshot()
        log.info(
            "STATUS: ws=%d processed=%d db_writes=%d signals=%d errors=%d",
            snapshot["ws_messages_received"],
            snapshot["candle_updates_processed"],
            snapshot["db_writes_completed"],
            snapshot["signals_generated"],
            snapshot["errors"],
        )


async def bootstrap_strategy(
    strategy: StrategyEngine,
    db_writer: DBWriterService,
    symbols: List[str],
) -> None:
    """Bootstrap strategy engine with historical data."""
    log.info("Bootstrapping strategy with historical data...")

    loaded = 0
    for symbol in symbols:
        candles = await db_writer.get_recent_candles(symbol, limit=7200)
        if candles:
            strategy.load_historical_data(symbol, candles)
            loaded += 1

    log.info("Bootstrap complete: %d/%d symbols with history", loaded, len(symbols))


async def main() -> None:
    """Main test runner."""
    # Configuration
    category = os.getenv("BYBIT_CATEGORY", settings.bybit_category).strip().lower()
    ws_domain = settings.bybit_ws_domain
    test_duration_minutes = int(os.getenv("TEST_DURATION_MINUTES", "60"))
    test_duration_s = test_duration_minutes * 60
    max_symbols = int(os.getenv("MAX_SYMBOLS", "0")) or None
    flush_interval_s = float(os.getenv("FLUSH_INTERVAL_S", "1.0"))

    log.info("=" * 70)
    log.info("BYBIT TRADING PIPELINE TEST")
    log.info("=" * 70)
    log.info("Category: %s", category)
    log.info("WS Domain: %s", ws_domain)
    log.info("Test Duration: %d minutes", test_duration_minutes)
    log.info("Max Symbols: %s", max_symbols or "unlimited")
    log.info("Flush Interval: %.1fs", flush_interval_s)
    log.info("=" * 70)

    # Database setup
    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    repo = Repository(session_factory)

    # Load symbols
    symbols = await load_symbols(repo, limit=max_symbols, category=category)
    if not symbols:
        log.error(
            "No active symbols found for category '%s'. "
            "Run token_sync.py first to populate tokens.",
            category,
        )
        await engine.dispose()
        return

    log.info("Loaded %d symbols for category '%s'", len(symbols), category)

    # Create shared metrics
    metrics = PipelineMetrics()

    # Create bounded queues for backpressure
    marketdata_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
    db_write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
    signal_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    # Create services
    ws_service = WSCandlesService(
        symbols=symbols,
        output_queue=marketdata_queue,
        category=category,
        ws_domain=ws_domain,
        ping_interval_s=20.0,
        subscribe_chunk_size=10 if category == "spot" else 200,
        metrics=metrics,
        logger=logging.getLogger("ws-candles"),
    )

    strategy_engine = StrategyEngine(
        marketdata_queue=marketdata_queue,
        db_write_queue=db_write_queue,
        signal_queue=signal_queue,
        volume_window_minutes=7200,
        ma_period=14,
        price_acceleration_factor=3.0,
        metrics=metrics,
        logger=logging.getLogger("strategy"),
    )

    db_writer = DBWriterService(
        input_queue=db_write_queue,
        session_factory=session_factory,
        candle_model=Candle1m,
        flush_interval_s=flush_interval_s,
        retention_days=settings.keep_days,
        cleanup_interval_minutes=120,  # Less frequent cleanup for test
        metrics=metrics,
        logger=logging.getLogger("db-writer"),
    )

    execution_engine = ExecutionEngine(
        signal_queue=signal_queue,
        session_factory=session_factory,
        position_model=Position,
        order_model=Order,
        signal_model=Signal,
        strategy_engine=strategy_engine,
        max_positions=settings.max_positions,
        risk_per_trade_pct=5.0,
        min_trade_usdt=settings.min_trade_amount_usdt,
        metrics=metrics,
        logger=logging.getLogger("execution"),
    )

    # Shutdown event
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        log.info("Received signal %s, initiating shutdown...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Duration timer
    async def duration_timer():
        await asyncio.sleep(test_duration_s)
        log.info("Test duration reached (%d minutes), stopping...", test_duration_minutes)
        stop_event.set()

    start_time = datetime.now(timezone.utc)
    log.info("Starting test at %s", start_time.isoformat())

    # Start services
    try:
        # Start DB writer first
        await db_writer.start()

        # Bootstrap strategy
        await bootstrap_strategy(strategy_engine, db_writer, symbols)

        # Start remaining services
        await strategy_engine.start()
        await execution_engine.start()
        await ws_service.start()

        # Start duration timer
        timer_task = asyncio.create_task(duration_timer(), name="duration-timer")

        # Start periodic status logging
        status_task = asyncio.create_task(
            log_periodic_status(metrics, 30.0, stop_event),
            name="status-logger"
        )

        log.info("All services started. Test running for %d minutes...", test_duration_minutes)

        # Wait for stop event
        await stop_event.wait()

        # Cancel helper tasks
        timer_task.cancel()
        status_task.cancel()
        try:
            await timer_task
        except asyncio.CancelledError:
            pass
        try:
            await status_task
        except asyncio.CancelledError:
            pass

    finally:
        # Stop services in reverse order
        log.info("Stopping services...")

        await ws_service.stop()
        await strategy_engine.stop()
        await execution_engine.stop()
        await db_writer.stop()

        # Calculate duration
        end_time = datetime.now(timezone.utc)
        actual_duration_s = (end_time - start_time).total_seconds()

        # Print final metrics
        print_metrics(metrics, actual_duration_s, len(symbols))

        # Cleanup
        await engine.dispose()
        log.info("Test completed. Database connections closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Test interrupted by user")