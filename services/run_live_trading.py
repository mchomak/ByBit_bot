"""
Live Trading Pipeline Runner.

Production-grade runner that orchestrates all pipeline components:
- WS Candles Service (Producer)
- Strategy Engine (Consumer/Producer)
- DB Writer (Consumer)
- Execution Engine (Consumer)

Features:
- Graceful shutdown on SIGINT/SIGTERM
- Bootstrap from historical data
- Metrics logging
- Queue-based backpressure
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

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import settings
from db.models import Candle1m, Position, Order, Signal, Token
from db.repository import Repository
from services.pipeline_events import PipelineMetrics
from services.ws_candles_service import WSCandlesService
from services.strategy_engine import StrategyEngine
from services.db_writer import DBWriterService
from services.execution_engine import ExecutionEngine


# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger("live-trading")


async def load_active_symbols(
    repo: Repository,
    category: str,
    limit: Optional[int] = None,
) -> List[str]:
    """Load active symbols from the database."""
    filters = {"is_active": True}
    tokens = await repo.get_all(Token, filters=filters, limit=limit)

    result = []
    for t in tokens:
        if not t.bybit_symbol:
            continue
        # Filter by category if specified
        if category and t.bybit_categories:
            available = [c.strip().lower() for c in t.bybit_categories.split(",")]
            if category.strip().lower() not in available:
                continue
        result.append(t.bybit_symbol)

    return result


async def bootstrap_strategy(
    strategy: StrategyEngine,
    db_writer: DBWriterService,
    symbols: List[str],
) -> None:
    """Bootstrap strategy engine with historical data."""
    log.info("Bootstrapping strategy with historical data...")

    for symbol in symbols:
        candles = await db_writer.get_recent_candles(symbol, limit=7200)
        if candles:
            strategy.load_historical_data(symbol, candles)
            log.debug("Loaded %d candles for %s", len(candles), symbol)

    log.info("Bootstrap complete for %d symbols", len(symbols))


async def log_metrics_periodically(
    metrics: PipelineMetrics,
    interval_s: float = 60.0,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Log pipeline metrics periodically."""
    while not (stop_event and stop_event.is_set()):
        await asyncio.sleep(interval_s)

        snapshot = metrics.snapshot()
        log.info(
            "METRICS: ws_msgs=%d candles=%d db_writes=%d signals=%d executed=%d errors=%d",
            snapshot["ws_messages_received"],
            snapshot["candle_updates_processed"],
            snapshot["db_writes_completed"],
            snapshot["signals_generated"],
            snapshot["signals_executed"],
            snapshot["errors"],
        )


async def main() -> None:
    """Main entry point for live trading."""
    log.info("=" * 60)
    log.info("BYBIT LIVE TRADING PIPELINE")
    log.info("=" * 60)

    # Configuration
    category = settings.bybit_category
    ws_domain = settings.bybit_ws_domain
    max_symbols = settings.max_symbols or None
    max_positions = settings.max_positions
    risk_pct = 5.0  # Fixed 5% risk per trade as per requirements

    log.info("Category: %s", category)
    log.info("WS Domain: %s", ws_domain)
    log.info("Max Positions: %d", max_positions)
    log.info("Risk Per Trade: %.1f%%", risk_pct)
    log.info("=" * 60)

    # Database setup
    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    repo = Repository(session_factory)

    # Load active symbols
    symbols = await load_active_symbols(repo, category, max_symbols)
    if not symbols:
        log.error("No active symbols found. Run token_sync.py first.")
        return

    log.info("Loaded %d active symbols", len(symbols))

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
        ping_interval_s=settings.bybit_ws_ping_interval_s,
        subscribe_chunk_size=10 if category == "spot" else 200,
        metrics=metrics,
        logger=logging.getLogger("ws-candles"),
    )

    strategy_engine = StrategyEngine(
        marketdata_queue=marketdata_queue,
        db_write_queue=db_write_queue,
        signal_queue=signal_queue,
        volume_window_minutes=7200,  # 5 days
        ma_period=14,
        price_acceleration_factor=3.0,  # close >= open * 3
        metrics=metrics,
        logger=logging.getLogger("strategy"),
    )

    db_writer = DBWriterService(
        input_queue=db_write_queue,
        session_factory=session_factory,
        candle_model=Candle1m,
        flush_interval_s=settings.flush_interval_s,
        retention_days=settings.keep_days,
        cleanup_interval_minutes=settings.candle_cleanup_interval_minutes,
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
        max_positions=max_positions,
        risk_per_trade_pct=risk_pct,
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

    # Start services
    try:
        # Start DB writer first (needed for bootstrap)
        await db_writer.start()

        # Bootstrap strategy with historical data
        await bootstrap_strategy(strategy_engine, db_writer, symbols)

        # Start remaining services
        await strategy_engine.start()
        await execution_engine.start()
        await ws_service.start()

        log.info("All services started. Running until interrupted...")

        # Start metrics logging task
        metrics_task = asyncio.create_task(
            log_metrics_periodically(metrics, 60.0, stop_event),
            name="metrics-logger"
        )

        # Wait for shutdown signal
        await stop_event.wait()

        log.info("Shutdown signal received, stopping services...")

        # Cancel metrics task
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass

    finally:
        # Stop services in reverse order
        log.info("Stopping WebSocket service...")
        await ws_service.stop()

        log.info("Stopping strategy engine...")
        await strategy_engine.stop()

        log.info("Stopping execution engine...")
        await execution_engine.stop()

        log.info("Stopping DB writer (flushing pending writes)...")
        await db_writer.stop()

        # Close database
        await engine.dispose()

        # Final metrics
        final_metrics = metrics.snapshot()
        log.info("=" * 60)
        log.info("FINAL METRICS")
        log.info("=" * 60)
        log.info("WS Messages Received: %d", final_metrics["ws_messages_received"])
        log.info("Candles Processed: %d", final_metrics["candle_updates_processed"])
        log.info("DB Writes Completed: %d", final_metrics["db_writes_completed"])
        log.info("Signals Generated: %d", final_metrics["signals_generated"])
        log.info("Signals Executed: %d", final_metrics["signals_executed"])
        log.info("Errors: %d", final_metrics["errors"])
        log.info("=" * 60)
        log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass