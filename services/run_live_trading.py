"""
Live Trading Pipeline Runner.

Production-grade runner that orchestrates all pipeline components:
- BybitClient (WebSocket Producer)
- StrategyEngine (Consumer/Producer)
- CandleWriterService (Consumer)
- ExecutionEngine (Consumer)

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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.config import settings
from db.database import Database, CandleWriterService
from db.models import Candle1m, Position, Order, Signal, Token
from db.repository import Repository

from services.pipeline_events import PipelineMetrics
from services.strategy_engine import StrategyEngine
from services.bybit_client import BybitClient
from services.execution_engine import ExecutionEngine


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
        if category and t.bybit_categories:
            available = [c.strip().lower() for c in t.bybit_categories.split(",")]
            if category.strip().lower() not in available:
                continue
        result.append(t.bybit_symbol)

    return result


async def bootstrap_strategy(
    strategy: StrategyEngine,
    candle_writer: CandleWriterService,
    symbols: List[str],
) -> None:
    """Bootstrap strategy engine with historical data."""
    log.info("Bootstrapping strategy with historical data...")

    for symbol in symbols:
        candles = await candle_writer.get_recent_candles(symbol, limit=7200)
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
    max_symbols = settings.max_symbols or None
    max_positions = settings.max_positions
    risk_pct = 5.0  # Fixed 5% risk per trade

    log.info("Category: %s", category)
    log.info("Max Positions: %d", max_positions)
    log.info("Risk Per Trade: %.1f%%", risk_pct)
    log.info("=" * 60)

    # Database setup
    db = Database(settings.database_url)
    await db.create_tables()
    repo = Repository(db.session_factory)

    # Load active symbols
    symbols = await load_active_symbols(repo, category, max_symbols)
    if not symbols:
        log.error("No active symbols found. Run token_sync.py first.")
        await db.close()
        return

    log.info("Loaded %d active symbols", len(symbols))

    # Create shared metrics
    metrics = PipelineMetrics()

    # Create bounded queues for backpressure
    marketdata_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
    db_write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
    signal_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    # Shutdown event
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        log.info("Received signal %s, initiating shutdown...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create services
    bybit_client = BybitClient(
        base_url=settings.bybit_rest_base_url,
        ws_domain=settings.bybit_ws_domain,
        testnet=settings.bybit_testnet,
    )

    strategy_engine = StrategyEngine(
        marketdata_queue=marketdata_queue,
        db_write_queue=db_write_queue,
        signal_queue=signal_queue,
        volume_window_minutes=7200,  # 5 days
        ma_period=14,
        price_acceleration_factor=3.0,
        metrics=metrics,
        logger=logging.getLogger("strategy"),
    )

    candle_writer = CandleWriterService(
        input_queue=db_write_queue,
        session_factory=db.session_factory,
        candle_model=Candle1m,
        flush_interval_s=settings.flush_interval_s,
        retention_days=settings.keep_days,
        cleanup_interval_minutes=settings.candle_cleanup_interval_minutes,
        metrics=metrics,
        logger=logging.getLogger("candle-writer"),
    )

    execution_engine = ExecutionEngine(
        signal_queue=signal_queue,
        session_factory=db.session_factory,
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

    # Start services
    try:
        # Start candle writer first (needed for bootstrap)
        await candle_writer.start()

        # Bootstrap strategy with historical data
        await bootstrap_strategy(strategy_engine, candle_writer, symbols)

        # Start remaining services
        await strategy_engine.start()
        await execution_engine.start()

        # Start WebSocket streaming task
        ws_task = asyncio.create_task(
            bybit_client.stream_klines_to_queue(
                category=category,
                symbols=symbols,
                output_queue=marketdata_queue,
                ping_interval_s=settings.bybit_ws_ping_interval_s,
                metrics=metrics,
                stop_event=stop_event,
            ),
            name="ws-stream"
        )

        log.info("All services started. Running until interrupted...")

        # Start metrics logging task
        metrics_task = asyncio.create_task(
            log_metrics_periodically(metrics, 60.0, stop_event),
            name="metrics-logger"
        )

        # Wait for shutdown signal
        await stop_event.wait()

        log.info("Shutdown signal received, stopping services...")

        # Cancel helper tasks
        metrics_task.cancel()
        ws_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

    finally:
        # Stop services in reverse order
        log.info("Stopping strategy engine...")
        await strategy_engine.stop()

        log.info("Stopping execution engine...")
        await execution_engine.stop()

        log.info("Stopping candle writer (flushing pending writes)...")
        await candle_writer.stop()

        # Close Bybit client
        await bybit_client.close()

        # Close database
        await db.close()

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