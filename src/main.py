"""Main entry point for Bybit Trading Bot.

This module orchestrates all services and manages the bot lifecycle:
1. Initialize database and create tables
2. Start all services in correct order
3. Handle graceful shutdown on signals
4. Coordinate inter-service communication via queues
"""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from ..config.config import settings
from .db.database import Database
from .db.repository import Repository
from .queues.event_bus import EventBus
from .test.market_universe import MarketUniverseService
from .test.market_data import BybitMarketDataService
from .test.strategy import StrategyService
from .test.trading_engine import TradingEngine
from .test.telegram_service import TelegramService


class TradingBot:
    """
    Main orchestrator for the Bybit trading bot.

    Manages service lifecycle and coordinates the event pipeline:
    Data Collection -> Signal Detection -> Execution -> Maintenance -> Result
    """

    def __init__(self):
        """Initialize the trading bot."""
        self.log = logger.bind(component="TradingBot")

        # Core components (initialized in setup)
        self.database: Optional[Database] = None
        self.repository: Optional[Repository] = None
        self.event_bus: Optional[EventBus] = None

        # Services
        self.market_universe: Optional[MarketUniverseService] = None
        self.market_data: Optional[BybitMarketDataService] = None
        self.strategy: Optional[StrategyService] = None
        self.trading_engine: Optional[TradingEngine] = None
        self.telegram: Optional[TelegramService] = None

        # State
        self._running = False
        self._shutdown_event = asyncio.Event()

    async def setup(self) -> None:
        """
        Initialize all components and services.

        Order is important:
        1. Logger
        2. Database
        3. Event Bus (queues)
        4. Services (in dependency order)
        """
        self.log.info("Setting up TradingBot")

        # 1. Configure logging
        self._configure_logging()
        self.log.info("Logging configured")

        # 2. Database
        self.database = Database(settings.database_url)
        await self.database.create_tables()
        self.repository = Repository(self.database.session_factory)
        await self.repository.start_write_worker()
        self.log.info("Database initialized")

        # 3. Event Bus / Queues
        self.event_bus = EventBus()
        self.log.info("Event bus initialized")

        # 4. Services
        # 4.1 Market Universe Service
        self.market_universe = MarketUniverseService(
            settings=settings,
            repository=self.repository,
        )

        # 4.2 Market Data Service
        self.market_data = BybitMarketDataService(
            settings=settings,
            repository=self.repository,
            market_data_queue=self.event_bus.market_data_queue,
        )

        # 4.3 Strategy Service
        self.strategy = StrategyService(
            settings=settings,
            repository=self.repository,
            market_data_queue=self.event_bus.market_data_queue,
            trade_signal_queue=self.event_bus.trade_signal_queue,
            notification_queue=self.event_bus.notification_queue,
        )

        # 4.4 Trading Engine
        self.trading_engine = TradingEngine(
            settings=settings,
            repository=self.repository,
            trade_signal_queue=self.event_bus.trade_signal_queue,
            notification_queue=self.event_bus.notification_queue,
            on_position_opened=self.strategy.on_position_opened,
            on_position_closed=self.strategy.on_position_closed,
        )

        # 4.5 Telegram Service
        self.telegram = TelegramService(
            settings=settings,
            repository=self.repository,
            notification_queue=self.event_bus.notification_queue,
            get_status_callback=self._get_status,
        )

        self.log.info("All services initialized")

    async def start(self) -> None:
        """
        Start all services.

        Services are started in order of dependency.
        """
        self.log.info("Starting services")
        self._running = True

        # Start Telegram first (for notifications)
        await self.telegram.start()
        self.log.info("Telegram service started")

        # Notify startup
        await self.event_bus.notification_queue.put_notification(
            message="<b>Trading Bot Starting...</b>",
            source="TradingBot",
        )

        # Start Market Universe (gets list of tradeable symbols)
        await self.market_universe.start()
        self.log.info("Market Universe service started")

        # Get active symbols
        active_symbols = self.market_universe.get_active_symbols()
        self.log.info(f"Active symbols: {len(active_symbols)}")

        # Start Market Data (WebSocket + historical data)
        if active_symbols:
            await self.market_data.start(active_symbols)
            self.log.info("Market Data service started")
        else:
            self.log.warning("No active symbols, market data service not started")

        # Start Strategy Service
        await self.strategy.start()
        self.log.info("Strategy service started")

        # Start Trading Engine
        await self.trading_engine.start()
        self.log.info("Trading Engine started")

        # Notify ready
        await self.event_bus.notification_queue.put_notification(
            message=(
                "<b>Trading Bot Ready!</b>\n"
                f"Monitoring {len(active_symbols)} symbols\n"
                f"Max positions: {settings.max_positions}"
            ),
            source="TradingBot",
            priority="high",
        )

        self.log.success("All services started successfully")

    async def stop(self) -> None:
        """
        Stop all services gracefully.

        Services are stopped in reverse order.
        """
        self.log.info("Stopping services")
        self._running = False

        # Notify shutdown
        if self.event_bus:
            await self.event_bus.notification_queue.put_notification(
                message="<b>Trading Bot Shutting Down...</b>",
                source="TradingBot",
            )

        # Stop in reverse order
        if self.trading_engine:
            await self.trading_engine.stop()
            self.log.info("Trading Engine stopped")

        if self.strategy:
            await self.strategy.stop()
            self.log.info("Strategy service stopped")

        if self.market_data:
            await self.market_data.stop()
            self.log.info("Market Data service stopped")

        if self.market_universe:
            await self.market_universe.stop()
            self.log.info("Market Universe service stopped")

        if self.telegram:
            await self.telegram.stop()
            self.log.info("Telegram service stopped")

        if self.repository:
            await self.repository.stop_write_worker()

        if self.database:
            await self.database.close()
            self.log.info("Database connection closed")

        self.log.success("Shutdown complete")

    async def run(self) -> None:
        """
        Main run loop.

        Waits for shutdown signal.
        """
        self.log.info("Bot running, waiting for shutdown signal...")

        # Wait for shutdown event
        await self._shutdown_event.wait()

    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self.log.info("Shutdown requested")
        self._shutdown_event.set()

    def _configure_logging(self) -> None:
        """Configure loguru logger."""
        # Create log directory
        log_path = Path(settings.log_path)
        log_path.mkdir(parents=True, exist_ok=True)

        # Remove default handler
        logger.remove()

        # Console handler
        logger.add(
            sys.stderr,
            level=settings.log_level,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{extra[component]}</cyan> | "
                "<level>{message}</level>"
            ),
            filter=lambda record: "component" in record["extra"],
        )

        # File handler - all logs
        logger.add(
            log_path / "bot_{time:YYYY-MM-DD}.log",
            level="DEBUG",
            rotation="00:00",
            retention="7 days",
            compression="gz",
        )

        # File handler - errors only
        logger.add(
            log_path / "errors_{time:YYYY-MM-DD}.log",
            level="ERROR",
            rotation="00:00",
            retention="30 days",
        )

    def _get_status(self) -> Dict[str, Any]:
        """
        Get current bot status for /status command.

        Returns:
            Dict with status information
        """
        status = {
            "active_symbols": len(self.market_universe.get_active_symbols())
            if self.market_universe
            else 0,
            "ws_connected": self.market_data.is_connected()
            if self.market_data
            else False,
            "open_positions": self.strategy.get_open_positions_count()
            if self.strategy
            else 0,
        }

        if self.event_bus:
            queue_stats = self.event_bus.get_queue_stats()
            status["market_queue_size"] = queue_stats["market_data"]
            status["signal_queue_size"] = queue_stats["trade_signal"]
            status["notification_queue_size"] = queue_stats["notification"]

        return status


async def main() -> None:
    """Main entry point."""
    bot = TradingBot()

    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler():
        bot.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        # Setup
        await bot.setup()

        # Start
        await bot.start()

        # Run until shutdown
        await bot.run()

    except Exception as e:
        logger.exception(f"Fatal error: {e}")

    finally:
        # Cleanup
        await bot.stop()


def run() -> None:
    """Entry point for the application."""
    asyncio.run(main())


if __name__ == "__main__":
    run()
