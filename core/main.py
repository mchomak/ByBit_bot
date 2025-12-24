"""
Main entry point for the Bybit Trading Bot.

This script orchestrates all trading bot components:
- WebSocket candle ingestion from Bybit
- Strategy engine for entry/exit signal generation
- Execution engine for order placement
- Database persistence for candles, positions, and signals
- Telegram notifications for trading decisions
- Daily token universe synchronization

Usage:
    python run_live_trading.py

Configuration:
    All settings are loaded from config/config.py and .env file.
    See .env.example for required environment variables.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import datetime, timedelta, time as dt_time, timezone
from pathlib import Path
from typing import List, Optional, Set
import os
import sys
import pytz
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import settings
from core.bootstrap_logging import setup_logging
from bot.TelegramBot import TelegramBot
from db.database import Database, CandleWriterService
from db.models import Candle1m, Position, Order, Signal, Token, User
from db.repository import Repository
from services.bybit_client import BybitClient
from services.pipeline_events import PipelineMetrics, CandleUpdate, TradingSignal, SignalType
from services.strategy_engine import StrategyEngine
from services.execution_engine import ExecutionEngine
from services.token_sync_service import TokenSyncService
from services.real_order_executor import (
    RealOrderExecutorService,
    create_place_order_function,
    create_get_balance_function,
)


class TradingBot:
    """
    Main trading bot orchestrator.

    Coordinates all components:
    - WebSocket candle streaming
    - Strategy evaluation
    - Order execution
    - Database persistence
    - Telegram notifications
    - Daily token sync
    """

    def __init__(self) -> None:
        """Initialize the trading bot with configuration."""
        self._stop_event = asyncio.Event()
        self._shutdown_complete = asyncio.Event()

        # Queues for pipeline communication
        self._marketdata_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue(maxsize=10000)
        self._db_write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
        self._signal_queue: asyncio.Queue[TradingSignal] = asyncio.Queue(maxsize=1000)
        self._telegram_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=500)

        # Shared metrics
        self._metrics = PipelineMetrics()

        # Components (initialized in start())
        self._db: Optional[Database] = None
        self._repository: Optional[Repository] = None
        self._bybit_client: Optional[BybitClient] = None
        self._candle_writer: Optional[CandleWriterService] = None
        self._strategy_engine: Optional[StrategyEngine] = None
        self._execution_engine: Optional[ExecutionEngine] = None
        self._order_executor: Optional[RealOrderExecutorService] = None
        self._token_sync_service: Optional[TokenSyncService] = None
        self._telegram_bot: Optional[TelegramBot] = None

        # Task handles
        self._ws_task: Optional[asyncio.Task] = None
        self._token_sync_scheduler_task: Optional[asyncio.Task] = None
        self._metrics_reporter_task: Optional[asyncio.Task] = None

        # Trading logger for buy/sell decisions
        self._trading_log = logger.bind(stream="trading")


    async def start(self) -> None:
        """Start all bot components."""
        logger.info("=" * 60)
        logger.info("BYBIT TRADING BOT STARTING")
        logger.info("=" * 60)

        try:
            # 1. Initialize database
            await self._init_database()

            # 2. Initialize Telegram notifier (if configured)
            await self._init_telegram()

            # 3. Initialize Bybit client
            await self._init_bybit_client()

            # 4. Run initial token sync or load existing tokens
            symbols = await self._init_tokens()

            if not symbols:
                logger.error("No symbols to trade. Check token sync configuration.")
                await self._notify("No symbols to trade. Bot stopping.", notify_type="error")
                return

            # 5. Seed historical candles for strategy bootstrap
            await self._seed_historical_candles(symbols)

            # 6. Initialize pipeline components
            await self._init_pipeline(symbols)

            # 7. Start WebSocket streaming
            await self._start_ws_streaming(symbols)

            # 8. Start daily token sync scheduler
            await self._start_token_sync_scheduler()

            # 9. Start metrics reporter
            await self._start_metrics_reporter()

            # Notify successful start
            await self._notify(
                f"Trading bot started\n"
                f"Mode: {settings.trading_mode}\n"
                f"Symbols: {len(symbols)}\n"
                f"Max positions: {settings.max_positions}\n"
                f"Risk per trade: {settings.risk_per_trade_pct}%",
                notify_type="status"
            )

            logger.info("=" * 60)
            logger.info("BOT RUNNING - Monitoring %d symbols", len(symbols))
            logger.info("=" * 60)

            # Wait for shutdown signal
            await self._stop_event.wait()

        except Exception as e:
            logger.critical("Fatal error during bot startup: {}", e)
            await self._notify(f"Fatal startup error: {e}", notify_type="error")
            raise
        finally:
            await self.stop()


    async def stop(self) -> None:
        """Stop all bot components gracefully."""
        logger.info("Shutting down trading bot...")

        self._stop_event.set()

        # Cancel tasks
        for task in [self._ws_task, self._token_sync_scheduler_task, self._metrics_reporter_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        # Stop components in reverse order
        if self._order_executor:
            await self._order_executor.stop()

        if self._execution_engine:
            await self._execution_engine.stop()

        if self._strategy_engine:
            await self._strategy_engine.stop()

        if self._candle_writer:
            await self._candle_writer.stop()

        if self._token_sync_service:
            await self._token_sync_service.stop()

        if self._telegram_bot:
            await self._telegram_bot.stop()

        if self._bybit_client:
            await self._bybit_client.close()

        if self._db:
            await self._db.close()

        logger.info("Trading bot shutdown complete")
        self._shutdown_complete.set()

    # =========================================================================
    # Initialization Methods
    # =========================================================================

    async def _init_database(self) -> None:
        """Initialize database connection and tables."""
        logger.info("Initializing database...")

        self._db = Database(database_url=settings.database_url)
        await self._db.create_tables()

        self._repository = Repository(session_factory=self._db.session_factory)

        logger.info("Database initialized")


    async def _init_telegram(self) -> None:
        """Initialize Telegram bot with commands if configured."""
        if not settings.is_telegram_configured():
            logger.info("Telegram notifications disabled (not configured)")
            return

        logger.info("Initializing Telegram bot...")

        # Create get_balance function for deposit calculations
        async def get_bot_balance() -> float:
            if self._order_executor:
                return await self._order_executor.get_balance("USDT")
            return 0.0

        self._telegram_bot = TelegramBot(
            token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
            message_queue=self._telegram_queue,
            session_factory=self._db.session_factory,
            user_model=User,
            get_bot_balance_fn=get_bot_balance,
            poll_interval=settings.telegram_rate_limit_s,
        )
        await self._telegram_bot.start()

        logger.info("Telegram bot started with command support")


    async def _init_bybit_client(self) -> None:
        """Initialize Bybit API client."""
        logger.info("Initializing Bybit client...")

        self._bybit_client = BybitClient(
            base_url=settings.bybit_rest_base_url,
            ws_domain=settings.bybit_ws_domain,
            timeout_s=settings.bybit_http_timeout_s,
            max_retries=settings.bybit_http_max_retries,
        )

        logger.info(
            "Bybit client initialized (category=%s, demo=%s)",
            settings.bybit_category,
            settings.bybit_demo
        )


    async def _init_tokens(self) -> List[str]:
        """
        Initialize token list.

        Either loads existing tokens from DB or runs initial sync.
        """
        assert self._repository is not None
        assert self._db is not None

        # Check existing tokens
        tokens = await self._repository.get_all(
            Token,
            filters={"is_active": True},
            limit=settings.max_symbols if settings.max_symbols > 0 else None
        )

        if tokens:
            logger.info("Loaded %d active tokens from database", len(tokens))
            # Filter by current category
            category = settings.bybit_category.lower()
            symbols = [
                t.bybit_symbol for t in tokens
                if t.bybit_categories and category in t.bybit_categories.lower()
            ]
            logger.info("Filtered to %d symbols for category '%s'", len(symbols), category)
            return symbols

        # No tokens - run initial sync
        logger.info("No tokens in database, running initial sync...")

        self._token_sync_service = TokenSyncService(
            repository=self._repository,
            market_cap_threshold_usd=settings.min_market_cap_usd,
            bybit_categories=settings.token_sync_categories_list,
            symbol_aliases=settings.symbol_aliases_dict,
            sync_interval_hours=24,  # Will be managed by scheduler
        )

        count = await self._token_sync_service.sync_now()
        logger.info("Initial token sync completed: %d tokens", count)

        await self._notify(f"Token sync: {count} tokens loaded", notify_type="sync")

        # Reload tokens
        tokens = await self._repository.get_all(
            Token,
            filters={"is_active": True},
            limit=settings.max_symbols if settings.max_symbols > 0 else None
        )

        category = settings.bybit_category.lower()
        symbols = [
            t.bybit_symbol for t in tokens
            if t.bybit_categories and category in t.bybit_categories.lower()
        ]

        return symbols


    async def _seed_historical_candles(self, symbols: List[str]) -> None:
        """
        Check and seed historical candle data for strategy bootstrap.

        For each symbol, checks if we have 5 days of candle data.
        Only backfills symbols that have insufficient data.
        """
        assert self._bybit_client is not None
        assert self._db is not None

        logger.info("Checking candle history for %d symbols...", len(symbols))

        # Check which symbols need backfill
        symbols_to_backfill = []
        required_candles = settings.seed_days * 24 * 60  # 5 days of minute candles

        async with self._db.session_factory() as session:
            from sqlalchemy import func, select

            for symbol in symbols:
                try:
                    # Count confirmed candles for this symbol
                    stmt = select(func.count(Candle1m.id)).where(
                        Candle1m.symbol == symbol,
                        Candle1m.is_confirmed == True  # noqa: E712
                    )
                    result = await session.execute(stmt)
                    count = result.scalar() or 0

                    if count < required_candles * 0.9:  # Allow 10% tolerance
                        symbols_to_backfill.append(symbol)
                        logger.debug(
                            "Symbol %s needs backfill: %d/%d candles",
                            symbol, count, required_candles
                        )
                except Exception as e:
                    logger.warning("Error checking candles for %s: %s", symbol, e)
                    symbols_to_backfill.append(symbol)

        if not symbols_to_backfill:
            logger.info("All %d symbols have sufficient candle history", len(symbols))
            return

        logger.info(
            "Backfilling %d/%d symbols with insufficient history (%d days)...",
            len(symbols_to_backfill),
            len(symbols),
            settings.seed_days
        )

        try:
            await self._bybit_client.seed_1m_history_to_db(
                category=settings.bybit_category,
                symbols=symbols_to_backfill,
                session_factory=self._db.session_factory,
                candle_model=Candle1m,
                days=settings.seed_days,
                concurrency=settings.seed_concurrency,
            )
            logger.info("Historical candle backfill complete for %d symbols", len(symbols_to_backfill))
        except Exception as e:
            logger.error("Error seeding historical candles: {}", e)
            # Continue anyway - strategy will work with available data


    async def _init_pipeline(self, symbols: List[str]) -> None:
        """Initialize the trading pipeline components."""
        assert self._db is not None

        logger.info("Initializing trading pipeline...")

        # Candle writer service
        self._candle_writer = CandleWriterService(
            input_queue=self._db_write_queue,
            session_factory=self._db.session_factory,
            candle_model=Candle1m,
            flush_interval_s=settings.flush_interval_s,
            retention_days=settings.keep_days,
            cleanup_interval_minutes=settings.candle_cleanup_interval_minutes,
            metrics=self._metrics,
        )
        await self._candle_writer.start()

        # Strategy engine
        self._strategy_engine = StrategyEngine(
            marketdata_queue=self._marketdata_queue,
            db_write_queue=self._db_write_queue,
            signal_queue=self._signal_queue,
            volume_window_minutes=settings.volume_window_minutes,
            ma_period=settings.ma_exit_period,
            price_acceleration_factor=settings.price_acceleration_factor,
            metrics=self._metrics,
        )

        # Load historical data for strategy state (ALL symbols)
        logger.info("Loading candle history for %d symbols into strategy engine...", len(symbols))
        loaded_count = 0
        for i, symbol in enumerate(symbols):
            try:
                candles = await self._candle_writer.get_recent_candles(symbol, limit=7200)
                if candles:
                    self._strategy_engine.load_historical_data(symbol, candles)
                    loaded_count += 1
                    if (i + 1) % 50 == 0:
                        logger.info("Loaded history for %d/%d symbols...", i + 1, len(symbols))
            except Exception as e:
                logger.debug("Failed to load history for {}: {}", symbol, e)

        logger.info(
            "Strategy state initialized: %d/%d symbols with history (max_vol, MA14)",
            loaded_count, len(symbols)
        )

        await self._strategy_engine.start()

        # Initialize order executor with correct API keys based on TEST mode
        place_order_fn = None
        get_balance_fn = None

        # Check if API keys are configured for the current mode
        if not settings.active_api_key or not settings.active_api_secret:
            logger.error(
                "API keys not configured for %s mode! "
                "Please set %s in .env",
                settings.trading_mode,
                "BYBIT_DEMO_API_KEY/SECRET" if settings.bybit_demo else "BYBIT_API_KEY/SECRET"
            )
            await self._notify(
                f"⚠️ API keys not configured for {settings.trading_mode} mode!\n"
                f"Bot will not place orders.",
                notify_type="error"
            )
        else:
            logger.info("Initializing order executor (%s mode)...", settings.trading_mode)
            self._order_executor = RealOrderExecutorService(
                api_key=settings.active_api_key,
                api_secret=settings.active_api_secret,
                session_factory=self._db.session_factory,
                order_model=Order,
                telegram_queue=self._telegram_queue,
                demo=settings.bybit_demo,
                max_concurrent=settings.order_max_concurrent,
                retry_count=settings.order_retry_count,
                category=settings.bybit_category,
            )
            await self._order_executor.start()

            # Create functions for ExecutionEngine
            place_order_fn = create_place_order_function(self._order_executor)
            get_balance_fn = create_get_balance_function(self._order_executor)

            if settings.bybit_demo:
                logger.info("=" * 60)
                logger.info("DEMO MODE - Orders will be placed on Bybit demo")
                logger.info("=" * 60)
                await self._notify(
                    "🟡 <b>DEMO MODE</b>\n"
                    "Orders will be placed on Bybit demo (test funds)",
                    notify_type="status"
                )
            else:
                logger.warning("=" * 60)
                logger.warning("PRODUCTION MODE - REAL ORDERS WILL BE PLACED")
                logger.warning("=" * 60)
                await self._notify(
                    "🔴 <b>PRODUCTION MODE</b>\n"
                    "Real orders will be placed on Bybit!",
                    notify_type="status"
                )

        # Execution engine with trading decision logging
        self._execution_engine = ExecutionEngine(
            signal_queue=self._signal_queue,
            session_factory=self._db.session_factory,
            position_model=Position,
            order_model=Order,
            signal_model=Signal,
            strategy_engine=self._strategy_engine,
            max_positions=settings.max_positions,
            risk_per_trade_pct=settings.risk_per_trade_pct,
            min_trade_usdt=settings.min_trade_amount_usdt,
            get_balance_fn=get_balance_fn,
            place_order_fn=place_order_fn,
            metrics=self._metrics,
        )

        # Wrap execution engine to add trading logs and notifications
        original_handle_entry = self._execution_engine._handle_entry
        original_handle_exit = self._execution_engine._handle_exit

        async def logged_handle_entry(signal: TradingSignal) -> None:
            """Wrapper to log entry decisions."""
            self._trading_log.info(
                "ENTRY SIGNAL: {} @ {:.6f} | volume_ratio={:.2f} | price_change={:.2f}%",
                signal.symbol,
                signal.price,
                signal.volume_ratio or 0,
                signal.price_change_pct or 0,
            )

            await original_handle_entry(signal)

            # Check if position was opened (just log, notification sent by RealOrderExecutor)
            if signal.symbol in self._execution_engine._open_positions:
                coin = signal.symbol.replace("USDT", "").replace("USDC", "")
                balance = await self._execution_engine._get_available_balance()
                position_usdt = balance * self._execution_engine._risk_pct
                self._trading_log.info("ENTRY EXECUTED: {} @ {} | size={:.2f} USDT", coin, signal.price, position_usdt)


        async def logged_handle_exit(signal: TradingSignal) -> None:
            """Wrapper to log exit decisions."""
            self._trading_log.info(
                "EXIT SIGNAL: {} @ {:.6f} | ma14={:.6f}",
                signal.symbol,
                signal.price,
                signal.ma14_value or 0,
            )

            # Get position info before exit
            position_id = self._execution_engine._open_positions.get(signal.symbol)
            position = None
            if position_id:
                position = await self._execution_engine._get_position(position_id)

            await original_handle_exit(signal)

            # Check if position was closed (notification sent by RealOrderExecutor)
            if signal.symbol not in self._execution_engine._open_positions and position:
                exit_value = position.entry_amount * signal.price
                profit_usdt = exit_value - position.entry_value_usdt
                profit_pct = (profit_usdt / position.entry_value_usdt) * 100

                coin = signal.symbol.replace("USDT", "").replace("USDC", "")
                profit_sign = "+" if profit_pct >= 0 else ""
                self._trading_log.info("EXIT EXECUTED: {} @ {} | P&L: {}{:.2f}%", coin, signal.price, profit_sign, profit_pct)

                # Update user deposits based on trade profit
                if self._telegram_bot and self._db:
                    try:
                        async with self._db.session_factory() as session:
                            await self._telegram_bot.update_user_deposits_on_trade(session, profit_pct)
                    except Exception as e:
                        self._trading_log.error("Failed to update user deposits: {}", e)

        self._execution_engine._handle_entry = logged_handle_entry
        self._execution_engine._handle_exit = logged_handle_exit

        await self._execution_engine.start()

        logger.info("Trading pipeline initialized (mode: {})", settings.trading_mode)


    async def _start_ws_streaming(self, symbols: List[str]) -> None:
        """Start WebSocket streaming for market data."""
        assert self._bybit_client is not None

        logger.info("Starting WebSocket market data stream...")

        self._ws_task = asyncio.create_task(
            self._bybit_client.stream_klines_to_queue(
                category=settings.bybit_category,
                symbols=symbols,
                output_queue=self._marketdata_queue,
                ping_interval_s=settings.bybit_ws_ping_interval_s,
                metrics=self._metrics,
                stop_event=self._stop_event,
            ),
            name="ws-kline-stream"
        )

        logger.info("WebSocket stream started for %d symbols", len(symbols))


    async def _start_token_sync_scheduler(self) -> None:
        """Start daily token sync scheduler at configured time."""
        self._token_sync_scheduler_task = asyncio.create_task(
            self._token_sync_scheduler_loop(),
            name="token-sync-scheduler"
        )
        logger.info(
            "Token sync scheduler started (daily at %s %s)",
            settings.token_sync_time,
            settings.timezone
        )


    async def _token_sync_scheduler_loop(self) -> None:
        """Run token sync daily at configured time."""
        tz = pytz.timezone(settings.timezone)
        target_time = datetime.strptime(settings.token_sync_time, "%H:%M").time()

        while not self._stop_event.is_set():
            try:
                now = datetime.now(tz)

                # Calculate next run time
                target_dt = datetime.combine(now.date(), target_time)
                target_dt = tz.localize(target_dt)

                if target_dt <= now:
                    # Already passed today, schedule for tomorrow
                    target_dt += timedelta(days=1)

                wait_seconds = (target_dt - now).total_seconds()
                logger.debug("Next token sync in {:.1f} hours", wait_seconds / 3600)

                # Wait until target time
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=wait_seconds
                    )
                    # Stop event was set
                    break
                except asyncio.TimeoutError:
                    # Time to sync
                    pass

                # Run sync
                logger.info("Starting scheduled token sync...")

                if self._token_sync_service and self._repository:
                    try:
                        count = await self._token_sync_service.sync_now()
                        logger.info("Scheduled token sync completed: %d tokens", count)
                        await self._notify(f"Token sync: {count} tokens synced", notify_type="sync")
                    except Exception as e:
                        logger.error("Token sync failed: {}", e)
                        await self._notify(f"Token sync failed: {e}", notify_type="error")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in token sync scheduler: {}", e)
                await asyncio.sleep(60)  # Wait before retry


    async def _start_metrics_reporter(self) -> None:
        """Start periodic metrics reporter."""
        self._metrics_reporter_task = asyncio.create_task(
            self._metrics_reporter_loop(),
            name="metrics-reporter"
        )


    async def _metrics_reporter_loop(self) -> None:
        """Report metrics every 5 minutes."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(300)  # 5 minutes

                snapshot = self._metrics.snapshot()
                logger.info(
                    "Pipeline metrics | ws_msgs={} | candles={} | db_writes={} | "
                    "signals={} | executed={} | errors={}",
                    snapshot["ws_messages_received"],
                    snapshot["candle_updates_processed"],
                    snapshot["db_writes_completed"],
                    snapshot["signals_generated"],
                    snapshot["signals_executed"],
                    snapshot["errors"],
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in metrics reporter: {}", e)

    # =========================================================================
    # Notification Helper
    # =========================================================================

    async def _notify(self, message: str, notify_type: str = "status") -> None:
        """
        Send notification if enabled for the given type.

        Args:
            message: Notification text
            notify_type: One of: trade, signal, error, sync, status
        """
        if not settings.is_telegram_configured():
            return

        if notify_type not in settings.telegram_notify_types_set:
            return

        try:
            self._telegram_queue.put_nowait({"text": message, "parse_mode": "HTML"})
        except asyncio.QueueFull:
            logger.warning("Telegram queue full, dropping notification")


def signal_handler(bot: TradingBot) -> None:
    """Create signal handler for graceful shutdown."""
    def handler(sig, frame):
        logger.info("Received signal %s, initiating shutdown...", sig)
        asyncio.get_event_loop().call_soon_threadsafe(bot._stop_event.set)
    return handler


async def main() -> None:
    """Main entry point."""
    # Setup logging with Telegram integration
    telegram_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
    setup_logging(settings.log_dir, telegram_queue=telegram_queue)

    logger.info("Starting Bybit Trading Bot...")
    logger.info("Config: category=%s, mode=%s",
                settings.bybit_category, settings.trading_mode)

    # Create bot instance
    bot = TradingBot()

    # The telegram queue from logging goes to the same notifier
    # (setup_logging uses it for CRITICAL logs)

    # Setup signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal_handler(bot))

    # Run bot with global error handling
    try:
        await bot.start()
    except Exception as e:
        logger.critical("Unhandled exception in main loop: {}", e)
        # Try to send error to Telegram
        try:
            error_msg = f"<b>🔴 CRITICAL ERROR</b>\n\nBot crashed: {e}"
            telegram_queue.put_nowait({"text": error_msg, "parse_mode": "HTML"})
            # Give a moment for the message to be sent
            await asyncio.sleep(2)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrupted by user")
    except Exception as e:
        logger.critical("Unhandled exception: {}", e)
        sys.exit(1)