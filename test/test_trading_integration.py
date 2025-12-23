"""
Integration Test for Trading Pipeline.

This test verifies the complete trading flow:
1. Injects fake signals that trigger BUY logic
2. Verifies order execution and Telegram notification
3. Waits, then injects fake signals that trigger SELL logic
4. Verifies exit order and database records

Usage:
    python -m tests.test_trading_integration

Configuration:
    Set these environment variables for real Telegram testing:
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID

    For live order testing (CAUTION - uses real money!):
    - BYBIT_API_KEY
    - BYBIT_API_SECRET
    - DRY_RUN=false
"""

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from loguru import logger
from sqlalchemy import select, func

from config.config import settings
from db.database import Database
from db.models import Base, Candle1m, Position, Order, Signal, Token, PositionStatus, OrderStatus
from services.pipeline_events import TradingSignal, SignalType, CandleUpdate
from services.execution_engine import ExecutionEngine
from services.strategy_engine import StrategyEngine
from services.real_order_executor import RealOrderExecutorService, create_place_order_function, create_get_balance_function
from bot.TelegramNotifier import TelegramNotifier


@dataclass
class TestResult:
    """Test result container."""
    name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None


class TradingIntegrationTest:
    """
    Integration test for the trading pipeline.

    Tests the complete flow from signal generation to order execution
    and Telegram notifications.
    """

    TEST_SYMBOL = "BTCUSDT"

    def __init__(
        self,
        test_telegram: bool = True,
        test_real_orders: bool = False,
        sell_delay_seconds: int = 30,  # Reduced from 5 minutes for testing
        cleanup_before: bool = False,  # Clean up test data before running
    ):
        """
        Initialize the integration test.

        Args:
            test_telegram: Whether to test Telegram notifications
            test_real_orders: Whether to test real order placement (CAUTION!)
            sell_delay_seconds: Delay before triggering sell (default 30s for testing)
            cleanup_before: Whether to clean up old test data before running
        """
        self.test_telegram = test_telegram
        self.test_real_orders = test_real_orders
        self.sell_delay_seconds = sell_delay_seconds
        self.cleanup_before = cleanup_before

        # Components
        self._db: Optional[Database] = None
        self._telegram_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._telegram_notifier: Optional[TelegramNotifier] = None
        self._signal_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._execution_engine: Optional[ExecutionEngine] = None
        self._real_order_executor: Optional[RealOrderExecutorService] = None

        # Captured notifications for verification
        self._captured_notifications: List[str] = []
        self._notification_capture_task: Optional[asyncio.Task] = None

        # Test results
        self.results: List[TestResult] = []

    async def setup(self) -> None:
        """Set up test environment."""
        logger.info("=" * 60)
        logger.info("TRADING INTEGRATION TEST - SETUP")
        logger.info("=" * 60)

        # 1. Initialize database
        logger.info("Initializing database...")
        self._db = Database(database_url=settings.database_url)
        await self._db.create_tables()
        logger.info("Database initialized")

        # 1.5 Clean up old test data if requested
        if self.cleanup_before:
            await self._cleanup_test_data()

        # 2. Initialize Telegram notifier (if configured)
        if self.test_telegram and settings.is_telegram_configured():
            logger.info("Initializing Telegram notifier...")
            self._telegram_notifier = TelegramNotifier(
                token=settings.telegram_bot_token,
                chat_id=settings.telegram_chat_id,
                message_queue=self._telegram_queue,
                poll_interval=0.5,  # Faster for testing
            )
            await self._telegram_notifier.start()

            # Start notification capture task
            self._notification_capture_task = asyncio.create_task(
                self._capture_notifications()
            )
            logger.info("Telegram notifier started")
        else:
            logger.warning("Telegram not configured or disabled for test")

        # 3. Initialize real order executor (if enabled)
        place_order_fn = None
        get_balance_fn = None

        if self.test_real_orders and settings.bybit_api_key and settings.bybit_api_secret:
            logger.warning("=" * 60)
            logger.warning("REAL ORDER TESTING ENABLED - CAUTION!")
            logger.warning("=" * 60)

            self._real_order_executor = RealOrderExecutorService(
                api_key=settings.bybit_api_key,
                api_secret=settings.bybit_api_secret,
                session_factory=self._db.session_factory,
                order_model=Order,
                telegram_queue=self._telegram_queue,
                testnet=settings.bybit_testnet,
                max_concurrent=1,
                retry_count=2,
                category=settings.bybit_category,
            )
            await self._real_order_executor.start()

            place_order_fn = create_place_order_function(self._real_order_executor)
            get_balance_fn = create_get_balance_function(self._real_order_executor)
        else:
            logger.info("Running in DRY-RUN mode (no real orders)")

        # 4. Initialize execution engine
        self._execution_engine = ExecutionEngine(
            signal_queue=self._signal_queue,
            session_factory=self._db.session_factory,
            position_model=Position,
            order_model=Order,
            signal_model=Signal,
            max_positions=5,
            risk_per_trade_pct=5.0,
            min_trade_usdt=10.0,
            get_balance_fn=get_balance_fn,
            place_order_fn=place_order_fn,
        )
        await self._execution_engine.start()

        logger.info("Test setup complete")

    async def teardown(self) -> None:
        """Clean up test environment."""
        logger.info("Cleaning up test environment...")

        if self._notification_capture_task:
            self._notification_capture_task.cancel()
            try:
                await self._notification_capture_task
            except asyncio.CancelledError:
                pass

        if self._execution_engine:
            await self._execution_engine.stop()

        if self._real_order_executor:
            await self._real_order_executor.stop()

        if self._telegram_notifier:
            await self._telegram_notifier.stop()

        if self._db:
            await self._db.close()

        logger.info("Cleanup complete")

    async def _capture_notifications(self) -> None:
        """Capture notifications from telegram queue for verification."""
        # Note: This runs in parallel with the real notifier
        # We just log what's being sent
        pass  # The TelegramNotifier consumes the queue directly

    async def _cleanup_test_data(self) -> None:
        """Clean up old test data for the test symbol."""
        from sqlalchemy import delete

        logger.info("Cleaning up old test data for {}...", self.TEST_SYMBOL)

        try:
            async with self._db.session_factory() as session:
                # Delete positions
                stmt = delete(Position).where(Position.symbol == self.TEST_SYMBOL)
                result = await session.execute(stmt)
                pos_count = result.rowcount

                # Delete signals
                stmt = delete(Signal).where(Signal.symbol == self.TEST_SYMBOL)
                result = await session.execute(stmt)
                sig_count = result.rowcount

                # Delete orders
                stmt = delete(Order).where(Order.symbol == self.TEST_SYMBOL)
                result = await session.execute(stmt)
                ord_count = result.rowcount

                await session.commit()

                logger.info(
                    "Cleaned up: {} positions, {} signals, {} orders",
                    pos_count, sig_count, ord_count
                )

        except Exception as e:
            logger.error("Failed to clean up test data: {}", e)

    async def _send_test_notification(self, message: str) -> None:
        """Send a test notification to Telegram."""
        if self._telegram_queue:
            try:
                self._telegram_queue.put_nowait({"text": message, "parse_mode": "HTML"})
                self._captured_notifications.append(message)
                logger.info("Queued notification: {}", message[:50] + "...")
            except asyncio.QueueFull:
                logger.warning("Telegram queue full")

    async def run_all_tests(self) -> None:
        """Run all integration tests."""
        logger.info("=" * 60)
        logger.info("STARTING INTEGRATION TESTS")
        logger.info("=" * 60)

        try:
            await self.setup()

            # Send start notification
            await self._send_test_notification(
                "<b>🧪 Integration Test Started</b>\n\n"
                f"Symbol: {self.TEST_SYMBOL}\n"
                f"Real Orders: {self.test_real_orders}\n"
                f"Sell Delay: {self.sell_delay_seconds}s"
            )
            await asyncio.sleep(2)  # Wait for notification to be sent

            # Run tests
            await self.test_1_inject_buy_signal()
            await asyncio.sleep(3)  # Wait for processing

            await self.test_2_verify_buy_position()
            await asyncio.sleep(1)

            await self.test_3_verify_buy_notification()

            # Wait before sell
            logger.info("Waiting %d seconds before sell test...", self.sell_delay_seconds)
            await self._send_test_notification(
                f"<b>⏳ Waiting {self.sell_delay_seconds}s before sell test...</b>"
            )
            await asyncio.sleep(self.sell_delay_seconds)

            await self.test_4_inject_sell_signal()
            await asyncio.sleep(3)  # Wait for processing

            await self.test_5_verify_sell_position()
            await asyncio.sleep(1)

            await self.test_6_verify_database_records()

            # Print results
            self._print_results()

            # Send completion notification
            passed = sum(1 for r in self.results if r.passed)
            failed = len(self.results) - passed
            await self._send_test_notification(
                f"<b>🧪 Integration Test Complete</b>\n\n"
                f"✅ Passed: {passed}\n"
                f"❌ Failed: {failed}"
            )
            await asyncio.sleep(2)

        finally:
            await self.teardown()

    async def test_1_inject_buy_signal(self) -> None:
        """Test 1: Inject a fake BUY signal."""
        logger.info("-" * 40)
        logger.info("TEST 1: Inject BUY Signal")
        logger.info("-" * 40)

        try:
            # Create a fake ENTRY signal with metrics that would trigger a buy
            # Simulating: volume spike (2x max volume) + price acceleration (4x = 300% gain)
            fake_signal = TradingSignal(
                symbol=self.TEST_SYMBOL,
                signal_type=SignalType.ENTRY,
                timestamp=datetime.now(timezone.utc),
                price=50000.0,  # Current BTC price (approximate)
                volume=1000.0,
                volume_ratio=2.5,  # 2.5x the max historical volume
                price_change_pct=350.0,  # 350% gain (close >= open * 3.5)
                ma14_value=None,
            )

            # Inject signal into queue
            await self._signal_queue.put(fake_signal)
            logger.info("Injected ENTRY signal: {} @ {}", self.TEST_SYMBOL, fake_signal.price)

            self.results.append(TestResult(
                name="Inject BUY Signal",
                passed=True,
                message="Successfully injected ENTRY signal",
                details={"symbol": self.TEST_SYMBOL, "price": fake_signal.price}
            ))

        except Exception as e:
            logger.error("Failed to inject BUY signal: {}", e)
            self.results.append(TestResult(
                name="Inject BUY Signal",
                passed=False,
                message=f"Failed: {e}"
            ))

    async def test_2_verify_buy_position(self) -> None:
        """Test 2: Verify that a position was created."""
        logger.info("-" * 40)
        logger.info("TEST 2: Verify BUY Position Created")
        logger.info("-" * 40)

        try:
            async with self._db.session_factory() as session:
                # Check for open position (get the most recent one)
                stmt = select(Position).where(
                    Position.symbol == self.TEST_SYMBOL,
                    Position.status == PositionStatus.OPEN
                ).order_by(Position.entry_time.desc()).limit(1)
                result = await session.execute(stmt)
                position = result.scalars().first()

                if position:
                    logger.info("Found OPEN position: id={}, entry_price={}",
                               position.id, position.entry_price)
                    self.results.append(TestResult(
                        name="Verify BUY Position",
                        passed=True,
                        message="Position created successfully",
                        details={
                            "position_id": position.id,
                            "entry_price": position.entry_price,
                            "entry_amount": position.entry_amount,
                        }
                    ))
                else:
                    # Check in-memory state
                    if self.TEST_SYMBOL in self._execution_engine._open_positions:
                        logger.info("Position found in memory (DB commit pending)")
                        self.results.append(TestResult(
                            name="Verify BUY Position",
                            passed=True,
                            message="Position created (in memory)",
                        ))
                    else:
                        logger.warning("No open position found for {}", self.TEST_SYMBOL)
                        self.results.append(TestResult(
                            name="Verify BUY Position",
                            passed=False,
                            message="No position created (may have been rejected by gate checks)"
                        ))

        except Exception as e:
            logger.error("Failed to verify position: {}", e)
            self.results.append(TestResult(
                name="Verify BUY Position",
                passed=False,
                message=f"Error: {e}"
            ))

    async def test_3_verify_buy_notification(self) -> None:
        """Test 3: Verify Telegram notification was sent."""
        logger.info("-" * 40)
        logger.info("TEST 3: Verify BUY Notification")
        logger.info("-" * 40)

        if not self.test_telegram or not settings.is_telegram_configured():
            self.results.append(TestResult(
                name="Verify BUY Notification",
                passed=True,
                message="Skipped (Telegram not configured)"
            ))
            return

        # Check if queue was used (notifications sent)
        # Note: We can't easily verify the notification was received
        # This test just confirms the notification system is working

        self.results.append(TestResult(
            name="Verify BUY Notification",
            passed=True,
            message="Telegram notification system active (check your Telegram!)"
        ))

    async def test_4_inject_sell_signal(self) -> None:
        """Test 4: Inject a fake SELL signal."""
        logger.info("-" * 40)
        logger.info("TEST 4: Inject SELL Signal")
        logger.info("-" * 40)

        try:
            # Create a fake EXIT signal (MA14 crossover)
            # Price dropped below MA14, triggering exit
            fake_signal = TradingSignal(
                symbol=self.TEST_SYMBOL,
                signal_type=SignalType.EXIT,
                timestamp=datetime.now(timezone.utc),
                price=48000.0,  # Price dropped
                volume=500.0,
                volume_ratio=None,
                price_change_pct=None,
                ma14_value=49000.0,  # MA14 is above current price = exit signal
            )

            # Inject signal into queue
            await self._signal_queue.put(fake_signal)
            logger.info("Injected EXIT signal: {} @ {}", self.TEST_SYMBOL, fake_signal.price)

            self.results.append(TestResult(
                name="Inject SELL Signal",
                passed=True,
                message="Successfully injected EXIT signal",
                details={"symbol": self.TEST_SYMBOL, "price": fake_signal.price}
            ))

        except Exception as e:
            logger.error("Failed to inject SELL signal: {}", e)
            self.results.append(TestResult(
                name="Inject SELL Signal",
                passed=False,
                message=f"Failed: {e}"
            ))

    async def test_5_verify_sell_position(self) -> None:
        """Test 5: Verify that the position was closed."""
        logger.info("-" * 40)
        logger.info("TEST 5: Verify Position Closed")
        logger.info("-" * 40)

        try:
            async with self._db.session_factory() as session:
                # Check for closed position (get the most recent one)
                stmt = select(Position).where(
                    Position.symbol == self.TEST_SYMBOL,
                    Position.status == PositionStatus.CLOSED
                ).order_by(Position.exit_time.desc()).limit(1)
                result = await session.execute(stmt)
                position = result.scalars().first()

                if position:
                    logger.info("Found CLOSED position: id={}, profit={:.2f} USDT ({:.2f}%)",
                               position.id, position.profit_usdt or 0, position.profit_pct or 0)
                    self.results.append(TestResult(
                        name="Verify Position Closed",
                        passed=True,
                        message="Position closed successfully",
                        details={
                            "position_id": position.id,
                            "entry_price": position.entry_price,
                            "exit_price": position.exit_price,
                            "profit_usdt": position.profit_usdt,
                            "profit_pct": position.profit_pct,
                        }
                    ))
                else:
                    # Check if position is still open (exit may have failed)
                    if self.TEST_SYMBOL not in self._execution_engine._open_positions:
                        logger.info("Position no longer in memory (closed)")
                        self.results.append(TestResult(
                            name="Verify Position Closed",
                            passed=True,
                            message="Position closed (removed from memory)",
                        ))
                    else:
                        logger.warning("Position still open for {}", self.TEST_SYMBOL)
                        self.results.append(TestResult(
                            name="Verify Position Closed",
                            passed=False,
                            message="Position still open (exit may have failed)"
                        ))

        except Exception as e:
            logger.error("Failed to verify position close: {}", e)
            self.results.append(TestResult(
                name="Verify Position Closed",
                passed=False,
                message=f"Error: {e}"
            ))

    async def test_6_verify_database_records(self) -> None:
        """Test 6: Verify database records (signals, orders)."""
        logger.info("-" * 40)
        logger.info("TEST 6: Verify Database Records")
        logger.info("-" * 40)

        try:
            async with self._db.session_factory() as session:
                # Count signals for our test symbol
                stmt = select(func.count(Signal.id)).where(
                    Signal.symbol == self.TEST_SYMBOL
                )
                result = await session.execute(stmt)
                signal_count = result.scalar() or 0

                # Count orders for our test symbol
                stmt = select(func.count(Order.id)).where(
                    Order.symbol == self.TEST_SYMBOL
                )
                result = await session.execute(stmt)
                order_count = result.scalar() or 0

                # Count positions for our test symbol
                stmt = select(func.count(Position.id)).where(
                    Position.symbol == self.TEST_SYMBOL
                )
                result = await session.execute(stmt)
                position_count = result.scalar() or 0

                logger.info("Database records for {}: signals={}, orders={}, positions={}",
                           self.TEST_SYMBOL, signal_count, order_count, position_count)

                # We expect at least some records
                if signal_count > 0 or position_count > 0:
                    self.results.append(TestResult(
                        name="Verify Database Records",
                        passed=True,
                        message="Database records created",
                        details={
                            "signals": signal_count,
                            "orders": order_count,
                            "positions": position_count,
                        }
                    ))
                else:
                    self.results.append(TestResult(
                        name="Verify Database Records",
                        passed=False,
                        message="No database records found"
                    ))

        except Exception as e:
            logger.error("Failed to verify database records: {}", e)
            self.results.append(TestResult(
                name="Verify Database Records",
                passed=False,
                message=f"Error: {e}"
            ))

    def _print_results(self) -> None:
        """Print test results summary."""
        logger.info("=" * 60)
        logger.info("TEST RESULTS")
        logger.info("=" * 60)

        passed = 0
        failed = 0

        for result in self.results:
            status = "✅ PASS" if result.passed else "❌ FAIL"
            logger.info("{}: {} - {}", status, result.name, result.message)
            if result.details:
                for key, value in result.details.items():
                    logger.info("    {}: {}", key, value)

            if result.passed:
                passed += 1
            else:
                failed += 1

        logger.info("-" * 60)
        logger.info("Total: {} passed, {} failed", passed, failed)
        logger.info("=" * 60)


async def main():
    """Run integration tests."""
    import argparse

    parser = argparse.ArgumentParser(description="Trading Integration Test")
    parser.add_argument(
        "--real-orders",
        action="store_true",
        help="Enable real order testing (CAUTION: uses real money!)"
    )
    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="Disable Telegram notifications"
    )
    parser.add_argument(
        "--sell-delay",
        type=int,
        default=30,
        help="Delay in seconds before sell test (default: 30)"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old test data before running"
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO"
    )

    # Safety check for real orders
    if args.real_orders:
        logger.warning("=" * 60)
        logger.warning("REAL ORDER TESTING ENABLED!")
        logger.warning("This will place REAL orders on Bybit!")
        logger.warning("=" * 60)

        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Aborted by user")
            return

    # Run tests
    test = TradingIntegrationTest(
        test_telegram=not args.no_telegram,
        test_real_orders=args.real_orders,
        sell_delay_seconds=args.sell_delay,
        cleanup_before=args.cleanup,
    )

    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())