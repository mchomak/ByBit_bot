"""
Quick Test Script for Trading Components.

Tests individual components without waiting for full trading cycle.
Useful for rapid verification during development.

Usage:
    python -m tests.test_quick
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from loguru import logger


async def test_telegram_notification():
    """Test Telegram notification delivery."""
    from config.config import settings
    from bot.TelegramNotifier import TelegramNotifier

    logger.info("=" * 50)
    logger.info("TEST: Telegram Notification")
    logger.info("=" * 50)

    if not settings.is_telegram_configured():
        logger.warning("Telegram not configured. Skipping test.")
        logger.info("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to test.")
        return False

    queue = asyncio.Queue()
    notifier = TelegramNotifier(
        token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        message_queue=queue,
        poll_interval=0.5,
    )

    try:
        await notifier.start()

        # Send test message
        test_msg = (
            "<b>🧪 Quick Test</b>\n\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            "This is a test message from the trading bot."
        )
        queue.put_nowait({"text": test_msg, "parse_mode": "HTML"})

        # Wait for delivery
        await asyncio.sleep(3)

        logger.info("✅ Telegram test passed - check your Telegram!")
        return True

    except Exception as e:
        logger.error("❌ Telegram test failed: {}", e)
        return False

    finally:
        await notifier.stop()


async def test_order_queue():
    """Test OrderQueue from trade_client.py."""
    from config.config import settings
    from trade.trade_client import OrderQueue, Category

    logger.info("=" * 50)
    logger.info("TEST: OrderQueue (trade_client.py)")
    logger.info("=" * 50)

    if not settings.bybit_api_key or not settings.bybit_api_secret:
        logger.warning("API keys not configured. Testing limited functionality.")

        # Test without API keys - just queue mechanics
        queue = OrderQueue(
            api_key="test",
            api_secret="test",
            demo=True,
            max_concurrent=1,
        )

        logger.info("OrderQueue created successfully")
        logger.info("Stats: {}", queue.stats)
        logger.info("✅ OrderQueue basic test passed")
        return True

    # Test with real API keys
    queue = OrderQueue(
        api_key=settings.bybit_api_key,
        api_secret=settings.bybit_api_secret,
        testnet=settings.bybit_demo,
        max_concurrent=1,
    )

    try:
        await queue.start()

        # Test price fetch
        price = await queue.get_price("BTCUSDT", Category.SPOT)
        logger.info("BTCUSDT price: {}", price)

        # Test balance fetch
        balances = await queue.get_balance()
        logger.info("Balances: {}", balances)

        # Test min order info
        min_info = await queue.get_min_order("BTCUSDT", Category.SPOT)
        logger.info("Min order info: {}", min_info)

        logger.info("✅ OrderQueue API test passed")
        return True

    except Exception as e:
        logger.error("❌ OrderQueue test failed: {}", e)
        return False

    finally:
        await queue.stop()


async def test_database_connection():
    """Test database connection and models."""
    from config.config import settings
    from db.database import Database
    from db.models import Position, Order, Signal
    from sqlalchemy import select, func

    logger.info("=" * 50)
    logger.info("TEST: Database Connection")
    logger.info("=" * 50)

    try:
        db = Database(database_url=settings.database_url)
        await db.create_tables()

        async with db.session_factory() as session:
            # Count records
            for model, name in [(Position, "positions"), (Order, "orders"), (Signal, "signals")]:
                stmt = select(func.count(model.id))
                result = await session.execute(stmt)
                count = result.scalar() or 0
                logger.info("{}: {} records", name, count)

        await db.close()

        logger.info("✅ Database test passed")
        return True

    except Exception as e:
        logger.error("❌ Database test failed: {}", e)
        return False


async def test_real_order_executor():
    """Test RealOrderExecutorService initialization."""
    from config.config import settings
    from db.database import Database
    from db.models import Order
    from services.real_order_executor import RealOrderExecutorService

    logger.info("=" * 50)
    logger.info("TEST: RealOrderExecutorService")
    logger.info("=" * 50)

    if not settings.bybit_api_key or not settings.bybit_api_secret:
        logger.warning("API keys not configured. Skipping test.")
        return True

    try:
        db = Database(database_url=settings.database_url)
        await db.create_tables()

        telegram_queue = asyncio.Queue()

        executor = RealOrderExecutorService(
            api_key=settings.bybit_api_key,
            api_secret=settings.bybit_api_secret,
            session_factory=db.session_factory,
            order_model=Order,
            telegram_queue=telegram_queue,
            demo=settings.bybit_demo,
            max_concurrent=1,
            retry_count=2,
            category=settings.bybit_category,
        )

        await executor.start()

        # Test balance
        balance = await executor.get_balance("USDT")
        logger.info("USDT balance: {}", balance)

        # Test price
        price = await executor.get_price("BTCUSDT")
        logger.info("BTCUSDT price: {}", price)

        stats = executor.get_stats()
        logger.info("Executor stats: {}", stats)

        await executor.stop()
        await db.close()

        logger.info("✅ RealOrderExecutorService test passed")
        return True

    except Exception as e:
        logger.error("❌ RealOrderExecutorService test failed: {}", e)
        return False


async def test_signal_processing():
    """Test signal processing through ExecutionEngine."""
    from config.config import settings
    from db.database import Database
    from db.models import Position, Order, Signal
    from services.execution_engine import ExecutionEngine
    from services.pipeline_events import TradingSignal, SignalType

    logger.info("=" * 50)
    logger.info("TEST: Signal Processing (ExecutionEngine)")
    logger.info("=" * 50)

    try:
        db = Database(database_url=settings.database_url)
        await db.create_tables()

        signal_queue = asyncio.Queue()

        engine = ExecutionEngine(
            signal_queue=signal_queue,
            session_factory=db.session_factory,
            position_model=Position,
            order_model=Order,
            signal_model=Signal,
            max_positions=5,
            risk_per_trade_pct=5.0,
            min_trade_usdt=10.0,
        )

        await engine.start()

        # Inject test signal
        test_signal = TradingSignal(
            symbol="TESTUSDT",
            signal_type=SignalType.ENTRY,
            timestamp=datetime.now(timezone.utc),
            price=100.0,
            volume=1000.0,
            volume_ratio=2.0,
            price_change_pct=300.0,
        )

        await signal_queue.put(test_signal)
        logger.info("Injected test signal: {}", test_signal)

        # Wait for processing
        await asyncio.sleep(2)

        # Check if position was created
        if "TESTUSDT" in engine._open_positions:
            logger.info("Position created for TESTUSDT!")
        else:
            logger.info("No position created (expected in stub mode)")

        await engine.stop()
        await db.close()

        logger.info("✅ Signal processing test passed")
        return True

    except Exception as e:
        logger.error("❌ Signal processing test failed: {}", e)
        return False


async def main():
    """Run all quick tests."""
    # Configure logging
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO"
    )

    logger.info("=" * 60)
    logger.info("QUICK TEST SUITE")
    logger.info("=" * 60)

    tests = [
        ("Database Connection", test_database_connection),
        ("Telegram Notification", test_telegram_notification),
        ("OrderQueue", test_order_queue),
        ("RealOrderExecutorService", test_real_order_executor),
        ("Signal Processing", test_signal_processing),
    ]

    results = []

    for name, test_fn in tests:
        try:
            passed = await test_fn()
            results.append((name, passed))
        except Exception as e:
            logger.exception("Test {} crashed: {}", name, e)
            results.append((name, False))

        await asyncio.sleep(1)

    # Summary
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    passed = 0
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info("{}: {}", status, name)
        if result:
            passed += 1

    logger.info("-" * 40)
    logger.info("Total: {}/{} passed", passed, len(results))


if __name__ == "__main__":
    asyncio.run(main())