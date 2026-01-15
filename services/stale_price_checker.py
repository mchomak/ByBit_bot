"""
StalePrice Checker Service.

Periodically checks tokens for stale/inactive prices and enables/disables them.
Runs every 50 minutes, separate from the daily token sync.

Only works with the `tokens` table:
- Sets is_active=False for tokens with stale prices
- Sets is_active=True when prices become active again
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

from services.bybit_client import BybitClient
from db.models import Token


class StalePriceChecker:
    """
    Service to check and update token stale price status.

    Runs every 50 minutes (configurable).
    Only modifies is_active column in tokens table.
    """

    def __init__(
        self,
        session_factory: sessionmaker,
        bybit_category: str = "spot",
        check_interval_minutes: int = 50,
        lookback_minutes: int = 50,
        consecutive_stale_candles: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the stale price checker.

        Args:
            session_factory: SQLAlchemy async session factory
            bybit_category: Bybit category to check (spot, linear)
            check_interval_minutes: How often to check (default 50 min)
            lookback_minutes: How many minutes of history to check
            consecutive_stale_candles: Number of flat candles to consider stale
            logger: Logger instance
        """
        self._session_factory = session_factory
        self._category = bybit_category
        self._interval_minutes = check_interval_minutes
        self._lookback_minutes = lookback_minutes
        self._consecutive_stale = consecutive_stale_candles
        self._log = logger or logging.getLogger(self.__class__.__name__)

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the stale price checker service."""
        if self._task is not None:
            self._log.warning("Stale price checker already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="stale-price-checker")
        self._log.info(
            "Stale price checker started (interval: {} min, lookback: {} min, consecutive: {})".format(
                self._interval_minutes,
                self._lookback_minutes,
                self._consecutive_stale,
            )
        )

    async def stop(self) -> None:
        """Stop the stale price checker service."""
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._log.info("Stale price checker stopped")

    async def _run_loop(self) -> None:
        """Main loop - check every N minutes."""
        # Run first check immediately on startup
        try:
            await self.check_and_update()
        except Exception as e:
            self._log.error("Error in initial stale price check: {}".format(e))

        while not self._stop_event.is_set():
            # Wait for next check
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._interval_minutes * 60
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Time for next check

            try:
                await self.check_and_update()
            except Exception as e:
                self._log.error("Error in stale price check: {}".format(e))

    async def check_and_update(self) -> dict:
        """
        Check all tokens for stale prices and update is_active accordingly.

        Returns:
            Dict with stats: {"checked", "disabled", "enabled"}
        """
        self._log.info("Starting stale price check...")
        start_time = datetime.now()

        stats = {"checked": 0, "disabled": 0, "enabled": 0}

        async with self._session_factory() as session:
            # Get all tokens from tokens table
            result = await session.execute(select(Token))
            all_tokens = result.scalars().all()

            if not all_tokens:
                self._log.info("No tokens to check")
                return stats

            symbols = [t.bybit_symbol for t in all_tokens]
            stats["checked"] = len(symbols)

            # Check for stale prices via Bybit API
            async with BybitClient(logger=self._log) as bybit:
                stale_symbols = await bybit.check_stale_prices(
                    category=self._category,
                    symbols=symbols,
                    lookback_minutes=self._lookback_minutes,
                    consecutive_stale=self._consecutive_stale,
                )

            # Update is_active based on stale check
            for token in all_tokens:
                is_stale = token.bybit_symbol in stale_symbols

                if is_stale and token.is_active:
                    # Disable stale token
                    await session.execute(
                        update(Token)
                        .where(Token.symbol == token.symbol)
                        .values(is_active=False)
                    )
                    stats["disabled"] += 1
                    self._log.info("Disabled token {}: stale price".format(token.symbol))

                elif not is_stale and not token.is_active:
                    # Re-enable token that is no longer stale
                    await session.execute(
                        update(Token)
                        .where(Token.symbol == token.symbol)
                        .values(is_active=True)
                    )
                    stats["enabled"] += 1
                    self._log.info("Re-enabled token {}: price active again".format(token.symbol))

            await session.commit()

        duration = (datetime.now() - start_time).total_seconds()
        self._log.info(
            "Stale price check completed in {:.1f}s: checked={}, disabled={}, enabled={}".format(
                duration, stats["checked"], stats["disabled"], stats["enabled"]
            )
        )

        return stats
