"""
StalePrice Checker Service.

Periodically checks tokens for stale/inactive prices and enables/disables them.
Runs every 50 minutes, separate from the daily token sync.

Only affects tokens that passed the main 4 filters (Blacklist, ST, LowMcap, New).
Cannot re-enable tokens that failed those filters.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Set

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from services.bybit_client import BybitClient
from db.models import Token, AllToken


class StalePriceChecker:
    """
    Service to check and update token stale price status.

    Runs every 50 minutes (configurable).
    - Checks all tokens in `tokens` table for stale prices
    - If stale: removes from `tokens`, updates `all_tokens.deactivation_reason = "StalePrice"`
    - If not stale and was StalePrice: adds back to `tokens`, clears reason in `all_tokens`
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
            "Stale price checker started (interval: %d min, lookback: %d min, consecutive: %d)",
            self._interval_minutes,
            self._lookback_minutes,
            self._consecutive_stale,
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
        # Initial delay to let other services start
        await asyncio.sleep(60)

        while not self._stop_event.is_set():
            try:
                await self.check_and_update()
            except Exception as e:
                self._log.error("Error in stale price check: %s", e)

            # Wait for next check
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._interval_minutes * 60
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Time for next check

    async def check_and_update(self) -> dict:
        """
        Check all tradable tokens for stale prices and update accordingly.

        Returns:
            Dict with stats: {"checked", "disabled", "enabled"}
        """
        self._log.info("Starting stale price check...")
        start_time = datetime.now()

        stats = {"checked": 0, "disabled": 0, "enabled": 0}

        async with self._session_factory() as session:
            # Get all currently tradable tokens
            from sqlalchemy import select
            result = await session.execute(select(Token))
            tradable_tokens = result.scalars().all()

            if not tradable_tokens:
                self._log.info("No tradable tokens to check")
                return stats

            symbols = [t.bybit_symbol for t in tradable_tokens]
            stats["checked"] = len(symbols)

            # Check for stale prices via Bybit API
            async with BybitClient(logger=self._log) as bybit:
                stale_symbols = await bybit.check_stale_prices(
                    category=self._category,
                    symbols=symbols,
                    lookback_minutes=self._lookback_minutes,
                    consecutive_stale=self._consecutive_stale,
                )

            # Disable tokens with stale prices
            for token in tradable_tokens:
                if token.bybit_symbol in stale_symbols:
                    # Remove from tokens table
                    await session.execute(
                        update(Token.__table__)
                        .where(Token.symbol == token.symbol)
                        .values()  # Will be deleted below
                    )
                    # Actually delete from tokens
                    from sqlalchemy import delete as sql_delete
                    await session.execute(
                        sql_delete(Token).where(Token.symbol == token.symbol)
                    )

                    # Update all_tokens with reason
                    await session.execute(
                        update(AllToken)
                        .where(AllToken.symbol == token.symbol)
                        .values(
                            is_active=False,
                            deactivation_reason="StalePrice"
                        )
                    )
                    stats["disabled"] += 1
                    self._log.info("Disabled token %s: stale price", token.symbol)

            # Check if any StalePrice tokens are now active
            result = await session.execute(
                select(AllToken).where(
                    AllToken.deactivation_reason == "StalePrice"
                )
            )
            stale_tokens = result.scalars().all()

            for token in stale_tokens:
                if token.bybit_symbol not in stale_symbols:
                    # Token is no longer stale - re-enable it
                    # First check it's not in tokens already
                    existing = await session.execute(
                        select(Token).where(Token.symbol == token.symbol)
                    )
                    if existing.scalar_one_or_none() is None:
                        # Add back to tokens table
                        new_token = Token(
                            symbol=token.symbol,
                            bybit_symbol=token.bybit_symbol,
                            name=token.name,
                            market_cap_usd=token.market_cap_usd,
                            bybit_categories=token.bybit_categories,
                            max_market_qty=token.max_market_qty,
                        )
                        session.add(new_token)

                    # Clear reason in all_tokens
                    await session.execute(
                        update(AllToken)
                        .where(AllToken.symbol == token.symbol)
                        .values(
                            is_active=True,
                            deactivation_reason=None
                        )
                    )
                    stats["enabled"] += 1
                    self._log.info("Re-enabled token %s: no longer stale", token.symbol)

            await session.commit()

        duration = (datetime.now() - start_time).total_seconds()
        self._log.info(
            "Stale price check completed in %.1fs: checked=%d, disabled=%d, enabled=%d",
            duration, stats["checked"], stats["disabled"], stats["enabled"]
        )

        return stats
