"""MarketUniverseService: Manages the list of tradeable tokens.

Responsibilities:
1. Fetch coins from CoinMarketCap API
2. Filter by market_cap > MIN_MARKET_CAP_USD (default: $1,000,000)
3. Map coins to Bybit spot pairs (BTC -> BTCUSDT)
4. Maintain list of "allowed" symbols in database
5. Refresh periodically (default: every hour)
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set

import httpx
from loguru import logger

from ...config.config import Settings
from ..db.repository import Repository
from ..db.models import Token


class MarketUniverseService:
    """
    Service for maintaining the tradeable token universe.

    Combines data from CoinMarketCap (market cap) and Bybit (available pairs)
    to determine which symbols the bot should monitor and trade.
    """

    # Bybit API endpoints
    BYBIT_SPOT_SYMBOLS_URL = "https://api.bybit.com/v5/market/instruments-info"

    # CoinMarketCap API endpoints
    CMC_LISTINGS_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    def __init__(
        self,
        settings: Settings,
        repository: Repository,
    ):
        """
        Initialize MarketUniverseService.

        Args:
            settings: Application settings
            repository: Database repository
        """
        self.settings = settings
        self.repository = repository
        self.log = logger.bind(component="MarketUniverseService")

        # Cache of active symbols
        self._active_symbols: Set[str] = set()
        self._bybit_pairs: Dict[str, str] = {}  # symbol -> bybit_symbol (BTC -> BTCUSDT)

        # Background task
        self._refresh_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the service and begin periodic refresh."""
        self.log.info("Starting MarketUniverseService")

        # Initial load
        await self._refresh_universe()

        # Start periodic refresh
        self._refresh_task = asyncio.create_task(self._periodic_refresh())
        self.log.info(
            f"Started periodic refresh (interval: {self.settings.universe_refresh_interval}s)"
        )

    async def stop(self) -> None:
        """Stop the service."""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        self.log.info("MarketUniverseService stopped")

    async def _periodic_refresh(self) -> None:
        """Background task to refresh universe periodically."""
        while True:
            try:
                await asyncio.sleep(self.settings.universe_refresh_interval)
                await self._refresh_universe()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Error in periodic refresh: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def _refresh_universe(self) -> None:
        """
        Refresh the token universe.

        Steps:
        1. Fetch available Bybit spot pairs
        2. Fetch CoinMarketCap listings with market cap
        3. Filter by market cap threshold
        4. Match CMC symbols to Bybit pairs
        5. Update database
        """
        self.log.info("Refreshing token universe")

        try:
            # Step 1: Get Bybit spot pairs
            bybit_pairs = await self._fetch_bybit_pairs()
            if not bybit_pairs:
                self.log.error("Failed to fetch Bybit pairs")
                return

            # Step 2: Get CoinMarketCap listings
            cmc_listings = await self._fetch_cmc_listings()
            if not cmc_listings:
                self.log.warning("Failed to fetch CMC listings, using Bybit data only")
                cmc_listings = []

            # Step 3 & 4: Filter and match
            tokens = self._process_listings(bybit_pairs, cmc_listings)

            # Step 5: Update database
            await self._update_database(tokens)

            # Update cache
            self._active_symbols = {t["bybit_symbol"] for t in tokens if t["is_active"]}
            self._bybit_pairs = bybit_pairs

            self.log.info(f"Universe refreshed: {len(self._active_symbols)} active symbols")

        except Exception as e:
            self.log.exception(f"Error refreshing universe: {e}")

    async def _fetch_bybit_pairs(self) -> Dict[str, str]:
        """
        Fetch available USDT spot pairs from Bybit.

        Returns:
            Dict mapping base symbol to Bybit pair name (BTC -> BTCUSDT)
        """
        # TODO: Implement actual API call
        # This is a stub that should call Bybit's instruments-info endpoint
        #
        # async with httpx.AsyncClient() as client:
        #     response = await client.get(
        #         self.BYBIT_SPOT_SYMBOLS_URL,
        #         params={"category": "spot"}
        #     )
        #     data = response.json()
        #     # Parse response and return {symbol: bybit_symbol} dict
        #     # Filter for USDT pairs only

        self.log.debug("Fetching Bybit spot pairs (stub)")

        # Placeholder: return empty dict, will be implemented
        return {}

    async def _fetch_cmc_listings(self) -> List[Dict]:
        """
        Fetch cryptocurrency listings from CoinMarketCap.

        Returns:
            List of coin data with market_cap, symbol, name
        """
        # TODO: Implement actual API call
        # This is a stub that should call CMC's listings endpoint
        #
        # if not self.settings.cmc_api_key:
        #     self.log.warning("CMC API key not configured")
        #     return []
        #
        # async with httpx.AsyncClient() as client:
        #     response = await client.get(
        #         self.CMC_LISTINGS_URL,
        #         headers={"X-CMC_PRO_API_KEY": self.settings.cmc_api_key},
        #         params={"limit": 500, "convert": "USD"}
        #     )
        #     data = response.json()
        #     return data.get("data", [])

        self.log.debug("Fetching CMC listings (stub)")

        # Placeholder: return empty list, will be implemented
        return []

    def _process_listings(
        self,
        bybit_pairs: Dict[str, str],
        cmc_listings: List[Dict],
    ) -> List[Dict]:
        """
        Process and filter token listings.

        Args:
            bybit_pairs: Available Bybit USDT pairs
            cmc_listings: CoinMarketCap listings with market cap

        Returns:
            List of token dicts ready for database insertion
        """
        # TODO: Implement filtering logic
        #
        # 1. Create market cap lookup from CMC data
        # 2. For each Bybit pair:
        #    - Get market cap from CMC (if available)
        #    - Determine if active (market_cap >= min_market_cap_usd)
        # 3. Return list of token dicts

        self.log.debug("Processing listings (stub)")

        # Placeholder: return empty list, will be implemented
        return []

    async def _update_database(self, tokens: List[Dict]) -> None:
        """
        Update token records in database.

        Uses upsert to insert new tokens or update existing ones.
        """
        # TODO: Implement database update
        #
        # for token in tokens:
        #     await self.repository.upsert(
        #         Token,
        #         conflict_keys={"symbol": token["symbol"]},
        #         update_values={
        #             "bybit_symbol": token["bybit_symbol"],
        #             "name": token.get("name"),
        #             "market_cap_usd": token.get("market_cap_usd"),
        #             "is_active": token["is_active"],
        #             "last_updated": datetime.utcnow(),
        #         }
        #     )

        self.log.debug(f"Updating database with {len(tokens)} tokens (stub)")

    # =========================================================================
    # Public API
    # =========================================================================

    def get_active_symbols(self) -> Set[str]:
        """
        Get set of currently active Bybit symbols.

        Returns:
            Set of symbol strings (e.g., {"BTCUSDT", "ETHUSDT", ...})
        """
        return self._active_symbols.copy()

    def is_symbol_active(self, symbol: str) -> bool:
        """
        Check if a symbol is currently active for trading.

        Args:
            symbol: Bybit symbol (e.g., "BTCUSDT")

        Returns:
            True if symbol is active
        """
        return symbol in self._active_symbols

    async def get_active_tokens(self) -> List[Token]:
        """
        Get all active tokens from database.

        Returns:
            List of Token model instances
        """
        return await self.repository.get_all(Token, filters={"is_active": True})

    async def deactivate_symbol(self, symbol: str) -> None:
        """
        Manually deactivate a symbol (e.g., due to trading issues).

        Args:
            symbol: Bybit symbol to deactivate
        """
        await self.repository.update(
            Token,
            filter_by={"bybit_symbol": symbol},
            update_values={"is_active": False},
        )
        self._active_symbols.discard(symbol)
        self.log.warning(f"Symbol {symbol} deactivated")

    async def activate_symbol(self, symbol: str) -> None:
        """
        Manually activate a symbol.

        Args:
            symbol: Bybit symbol to activate
        """
        await self.repository.update(
            Token,
            filter_by={"bybit_symbol": symbol},
            update_values={"is_active": True},
        )
        self._active_symbols.add(symbol)
        self.log.info(f"Symbol {symbol} activated")
