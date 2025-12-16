"""BybitMarketDataService: Real-time market data via WebSocket.

Responsibilities:
1. REST: Fetch historical 1m candles for initialization (5 days)
2. WebSocket: Subscribe to kline.1 for all active symbols
3. Normalize candles (OHLCV, turnover, confirm flag)
4. Write candles to database
5. Send candle events to MarketDataQueue for strategy
6. Handle reconnection on disconnect
7. Clean old candles (> 5 days) periodically
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import httpx
import websockets
from loguru import logger

from ..config.settings import Settings
from ..db.repository import Repository
from ..db.models import Candle1m
from ..queues.event_bus import MarketDataQueue


class BybitMarketDataService:
    """
    Service for real-time market data collection from Bybit.

    Uses WebSocket for live updates and REST for historical backfill.
    Updates candles every second and sends events to strategy.
    """

    # Bybit API endpoints
    BYBIT_WS_PUBLIC = "wss://stream.bybit.com/v5/public/spot"
    BYBIT_KLINE_REST = "https://api.bybit.com/v5/market/kline"

    # Max symbols per WebSocket connection (Bybit limit)
    MAX_SYMBOLS_PER_WS = 10

    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        market_data_queue: MarketDataQueue,
    ):
        """
        Initialize BybitMarketDataService.

        Args:
            settings: Application settings
            repository: Database repository
            market_data_queue: Queue for sending candle events
        """
        self.settings = settings
        self.repository = repository
        self.queue = market_data_queue
        self.log = logger.bind(component="BybitMarketDataService")

        # Active subscriptions
        self._subscribed_symbols: Set[str] = set()

        # WebSocket connections
        self._ws_connections: List[websockets.WebSocketClientProtocol] = []
        self._ws_tasks: List[asyncio.Task] = []

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None

        # Current candle state (for real-time updates)
        # {symbol: {timestamp, open, high, low, close, volume}}
        self._current_candles: Dict[str, Dict[str, Any]] = {}

        # Running flag
        self._running = False

    async def start(self, symbols: Set[str]) -> None:
        """
        Start the market data service.

        Args:
            symbols: Set of Bybit symbols to subscribe to
        """
        self.log.info(f"Starting BybitMarketDataService with {len(symbols)} symbols")
        self._running = True

        # Fetch historical data first
        await self._fetch_historical_data(symbols)

        # Start WebSocket connections
        await self._connect_websockets(symbols)

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

        self.log.info("BybitMarketDataService started")

    async def stop(self) -> None:
        """Stop the service and close all connections."""
        self._running = False

        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket connections
        for ws in self._ws_connections:
            await ws.close()

        # Cancel WebSocket tasks
        for task in self._ws_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.log.info("BybitMarketDataService stopped")

    async def add_symbols(self, symbols: Set[str]) -> None:
        """
        Add new symbols to monitor.

        Args:
            symbols: New symbols to subscribe to
        """
        new_symbols = symbols - self._subscribed_symbols
        if not new_symbols:
            return

        # Fetch historical data for new symbols
        await self._fetch_historical_data(new_symbols)

        # Subscribe via WebSocket
        await self._subscribe_symbols(new_symbols)

        self._subscribed_symbols.update(new_symbols)
        self.log.info(f"Added {len(new_symbols)} new symbols")

    async def remove_symbols(self, symbols: Set[str]) -> None:
        """
        Remove symbols from monitoring.

        Args:
            symbols: Symbols to unsubscribe from
        """
        # TODO: Implement unsubscribe logic
        self._subscribed_symbols -= symbols
        self.log.info(f"Removed {len(symbols)} symbols")

    # =========================================================================
    # Historical Data (REST)
    # =========================================================================

    async def _fetch_historical_data(self, symbols: Set[str]) -> None:
        """
        Fetch historical 1m candles for symbols via REST API.

        Fetches 5 days of history for each symbol.
        """
        self.log.info(f"Fetching historical data for {len(symbols)} symbols")

        # Calculate time range (5 days)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.settings.candle_history_days)

        # Fetch in parallel with rate limiting
        semaphore = asyncio.Semaphore(5)  # Max 5 concurrent requests

        async def fetch_symbol(symbol: str) -> None:
            async with semaphore:
                await self._fetch_symbol_history(symbol, start_time, end_time)

        tasks = [fetch_symbol(symbol) for symbol in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)

        self.log.info("Historical data fetch completed")

    async def _fetch_symbol_history(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """
        Fetch historical candles for a single symbol.

        Args:
            symbol: Bybit symbol (e.g., "BTCUSDT")
            start_time: Start of time range
            end_time: End of time range
        """
        # TODO: Implement actual API call
        # This is a stub that should:
        # 1. Call Bybit kline endpoint with pagination
        # 2. Parse response into candle dicts
        # 3. Upsert candles to database
        #
        # async with httpx.AsyncClient() as client:
        #     start_ms = int(start_time.timestamp() * 1000)
        #     end_ms = int(end_time.timestamp() * 1000)
        #
        #     response = await client.get(
        #         self.BYBIT_KLINE_REST,
        #         params={
        #             "category": "spot",
        #             "symbol": symbol,
        #             "interval": "1",  # 1 minute
        #             "start": start_ms,
        #             "end": end_ms,
        #             "limit": 1000,  # Max per request
        #         }
        #     )
        #     data = response.json()
        #     candles = data.get("result", {}).get("list", [])
        #
        #     for candle in candles:
        #         await self._save_candle(symbol, candle)

        self.log.debug(f"Fetching history for {symbol} (stub)")

    # =========================================================================
    # WebSocket (Real-time)
    # =========================================================================

    async def _connect_websockets(self, symbols: Set[str]) -> None:
        """
        Establish WebSocket connections for all symbols.

        Bybit limits subscriptions per connection, so we create multiple
        connections if needed.
        """
        self.log.info("Connecting to WebSocket")

        # Split symbols into groups
        symbol_list = list(symbols)
        groups = [
            symbol_list[i:i + self.MAX_SYMBOLS_PER_WS]
            for i in range(0, len(symbol_list), self.MAX_SYMBOLS_PER_WS)
        ]

        # Create connection for each group
        for i, group in enumerate(groups):
            task = asyncio.create_task(self._ws_handler(group, i))
            self._ws_tasks.append(task)

    async def _ws_handler(self, symbols: List[str], connection_id: int) -> None:
        """
        Handle a single WebSocket connection.

        Args:
            symbols: Symbols for this connection
            connection_id: ID for logging
        """
        while self._running:
            try:
                async with websockets.connect(self.BYBIT_WS_PUBLIC) as ws:
                    self._ws_connections.append(ws)
                    self.log.info(f"WS#{connection_id} connected")

                    # Subscribe to kline streams
                    await self._subscribe_symbols_ws(ws, symbols)

                    # Process messages
                    async for message in ws:
                        await self._handle_ws_message(message)

            except websockets.ConnectionClosed:
                self.log.warning(f"WS#{connection_id} connection closed")
            except Exception as e:
                self.log.error(f"WS#{connection_id} error: {e}")

            if self._running:
                self.log.info(
                    f"WS#{connection_id} reconnecting in "
                    f"{self.settings.ws_reconnect_delay}s"
                )
                await asyncio.sleep(self.settings.ws_reconnect_delay)

    async def _subscribe_symbols_ws(
        self,
        ws: websockets.WebSocketClientProtocol,
        symbols: List[str],
    ) -> None:
        """
        Send subscription request for kline streams.

        Args:
            ws: WebSocket connection
            symbols: Symbols to subscribe to
        """
        # Build subscription topics (kline.1.SYMBOL)
        topics = [f"kline.1.{symbol}" for symbol in symbols]

        subscribe_msg = {
            "op": "subscribe",
            "args": topics,
        }

        await ws.send(json.dumps(subscribe_msg))
        self.log.debug(f"Subscribed to {len(topics)} kline streams")

    async def _subscribe_symbols(self, symbols: Set[str]) -> None:
        """
        Subscribe to additional symbols on existing connections.

        Args:
            symbols: New symbols to subscribe to
        """
        # TODO: Implement dynamic subscription
        # This should distribute new symbols across existing connections
        # or create new connections if needed
        pass

    async def _handle_ws_message(self, message: str) -> None:
        """
        Handle incoming WebSocket message.

        Args:
            message: Raw JSON message from WebSocket
        """
        try:
            data = json.loads(message)

            # Check if this is a kline update
            if data.get("topic", "").startswith("kline."):
                await self._process_kline_update(data)

        except json.JSONDecodeError:
            self.log.warning(f"Invalid JSON: {message[:100]}")
        except Exception as e:
            self.log.error(f"Error handling message: {e}")

    async def _process_kline_update(self, data: Dict[str, Any]) -> None:
        """
        Process a kline (candle) update from WebSocket.

        This is called every second with updated candle data.
        At minute boundary, previous candle is marked as confirmed.

        Args:
            data: Kline update data from WebSocket
        """
        # TODO: Implement actual processing
        # This is a stub that should:
        # 1. Parse kline data from message
        # 2. Update current candle state
        # 3. Upsert to database
        # 4. Send event to queue
        #
        # Expected data structure:
        # {
        #     "topic": "kline.1.BTCUSDT",
        #     "data": [{
        #         "start": 1234567890000,  # candle start time (ms)
        #         "end": 1234567949999,    # candle end time (ms)
        #         "interval": "1",
        #         "open": "50000.00",
        #         "close": "50100.00",
        #         "high": "50200.00",
        #         "low": "49900.00",
        #         "volume": "100.5",
        #         "turnover": "5025000.00",
        #         "confirm": false,  # true when candle is finalized
        #         "timestamp": 1234567891000,
        #     }]
        # }

        topic = data.get("topic", "")
        symbol = topic.split(".")[-1] if topic else ""

        kline_data = data.get("data", [{}])[0]
        if not kline_data or not symbol:
            return

        # Parse candle data
        candle_start_ms = kline_data.get("start", 0)
        candle_timestamp = datetime.fromtimestamp(candle_start_ms / 1000, tz=timezone.utc)

        open_price = float(kline_data.get("open", 0))
        high_price = float(kline_data.get("high", 0))
        low_price = float(kline_data.get("low", 0))
        close_price = float(kline_data.get("close", 0))
        volume = float(kline_data.get("volume", 0))
        turnover = float(kline_data.get("turnover", 0))
        is_confirmed = kline_data.get("confirm", False)

        # Upsert to database
        await self.repository.upsert(
            Candle1m,
            conflict_keys={"symbol": symbol, "timestamp": candle_timestamp},
            update_values={
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "turnover": turnover,
                "is_confirmed": is_confirmed,
            },
        )

        # Send to queue for strategy
        await self.queue.put_candle(
            symbol=symbol,
            open_=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            candle_timestamp=candle_timestamp,
            is_confirmed=is_confirmed,
        )

        self.log.debug(
            f"Candle update: {symbol} @ {candle_timestamp} "
            f"C={close_price} V={volume} confirmed={is_confirmed}"
        )

    # =========================================================================
    # Data Cleanup
    # =========================================================================

    async def _periodic_cleanup(self) -> None:
        """Background task to clean old candles periodically."""
        while self._running:
            try:
                await asyncio.sleep(
                    self.settings.candle_cleanup_interval_minutes * 60
                )
                await self._cleanup_old_candles()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Error in cleanup task: {e}")

    async def _cleanup_old_candles(self) -> None:
        """
        Remove candles older than the retention period.

        Keeps only the last N days of candle history.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(
            days=self.settings.candle_history_days
        )

        deleted = await self.repository.delete_older_than(
            Candle1m,
            "timestamp",
            cutoff,
        )

        self.log.info(f"Cleaned {deleted} old candles (older than {cutoff})")

    # =========================================================================
    # Public API
    # =========================================================================

    async def get_candles(
        self,
        symbol: str,
        limit: int = 100,
    ) -> List[Candle1m]:
        """
        Get recent candles for a symbol.

        Args:
            symbol: Bybit symbol
            limit: Maximum number of candles to return

        Returns:
            List of Candle1m records, newest first
        """
        return await self.repository.get_all(
            Candle1m,
            filters={"symbol": symbol},
            order_by="timestamp",
            order_desc=True,
            limit=limit,
        )

    def get_current_candle(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the current (unconfirmed) candle for a symbol.

        Args:
            symbol: Bybit symbol

        Returns:
            Current candle data or None
        """
        return self._current_candles.get(symbol)

    def is_connected(self) -> bool:
        """Check if WebSocket connections are active."""
        return len(self._ws_connections) > 0 and self._running
