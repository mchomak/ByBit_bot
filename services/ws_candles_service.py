"""
WebSocket Candle Ingestion Service.

Connects to Bybit V5 public WebSocket and streams 1-minute klines
for all subscribed symbols. Produces CandleUpdate events to a queue.

Features:
- Automatic reconnection with exponential backoff
- Heartbeat ping every ~20 seconds
- Handles connection splitting for large symbol lists
- Non-blocking: pushes to bounded asyncio.Queue
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone
import sys
from typing import Any, Callable, Dict, List, Optional
import os
import aiohttp
from aiohttp import ClientWebSocketResponse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.pipeline_events import CandleUpdate, PipelineMetrics


class WSCandlesService:
    """
    WebSocket service for streaming 1-minute candles from Bybit V5.

    This is the Producer in the pipeline architecture.
    """

    def __init__(
        self,
        symbols: List[str],
        output_queue: asyncio.Queue,
        *,
        category: str = "spot",
        ws_domain: str = "stream.bybit.com",
        ping_interval_s: float = 20.0,
        reconnect_backoff_s: float = 2.0,
        max_topics_per_connection: int = 20000,
        subscribe_chunk_size: int = 10,
        metrics: Optional[PipelineMetrics] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize the WebSocket candles service.

        Args:
            symbols: List of Bybit symbols to subscribe to (e.g., ["BTCUSDT", "ETHUSDT"])
            output_queue: Bounded asyncio.Queue to push CandleUpdate events
            category: Bybit market category (spot, linear, inverse, option)
            ws_domain: WebSocket domain
            ping_interval_s: Interval for heartbeat pings
            reconnect_backoff_s: Base backoff for reconnection
            max_topics_per_connection: Max topic chars per WS connection
            subscribe_chunk_size: Args per subscribe request (10 for spot)
            metrics: Optional shared metrics object
            logger: Optional logger instance
        """
        self._symbols = list(symbols)
        self._queue = output_queue
        self._category = category.strip().lower()
        self._ws_domain = ws_domain.strip()
        self._ping_interval_s = float(ping_interval_s)
        self._reconnect_backoff_s = float(reconnect_backoff_s)
        self._max_topics = int(max_topics_per_connection)
        self._chunk_size = int(subscribe_chunk_size)
        self._metrics = metrics or PipelineMetrics()
        self._log = logger or logging.getLogger(self.__class__.__name__)

        self._session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

    @property
    def ws_url(self) -> str:
        """Construct the WebSocket URL."""
        return f"wss://{self._ws_domain}/v5/public/{self._category}"

    async def start(self) -> None:
        """Start the WebSocket service."""
        if self._tasks:
            self._log.warning("Service already running")
            return

        if not self._symbols:
            self._log.warning("No symbols to subscribe to")
            return

        self._stop_event.clear()
        self._session = aiohttp.ClientSession()

        # Build topic list: kline.1.<symbol>
        topics = [f"kline.1.{s}" for s in self._symbols]

        # Split topics into groups to respect connection limits
        topic_groups = self._split_topics(topics)
        self._log.info(
            "Starting WS service: %d symbols, %d connections",
            len(self._symbols), len(topic_groups)
        )

        # Start a task for each connection group
        for i, group in enumerate(topic_groups):
            task = asyncio.create_task(
                self._run_connection(group, conn_id=i),
                name=f"ws-conn-{i}"
            )
            self._tasks.append(task)

    async def stop(self) -> None:
        """Stop the WebSocket service gracefully."""
        self._log.info("Stopping WS service...")
        self._stop_event.set()

        # Cancel all tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        self._tasks.clear()

        # Close session
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        self._log.info("WS service stopped")

    def _split_topics(self, topics: List[str]) -> List[List[str]]:
        """Split topics into groups to respect Bybit's args length limit."""
        groups: List[List[str]] = []
        current: List[str] = []
        current_len = 0

        for topic in topics:
            topic_len = len(topic)
            if current and (current_len + topic_len) > self._max_topics:
                groups.append(current)
                current = []
                current_len = 0
            current.append(topic)
            current_len += topic_len

        if current:
            groups.append(current)

        return groups

    async def _run_connection(self, topics: List[str], conn_id: int) -> None:
        """
        Run a single WebSocket connection with auto-reconnect.

        Args:
            topics: List of topics for this connection
            conn_id: Connection ID for logging
        """
        attempt = 0

        while not self._stop_event.is_set():
            ws: Optional[ClientWebSocketResponse] = None
            ping_task: Optional[asyncio.Task] = None

            try:
                assert self._session is not None

                self._log.info(
                    "[conn-%d] Connecting to %s (topics=%d)",
                    conn_id, self.ws_url, len(topics)
                )

                ws = await self._session.ws_connect(
                    self.ws_url,
                    autoping=False,
                    heartbeat=None,
                )

                # Start ping loop
                ping_task = asyncio.create_task(
                    self._ping_loop(ws, conn_id),
                    name=f"ping-{conn_id}"
                )

                # Subscribe in chunks
                await self._subscribe(ws, topics)
                attempt = 0  # Reset on successful connect

                # Process messages
                await self._message_loop(ws, conn_id)

            except asyncio.CancelledError:
                self._log.info("[conn-%d] Cancelled", conn_id)
                raise

            except Exception as e:
                if self._stop_event.is_set():
                    break

                attempt += 1
                sleep_s = min(60.0, self._reconnect_backoff_s * (2 ** min(attempt, 6)))
                self._log.warning(
                    "[conn-%d] Error: %s. Reconnecting in %.1fs (attempt %d)",
                    conn_id, e, sleep_s, attempt
                )
                self._metrics.errors += 1
                await asyncio.sleep(sleep_s)

            finally:
                if ping_task:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task
                if ws is not None:
                    with contextlib.suppress(Exception):
                        await ws.close()

    async def _subscribe(self, ws: ClientWebSocketResponse, topics: List[str]) -> None:
        """Subscribe to topics in chunks."""
        for i in range(0, len(topics), self._chunk_size):
            chunk = topics[i:i + self._chunk_size]
            payload = {"op": "subscribe", "args": chunk}
            await ws.send_str(json.dumps(payload, separators=(",", ":")))
            await asyncio.sleep(0.05)  # Small delay between chunks

    async def _ping_loop(self, ws: ClientWebSocketResponse, conn_id: int) -> None:
        """Send periodic pings to keep connection alive."""
        while True:
            await asyncio.sleep(self._ping_interval_s)
            try:
                await ws.send_str('{"op":"ping"}')
                self._log.debug("[conn-%d] Ping sent", conn_id)
            except Exception as e:
                self._log.warning("[conn-%d] Ping failed: %s", conn_id, e)
                break

    async def _message_loop(self, ws: ClientWebSocketResponse, conn_id: int) -> None:
        """Process incoming WebSocket messages."""
        async for msg in ws:
            if self._stop_event.is_set():
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    await self._handle_message(msg.data)
                except Exception as e:
                    self._log.debug("[conn-%d] Message error: %s", conn_id, e)

            elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                raise ConnectionError(f"WS closed/error: {msg.type}")

    async def _handle_message(self, data: str) -> None:
        """Parse and handle a single WebSocket message."""
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return

        # Ignore pong/ping responses
        op = payload.get("op")
        if op in {"pong", "ping"}:
            return

        # Ignore command responses
        if payload.get("type") == "COMMAND_RESP":
            return

        # Check for kline topic
        topic = payload.get("topic", "")
        if not topic.startswith("kline."):
            return

        # Extract symbol from topic: kline.1.BTCUSDT -> BTCUSDT
        parts = topic.split(".")
        if len(parts) < 3:
            return
        symbol = parts[-1]

        # Process each candle in the data array
        for candle_data in payload.get("data", []) or []:
            try:
                update = self._parse_candle(symbol, candle_data)
                if update:
                    # Non-blocking put with timeout
                    try:
                        self._queue.put_nowait(update)
                        self._metrics.ws_messages_received += 1
                    except asyncio.QueueFull:
                        self._log.warning("Queue full, dropping candle update for %s", symbol)
            except Exception as e:
                self._log.debug("Failed to parse candle: %r (%s)", candle_data, e)

    def _parse_candle(self, symbol: str, data: Dict[str, Any]) -> Optional[CandleUpdate]:
        """Parse raw candle data into CandleUpdate event."""
        try:
            ts_ms = int(data["start"])
            timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            return CandleUpdate(
                symbol=symbol,
                timestamp=timestamp,
                open=float(data["open"]),
                high=float(data["high"]),
                low=float(data["low"]),
                close=float(data["close"]),
                volume=float(data["volume"]),
                turnover=float(data["turnover"]) if data.get("turnover") is not None else None,
                confirm=bool(data.get("confirm", False)),
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Invalid candle data: {e}") from e