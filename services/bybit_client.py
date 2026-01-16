from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp import ClientWebSocketResponse
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import Settings
from services.pipeline_events import CandleUpdate, PipelineMetrics


@dataclass(frozen=True)
class BybitInstrument:
    """Minimal normalized representation of a Bybit instrument."""
    symbol: str
    base_coin: str
    quote_coin: str
    category: str  # spot/linear/inverse/option
    is_st: bool = False  # Special Treatment (high risk) token


class BybitAPIError(RuntimeError):
    """Raised when Bybit returns non-zero retCode or malformed response."""


class _CandleUpsertBuffer:
    """
    Batches candle UPSERTs to PostgreSQL.

    This is critical when monitoring many symbols: Bybit kline streams can push updates frequently
    (e.g., multiple updates for the current 1m candle). Persisting each WS message with its own
    transaction will not scale.

    Buffer semantics:
      - key: (symbol, timestamp_start_of_minute_utc)
      - value: latest candle fields (open/high/low/close/volume/turnover/is_confirmed)
    """

    def __init__(
        self,
        session_factory: sessionmaker,
        candle_model: Any,
        *,
        flush_interval_s: float = 1.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._session_factory = session_factory
        self._model = candle_model
        self._flush_interval_s = float(flush_interval_s)
        self._log = logger or logging.getLogger(self.__class__.__name__)

        self._lock = asyncio.Lock()
        self._buf: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="candle-upsert-buffer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
            self._task = None

    async def add(self, row: Dict[str, Any]) -> None:
        key = (row["symbol"], row["timestamp"])
        async with self._lock:
            self._buf[key] = row

    async def flush_now(self) -> int:
        async with self._lock:
            if not self._buf:
                return 0
            rows = list(self._buf.values())
            self._buf.clear()

        async with self._session_factory() as session:
            stmt = pg_insert(self._model).values(rows)
            update_cols = {
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "turnover": stmt.excluded.turnover,
                "is_confirmed": stmt.excluded.is_confirmed,
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol", "timestamp"],
                set_=update_cols,
            )
            await session.execute(stmt)
            await session.commit()

        return len(rows)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self._flush_interval_s)
                n = await self.flush_now()
                if n:
                    self._log.debug("Flushed %d candle rows", n)
            except Exception as e:  # noqa: BLE001
                self._log.exception("Candle buffer flush failed: %s", e)
                await asyncio.sleep(min(5.0, self._flush_interval_s * 2))


class BybitClient:
    """
    Bybit V5 public REST + WebSocket client.

    REST:
      - instruments universe: GET /v5/market/instruments-info
      - kline history (bootstrap): GET /v5/market/kline

    WebSocket:
      - kline stream topic: kline.{interval}.{symbol}
    """

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        *,
        ws_domain: str = "stream.bybit.com",
        timeout_s: int = 30,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._ws_domain = ws_domain.strip()
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = int(max_retries)
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "BybitClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)

    async def _get_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        assert self._session is not None

        url = f"{self._base_url}{path}"
        params = params or {}

        for attempt in range(self._max_retries + 1):
            try:
                async with self._session.get(url, params=params) as resp:
                    data = await resp.json(content_type=None)
                    if resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=f"Server error: {resp.status}",
                            headers=resp.headers,
                        )
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= self._max_retries:
                    raise
                sleep_s = (0.8 * (2**attempt)) + random.random() * 0.2
                self.logger.warning(
                    "GET %s failed (%s). Retry in %.2fs (attempt %d/%d)",
                    url, str(e), sleep_s, attempt + 1, self._max_retries + 1,
                )
                await asyncio.sleep(sleep_s)

        raise RuntimeError("Unreachable")

    @staticmethod
    def _ensure_ok(payload: Dict[str, Any]) -> None:
        ret_code = payload.get("retCode")
        if ret_code is None:
            raise BybitAPIError(f"Malformed response (no retCode): {payload!r}")
        if int(ret_code) != 0:
            raise BybitAPIError(
                f"Bybit error retCode={ret_code}, retMsg={payload.get('retMsg')}"
            )

    # -------------------------------------------------------------------------
    # Instruments
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_instruments(category: str, payload: Dict[str, Any], logger: Optional[logging.Logger] = None) -> List[BybitInstrument]:
        result = payload.get("result") or {}
        items = result.get("list") or []
        log = logger or logging.getLogger("BybitClient")

        instruments: List[BybitInstrument] = []
        for it in items:
            symbol = (it.get("symbol") or "").strip()
            base_coin = (it.get("baseCoin") or "").strip()
            quote_coin = (it.get("quoteCoin") or "").strip()
            if not symbol or not base_coin:
                continue

            # Check for ST (Special Treatment) tokens
            is_st = False

            # METHOD 1 (PRIMARY): Check stTag field - this is the official ST marker
            # stTag='1' means the token is marked as ST (Special Treatment / High Risk)
            # stTag='0' means normal token
            st_tag = str(it.get("stTag", "0"))
            if st_tag == "1":
                is_st = True

            # METHOD 2: Check innovation field
            # innovation='1' means innovation zone (higher risk)
            if not is_st:
                innovation = str(it.get("innovation", "0"))
                if innovation == "1":
                    is_st = True

            # METHOD 3: Check contractType for risky designation (futures only)
            if not is_st and category in ["linear", "inverse"]:
                contract_type = (it.get("contractType") or "").strip()
                if "ST" in contract_type.upper():
                    is_st = True

            # Log ST tokens for monitoring (only at debug level to avoid spam)
            if is_st:
                log.debug(
                    "ST token detected: %s (stTag=%s, innovation=%s)",
                    symbol, st_tag, it.get('innovation', '0')
                )

            instruments.append(
                BybitInstrument(
                    symbol=symbol,
                    base_coin=base_coin.upper(),
                    quote_coin=quote_coin.upper(),
                    category=category,
                    is_st=is_st,
                )
            )
        return instruments

    @staticmethod
    def _next_cursor(payload: Dict[str, Any]) -> str:
        result = payload.get("result") or {}
        return (result.get("nextPageCursor") or "").strip()

    async def iter_instruments(
        self,
        category: str,
        status: Optional[str] = "Trading",
        limit: int = 1000,
    ) -> AsyncIterator[BybitInstrument]:
        """
        Iterate instruments for a category.

        Spot: no pagination (do not pass limit/cursor).
        Linear/inverse/option: cursor pagination via nextPageCursor.
        """
        category = category.strip().lower()
        if category not in {"spot", "linear", "inverse", "option"}:
            raise ValueError("category must be one of: spot, linear, inverse, option")

        cursor: str = ""
        while True:
            params: Dict[str, Any] = {"category": category}
            if status:
                params["status"] = status

            if category != "spot":
                params["limit"] = int(limit)
                if cursor:
                    params["cursor"] = cursor

            payload = await self._get_json("/v5/market/instruments-info", params=params)
            self._ensure_ok(payload)

            for inst in self._parse_instruments(category, payload, self.logger):
                yield inst

            if category == "spot":
                return

            cursor = self._next_cursor(payload)
            if not cursor:
                return

    async def get_base_coin_map(
        self,
        categories: List[str],
        status: Optional[str] = "Trading",
    ) -> Dict[str, Set[str]]:
        """Returns mapping BASE_COIN -> set(categories) where the coin appears as baseCoin."""
        out: Dict[str, Set[str]] = {}
        for cat in categories:
            async for inst in self.iter_instruments(category=cat, status=status):
                out.setdefault(inst.base_coin, set()).add(inst.category)
        return out

    async def get_st_tokens(
        self,
        categories: List[str],
        status: Optional[str] = "Trading",
    ) -> Set[str]:
        """
        Get set of ST (Special Treatment / high-risk) tokens.

        Returns baseCoin symbols that are marked as ST by Bybit.
        """
        st_tokens: Set[str] = set()
        for cat in categories:
            async for inst in self.iter_instruments(category=cat, status=status):
                if inst.is_st:
                    st_tokens.add(inst.base_coin)
                    self.logger.debug("Found ST token: %s (%s)", inst.base_coin, inst.symbol)
        return st_tokens

    async def check_stale_prices(
        self,
        category: str,
        symbols: List[str],
        lookback_minutes: int = 50,
        consecutive_stale: int = 3,
        concurrency: int = 10,
    ) -> Set[str]:
        """
        Check for symbols with stale prices (no price movement).

        A symbol is considered stale if for N or more consecutive minutes
        the open price equals the close price (no trading activity).

        Args:
            category: Bybit category (spot, linear, etc.)
            symbols: List of symbols to check
            lookback_minutes: How many minutes of history to check
            consecutive_stale: Number of consecutive stale candles to trigger filter
            concurrency: Max concurrent API requests

        Returns:
            Set of symbol names that have stale prices (should be excluded)
        """
        stale_symbols: Set[str] = set()
        sem = asyncio.Semaphore(concurrency)

        async def _check_one(symbol: str) -> Optional[str]:
            async with sem:
                try:
                    # Fetch recent candles
                    raw = await self.get_kline_page(
                        category=category,
                        symbol=symbol,
                        interval="1",
                        limit=lookback_minutes,
                    )

                    if not raw or len(raw) < consecutive_stale:
                        return None

                    # Check for consecutive stale candles (open == close)
                    max_consecutive = 0
                    current_streak = 0

                    # raw is sorted newest->oldest, reverse for chronological order
                    for candle in reversed(raw):
                        try:
                            open_price = float(candle[1])
                            close_price = float(candle[4])

                            if open_price == close_price:
                                current_streak += 1
                                max_consecutive = max(max_consecutive, current_streak)
                            else:
                                current_streak = 0
                        except (IndexError, ValueError):
                            continue

                    if max_consecutive >= consecutive_stale:
                        self.logger.debug(
                            "Stale price detected for %s: %d consecutive flat candles",
                            symbol, max_consecutive
                        )
                        return symbol

                    return None

                except Exception as e:
                    self.logger.debug("Failed to check stale prices for %s: %s", symbol, e)
                    return None

        results = await asyncio.gather(*[_check_one(s) for s in symbols])

        for result in results:
            if result:
                stale_symbols.add(result)

        if stale_symbols:
            self.logger.info(
                "Found %d symbols with stale prices (>=%d consecutive flat candles)",
                len(stale_symbols), consecutive_stale
            )

        return stale_symbols

    # -------------------------------------------------------------------------
    # Kline REST (bootstrap)
    # -------------------------------------------------------------------------

    async def get_kline_page(
        self,
        *,
        category: str,
        symbol: str,
        interval: str = "1",
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> List[List[str]]:
        """
        Fetch one REST page of klines.

        Bybit returns list sorted newest->oldest.
        Each kline item is an array of strings:
          [startTime, open, high, low, close, volume, turnover]
        """
        params: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "limit": int(limit),
        }
        if start_ms is not None:
            params["start"] = int(start_ms)
        if end_ms is not None:
            params["end"] = int(end_ms)

        payload = await self._get_json("/v5/market/kline", params=params)
        self._ensure_ok(payload)

        result = payload.get("result") or {}
        return result.get("list") or []

    async def fetch_1m_history(
        self,
        *,
        category: str,
        symbol: str,
        days: int = 5,
        now: Optional[datetime] = None,
        page_limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch N days of 1m candles via REST and normalize to dicts ready for DB insertion.

        Intended for backfill at startup.
        """
        if days <= 0:
            return []

        now_dt = now or datetime.now(timezone.utc)
        start_dt = now_dt - timedelta(days=days)

        start_ms = int(start_dt.timestamp() * 1000)
        cursor_end_ms = int(now_dt.timestamp() * 1000)

        out: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
        safety = 0

        while cursor_end_ms > start_ms:
            safety += 1
            if safety > 2000:
                break

            raw = await self.get_kline_page(
                category=category,
                symbol=symbol,
                interval="1",
                start_ms=start_ms,
                end_ms=cursor_end_ms,
                limit=page_limit,
            )
            if not raw:
                break

            # reverse to oldest->newest
            page = list(reversed(raw))

            for item in page:
                try:
                    start_ts_ms = int(item[0])
                    ts = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc)
                    out[(symbol, ts)] = {
                        "symbol": symbol,
                        "timestamp": ts,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                        "turnover": float(item[6]) if len(item) > 6 and item[6] is not None else None,
                        "is_confirmed": True,
                    }
                except Exception as e:  # noqa: BLE001
                    self.logger.debug("Skip malformed kline item for %s: %r (%s)", symbol, item, e)

            oldest_ms = int(page[0][0])
            if oldest_ms <= start_ms:
                break
            cursor_end_ms = oldest_ms - 1

        return list(out.values())

    async def seed_1m_history_to_db(
        self,
        *,
        category: str,
        symbols: List[str],
        session_factory: sessionmaker,
        candle_model: Any,
        days: int = 5,
        concurrency: int = 8,
        batch_size: int = 3000,
    ) -> None:
        """
        Backfill candles for multiple symbols (REST) and write to PostgreSQL.

        Uses batched inserts to avoid exceeding PostgreSQL's 32767 parameter limit.
        With 9 columns per row, max safe batch is ~3600 rows.
        """
        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def _insert_batch(session, batch: List[Dict[str, Any]]) -> None:
            """Insert a batch of rows with ON CONFLICT DO UPDATE."""
            if not batch:
                return
            stmt = pg_insert(candle_model).values(batch)
            update_cols = {
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "turnover": stmt.excluded.turnover,
                "is_confirmed": stmt.excluded.is_confirmed,
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol", "timestamp"],
                set_=update_cols,
            )
            await session.execute(stmt)

        async def _one(sym: str) -> None:
            async with sem:
                try:
                    rows = await self.fetch_1m_history(category=category, symbol=sym, days=days)
                    if not rows:
                        return

                    # Insert in batches to avoid parameter limit
                    async with session_factory() as session:
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i + batch_size]
                            await _insert_batch(session, batch)
                        await session.commit()

                    self.logger.info("Seeded %s: %d rows", sym, len(rows))
                except Exception as e:
                    self.logger.warning("Failed to seed %s: %s", sym, e)

        await asyncio.gather(*[_one(s) for s in symbols])

    # -------------------------------------------------------------------------
    # WebSocket kline ingestion
    # -------------------------------------------------------------------------

    def _ws_base_url(self, category: str) -> str:
        cat = category.strip().lower()
        if cat not in {"spot", "linear", "inverse", "option"}:
            raise ValueError("category must be one of: spot, linear, inverse, option")

        domain = self._ws_domain
        return f"wss://{domain}/v5/public/{cat}"

    async def _ws_send(self, ws: ClientWebSocketResponse, payload: Dict[str, Any]) -> None:
        await ws.send_str(json.dumps(payload, separators=(",", ":")))

    async def _ws_ping_loop(self, ws: ClientWebSocketResponse, interval_s: float = 20.0) -> None:
        while True:
            await asyncio.sleep(interval_s)
            await self._ws_send(ws, {"op": "ping"})

    @staticmethod
    def _split_topics_for_connection(
        topics: List[str],
        *,
        max_chars_total: int = 20000,
    ) -> List[List[str]]:
        """
        Split topics into groups to respect Bybit's public-channel args length limit per connection.

        The docs state that, for one public connection, the "args" array cannot exceed 21,000 characters.
        We keep a slightly smaller max_chars_total as a safety margin.
        """
        groups: List[List[str]] = []
        cur: List[str] = []
        cur_len = 0

        for t in topics:
            tlen = len(t)
            if cur and (cur_len + tlen) > max_chars_total:
                groups.append(cur)
                cur = []
                cur_len = 0
            cur.append(t)
            cur_len += tlen

        if cur:
            groups.append(cur)
        return groups

    @staticmethod
    def _chunk(
        items: List[str],
        *,
        size: int,
    ) -> List[List[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    async def stream_kline_1m_to_db(
        self,
        *,
        category: str,
        symbols: List[str],
        session_factory: sessionmaker,
        candle_model: Any,
        flush_interval_s: float = 1.0,
        reconnect_backoff_s: float = 2.0,
    ) -> None:
        """
        Subscribe to kline.1 for symbols and write updates to PostgreSQL.

        Operational behavior:
          - Opens 1..N WS connections (if required by args length constraint)
          - Sends ping every ~20s (recommended by Bybit)
          - Batch UPSERTs to candles_1m every flush_interval_s seconds
          - Auto-reconnects each connection independently
        """
        if not symbols:
            self.logger.warning("No symbols provided for WS stream; exiting")
            return

        topics = [f"kline.1.{s}" for s in symbols]
        topic_groups = self._split_topics_for_connection(topics)

        buffer = _CandleUpsertBuffer(
            session_factory=session_factory,
            candle_model=candle_model,
            flush_interval_s=flush_interval_s,
            logger=self.logger,
        )
        await buffer.start()

        async def _run_one_connection(group_topics: List[str]) -> None:
            attempt = 0

            # Bybit docs: for spot subscription request, max 10 args per request.
            # For linear/inverse there is no explicit args-per-request cap "for now", but we still chunk.
            per_request = 10 if category.strip().lower() == "spot" else 200

            while True:
                ws: Optional[ClientWebSocketResponse] = None
                ping_task: Optional[asyncio.Task] = None
                try:
                    await self._ensure_session()
                    assert self._session is not None

                    ws_url = self._ws_base_url(category)
                    self.logger.info("WS connecting: %s (topics=%d)", ws_url, len(group_topics))

                    ws = await self._session.ws_connect(ws_url, autoping=False)
                    ping_task = asyncio.create_task(self._ws_ping_loop(ws), name="bybit-ws-ping")

                    for chunk in self._chunk(group_topics, size=per_request):
                        await self._ws_send(ws, {"op": "subscribe", "args": chunk})
                        await asyncio.sleep(0.05)

                    attempt = 0

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue

                            op = payload.get("op")
                            if op in {"pong", "ping"}:
                                continue
                            if payload.get("type") == "COMMAND_RESP":
                                continue

                            topic = payload.get("topic")
                            if not topic or not topic.startswith("kline."):
                                continue

                            sym = topic.split(".")[-1]
                            for c in payload.get("data", []) or []:
                                try:
                                    ts = datetime.fromtimestamp(int(c["start"]) / 1000, tz=timezone.utc)
                                    row = {
                                        "symbol": sym,
                                        "timestamp": ts,
                                        "open": float(c["open"]),
                                        "high": float(c["high"]),
                                        "low": float(c["low"]),
                                        "close": float(c["close"]),
                                        "volume": float(c["volume"]),
                                        "turnover": float(c["turnover"]) if c.get("turnover") is not None else None,
                                        "is_confirmed": bool(c.get("confirm", False)),
                                    }
                                    await buffer.add(row)
                                except Exception as e:  # noqa: BLE001
                                    self.logger.debug("Bad WS kline payload: %r (%s)", c, e)

                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            raise ConnectionError(f"WS closed/error: {msg.type}")

                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    attempt += 1
                    sleep_s = min(60.0, reconnect_backoff_s * (2 ** min(attempt, 6)))
                    self.logger.warning("WS error: %s. Reconnect in %.1fs", e, sleep_s)
                    await asyncio.sleep(sleep_s)
                finally:
                    if ping_task:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await ping_task
                    if ws is not None:
                        with contextlib.suppress(Exception):
                            await ws.close()

        tasks = [asyncio.create_task(_run_one_connection(g)) for g in topic_groups]

        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks)
            with contextlib.suppress(Exception):
                await buffer.flush_now()
            await buffer.stop()

    # -------------------------------------------------------------------------
    # WebSocket kline stream to Queue (for pipeline architecture)
    # -------------------------------------------------------------------------

    async def stream_klines_to_queue(
        self,
        *,
        category: str,
        symbols: List[str],
        output_queue: asyncio.Queue,
        ping_interval_s: float = 20.0,
        reconnect_backoff_s: float = 2.0,
        metrics: Optional[PipelineMetrics] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """
        Stream kline updates to a queue instead of directly to DB.

        This method is designed for the event-driven pipeline architecture where
        candle updates are processed by downstream consumers.

        Args:
            category: Bybit category (spot, linear, inverse, option)
            symbols: List of symbols to subscribe to
            output_queue: Bounded asyncio.Queue to push CandleUpdate events
            ping_interval_s: Interval for heartbeat pings (~20s recommended)
            reconnect_backoff_s: Base backoff for reconnection
            metrics: Optional PipelineMetrics for tracking
            stop_event: Optional event to signal graceful shutdown
        """
        if not symbols:
            self.logger.warning("No symbols provided for WS stream; exiting")
            return

        stop = stop_event or asyncio.Event()
        topics = [f"kline.1.{s}" for s in symbols]
        topic_groups = self._split_topics_for_connection(topics)

        self.logger.info(
            "Starting WS kline stream: %d symbols, %d connections",
            len(symbols), len(topic_groups)
        )

        async def _run_connection(group_topics: List[str], conn_id: int) -> None:
            attempt = 0
            per_request = 10 if category.strip().lower() == "spot" else 200

            while not stop.is_set():
                ws: Optional[ClientWebSocketResponse] = None
                ping_task: Optional[asyncio.Task] = None

                try:
                    await self._ensure_session()
                    assert self._session is not None

                    ws_url = self._ws_base_url(category)
                    self.logger.info(
                        "[conn-%d] Connecting to %s (topics=%d)",
                        conn_id, ws_url, len(group_topics)
                    )

                    ws = await self._session.ws_connect(ws_url, autoping=False)
                    ping_task = asyncio.create_task(
                        self._ws_ping_loop(ws, ping_interval_s),
                        name=f"ws-ping-{conn_id}"
                    )

                    # Subscribe in chunks
                    for chunk in self._chunk(group_topics, size=per_request):
                        await self._ws_send(ws, {"op": "subscribe", "args": chunk})
                        await asyncio.sleep(0.05)

                    attempt = 0  # Reset on successful connect

                    # Message loop
                    async for msg in ws:
                        if stop.is_set():
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue

                            op = payload.get("op")
                            if op in {"pong", "ping"}:
                                continue
                            if payload.get("type") == "COMMAND_RESP":
                                continue

                            topic = payload.get("topic", "")
                            if not topic.startswith("kline."):
                                continue

                            sym = topic.split(".")[-1]
                            for c in payload.get("data", []) or []:
                                try:
                                    update = self._parse_candle_update(sym, c)
                                    if update:
                                        try:
                                            output_queue.put_nowait(update)
                                            if metrics:
                                                metrics.ws_messages_received += 1
                                        except asyncio.QueueFull:
                                            self.logger.warning(
                                                "Queue full, dropping update for %s", sym
                                            )
                                except Exception as e:
                                    self.logger.debug("Bad kline payload: %r (%s)", c, e)

                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            raise ConnectionError(f"WS closed/error: {msg.type}")

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if stop.is_set():
                        break
                    attempt += 1
                    sleep_s = min(60.0, reconnect_backoff_s * (2 ** min(attempt, 6)))
                    self.logger.warning(
                        "[conn-%d] Error: %s. Reconnect in %.1fs",
                        conn_id, e, sleep_s
                    )
                    if metrics:
                        metrics.errors += 1
                    await asyncio.sleep(sleep_s)
                finally:
                    if ping_task:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await ping_task
                    if ws is not None:
                        with contextlib.suppress(Exception):
                            await ws.close()

        tasks = [
            asyncio.create_task(_run_connection(g, i), name=f"ws-conn-{i}")
            for i, g in enumerate(topic_groups)
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)

    def _parse_candle_update(self, symbol: str, data: Dict[str, Any]) -> Optional[CandleUpdate]:
        """Parse raw WS candle data into CandleUpdate event."""
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