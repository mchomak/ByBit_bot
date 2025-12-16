from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set

import aiohttp


@dataclass(frozen=True)
class BybitInstrument:
    """Minimal normalized representation of Bybit instrument."""
    symbol: str
    base_coin: str
    quote_coin: str
    category: str


class BybitAPIError(RuntimeError):
    """Raised when Bybit returns non-zero retCode or malformed response."""


class BybitClient:
    """
    Minimal Bybit V5 public REST client for instruments universe.

    Endpoint:
      GET /v5/market/instruments-info

    Notes from Bybit docs:
      - spot does not support pagination: limit/cursor invalid
      - linear/inverse/option can require cursor pagination via nextPageCursor
    """

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        timeout_s: int = 30,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max_retries
        self._log = logger or logging.getLogger(self.__class__.__name__)
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
                    # Bybit sometimes returns 200 with retCode!=0; still parse JSON
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
                self._log.warning(
                    "GET %s failed (%s). Retry in %.2fs (attempt %d/%d)",
                    url, str(e), sleep_s, attempt + 1, self._max_retries + 1
                )
                await asyncio.sleep(sleep_s)

        raise RuntimeError("Unreachable")

    @staticmethod
    def _parse_instruments(category: str, payload: Dict[str, Any]) -> List[BybitInstrument]:
        result = payload.get("result") or {}
        items = result.get("list") or []

        instruments: List[BybitInstrument] = []
        for it in items:
            symbol = (it.get("symbol") or "").strip()
            base_coin = (it.get("baseCoin") or "").strip()
            quote_coin = (it.get("quoteCoin") or "").strip()

            if not symbol or not base_coin:
                continue

            instruments.append(
                BybitInstrument(
                    symbol=symbol,
                    base_coin=base_coin.upper(),
                    quote_coin=quote_coin.upper(),
                    category=category,
                )
            )
        return instruments

    @staticmethod
    def _next_cursor(payload: Dict[str, Any]) -> str:
        result = payload.get("result") or {}
        return (result.get("nextPageCursor") or "").strip()

    @staticmethod
    def _ensure_ok(payload: Dict[str, Any]) -> None:
        # Bybit typical schema: retCode=0 is OK
        ret_code = payload.get("retCode")
        if ret_code is None:
            raise BybitAPIError(f"Malformed response (no retCode): {payload!r}")

        if int(ret_code) != 0:
            raise BybitAPIError(
                f"Bybit error retCode={ret_code}, retMsg={payload.get('retMsg')}"
            )

    async def iter_instruments(
        self,
        category: str,
        status: Optional[str] = "Trading",
        limit: int = 1000,
    ) -> Iterable[BybitInstrument]:
        """
        Iterate through launched instruments for a given category.

        For spot: pagination is not supported; a single call is used.
        For others: paginate using cursor/nextPageCursor until exhausted.
        """
        category = category.strip().lower()
        if category not in {"spot", "linear", "inverse", "option"}:
            raise ValueError("category must be one of: spot, linear, inverse, option")

        cursor: str = ""
        first = True

        while True:
            params: Dict[str, Any] = {"category": category}
            if status:
                params["status"] = status

            # Spot does not support pagination: do NOT send limit/cursor
            if category != "spot":
                params["limit"] = int(limit)
                if cursor:
                    params["cursor"] = cursor

            payload = await self._get_json("/v5/market/instruments-info", params=params)
            self._ensure_ok(payload)

            for inst in self._parse_instruments(category, payload):
                yield inst

            if category == "spot":
                return

            next_cursor = self._next_cursor(payload)
            if not next_cursor:
                return

            cursor = next_cursor
            first = False

    async def get_base_coin_map(
        self,
        categories: List[str],
        status: Optional[str] = "Trading",
    ) -> Dict[str, Set[str]]:
        """
        Returns mapping: BASE_COIN -> set(categories) where the coin appears as baseCoin.
        """
        base_map: Dict[str, Set[str]] = {}

        for cat in categories:
            async for inst in self.iter_instruments(category=cat, status=status):
                base_map.setdefault(inst.base_coin, set()).add(inst.category)

        return base_map
