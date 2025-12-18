from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set
import aiohttp
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.bybit_client import BybitClient
from db.repository import Repository
from db.models import Token


@dataclass(frozen=True)
class PaprikaTicker:
    """Minimal normalized representation of CoinPaprika ticker."""
    paprika_id: str
    symbol: str
    name: str
    market_cap_usd: float


class CoinPaprikaAPIError(RuntimeError):
    """Raised when CoinPaprika returns malformed response."""


class CoinPaprikaClient:
    """
    Minimal CoinPaprika REST client.

    Endpoint used:
      GET /v1/tickers
    """

    def __init__(
        self,
        base_url: str = "https://api.coinpaprika.com",
        timeout_s: int = 30,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max_retries
        self._log = logger or logging.getLogger(self.__class__.__name__)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "CoinPaprikaClient":
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

    async def _get_json(self, path: str) -> Any:
        await self._ensure_session()
        assert self._session is not None

        url = f"{self._base_url}{path}"

        for attempt in range(self._max_retries + 1):
            try:
                async with self._session.get(url) as resp:
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
    def _parse_tickers(payload: Any) -> List[PaprikaTicker]:
        if not isinstance(payload, list):
            raise CoinPaprikaAPIError("Malformed response: expected list")

        out: List[PaprikaTicker] = []
        for it in payload:
            if not isinstance(it, dict):
                continue
            paprika_id = (it.get("id") or "").strip()
            symbol = (it.get("symbol") or "").strip().upper()
            name = (it.get("name") or "").strip()

            quotes = it.get("quotes") or {}
            usd = quotes.get("USD") or {}
            market_cap = usd.get("market_cap")

            if not paprika_id or not symbol or market_cap is None:
                continue

            try:
                mcap = float(market_cap)
            except (TypeError, ValueError):
                continue

            out.append(
                PaprikaTicker(
                    paprika_id=paprika_id,
                    symbol=symbol,
                    name=name,
                    market_cap_usd=mcap,
                )
            )
        return out

    async def get_tickers(self) -> List[PaprikaTicker]:
        payload = await self._get_json("/v1/tickers")
        return self._parse_tickers(payload)


def _apply_symbol_aliases(symbol: str, aliases: Dict[str, str]) -> str:
    """
    Optional normalization layer.
    Example: {"MATIC":"POL"} if your matching policy requires rebrand remapping.
    """
    return aliases.get(symbol, symbol)


async def sync_tokens_to_database(
    repository: Repository,
    market_cap_threshold_usd: float,
    bybit_categories: List[str],
    symbol_aliases: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None,
) -> int:
    """
    Синхронизирует токены в БД:
    1) Получает данные из CoinPaprika (/v1/tickers)
    2) Фильтрует по порогу market cap
    3) Получает инструменты Bybit и строит universe baseCoin для категорий
    4) Делает пересечение по symbol==baseCoin
    5) Сохраняет токены в БД через очередь (upsert по symbol)
    6) Деактивирует токены, которых больше нет в списке
    
    Args:
        repository: Repository instance для работы с БД
        market_cap_threshold_usd: Минимальный market cap в USD
        bybit_categories: Список категорий Bybit для фильтрации
        symbol_aliases: Словарь алиасов для нормализации символов
        logger: Logger instance
        
    Returns:
        Количество синхронизированных токенов
    """
    log = logger or logging.getLogger("TokenSync")
    aliases = symbol_aliases or {}

    # Запускаем write worker если еще не запущен
    await repository.start_write_worker()

    async with CoinPaprikaClient(logger=log) as paprika, BybitClient(logger=log) as bybit:
        log.info("Fetching CoinPaprika tickers...")
        tickers = await paprika.get_tickers()

        candidates = [
            t for t in tickers
            if t.market_cap_usd >= market_cap_threshold_usd
        ]
        log.info("CoinPaprika candidates with mcap>=%.0f: %d", market_cap_threshold_usd, len(candidates))

        log.info("Fetching Bybit instruments baseCoin universe for categories: %s", bybit_categories)
        base_map = await bybit.get_base_coin_map(categories=bybit_categories)

        synced_symbols: Set[str] = set()
        tokens_to_sync = []

        for t in candidates:
            normalized_symbol = _apply_symbol_aliases(t.symbol, aliases)
            bybit_cats: Set[str] = base_map.get(normalized_symbol, set())

            if not bybit_cats:
                continue

            # Формируем bybit_symbol (для spot обычно SYMBOL+USDT)
            # Если в категориях есть "linear" - это фьючерсы, иначе spot
            bybit_symbol = f"{normalized_symbol}USDT"

            # Store categories as comma-separated string for filtering
            categories_str = ",".join(sorted(bybit_cats))

            tokens_to_sync.append({
                "symbol": normalized_symbol,
                "bybit_symbol": bybit_symbol,
                "name": t.name,
                "market_cap_usd": int(t.market_cap_usd),
                "bybit_categories": categories_str,
                "is_active": True,
            })

            synced_symbols.add(normalized_symbol)

        # Сортируем по market cap (от большего к меньшему)
        tokens_to_sync.sort(key=lambda x: x["market_cap_usd"], reverse=True)

        # Отправляем токены в очередь на запись (upsert)
        log.info(f"Enqueueing {len(tokens_to_sync)} tokens for upsert...")
        for token_data in tokens_to_sync:
            await repository.enqueue_upsert(
                model=Token,
                conflict_keys={"symbol": token_data["symbol"]},
                update_values={
                    "bybit_symbol": token_data["bybit_symbol"],
                    "name": token_data["name"],
                    "market_cap_usd": token_data["market_cap_usd"],
                    "bybit_categories": token_data["bybit_categories"],
                    "is_active": True,
                }
            )

        # Получаем все активные токены из БД
        log.info("Fetching active tokens from database...")
        active_tokens = await repository.get_all(
            model=Token,
            filters={"is_active": True}
        )

        # Деактивируем токены, которых больше нет в списке
        tokens_to_deactivate = 0
        for token in active_tokens:
            if token.symbol not in synced_symbols:
                await repository.enqueue_update(
                    model=Token,
                    filter_by={"symbol": token.symbol},
                    update_values={"is_active": False}
                )
                tokens_to_deactivate += 1

        if tokens_to_deactivate > 0:
            log.info(f"Deactivating {tokens_to_deactivate} tokens that are no longer tradable")

        # Ждем завершения всех операций в очереди
        log.info("Waiting for write queue to complete...")
        await repository._write_queue.join()

        log.info(f"Token sync completed: {len(tokens_to_sync)} tokens synced, "
                f"{tokens_to_deactivate} deactivated")
        
        return len(tokens_to_sync)


async def collect_tokens_tradable_on_bybit_with_mcap(
    market_cap_threshold_usd: float,
    bybit_categories: List[str],
    symbol_aliases: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    DEPRECATED: Используйте sync_tokens_to_database для записи в БД.
    
    Эта функция оставлена для обратной совместимости и возвращает данные в формате CSV.
    
    1) Pull tickers from CoinPaprika (/v1/tickers)
    2) Filter by market cap threshold
    3) Pull Bybit instruments and build baseCoin universe for categories
    4) Intersect by symbol==baseCoin
    5) Return rows ready to be written to CSV
    """
    log = logger or logging.getLogger("Collector")
    aliases = symbol_aliases or {}

    async with CoinPaprikaClient(logger=log) as paprika, BybitClient(logger=log) as bybit:
        log.info("Fetching CoinPaprika tickers...")
        tickers = await paprika.get_tickers()

        candidates = [
            t for t in tickers
            if t.market_cap_usd >= market_cap_threshold_usd
        ]
        log.info("CoinPaprika candidates with mcap>=%.0f: %d", market_cap_threshold_usd, len(candidates))

        log.info("Fetching Bybit instruments baseCoin universe for categories: %s", bybit_categories)
        base_map = await bybit.get_base_coin_map(categories=bybit_categories)

        rows: List[Dict[str, Any]] = []
        for t in candidates:
            normalized_symbol = _apply_symbol_aliases(t.symbol, aliases)
            bybit_cats: Set[str] = base_map.get(normalized_symbol, set())
            if not bybit_cats:
                continue

            rows.append(
                {
                    "symbol": t.symbol,
                    "symbol_normalized": normalized_symbol,
                    "name": t.name,
                    "paprika_id": t.paprika_id,
                    "market_cap_usd": f"{t.market_cap_usd:.2f}",
                    "bybit_categories": ",".join(sorted(bybit_cats)),
                }
            )

        rows.sort(key=lambda r: float(r["market_cap_usd"]), reverse=True)
        log.info("Matched tokens tradable on Bybit: %d", len(rows))
        return rows