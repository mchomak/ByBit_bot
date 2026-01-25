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
from db.models import Token, AllToken, BlacklistedToken


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
    filter_st_tokens: bool = True,
    volume_threshold_usd: float = 700_000,
) -> int:
    """
    Синхронизирует токены в БД (daily sync).

    ВАЖНО: Начинаем с Bybit (источник правды для торговли), затем проверяем mcap через CoinPaprika.

    Applies 5 main filters (NOT StalePrice - that's separate):
    1) Available on Bybit (USDT pairs in Trading status)
    2) Market cap threshold (via CoinPaprika if available)
    3) Blacklist
    4) ST (Special Treatment / high-risk)
    5) 24h volume >= threshold

    Two-table structure:
    - all_tokens: stores ALL tokens with deactivation_reason
    - tokens: stores only tradable tokens (passed all 5 filters)

    StalePrice is NOT checked here - it runs separately every 50 minutes.

    Args:
        repository: Repository instance для работы с БД
        market_cap_threshold_usd: Минимальный market cap в USD
        bybit_categories: Список категорий Bybit для фильтрации
        symbol_aliases: Словарь алиасов для нормализации символов
        logger: Logger instance
        filter_st_tokens: Исключать ST (Special Treatment) токены
        volume_threshold_usd: Минимальный 24h объём торгов в USD (default $700k)

    Returns:
        Количество активных (tradable) токенов
    """
    log = logger or logging.getLogger("TokenSync")
    aliases = symbol_aliases or {}

    # Запускаем write worker если еще не запущен
    await repository.start_write_worker()

    # Получаем чёрный список токенов
    blacklisted_tokens = await repository.get_all(model=BlacklistedToken)
    blacklisted_symbols: Set[str] = {t.symbol.upper() for t in blacklisted_tokens}
    if blacklisted_symbols:
        log.info(f"Loaded {len(blacklisted_symbols)} blacklisted tokens: {sorted(blacklisted_symbols)}")

    async with CoinPaprikaClient(logger=log) as paprika, BybitClient(logger=log) as bybit:
        # ============================================================
        # 1. START FROM BYBIT - get all USDT trading pairs
        # ============================================================
        log.info("Fetching Bybit USDT trading pairs for categories: %s", bybit_categories)

        # Collect all Bybit USDT tokens with their categories
        bybit_tokens: Dict[str, Dict[str, Any]] = {}  # baseCoin -> {categories, bybit_symbol}
        for cat in bybit_categories:
            async for inst in bybit.iter_instruments(category=cat, status="Trading"):
                if inst.quote_coin.upper() != "USDT":
                    continue
                base = inst.base_coin.upper()
                if base not in bybit_tokens:
                    bybit_tokens[base] = {
                        "categories": set(),
                        "bybit_symbol": inst.symbol,
                    }
                bybit_tokens[base]["categories"].add(cat)

        log.info("Found %d USDT trading pairs on Bybit", len(bybit_tokens))

        # ============================================================
        # 2. Get CoinPaprika data for market cap filtering
        # ============================================================
        log.info("Fetching CoinPaprika tickers for market cap data...")
        tickers = await paprika.get_tickers()

        # Build symbol -> mcap mapping (apply aliases in reverse too)
        paprika_mcap: Dict[str, float] = {}
        paprika_names: Dict[str, str] = {}
        reverse_aliases = {v: k for k, v in aliases.items()}

        for t in tickers:
            symbol = t.symbol.upper()
            paprika_mcap[symbol] = t.market_cap_usd
            paprika_names[symbol] = t.name
            # Also check if this symbol is an alias target
            if symbol in reverse_aliases:
                orig = reverse_aliases[symbol].upper()
                paprika_mcap[orig] = t.market_cap_usd
                paprika_names[orig] = t.name

        log.info("CoinPaprika has %d tickers with market cap data", len(paprika_mcap))

        # ============================================================
        # 3. Get ST (Special Treatment) tokens
        # ============================================================
        st_tokens: Set[str] = set()
        if filter_st_tokens:
            log.info("Checking for ST (Special Treatment / high-risk) tokens...")
            st_tokens = await bybit.get_st_tokens(categories=bybit_categories)
            if st_tokens:
                log.info("Found %d ST tokens on Bybit: %s", len(st_tokens), sorted(st_tokens)[:20])

                # Auto-add ST tokens to permanent blacklist
                new_blacklisted = 0
                for st_symbol in st_tokens:
                    if st_symbol.upper() not in blacklisted_symbols:
                        await repository.enqueue_upsert(
                            model=BlacklistedToken,
                            conflict_keys={"symbol": st_symbol.upper()},
                            update_values={
                                "bybit_symbol": f"{st_symbol.upper()}USDT",
                                "reason": "ST (auto-detected high-risk token)",
                                "added_by": "system",
                            }
                        )
                        blacklisted_symbols.add(st_symbol.upper())
                        new_blacklisted += 1

                if new_blacklisted > 0:
                    log.info("Added %d new ST tokens to permanent blacklist", new_blacklisted)

        # ============================================================
        # 4. Get 24h volumes for volume filtering
        # ============================================================
        log.info("Fetching 24h trading volumes...")
        volumes_24h: Dict[str, float] = {}
        for cat in bybit_categories:
            cat_volumes = await bybit.get_24h_volumes(category=cat)
            volumes_24h.update(cat_volumes)
        log.info("Fetched 24h volumes for %d symbols", len(volumes_24h))

        # ============================================================
        # 5. Process each Bybit token and apply filters
        # ============================================================
        all_tokens_data: List[Dict[str, Any]] = []
        active_tokens_data: List[Dict[str, Any]] = []

        blacklisted_count = 0
        st_filtered_count = 0
        low_volume_count = 0
        low_mcap_count = 0
        no_mcap_count = 0

        for base_coin, info in bybit_tokens.items():
            # Apply symbol aliases
            normalized_symbol = _apply_symbol_aliases(base_coin, aliases)
            bybit_symbol = info["bybit_symbol"]
            categories_str = ",".join(sorted(info["categories"]))

            # Get market cap from CoinPaprika
            mcap = paprika_mcap.get(normalized_symbol.upper(), 0)
            name = paprika_names.get(normalized_symbol.upper(), normalized_symbol)

            token_data = {
                "symbol": normalized_symbol,
                "bybit_symbol": bybit_symbol,
                "name": name,
                "market_cap_usd": int(mcap),
                "bybit_categories": categories_str,
            }

            # Determine deactivation reason (priority order)
            deactivation_reason = None

            # Filter 1: Blacklist (highest priority)
            if normalized_symbol.upper() in blacklisted_symbols:
                deactivation_reason = "Blacklist"
                blacklisted_count += 1
            # Filter 2: ST tokens
            elif normalized_symbol.upper() in st_tokens:
                deactivation_reason = "ST"
                st_filtered_count += 1
            # Filter 3: Market cap (skip if not on CoinPaprika)
            elif mcap == 0:
                deactivation_reason = "NoMcapData"
                no_mcap_count += 1
            elif mcap < market_cap_threshold_usd:
                deactivation_reason = "LowMcap"
                low_mcap_count += 1
            # Filter 4: Volume
            else:
                volume_24h = volumes_24h.get(bybit_symbol, 0)
                if volume_24h < volume_threshold_usd:
                    deactivation_reason = "LowVolume"
                    low_volume_count += 1

            # Add to all_tokens with reason
            all_tokens_data.append({
                **token_data,
                "is_active": deactivation_reason is None,
                "deactivation_reason": deactivation_reason,
            })

            # Add to active tokens only if no deactivation reason
            if deactivation_reason is None:
                active_tokens_data.append(token_data)

        if blacklisted_count > 0:
            log.info(f"Filtered {blacklisted_count} blacklisted tokens")
        if st_filtered_count > 0:
            log.info(f"Filtered {st_filtered_count} ST (high-risk) tokens from candidates")
        if no_mcap_count > 0:
            log.info(f"Filtered {no_mcap_count} tokens with no market cap data on CoinPaprika")
        if low_mcap_count > 0:
            log.info(f"Filtered {low_mcap_count} tokens with market cap < ${market_cap_threshold_usd:,.0f}")
        if low_volume_count > 0:
            log.info(f"Filtered {low_volume_count} tokens with 24h volume < ${volume_threshold_usd:,.0f}")

        # Sort by market cap
        all_tokens_data.sort(key=lambda x: x["market_cap_usd"], reverse=True)
        active_tokens_data.sort(key=lambda x: x["market_cap_usd"], reverse=True)

        # Get current active symbols for deactivation check
        active_symbols = {t["symbol"] for t in active_tokens_data}

        # ============================================================
        # 1. Update all_tokens table (full list with reasons)
        # ============================================================
        log.info(f"Updating all_tokens table: {len(all_tokens_data)} tokens...")
        for token_data in all_tokens_data:
            await repository.enqueue_upsert(
                model=AllToken,
                conflict_keys={"symbol": token_data["symbol"]},
                update_values={
                    "bybit_symbol": token_data["bybit_symbol"],
                    "name": token_data["name"],
                    "market_cap_usd": token_data["market_cap_usd"],
                    "bybit_categories": token_data["bybit_categories"],
                    "is_active": token_data["is_active"],
                    "deactivation_reason": token_data["deactivation_reason"],
                }
            )

        # Deactivate tokens no longer in the list (low mcap)
        existing_all_tokens = await repository.get_all(model=AllToken)
        current_symbols = {t["symbol"] for t in all_tokens_data}
        for token in existing_all_tokens:
            if token.symbol not in current_symbols:
                await repository.enqueue_update(
                    model=AllToken,
                    filter_by={"symbol": token.symbol},
                    update_values={
                        "is_active": False,
                        "deactivation_reason": "LowMcap",
                    }
                )

        # ============================================================
        # 2. Update tokens table (only active/tradable tokens)
        # ============================================================
        log.info(f"Updating tokens table: {len(active_tokens_data)} active tokens...")

        # Upsert active tokens (reset is_active to True during daily sync)
        for token_data in active_tokens_data:
            await repository.enqueue_upsert(
                model=Token,
                conflict_keys={"symbol": token_data["symbol"]},
                update_values={
                    "bybit_symbol": token_data["bybit_symbol"],
                    "name": token_data["name"],
                    "market_cap_usd": token_data["market_cap_usd"],
                    "bybit_categories": token_data["bybit_categories"],
                    "is_active": True,  # Reset for StalePrice checker
                }
            )

        # Remove tokens that are no longer active from tokens table
        existing_tokens = await repository.get_all(model=Token)
        tokens_removed = 0
        for token in existing_tokens:
            if token.symbol not in active_symbols:
                await repository.enqueue_delete(
                    model=Token,
                    filter_by={"symbol": token.symbol}
                )
                tokens_removed += 1

        if tokens_removed > 0:
            log.info(f"Removed {tokens_removed} tokens from tradable list")

        # Ждем завершения всех операций в очереди
        log.info("Waiting for write queue to complete...")
        await repository._write_queue.join()

        log.info(
            f"Token sync completed: {len(all_tokens_data)} total on Bybit, "
            f"{len(active_tokens_data)} tradable, "
            f"{blacklisted_count} blacklisted, "
            f"{st_filtered_count} ST, "
            f"{no_mcap_count} no mcap data, "
            f"{low_mcap_count} low mcap, "
            f"{low_volume_count} low volume"
        )

        return len(active_tokens_data)


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