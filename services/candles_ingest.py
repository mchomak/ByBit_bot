from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from bybit_client_new import BybitClient  # replace import if you renamed the file
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import settings
from db.models import Candle1m, Token
from db.repository import Repository


log = logging.getLogger("candles_ingest")


async def _load_symbols(repo: Repository, limit: int | None = None, category: str | None = None) -> List[str]:
    """
    Load symbols from DB, optionally filtered by Bybit category.

    Args:
        repo: Repository instance
        limit: Max number of symbols to return
        category: Filter by Bybit category (spot, linear, etc.)
                 Only returns symbols that have this category in bybit_categories
    """
    filters = {"is_active": True}
    tokens = await repo.get_all(Token, filters=filters, limit=limit)

    result = []
    for t in tokens:
        if not t.bybit_symbol:
            continue
        # If category filter is specified, check if token is available in that category
        if category and t.bybit_categories:
            # bybit_categories is comma-separated, e.g. "linear,spot"
            available_cats = [c.strip().lower() for c in t.bybit_categories.split(",")]
            if category.strip().lower() not in available_cats:
                continue
        result.append(t.bybit_symbol)

    return result


async def _cleanup_old_candles(repo: Repository, days: int = 5) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    await repo.delete_older_than(Candle1m, "timestamp", cutoff)


async def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

    database_url = settings.database_url
    bybit_category = os.getenv("BYBIT_CATEGORY", "spot").strip().lower()  # spot|linear|inverse|option
    ws_domain = os.getenv("BYBIT_WS_DOMAIN", "stream.bybit.com").strip()

    # SQLAlchemy async setup
    engine = create_async_engine(database_url, pool_pre_ping=True)
    session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    repo = Repository(session_factory)

    symbols = await _load_symbols(
        repo,
        limit=int(os.getenv("MAX_SYMBOLS", "0")) or None,
        category=bybit_category,
    )
    if not symbols:
        raise RuntimeError(
            f"No active symbols found in tokens table for category '{bybit_category}'. "
            "Populate tokens first or check BYBIT_CATEGORY setting."
        )

    log.info("Symbols loaded for category '%s': %d", bybit_category, len(symbols))

    async with BybitClient(ws_domain=ws_domain) as bybit:
        # 1) Bootstrap last 5 days of 1m history (REST)
        await bybit.seed_1m_history_to_db(
            category=bybit_category,
            symbols=symbols,
            session_factory=session_factory,
            candle_model=Candle1m,
            days=int(os.getenv("SEED_DAYS", "5")),
            concurrency=int(os.getenv("SEED_CONCURRENCY", "8")),
        )

        # 2) Optional cleanup (keep rolling 5-day window)
        await _cleanup_old_candles(repo, days=int(os.getenv("KEEP_DAYS", "5")))

        # 3) Start live WS ingestion (runs until cancelled)
        await bybit.stream_kline_1m_to_db(
            category=bybit_category,
            symbols=symbols,
            session_factory=session_factory,
            candle_model=Candle1m,
            flush_interval_s=float(os.getenv("FLUSH_INTERVAL_S", "1")),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
