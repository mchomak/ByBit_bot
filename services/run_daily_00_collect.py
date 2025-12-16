from __future__ import annotations

import asyncio
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from paprika_bybit_matcher import collect_tokens_tradable_on_bybit_with_mcap


@dataclass(frozen=True)
class AppConfig:
    market_cap_threshold_usd: float
    bybit_categories: List[str]
    output_dir: Path
    run_immediately: bool
    symbol_aliases: Dict[str, str]


def load_config() -> AppConfig:
    threshold = float(os.getenv("MARKET_CAP_THRESHOLD_USD", "1000000"))
    categories_raw = os.getenv("BYBIT_CATEGORIES", "spot,linear")
    categories = [c.strip().lower() for c in categories_raw.split(",") if c.strip()]

    out_dir = Path(os.getenv("OUTPUT_DIR", "./out")).resolve()
    run_immediately = os.getenv("RUN_IMMEDIATELY", "0").strip() == "1"

    # Optional manual alias map via env (very lightweight: "MATIC=POL,FOO=BAR")
    aliases_raw = os.getenv("SYMBOL_ALIASES", "").strip()
    aliases: Dict[str, str] = {}
    if aliases_raw:
        for pair in aliases_raw.split(","):
            pair = pair.strip()
            if not pair or "=" not in pair:
                continue
            k, v = pair.split("=", 1)
            aliases[k.strip().upper()] = v.strip().upper()

    return AppConfig(
        market_cap_threshold_usd=threshold,
        bybit_categories=categories,
        output_dir=out_dir,
        run_immediately=run_immediately,
        symbol_aliases=aliases,
    )


def seconds_until_next_midnight(now: datetime) -> float:
    """
    Compute seconds until next 00:00:00 in TZ.
    """
    assert now.tzinfo is not None
    tomorrow = (now + timedelta(days=1)).date()
    next_midnight = datetime.combine(tomorrow, time(0, 0, 0), tzinfo=now.tzinfo)
    return max(0.0, (next_midnight - now).total_seconds())


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "symbol",
        "symbol_normalized",
        "name",
        "paprika_id",
        "market_cap_usd",
        "bybit_categories",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("DailyCollector")

    cfg = load_config()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Config: threshold=%.0f, categories=%s, tz=%s, output_dir=%s",
             cfg.market_cap_threshold_usd, cfg.bybit_categories, cfg.output_dir)


    rows = await collect_tokens_tradable_on_bybit_with_mcap(
        market_cap_threshold_usd=cfg.market_cap_threshold_usd,
        bybit_categories=cfg.bybit_categories,
        symbol_aliases=cfg.symbol_aliases,
        logger=log,
    )
    threshold_int = int(cfg.market_cap_threshold_usd)
    out_file = cfg.output_dir / f"bybit_tokens_mcap_ge_{threshold_int}_.csv"
    write_csv(rows, out_file)
    log.info("Saved %d rows -> %s", len(rows), str(out_file))



if __name__ == "__main__":
    asyncio.run(main())
