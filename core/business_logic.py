"""Business logic for deciding whether to buy or sell tokens based on model predictions and current holdings."""

from typing import Optional, Dict
import asyncio
import os
import sys
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.models import Hold
from db.db_reader import DatabaseReader


log = logger.bind(component="decide_token")


async def decide_token(
    pool_address: str,
    prob: float,
    threshold_buy: float,
    threshold_sell: float,
    buy_amount: float,
    sell_prc: float,
    db_reader: DatabaseReader,
    trade_queue: asyncio.Queue,
    token_map: Dict[str,str],
) -> None:
    """
    1) Если нет позиции в Hold и prob > threshold_buy → BUY amount_for_buy.
    2) Если позиция есть → проверяем TP/SL и модельный откат → SELL с sell_percent.
    """

    token_address = token_map[pool_address]
    hold = await db_reader.get_by_fields(Hold, {"pool_address": pool_address})

    # — BUY —
    if hold is None:
        if prob >= threshold_buy:
            log.info("BUY signal for {} (p={:.2f} > th={:.2f})", pool_address, prob, threshold_buy)
            await trade_queue.put({
                "pool_address":   pool_address,
                "token_address":  token_address,
                "action":         "buy",
                "amount_for_buy": buy_amount,
                "retries":       0,
                "prob": prob,
                "threshold": threshold_buy
            })
        else:
            log.debug("SKIP {} (p={:.2f} ≤ th={:.2f})", pool_address, prob, threshold_buy)

        return

    if prob < threshold_sell:
        sell_reason = f"model_revoked: p={prob:.2f} < th={threshold_sell:.2f}"
        log.info("SELL signal for {}: {} → sell_prc={:.2%}", pool_address, sell_reason, sell_prc)
        await trade_queue.put({
            "pool_address":  pool_address,
            "token_address": token_address,
            "action":        "sell",
            "sell_percent":  sell_prc,
            "retries":       0
        })
    else:
        log.debug("HOLD {} (p={:.2f}) — no action", pool_address, prob)
