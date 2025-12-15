"""Создаёт ежедневный отчёт по логам, прогнозам и сделкам."""

from __future__ import annotations
import sys
import os
from pathlib import Path
from typing import Dict
import pandas as pd
import numpy as np
import plotly.express as px
from collections import Counter
from datetime import datetime, time, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.models import PredictLog, TransactionLog


async def daily_report(logger, report_path, log_path, db_reader, threshold_buy, message_queue) -> None:
    """
    Собирает статистику по логам, прогнозам и сделкам за вчерашний день,
    генерирует графики и сохраняет отчёт в папку reports/YYYY-MM-DD.
    """
    log = logger.bind(component="TraidingBot.daily_report")

    report_path = Path(report_path)
    log_path    = Path(log_path)

    report_date = (datetime.utcnow().date() - timedelta(days=1))
    start_dt = datetime.combine(report_date, time.min)
    end_dt   = start_dt + timedelta(days=1)

    # папка для отчёта
    folder = report_path / report_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)

    # 1) Статистика по логам
    level_counters: Dict[str, Counter] = {
        "WARNING": Counter(),
        "ERROR":   Counter(),
        "CRITICAL": Counter(),
    }
    for f in log_path.glob("*.log"):
        for line in f.open(encoding="utf-8", errors="ignore"):
            try:
                date_str, lvl, msg = line.split("|", 2)
                ts = datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S")
            except:
                continue

            lvl = lvl.strip()
            if ts >= start_dt and ts < end_dt and lvl in level_counters:
                level_counters[lvl][msg.strip()] += 1

    # сохраняем текстовую часть
    txt = []
    txt.append(f"=== Логи за {report_date} ===\n")
    for lvl in ("CRITICAL","ERROR","WARNING"):
        cnt = sum(level_counters[lvl].values())
        txt.append(f"{lvl}: {cnt}\n")
        for msg, n in level_counters[lvl].most_common():
            txt.append(f"  {n:4d} × {msg}\n")

        txt.append("\n")

    (folder / "logs_stats.txt").write_text("".join(txt), encoding="utf-8")
    log.info("Saved log stats")

    # 2) Статистика прогнозов
    preds = await db_reader.get_all(
        PredictLog,
        filters={
            "time__gte": start_dt,
            "time__lt":  end_dt,
        }
    )
    probs = np.array([p.prob for p in preds], dtype=float)
    if probs.size:
        avg   = probs.mean()
        med   = np.median(probs)
        pmax  = probs.max()
        pmin  = probs.min()
        thr   = threshold_buy
        almost = ((probs >= thr - 0.02) & (probs < thr)).sum()
        above  = (probs >= thr).sum()
    else:
        avg = med = pmax = pmin = above = almost = 0

    dfp = pd.DataFrame({
        "metric": ["avg","median","max","min",f">={thr}",f"≈{thr}±0.02"],
        "value":  [avg,med,pmax,pmin,above,almost]
    })
    dfp.to_csv(folder / "predict_stats.csv", index=False)
    log.info("Saved predict stats")

    # 3) Статистика по транзакциям
    # 3.1 По логам ошибок/успехов (берём те же level_counters)
    # 3.2 Из БД по TransactionLog
    txs = await db_reader.get_all(
        TransactionLog,
        filters={
            "timestamp__gte": start_dt,
            "timestamp__lt":  end_dt,
        }
    )
    df = pd.DataFrame([{
        "time":       t.timestamp,
        "trade_type": t.trade_type,
        "profit":     t.profit or 0.0
    } for t in txs])
    if df.empty:
        df = pd.DataFrame(columns=["time","trade_type","profit"])

    # счётчики
    total = len(df)
    buys  = (df.trade_type=="buy").sum()
    sells = (df.trade_type=="sell").sum()

    # пиковый час
    df["hour"] = df.time.dt.hour
    peak_hour = int(df["hour"].value_counts().idxmax()) if total else None

    # график числа сделок по времени
    fig1 = px.histogram(
        df,
        x="time",
        color="trade_type",
        nbins=24,
        title=f"Транзакции за {report_date}"
    )
    fig1.write_html(str(folder / "tx_count.html"), auto_open=False)

    # график прибыли к времени
    df_sells = df[df.trade_type=="sell"]
    if not df_sells.empty:
        df_sells = df_sells.sort_values("time")
        df_sells["cum_profit"] = df_sells.profit.cumsum()
        fig2 = px.line(
            df_sells,
            x="time",
            y="cum_profit",
            title=f"Кумулятивная прибыль за {report_date}"
        )
        fig2.write_html(str(folder / "profit.html"), auto_open=False)
        total_profit = df_sells.profit.sum()
    else:
        total_profit = 0.0

    # сохраняем summary
    summ = [
        f"=== Транзакции за {report_date} ===\n",
        f"Всего: {total}, buy: {buys}, sell: {sells}\n",
        f"Пиковый час: {peak_hour}\n",
        f"Общая прибыль: {total_profit:.2f}\n",
    ]
    (folder / "tx_summary.txt").write_text("".join(summ), encoding="utf-8")
    log.info("Saved transaction stats")

    # 4) Причины закрытия (из логов decide_token)
    reasons = Counter()
    for f in log_path.glob("*.log"):
        for line in f.open(encoding="utf-8", errors="ignore"):
            if report_date.isoformat() in line and "SELL signal for" in line:
                part = line.split("SELL signal for",1)[1]
                # " pool: reason →"
                reason = part.split("→")[0].strip()
                reasons[reason] += 1

    with open(folder / "sell_reasons.txt","w",encoding="utf-8") as fh:
        fh.write("=== Причины SELL ===\n")
        for reason, cnt in reasons.most_common():
            fh.write(f"{cnt:4d} × {reason}\n")

    log.info("Saved sell reasons")

    # 5) Отправляем в Telegram краткий отчёт
    summary_msg = (
        f"📊 Ежедневный отчёт за {report_date}:\n"
        f"Логи: W={len(level_counters['WARNING'])}, "
        f"E={len(level_counters['ERROR'])}, "
        f"C={len(level_counters['CRITICAL'])}\n"
        f"Прогнозов: {len(probs)}, транзакций: {total}, профит: {total_profit:.2f}"
    )
    await message_queue.put(summary_msg)
    log.success("Daily report done")
