"""Main module for the Solana trading bot system."""

from __future__ import annotations
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.bootstrap_logging import setup_logging
from ml.ml_utils import create_all_tables, get_device, load_model
from bot.TelegramNotifier import TelegramNotifier
from tx_tools.transaction_processor import TransactionProcessor
from db.db_reader import DatabaseReader
from db.db_writer import DatabaseWriter
from config.config import *
from db.models import *
from api.get_price_tracker import PriceFetcher
from db.db_ohlcv_creator import OHLCVService
from api.gecko_ohlcv_new import OHLCVFFREEAPI
from ml.preprocessor import Preprocessor
from core.business_logic import decide_token
from core.create_daily_report import daily_report
from ml.lstm_predictor import LSTMPredictor
from tx_tools.raydium_trader import RaydiumTrader
from api.sol_price_client import SolanaTokenPriceClient
from core.main_config import config

from datetime import datetime, timezone
import asyncio
from pathlib import Path
from typing import Any, Dict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import joblib


class TraidingBot:
    """Main orchestrator for the Solana trading system."""

    # --- 1. INIT -----------------------------------------------------------------
    def __init__(self, config: str | Path) -> None:
        self.cfg: Dict[str, Any] = config
        self.loop = asyncio.get_event_loop()

        # queues
        self.db_queue: asyncio.Queue = asyncio.Queue(maxsize=1_000)
        self.message_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.trade_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.price_queue: asyncio.Queue = asyncio.Queue(maxsize=1_000)
        self.database_url = DATABASE_URL


    async def setup(self) -> None:
        """Heavy initialisation performed explicitly after __init__."""
        log = logger.bind(component="TraidingBot.setup")

        # 2.1 Logger
        self._configure_logger()
        log.info("Logger configured")

        # 2.1.1 Device
        self.device = get_device()
        log.info("Device selected: {}", self.device)

        # 2.2 Database engine & tables
        self.db_engine = create_async_engine(self.database_url, echo=False, future=True)
        self.SQLSession = sessionmaker(self.db_engine, class_=AsyncSession, expire_on_commit=False)
        await create_all_tables(self.db_engine)
        log.info("Database engine created and tables ensured")

        # 2.2.1 Reader & Writer
        self.db_reader = DatabaseReader(self.SQLSession)
        log.info("DatabaseReader initialized")
        self.db_writer = DatabaseWriter(self.SQLSession)
        log.info("DatabaseWriter initialized")

        # 2.4 ML-модель
        self.model, dataset_cfg, train_cfg, model_cfg = load_model(self.cfg["model_path"], device=self.device)
        log.info("ML model loaded from {}", self.cfg["model_path"])
        self.scaler = joblib.load(self.cfg["scaler_path"])
        log.info("Scaler loaded from {}", self.cfg["scaler_path"])

        # dataset params
        self.window_len  = dataset_cfg["window_len"]
        self.look_ahead  = dataset_cfg["look_ahead"]
        self.tp_prc      = dataset_cfg["tp_prc"]
        self.sl_prc      = dataset_cfg["sl_prc"]
        log.debug("Dataset cfg: window_len={}, look_ahead={}, tp_prc={}, sl_prc={}",
                  self.window_len, self.look_ahead, self.tp_prc, self.sl_prc)

        # thresholds & amounts
        self.threshold_buy = self.cfg.get("threshold_buy")
        self.threshold_sell = self.cfg.get("threshold_sell")
        self.buy_amount   = self.cfg.get("buy_amount", 0.0001)
        self.sell_prc  = self.cfg.get("sell_prc", 1.0)
        log.info("Trading parameters: threshold_buy={}, threshold_sell={}, buy_amt={}, sell_prc={}",
                 self.threshold_buy, self.threshold_sell, self.buy_amount, self.sell_prc)

        # 3) Load tokens list
        parsed_tokens = await self.db_reader.get_all(Token)
        self.token_map = {t.pool_address: t.token_address for t in parsed_tokens}
        log.info("Tokens loaded: {} pools", len(self.token_map))

        # 2.3 External services
        self.tg_notifier = TelegramNotifier(
            token= BOT_TOKEN,
            chat_id=self.cfg["tg_channel_id"],
            message_queue=self.message_queue,
            loop=self.loop,
            poll_interval=1.0,
        )       
        log.info("TelegramNotifier initialized")

        self.trader = RaydiumTrader(
            rpc_url=RPC_URL,
            secret_key_base58=PRIVATE_WALLET_KEY,
            wallet_address=WALLET_ADDRESS,
            confirm_level="finalized",
        )
        log.info("SolanaSwapService initialized (RPC={})", RPC_URL)

        self.trans_proc = TransactionProcessor(
            trader=self.trader,
            db_writer=self.db_writer,
            db_reader=self.db_reader,
            message_queue=self.message_queue,
            queue=self.trade_queue,
        )
        log.info("TransactionProcessor initialized")
        self.sol_price_client = SolanaTokenPriceClient()

        self.price_fetcher = PriceFetcher(
            sol_price_client=self.sol_price_client,
            db_writer=self.db_writer,
            db_reader=self.db_reader,
            num_workers=3,
            retry_delay=2.0,
            max_retries=5,
            queue=self.price_queue,
            tp_prc= self.tp_prc,
            sl_prc= self.sl_prc,
            trade_queue= self.trade_queue,
            rate_limit_delay= 5
        )
        log.info("PriceFetcher initialized (workers={}, retries={})",
                 self.price_fetcher.num_workers, self.price_fetcher.max_retries)

        self.ohlcv_service = OHLCVService(
            reader=self.db_reader,
            writer=self.db_writer,
            concurrency=5,
            tokens_table=Token,
        )
        log.info("OHLCVService initialized (concurrency=5)")

        self.free_api_fetcher = OHLCVFFREEAPI(
            tokens=self.token_map,
            save_path="free_api_data",
            db_table= OHLCV_API,
            db_writer=self.db_writer,
            timeframe="hour",
            aggregate=1,
            limit=1000,
            fetch_historical=True,
            update_value=True,
            SQLSession=self.SQLSession,
        )
        log.info("OHLCVFFREEAPI initialized (historical={})", self.free_api_fetcher.fetch_historical)

        self.preproc = Preprocessor(
            dataset_cfg=dataset_cfg,
            table=OHLCV,
            scaler=self.scaler,
            prod=True,
            SQLSession=self.SQLSession,
            db_reader=self.db_reader
        )
        self.predictor = LSTMPredictor(
            preproc = self.preproc,
            db_reader=self.db_reader,
            model_path=self.cfg["model_path"],
            t_scaler_path=self.cfg.get("t_scaler_path", None),
        )
        self.preproc.tokens = self.token_map.keys()
        log.info("Preprocessor initialized (prod mode)")

        # Start all classes that require async start
        self._writer_task = self.loop.create_task(self.db_writer.start())
        log.info("DatabaseWriter worker started")

        self._trade_task = self.loop.create_task(self.trans_proc.start())
        log.info("TransactionProcessor worker task started")

        await self.price_fetcher.start()
        log.info("PriceFetcher workers started")

        await self.tg_notifier.start()
        log.info("TelegramNotifier polling started")

        # 2.5 APScheduler
        self.scheduler = AsyncIOScheduler(timezone="UTC", event_loop=self.loop)
        self._register_jobs()
        self.scheduler.start()
        log.info("AsyncIOScheduler started with jobs: {}", list(self.scheduler.get_jobs()))

        # 1) Fetch all historical OHLCV
        self.free_api_fetcher.fetch_historical = False
        # await self.free_api_fetcher.fetch_all() 
        log.info("Historical OHLCV data fetched; switching to hourly mode")

        # 2.6 Ready
        self.buy_mode = self.cfg.get("buy_mode")
        await self.message_queue.put("ℹ️ Trading bot initialised and running 🚀")
        log.success("Setup completed successfully")


    def _configure_logger(self) -> None:
        # Берём путь из конфига (или по умолчанию ./logs) и оборачиваем в Path
        log_dir = Path(self.cfg.get("log_path", "./logs"))
        # Убедимся, что папка существует
        log_dir.mkdir(parents=True, exist_ok=True)
        setup_logging(log_dir, telegram_queue=self.message_queue)


    async def shutdown(self) -> None:
        """
        Graceful shutdown: останавливаем scheduler, таски, закрываем сервисы.
        """
        logger.bind(component="TraidingBot").info("Shutting down TradingBot…")

        # 1) Остановить APScheduler
        self.scheduler.shutdown(wait=False)

        # 2) Отменить таск обработки транзакций
        if hasattr(self, "_trade_task"):
            self._trade_task.cancel()
            try:
                await self._trade_task
            except asyncio.CancelledError:
                pass

        # 4) Остановить PriceFetcher
        await self.price_fetcher.stop()

        # 5) Остановить TelegramNotifier
        await self.tg_notifier.stop()

        logger.bind(component="TraidingBot").success("Shutdown complete")


    def _register_jobs(self) -> None:
        """Создаём cron/interval-задачи согласно ТЗ."""
        self.scheduler.add_job(
            self.update_prices,
            "interval",
            seconds=120,
            id="prices_2m",
            max_instances=1,
            misfire_grace_time=120,
            args=[False],
        )

        self.scheduler.add_job(
            self.update_prices,
            "interval",
            seconds=30,
            id="holds_1m",
            max_instances=1,
            misfire_grace_time=30,
            args=[True],
        )

        # создаём ежечасный джоб
        self.scheduler.add_job(
            self.build_ohlcv_and_predict,
            trigger="cron",
            minute=0,    
            id="ohlcv_1h",
            max_instances=1,
            misfire_grace_time=300,
            replace_existing=True
        )
        # дневные задачи
        self.scheduler.add_job(
            self.daily_tasks,
            "cron",
            hour=0,
            id="daily",
            max_instances=1,
            misfire_grace_time=300
        )

        # недельная статистика
        self.scheduler.add_job(
            self.weekly_stats,
            "cron",
            day_of_week="mon",
            hour=0,
            id="weekly",
            max_instances=1,
            misfire_grace_time=300
        )

        self.scheduler.add_job(
            self.free_api_fetcher.fetch_all,
            "cron",
            minute="0",
            id="freeapi_hourly",
            max_instances=1,
            misfire_grace_time=300
        )


    async def update_prices(self, hold = False) -> None:
        """
        Задача APScheduler: каждые 60 секунд кидает все токены в очередь PriceFetcher
        и ждёт обработки.
        """
        log = logger.bind(job="update_prices", component="TraidingBot")
        log.info("Starting price update cycle")

        try:
            if hold:
                await self.price_fetcher.add_hold_tokens()
            else:
                await self.price_fetcher.add_other_tokens(self.token_map)
            log.debug(f"Enqueued {len(self.token_map)} tasks")

            # Ждём, когда очередь опустеет
            await self.price_queue.join()
            log.info("Price update cycle completed successfully")

        except Exception:
            log.exception("Error during price update cycle")


    async def _compute_dynamic_buy_amount(self, prob = None) -> float:
        """
        Считает динамический депозит на токен:
        (SOL_на_кошельке + сумма amount из HOLD) - резерв_на_комиссии) / max_tokens.
        Если max_tokens не задан — возвращает текущий self.buy_amount из конфига.
        """
        log = logger.bind(component="TraidingBot", job="compute_buy_amount")
        if self.buy_mode == "max_tokens":
            # поддерживаем несколько возможных ключей из конфига
            max_tokens = self.cfg.get("max_tokens_cnt", 0)

            if not max_tokens or max_tokens <= 0:
                log.debug("max_tokens не задан, используем статический buy_amount={}", self.buy_amount)
                return self.buy_amount

            # 1) баланс SOL на кошельке
            sol_balance = await self.trader.get_solana_balance()

            # 2) сумма amount по всем удержаниям (в SOL)
            holds = await self.db_reader.get_all(Hold)
            now_t_cnt = len(holds)
            diff = max_tokens - now_t_cnt
            fee_reserve = 0.03
            if diff > 0:
                available = sol_balance - fee_reserve
                per_token = round(available / diff, 3)
                log.info(
                    "Dynamic buy calc ⇒ balanceSOL={:.6f}, available={:.6f}, diff={:.6f}, fee_reserve={:.6f}, "
                    "max_tokens={}, buy_amount={:.6f}",
                    sol_balance, available, diff, fee_reserve, max_tokens, per_token
                )
                return per_token
        
            else:
                log.info(f"The maximum number of tokens has been reached: {now_t_cnt}.")
                return self.buy_amount
        
        elif self.buy_mode == "correlation":
            sol_per_trade = self.cfg.get("sol_per_trade", 1.0)
            sol_balance = await self.trader.get_solana_balance()
            avalible_sol = sol_balance - 0.05  # резерв на комиссии
            per_pos_cap_max = self.cfg.get("max_alloc_per_pos", 0.1)

            # Если прогноз ниже порога — не покупаем
            if prob < self.threshold_buy or avalible_sol <= 0.0:
                return 0.0

            # Вес сигнала и целевая аллокация внутри [B, per_pos_cap_max]
            denom = max(1e-9, 1.0 - self.threshold_buy)
            w = (prob - self.threshold_buy) / denom
            if w < 0.0: w = 0.0
            if w > 1.0: w = 1.0

            cap_delta = max(0.0, float(per_pos_cap_max) - sol_per_trade)
            target_sol = sol_per_trade + w * cap_delta

            # Финальная сумма ограничена доступным кэшем
            alloc_sol = min(target_sol, avalible_sol)

            # Любые микроскопические значения отсечём
            return alloc_sol if alloc_sol > 1e-9 else 0.0


    async def build_ohlcv_and_predict(self) -> None:
        log = logger.bind(job="build_ohlcv", component="TraidingBot")
        log.info("Starting OHLCV build + predict")
        try:
            # 1) Построить OHLCV и загрузить данные
            await self.ohlcv_service.run()
            data_by_token = await self.preproc.load_all_data()

            # 2) Получить список пулов из HOLD
            holds = await self.db_reader.get_all(Hold)
            hold_pools_raw = [h.pool_address for h in holds if getattr(h, "pool_address", None)]

            # 3) Разделить на пулы из HOLD и остальные (из тех, по которым есть данные)
            all_pools_with_data = list(data_by_token.keys())
            hold_pools = [p for p in hold_pools_raw if p in data_by_token]
            other_pools = [p for p in all_pools_with_data if p not in set(hold_pools)]

            log.info("Prediction order ⇒ in HOLD: {}, others: {}", len(hold_pools), len(other_pools))

            async def _process_one(pool_address: str) -> None:
                df = data_by_token.get(pool_address)
                if df is None:
                    log.warning("No data for pool {}, skipping", pool_address)
                    return
                
                if len(df) < self.preproc.window_len:
                    log.warning("Not enough data for {}: {}/{}", pool_address, len(df), self.preproc.window_len)
                    return

                df_window = df.iloc[-self.preproc.window_len :]
                # X = self.preproc.transform_window(df_window).unsqueeze(0).to(self.device)

                prob = self.predictor.predict(df_window)

                log.info("Model p for {}: {:.2f}", pool_address, prob)

                # --- записываем лог предсказания в базу ---
                token_addr = self.token_map.get(pool_address)
                if not token_addr:
                    log.warning(
                        "token_address not found in token_map for pool {}, logging with token_address=None",
                        pool_address
                    )
                timestamp = datetime.now(timezone.utc)
                await self.db_writer.enqueue_insert(
                    PredictLog,
                    {
                        "token_address": token_addr,
                        "pool_address": pool_address,
                        "time": timestamp,
                        "prob": prob,
                        "threshold": self.threshold_buy,
                    }
                )
                log.debug(
                    "Saved prediction for {} (token {}) at {}: prob={:.2f}, threshold_buy={:.2f}",
                    pool_address, token_addr, timestamp, prob, self.threshold_buy
                )

                try:
                    self.buy_amount = await self._compute_dynamic_buy_amount(prob)
                    log.info(f"now buy_amount {self.buy_amount}")

                except Exception as e:
                    log.exception(
                        f"Не удалось посчитать динамический buy_amount — оставляем прежнее значение: {self.buy_amount}, ошибка: {e}",
                    )

                # использование актуального (возможно динамически пересчитанного) self.buy_amount
                await decide_token(
                    pool_address   = pool_address,
                    prob           = prob,
                    threshold_buy  = self.threshold_buy,
                    threshold_sell = self.threshold_sell,
                    buy_amount     = self.buy_amount,
                    sell_prc       = self.sell_prc,
                    db_reader      = self.db_reader,
                    trade_queue    = self.trade_queue,
                    token_map      = self.token_map,
                )

            # 4) Сначала — токены из HOLD (по идее только Sell)
            for pool in hold_pools:
                await _process_one(pool)
            
            # 5) Затем — остальные токены (тут только Buy)
            for pool in other_pools:
                await _process_one(pool)

            log.success("OHLCV build + predict completed")

        except Exception:
            log.exception("Error in build_ohlcv_and_predict")


    async def daily_tasks(self):
        await daily_report(
            logger=logger.bind(job="daily_report", component="TraidingBot"),
            report_path=self.cfg.get("report_path", "./reports"),
            log_path=self.cfg.get("log_path", "./logs"),
            db_reader=self.db_reader,
            threshold_buy=self.threshold_buy,
            message_queue=self.message_queue
        )


    async def weekly_stats(self) -> None: ...


async def main(config):
    bot = TraidingBot(config)
    await bot.setup()

    # блокируемся «навсегда»
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(config))