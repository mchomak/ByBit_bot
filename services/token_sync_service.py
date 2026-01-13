"""
Сервис для периодической синхронизации токенов.

Автоматически обновляет список торгуемых токенов в БД
на основе данных CoinPaprika и Bybit.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.paprika_bybit_matcher import sync_tokens_to_database
from db.repository import Repository
from db.models import Base, Token


class TokenSyncService:
    """
    Сервис для периодической синхронизации токенов в БД.

    Applies 4 main filters (daily):
    - Blacklist: Manual blacklist
    - ST: Special Treatment / high-risk tokens
    - LowMcap: Low market cap or not on Bybit
    - New: Token appeared < 24h ago (checked separately)

    StalePrice is NOT checked here - it's handled by StalePriceChecker (every 50 min).
    """

    def __init__(
        self,
        repository: Repository,
        market_cap_threshold_usd: float = 100_000_000,
        bybit_categories: Optional[list] = None,
        symbol_aliases: Optional[dict] = None,
        sync_interval_hours: int = 6,
        filter_st_tokens: bool = True,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Инициализация сервиса.

        Args:
            repository: Repository для работы с БД
            market_cap_threshold_usd: Минимальный market cap
            bybit_categories: Список категорий Bybit
            symbol_aliases: Словарь алиасов символов
            sync_interval_hours: Интервал синхронизации в часах
            filter_st_tokens: Фильтровать ST (высокорисковые) токены
            logger: Logger instance
        """
        self.repository = repository
        self.market_cap_threshold = market_cap_threshold_usd
        self.bybit_categories = bybit_categories or ["linear", "spot"]
        self.symbol_aliases = symbol_aliases or {}
        self.sync_interval = timedelta(hours=sync_interval_hours)
        self.filter_st_tokens = filter_st_tokens
        self.log = logger or logging.getLogger(self.__class__.__name__)

        self._task: Optional[asyncio.Task] = None
        self._is_running = False
        self._last_sync: Optional[datetime] = None
        self._last_sync_count: int = 0
    
    async def start(self) -> None:
        """Запускает сервис периодической синхронизации."""
        if self._is_running:
            self.log.warning("Service is already running")
            return
        
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())
        self.log.info(
            f"Token sync service started (interval: {self.sync_interval.total_seconds() / 3600:.1f}h, "
            f"min market cap: ${self.market_cap_threshold:,.0f})"
        )
    
    async def stop(self) -> None:
        """Останавливает сервис."""
        if not self._is_running:
            return
        
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        self.log.info("Token sync service stopped")
    
    async def _run_loop(self) -> None:
        """Основной цикл синхронизации."""
        # Первая синхронизация сразу при старте
        await self._sync_once()
        
        while self._is_running:
            try:
                # Ждем до следующей синхронизации
                await asyncio.sleep(self.sync_interval.total_seconds())
                
                if not self._is_running:
                    break
                
                # Выполняем синхронизацию
                await self._sync_once()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Error in sync loop: {e}", exc_info=True)
                # Ждем 5 минут перед повторной попыткой
                await asyncio.sleep(300)
    
    async def _sync_once(self) -> None:
        """Выполняет одну синхронизацию с обработкой ошибок."""
        try:
            self.log.info("Starting token synchronization...")
            start_time = datetime.now()

            synced_count = await sync_tokens_to_database(
                repository=self.repository,
                market_cap_threshold_usd=self.market_cap_threshold,
                bybit_categories=self.bybit_categories,
                symbol_aliases=self.symbol_aliases,
                logger=self.log,
                filter_st_tokens=self.filter_st_tokens,
            )

            duration = (datetime.now() - start_time).total_seconds()

            self._last_sync = datetime.now()
            self._last_sync_count = synced_count

            self.log.info(
                f"✅ Token sync completed in {duration:.1f}s: "
                f"{synced_count} tradable tokens"
            )

        except Exception as e:
            self.log.error(f"❌ Token sync failed: {e}", exc_info=True)

    async def sync_now(self) -> int:
        """
        Принудительная синхронизация вне расписания.

        Returns:
            Количество tradable токенов
        """
        self.log.info("Manual sync triggered")

        synced_count = await sync_tokens_to_database(
            repository=self.repository,
            market_cap_threshold_usd=self.market_cap_threshold,
            bybit_categories=self.bybit_categories,
            symbol_aliases=self.symbol_aliases,
            logger=self.log,
            filter_st_tokens=self.filter_st_tokens,
        )

        self._last_sync = datetime.now()
        self._last_sync_count = synced_count

        return synced_count

    def get_status(self) -> dict:
        """
        Возвращает текущий статус сервиса.

        Returns:
            Словарь со статусом сервиса
        """
        return {
            "is_running": self._is_running,
            "last_sync": self._last_sync.isoformat() if self._last_sync else None,
            "last_sync_count": self._last_sync_count,
            "sync_interval_hours": self.sync_interval.total_seconds() / 3600,
            "market_cap_threshold": self.market_cap_threshold,
            "bybit_categories": self.bybit_categories,
            "filter_st_tokens": self.filter_st_tokens,
        }


# Пример использования
async def example_usage():
    """Пример использования TokenSyncService."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Подключение к БД
    DATABASE_URL = "postgresql+asyncpg://postgres:rWg3_0XFt1@localhost/ByBit_bot"
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session_factory = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    repository = Repository(session_factory=async_session_factory)
    
    # Создаем и запускаем сервис
    service = TokenSyncService(
        repository=repository,
        market_cap_threshold_usd=100_000_000,  # 100M USD
        bybit_categories=["linear", "spot"],
        symbol_aliases={"MATIC": "POL"},
        sync_interval_hours=6,  # Каждые 6 часов
    )
    
    try:
        # Запускаем сервис
        await service.start()
        
        # Ждем (в реальном приложении сервис работает постоянно)
        await asyncio.sleep(3600)  # 1 час для примера
        
        # Проверяем статус
        status = service.get_status()
        print(f"Service status: {status}")
        
        # Принудительная синхронизация
        count = await service.sync_now()
        print(f"Manual sync completed: {count} tokens")
        
    finally:
        # Останавливаем сервис
        await service.stop()
        await repository.stop_write_worker()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(example_usage())