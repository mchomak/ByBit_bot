"""
Пример использования обновленной функции синхронизации токенов.

Демонстрирует:
1. Настройку подключения к БД
2. Создание Repository
3. Синхронизацию токенов из CoinPaprika + Bybit в БД
4. Использование очереди для batch записи
"""

import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from paprika_bybit_matcher import sync_tokens_to_database
from db.repository import Repository
from db.models import Base, Token

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Основная функция для синхронизации токенов."""
    
    # Параметры подключения к БД
    DATABASE_URL = "postgresql+asyncpg://postgres:rWg3_0XFt1@localhost/ByBit_bot"
    
    # Создаем async engine
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,  # Установите True для отладки SQL запросов
        pool_size=10,
        max_overflow=20,
    )
    
    # Создаем таблицы (если их нет)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Создаем session factory
    async_session_factory = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    # Создаем Repository
    repository = Repository(session_factory=async_session_factory)
    
    try:
        # Параметры синхронизации
        MIN_MARKET_CAP_USD = 100_000_000  # 100M USD
        BYBIT_CATEGORIES = ["linear", "spot"]  # Фьючерсы и спот
        
        # Алиасы для переименованных токенов (опционально)
        SYMBOL_ALIASES = {
            "MATIC": "POL",  # Polygon ребрендинг
            # Добавьте другие алиасы при необходимости
        }
        
        logger.info("Starting token synchronization...")
        logger.info(f"Min market cap: ${MIN_MARKET_CAP_USD:,.0f}")
        logger.info(f"Bybit categories: {BYBIT_CATEGORIES}")
        
        # Синхронизируем токены
        synced_count = await sync_tokens_to_database(
            repository=repository,
            market_cap_threshold_usd=MIN_MARKET_CAP_USD,
            bybit_categories=BYBIT_CATEGORIES,
            symbol_aliases=SYMBOL_ALIASES,
            logger=logger,
        )
        
        logger.info(f"✅ Synchronization completed successfully!")
        logger.info(f"   Total tokens synced: {synced_count}")
        
        # Получаем статистику из БД
        all_tokens = await repository.get_all(Token)
        active_tokens = await repository.get_all(Token, filters={"is_active": True})
        inactive_tokens = await repository.get_all(Token, filters={"is_active": False})
        
        logger.info(f"   Total tokens in DB: {len(all_tokens)}")
        logger.info(f"   Active tokens: {len(active_tokens)}")
        logger.info(f"   Inactive tokens: {len(inactive_tokens)}")
        
        # Выводим топ-10 токенов по market cap
        if active_tokens:
            logger.info("\n📊 Top 10 tokens by market cap:")
            top_tokens = sorted(
                active_tokens,
                key=lambda t: t.market_cap_usd or 0,
                reverse=True
            )[:10]
            
            for i, token in enumerate(top_tokens, 1):
                mcap_formatted = f"${token.market_cap_usd:,.0f}" if token.market_cap_usd else "N/A"
                logger.info(
                    f"   {i:2d}. {token.symbol:6s} - {token.name:20s} - {mcap_formatted}"
                )
    
    except Exception as e:
        logger.error(f"❌ Error during synchronization: {e}", exc_info=True)
        raise
    
    finally:
        # Останавливаем write worker
        await repository.stop_write_worker()
        
        # Закрываем соединения
        await engine.dispose()
        logger.info("Database connections closed")


if __name__ == "__main__":
    # Запускаем синхронизацию
    asyncio.run(main())
