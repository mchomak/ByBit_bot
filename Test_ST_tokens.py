from pybit.unified_trading import HTTP
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_high_risk_token(symbol: str, session: Optional[HTTP] = None) -> bool:
    """
    Проверяет, является ли токен высокорисковым (ST - Special Treatment) на Bybit.
    
    Args:
        symbol: Символ торговой пары (например, 'BTCUSDT')
        session: Объект HTTP сессии Bybit. Если None, создается новый.
    
    Returns:
        bool: True если токен высокорисковый (ST), False в противном случае
    """
    if session is None:
        session = HTTP(testnet=False)
    
    try:
        response = session.get_instruments_info(
            category="spot",
            symbol=symbol
        )
        
        if response['retCode'] != 0:
            logger.error(f"Ошибка API для {symbol}: {response['retMsg']}")
            return False
        
        if not response['result']['list']:
            logger.warning(f"Инструмент {symbol} не найден")
            return False
        
        instrument = response['result']['list'][0]
        
        # ОСНОВНОЙ МЕТОД: Проверка поля 'stTag'
        # stTag = '1' означает ST токен
        if 'stTag' in instrument:
            is_st = instrument['stTag'] == '1'
            if is_st:
                logger.info(f"{symbol} - ST токен (stTag=1)")
                return True
        
        # ДОПОЛНИТЕЛЬНЫЕ ИНДИКАТОРЫ (опционально)
        
        # Метод 2: Проверка symbolType
        # Некоторые ST токены могут иметь специальный тип
        if 'symbolType' in instrument and instrument['symbolType']:
            symbol_type = instrument['symbolType']
            if symbol_type in ['adventure', 'innovation']:
                logger.info(f"{symbol} - специальный тип: {symbol_type}")
                # Можно вернуть True, если считаем такие токены рисковыми
                # return True
        
        # Метод 3: Проверка innovation (оставляем как дополнительную проверку)
        if 'innovation' in instrument and instrument['innovation'] == '1':
            logger.info(f"{symbol} - innovation токен")
            return True
        
        # Метод 4: Проверка riskParameters
        # ST токены могут иметь более широкие ценовые лимиты
        if 'riskParameters' in instrument:
            risk = instrument['riskParameters']
            price_limit_y = float(risk.get('priceLimitRatioY', 0))
            # Если ценовой лимит > 0.05 (5%) - повышенный риск
            if price_limit_y > 0.05:
                logger.debug(f"{symbol} имеет широкий ценовой лимит: {price_limit_y}")
                # Это дополнительный индикатор, не всегда означает ST
        
        logger.debug(f"{symbol} - обычный токен")
        return False
        
    except Exception as e:
        logger.error(f"Ошибка при проверке {symbol}: {e}")
        return False


# Обновленный класс с правильной проверкой
class BybitRiskChecker:
    """
    Класс для проверки высокорисковых токенов с кэшированием результатов
    """
    
    def __init__(self, testnet: bool = False):
        self.session = HTTP(testnet=testnet)
        self._cache = {}
        self._st_tokens_cache = None
    
    def is_high_risk(self, symbol: str, use_cache: bool = True) -> bool:
        """
        Проверяет высокорисковость токена с поддержкой кэширования
        """
        if use_cache and symbol in self._cache:
            logger.debug(f"Результат для {symbol} взят из кэша")
            return self._cache[symbol]
        
        result = is_high_risk_token(symbol, self.session)
        self._cache[symbol] = result
        
        return result
    
    def get_all_st_tokens(self, refresh: bool = False) -> list:
        """
        Получает список всех ST токенов на Bybit Spot
        """
        if not refresh and self._st_tokens_cache is not None:
            return self._st_tokens_cache
        
        try:
            response = self.session.get_instruments_info(category="spot")
            
            if response['retCode'] != 0:
                logger.error(f"Ошибка при получении списка инструментов: {response['retMsg']}")
                return []
            
            st_tokens = []
            
            for instrument in response['result']['list']:
                symbol = instrument['symbol']
                
                # Проверяем stTag (основной индикатор ST)
                if instrument.get('stTag') == '1':
                    st_tokens.append(symbol)
                    self._cache[symbol] = True
                # Дополнительно: innovation токены
                elif instrument.get('innovation') == '1':
                    st_tokens.append(symbol)
                    self._cache[symbol] = True
                else:
                    self._cache[symbol] = False
            
            self._st_tokens_cache = st_tokens
            
            logger.info(f"Найдено {len(st_tokens)} ST токенов")
            return st_tokens
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка ST токенов: {e}")
            return []
    
    def get_token_risk_info(self, symbol: str) -> dict:
        """
        Получает детальную информацию о рисках токена
        
        Returns:
            dict: {
                'is_st': bool,
                'st_tag': str,
                'innovation': str,
                'margin_trading': str,
                'symbol_type': str,
                'price_limit_y': float
            }
        """
        try:
            response = self.session.get_instruments_info(
                category="spot",
                symbol=symbol
            )
            
            if response['retCode'] != 0 or not response['result']['list']:
                return {}
            
            instrument = response['result']['list'][0]
            
            risk_info = {
                'is_st': instrument.get('stTag') == '1',
                'st_tag': instrument.get('stTag', '0'),
                'innovation': instrument.get('innovation', '0'),
                'margin_trading': instrument.get('marginTrading', 'unknown'),
                'symbol_type': instrument.get('symbolType', ''),
                'price_limit_y': float(instrument.get('riskParameters', {}).get('priceLimitRatioY', 0))
            }
            
            return risk_info
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о {symbol}: {e}")
            return {}
    
    def clear_cache(self):
        """Очищает кэш"""
        self._cache.clear()
        self._st_tokens_cache = None
        logger.info("Кэш очищен")


# Тестирование
if __name__ == "__main__":
    print("=== Проверка токенов ===")
    
    checker = BybitRiskChecker(testnet=False)
    
    # Тестируем токены из вашего примера
    test_tokens = ['ETHUSDT', 'VENOMUSDT', 'MEMEFIUSDT', 'BTCUSDT']
    
    for token in test_tokens:
        is_st = checker.is_high_risk(token)
        status = "⚠️ ST (HIGH RISK)" if is_st else "✓ OK"
        print(f"{token:15} {status}")
        
        # Детальная информация
        info = checker.get_token_risk_info(token)
        if info:
            print(f"  └─ stTag={info['st_tag']}, "
                  f"innovation={info['innovation']}, "
                  f"margin={info['margin_trading']}, "
                  f"type='{info['symbol_type']}'")
    
    # Получаем полный список ST токенов
    print("\n=== Все ST токены ===")
    st_tokens = checker.get_all_st_tokens()
    print(f"Всего найдено: {len(st_tokens)}")
    if st_tokens:
        print(f"Первые 20: {st_tokens[:20]}")