"""
Bybit Trading Client
====================
Модуль для работы с Bybit API V5
Поддерживает спотовую и фьючерсную торговлю
"""

import time
import hmac
import hashlib
import requests
import json
from typing import Optional, Dict, Any, Literal
from dataclasses import dataclass
from enum import Enum
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderSide(Enum):
    """Сторона ордера"""
    BUY = "Buy"
    SELL = "Sell"


class OrderType(Enum):
    """Тип ордера"""
    MARKET = "Market"
    LIMIT = "Limit"


class Category(Enum):
    """Категория торговли"""
    SPOT = "spot"
    LINEAR = "linear"      # USDT Perpetual
    INVERSE = "inverse"    # Inverse Perpetual


class TimeInForce(Enum):
    """Время действия ордера"""
    GTC = "GTC"  # Good Till Cancel
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill
    POST_ONLY = "PostOnly"


@dataclass
class OrderResult:
    """Результат выполнения ордера"""
    success: bool
    order_id: Optional[str] = None
    message: Optional[str] = None
    data: Optional[Dict] = None


class Tradeclient:
    """
    Клиент для работы с Bybit API V5
    
    Attributes:
        api_key: API ключ
        api_secret: Секретный ключ API
        testnet: Использовать тестовую сеть
    """
    
    # Базовые URL
    MAINNET_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = self.TESTNET_URL if testnet else self.MAINNET_URL
        self.testnet = testnet
        self.recv_window = 5000
        
        logger.info(f"Инициализация клиента Bybit ({'TESTNET' if testnet else 'MAINNET'})")
    
    def _get_timestamp(self) -> str:
        """Получить текущий timestamp в миллисекундах"""
        return str(int(time.time() * 1000))
    
    def _generate_signature(self, timestamp: str, params: str) -> str:
        """
        Генерация HMAC SHA256 подписи
        
        Args:
            timestamp: Временная метка
            params: Строка параметров для подписи
            
        Returns:
            Подпись в hex формате
        """
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        signed: bool = True
    ) -> Dict[str, Any]:
        """
        Выполнить HTTP запрос к API
        
        Args:
            method: HTTP метод (GET, POST)
            endpoint: Эндпоинт API
            params: Параметры запроса
            signed: Требуется ли подпись
            
        Returns:
            Ответ API в виде словаря
        """
        url = f"{self.base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
        
        if signed:
            timestamp = self._get_timestamp()
            
            if method == "GET":
                param_str = "&".join([f"{k}={v}" for k, v in (params or {}).items()])
            else:
                param_str = json.dumps(params) if params else ""
            
            signature = self._generate_signature(timestamp, param_str)
            
            headers.update({
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": str(self.recv_window)
            })
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params, timeout=10)
            else:
                response = requests.post(url, headers=headers, json=params, timeout=10)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса: {e}")
            return {"retCode": -1, "retMsg": str(e)}
    
    # ==================== ИНФОРМАЦИОННЫЕ МЕТОДЫ ====================
    
    def get_server_time(self) -> Dict:
        """Получить время сервера"""
        return self._make_request("GET", "/v5/market/time", signed=False)
    
    def get_wallet_balance(
        self,
        account_type: str = "UNIFIED",
        coin: Optional[str] = None
    ) -> Dict:
        """
        Получить баланс кошелька
        
        Args:
            account_type: Тип аккаунта (UNIFIED, SPOT, CONTRACT)
            coin: Конкретная монета (опционально)
        """
        params = {"accountType": account_type}
        if coin:
            params["coin"] = coin
        return self._make_request("GET", "/v5/account/wallet-balance", params)
    
    def get_ticker(self, category: Category, symbol: str) -> Dict:
        """
        Получить текущую цену инструмента
        
        Args:
            category: Категория (spot, linear, inverse)
            symbol: Торговая пара (например, BTCUSDT)
        """
        params = {
            "category": category.value,
            "symbol": symbol
        }
        return self._make_request("GET", "/v5/market/tickers", params, signed=False)
    
    def get_orderbook(
        self,
        category: Category,
        symbol: str,
        limit: int = 25
    ) -> Dict:
        """Получить стакан ордеров"""
        params = {
            "category": category.value,
            "symbol": symbol,
            "limit": limit
        }
        return self._make_request("GET", "/v5/market/orderbook", params, signed=False)
    
    def get_instruments_info(
        self,
        category: Category,
        symbol: Optional[str] = None
    ) -> Dict:
        """Получить информацию об инструментах"""
        params = {"category": category.value}
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/v5/market/instruments-info", params, signed=False)
    
    # ==================== ТОРГОВЫЕ МЕТОДЫ ====================
    
    def place_order(
        self,
        category: Category,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        qty: str,
        price: Optional[str] = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        reduce_only: bool = False,
        close_on_trigger: bool = False,
        order_link_id: Optional[str] = None,
        market_unit: Optional[str] = None
    ) -> OrderResult:
        """
        Разместить ордер
        
        Args:
            category: Категория торговли
            symbol: Торговая пара
            side: Сторона (Buy/Sell)
            order_type: Тип ордера (Market/Limit)
            qty: Количество
            price: Цена (для лимитного ордера)
            time_in_force: Время действия
            reduce_only: Только уменьшение позиции
            close_on_trigger: Закрыть при срабатывании
            order_link_id: Пользовательский ID ордера
            market_unit: Единица измерения для market ордеров на споте:
                         "baseCoin" - qty в базовой монете (BTC)
                         "quoteCoin" - qty в котировочной монете (USDT)
            
        Returns:
            OrderResult с результатом операции
        """
        params = {
            "category": category.value,
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "qty": qty,
            "timeInForce": time_in_force.value
        }
        
        if price and order_type == OrderType.LIMIT:
            params["price"] = price
        
        # Для спотовых рыночных ордеров добавляем marketUnit
        if category == Category.SPOT and order_type == OrderType.MARKET and market_unit:
            params["marketUnit"] = market_unit
        
        if category != Category.SPOT:
            params["reduceOnly"] = reduce_only
            params["closeOnTrigger"] = close_on_trigger
        
        if order_link_id:
            params["orderLinkId"] = order_link_id
        
        logger.info(f"Размещение ордера: {side.value} {qty} {symbol} @ {price or 'MARKET'}")
        
        response = self._make_request("POST", "/v5/order/create", params)
        
        if response.get("retCode") == 0:
            order_id = response.get("result", {}).get("orderId")
            logger.info(f"Ордер успешно размещен: {order_id}")
            return OrderResult(
                success=True,
                order_id=order_id,
                message="Ордер успешно размещен",
                data=response.get("result")
            )
        else:
            error_msg = response.get("retMsg", "Неизвестная ошибка")
            logger.error(f"Ошибка размещения ордера: {error_msg}")
            return OrderResult(
                success=False,
                message=error_msg,
                data=response
            )
    
    def market_buy(
        self,
        symbol: str,
        qty: str,
        category: Category = Category.SPOT,
        in_quote_coin: bool = False
    ) -> OrderResult:
        """
        Рыночная покупка
        
        Args:
            symbol: Торговая пара
            qty: Количество (в базовой монете или в USDT если in_quote_coin=True)
            category: Категория
            in_quote_coin: Если True, qty - это сумма в USDT
        """
        market_unit = "quoteCoin" if in_quote_coin else "baseCoin"
        return self.place_order(
            category=category,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            qty=qty,
            time_in_force=TimeInForce.IOC,
            market_unit=market_unit if category == Category.SPOT else None
        )
    
    def market_sell(
        self,
        symbol: str,
        qty: str,
        category: Category = Category.SPOT,
        in_quote_coin: bool = False
    ) -> OrderResult:
        """
        Рыночная продажа
        
        Args:
            symbol: Торговая пара
            qty: Количество (в базовой монете или в USDT если in_quote_coin=True)
            category: Категория
            in_quote_coin: Если True, qty - это сумма в USDT
        """
        market_unit = "quoteCoin" if in_quote_coin else "baseCoin"
        return self.place_order(
            category=category,
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            qty=qty,
            time_in_force=TimeInForce.IOC,
            market_unit=market_unit if category == Category.SPOT else None
        )
    
    def market_sell_all(
        self,
        symbol: str,
        category: Category = Category.SPOT
    ) -> OrderResult:
        """
        Продать ВСЕ токены базовой монеты
        
        Args:
            symbol: Торговая пара (например BTCUSDT)
            category: Категория
            
        Returns:
            OrderResult с результатом операции
        """
        # Извлекаем базовую монету из символа (BTCUSDT -> BTC)
        # Обычно это всё до USDT/USDC/EUR и т.д.
        base_coin = symbol.replace("USDT", "").replace("USDC", "").replace("EUR", "").replace("BTC", "" if symbol.endswith("BTC") else "BTC")
        
        # Для пар типа BTCUSDT базовая монета - BTC
        if symbol.endswith("USDT"):
            base_coin = symbol[:-4]
        elif symbol.endswith("USDC"):
            base_coin = symbol[:-4]
        elif symbol.endswith("EUR"):
            base_coin = symbol[:-3]
        elif symbol.endswith("BTC"):
            base_coin = symbol[:-3]
        
        logger.info(f"Получение баланса {base_coin} для продажи всех токенов")
        
        # Получаем баланс
        balance = self.get_coin_balance(base_coin) * 0.99
        
        if not balance or float(balance) == 0:
            return OrderResult(
                success=False,
                message=f"Нет доступного баланса {base_coin} для продажи"
            )
        
        logger.info(f"Найден баланс: {balance} {base_coin}")
        
        # Получаем информацию о минимальном ордере и точности
        min_info = self.get_min_order_info(symbol, category)
        base_precision = min_info.get("base_precision", "0.000001")
        min_qty = min_info.get("min_qty", "0")
        
        # Определяем количество знаков после запятой
        if "." in base_precision:
            decimals = len(base_precision.split(".")[1].rstrip("0")) or 1
        else:
            decimals = 6
        
        # Форматируем баланс с правильной точностью
        qty = f"{float(balance):.{decimals}f}"
        
        # Проверяем минимум
        if float(qty) < float(min_qty):
            return OrderResult(
                success=False,
                message=f"Баланс {qty} {base_coin} меньше минимального ордера {min_qty}"
            )
        
        logger.info(f"Продажа всех токенов: {qty} {base_coin}")
        
        # Продаем
        return self.market_sell(symbol, qty, category, in_quote_coin=False)
    
    def limit_buy(
        self,
        symbol: str,
        qty: str,
        price: str,
        category: Category = Category.SPOT,
        time_in_force: TimeInForce = TimeInForce.GTC
    ) -> OrderResult:
        """
        Лимитная покупка
        
        Args:
            symbol: Торговая пара
            qty: Количество
            price: Цена
            category: Категория
            time_in_force: Время действия
        """
        return self.place_order(
            category=category,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            qty=qty,
            price=price,
            time_in_force=time_in_force
        )
    
    def limit_sell(
        self,
        symbol: str,
        qty: str,
        price: str,
        category: Category = Category.SPOT,
        time_in_force: TimeInForce = TimeInForce.GTC
    ) -> OrderResult:
        """
        Лимитная продажа
        
        Args:
            symbol: Торговая пара
            qty: Количество
            price: Цена
            category: Категория
            time_in_force: Время действия
        """
        return self.place_order(
            category=category,
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            qty=qty,
            price=price,
            time_in_force=time_in_force
        )
    
    def cancel_order(
        self,
        category: Category,
        symbol: str,
        order_id: Optional[str] = None,
        order_link_id: Optional[str] = None
    ) -> OrderResult:
        """
        Отменить ордер
        
        Args:
            category: Категория
            symbol: Торговая пара
            order_id: ID ордера
            order_link_id: Пользовательский ID ордера
        """
        if not order_id and not order_link_id:
            return OrderResult(
                success=False,
                message="Необходимо указать order_id или order_link_id"
            )
        
        params = {
            "category": category.value,
            "symbol": symbol
        }
        
        if order_id:
            params["orderId"] = order_id
        if order_link_id:
            params["orderLinkId"] = order_link_id
        
        logger.info(f"Отмена ордера: {order_id or order_link_id}")
        
        response = self._make_request("POST", "/v5/order/cancel", params)
        
        if response.get("retCode") == 0:
            logger.info("Ордер успешно отменен")
            return OrderResult(
                success=True,
                order_id=response.get("result", {}).get("orderId"),
                message="Ордер успешно отменен",
                data=response.get("result")
            )
        else:
            error_msg = response.get("retMsg", "Неизвестная ошибка")
            logger.error(f"Ошибка отмены ордера: {error_msg}")
            return OrderResult(
                success=False,
                message=error_msg,
                data=response
            )
    
    def cancel_all_orders(
        self,
        category: Category,
        symbol: Optional[str] = None
    ) -> OrderResult:
        """Отменить все ордера"""
        params = {"category": category.value}
        if symbol:
            params["symbol"] = symbol
        
        logger.info(f"Отмена всех ордеров для {symbol or 'всех пар'}")
        
        response = self._make_request("POST", "/v5/order/cancel-all", params)
        
        if response.get("retCode") == 0:
            return OrderResult(
                success=True,
                message="Все ордера отменены",
                data=response.get("result")
            )
        else:
            return OrderResult(
                success=False,
                message=response.get("retMsg"),
                data=response
            )
    
    def get_open_orders(
        self,
        category: Category,
        symbol: Optional[str] = None,
        limit: int = 50
    ) -> Dict:
        """Получить открытые ордера"""
        params = {
            "category": category.value,
            "limit": limit
        }
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/v5/order/realtime", params)
    
    def get_order_history(
        self,
        category: Category,
        symbol: Optional[str] = None,
        limit: int = 50
    ) -> Dict:
        """Получить историю ордеров"""
        params = {
            "category": category.value,
            "limit": limit
        }
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/v5/order/history", params)
    
    def get_positions(
        self,
        category: Category,
        symbol: Optional[str] = None
    ) -> Dict:
        """Получить открытые позиции (для фьючерсов)"""
        params = {"category": category.value}
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/v5/position/list", params)
    
    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================
    
    def get_current_price(self, symbol: str, category: Category = Category.SPOT) -> Optional[float]:
        """
        Получить текущую цену
        
        Args:
            symbol: Торговая пара
            category: Категория
            
        Returns:
            Текущая цена или None
        """
        ticker = self.get_ticker(category, symbol)
        if ticker.get("retCode") == 0:
            result = ticker.get("result", {}).get("list", [])
            if result:
                return float(result[0].get("lastPrice", 0))
        return None
    
    def get_coin_balance(self, coin: str, account_type: str = "UNIFIED") -> Optional[str]:
        """
        Получить баланс конкретной монеты
        
        Args:
            coin: Монета (BTC, ETH, USDT и т.д.)
            account_type: Тип аккаунта (UNIFIED, SPOT, CONTRACT)
            
        Returns:
            Доступный баланс в виде строки или None
        """
        result = self.get_wallet_balance(account_type, coin)
        if result.get("retCode") == 0:
            accounts = result.get("result", {}).get("list", [])
            for account in accounts:
                for coin_data in account.get("coin", []):
                    if coin_data.get("coin") == coin:
                        # Возвращаем доступный для торговли баланс
                        available = coin_data.get("availableToWithdraw", "0")
                        # Если availableToWithdraw пуст, берем walletBalance
                        if not available or float(available) == 0:
                            available = coin_data.get("walletBalance", "0")

                        return float(available)
        return None
    
    def get_all_balances(self, account_type: str = "UNIFIED") -> Dict[str, str]:
        """
        Получить все балансы с ненулевым значением
        
        Returns:
            Словарь {coin: balance}
        """
        balances = {}
        result = self.get_wallet_balance(account_type)
        if result.get("retCode") == 0:
            accounts = result.get("result", {}).get("list", [])
            for account in accounts:
                for coin_data in account.get("coin", []):
                    balance = float(coin_data.get("walletBalance", 0))
                    if balance > 0:
                        balances[coin_data.get("coin")] = coin_data.get("walletBalance")
        return balances

    def get_min_order_qty(self, symbol: str, category: Category = Category.SPOT) -> Optional[str]:
        """Получить минимальный размер ордера"""
        info = self.get_instruments_info(category, symbol)
        if info.get("retCode") == 0:
            result = info.get("result", {}).get("list", [])
            if result:
                lot_filter = result[0].get("lotSizeFilter", {})
                return lot_filter.get("minOrderQty")
        return None
    
    def get_min_order_info(self, symbol: str, category: Category = Category.SPOT) -> Dict[str, Optional[str]]:
        """
        Получить полную информацию о минимальном ордере
        
        Returns:
            Dict с ключами:
            - min_qty: минимальное количество токенов
            - min_amt: минимальная сумма в USDT
            - base_precision: шаг количества
        """
        info = self.get_instruments_info(category, symbol)
        if info.get("retCode") == 0:
            result = info.get("result", {}).get("list", [])
            if result:
                lot_filter = result[0].get("lotSizeFilter", {})
                return {
                    "min_qty": lot_filter.get("minOrderQty"),
                    "min_amt": lot_filter.get("minOrderAmt"),  # Минимум в USDT!
                    "base_precision": lot_filter.get("basePrecision"),
                    "quote_precision": lot_filter.get("quotePrecision")
                }
        return {"min_qty": None, "min_amt": None, "base_precision": None, "quote_precision": None}
    
    def calculate_qty_from_usdt(
        self,
        symbol: str,
        usdt_amount: float,
        category: Category = Category.SPOT
    ) -> Optional[str]:
        """
        Рассчитать количество монет из суммы USDT
        
        Args:
            symbol: Торговая пара
            usdt_amount: Сумма в USDT
            category: Категория
            
        Returns:
            Количество монет в виде строки
        """
        price = self.get_current_price(symbol, category)
        if price:
            qty = usdt_amount / price
            # Получаем информацию о минимальном шаге
            info = self.get_instruments_info(category, symbol)
            if info.get("retCode") == 0:
                result = info.get("result", {}).get("list", [])
                if result:
                    base_precision = result[0].get("lotSizeFilter", {}).get("basePrecision", "0.001")
                    # Определяем количество знаков после запятой
                    if "." in base_precision:
                        decimals = len(base_precision.split(".")[1])
                    else:
                        decimals = 0
                    return f"{qty:.{decimals}f}"
            return f"{qty:.6f}"
        return None


# ==================== УТИЛИТЫ ====================

def format_order_result(result: OrderResult) -> str:
    """Форматирование результата ордера для вывода"""
    if result.success:
        return f"""
✅ ОРДЕР УСПЕШНО РАЗМЕЩЕН
   Order ID: {result.order_id}
   {result.message}
"""
    else:
        return f"""
❌ ОШИБКА ОРДЕРА
   {result.message}
   Данные: {result.data}
"""