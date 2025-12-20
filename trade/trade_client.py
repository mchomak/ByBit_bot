"""
Bybit Trading Client (Async + Order Queue)
==========================================
Асинхронный клиент для Bybit API V5
Вся торговля через очередь ордеров
"""

import time
import hmac
import hashlib
import json
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import logging
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== ENUMS ====================

class OrderSide(Enum):
    BUY = "Buy"
    SELL = "Sell"


class OrderType(Enum):
    MARKET = "Market"
    LIMIT = "Limit"


class Category(Enum):
    SPOT = "spot"
    LINEAR = "linear"
    INVERSE = "inverse"


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    POST_ONLY = "PostOnly"


class OrderStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ==================== DATACLASSES ====================

@dataclass
class OrderResult:
    """Результат выполнения ордера"""
    success: bool
    order_id: Optional[str] = None
    message: Optional[str] = None
    data: Optional[Dict] = None


@dataclass
class QueuedOrder:
    """Ордер в очереди"""
    id: str
    category: Category
    symbol: str
    side: OrderSide
    order_type: OrderType
    qty: str
    price: Optional[str] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    market_unit: Optional[str] = None
    
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    executed_at: Optional[datetime] = None
    result: Optional[OrderResult] = None
    callback: Optional[Callable[['QueuedOrder'], Awaitable[None]]] = None
    priority: int = 0
    
    def __lt__(self, other):
        return self.priority > other.priority


# ==================== API CLIENT ====================

class _TradeApi:
    """Внутренний класс для работы с Bybit API"""
    
    MAINNET_URL = "https://api.bybit.com"
    TESTNET_URL = "https://api-testnet.bybit.com"
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = self.TESTNET_URL if testnet else self.MAINNET_URL
        self.testnet = testnet
        self.recv_window = 5000
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _get_timestamp(self) -> str:
        return str(int(time.time() * 1000))
    
    def _generate_signature(self, timestamp: str, params: str) -> str:
        param_str = f"{timestamp}{self.api_key}{self.recv_window}{params}"
        return hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    async def request(self, method: str, endpoint: str, params: Optional[Dict] = None, signed: bool = True) -> Dict:
        url = f"{self.base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
        
        if signed:
            timestamp = self._get_timestamp()
            param_str = "&".join([f"{k}={v}" for k, v in (params or {}).items()]) if method == "GET" else (json.dumps(params) if params else "")
            signature = self._generate_signature(timestamp, param_str)
            headers.update({
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": signature,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": str(self.recv_window)
            })
        
        try:
            session = await self._get_session()
            if method == "GET":
                async with session.get(url, headers=headers, params=params, timeout=10) as resp:
                    return await resp.json()
            else:
                async with session.post(url, headers=headers, json=params, timeout=10) as resp:
                    return await resp.json()
        except asyncio.TimeoutError:
            return {"retCode": -1, "retMsg": "Timeout"}
        except aiohttp.ClientError as e:
            return {"retCode": -1, "retMsg": str(e)}


# ==================== ORDER QUEUE ====================

class OrderQueue:
    """
    Очередь ордеров для торгового бота
    
    Пример:
        queue = OrderQueue(api_key, api_secret, testnet=False)
        await queue.start()
        
        order_id = await queue.buy("BTCUSDT", "50")  # Покупка на $50
        order_id = await queue.sell("BTCUSDT", "all")  # Продать всё
        
        await queue.wait(order_id)
        await queue.stop()
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        max_concurrent: int = 1,
        retry_count: int = 3,
        retry_delay: float = 1.0
    ):
        self._api = _TradeApi(api_key, api_secret, testnet)
        self.testnet = testnet
        self.max_concurrent = max_concurrent
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._orders: Dict[str, QueuedOrder] = {}
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._stats = {"completed": 0, "failed": 0}
        
        self.on_completed: Optional[Callable[[QueuedOrder], Awaitable[None]]] = None
        self.on_failed: Optional[Callable[[QueuedOrder], Awaitable[None]]] = None
        
        logger.info(f"OrderQueue: {'TESTNET' if testnet else 'MAINNET'}")
    
    # ==================== LIFECYCLE ====================
    
    async def start(self):
        """Запустить очередь"""
        if self._running:
            return
        self._running = True
        for i in range(self.max_concurrent):
            self._workers.append(asyncio.create_task(self._worker(i)))
        logger.info(f"Очередь запущена ({self.max_concurrent} workers)")
    
    async def stop(self, wait: bool = True):
        """Остановить очередь"""
        self._running = False
        for w in self._workers:
            w.cancel()
            try:
                await w
            except asyncio.CancelledError:
                pass
        self._workers.clear()
        await self._api.close()
        logger.info(f"Очередь остановлена. Выполнено: {self._stats['completed']}, Ошибок: {self._stats['failed']}")
    
    # ==================== INFO METHODS ====================
    
    async def get_price(self, symbol: str, category: Category = Category.SPOT) -> Optional[float]:
        """Получить текущую цену"""
        resp = await self._api.request("GET", "/v5/market/tickers", {"category": category.value, "symbol": symbol}, signed=False)
        if resp.get("retCode") == 0:
            result = resp.get("result", {}).get("list", [])
            if result:
                return float(result[0].get("lastPrice", 0))
        return None
    
    async def get_balance(self, coin: str = None) -> Dict[str, float]:
        """Получить баланс (все монеты или конкретную)"""
        params = {"accountType": "UNIFIED"}
        if coin:
            params["coin"] = coin
        resp = await self._api.request("GET", "/v5/account/wallet-balance", params)
        
        balances = {}
        if resp.get("retCode") == 0:
            for acc in resp.get("result", {}).get("list", []):
                for c in acc.get("coin", []):
                    bal = float(c.get("walletBalance", 0))
                    if bal > 0:
                        balances[c.get("coin")] = bal
        return balances
    
    async def get_min_order(self, symbol: str, category: Category = Category.SPOT) -> Dict:
        """Получить минимальный ордер"""
        resp = await self._api.request("GET", "/v5/market/instruments-info", {"category": category.value, "symbol": symbol}, signed=False)
        if resp.get("retCode") == 0:
            result = resp.get("result", {}).get("list", [])
            if result:
                lot = result[0].get("lotSizeFilter", {})
                return {
                    "min_qty": lot.get("minOrderQty"),
                    "min_amt": lot.get("minOrderAmt"),
                    "precision": lot.get("basePrecision")
                }
        return {}
    
    async def get_open_orders(self, symbol: str = None, category: Category = Category.SPOT) -> List[Dict]:
        """Получить открытые ордера на бирже"""
        params = {"category": category.value, "limit": 50}
        if symbol:
            params["symbol"] = symbol
        resp = await self._api.request("GET", "/v5/order/realtime", params)
        if resp.get("retCode") == 0:
            return resp.get("result", {}).get("list", [])
        return []
    
    async def cancel_exchange_order(self, symbol: str, order_id: str, category: Category = Category.SPOT) -> OrderResult:
        """Отменить ордер на бирже"""
        resp = await self._api.request("POST", "/v5/order/cancel", {
            "category": category.value,
            "symbol": symbol,
            "orderId": order_id
        })
        if resp.get("retCode") == 0:
            return OrderResult(True, order_id, "Отменён")
        return OrderResult(False, message=resp.get("retMsg"))
    
    # ==================== ORDER METHODS ====================
    
    async def buy(
        self,
        symbol: str,
        amount: str,
        price: str = None,
        priority: int = 0,
        callback: Callable[[QueuedOrder], Awaitable[None]] = None
    ) -> str:
        """
        Купить
        
        Args:
            symbol: Пара (BTCUSDT)
            amount: Сумма в USDT (для market) или количество (для limit)
            price: Цена (None = market order)
            priority: Приоритет (выше = раньше)
            callback: Функция после выполнения
        """
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        market_unit = "quoteCoin" if not price else None
        
        return await self._add_order(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=order_type,
            qty=amount,
            price=price,
            market_unit=market_unit,
            priority=priority,
            callback=callback
        )
    
    async def sell(
        self,
        symbol: str,
        amount: str,
        price: str = None,
        priority: int = 0,
        callback: Callable[[QueuedOrder], Awaitable[None]] = None
    ) -> str:
        """
        Продать
        
        Args:
            symbol: Пара (BTCUSDT)
            amount: Количество токенов или "all" для продажи всего
            price: Цена (None = market order)
            priority: Приоритет
            callback: Функция после выполнения
        """
        # Если "all" - получаем баланс
        if amount.lower() == "all":
            base_coin = symbol.replace("USDT", "").replace("USDC", "")
            balances = await self.get_balance(base_coin)
            balance = balances.get(base_coin, 0) * 0.99
            
            if balance == 0:
                logger.error(f"Нет баланса {base_coin}")
                order_id = f"q_{uuid.uuid4().hex[:8]}"
                order = QueuedOrder(
                    id=order_id, category=Category.SPOT, symbol=symbol,
                    side=OrderSide.SELL, order_type=OrderType.MARKET, qty="0",
                    status=OrderStatus.FAILED,
                    result=OrderResult(False, message=f"Нет баланса {base_coin}")
                )
                self._orders[order_id] = order
                return order_id
            
            min_info = await self.get_min_order(symbol)
            precision = min_info.get("precision", "0.000001")
            decimals = len(precision.split(".")[1].rstrip("0")) if "." in precision else 6
            amount = f"{balance:.{decimals}f}"
        
        order_type = OrderType.LIMIT if price else OrderType.MARKET
        
        return await self._add_order(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=order_type,
            qty=amount,
            price=price,
            market_unit=None,
            priority=priority,
            callback=callback
        )
    
    async def _add_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        qty: str,
        price: str = None,
        market_unit: str = None,
        priority: int = 0,
        callback: Callable = None,
        category: Category = Category.SPOT
    ) -> str:
        """Добавить ордер в очередь"""
        order_id = f"q_{uuid.uuid4().hex[:8]}"
        
        order = QueuedOrder(
            id=order_id,
            category=category,
            symbol=symbol,
            side=side,
            order_type=order_type,
            qty=qty,
            price=price,
            time_in_force=TimeInForce.GTC if price else TimeInForce.IOC,
            market_unit=market_unit,
            priority=priority,
            callback=callback
        )
        
        self._orders[order_id] = order
        await self._queue.put((-priority, order))
        
        logger.info(f"📥 {order_id}: {side.value} {qty} {symbol} @ {price or 'MARKET'}")
        return order_id
    
    # ==================== QUEUE MANAGEMENT ====================
    
    def get(self, order_id: str) -> Optional[QueuedOrder]:
        """Получить ордер по ID"""
        return self._orders.get(order_id)
    
    async def wait(self, order_id: str, timeout: float = 60) -> Optional[QueuedOrder]:
        """Ждать выполнения ордера"""
        start = asyncio.get_event_loop().time()
        while True:
            order = self._orders.get(order_id)
            if not order:
                return None
            if order.status in (OrderStatus.COMPLETED, OrderStatus.FAILED, OrderStatus.CANCELLED):
                return order
            if asyncio.get_event_loop().time() - start > timeout:
                return order
            await asyncio.sleep(0.1)
    
    def cancel(self, order_id: str) -> bool:
        """Отменить ордер в очереди (до отправки на биржу)"""
        order = self._orders.get(order_id)
        if order and order.status == OrderStatus.PENDING:
            order.status = OrderStatus.CANCELLED
            logger.info(f"🚫 {order_id} отменён")
            return True
        return False
    
    @property
    def pending(self) -> List[QueuedOrder]:
        """Ожидающие ордера"""
        return [o for o in self._orders.values() if o.status == OrderStatus.PENDING]
    
    @property
    def completed(self) -> List[QueuedOrder]:
        """Выполненные ордера"""
        return [o for o in self._orders.values() if o.status == OrderStatus.COMPLETED]
    
    @property
    def failed(self) -> List[QueuedOrder]:
        """Проваленные ордера"""
        return [o for o in self._orders.values() if o.status == OrderStatus.FAILED]
    
    @property
    def stats(self) -> Dict:
        """Статистика"""
        return {
            "queue": self._queue.qsize(),
            "pending": len(self.pending),
            "completed": self._stats["completed"],
            "failed": self._stats["failed"],
            "running": self._running
        }
    
    # ==================== WORKER ====================
    
    async def _worker(self, worker_id: int):
        """Обработчик очереди"""
        while self._running:
            try:
                try:
                    _, order = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                if order.status == OrderStatus.CANCELLED:
                    self._queue.task_done()
                    continue
                
                await self._execute(order)
                self._queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id}: {e}")
    
    async def _execute(self, order: QueuedOrder):
        """Исполнить ордер"""
        order.status = OrderStatus.PROCESSING
        logger.info(f"🔄 {order.id}: {order.side.value} {order.qty} {order.symbol}")
        
        result = None
        for attempt in range(self.retry_count):
            try:
                params = {
                    "category": order.category.value,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "orderType": order.order_type.value,
                    "qty": order.qty,
                    "timeInForce": order.time_in_force.value
                }
                
                if order.price:
                    params["price"] = order.price
                if order.market_unit:
                    params["marketUnit"] = order.market_unit
                
                resp = await self._api.request("POST", "/v5/order/create", params)
                
                if resp.get("retCode") == 0:
                    result = OrderResult(
                        success=True,
                        order_id=resp.get("result", {}).get("orderId"),
                        message="OK",
                        data=resp.get("result")
                    )
                    break
                else:
                    result = OrderResult(False, message=resp.get("retMsg"), data=resp)
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay)
                        
            except Exception as e:
                result = OrderResult(False, message=str(e))
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay)
        
        order.executed_at = datetime.now()
        order.result = result
        
        if result and result.success:
            order.status = OrderStatus.COMPLETED
            self._stats["completed"] += 1
            logger.info(f"✅ {order.id} → {result.order_id}")
            if order.callback:
                await order.callback(order)
            if self.on_completed:
                await self.on_completed(order)
        else:
            order.status = OrderStatus.FAILED
            self._stats["failed"] += 1
            logger.error(f"❌ {order.id}: {result.message if result else 'Unknown'}")
            if self.on_failed:
                await self.on_failed(order)


# ==================== HELPERS ====================

def format_order(order: QueuedOrder) -> str:
    """Форматировать ордер для вывода"""
    icons = {
        OrderStatus.PENDING: "⏳",
        OrderStatus.PROCESSING: "🔄", 
        OrderStatus.COMPLETED: "✅",
        OrderStatus.FAILED: "❌",
        OrderStatus.CANCELLED: "🚫"
    }
    icon = icons.get(order.status, "❓")
    
    result = ""
    if order.result:
        result = f" → {order.result.order_id or order.result.message}"
    
    price_str = f"@ ${order.price}" if order.price else "@ MARKET"
    return f"{icon} {order.id}: {order.side.value} {order.qty} {order.symbol} {price_str}{result}"