import asyncio
from trade_client import OrderQueue, OrderStatus, format_order

# ╔══════════════════════════════════════════════════════════════════╗
# ║                         НАСТРОЙКИ                                ║
# ╚══════════════════════════════════════════════════════════════════╝

# Ваши API ключи (создайте на bybit.com в режиме Demo Trading)
# API_KEY = "LBYHkDXl4eTUHQqldp"
# API_SECRET = "Ua0wo6oFPqWhdqcKVJNCaXAA0GIpLhr4Mzlr"

API_KEY = "S77HXol52YNopygxMg"
API_SECRET = "yFSwQOZX7wnVmSZMuMnvrYXLOmqejxzcahP3"

# True = демо-торговля (виртуальные деньги, рекомендуется для начала!)
# False = реальная торговля (ОСТОРОЖНО!)
USE_DEMO = True


# ╔══════════════════════════════════════════════════════════════════╗
# ║                          ТЕСТЫ                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

async def test_info():
    """Проверка подключения и баланса"""
    print("\n" + "="*50)
    print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ")
    print("="*50)
    
    queue = OrderQueue(API_KEY, API_SECRET, USE_DEMO)
    
    try:
        # Цена
        price = await queue.get_price("BTCUSDT")
        print(f"    BTC: ${price:,.2f}" if price else "   ❌ Нет связи")
        
        # Баланс
        balances = await queue.get_balance()
        print(f"\n    Баланс:")
        if balances:
            for coin, bal in balances.items():
                print(f"      {coin}: {bal}")
        else:
            print("      Пусто")
        
        # Минимум
        min_order = await queue.get_min_order("BTCUSDT")
        print(f"\n    Мин. ордер BTCUSDT: ${min_order.get('min_amt', '?')}")
        
    finally:
        await queue._api.close()


async def test_queue_interactive():
    """Интерактивный тест очереди"""
    print("\n" + "="*50)
    print("ИНТЕРАКТИВНАЯ ОЧЕРЕДЬ")
    print("="*50)
    
    queue = OrderQueue(API_KEY, API_SECRET, USE_DEMO, retry_count=2)
    
    # Callbacks
    async def on_done(order):
        print(f"\n   ✅ {format_order(order)}")
    
    async def on_fail(order):
        print(f"\n   ❌ {format_order(order)}")
    
    queue.on_completed = on_done
    queue.on_failed = on_fail
    
    try:
        await queue.start()
        
        price = await queue.get_price("BTCUSDT")
        print(f"   BTC: ${price:,.2f}" if price else "")
        
        print("\n   Команды:")
        print("   1. Купить (USDT)")
        print("   2. Продать (количество)")
        print("   3. Продать ВСЁ")
        print("   4. Лимитная покупка")
        print("   5. Статус очереди")
        print("   6. Все ордера")
        print("   7. Открытые на бирже")
        print("   0. Выход")
        
        while True:
            cmd = input("\n   > ").strip()
            
            if cmd == "0":
                break
            
            elif cmd == "1":
                symbol = input("   Пара (BTCUSDT): ").strip() or "BTCUSDT"
                amount = input("   Сумма USDT: ").strip()
                if amount:
                    oid = await queue.buy(symbol, amount)
                    print(f"    {oid}")
            
            elif cmd == "2":
                symbol = input("   Пара (BTCUSDT): ").strip() or "BTCUSDT"
                amount = input("   Количество: ").strip()
                if amount:
                    oid = await queue.sell(symbol, amount)
                    print(f"    {oid}")
            
            elif cmd == "3":
                symbol = input("   Пара (BTCUSDT): ").strip() or "BTCUSDT"
                confirm = input(f"   Продать ВСЁ {symbol.replace('USDT','')}? (yes): ")
                if confirm == "yes":
                    oid = await queue.sell(symbol, "all")
                    print(f"    {oid}")
            
            elif cmd == "4":
                symbol = input("   Пара (BTCUSDT): ").strip() or "BTCUSDT"
                qty = input("   Количество: ").strip()
                price = input("   Цена: ").strip()
                if qty and price:
                    oid = await queue.buy(symbol, qty, price=price)
                    print(f"    {oid}")
            
            elif cmd == "5":
                print(f"\n    {queue.stats}")
            
            elif cmd == "6":
                print(f"\n    Ордера ({len(queue._orders)}):")
                for o in queue._orders.values():
                    print(f"      {format_order(o)}")
            
            elif cmd == "7":
                orders = await queue.get_open_orders()
                print(f"\n    На бирже ({len(orders)}):")
                for o in orders:
                    print(f"      {o.get('orderId')}: {o.get('side')} {o.get('qty')} @ {o.get('price')}")
            
            await asyncio.sleep(0.3)
    
    finally:
        await queue.stop()


async def test_batch():
    """Пакетный тест"""
    print("\n" + "="*50)
    print("ПАКЕТНЫЙ ТЕСТ")
    print("="*50)
    
    queue = OrderQueue(API_KEY, API_SECRET, USE_DEMO)
    
    results = []
    
    async def on_done(order):
        results.append(("✅", order))
        print(f"   ✅ {order.id}")
    
    async def on_fail(order):
        results.append(("❌", order))
        print(f"   ❌ {order.id}: {order.result.message}")
    
    queue.on_completed = on_done
    queue.on_failed = on_fail
    
    try:
        await queue.start()
        
        print("\n   Добавляю 3 ордера с приоритетами...")
        
        ids = []
        ids.append(await queue.buy("BTCUSDT", "10", priority=0))
        print(f"    Buy $10 (priority=0)")
        
        ids.append(await queue.buy("BTCUSDT", "15", priority=10))
        print(f"    Buy $15 (priority=10) ← выполнится первым!")
        
        ids.append(await queue.buy("BTCUSDT", "5", priority=5))
        print(f"    Buy $5 (priority=5)")
        
        print(f"\n    Ожидание...")
        
        for oid in ids:
            await queue.wait(oid, timeout=30)
        
        print(f"\n   📊 Итого: {len(results)} ордеров")
        
    finally:
        await queue.stop()


async def show_example():
    """Показать пример кода"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║                    ПРИМЕР ДЛЯ БОТА                           ║
╚══════════════════════════════════════════════════════════════╝

from trade_client import OrderQueue, OrderStatus

# Инициализация
queue = OrderQueue(
    api_key="...",
    api_secret="...",
    demo=True,         # True = демо, False = реальная торговля
    max_concurrent=1,  # Один ордер за раз
    retry_count=3      # 3 попытки при ошибке
)

# Callbacks (опционально)
async def on_completed(order):
    print(f"✅ {order.id} → Bybit ID: {order.result.order_id}")
    # Записать в БД, отправить уведомление...

async def on_failed(order):
    print(f"❌ {order.id}: {order.result.message}")

queue.on_completed = on_completed
queue.on_failed = on_failed

# Запуск
await queue.start()

# === ТОРГОВЛЯ ===

# Рыночная покупка на $100 USDT
order_id = await queue.buy("BTCUSDT", "100")

# Рыночная покупка на $50 с высоким приоритетом
order_id = await queue.buy("BTCUSDT", "50", priority=10)

# Лимитная покупка
order_id = await queue.buy("BTCUSDT", "0.001", price="80000")

# Продать всё
order_id = await queue.sell("BTCUSDT", "all")

# Продать конкретное количество
order_id = await queue.sell("BTCUSDT", "0.0005")

# Лимитная продажа
order_id = await queue.sell("BTCUSDT", "0.001", price="100000")

# === УПРАВЛЕНИЕ ===

# Проверить статус
order = queue.get(order_id)
if order.status == OrderStatus.COMPLETED:
    print(order.result.order_id)

# Ждать выполнения
order = await queue.wait(order_id, timeout=60)

# Отменить до отправки на биржу
queue.cancel(order_id)

# Статистика
print(queue.stats)
# {'queue': 0, 'pending': 0, 'completed': 5, 'failed': 1, 'running': True}

# Списки
queue.pending    # Ожидающие
queue.completed  # Выполненные
queue.failed     # Проваленные

# === ИНФО ===

price = await queue.get_price("BTCUSDT")
balances = await queue.get_balance()
min_order = await queue.get_min_order("BTCUSDT")

# Отмена на бирже
await queue.cancel_exchange_order("BTCUSDT", "order_id_from_bybit")

# Остановка
await queue.stop()
""")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                          МЕНЮ                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

async def main():
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║              BYBIT ORDER QUEUE - ТЕСТИРОВАНИЕ                ║
╠══════════════════════════════════════════════════════════════╣
║  Режим: {'    DEMO' if USE_DEMO else ' MAINNET'}     ║
║  Установка: pip install aiohttp                              ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    if API_KEY == "YOUR_API_KEY":
        print("   ❌ Заполните API_KEY и API_SECRET!\n")
        return
    
    while True:
        print("\n   1. Проверка подключения")
        print("   2. Интерактивная очередь")
        print("   3. Пакетный тест")
        print("   4. Пример кода")
        print("   0. Выход")
        
        cmd = input("\n   > ").strip()
        
        if cmd == "0":
            print("\n    Пока!")
            break
        elif cmd == "1":
            await test_info()
        elif cmd == "2":
            await test_queue_interactive()
        elif cmd == "3":
            await test_batch()
        elif cmd == "4":
            await show_example()
        
        input("\n   Enter...")


if __name__ == "__main__":
    asyncio.run(main())