"""
Bybit Trading Client - Тестовый файл
=====================================
Используйте этот файл для тестирования торговых операций

⚠️  ВАЖНО: Сначала тестируйте на TESTNET!
"""

from bybit_client import (
    BybitClient,
    Category,
    OrderSide,
    OrderType,
    TimeInForce,
    format_order_result
)
import json

# ╔══════════════════════════════════════════════════════════════════╗
# ║                    НАСТРОЙКИ - ЗАПОЛНИТЕ!                        ║
# ╚══════════════════════════════════════════════════════════════════╝

# Ваши API ключи (получите на https://testnet.bybit.com для тестов)
API_KEY = "LBYHkDXl4eTUHQqldp"
API_SECRET = "Ua0wo6oFPqWhdqcKVJNCaXAA0GIpLhr4Mzlr"

# True = тестовая сеть (рекомендуется для начала!)
# False = реальная торговля (ОСТОРОЖНО!)
USE_TESTNET = False


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ИНИЦИАЛИЗАЦИЯ КЛИЕНТА                         ║
# ╚══════════════════════════════════════════════════════════════════╝

def create_client():
    """Создание клиента Bybit"""
    return BybitClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        testnet=USE_TESTNET
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ТЕСТОВЫЕ ФУНКЦИИ                              ║
# ╚══════════════════════════════════════════════════════════════════╝

def test_connection():
    """Тест 1: Проверка подключения к серверу"""
    print("\n" + "="*60)
    print("ТЕСТ 1: Проверка подключения")
    print("="*60)
    
    client = create_client()
    result = client.get_server_time()
    
    if result.get("retCode") == 0:
        server_time = result.get("result", {}).get("timeSecond")
        print(f"✅ Подключение успешно!")
        print(f"   Время сервера: {server_time}")
    else:
        print(f"❌ Ошибка подключения: {result.get('retMsg')}")
    
    return result


def test_balance():
    """Тест 2: Проверка баланса"""
    print("\n" + "="*60)
    print("ТЕСТ 2: Проверка баланса")
    print("="*60)
    
    client = create_client()
    result = client.get_wallet_balance(account_type="UNIFIED")
    
    if result.get("retCode") == 0:
        print("✅ Баланс получен!")
        coins = result.get("result", {}).get("list", [])
        if coins:
            for account in coins:
                print(f"\n   Аккаунт: {account.get('accountType')}")
                for coin in account.get("coin", []):
                    if float(coin.get("walletBalance", 0)) > 0:
                        print(f"   • {coin.get('coin')}: {coin.get('walletBalance')}")
        else:
            print("   Баланс пуст или аккаунт не найден")
    else:
        print(f"❌ Ошибка: {result.get('retMsg')}")
    
    return result


def test_get_price(symbol: str = "BTCUSDT"):
    """Тест 3: Получение текущей цены"""
    print("\n" + "="*60)
    print(f"ТЕСТ 3: Получение цены {symbol}")
    print("="*60)
    
    client = create_client()
    
    # Спотовая цена
    price = client.get_current_price(symbol, Category.SPOT)
    if price:
        print(f"✅ Текущая цена {symbol}: ${price:,.2f}")
    else:
        print(f"❌ Не удалось получить цену")
    
    # Полная информация о тикере
    ticker = client.get_ticker(Category.SPOT, symbol)
    if ticker.get("retCode") == 0:
        data = ticker.get("result", {}).get("list", [{}])[0]
        print(f"   24h High: ${float(data.get('highPrice24h', 0)):,.2f}")
        print(f"   24h Low:  ${float(data.get('lowPrice24h', 0)):,.2f}")
        print(f"   24h Volume: {data.get('volume24h')}")
    
    return price


def test_orderbook(symbol: str = "BTCUSDT"):
    """Тест 4: Получение стакана ордеров"""
    print("\n" + "="*60)
    print(f"ТЕСТ 4: Стакан ордеров {symbol}")
    print("="*60)
    
    client = create_client()
    result = client.get_orderbook(Category.SPOT, symbol, limit=5)
    
    if result.get("retCode") == 0:
        data = result.get("result", {})
        print("✅ Стакан получен!")
        
        print("\n   📗 ПОКУПКА (Bids):")
        for bid in data.get("b", [])[:5]:
            print(f"      ${float(bid[0]):,.2f} - {bid[1]}")
        
        print("\n   📕 ПРОДАЖА (Asks):")
        for ask in data.get("a", [])[:5]:
            print(f"      ${float(ask[0]):,.2f} - {ask[1]}")
    else:
        print(f"❌ Ошибка: {result.get('retMsg')}")
    
    return result


def test_instrument_info(symbol: str = "BTCUSDT"):
    """Тест 5: Информация об инструменте"""
    print("\n" + "="*60)
    print(f"ТЕСТ 5: Информация об инструменте {symbol}")
    print("="*60)
    
    client = create_client()
    result = client.get_instruments_info(Category.SPOT, symbol)
    
    if result.get("retCode") == 0:
        data = result.get("result", {}).get("list", [{}])[0]
        lot_filter = data.get("lotSizeFilter", {})
        price_filter = data.get("priceFilter", {})
        
        print("✅ Информация получена!")
        print(f"   Статус: {data.get('status')}")
        print(f"   Мин. количество: {lot_filter.get('minOrderQty')}")
        print(f"   Макс. количество: {lot_filter.get('maxOrderQty')}")
        print(f"   Шаг количества: {lot_filter.get('basePrecision')}")
        print(f"   Шаг цены: {price_filter.get('tickSize')}")
    else:
        print(f"❌ Ошибка: {result.get('retMsg')}")
    
    return result


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ТОРГОВЫЕ ТЕСТЫ                                ║
# ╚══════════════════════════════════════════════════════════════════╝

def test_market_buy(symbol: str = "BTCUSDT", qty: str = "0.001", in_usdt: bool = False):
    """
    Тест: Рыночная покупка
    
    Args:
        symbol: Торговая пара
        qty: Количество (в токенах или USDT в зависимости от in_usdt)
        in_usdt: Если True, qty интерпретируется как сумма в USDT
    
    ⚠️ ВНИМАНИЕ: Этот тест создает РЕАЛЬНЫЙ ордер!
    """
    print("\n" + "="*60)
    print(f"ТЕСТ: Рыночная покупка {'$' + qty + ' USDT' if in_usdt else qty + ' ' + symbol.replace('USDT', '')}")
    print("="*60)
    
    client = create_client()
    
    # Показываем текущую цену
    price = client.get_current_price(symbol, Category.SPOT)
    print(f"   Текущая цена: ${price:,.2f}" if price else "   Цена недоступна")
    
    # Если сумма в USDT, рассчитываем количество токенов
    if in_usdt and price:
        usdt_amount = float(qty)
        calculated_qty = client.calculate_qty_from_usdt(symbol, usdt_amount, Category.SPOT)
        if calculated_qty:
            print(f"   💰 ${usdt_amount} USDT = {calculated_qty} {symbol.replace('USDT', '')}")
            qty = calculated_qty
        else:
            print("   ❌ Не удалось рассчитать количество")
            return None
    
    # Показываем минимальный ордер
    min_qty = client.get_min_order_qty(symbol, Category.SPOT)
    if min_qty:
        min_value = float(min_qty) * price if price else 0
        print(f"   📊 Минимальный ордер: {min_qty} (~${min_value:.2f})")
    
    # Подтверждение
    confirm = input("\n   ⚠️  Создать ордер? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.market_buy(symbol, qty, Category.SPOT)
    print(format_order_result(result))
    
    return result


def test_market_sell(symbol: str = "BTCUSDT", qty: str = "0.001", in_usdt: bool = False):
    """
    Тест: Рыночная продажа
    
    Args:
        symbol: Торговая пара
        qty: Количество (в токенах или USDT в зависимости от in_usdt)
        in_usdt: Если True, qty интерпретируется как сумма в USDT
    
    ⚠️ ВНИМАНИЕ: Этот тест создает РЕАЛЬНЫЙ ордер!
    """
    print("\n" + "="*60)
    print(f"ТЕСТ: Рыночная продажа {'$' + qty + ' USDT' if in_usdt else qty + ' ' + symbol.replace('USDT', '')}")
    print("="*60)
    
    client = create_client()
    
    # Показываем текущую цену
    price = client.get_current_price(symbol, Category.SPOT)
    print(f"   Текущая цена: ${price:,.2f}" if price else "   Цена недоступна")
    
    # Если сумма в USDT, рассчитываем количество токенов
    if in_usdt and price:
        usdt_amount = float(qty)
        calculated_qty = client.calculate_qty_from_usdt(symbol, usdt_amount, Category.SPOT)
        if calculated_qty:
            print(f"   💰 ${usdt_amount} USDT = {calculated_qty} {symbol.replace('USDT', '')}")
            qty = calculated_qty
        else:
            print("   ❌ Не удалось рассчитать количество")
            return None
    
    # Показываем минимальный ордер
    min_qty = client.get_min_order_qty(symbol, Category.SPOT)
    if min_qty:
        min_value = float(min_qty) * price if price else 0
        print(f"   📊 Минимальный ордер: {min_qty} (~${min_value:.2f})")
    
    # Подтверждение
    confirm = input("\n   ⚠️  Создать ордер? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.market_sell(symbol, qty, Category.SPOT)
    print(format_order_result(result))
    
    return result


def test_limit_buy(symbol: str = "BTCUSDT", qty: str = "0.001", price: str = "50000"):
    """
    Тест: Лимитная покупка
    
    ⚠️ ВНИМАНИЕ: Этот тест создает РЕАЛЬНЫЙ ордер!
    """
    print("\n" + "="*60)
    print(f"ТЕСТ: Лимитная покупка {qty} {symbol.replace('USDT', '')} @ ${price}")
    print("="*60)
    
    client = create_client()
    
    # Показываем текущую цену
    current_price = client.get_current_price(symbol, Category.SPOT)
    print(f"   Текущая цена: ${current_price:,.2f}" if current_price else "   Цена недоступна")
    
    # Показываем минимальный ордер
    min_qty = client.get_min_order_qty(symbol, Category.SPOT)
    if min_qty and current_price:
        min_value = float(min_qty) * current_price
        print(f"   📊 Минимальный ордер: {min_qty} (~${min_value:.2f})")
    
    # Показываем стоимость ордера
    order_value = float(qty) * float(price)
    print(f"   💵 Стоимость ордера: ${order_value:,.2f}")
    
    # Подтверждение
    confirm = input("\n   ⚠️  Создать ордер? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.limit_buy(symbol, qty, price, Category.SPOT)
    print(format_order_result(result))
    
    return result


def test_limit_sell(symbol: str = "BTCUSDT", qty: str = "0.001", price: str = "150000"):
    """
    Тест: Лимитная продажа
    
    ⚠️ ВНИМАНИЕ: Этот тест создает РЕАЛЬНЫЙ ордер!
    """
    print("\n" + "="*60)
    print(f"ТЕСТ: Лимитная продажа {qty} {symbol.replace('USDT', '')} @ ${price}")
    print("="*60)
    
    client = create_client()
    
    # Показываем текущую цену
    current_price = client.get_current_price(symbol, Category.SPOT)
    print(f"   Текущая цена: ${current_price:,.2f}" if current_price else "   Цена недоступна")
    
    # Показываем минимальный ордер
    min_qty = client.get_min_order_qty(symbol, Category.SPOT)
    if min_qty and current_price:
        min_value = float(min_qty) * current_price
        print(f"   📊 Минимальный ордер: {min_qty} (~${min_value:.2f})")
    
    # Показываем стоимость ордера
    order_value = float(qty) * float(price)
    print(f"   💵 Стоимость ордера: ${order_value:,.2f}")
    
    # Подтверждение
    confirm = input("\n   ⚠️  Создать ордер? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.limit_sell(symbol, qty, price, Category.SPOT)
    print(format_order_result(result))
    
    return result


def test_open_orders(symbol: str = None):
    """Тест: Получение открытых ордеров"""
    print("\n" + "="*60)
    print(f"ТЕСТ: Открытые ордера {symbol or 'все'}")
    print("="*60)
    
    client = create_client()
    result = client.get_open_orders(Category.SPOT, symbol)
    
    if result.get("retCode") == 0:
        orders = result.get("result", {}).get("list", [])
        if orders:
            print(f"✅ Найдено ордеров: {len(orders)}")
            for order in orders:
                print(f"\n   Order ID: {order.get('orderId')}")
                print(f"   {order.get('side')} {order.get('qty')} {order.get('symbol')}")
                print(f"   Цена: {order.get('price')}")
                print(f"   Статус: {order.get('orderStatus')}")
        else:
            print("   Открытых ордеров нет")
    else:
        print(f"❌ Ошибка: {result.get('retMsg')}")
    
    return result


def test_cancel_order(symbol: str, order_id: str):
    """Тест: Отмена ордера"""
    print("\n" + "="*60)
    print(f"ТЕСТ: Отмена ордера {order_id}")
    print("="*60)
    
    client = create_client()
    
    confirm = input("   ⚠️  Отменить ордер? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.cancel_order(Category.SPOT, symbol, order_id=order_id)
    print(format_order_result(result))
    
    return result


def test_cancel_all_orders(symbol: str = None):
    """Тест: Отмена всех ордеров"""
    print("\n" + "="*60)
    print(f"ТЕСТ: Отмена ВСЕХ ордеров {symbol or ''}")
    print("="*60)
    
    client = create_client()
    
    confirm = input("   ⚠️  Отменить ВСЕ ордера? (yes/no): ")
    if confirm.lower() != "yes":
        print("   Отменено")
        return None
    
    result = client.cancel_all_orders(Category.SPOT, symbol)
    print(format_order_result(result))
    
    return result


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    БЫСТРЫЙ СТАРТ                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

def run_all_info_tests():
    """Запуск всех информационных тестов (безопасно)"""
    print("\n" + "🚀 ЗАПУСК ИНФОРМАЦИОННЫХ ТЕСТОВ ".center(60, "="))
    
    test_connection()
    test_balance()
    test_get_price("BTCUSDT")
    test_orderbook("BTCUSDT")
    test_instrument_info("BTCUSDT")
    
    print("\n" + "="*60)
    print("✅ Все информационные тесты завершены!")
    print("="*60)


def interactive_menu():
    """Интерактивное меню"""
    while True:
        print("\n" + "="*60)
        print("        BYBIT TRADING CLIENT - МЕНЮ")
        print("="*60)
        print(f"   Режим: {'🧪 TESTNET' if USE_TESTNET else '💰 MAINNET'}")
        print("-"*60)
        print("   ИНФОРМАЦИЯ:")
        print("   1. Проверить подключение")
        print("   2. Проверить баланс")
        print("   3. Получить цену")
        print("   4. Стакан ордеров")
        print("   5. Информация об инструменте")
        print("-"*60)
        print("   ТОРГОВЛЯ:")
        print("   6. Рыночная покупка")
        print("   7. Рыночная продажа")
        print("   8. Лимитная покупка")
        print("   9. Лимитная продажа")
        print("-"*60)
        print("   УПРАВЛЕНИЕ:")
        print("   10. Открытые ордера")
        print("   11. Отменить ордер")
        print("   12. Отменить все ордера")
        print("-"*60)
        print("   0. Выход")
        print("="*60)
        
        choice = input("\n   Выберите опцию: ").strip()
        
        if choice == "0":
            print("\n   До свидания! 👋")
            break
        elif choice == "1":
            test_connection()
        elif choice == "2":
            test_balance()
        elif choice == "3":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            test_get_price(symbol)
        elif choice == "4":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            test_orderbook(symbol)
        elif choice == "5":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            test_instrument_info(symbol)
        elif choice == "6":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            mode = input("   Ввести сумму в USDT? (y/n, по умолчанию n): ").strip().lower()
            in_usdt = mode == "y" or mode == "yes"
            if in_usdt:
                qty = input("   Введите сумму в USDT: ").strip()
            else:
                qty = input("   Введите количество токенов (0.001): ").strip() or "0.001"
            test_market_buy(symbol, qty, in_usdt)
        elif choice == "7":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            mode = input("   Ввести сумму в USDT? (y/n, по умолчанию n): ").strip().lower()
            in_usdt = mode == "y" or mode == "yes"
            if in_usdt:
                qty = input("   Введите сумму в USDT: ").strip()
            else:
                qty = input("   Введите количество токенов (0.001): ").strip() or "0.001"
            test_market_sell(symbol, qty, in_usdt)
        elif choice == "8":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            qty = input("   Введите количество (0.001): ").strip() or "0.001"
            price = input("   Введите цену: ").strip()
            test_limit_buy(symbol, qty, price)
        elif choice == "9":
            symbol = input("   Введите пару (BTCUSDT): ").strip() or "BTCUSDT"
            qty = input("   Введите количество (0.001): ").strip() or "0.001"
            price = input("   Введите цену: ").strip()
            test_limit_sell(symbol, qty, price)
        elif choice == "10":
            symbol = input("   Введите пару (пусто = все): ").strip() or None
            test_open_orders(symbol)
        elif choice == "11":
            symbol = input("   Введите пару: ").strip()
            order_id = input("   Введите Order ID: ").strip()
            test_cancel_order(symbol, order_id)
        elif choice == "12":
            symbol = input("   Введите пару (пусто = все): ").strip() or None
            test_cancel_all_orders(symbol)
        else:
            print("   ❌ Неизвестная опция")
        
        input("\n   Нажмите Enter для продолжения...")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    ТОЧКА ВХОДА                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║           BYBIT TRADING CLIENT - ТЕСТИРОВАНИЕ                ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  ⚠️  Перед началом убедитесь что:                            ║
    ║     1. Вы заполнили API_KEY и API_SECRET                     ║
    ║     2. USE_TESTNET = True для тестирования                   ║
    ║     3. У вас есть тестовые средства на testnet               ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    if API_KEY == "ВАШ_API_KEY" or API_SECRET == "ВАШ_API_SECRET":
        print("   ❌ ОШИБКА: Заполните API_KEY и API_SECRET в файле!")
        print("   📖 См. инструкцию в README.md")
    else:
        # Выберите один из вариантов:
        
        # Вариант 1: Запуск интерактивного меню
        interactive_menu()
        
        # Вариант 2: Запуск только информационных тестов
        # run_all_info_tests()
        
        # Вариант 3: Запуск конкретного теста
        # test_connection()
        # test_balance()
        # test_market_buy("BTCUSDT", "0.001")