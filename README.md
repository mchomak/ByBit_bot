# Bybit Trading Bot v1.0.0

Автоматизированный торговый бот для биржи Bybit. Мониторит 300+ монет 24/7, обнаруживает импульсные движения (аномальный объём + ценовое ускорение) и исполняет сделки с автоматическим управлением позициями.

> **Disclaimer**: Торговля криптовалютами сопряжена с высоким риском. Используйте Demo режим для тестирования. Авторы не несут ответственности за финансовые потери.

---

## Содержание

- [Возможности](#возможности)
- [Архитектура](#архитектура)
- [Быстрый старт](#быстрый-старт)
  - [Вариант 1: Запуск через Python](#вариант-1-запуск-через-python)
  - [Вариант 2: Запуск через Docker](#вариант-2-запуск-через-docker)
- [Конфигурация](#конфигурация)
- [Получение API ключей](#получение-api-ключей)
- [Фильтрация токенов](#фильтрация-токенов)
- [Торговая стратегия](#торговая-стратегия)
- [Структура проекта](#структура-проекта)
- [Мониторинг и логи](#мониторинг-и-логи)
- [Troubleshooting](#troubleshooting)

---

## Возможности

- Автоматическая торговля на спотовом рынке Bybit
- 7 уровней фильтрации токенов (капитализация, объём, риски и др.)
- Уведомления в Telegram (сделки, ошибки, ежедневные отчёты)
- Поддержка Demo и Production режимов
- Защита от торговли "мёртвыми" токенами (StalePrice)
- Автоматическое отключение убыточных токенов (BigLoss)
- Синхронизация серверного времени Bybit

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                         BYBIT API                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  REST API    │  │  WebSocket   │  │  Trading API         │  │
│  │  (данные)    │  │  (real-time) │  │  (ордера, баланс)    │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└─────────┼─────────────────┼─────────────────────┼──────────────┘
          │                 │                     │
          ▼                 ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TRADING BOT                                │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐ │
│  │ Token Sync      │    │ Market Data     │    │ Execution   │ │
│  │ Service         │───▶│ Service         │───▶│ Engine      │ │
│  │ (Bybit+Paprika) │    │ (WS Klines)     │    │ (Orders)    │ │
│  └─────────────────┘    └─────────────────┘    └─────────────┘ │
│          │                      │                     │         │
│          ▼                      ▼                     ▼         │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    PostgreSQL Database                      ││
│  │  ┌──────────┐ ┌────────────┐ ┌─────────┐ ┌───────────────┐ ││
│  │  │  tokens  │ │ all_tokens │ │ candles │ │   positions   │ ││
│  │  └──────────┘ └────────────┘ └─────────┘ └───────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                  │
│                              ▼                                  │
│                    ┌─────────────────┐                         │
│                    │    Telegram     │                         │
│                    │    Notifier     │                         │
│                    └─────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Быстрый старт

### Требования

- **Python**: 3.11+ (рекомендуется 3.12)
- **PostgreSQL**: 14+
- **Bybit API**: Demo или Production ключи
- **Telegram Bot** (опционально): для уведомлений

---

### Вариант 1: Запуск через Python

#### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/your-repo/ByBit_bot.git
cd ByBit_bot
```

#### Шаг 2: Создание виртуального окружения

```bash
# Создание
python -m venv venv

# Активация (Linux/macOS)
source venv/bin/activate

# Активация (Windows)
venv\Scripts\activate
```

#### Шаг 3: Установка зависимостей

```bash
pip install -r requirements.txt
```

#### Шаг 4: Настройка PostgreSQL

```bash
# Создание базы данных (Linux/macOS)
sudo -u postgres psql -c "CREATE DATABASE bybit_bot;"
sudo -u postgres psql -c "CREATE USER botuser WITH PASSWORD 'botpass';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bybit_bot TO botuser;"
sudo -u postgres psql -c "ALTER DATABASE bybit_bot OWNER TO botuser;"

# Windows (PowerShell от имени администратора)
psql -U postgres -c "CREATE DATABASE bybit_bot;"
psql -U postgres -c "CREATE USER botuser WITH PASSWORD 'botpass';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE bybit_bot TO botuser;"
```

#### Шаг 5: Конфигурация

```bash
# Копируем пример конфигурации
cp .env.example .env

# Редактируем файл (используйте любой редактор)
nano .env
```

**Минимальная конфигурация `.env`:**

```env
# === База данных ===
DATABASE_URL=postgresql+asyncpg://botuser:botpass@localhost:5432/bybit_bot

# === Bybit API (Demo режим) ===
DEMO=true
BYBIT_DEMO_API_KEY=ваш_demo_api_key
BYBIT_DEMO_API_SECRET=ваш_demo_api_secret

# === Telegram (опционально) ===
TELEGRAM_BOT_TOKEN=ваш_telegram_bot_token
TELEGRAM_CHAT_ID=ваш_chat_id

# === Режим работы ===
DRY_RUN=true
# true = симуляция (ордера НЕ отправляются)
# false = реальные ордера
```

#### Шаг 6: Запуск бота

```bash
python -m core.main
```

**Ожидаемый вывод при успешном запуске:**

```
INFO     | Bot starting in TESTNET mode
INFO     | Database connection established
INFO     | Fetching Bybit USDT trading pairs...
INFO     | Found 450 USDT trading pairs on Bybit
INFO     | Token sync completed: 450 total, 127 tradable
INFO     | WebSocket connected to stream.bybit.com
INFO     | Bot is running. Press Ctrl+C to stop.
```

---

### Вариант 2: Запуск через Docker

#### Шаг 1: Установка Docker

- **Linux**: https://docs.docker.com/engine/install/
- **macOS/Windows**: https://www.docker.com/products/docker-desktop

#### Шаг 2: Клонирование и настройка

```bash
git clone https://github.com/your-repo/ByBit_bot.git
cd ByBit_bot

# Копируем конфигурацию
cp .env.example .env
nano .env
```

**Конфигурация `.env` для Docker:**

```env
# === PostgreSQL (для docker-compose) ===
POSTGRES_USER=botuser
POSTGRES_PASSWORD=botpass
POSTGRES_DB=bybit_bot

# === Bybit API ===
DEMO=true
BYBIT_DEMO_API_KEY=ваш_demo_api_key
BYBIT_DEMO_API_SECRET=ваш_demo_api_secret

# === Telegram ===
TELEGRAM_BOT_TOKEN=ваш_telegram_bot_token
TELEGRAM_CHAT_ID=ваш_chat_id

# === Режим работы ===
DRY_RUN=true
```

#### Шаг 3: Сборка и запуск

```bash
# Сборка образа и запуск контейнеров
docker-compose up -d --build

# Проверка статуса
docker-compose ps
```

**Ожидаемый вывод:**

```
NAME              STATUS                   PORTS
bybit_bot         Up 2 minutes
bybit_bot_db      Up 2 minutes (healthy)   5432/tcp
```

#### Шаг 4: Просмотр логов

```bash
# Логи бота в реальном времени
docker-compose logs -f bot

# Логи базы данных
docker-compose logs -f db
```

#### Полезные команды Docker

```bash
# Остановка
docker-compose down

# Перезапуск бота
docker-compose restart bot

# Вход в контейнер бота
docker-compose exec bot bash

# Подключение к базе данных
docker-compose exec db psql -U botuser -d bybit_bot

# Полная очистка (включая данные БД!)
docker-compose down -v
```

---

## Конфигурация

### Основные параметры

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DEMO` | `true` | `true` = Demo аккаунт, `false` = Production |
| `DRY_RUN` | `true` | `true` = симуляция, `false` = реальные ордера |
| `RISK_PER_TRADE_PCT` | `5.0` | Процент капитала на одну сделку |
| `MAX_POSITIONS` | `5` | Максимум одновременно открытых позиций |
| `STOP_LOSS_PCT` | `7.0` | Стоп-лосс в процентах |
| `MIN_MARKET_CAP_USD` | `100000000` | Минимальная капитализация ($100M) |

### Параметры стратегии

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `VOLUME_WINDOW_DAYS` | `5` | Период анализа объёма (дней) |
| `PRICE_ACCELERATION_FACTOR` | `3.0` | Фактор ценового ускорения |
| `MA_EXIT_PERIOD` | `14` | Период MA для сигнала выхода |

Полный список параметров с описаниями: см. файл `.env.example`

---

## Получение API ключей

### Bybit Demo API (для тестирования)

1. Зарегистрируйтесь на [demo.bybit.com](https://demo.bybit.com)
2. Перейдите: **Account** → **API Management**
3. Нажмите **Create New Key**
4. Настройки:
   - API Key Type: **System-generated**
   - Permissions: **Read-Write**
   - Contract: **USDT Perpetual**, **Spot** ✓
5. Скопируйте **API Key** и **API Secret**

### Bybit Production API (для реальной торговли)

1. Войдите на [bybit.com](https://www.bybit.com)
2. Перейдите: **Account & Security** → **API Management**
3. Создайте ключ с правами **Read-Write** и **Spot Trading**
4. **Рекомендуется**: Ограничьте доступ по IP-адресу

### Telegram Bot Token

1. Откройте [@BotFather](https://t.me/BotFather) в Telegram
2. Отправьте `/newbot`
3. Следуйте инструкциям, скопируйте токен
4. Узнайте Chat ID через [@userinfobot](https://t.me/userinfobot)

---

## Фильтрация токенов

Бот начинает с полного списка токенов Bybit и применяет 7 фильтров:

| # | Фильтр | Условие | Когда |
|---|--------|---------|-------|
| 1 | **Bybit** | USDT пара в статусе Trading | При синхронизации |
| 2 | **Blacklist** | Ручной чёрный список | При синхронизации |
| 3 | **ST Tokens** | Высокорисковые токены (авто-blacklist) | При синхронизации |
| 4 | **NoMcapData** | Нет данных о капитализации | При синхронизации |
| 5 | **LowMcap** | Капитализация < $100M | При синхронизации |
| 6 | **LowVolume** | 24h объём < $700k | При синхронизации |
| 7 | **StalePrice** | ≥80% плоских свечей ИЛИ ≥3 подряд | Каждые 50 мин |
| 8 | **BigLoss** | Убыток > 1% по сделке | При закрытии |

---

## Торговая стратегия

### Условия входа (все должны выполняться)

1. **Volume Spike**: Текущий объём > максимального за 5 дней
2. **Price Acceleration**: close ≥ open × 3.0
3. **Price > MA14**: Цена выше 14-периодной скользящей средней
4. **Token Active**: Токен прошёл все фильтры
5. **Risk Check**: Открытых позиций < MAX_POSITIONS

### Условия выхода

1. **MA Crossover**: Цена пересекла MA14 сверху вниз

### Управление рисками

- **Position Sizing**: RISK_PER_TRADE_PCT% от доступного баланса
- **Stop-Loss**: Автоматическое закрытие при падении на STOP_LOSS_PCT%
- **BigLoss Protection**: Токен отключается после убытка >1%
- **Stale Price Protection**: Неактивные токены исключаются

---

## Структура проекта

```
ByBit_bot/
├── config/
│   └── config.py              # Настройки из .env
├── core/
│   ├── main.py                # Точка входа
│   ├── bootstrap_logging.py   # Настройка логирования
│   └── create_daily_report.py # Ежедневные отчёты
├── db/
│   ├── database.py            # Подключение к БД
│   ├── models.py              # SQLAlchemy модели
│   └── repository.py          # CRUD операции
├── services/
│   ├── bybit_client.py        # REST + WebSocket клиент
│   ├── strategy_engine.py     # Торговая стратегия
│   ├── execution_engine.py    # Исполнение ордеров
│   ├── paprika_bybit_matcher.py  # Синхронизация токенов
│   ├── stale_price_checker.py    # Проверка активности цен
│   └── real_order_executor.py    # Отправка ордеров
├── bot/
│   ├── TelegramBot.py         # Telegram уведомления
│   └── TelegramNotifier.py
├── trade/
│   └── trade_client.py        # Торговый клиент
├── .env.example               # Пример конфигурации
├── requirements.txt           # Python зависимости
├── Dockerfile                 # Docker образ
└── docker-compose.yml         # Docker Compose
```

---

## Мониторинг и логи

### Просмотр логов

```bash
# Python запуск
tail -f logs/bot.log

# Docker
docker-compose logs -f bot
```

### Telegram уведомления

| Тип | Пример |
|-----|--------|
| Покупка | 🟢 **Покупка** BTCUSDT @ 42,150.50 |
| Продажа | 🔴 **Продажа** BTCUSDT P&L: +2.5% |
| Отключение токена | ⛔ Токен отключён: XYZ (убыток -5.5%) |
| Ежедневный отчёт | 📊 Ордеров: 15, P&L: +$127.50 |

### SQL запросы для диагностики

```sql
-- Проверить статус токена
SELECT symbol, is_active, deactivation_reason
FROM all_tokens WHERE symbol = 'BTC';

-- Список отключённых токенов
SELECT symbol, deactivation_reason
FROM all_tokens WHERE is_active = false;

-- Открытые позиции
SELECT * FROM positions WHERE status = 'OPEN';
```

---

## Troubleshooting

### Ошибка подключения к БД

```
sqlalchemy.exc.OperationalError: connection refused
```

**Решение**: Проверьте что PostgreSQL запущен и DATABASE_URL корректен.

### Invalid API Key

```
Bybit API Error: Invalid API key
```

**Решение**: Убедитесь что используете Demo ключи при `DEMO=true` и Production при `DEMO=false`.

### Invalid timestamp

```
Bybit API Error: Invalid timestamp
```

**Решение**: Бот автоматически синхронизирует время. Если ошибка повторяется - проверьте системные часы.

### Docker: Permission denied

```
PermissionError: [Errno 13] Permission denied: '/app/logs'
```

**Решение**:
```bash
chmod -R 777 logs/
docker-compose restart bot
```

---

## Режимы работы

| Режим | DEMO | DRY_RUN | Описание |
|-------|------|---------|----------|
| **Тестирование** | `true` | `true` | Симуляция на Demo данных |
| **Demo торговля** | `true` | `false` | Реальные ордера на Demo бирже |
| **Production** | `false` | `false` | Реальная торговля |

---

## Лицензия

MIT License

---

## Поддержка

- **Issues**: https://github.com/your-repo/ByBit_bot/issues
