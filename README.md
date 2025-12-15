# ReBuild Trading Bot (Solana × Raydium × ML)

Production‑ready algorithmic trading bot for **Solana** on DEX **Raydium** using ML models.  
The bot collects and filters pools, builds a dataset from OHLCV (GeckoTerminal), trains the LSTM model, makes purchase/sale decisions based on the strategy configuration, and sends reports to **Telegram**. It works locally or in Docker. Secrets and keys are available only through environment variables.

> **Disclaimer**: The project is provided for educational purposes. Trading crypto assets is risky. Use mock mode/non‑combat wallet at your own risk.

---

## Content
- [Features](#features)
- [Architecture](#architecture)
- [Repository structure](#repository-structure)
- [Quick start](#quick-start)
  - [Requirements](#requirements)
  - [Configuration](#configuration)
- [Work stages](#work-stages)
  - [1) Collecting and filtering Raydium pools](#1-collecting-and-filtering-raydium-pools)
  - [2) Dataset formation](#2-dataset-formation)
  - [3) Model training](#3-model-training)
  - [4) Validation](#4-validation)
  - [5) Launching the bot](#5-launching-the-bot)
- [Strategy parameters](#strategy-parameters)
- [Docker](#docker)
- [Testing](#testing)
- [Practical notes](#practical-notes)
- [Recommended GitHub tags](#recommended-github-tags)
- [License](#license)

---

## Features
- **Solana + Raydium**: collecting *all* pools and filtering according to your criteria (liquidity, number of holders, 24h volume, etc.).
- **Data pipeline**: OHLCV from **GeckoTerminal** → PostgreSQL.
- **ML**: preprocessing, LSTM training, artifact export (model, scaler, t_scaler), offline threshold validation.
- **Execution**: entry/exit rules by thresholds, two purchase modes, position limits.
- **Reporting**: messages in the Telegram channel(s).
- **Operational practices**: configuration in JSON, secrets from `.env`, launch via Docker or directly from VSCode.

---

## Architecture

```mermaid
flowchart LR
    A[Raydium pools] -->|filtered pools| B[(PostgreSQL)]
    A2[GeckoTerminal OHLCV] --> B
    B --> C[ML preprocessing]
    C --> D[LSTM training]
    D --> E[Validation & thresholds]
    E --> F[Strategy / transaction planner]
    F --> G[Solana contractor]
    G --> H[Telegram reports]
```

---

## Repository structure

```text
.
├── core/
│   ├── main.py                 # launching the bot (locally)
│   ├── docker_main.py          # Docker entry point
│   ├── main_config.json        # strategy/path settings
│   ├── execution/              # tx execution (slippage, retries)
│   ├── strategy/               # thresholds and sizing rules (buy_mode)
│   ├── reporters/              # Telegram reports and other sinks
│   └── utils/                  # logs, metrics, helpers
├── ml/
│   ├── use_preprocessor.py     # building dataset
│   ├── lstm_trainer.py         # train the model (PyTorch)
│   ├── validate.py             # validation and review of metrics by thresholds
│   ├── ready_models/           # exported .pth
│   ├── scalers/                # .joblib for features
│   └── t_scalers/              # .pt for targets
├── api/
│   └── gecko_ohlcv_new.py      # get OHLCV of GeckoTerminal → SQL
├── tools/
│   ├── raydium_pools.ipynb     # full acquisition/enrichment of pools → DB
│   └── ...                     # auxiliary scripts
├── configs/
│   ├── dataset_config.yaml     # dataset config example
│   └── trainer_config.yaml     # training config example
├── reports/                    # reports and graphs
├── logs/                       # logs
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── tests/                      # pytest (unit/integration/e2e)
├── .env.example                # sample with empty values
├── requirements.txt / pyproject.toml
└── README.md
```

---

## Quick start

### Requirements
- Python **3.12+** or Docker
- PostgreSQL **13+**
- Available **Solana RPC** (public/paid)
- Telegram bot token and Channel ID

### Configuration

#### 1) Secrets in `.env`
Copy `.env.example` → `.env` and fill in:

```bash
# Solana / Wallet
SOLANA_RPC_URL=...
SOL_PRIVATE_KEY=...              # Don't show it to anyone!
SOL_WALLET_ADDRESS=...

# Database
POSTGRES_DSN=postgresql://user:pass@host:5432/dbname

# Telegram
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHANNEL_ID=-100XXXXXXXXXX

# External APIs
RAYDIUM_API_BASE=https://api-v3.raydium.io/
GECKOTERMINAL_BASE=https://api.geckoterminal.com/
```

#### 2) Runtime configuration `core/main_config.json`

```json
{
  "scaler_path": "ml/scalers/scaler_test_47_fold1.joblib",
  "model_path": "ml/ready_models/model_test_47_fold1.pth",
  "t_scaler_path": "ml/t_scalers/t_scaler_test_47_fold1.pt",
  "report_path": "reports",
  "tg_channel_id": "-1xxxxxxxxxxxx",
  "log_path": "logs",
  "threshold_buy": 0.8,
  "threshold_sell": 0.6,
  "buy_mode": "correlation",
  "max_tokens_cnt": 20,
  "sol_per_trade": 0.001,
  "max_alloc_per_pos": 1.0
}
```

- `threshold_buy` — if the model score ≥ threshold → **buy**.
- `threshold_sell` — if the model score < threshold → **sell**.
- `buy_mode`:
  - **correlation** — `sol_per_trade` is **minimum**, allocation is scaled by the confidence of the model, but not higher than `max_alloc_per_pos`. The `max_tokens_cnt` parameter does not limit the purchase until the balance is exhausted.
  - **max_tokens** — purchase of exactly `sol_per_trade` and no more than `max_tokens_cnt` active positions.

---

## Work stages

### 1) Collecting and filtering Raydium pools
Open the notebook `tools/raydium_pools.ipynb`: get all the pools, filter (Raydium only and according to the criteria of liquidity/holders/volume), enrich and save to the database.

### 2) Dataset formation

```bash
python ml/use_preprocessor.py   --dsn "$POSTGRES_DSN"   --timeframe day --window 128   --features rsi,ema,corr,vol,spread   --out data/dataset_v1.parquet
```

### 3) Model training

```bash
python ml/lstm_trainer.py   --dataset data/dataset_v1.parquet   --model-out ml/ready_models/model_v1.pth   --scaler-out ml/scalers/scaler_v1.joblib   --t-scaler-out ml/t_scalers/t_scaler_v1.pt   --epochs 50 --batch-size 512 --lr 1e-3
```

### 4) Validation

```bash
python ml/validate.py   --dataset data/dataset_v1.parquet   --model ml/ready_models/model_v1.pth   --scaler ml/scalers/scaler_v1.joblib   --t-scaler ml/t_scalers/t_scaler_v1.pt   --thresholds 0.5,0.6,0.7,0.8,0.9
```

The script will output metrics by thresholds and save graphs in `reports/`.

### 5) Launching the bot

**Locally**

```bash
python core/main.py --config core/main_config.json
```

**Docker**

```bash
docker build -t rebuild-bot ./docker
docker run --rm --env-file .env   -v $(pwd)/core/main_config.json:/app/core/main_config.json   rebuild-bot
```

---

## Strategy parameters
- **Input**: `score ≥ threshold_buy` → open/increase.
- **Output**: `score < threshold_sell` → close.
- **Sizing**:
  - mode **correlation** (minimum = `sol_per_trade`, confidence scale, cap `max_alloc_per_pos`)
  - or **max_tokens** (exactly `sol_per_trade`, maximum `max_tokens_cnt`).

---

## Docker
We recommend a multistage build + `.dockerignore`, pinned dependencies and a minimum of artifacts in the runtime layer.  
The `CMD` example is set to `core/docker_main.py`.

---

## Testing
We use **pytest** (unit/integration/e2e).

Basic launch:

```bash
python -m pytest -q
```

Recommended practices: fixtures and mockups for network/chain calls; property‑based tests (Hypothesis) for sizing functions and rules.

---

## Practical notes
- **GeckoTerminal OHLCV**: use the API at the pool address and the desired timeframe.
- **Solana RPC**: specify a reliable endpoint, provide a backup.
- **Telegram**: add the bot as an admin to the channel; format messages (`MarkdownV2`/`HTML`).

---

## Recommended GitHub tags
`solana`, `raydium`, `defi`, `dex`, `amm`, `algorithmic-trading`, `trading-bot`, `market-data`, `ohlcv`, `geckoterminal`, `lstm`, `machine-learning`, `time-series`, `pytorch`, `python`, `docker`, `postgresql`, `telegram-bot`, `jupyter-notebook`, `pytest`, `ci-cd`

---

## License
MIT or your internal license of choice.
