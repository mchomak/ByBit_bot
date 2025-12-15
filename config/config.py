from dotenv import load_dotenv
import os
from loguru import logger


RAYDIUM_V4_ADDRESS = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

SOL_ADDRESS = "So11111111111111111111111111111111111111112"

USDC_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def load_env(env_file:str=".env"):
    """Загружает переменные окружения из файла .env"""
    load_dotenv(env_file)

    RPC_URL = os.getenv("RPC_URL")
    PRIVATE_WALLET_KEY = os.getenv("PRIVATE_WALLET_KEY")
    ADDRESS_WALLET = os.getenv("ADDRESS_WALLET")
    DATABASE_URL = os.getenv("DATABASE_URL")
    DATABASE_DOCKER_URL = os.getenv("DATABASE_DOCKER_URL")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    logger.log("INFO", "All variables loaded successfully")
    return RPC_URL, DATABASE_URL, DATABASE_DOCKER_URL, BOT_TOKEN, ADDRESS_WALLET, PRIVATE_WALLET_KEY


RPC_URL, DATABASE_URL, DATABASE_DOCKER_URL, BOT_TOKEN, WALLET_ADDRESS, PRIVATE_WALLET_KEY = load_env()
TX_VERSION   = "V0"

RAY_COMPUTE_URL = "https://transaction-v1.raydium.io/compute/swap-base-in"
RAY_TX_URL      = "https://transaction-v1.raydium.io/transaction/swap-base-in"
RAY_AUTO_FEE    = "https://api-v3.raydium.io/main/auto-fee"

# Подтверждать до какого уровня: "processed" | "confirmed" | "finalized"
CONFIRM_LEVEL = "finalized"
MAX_WAIT_S    = 90.0  # больше 30с
FEE_MULTIPLIER = 1.3  # слегка ускоряем включение

CONFIRM_TIMEOUT_S: float = float(45)     # таймаут для confirmed
FINALIZE_TIMEOUT_S: float = float(90)   # доп. таймаут для finalized
POLL_START_DELAY_S: float = float("0.4")  # стартовая задержка опроса

# Поведение SOL/WSOL
WRAP_SOL: bool = True
UNWRAP_SOL: bool = True

# Логи
LOG_LEVEL: str = "DEBUG"