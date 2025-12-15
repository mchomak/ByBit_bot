import logging
import inspect
from pathlib import Path
from loguru import logger
import asyncio
import sys

class InterceptHandler(logging.Handler):
    """Redirect standard `logging` records to Loguru."""
    def emit(self, record: logging.LogRecord):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = inspect.currentframe(), 2
        # Skip internal logging frames
        while frame and frame.f_code.co_filename == logging.__file__:
            frame, depth = frame.f_back, depth + 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

def rotate_on_line_count(max_lines: int):
    """
    Возвращает функцию ротации, которая считает количество строк
    по количеству '\\n' в каждом сообщении и ротирует при >= max_lines.
    """
    state = {"count": 0}

    def should_rotate(message, file):
        # message может быть bytes или str
        text = message.decode('utf-8', 'ignore') if isinstance(message, (bytes, bytearray)) else message
        # считаем новые строки
        new_lines = text.count("\n")
        state["count"] += new_lines
        if state["count"] >= max_lines:
            # сбросим счётчик на остаток (чтоб не потерять переносы в этом же сообщении)
            state["count"] = state["count"] - max_lines
            return True
        return False

    return should_rotate

def setup_logging(log_dir: str | Path, telegram_queue: asyncio.Queue[str] | None = None) -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    trace_dir = log_dir / "trace"
    trace_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()

    # 1) Общий лог (всё)
    logger.add(
        log_dir / "bot.log",
        level="TRACE",
        rotation="1 week",
        retention="12 weeks",
        enqueue=True,
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )

    # 2) Логи по транзакциям
    logger.add(
        log_dir / "trading.log",
        level="TRACE",
        filter=lambda r: r["extra"].get("stream") == "trading",
        rotation="1 week",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )

    # 3) Важные логи — INFO и выше
    logger.add(
        log_dir / "important.log",
        level="INFO",
        rotation="1 week",
        retention="4 weeks",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )

    # 4) Полный trace-лог ротацией по строкам
    logger.add(
        trace_dir / "trace.log",
        level="TRACE",
        rotation=rotate_on_line_count(100_000),
        retention="8 weeks",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
    )

    # 5) Логи в консоль
    logger.add(
        sys.stdout,
        level="INFO",  # Уровень INFO для консольных логов
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )

    # CRITICAL → Telegram
    if telegram_queue is not None:
        def _telegram_sink(message):
            # message — это экземпляр loguru._handler.Message
            record = message.record
            text = f"<b>{record['level'].name}</b>: {record['message'].strip()}"
            try:
                loop = asyncio.get_event_loop()
                loop.call_soon_threadsafe(telegram_queue.put_nowait, text)
            except RuntimeError:
                pass

        logger.add(
            _telegram_sink,
            level="CRITICAL",
            enqueue=True,
            catch=True,
        )

    # Заменяем root handlers
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
