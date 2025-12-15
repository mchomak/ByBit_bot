"""TelegramNotifier: асинхронный отправщик сообщений в Telegram через aiogram."""

import asyncio
import logging
from queue import Queue, Empty
from aiogram import Bot
from aiogram.client.bot import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from typing import Optional
from loguru import logger


class TelegramNotifier:
    """
    Async notifier: читает из asyncio.Queue и шлёт сообщения через aiogram.
    """

    def __init__(
        self,
        token: str,
        chat_id: str | int,
        message_queue: asyncio.Queue[str],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        poll_interval: float = 1.0,
    ):
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(
                parse_mode="HTML",
                protect_content=False,
            ),
        )
        self.chat_id = chat_id
        self.queue = message_queue
        self.loop = loop or asyncio.get_event_loop()
        self.poll_interval = poll_interval

        self._consumer_task: Optional[asyncio.Task] = None
        self.logger = logger.bind(component="TelegramNotifier")


    async def start(self) -> None:
        """Запускаем consumer-задачу в текущем loop."""
        if self._consumer_task is None:
            self._consumer_task = self.loop.create_task(self._message_consumer())
            self.logger.info("TelegramNotifier started")


    async def _message_consumer(self) -> None:
        """Постоянно ждём новые сообщения в очереди и отправляем их."""
        while True:
            msg = await self.queue.get()
            try:
                if isinstance(msg, dict):
                    # ожидаем, что msg = {"text": ..., "parse_mode": ...}
                    text = msg.get("text")
                    parse_mode = msg.get("parse_mode", "HTML")
                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=text,
                        parse_mode=parse_mode
                    )
                else:
                    # на случай, если вдруг в очередь попала простая строка
                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=msg
                    )

                self.logger.debug("Sent message to Telegram: {}", msg)

            except TelegramAPIError as e:
                self.logger.error("Telegram API error: {}", e)

            finally:
                self.queue.task_done()

            await asyncio.sleep(self.poll_interval)


    async def stop(self) -> None:
        """Останавливаем consumer и закрываем сессию бота."""
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task

            except asyncio.CancelledError:
                pass

        await self.bot.session.close()
        self.logger.info("TelegramNotifier stopped")

