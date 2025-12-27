"""TelegramBot: Full-featured Telegram bot with commands and notifications."""

import asyncio
from datetime import datetime
from typing import Optional, Callable, Awaitable

from aiogram import Bot, Dispatcher, Router
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker


class TelegramBot:
    """
    Full-featured Telegram bot with command handling and notifications.

    Commands:
        /start - Welcome message
        /help - List of commands
        /deposit - Add $1000 virtual deposit
        /profile - View deposit balance and profit
    """

    def __init__(
        self,
        token: str,
        chat_id: str | int,
        message_queue: asyncio.Queue,
        session_factory: sessionmaker,
        user_model,
        *,
        get_bot_balance_fn: Optional[Callable[[], Awaitable[float]]] = None,
        poll_interval: float = 1.0,
    ):
        """
        Initialize the Telegram bot.

        Args:
            token: Telegram bot token
            chat_id: Default chat ID for notifications
            message_queue: Queue for outgoing notifications
            session_factory: SQLAlchemy async session factory
            user_model: User SQLAlchemy model
            get_bot_balance_fn: Async function to get bot's total USDT balance
            poll_interval: Interval between message sends (rate limiting)
        """
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode="HTML"),
        )
        self.chat_id = chat_id
        self.queue = message_queue
        self.poll_interval = poll_interval
        self._session_factory = session_factory
        self._user_model = user_model
        self._get_bot_balance = get_bot_balance_fn

        # Aiogram dispatcher and router for commands
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)

        # Register command handlers
        self._register_handlers()

        # Tasks
        self._consumer_task: Optional[asyncio.Task] = None
        self._polling_task: Optional[asyncio.Task] = None

        self.logger = logger.bind(component="TelegramBot")

    def _register_handlers(self) -> None:
        """Register command handlers."""

        @self.router.message(Command("start"))
        async def cmd_start(message: Message) -> None:
            """Handle /start command."""
            welcome_text = (
                "<b>🤖 Добро пожаловать в Торгового Бота!</b>\n\n"
                "Этот бот выполняет алгоритмическую торговлю на бирже Bybit.\n\n"
                "Используйте /help чтобы увидеть доступные команды.\n\n"
                "<i>Примечание: Это демо торговый бот для образовательных целей.</i>"
            )
            await message.answer(welcome_text)
            self.logger.info("User {} started the bot", message.from_user.id)

        @self.router.message(Command("help"))
        async def cmd_help(message: Message) -> None:
            """Handle /help command."""
            help_text = (
                "<b>📋 Доступные команды:</b>\n\n"
                "/start - Приветствие и информация о боте\n"
                "/help - Показать это сообщение помощи\n"
                "/deposit - Добавить $1,000 виртуального депозита\n"
                "/profile - Посмотреть ваш депозит и прибыль\n\n"
                "<b>Как это работает:</b>\n"
                "При депозите вы получаете виртуальный баланс. "
                "Когда бот закрывает сделки, ваш депозит обновляется "
                "в соответствии с процентом прибыли или убытка."
            )
            await message.answer(help_text)

        @self.router.message(Command("deposit"))
        async def cmd_deposit(message: Message) -> None:
            """Handle /deposit command - add $1000 to user's virtual deposit."""
            telegram_id = message.from_user.id
            username = message.from_user.username or message.from_user.first_name

            try:
                # Get current bot balance to check if deposit is possible
                bot_balance = await self._get_current_bot_balance()
                if bot_balance <= 0:
                    await message.answer(
                        "⚠️ Невозможно обработать депозит: Баланс бота недоступен.\n"
                        "Пожалуйста, попробуйте позже."
                    )
                    return

                deposit_amount = 1000.0

                # Check if deposit would exceed bot balance
                if deposit_amount > bot_balance:
                    await message.answer(
                        f"⚠️ Сумма депозита ${deposit_amount:.2f} превышает "
                        f"доступный баланс бота ${bot_balance:.2f}.\n"
                        "Пожалуйста, попробуйте меньшую сумму."
                    )
                    return

                async with self._session_factory() as session:
                    # Get or create user
                    result = await session.execute(
                        select(self._user_model).where(
                            self._user_model.telegram_id == telegram_id
                        )
                    )
                    user = result.scalar_one_or_none()

                    if user is None:
                        # Create new user
                        user = self._user_model(
                            telegram_id=telegram_id,
                            username=username,
                            deposit=0.0,
                            total_profit=0.0,
                        )
                        session.add(user)

                    # Add deposit to user's balance
                    user.deposit += deposit_amount
                    user.last_active = datetime.utcnow()
                    if username:
                        user.username = username

                    await session.commit()

                    new_total = user.deposit

                await message.answer(
                    f"<b>✅ Депозит успешно внесён!</b>\n\n"
                    f"Сумма депозита: <b>${deposit_amount:.2f}</b>\n"
                    f"Ваш общий депозит: <b>${new_total:.2f}</b>\n\n"
                    f"<i>Ваш депозит будет расти/уменьшаться в зависимости от результатов торговли.</i>"
                )
                self.logger.info(
                    "User {} deposited ${:.2f}, total: ${:.2f}",
                    telegram_id, deposit_amount, new_total
                )

            except Exception as e:
                self.logger.exception("Error processing deposit for user {}: {}", telegram_id, e)
                await message.answer(
                    "❌ Произошла ошибка при обработке вашего депозита.\n"
                    "Пожалуйста, попробуйте позже."
                )

        @self.router.message(Command("profile"))
        async def cmd_profile(message: Message) -> None:
            """Handle /profile command - show user's deposit balance and profit."""
            telegram_id = message.from_user.id

            try:
                async with self._session_factory() as session:
                    result = await session.execute(
                        select(self._user_model).where(
                            self._user_model.telegram_id == telegram_id
                        )
                    )
                    user = result.scalar_one_or_none()

                    if user is None or user.deposit == 0:
                        await message.answer(
                            "<b>👤 Ваш профиль</b>\n\n"
                            "Вы ещё не сделали ни одного депозита.\n"
                            "Используйте /deposit чтобы добавить $1,000 на ваш счёт."
                        )
                        return

                    # Format profit with sign
                    profit_sign = "+" if user.total_profit >= 0 else ""
                    profit_emoji = "📈" if user.total_profit >= 0 else "📉"

                    profile_text = (
                        f"<b>👤 Ваш профиль</b>\n\n"
                        f"💰 Текущий баланс: <b>${user.deposit:.2f}</b>\n"
                        f"{profit_emoji} Общая прибыль: <b>{profit_sign}{user.total_profit:.2f}%</b>\n\n"
                        f"<i>Обновлено: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</i>"
                    )
                    await message.answer(profile_text)

                    # Update last_active
                    user.last_active = datetime.utcnow()
                    await session.commit()

            except Exception as e:
                self.logger.exception("Error fetching profile for user {}: {}", telegram_id, e)
                await message.answer(
                    "❌ Произошла ошибка при получении вашего профиля.\n"
                    "Пожалуйста, попробуйте позже."
                )

    async def _get_current_bot_balance(self) -> float:
        """Get current bot USDT balance."""
        if self._get_bot_balance:
            try:
                return await self._get_bot_balance()
            except Exception as e:
                self.logger.error("Error getting bot balance: {}", e)
        return 0.0

    async def start(self) -> None:
        """Start the bot (notification consumer + command polling)."""
        if self._consumer_task is None:
            self._consumer_task = asyncio.create_task(
                self._message_consumer(),
                name="telegram-consumer"
            )

        if self._polling_task is None:
            self._polling_task = asyncio.create_task(
                self._start_polling(),
                name="telegram-polling"
            )

        self.logger.info("TelegramBot started")

    async def _start_polling(self) -> None:
        """Start polling for commands."""
        try:
            await self.dp.start_polling(self.bot)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error("Polling error: {}", e)

    async def _message_consumer(self) -> None:
        """Consume and send notification messages from queue."""
        while True:
            try:
                msg = await self.queue.get()
                try:
                    if isinstance(msg, dict):
                        text = msg.get("text")
                        parse_mode = msg.get("parse_mode", "HTML")
                        chat_id = msg.get("chat_id", self.chat_id)
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=parse_mode
                        )
                    else:
                        await self.bot.send_message(
                            chat_id=self.chat_id,
                            text=str(msg)
                        )
                    self.logger.debug("Sent notification: {}", msg)
                except TelegramAPIError as e:
                    self.logger.error("Telegram API error: {}", e)
                finally:
                    self.queue.task_done()

                await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Consumer error: {}", e)
                await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the bot gracefully."""
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        await self.dp.stop_polling()
        await self.bot.session.close()
        self.logger.info("TelegramBot stopped")

    async def update_user_deposits_on_trade(
        self,
        session: AsyncSession,
        profit_pct: float
    ) -> None:
        """
        Update all user deposits based on trade profit percentage.
        Call this after a successful token sale.

        Args:
            session: Database session
            profit_pct: Profit percentage from the trade (e.g., 20.0 for 20% profit)

        Example:
            If profit_pct = 20.0 and user has $1000 deposit:
            - New deposit = $1000 * (1 + 20/100) = $1200
            - total_profit is incremented by 20.0
        """
        try:
            # Get all users with deposits
            result = await session.execute(
                select(self._user_model).where(
                    self._user_model.deposit > 0
                )
            )
            users = result.scalars().all()

            if not users:
                return

            for user in users:
                # Apply profit/loss percentage to deposit
                multiplier = 1 + (profit_pct / 100.0)
                user.deposit = user.deposit * multiplier

                # Accumulate total profit percentage
                # Note: This is additive for simplicity. For compound tracking,
                # you would need: ((1 + old/100) * (1 + new/100) - 1) * 100
                user.total_profit += profit_pct

            await session.commit()
            self.logger.info(
                "Updated {} user deposits with {:.2f}% profit",
                len(users), profit_pct
            )

        except Exception as e:
            self.logger.exception("Error updating user deposits: {}", e)