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
from sqlalchemy import select, func as sql_func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker


class TelegramBot:
    """
    Full-featured Telegram bot with command handling and notifications.

    Commands:
        /start - Welcome message
        /help - List of commands
        /deposit - Add $1000 virtual deposit
        /profile - View deposit balance
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
                "<b>Welcome to the Trading Bot!</b>\n\n"
                "This bot performs algorithmic trading on Bybit exchange.\n\n"
                "Use /help to see available commands.\n\n"
                "<i>Note: This is a demo trading bot for educational purposes.</i>"
            )
            await message.answer(welcome_text)
            self.logger.info("User {} started the bot", message.from_user.id)

        @self.router.message(Command("help"))
        async def cmd_help(message: Message) -> None:
            """Handle /help command."""
            help_text = (
                "<b>Available Commands:</b>\n\n"
                "/start - Welcome message and bot info\n"
                "/help - Show this help message\n"
                "/deposit - Add $1,000 virtual deposit to your account\n"
                "/profile - View your current deposit balance\n\n"
                "<b>How deposits work:</b>\n"
                "When you deposit, your percentage of the total bot balance is recorded. "
                "As the bot trades and the balance grows, your deposit grows proportionally."
            )
            await message.answer(help_text)

        @self.router.message(Command("deposit"))
        async def cmd_deposit(message: Message) -> None:
            """Handle /deposit command - add $1000 to user's virtual deposit."""
            telegram_id = message.from_user.id
            username = message.from_user.username or message.from_user.first_name

            try:
                # Get current bot balance
                bot_balance = await self._get_current_bot_balance()
                if bot_balance <= 0:
                    await message.answer(
                        "Unable to process deposit: Bot balance unavailable.\n"
                        "Please try again later."
                    )
                    return

                deposit_amount = 1000.0

                # Check if deposit would exceed bot balance
                if deposit_amount > bot_balance:
                    await message.answer(
                        f"Deposit amount ${deposit_amount:.2f} exceeds "
                        f"available bot balance ${bot_balance:.2f}.\n"
                        "Please try a smaller amount."
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
                            deposit_percentage=0.0,
                        )
                        session.add(user)

                    # Calculate current deposit value based on percentage
                    current_value = user.deposit_percentage * bot_balance / 100.0

                    # Add new deposit
                    new_total = current_value + deposit_amount
                    new_percentage = (new_total / bot_balance) * 100.0

                    # Update user
                    user.deposit = new_total
                    user.deposit_percentage = new_percentage
                    user.last_active = datetime.utcnow()
                    if username:
                        user.username = username

                    await session.commit()

                await message.answer(
                    f"<b>Deposit Successful!</b>\n\n"
                    f"Amount deposited: <b>${deposit_amount:.2f}</b>\n"
                    f"Your total deposit: <b>${new_total:.2f}</b>\n"
                    f"Your share: <b>{new_percentage:.4f}%</b> of total bot balance\n\n"
                    f"<i>Your deposit will grow proportionally as the bot trades.</i>"
                )
                self.logger.info(
                    "User {} deposited ${:.2f}, total: ${:.2f}, share: {:.4f}%",
                    telegram_id, deposit_amount, new_total, new_percentage
                )

            except Exception as e:
                self.logger.exception("Error processing deposit for user {}: {}", telegram_id, e)
                await message.answer(
                    "An error occurred while processing your deposit.\n"
                    "Please try again later."
                )

        @self.router.message(Command("profile"))
        async def cmd_profile(message: Message) -> None:
            """Handle /profile command - show user's deposit balance."""
            telegram_id = message.from_user.id

            try:
                # Get current bot balance
                bot_balance = await self._get_current_bot_balance()

                async with self._session_factory() as session:
                    result = await session.execute(
                        select(self._user_model).where(
                            self._user_model.telegram_id == telegram_id
                        )
                    )
                    user = result.scalar_one_or_none()

                    if user is None or user.deposit_percentage == 0:
                        await message.answer(
                            "<b>Your Profile</b>\n\n"
                            "You haven't made any deposits yet.\n"
                            "Use /deposit to add $1,000 to your account."
                        )
                        return

                    # Calculate current value based on percentage
                    if bot_balance > 0:
                        current_value = user.deposit_percentage * bot_balance / 100.0
                    else:
                        current_value = user.deposit  # Fallback to stored value

                    # Calculate profit/loss
                    initial_deposit = user.deposit  # This is updated on each deposit
                    # For accurate P&L, we'd need to track initial deposit separately
                    # For now, show current value

                    profile_text = (
                        f"<b>Your Profile</b>\n\n"
                        f"Current Balance: <b>${current_value:.2f}</b>\n"
                        f"Your Share: <b>{user.deposit_percentage:.4f}%</b>\n"
                        f"Bot Total Balance: <b>${bot_balance:.2f}</b>\n\n"
                        f"<i>Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</i>"
                    )
                    await message.answer(profile_text)

                    # Update last_active
                    user.last_active = datetime.utcnow()
                    await session.commit()

            except Exception as e:
                self.logger.exception("Error fetching profile for user {}: {}", telegram_id, e)
                await message.answer(
                    "An error occurred while fetching your profile.\n"
                    "Please try again later."
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

    async def update_user_deposits_on_sale(self, session: AsyncSession) -> None:
        """
        Update all user deposits based on current bot balance.
        Call this after a successful token sale.

        This recalculates each user's deposit value based on their
        percentage share of the total bot balance.
        """
        try:
            bot_balance = await self._get_current_bot_balance()
            if bot_balance <= 0:
                return

            # Get all users with deposits
            result = await session.execute(
                select(self._user_model).where(
                    self._user_model.deposit_percentage > 0
                )
            )
            users = result.scalars().all()

            for user in users:
                # Update deposit value based on current percentage
                user.deposit = user.deposit_percentage * bot_balance / 100.0

            await session.commit()
            self.logger.info(
                "Updated {} user deposits based on bot balance ${:.2f}",
                len(users), bot_balance
            )

        except Exception as e:
            self.logger.exception("Error updating user deposits: {}", e)