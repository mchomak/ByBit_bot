"""TelegramBot: Full-featured Telegram bot with commands and notifications."""

import asyncio
import math
from datetime import datetime
from typing import Optional, Callable, Awaitable, Dict, Any

from aiogram import Bot, Dispatcher, Router
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker


def format_price(price: float, min_significant: int = 3) -> str:
    """
    Format price with enough decimal places to show significant digits.
    """
    if price == 0:
        return "0.000000"

    if price >= 0.000001:
        formatted = f"{price:.6f}"
        if float(formatted) != 0:
            return formatted

    abs_price = abs(price)
    if abs_price >= 1:
        return f"{price:.6f}"

    decimals_needed = int(-math.log10(abs_price)) + min_significant
    decimals_needed = max(6, min(decimals_needed, 15))
    return f"{price:.{decimals_needed}f}"


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
                # Get current bot total portfolio value (USDT + tokens)
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

                # Calculate user's share of the portfolio
                share_pct = (deposit_amount / bot_balance) * 100

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
                            share_pct=0.0,
                            total_profit_usdt=0.0,
                        )
                        session.add(user)

                    # Add deposit and share to user's balance
                    user.deposit += deposit_amount
                    user.share_pct += share_pct  # Accumulate share
                    user.last_active = datetime.utcnow()
                    if username:
                        user.username = username

                    await session.commit()

                    new_total = user.deposit
                    total_share = user.share_pct

                await message.answer(
                    f"<b>✅ Депозит успешно внесён!</b>\n\n"
                    f"Сумма депозита: <b>${deposit_amount:.2f}</b>\n"
                    f"Ваш общий депозит: <b>${new_total:.2f}</b>\n"
                    f"<i>Прибыль от сделок будет начисляться пропорционально вашей доле.</i>"
                )
                self.logger.info(
                    "User {} deposited ${:.2f}, total: ${:.2f}, share: {:.2f}%",
                    telegram_id, deposit_amount, new_total, total_share
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

                    # Get profit values (handle old records without new fields)
                    total_profit_usdt = getattr(user, 'total_profit_usdt', 0.0) or 0.0
                    share_pct = getattr(user, 'share_pct', 0.0) or 0.0

                    # Format profit with sign
                    profit_sign = "+" if total_profit_usdt >= 0 else ""
                    profit_emoji = "📈" if total_profit_usdt >= 0 else "📉"

                    profile_text = (
                        f"<b>👤 Ваш профиль</b>\n\n"
                        f"💰 Текущий баланс: <b>${user.deposit:.2f}</b>\n"
                        f"{profit_emoji} Общая прибыль: <b>{profit_sign}${total_profit_usdt:.2f}</b>\n\n"
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
                        broadcast = msg.get("broadcast", False)
                        trade_data = msg.get("trade_data")

                        # Send to main chat (full position info)
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=parse_mode
                        )

                        # Broadcast personalized messages to users
                        if broadcast and trade_data:
                            await self._broadcast_personalized(trade_data, parse_mode)
                        elif broadcast:
                            await self._broadcast_to_users(text, parse_mode)
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

    async def _broadcast_to_users(self, text: str, parse_mode: str = "HTML") -> None:
        """
        Send a message to all users with deposits.

        Args:
            text: Message text to send
            parse_mode: Telegram parse mode (HTML, Markdown, etc.)
        """
        try:
            async with self._session_factory() as session:
                # Get all users with deposits (active investors)
                result = await session.execute(
                    select(self._user_model).where(
                        self._user_model.deposit > 0
                    )
                )
                users = result.scalars().all()

                if not users:
                    return

                sent_count = 0
                failed_count = 0

                for user in users:
                    try:
                        # Don't send to main chat again (already sent)
                        if str(user.telegram_id) == str(self.chat_id):
                            continue

                        await self.bot.send_message(
                            chat_id=user.telegram_id,
                            text=text,
                            parse_mode=parse_mode
                        )
                        sent_count += 1

                        # Small delay to avoid rate limiting
                        await asyncio.sleep(0.1)

                    except TelegramAPIError as e:
                        # User may have blocked the bot or deleted account
                        self.logger.warning(
                            "Failed to send to user {}: {}",
                            user.telegram_id, e
                        )
                        failed_count += 1

                if sent_count > 0 or failed_count > 0:
                    self.logger.info(
                        "Broadcast complete: sent={}, failed={}",
                        sent_count, failed_count
                    )

        except Exception as e:
            self.logger.error("Broadcast error: {}", e)

    async def _broadcast_personalized(
        self,
        trade_data: Dict[str, Any],
        parse_mode: str = "HTML"
    ) -> None:
        """
        Send personalized trade notifications to users based on their share_pct.

        Each user receives a message with their portion of the position size,
        calculated as: user_size = total_size * (share_pct / 100)

        Args:
            trade_data: Dict containing trade info:
                - type: "entry" or "exit"
                - coin: Token symbol (e.g., "BTC")
                - price: Execution price
                - quantity: Total quantity traded
                - position_size_usdt: Total position size in USDT
                - pnl_pct: Profit/loss percentage (for exit)
                - orders_count: Number of orders executed
                - time: Execution time string
                - is_retry: Whether this was a retry (optional)
            parse_mode: Telegram parse mode
        """
        try:
            async with self._session_factory() as session:
                # Get all users with deposits (active investors)
                result = await session.execute(
                    select(self._user_model).where(
                        self._user_model.deposit > 0
                    )
                )
                users = result.scalars().all()

                if not users:
                    return

                sent_count = 0
                failed_count = 0

                trade_type = trade_data.get("type", "entry")
                coin = trade_data.get("coin", "")
                price = trade_data.get("price", 0)
                total_position_usdt = trade_data.get("position_size_usdt", 0)
                pnl_pct = trade_data.get("pnl_pct", 0)
                trade_time = trade_data.get("time", "")
                is_retry = trade_data.get("is_retry", False)

                price_formatted = format_price(price)
                retry_suffix = " (после повтора)" if is_retry else ""

                for user in users:
                    try:
                        # Don't send to main chat again (already sent)
                        if str(user.telegram_id) == str(self.chat_id):
                            continue

                        # Calculate user's portion based on share_pct
                        share_pct = getattr(user, 'share_pct', 0.0) or 0.0
                        if share_pct <= 0:
                            continue

                        user_position_usdt = total_position_usdt * (share_pct / 100.0)

                        # Generate personalized message
                        if trade_type == "entry":
                            msg = (
                                f"🟢 <b>Вход в позицию</b>{retry_suffix}\n\n"
                                f"Монета: <b>{coin}</b>\n"
                                f"Цена: {price_formatted}\n"
                                f"Ваш размер: <b>${user_position_usdt:.2f}</b>\n"
                                f"Время: {trade_time}"
                            )
                        else:
                            profit_sign = "+" if pnl_pct >= 0 else ""
                            user_pnl_usdt = user_position_usdt * (pnl_pct / 100.0) if pnl_pct else 0
                            pnl_sign_usdt = "+" if user_pnl_usdt >= 0 else ""
                            msg = (
                                f"🔴 <b>Выход из позиции</b>{retry_suffix}\n\n"
                                f"Монета: <b>{coin}</b>\n"
                                f"Цена: {price_formatted}\n"
                                f"Ваша сумма: <b>${user_position_usdt:.2f}</b>\n"
                                f"Ваш результат: <b>{profit_sign}{pnl_pct:.1f}%</b> ({pnl_sign_usdt}${abs(user_pnl_usdt):.2f})\n"
                                f"Время: {trade_time}"
                            )

                        await self.bot.send_message(
                            chat_id=user.telegram_id,
                            text=msg,
                            parse_mode=parse_mode
                        )
                        sent_count += 1

                        # Small delay to avoid rate limiting
                        await asyncio.sleep(0.1)

                    except TelegramAPIError as e:
                        self.logger.warning(
                            "Failed to send personalized msg to user {}: {}",
                            user.telegram_id, e
                        )
                        failed_count += 1

                if sent_count > 0 or failed_count > 0:
                    self.logger.info(
                        "Personalized broadcast complete: sent={}, failed={}",
                        sent_count, failed_count
                    )

        except Exception as e:
            self.logger.error("Personalized broadcast error: {}", e)

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
        profit_usdt: float,
        profit_pct: float = 0.0,
    ) -> None:
        """
        Update all user deposits based on trade profit.
        Distributes profit proportionally based on each user's share_pct.

        Args:
            session: Database session
            profit_usdt: Profit amount in USDT from the trade (e.g., 500.0)
            profit_pct: Profit percentage (for logging, optional)

        Example:
            Bot makes trade with $500 profit.
            User has 2% share (share_pct = 2.0):
            - User gets: $500 * 0.02 = $10
            - user.deposit += $10
            - user.total_profit_usdt += $10
        """
        try:
            # Get all users with shares
            result = await session.execute(
                select(self._user_model).where(
                    self._user_model.share_pct > 0
                )
            )
            users = result.scalars().all()

            if not users:
                return

            total_distributed = 0.0
            for user in users:
                # Calculate user's share of profit
                share_pct = getattr(user, 'share_pct', 0.0) or 0.0
                user_profit = profit_usdt * (share_pct / 100.0)

                # Add profit to deposit
                user.deposit += user_profit

                # Track total profit in USDT
                if hasattr(user, 'total_profit_usdt'):
                    user.total_profit_usdt = (user.total_profit_usdt or 0.0) + user_profit

                total_distributed += user_profit

            await session.commit()
            self.logger.info(
                "Distributed ${:.2f} profit to {} users (total trade profit: ${:.2f}, {:.2f}%)",
                total_distributed, len(users), profit_usdt, profit_pct
            )

        except Exception as e:
            self.logger.exception("Error updating user deposits: {}", e)