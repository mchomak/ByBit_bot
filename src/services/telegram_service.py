"""TelegramService: Bot interface for notifications and commands.

Responsibilities:
1. Send notifications from NotificationQueue (signals, trades, errors)
2. Handle user commands:
   - /start - Register user, show welcome
   - /stop - Pause trading (admin only)
   - /status - Show bot status and statistics
   - /setlimit <n> - Set max positions limit
   - /positions - Show open positions
   - /stats - Show trading statistics
   - /help - Show available commands
3. Update user settings in database
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.client.bot import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command
from loguru import logger

from ..config.settings import Settings
from ..db.repository import Repository
from ..db.models import User, Position, PositionStatus
from ..queues.event_bus import NotificationQueue, NotificationEvent


class TelegramService:
    """
    Telegram bot service for notifications and user interaction.

    Sends notifications and handles user commands.
    """

    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        notification_queue: NotificationQueue,
        get_status_callback: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        """
        Initialize TelegramService.

        Args:
            settings: Application settings
            repository: Database repository
            notification_queue: Queue to receive notifications from
            get_status_callback: Callback to get current bot status
        """
        self.settings = settings
        self.repository = repository
        self.queue = notification_queue
        self.get_status = get_status_callback
        self.log = logger.bind(component="TelegramService")

        # Initialize aiogram bot
        self.bot = Bot(
            token=settings.telegram_bot_token,
            default=DefaultBotProperties(parse_mode="HTML"),
        )
        self.dp = Dispatcher()

        # Default chat ID for notifications
        self.default_chat_id = settings.telegram_chat_id

        # Background tasks
        self._notification_consumer_task: Optional[asyncio.Task] = None
        self._polling_task: Optional[asyncio.Task] = None
        self._running = False

        # Register handlers
        self._register_handlers()

    def _register_handlers(self) -> None:
        """Register command handlers."""

        @self.dp.message(Command("start"))
        async def cmd_start(message: types.Message):
            await self._handle_start(message)

        @self.dp.message(Command("stop"))
        async def cmd_stop(message: types.Message):
            await self._handle_stop(message)

        @self.dp.message(Command("status"))
        async def cmd_status(message: types.Message):
            await self._handle_status(message)

        @self.dp.message(Command("setlimit"))
        async def cmd_setlimit(message: types.Message):
            await self._handle_setlimit(message)

        @self.dp.message(Command("positions"))
        async def cmd_positions(message: types.Message):
            await self._handle_positions(message)

        @self.dp.message(Command("stats"))
        async def cmd_stats(message: types.Message):
            await self._handle_stats(message)

        @self.dp.message(Command("help"))
        async def cmd_help(message: types.Message):
            await self._handle_help(message)

    async def start(self) -> None:
        """Start the Telegram service."""
        self.log.info("Starting TelegramService")
        self._running = True

        # Start notification consumer
        self._notification_consumer_task = asyncio.create_task(
            self._consume_notifications()
        )

        # Start bot polling in background
        self._polling_task = asyncio.create_task(self._start_polling())

        self.log.info("TelegramService started")

    async def stop(self) -> None:
        """Stop the Telegram service."""
        self._running = False

        if self._notification_consumer_task:
            self._notification_consumer_task.cancel()
            try:
                await self._notification_consumer_task
            except asyncio.CancelledError:
                pass

        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

        await self.bot.session.close()
        self.log.info("TelegramService stopped")

    async def _start_polling(self) -> None:
        """Start bot polling for commands."""
        try:
            await self.dp.start_polling(self.bot)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"Polling error: {e}")

    # =========================================================================
    # Notification Consumer
    # =========================================================================

    async def _consume_notifications(self) -> None:
        """Consume and send notifications from queue."""
        self.log.info("Starting notification consumer")

        while self._running:
            try:
                event: NotificationEvent = await self.queue.get()

                try:
                    await self._send_notification(event)
                except Exception as e:
                    self.log.error(f"Error sending notification: {e}")
                finally:
                    self.queue.task_done()

            except asyncio.CancelledError:
                break

    async def _send_notification(self, event: NotificationEvent) -> None:
        """
        Send a notification to Telegram.

        Args:
            event: Notification event to send
        """
        try:
            await self.bot.send_message(
                chat_id=self.default_chat_id,
                text=event.message,
                parse_mode=event.parse_mode,
            )
            self.log.debug(f"Sent notification: {event.message[:50]}...")

        except TelegramAPIError as e:
            self.log.error(f"Telegram API error: {e}")

    async def send_message(
        self,
        message: str,
        chat_id: Optional[str] = None,
        parse_mode: str = "HTML",
    ) -> None:
        """
        Send a message directly (not via queue).

        Args:
            message: Message text
            chat_id: Target chat ID (default: default_chat_id)
            parse_mode: Parse mode (HTML/Markdown)
        """
        try:
            await self.bot.send_message(
                chat_id=chat_id or self.default_chat_id,
                text=message,
                parse_mode=parse_mode,
            )
        except TelegramAPIError as e:
            self.log.error(f"Failed to send message: {e}")

    # =========================================================================
    # Command Handlers
    # =========================================================================

    async def _handle_start(self, message: types.Message) -> None:
        """
        Handle /start command - Register user and show welcome.

        Args:
            message: Telegram message object
        """
        user_id = message.from_user.id
        username = message.from_user.username

        # Check if user exists
        existing = await self.repository.get_by_field(User, "telegram_id", user_id)

        if not existing:
            # Create new user
            await self.repository.insert(
                User,
                {
                    "telegram_id": user_id,
                    "username": username,
                    "is_admin": False,
                    "limit_positions": self.settings.max_positions,
                },
            )
            self.log.info(f"New user registered: {user_id} ({username})")

        welcome_text = (
            "<b>Welcome to Bybit Trading Bot!</b>\n\n"
            "I monitor 300+ coins and trade automatically based on:\n"
            "- Volume spikes (abnormal trading activity)\n"
            "- Price acceleration (momentum)\n"
            "- MA14 exit signals\n\n"
            "Use /help to see available commands."
        )

        await message.answer(welcome_text)

    async def _handle_stop(self, message: types.Message) -> None:
        """
        Handle /stop command - Admin only, pause trading.

        Args:
            message: Telegram message object
        """
        user_id = message.from_user.id

        # Check if admin
        user = await self.repository.get_by_field(User, "telegram_id", user_id)
        if not user or not user.is_admin:
            await message.answer("This command is admin-only.")
            return

        # TODO: Implement trading pause functionality
        await message.answer("Trading pause not yet implemented.")

    async def _handle_status(self, message: types.Message) -> None:
        """
        Handle /status command - Show bot status.

        Args:
            message: Telegram message object
        """
        # Get status from callback if available
        status_data = {}
        if self.get_status:
            try:
                status_data = self.get_status()
            except Exception as e:
                self.log.error(f"Error getting status: {e}")

        # Get position counts
        open_positions = await self.repository.count(
            Position, filters={"status": PositionStatus.OPEN}
        )

        status_text = (
            "<b>Bot Status</b>\n"
            f"Active symbols: {status_data.get('active_symbols', 'N/A')}\n"
            f"Open positions: {open_positions}\n"
            f"Max positions: {self.settings.max_positions}\n"
            f"WebSocket: {status_data.get('ws_connected', 'N/A')}\n"
            f"Queue sizes:\n"
            f"  - Market data: {status_data.get('market_queue_size', 'N/A')}\n"
            f"  - Trade signals: {status_data.get('signal_queue_size', 'N/A')}\n"
            f"  - Notifications: {status_data.get('notification_queue_size', 'N/A')}"
        )

        await message.answer(status_text)

    async def _handle_setlimit(self, message: types.Message) -> None:
        """
        Handle /setlimit <n> command - Set max positions.

        Args:
            message: Telegram message object
        """
        user_id = message.from_user.id

        # Parse argument
        args = message.text.split()
        if len(args) != 2:
            await message.answer("Usage: /setlimit <number>")
            return

        try:
            new_limit = int(args[1])
            if new_limit < 1 or new_limit > 50:
                raise ValueError("Limit must be between 1 and 50")
        except ValueError as e:
            await message.answer(f"Invalid limit: {e}")
            return

        # Update user setting
        await self.repository.update(
            User,
            filter_by={"telegram_id": user_id},
            update_values={"limit_positions": new_limit},
        )

        await message.answer(f"Position limit set to {new_limit}")
        self.log.info(f"User {user_id} set limit to {new_limit}")

    async def _handle_positions(self, message: types.Message) -> None:
        """
        Handle /positions command - Show open positions.

        Args:
            message: Telegram message object
        """
        positions = await self.repository.get_all(
            Position, filters={"status": PositionStatus.OPEN}
        )

        if not positions:
            await message.answer("No open positions.")
            return

        text = "<b>Open Positions</b>\n\n"
        for pos in positions:
            # Calculate unrealized P&L (would need current price)
            text += (
                f"<b>{pos.symbol}</b>\n"
                f"  Entry: {pos.entry_price:.8f}\n"
                f"  Amount: {pos.entry_amount:.6f}\n"
                f"  Value: {pos.entry_value_usdt:.2f} USDT\n"
                f"  Since: {pos.entry_time.strftime('%Y-%m-%d %H:%M')}\n\n"
            )

        await message.answer(text)

    async def _handle_stats(self, message: types.Message) -> None:
        """
        Handle /stats command - Show trading statistics.

        Args:
            message: Telegram message object
        """
        # Get closed positions for stats
        closed_positions = await self.repository.get_all(
            Position, filters={"status": PositionStatus.CLOSED}
        )

        if not closed_positions:
            await message.answer("No completed trades yet.")
            return

        # Calculate statistics
        total_trades = len(closed_positions)
        winning_trades = sum(1 for p in closed_positions if p.profit_usdt > 0)
        total_profit = sum(p.profit_usdt or 0 for p in closed_positions)
        avg_profit_pct = sum(p.profit_pct or 0 for p in closed_positions) / total_trades

        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0

        text = (
            "<b>Trading Statistics</b>\n\n"
            f"Total trades: {total_trades}\n"
            f"Winning trades: {winning_trades}\n"
            f"Win rate: {win_rate:.1f}%\n"
            f"Total P&L: {total_profit:.2f} USDT\n"
            f"Avg P&L: {avg_profit_pct:.2f}%"
        )

        await message.answer(text)

    async def _handle_help(self, message: types.Message) -> None:
        """
        Handle /help command - Show available commands.

        Args:
            message: Telegram message object
        """
        help_text = (
            "<b>Available Commands</b>\n\n"
            "/start - Register and get started\n"
            "/status - Show bot status\n"
            "/positions - List open positions\n"
            "/stats - Trading statistics\n"
            "/setlimit <n> - Set max positions (1-50)\n"
            "/help - Show this message\n\n"
            "<i>Admin commands:</i>\n"
            "/stop - Pause trading"
        )

        await message.answer(help_text)

    # =========================================================================
    # User Management
    # =========================================================================

    async def get_user(self, telegram_id: int) -> Optional[User]:
        """Get user by Telegram ID."""
        return await self.repository.get_by_field(User, "telegram_id", telegram_id)

    async def update_user_activity(self, telegram_id: int) -> None:
        """Update user's last active timestamp."""
        await self.repository.update(
            User,
            filter_by={"telegram_id": telegram_id},
            update_values={"last_active": datetime.now(timezone.utc)},
        )
