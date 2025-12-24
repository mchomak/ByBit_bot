"""Daily trading report generator.

Generates and sends daily trading statistics via Telegram at 00:00.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Optional

from loguru import logger
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.models import Order, Position, Token, OrderSide, OrderStatus, PositionStatus


class DailyReportService:
    """
    Service for generating and sending daily trading reports.

    Runs at 00:00 UTC and sends statistics to Telegram.
    """

    def __init__(
        self,
        session_factory: sessionmaker,
        telegram_queue: asyncio.Queue,
        timezone_str: str = "UTC",
    ):
        """
        Initialize the daily report service.

        Args:
            session_factory: SQLAlchemy async session factory
            telegram_queue: Queue for sending Telegram messages
            timezone_str: Timezone string (default: UTC)
        """
        self._session_factory = session_factory
        self._telegram_queue = telegram_queue
        self._timezone_str = timezone_str
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._log = logger.bind(component="DailyReport")

    async def start(self) -> None:
        """Start the daily report scheduler."""
        if self._task is not None:
            self._log.warning("Daily report service already running")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._scheduler_loop(),
            name="daily-report-scheduler"
        )
        self._log.info("Daily report scheduler started (runs at 00:00 UTC)")

    async def stop(self) -> None:
        """Stop the daily report scheduler."""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._log.info("Daily report scheduler stopped")

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop - runs report at 00:00 UTC daily."""
        while not self._stop_event.is_set():
            try:
                # Calculate time until next 00:00 UTC
                now = datetime.now(timezone.utc)
                tomorrow = now.date() + timedelta(days=1)
                next_midnight = datetime.combine(tomorrow, time.min, tzinfo=timezone.utc)
                wait_seconds = (next_midnight - now).total_seconds()

                self._log.debug(
                    "Next daily report in {:.1f} hours",
                    wait_seconds / 3600
                )

                # Wait until midnight (or stop event)
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=wait_seconds
                    )
                    # Stop event was set
                    break
                except asyncio.TimeoutError:
                    # Time to run the report
                    pass

                # Generate and send report
                await self.generate_and_send_report()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.exception("Error in daily report scheduler: {}", e)
                await asyncio.sleep(60)  # Wait before retry

    async def generate_and_send_report(self, report_date: datetime = None) -> None:
        """
        Generate and send the daily report.

        Args:
            report_date: Date to report on (default: yesterday)
        """
        try:
            # Default to yesterday
            if report_date is None:
                report_date = datetime.now(timezone.utc).date() - timedelta(days=1)

            start_dt = datetime.combine(report_date, time.min, tzinfo=timezone.utc)
            end_dt = start_dt + timedelta(days=1)

            self._log.info("Generating daily report for {}", report_date)

            async with self._session_factory() as session:
                # 1. Get order statistics
                order_stats = await self._get_order_stats(session, start_dt, end_dt)

                # 2. Get position/profit statistics
                profit_stats = await self._get_profit_stats(session, start_dt, end_dt)

                # 3. Get active tokens count
                active_tokens = await self._get_active_tokens_count(session)

            # Format and send the report
            report_msg = self._format_report(
                report_date,
                order_stats,
                profit_stats,
                active_tokens
            )

            self._telegram_queue.put_nowait({
                "text": report_msg,
                "parse_mode": "HTML"
            })

            self._log.info("Daily report sent successfully")

        except Exception as e:
            self._log.exception("Failed to generate daily report: {}", e)
            # Send error notification
            try:
                self._telegram_queue.put_nowait({
                    "text": f"❌ Failed to generate daily report: {e}",
                    "parse_mode": "HTML"
                })
            except:
                pass

    async def _get_order_stats(
        self,
        session: AsyncSession,
        start_dt: datetime,
        end_dt: datetime
    ) -> dict:
        """Get order statistics for the period."""
        # Total orders
        total_result = await session.execute(
            select(func.count(Order.id)).where(
                and_(
                    Order.created_at >= start_dt,
                    Order.created_at < end_dt
                )
            )
        )
        total_orders = total_result.scalar() or 0

        # Buy orders
        buy_result = await session.execute(
            select(func.count(Order.id)).where(
                and_(
                    Order.created_at >= start_dt,
                    Order.created_at < end_dt,
                    Order.side == OrderSide.BUY
                )
            )
        )
        buy_orders = buy_result.scalar() or 0

        # Sell orders
        sell_result = await session.execute(
            select(func.count(Order.id)).where(
                and_(
                    Order.created_at >= start_dt,
                    Order.created_at < end_dt,
                    Order.side == OrderSide.SELL
                )
            )
        )
        sell_orders = sell_result.scalar() or 0

        # Filled orders
        filled_result = await session.execute(
            select(func.count(Order.id)).where(
                and_(
                    Order.created_at >= start_dt,
                    Order.created_at < end_dt,
                    Order.status == OrderStatus.FILLED
                )
            )
        )
        filled_orders = filled_result.scalar() or 0

        return {
            "total": total_orders,
            "buys": buy_orders,
            "sells": sell_orders,
            "filled": filled_orders,
        }

    async def _get_profit_stats(
        self,
        session: AsyncSession,
        start_dt: datetime,
        end_dt: datetime
    ) -> dict:
        """Get profit statistics from closed positions."""
        # Closed positions in the period
        result = await session.execute(
            select(Position).where(
                and_(
                    Position.exit_time >= start_dt,
                    Position.exit_time < end_dt,
                    Position.status == PositionStatus.CLOSED
                )
            )
        )
        closed_positions = result.scalars().all()

        total_profit_usdt = 0.0
        total_profit_pct = 0.0
        winning_trades = 0
        losing_trades = 0

        for pos in closed_positions:
            if pos.profit_usdt is not None:
                total_profit_usdt += pos.profit_usdt
                if pos.profit_usdt >= 0:
                    winning_trades += 1
                else:
                    losing_trades += 1
            if pos.profit_pct is not None:
                total_profit_pct += pos.profit_pct

        return {
            "closed_positions": len(closed_positions),
            "total_profit_usdt": total_profit_usdt,
            "total_profit_pct": total_profit_pct,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
        }

    async def _get_active_tokens_count(self, session: AsyncSession) -> int:
        """Get count of active tokens."""
        result = await session.execute(
            select(func.count(Token.id)).where(Token.is_active == True)
        )
        return result.scalar() or 0

    def _format_report(
        self,
        report_date,
        order_stats: dict,
        profit_stats: dict,
        active_tokens: int
    ) -> str:
        """Format the daily report message."""
        profit_sign = "+" if profit_stats["total_profit_usdt"] >= 0 else ""
        profit_emoji = "📈" if profit_stats["total_profit_usdt"] >= 0 else "📉"

        # Calculate win rate
        total_trades = profit_stats["winning_trades"] + profit_stats["losing_trades"]
        win_rate = (profit_stats["winning_trades"] / total_trades * 100) if total_trades > 0 else 0

        report = (
            f"<b>📊 Daily Report - {report_date}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"

            f"<b>📦 Orders:</b>\n"
            f"  • Total: <b>{order_stats['total']}</b>\n"
            f"  • Buys: <b>{order_stats['buys']}</b>\n"
            f"  • Sells: <b>{order_stats['sells']}</b>\n"
            f"  • Filled: <b>{order_stats['filled']}</b>\n\n"

            f"<b>{profit_emoji} Profit:</b>\n"
            f"  • Closed trades: <b>{profit_stats['closed_positions']}</b>\n"
            f"  • Total P&L: <b>{profit_sign}${profit_stats['total_profit_usdt']:.2f}</b>\n"
            f"  • Win/Loss: <b>{profit_stats['winning_trades']}/{profit_stats['losing_trades']}</b>\n"
            f"  • Win Rate: <b>{win_rate:.1f}%</b>\n\n"

            f"<b>🪙 Active Tokens:</b> <b>{active_tokens}</b>\n\n"

            f"<i>Report generated at {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC</i>"
        )

        return report


async def generate_daily_report(
    session_factory: sessionmaker,
    telegram_queue: asyncio.Queue,
    report_date: datetime = None
) -> None:
    """
    Convenience function to generate a one-time daily report.

    Args:
        session_factory: SQLAlchemy async session factory
        telegram_queue: Queue for sending Telegram messages
        report_date: Date to report on (default: yesterday)
    """
    service = DailyReportService(session_factory, telegram_queue)
    await service.generate_and_send_report(report_date)
