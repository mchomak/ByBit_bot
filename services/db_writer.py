"""
Database Writer Service for the Trading Pipeline.

Consumes DBWriteTask events from the queue and performs batched
upserts to PostgreSQL. Also handles periodic retention cleanup.

Features:
- Batched upserts for efficiency (configurable flush interval)
- ON CONFLICT DO UPDATE for candle upsert semantics
- Periodic cleanup of candles older than retention window
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import os
import sys
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.pipeline_events import DBWriteTask, PipelineMetrics


class DBWriterService:
    """
    Database writer service that batches candle upserts.

    Consumes from a queue and flushes periodically for efficiency.
    """

    def __init__(
        self,
        input_queue: asyncio.Queue,
        session_factory: sessionmaker,
        candle_model: Any,
        *,
        flush_interval_s: float = 1.0,
        retention_days: int = 5,
        cleanup_interval_minutes: int = 60,
        metrics: Optional[PipelineMetrics] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize the DB writer service.

        Args:
            input_queue: Queue to consume DBWriteTask events from
            session_factory: SQLAlchemy async session factory
            candle_model: SQLAlchemy model for candles table
            flush_interval_s: How often to flush buffer to DB
            retention_days: How many days of candles to keep
            cleanup_interval_minutes: How often to run cleanup
            metrics: Optional shared metrics object
            logger: Optional logger instance
        """
        self._queue = input_queue
        self._session_factory = session_factory
        self._model = candle_model
        self._flush_interval = float(flush_interval_s)
        self._retention_days = int(retention_days)
        self._cleanup_interval = int(cleanup_interval_minutes) * 60  # Convert to seconds

        self._metrics = metrics or PipelineMetrics()
        self._log = logger or logging.getLogger(self.__class__.__name__)

        # Buffer for batching writes
        self._buffer: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
        self._buffer_lock = asyncio.Lock()

        # Task handles
        self._consumer_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the DB writer service."""
        if self._consumer_task is not None:
            self._log.warning("Service already running")
            return

        self._stop_event.clear()

        # Start consumer task
        self._consumer_task = asyncio.create_task(
            self._consume_loop(),
            name="db-writer-consumer"
        )

        # Start flush task
        self._flush_task = asyncio.create_task(
            self._flush_loop(),
            name="db-writer-flush"
        )

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(),
            name="db-writer-cleanup"
        )

        self._log.info(
            "DB writer started (flush=%.1fs, retention=%dd, cleanup=%dm)",
            self._flush_interval,
            self._retention_days,
            self._cleanup_interval // 60,
        )

    async def stop(self) -> None:
        """Stop the DB writer service gracefully."""
        self._stop_event.set()

        # Final flush before stopping
        await self.flush_now()

        # Cancel tasks
        for task in [self._consumer_task, self._flush_task, self._cleanup_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._consumer_task = None
        self._flush_task = None
        self._cleanup_task = None

        self._log.info("DB writer stopped")

    async def _consume_loop(self) -> None:
        """Consume DBWriteTask events from the queue."""
        while not self._stop_event.is_set():
            try:
                try:
                    task: DBWriteTask = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=0.5
                    )
                except asyncio.TimeoutError:
                    continue

                # Add to buffer
                await self._add_to_buffer(task)
                self._queue.task_done()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error consuming DB task: %s", e)
                self._metrics.errors += 1

    async def _add_to_buffer(self, task: DBWriteTask) -> None:
        """Add a write task to the buffer."""
        key = (task.symbol, task.timestamp)
        row = {
            "symbol": task.symbol,
            "timestamp": task.timestamp,
            "open": task.open,
            "high": task.high,
            "low": task.low,
            "close": task.close,
            "volume": task.volume,
            "turnover": task.turnover,
            "is_confirmed": task.is_confirmed,
        }

        async with self._buffer_lock:
            self._buffer[key] = row

    async def _flush_loop(self) -> None:
        """Periodically flush the buffer to the database."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._flush_interval)
                await self.flush_now()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error in flush loop: %s", e)
                self._metrics.errors += 1
                await asyncio.sleep(min(5.0, self._flush_interval * 2))

    async def flush_now(self) -> int:
        """
        Flush the current buffer to the database.

        Returns the number of rows upserted.
        """
        async with self._buffer_lock:
            if not self._buffer:
                return 0
            rows = list(self._buffer.values())
            self._buffer.clear()

        if not rows:
            return 0

        try:
            async with self._session_factory() as session:
                stmt = pg_insert(self._model).values(rows)
                update_cols = {
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "turnover": stmt.excluded.turnover,
                    "is_confirmed": stmt.excluded.is_confirmed,
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol", "timestamp"],
                    set_=update_cols,
                )
                await session.execute(stmt)
                await session.commit()

            self._metrics.db_writes_completed += len(rows)
            self._log.debug("Flushed %d candle rows", len(rows))
            return len(rows)

        except Exception as e:
            self._log.exception("Failed to flush %d rows: %s", len(rows), e)
            self._metrics.errors += 1
            # Put rows back in buffer for retry
            async with self._buffer_lock:
                for row in rows:
                    key = (row["symbol"], row["timestamp"])
                    if key not in self._buffer:  # Don't overwrite newer data
                        self._buffer[key] = row
            return 0

    async def _cleanup_loop(self) -> None:
        """Periodically clean up old candles."""
        # Wait a bit before first cleanup
        await asyncio.sleep(60)

        while not self._stop_event.is_set():
            try:
                await self._cleanup_old_candles()
                await asyncio.sleep(self._cleanup_interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.exception("Error in cleanup loop: %s", e)
                self._metrics.errors += 1
                await asyncio.sleep(60)

    async def _cleanup_old_candles(self) -> int:
        """Delete candles older than retention window."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._retention_days)

        try:
            from sqlalchemy import delete

            async with self._session_factory() as session:
                stmt = delete(self._model).where(self._model.timestamp < cutoff)
                result = await session.execute(stmt)
                await session.commit()
                deleted = result.rowcount

            if deleted > 0:
                self._log.info(
                    "Cleaned up %d candles older than %s",
                    deleted,
                    cutoff.isoformat(),
                )
            return deleted

        except Exception as e:
            self._log.exception("Failed to cleanup old candles: %s", e)
            return 0

    async def get_recent_candles(
        self,
        symbol: str,
        limit: int = 7200,  # 5 days of 1m candles
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent candles for a symbol from the database.

        Used for bootstrapping strategy state at startup.
        """
        from sqlalchemy import select

        try:
            async with self._session_factory() as session:
                stmt = (
                    select(self._model)
                    .where(self._model.symbol == symbol)
                    .where(self._model.is_confirmed == True)  # noqa: E712
                    .order_by(self._model.timestamp.desc())
                    .limit(limit)
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()

            return [
                {
                    "timestamp": row.timestamp,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "turnover": row.turnover,
                }
                for row in rows
            ]

        except Exception as e:
            self._log.exception("Failed to fetch candles for %s: %s", symbol, e)
            return []