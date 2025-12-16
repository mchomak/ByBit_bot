"""Generic repository for database operations."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypeVar

from loguru import logger
from sqlalchemy import delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.models import Base

T = TypeVar("T", bound= Base)


class Repository:
    """
    Generic async repository for database CRUD operations.

    Provides both direct operations and queue-based batch operations.
    """

    def __init__(self, session_factory: sessionmaker):
        """
        Initialize repository.

        Args:
            session_factory: SQLAlchemy async session factory
        """
        self.session_factory = session_factory
        self._write_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self.log = logger.bind(component="Repository")

    # =========================================================================
    # Read Operations
    # =========================================================================

    async def get_by_id(self, model: Type[T], id_value: Any) -> Optional[T]:
        """Get a single record by primary key."""
        try:
            async with self.session_factory() as session:
                result = await session.get(model, id_value)
                return result
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching {model.__name__} by id={id_value}: {e}")
            return None

    async def get_by_field(
        self, model: Type[T], field_name: str, field_value: Any
    ) -> Optional[T]:
        """Get a single record by a specific field."""
        try:
            async with self.session_factory() as session:
                stmt = select(model).where(
                    getattr(model, field_name) == field_value
                )
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            self.log.error(
                f"Error fetching {model.__name__} by {field_name}={field_value}: {e}"
            )
            return None

    async def get_by_fields(
        self, model: Type[T], filters: Dict[str, Any]
    ) -> Optional[T]:
        """Get a single record matching multiple field conditions."""
        try:
            async with self.session_factory() as session:
                stmt = select(model)
                for field, value in filters.items():
                    stmt = stmt.where(getattr(model, field) == value)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching {model.__name__} by filters={filters}: {e}")
            return None

    async def get_all(
        self,
        model: Type[T],
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[T]:
        """
        Get all records matching optional filters.

        Args:
            model: SQLAlchemy model class
            filters: Optional dict of field=value filters
                     Supports operators: field__gt, field__gte, field__lt, field__lte
            order_by: Field name to order by
            order_desc: If True, order descending
            limit: Maximum number of records to return
        """
        try:
            async with self.session_factory() as session:
                stmt = select(model)

                # Apply filters
                if filters:
                    for field, value in filters.items():
                        if "__" in field:
                            col_name, op = field.rsplit("__", 1)
                            col = getattr(model, col_name)
                            if op == "gt":
                                stmt = stmt.where(col > value)
                            elif op == "gte":
                                stmt = stmt.where(col >= value)
                            elif op == "lt":
                                stmt = stmt.where(col < value)
                            elif op == "lte":
                                stmt = stmt.where(col <= value)
                            elif op == "in":
                                stmt = stmt.where(col.in_(value))
                            else:
                                self.log.warning(f"Unknown filter operator: {op}")
                        else:
                            stmt = stmt.where(getattr(model, field) == value)

                # Apply ordering
                if order_by:
                    col = getattr(model, order_by)
                    stmt = stmt.order_by(col.desc() if order_desc else col.asc())

                # Apply limit
                if limit:
                    stmt = stmt.limit(limit)

                result = await session.execute(stmt)
                return list(result.scalars().all())

        except SQLAlchemyError as e:
            self.log.error(f"Error fetching all {model.__name__}: {e}")
            return []

    async def get_latest(
        self, model: Type[T], field_name: str, field_value: Any, sort_field: str
    ) -> Optional[T]:
        """Get the most recent record by a field, ordered by sort_field descending."""
        try:
            async with self.session_factory() as session:
                stmt = (
                    select(model)
                    .where(getattr(model, field_name) == field_value)
                    .order_by(getattr(model, sort_field).desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            self.log.error(f"Error fetching latest {model.__name__}: {e}")
            return None

    async def count(
        self, model: Type[T], filters: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records matching filters."""
        records = await self.get_all(model, filters)
        return len(records)

    # =========================================================================
    # Write Operations (Direct)
    # =========================================================================

    async def insert(self, model: Type[T], data: Dict[str, Any]) -> bool:
        """Insert a new record directly."""
        try:
            async with self.session_factory() as session:
                stmt = insert(model).values(**data)
                await session.execute(stmt)
                await session.commit()
                self.log.debug(f"Inserted into {model.__name__}: {data}")
                return True
        except IntegrityError as e:
            self.log.warning(f"Integrity error inserting {model.__name__}: {e.orig}")
            return False
        except SQLAlchemyError as e:
            self.log.error(f"Error inserting {model.__name__}: {e}")
            return False

    async def update(
        self,
        model: Type[T],
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ) -> int:
        """Update records matching filter_by with update_values."""
        try:
            async with self.session_factory() as session:
                stmt = update(model).filter_by(**filter_by).values(**update_values)
                result = await session.execute(stmt)
                await session.commit()
                rowcount = result.rowcount
                self.log.debug(
                    f"Updated {rowcount} rows in {model.__name__}: {filter_by} -> {update_values}"
                )
                return rowcount
        except SQLAlchemyError as e:
            self.log.error(f"Error updating {model.__name__}: {e}")
            return 0

    async def upsert(
        self,
        model: Type[T],
        conflict_keys: Dict[str, Any],
        update_values: Dict[str, Any],
    ) -> bool:
        """
        Insert or update a record (PostgreSQL upsert).

        Args:
            model: SQLAlchemy model class
            conflict_keys: Fields that define uniqueness (used for conflict detection)
            update_values: Fields to update if record exists
        """
        try:
            async with self.session_factory() as session:
                all_values = {**conflict_keys, **update_values}
                stmt = pg_insert(model).values(**all_values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=list(conflict_keys.keys()),
                    set_=update_values,
                )
                await session.execute(stmt)
                await session.commit()
                self.log.debug(f"Upserted {model.__name__}: {conflict_keys}")
                return True
        except SQLAlchemyError as e:
            self.log.error(f"Error upserting {model.__name__}: {e}")
            return False

    async def delete(self, model: Type[T], filter_by: Dict[str, Any]) -> int:
        """Delete records matching filter_by."""
        try:
            async with self.session_factory() as session:
                stmt = delete(model).filter_by(**filter_by)
                result = await session.execute(stmt)
                await session.commit()
                rowcount = result.rowcount
                self.log.debug(f"Deleted {rowcount} rows from {model.__name__}")
                return rowcount
        except SQLAlchemyError as e:
            self.log.error(f"Error deleting from {model.__name__}: {e}")
            return 0

    async def delete_older_than(
        self, model: Type[T], timestamp_field: str, cutoff: datetime
    ) -> int:
        """
        Delete records older than cutoff datetime.

        Used for cleaning old candles (> 5 days).
        """
        try:
            async with self.session_factory() as session:
                col = getattr(model, timestamp_field)
                stmt = delete(model).where(col < cutoff)
                result = await session.execute(stmt)
                await session.commit()
                rowcount = result.rowcount
                self.log.info(
                    f"Cleaned {rowcount} old records from {model.__name__} "
                    f"(older than {cutoff})"
                )
                return rowcount
        except SQLAlchemyError as e:
            self.log.error(f"Error cleaning old {model.__name__} records: {e}")
            return 0

    # =========================================================================
    # Queue-based Batch Operations
    # =========================================================================

    async def start_write_worker(self) -> None:
        """Start the background write worker for batch operations."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._write_worker())
            self.log.info("Write worker started")

    async def stop_write_worker(self) -> None:
        """Stop the background write worker."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
            self.log.info("Write worker stopped")

    async def _write_worker(self) -> None:
        """Background worker that processes write queue."""
        while True:
            try:
                task = await self._write_queue.get()
                action = task.get("action")
                model = task.get("model")
                data = task.get("data", {})

                if action == "insert":
                    await self.insert(model, data)
                elif action == "update":
                    await self.update(
                        model, data.get("filter_by", {}), data.get("update_values", {})
                    )
                elif action == "upsert":
                    await self.upsert(
                        model,
                        data.get("conflict_keys", {}),
                        data.get("update_values", {}),
                    )
                elif action == "delete":
                    await self.delete(model, data.get("filter_by", {}))
                else:
                    self.log.warning(f"Unknown write action: {action}")

                self._write_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Write worker error: {e}")
                self._write_queue.task_done()

    async def enqueue_insert(self, model: Type[T], data: Dict[str, Any]) -> None:
        """Queue an insert operation for batch processing."""
        await self._write_queue.put({"action": "insert", "model": model, "data": data})

    async def enqueue_upsert(
        self,
        model: Type[T],
        conflict_keys: Dict[str, Any],
        update_values: Dict[str, Any],
    ) -> None:
        """Queue an upsert operation for batch processing."""
        await self._write_queue.put({
            "action": "upsert",
            "model": model,
            "data": {"conflict_keys": conflict_keys, "update_values": update_values},
        })

    async def enqueue_update(
        self,
        model: Type[T],
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ) -> None:
        """Queue an update operation for batch processing."""
        await self._write_queue.put({
            "action": "update",
            "model": model,
            "data": {"filter_by": filter_by, "update_values": update_values},
        })

    async def enqueue_delete(self, model: Type[T], filter_by: Dict[str, Any]) -> None:
        """Queue a delete operation for batch processing."""
        await self._write_queue.put({
            "action": "delete",
            "model": model,
            "data": {"filter_by": filter_by},
        })
