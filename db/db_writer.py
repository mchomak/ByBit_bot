"""Asynchronous database writer with batching capabilities."""

import asyncio
import logging
from typing import Type, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, update, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from loguru import logger


class DatabaseWriter:
    """
    Asynchronous writer that batches insert/update/upsert/delete tasks.
    """

    def __init__(self, async_session_maker: AsyncSession):
        """
        Initialize the DatabaseWriter.

        Args:
            async_session_maker (AsyncSession): Asynchronous SQLAlchemy session factory.
        """
        self.log = logger.bind(component="DatabaseWriter")
        self.queue = asyncio.Queue()
        self.async_session_maker = async_session_maker
        self.log.info("Initialized DatabaseWriter")


    async def start(self):
        """
        Start the worker that continuously processes tasks from the queue.
        """
        self.log.info("Writer worker started")
        while True:
            task = await self.queue.get()
            action = task.get("action")
            model = task.get("model")
            model_name = getattr(model, "__name__", "Unknown")
            self.log.debug(
                "Dequeued task → action={action} model={model}",
                action=action,
                model=model_name,
            )
            try:
                await self._handle_task(task)
            except Exception as e:
                self.log.error(
                    "Unhandled exception processing task {action} on {model}: {error}",
                    action=action,
                    model=model_name,
                    error=e,
                    exc_info=True,
                )
            finally:
                self.queue.task_done()


    async def _handle_task(self, task: Dict[str, Any]):
        action = task["action"]
        model_class: Type = task.get("model")
        data: Dict[str, Any] = task.get("data", {})

        if action == "insert":
            await self._insert(model_class, data)

        elif action == "update":
            await self._update(
                model_class,
                data.get("filter_by", {}),
                data.get("update_values", {}),
            )

        elif action == "upsert":
            await self._upsert(
                model_class,
                data.get("filter_by", {}),
                data.get("update_values", {}),
            )

        elif action == "delete":
            await self._delete(model_class, data.get("filter_by", {}))

        else:
            self.log.warning(
                "Received unknown action: {action} for model {model}",
                action=action,
                model=getattr(model_class, "__name__", "Unknown"),
            )

    async def _insert(self, model_class: Type, values: Dict[str, Any]):
        async with self.async_session_maker() as session:
            try:
                stmt = insert(model_class).values(**values)
                result = await session.execute(stmt)
                await session.commit()

                rowcount = result.rowcount
                self.log.debug(
                    "Inserted into {model}: rowcount={rows} values={values}",
                    model=model_class.__name__,
                    rows=rowcount,
                    values=values,
                )

            except IntegrityError as e:
                await session.rollback()
                self.log.warning(
                    "Integrity error on insert into {model}: {error}. Values={values}",
                    model=model_class.__name__,
                    error=e.orig,
                    values=values,
                )

    async def _update(
        self,
        model_class: Type,
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ):
        async with self.async_session_maker() as session:
            try:
                stmt = (
                    update(model_class)
                    .filter_by(**filter_by)
                    .values(**update_values)
                )
                result = await session.execute(stmt)
                await session.commit()

                rowcount = result.rowcount
                if rowcount:
                    self.log.debug(
                        "Updated {model}: rows={rows} filter={filter} set={values}",
                        model=model_class.__name__,
                        rows=rowcount,
                        filter=filter_by,
                        values=update_values,
                    )
                else:
                    self.log.warning(
                        "No rows updated for {model}: filter={filter}. Nothing to change.",
                        model=model_class.__name__,
                        filter=filter_by,
                    )

            except Exception as e:
                await session.rollback()
                self.log.error(
                    "Error updating {model}: {error}. Filter={filter}, Values={values}",
                    model=model_class.__name__,
                    error=e,
                    filter=filter_by,
                    values=update_values,
                    exc_info=True,
                )

    async def _upsert(
        self,
        model_class: Type,
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ):
        async with self.async_session_maker() as session:
            try:
                stmt = pg_insert(model_class).values(**{**filter_by, **update_values})
                stmt = stmt.on_conflict_do_update(
                    index_elements=list(filter_by.keys()),
                    set_=update_values,
                )
                result = await session.execute(stmt)
                await session.commit()

                rowcount = result.rowcount
                self.log.debug(
                    "Upserted into {model}: rows={rows} conflict_keys={keys} values={values}",
                    model=model_class.__name__,
                    rows=rowcount,
                    keys=filter_by,
                    values=update_values,
                )

            except Exception as e:
                await session.rollback()
                self.log.error(
                    "Error on upsert {model}: {error}. Conflict_keys={keys}, Values={values}",
                    model=model_class.__name__,
                    error=e,
                    keys=filter_by,
                    values=update_values,
                    exc_info=True,
                )


    async def _delete(self, model_class: Type, filter_by: Dict[str, Any]):
        async with self.async_session_maker() as session:
            try:
                stmt = delete(model_class).filter_by(**filter_by)
                result = await session.execute(stmt)
                await session.commit()

                rowcount = result.rowcount
                if rowcount:
                    self.log.debug(
                        "Deleted from {model}: rows={rows} filter={filter}",
                        model=model_class.__name__,
                        rows=rowcount,
                        filter=filter_by,
                    )
                else:
                    self.log.warning(
                        "No rows deleted for {model}: filter={filter}. Nothing found.",
                        model=model_class.__name__,
                        filter=filter_by,
                    )

            except Exception as e:
                await session.rollback()
                self.log.error(
                    "Error deleting from {model}: {error}. Filter={filter}",
                    model=model_class.__name__,
                    error=e,
                    filter=filter_by,
                    exc_info=True,
                )


    async def enqueue_insert(self, model_class: Type, data: Dict[str, Any]):
        await self.queue.put({"action": "insert", "model": model_class, "data": data})
        self.log.debug(
            "Enqueued insert for {model}: values={data}",
            model=model_class.__name__,
            data=data,
        )

    async def enqueue_upsert(
        self,
        model_class: Type,
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ):
        await self.queue.put(
            {
                "action": "upsert",
                "model": model_class,
                "data": {"filter_by": filter_by, "update_values": update_values},
            }
        )
        self.log.debug(
            "Enqueued upsert for {model}: conflict_keys={keys}, values={values}",
            model=model_class.__name__,
            keys=filter_by,
            values=update_values,
        )

    async def enqueue_delete(self, model_class: Type, filter_by: Dict[str, Any]):
        await self.queue.put(
            {"action": "delete", "model": model_class, "data": {"filter_by": filter_by}}
        )
        self.log.debug(
            "Enqueued delete for {model}: filter={filter}",
            model=model_class.__name__,
            filter=filter_by,
        )

    async def enqueue_update(
        self,
        model_class: Type,
        filter_by: Dict[str, Any],
        update_values: Dict[str, Any],
    ):
        await self.queue.put(
            {
                "action": "update",
                "model": model_class,
                "data": {"filter_by": filter_by, "update_values": update_values},
            }
        )
        self.log.debug(
            "Enqueued update for {model}: filter={filter}, values={values}",
            model=model_class.__name__,
            filter=filter_by,
            values=update_values,
        )