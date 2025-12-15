"""Asynchronous database reader using SQLAlchemy ORM."""

from typing import Optional, Type, Any, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger


class DatabaseReader:
    """
    Asynchronous reader for fetching records from the database.
    """

    def __init__(self, async_session_maker: AsyncSession):
        """
        Initialize the DatabaseReader.

        Args:
            async_session_maker (AsyncSession): Asynchronous SQLAlchemy session factory.
        """
        self.log = logger.bind(component="DatabaseReader")
        self.async_session_maker = async_session_maker
        self.log.info("Initialized DatabaseReader")


    async def get_by_field(
        self,
        model_class: Type,
        field_name: str,
        field_value: Any
    ) -> Optional[Any]:
        """
        Retrieve a single record by a single field.

        Returns:
            Optional[Any]: A single matching record or None.
        """
        model = model_class.__name__
        self.log.debug(
            "get_by_field → Preparing query on {model}.{field} == {value}",
            model=model, field=field_name, value=field_value
        )
        try:
            async with self.async_session_maker() as session:
                stmt = select(model_class).where(
                    getattr(model_class, field_name) == field_value
                )
                result = await session.execute(stmt)
                record = result.scalar_one_or_none()

            if record:
                self.log.debug(
                    "get_by_field → Found record in {model} where {field}={value}",
                    model=model, field=field_name, value=field_value
                )
            else:
                self.log.warning(
                    "get_by_field → No record in {model} for {field}={value}",
                    model=model, field=field_name, value=field_value
                )
            return record

        except SQLAlchemyError as e:
            self.log.error(
                "get_by_field → Query failed on {model}.{field}={value}: {error}",
                model=model, field=field_name, value=field_value, error=e, exc_info=True
            )
            return None

    async def get_latest_by_field(
        self,
        model_class: Type,
        field_name: str,
        field_value: Any,
        sort_field: str
    ) -> Optional[Any]:
        """
        Retrieve the most recent record by a field, ordered by another field.

        Returns:
            Optional[Any]: The most recent matching record or None.
        """
        model = model_class.__name__
        self.log.debug(
            "get_latest_by_field → Query on {model}.{field}=={value}, sort desc by {sort}",
            model=model, field=field_name, value=field_value, sort=sort_field
        )
        try:
            async with self.async_session_maker() as session:
                stmt = (
                    select(model_class)
                    .where(getattr(model_class, field_name) == field_value)
                    .order_by(getattr(model_class, sort_field).desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                record = result.scalar_one_or_none()

            if record:
                self.log.debug(
                    "get_latest_by_field → Found latest {model} for {field}={value}, {sort}={ts}",
                    model=model, field=field_name, value=field_value,
                    sort=sort_field,
                    ts=getattr(record, sort_field, None)
                )
            else:
                self.log.warning(
                    "get_latest_by_field → No records in {model} for {field}={value}",
                    model=model, field=field_name, value=field_value
                )
            return record

        except SQLAlchemyError as e:
            self.log.error(
                "get_latest_by_field → Query failed on {model}.{field}={value}: {error}",
                model=model, field=field_name, value=field_value, error=e, exc_info=True
            )
            return None

    async def get_by_fields(
        self,
        model_class: Type,
        filters: Dict[str, Any]
    ) -> Optional[Any]:
        """
        Retrieve a single record by multiple fields.

        Returns:
            Optional[Any]: A single matching record or None.
        """
        model = model_class.__name__
        self.log.debug(
            "get_by_fields → Query on {model} with filters {filters}",
            model=model, filters=filters
        )
        try:
            async with self.async_session_maker() as session:
                stmt = select(model_class)
                for field, value in filters.items():
                    stmt = stmt.where(getattr(model_class, field) == value)

                result = await session.execute(stmt)
                record = result.scalar_one_or_none()

            if record:
                self.log.debug(
                    "get_by_fields → Found record in {model} for filters {filters}",
                    model=model, filters=filters
                )
            else:
                self.log.warning(
                    "get_by_fields → No record in {model} for filters {filters}",
                    model=model, filters=filters
                )
            return record

        except SQLAlchemyError as e:
            self.log.error(
                "get_by_fields → Query failed on {model} filters={filters}: {error}",
                model=model, filters=filters, error=e, exc_info=True
            )
            return None

    async def get_all(
        self,
        model_class: Type,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Retrieve all records, with optional filtering.

        Returns:
            list[Any]: A list of matching records.
        """
        model = model_class.__name__
        self.log.debug(
            "get_all → Query on {model} with filters {filters}",
            model=model, filters=filters
        )
        try:
            async with self.async_session_maker() as session:
                stmt = select(model_class)

                if filters:
                    for field, value in filters.items():
                        if "__" in field:
                            col_name, op = field.split("__", 1)
                            col = getattr(model_class, col_name)
                            if op == "gte":
                                stmt = stmt.where(col >= value)
                            elif op == "lte":
                                stmt = stmt.where(col <= value)
                            elif op == "lt":
                                stmt = stmt.where(col < value)
                            elif op == "gt":
                                stmt = stmt.where(col > value)
                            else:
                                raise ValueError(f"Unsupported filter operator: {op}")
                        else:
                            stmt = stmt.where(getattr(model_class, field) == value)

                result = await session.execute(stmt)
                records = result.scalars().all()

            count = len(records)
            self.log.debug(
                "get_all → Retrieved {count} rows from {model} with filters {filters}",
                count=count, model=model, filters=filters
            )
            return records

        except SQLAlchemyError as e:
            self.log.error(
                "get_all → Query failed on {model} filters={filters}: {error}",
                model=model, filters=filters, error=e, exc_info=True
            )
            return []

    async def get_oldest_by_field(
        self,
        model_class: Type,
        field_name: str,
        field_value: Any,
        sort_field: str
    ) -> Optional[Any]:
        """
        Retrieve the earliest record by a field, ordered ascending by sort_field.
        """
        model = model_class.__name__
        self.log.debug(
            "get_oldest_by_field → Query on {model}.{field}=={value}, sort asc by {sort}",
            model=model, field=field_name, value=field_value, sort=sort_field
        )
        try:
            async with self.async_session_maker() as session:
                stmt = (
                    select(model_class)
                    .where(getattr(model_class, field_name) == field_value)
                    .order_by(getattr(model_class, sort_field).asc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                record = result.scalar_one_or_none()

            if record:
                self.log.debug(
                    "get_oldest_by_field → Found oldest {model} for {field}={value}, {sort}={ts}",
                    model=model, field=field_name, value=field_value,
                    sort=sort_field,
                    ts=getattr(record, sort_field, None)
                )
            else:
                self.log.warning(
                    "get_oldest_by_field → No records in {model} for {field}={value}",
                    model=model, field=field_name, value=field_value
                )
            return record

        except SQLAlchemyError as e:
            self.log.error(
                "get_oldest_by_field → Query failed on {model}.{field}={value}: {error}",
                model=model, field=field_name, value=field_value, error=e, exc_info=True
            )
            return None

