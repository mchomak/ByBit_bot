"""Database connection and session management."""

from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker

from .models import Base


class Database:
    """
    Async database connection manager.

    Handles engine creation, session factory, and table initialization.
    """

    def __init__(self, database_url: str, echo: bool = False):
        """
        Initialize database connection.

        Args:
            database_url: PostgreSQL connection string (asyncpg format)
            echo: Whether to log SQL statements
        """
        self.database_url = database_url
        self.echo = echo
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker] = None
        self.log = logger.bind(component="Database")

    @property
    def engine(self) -> AsyncEngine:
        """Get the database engine, creating it if necessary."""
        if self._engine is None:
            self._engine = create_async_engine(
                self.database_url,
                echo=self.echo,
                future=True,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
            )
            self.log.info("Database engine created")
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get the session factory, creating it if necessary."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )
            self.log.info("Session factory created")
        return self._session_factory

    async def create_tables(self) -> None:
        """Create all database tables if they don't exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.log.info("Database tables created/verified")

    async def drop_tables(self) -> None:
        """Drop all database tables. USE WITH CAUTION."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        self.log.warning("All database tables dropped")

    async def close(self) -> None:
        """Close the database connection."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            self.log.info("Database connection closed")

    def get_session(self) -> AsyncSession:
        """
        Get a new database session.

        Usage:
            async with db.get_session() as session:
                # do something with session
        """
        return self.session_factory()

    async def health_check(self) -> bool:
        """
        Check database connectivity.

        Returns:
            True if connection is healthy, False otherwise.
        """
        try:
            async with self.session_factory() as session:
                await session.execute("SELECT 1")
            return True
        except Exception as e:
            self.log.error(f"Database health check failed: {e}")
            return False
