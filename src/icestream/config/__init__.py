import os
from typing import Any

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool


class Config:
    def __init__(self):
        self.DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
        self.async_session_factory: async_sessionmaker[AsyncSession] | None = None
        self.engine = None

        self.create_engine()

    def create_engine(self):
        url = make_url(self.DATABASE_URL)

        engine_options: dict[str, Any] = {
            "echo": True,
            "future": True,
        }

        if url.drivername.startswith("sqlite"):
            # Assign separately to keep type checkers happy
            engine_options["connect_args"] = {"check_same_thread": False}
            engine_options["poolclass"] = StaticPool

        elif url.drivername.startswith("postgresql"):
            if not url.drivername.startswith("postgresql+asyncpg"):
                url = url.set(drivername="postgresql+asyncpg")
        else:
            raise ValueError(f"Unsupported database dialect: {url.drivername}")

        self.engine = create_async_engine(url, **engine_options)
        self.async_session_factory = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )
