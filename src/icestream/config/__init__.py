import os
from typing import Any, TypeAlias

from boto3 import Session
from obstore.auth.boto3 import Boto3CredentialProvider
from obstore.store import (
    AzureStore,
    GCSStore,
    HTTPStore,
    LocalStore,
    MemoryStore,
    S3Store,
)
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

ObjectStore: TypeAlias = (
    AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore
)


class Config:
    def __init__(self):
        # db
        self.DATABASE_URL = os.getenv(
            "ICESTREAM_DATABASE_URL", "sqlite+aiosqlite:///:memory:"
        )
        self.async_session_factory: async_sessionmaker[AsyncSession] | None = None
        self.engine = None

        # obj store
        self.OBJECT_STORE_PROVIDER = os.getenv(
            "ICESTREAM_OBJECT_STORE_PROVIDER", "memory"
        )
        self.WAL_BUCKET = os.getenv("ICESTREAM_WAL_BUCKET", "icestream-wal")
        self.WAL_BUCKET_PREFIX = os.getenv("ICESTREAM_WAL_BUCKET_PREFIX", "")
        self.S3_ENDPOINT_URL = os.getenv("ICESTREAM_S3_ENDPOINT_URL")
        self.REGION = os.getenv("ICESTREAM_REGION", "us-east-1")
        self.MAX_IN_FLIGHT_FLUSHES = int(
            os.getenv("ICESTREAM_MAX_IN_FLIGHT_FLUSHES", "3")
        )
        self.store: ObjectStore = MemoryStore()

        # wal
        self.FLUSH_INTERVAL = int(os.getenv("ICESTREAM_FLUSH_INTERVAL", 2))
        self.FLUSH_SIZE = int(os.getenv("ICESTREAM_FLUSH_SIZE", 100 * 1024 * 1024))

        # compaction (technically just processing and writing to iceberg)
        self.ENABLE_COMPACTION = os.getenv("ICESTREAM_ENABLE_COMPACTION", "true").lower() == "true"
        self.COMPACTION_INTERVAL = int(os.getenv("ICESTREAM_COMPACTION_INTERVAL", 60)) # seconds
        self.MAX_COMPACTION_SELECT_LIMIT = int(os.getenv("ICESTREAM_MAX_COMPACTION_SELECT_LIMIT", 10))
        self.MAX_COMPACTION_WAL_FILES = int(os.getenv("ICESTREAM_MAX_COMPACTION_WAL_FILES", 60))
        self.MAX_COMPACTION_BYTES = int(os.getenv("ICESTREAM_MAX_COMPACTION_BYTES", 100 * 1024 * 1024))

        # pyiceberg
        #
        self.USE_PYICEBERG_CONFIG = os.getenv("ICESTREAM_USE_PYICEBERG_CONFIG", "false").lower() == "true"
        self.ICEBERG_NAMESPACE = os.getenv("ICESTREAM_ICEBERG_NAMESPACE", "icestream")
        self.ICEBERG_CREATE_NAMESPACE = os.getenv("ICESTREAM_ICEBERG_CREATE_NAMESPACE", "true").lower() == "true"
        # for now only support rest catalog
        # if s3 tables or glue (rest) then AWS_* environment variables need to be set

        self.create_engine()
        self.create_store()

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

    def create_store(self):
        bucket_path = self.WAL_BUCKET + self.WAL_BUCKET_PREFIX # TODO
        region = self._get_region()
        endpoint = self._get_endpoint()
        if self.OBJECT_STORE_PROVIDER == "aws":
            store_kwargs: dict[str, Any] = {"region": region}
            if endpoint is not None:
                store_kwargs["endpoint"] = endpoint
            # if any env var starts with AWS_, assume that we should get credentials that way
            if not any(key.startswith("AWS_") for key in os.environ):
                session = Session()
                credential_provider = Boto3CredentialProvider(session)
                store_kwargs["credential_provider"] = credential_provider
            self.store = S3Store(bucket_path, **store_kwargs)

    def _get_region(self):
        return os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or self.REGION

    def _get_endpoint(self):
        return (
            os.getenv("AWS_ENDPOINT")
            or os.getenv("AWS_ENDPOINT_URL")
            or self.S3_ENDPOINT_URL
        )
