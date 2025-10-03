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
from pyiceberg.catalog import load_catalog
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from icestream.errors import IcebergConfigurationError

ObjectStore: TypeAlias = (
    AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore
)


class Config:
    def __init__(self):
        # broker
        self.BROKER_ID = os.getenv("ICESTREAM_BROKER_ID", "unknown")

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
        self.WAL_BUCKET_PREFIX = os.getenv("ICESTREAM_WAL_BUCKET_PREFIX", None)
        self.S3_ENDPOINT_URL = os.getenv("ICESTREAM_S3_ENDPOINT_URL")
        self.REGION = os.getenv("ICESTREAM_REGION", "us-east-1")
        self.MAX_IN_FLIGHT_FLUSHES = int(
            os.getenv("ICESTREAM_MAX_IN_FLIGHT_FLUSHES", "3")
        )
        self.store: ObjectStore = MemoryStore()

        # wal
        self.FLUSH_INTERVAL = float(os.getenv("ICESTREAM_FLUSH_INTERVAL", 2))
        self.FLUSH_SIZE = int(os.getenv("ICESTREAM_FLUSH_SIZE", 100 * 1024 * 1024))
        self.FLUSH_TIMEOUT = float(os.getenv("ICESTREAM_FLUSH_TIMEOUT", 30))

        # compaction (technically just processing and writing to parquet)
        self.ENABLE_COMPACTION = (
            os.getenv("ICESTREAM_ENABLE_COMPACTION", "true").lower() == "true"
        )
        self.COMPACTION_INTERVAL = int(
            os.getenv("ICESTREAM_COMPACTION_INTERVAL", 60)
        )  # seconds
        self.MAX_COMPACTION_SELECT_LIMIT = int(
            os.getenv("ICESTREAM_MAX_COMPACTION_SELECT_LIMIT", 10)
        )
        self.MAX_COMPACTION_WAL_FILES = int(
            os.getenv("ICESTREAM_MAX_COMPACTION_WAL_FILES", 60)
        )
        self.MAX_COMPACTION_BYTES = int(
            os.getenv("ICESTREAM_MAX_COMPACTION_BYTES", 100 * 1024 * 1024)
        )

        # parquet / compaction tuning
        self.PARQUET_TARGET_FILE_BYTES = int(
            os.getenv("ICESTREAM_PARQUET_TARGET_FILE_BYTES", 256 * 1024 * 1024)
        )
        self.PARQUET_ROW_GROUP_TARGET_BYTES = int(
            os.getenv("ICESTREAM_PARQUET_ROW_GROUP_TARGET_BYTES", 64 * 1024 * 1024)
        )
        self.PARQUET_FORCE_FLUSH_MAX_LATENCY_SEC = int(
            os.getenv("ICESTREAM_PARQUET_FORCE_FLUSH_MAX_LATENCY_SEC", 300)
        )

        # parquet→parquet compaction policy
        self.PARQUET_COMPACTION_TARGET_BYTES = int(
            os.getenv("ICESTREAM_PARQUET_COMPACTION_TARGET_BYTES", 512 * 1024 * 1024)
        )
        self.PARQUET_COMPACTION_MIN_INPUT_FILES = int(
            os.getenv("ICESTREAM_PARQUET_COMPACTION_MIN_INPUT_FILES", 4)
        )
        self.PARQUET_COMPACTION_MAX_INPUT_FILES = int(
            os.getenv("ICESTREAM_PARQUET_COMPACTION_MAX_INPUT_FILES", 200)
        )
        self.PARQUET_COMPACTION_FORCE_AGE_SEC = int(
            os.getenv("ICESTREAM_PARQUET_COMPACTION_FORCE_AGE_SEC", 3600)
        )

        # where to write parquet files (a prefix/keyspace in your object store)
        self.PARQUET_PREFIX = os.getenv("ICESTREAM_PARQUET_PREFIX", "parquet")

        # pyiceberg - not supported until manifest compaction/rewriting/file deleting is supported
        #
        self.USE_PYICEBERG_CONFIG = (
            os.getenv("ICESTREAM_USE_PYICEBERG_CONFIG", "false").lower() == "true"
        )
        self.ICEBERG_NAMESPACE = os.getenv("ICESTREAM_ICEBERG_NAMESPACE", "icestream")
        self.ICEBERG_CREATE_NAMESPACE = (
            os.getenv("ICESTREAM_ICEBERG_CREATE_NAMESPACE", "true").lower() == "true"
        )
        # for now only support rest catalog
        # if s3 tables or glue (rest) then AWS_* environment variables need to be set
        # because pyiceberg doesn't support compaction, only managed tables should be used
        # or be able to compact or run maintenance operations
        self.ICEBERG_WAREHOUSE = os.getenv("ICESTREAM_ICEBERG_WAREHOUSE")
        self.ICEBERG_REST_URI = os.getenv("ICESTREAM_ICEBERG_REST_URI")
        self.ICEBERG_REST_TOKEN = os.getenv("ICESTREAM_ICEBERG_REST_TOKEN")
        self.ICEBERG_REST_SIGV4_ENABLED = (
            os.getenv("ICESTREAM_ICEBERG_REST_SIGV4_ENABLED", "false").lower() == "true"
        )
        self.ICEBERG_REST_SIGNING_NAME = os.getenv(
            "ICESTREAM_ICEBERG_REST_SIGNING_NAME"
        )
        self.ICEBERG_REST_SIGNING_REGION = os.getenv("ICESTREAM_REST_SIGNING_REGION")
        self.iceberg_catalog = None

        self.create_engine()
        self.create_store()

    def create_engine(self):
        url = make_url(self.DATABASE_URL)

        engine_options: dict[str, Any] = {
            "echo": False,
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
        bucket_path = self.WAL_BUCKET
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
            self.store = S3Store(
                bucket_path, prefix=self.WAL_BUCKET_PREFIX, **store_kwargs
            )

    def create_iceberg_catalog(self):
        if not self.ENABLE_COMPACTION:
            return
        catalog_opts = {"type": "rest"}
        if self.ICEBERG_WAREHOUSE is None:
            raise IcebergConfigurationError("warehouse needs to be set")
        catalog_opts["warehouse"] = self.ICEBERG_WAREHOUSE
        if self.ICEBERG_REST_URI is None:
            raise IcebergConfigurationError("rest uri needs to be set")
        catalog_opts["uri"] = self.ICEBERG_REST_URI
        if self.ICEBERG_REST_SIGV4_ENABLED:
            if not all(
                x is not None
                for x in (
                    self.ICEBERG_REST_SIGNING_NAME,
                    self.ICEBERG_REST_SIGNING_REGION,
                )
            ):
                raise IcebergConfigurationError(
                    "if sigv4 is enabled then signing name and signing region need to be set"
                )
            catalog_opts["rest.sigv4-enabled"] = "true"
            catalog_opts["rest.signing-name"] = self.ICEBERG_REST_SIGNING_NAME
            catalog_opts["rest.signing-region"] = self.ICEBERG_REST_SIGNING_REGION
        elif self.ICEBERG_REST_TOKEN:
            catalog_opts["token"] = self.ICEBERG_REST_TOKEN

        self.iceberg_catalog = load_catalog("default", **catalog_opts)
        if self.ICEBERG_CREATE_NAMESPACE:
            self.iceberg_catalog.create_namespace(self.ICEBERG_NAMESPACE)

    def _get_region(self):
        return os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or self.REGION

    def _get_endpoint(self):
        return (
            os.getenv("AWS_ENDPOINT")
            or os.getenv("AWS_ENDPOINT_URL")
            or self.S3_ENDPOINT_URL
        )
