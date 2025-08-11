from unittest.mock import MagicMock, AsyncMock

import pytest
from tests.utils.time import FakeClock

@pytest.fixture
def fake_clock():
    return FakeClock()

@pytest.fixture
def mock_async_session_factory():
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(return_value=cm)
    return factory, session

@pytest.fixture
def base_config(mock_async_session_factory):
    factory, _ = mock_async_session_factory
    cfg = MagicMock()
    cfg.WAL_BUCKET = "bucket"
    cfg.WAL_BUCKET_PREFIX = ""
    cfg.store.put_async = AsyncMock(return_value=MagicMock(etag="etag123"))
    cfg.async_session_factory = factory
    cfg.FLUSH_SIZE = 10**9
    cfg.FLUSH_INTERVAL = 5
    cfg.MAX_IN_FLIGHT_FLUSHES = 2
    cfg.FLUSH_TIMEOUT = 60.0
    cfg.FLUSH_MAX_BATCHES = None
    cfg.BROKER_ID = "b1"
    return cfg
