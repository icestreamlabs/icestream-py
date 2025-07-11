import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.manager import WALManager
from tests.utils.time import FakeClock


@pytest.mark.asyncio
async def test_run_once_flushes_on_size(monkeypatch):
    config = MagicMock()
    config.FLUSH_SIZE = 100
    config.FLUSH_INTERVAL = 999
    config.MAX_IN_FLIGHT_FLUSHES = 1
    config.WAL_BUCKET = "test"
    config.WAL_BUCKET_PREFIX = ""
    config.store.put_async = AsyncMock()
    config.async_session_factory = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(),
            __aexit__=AsyncMock(),
        )
    )

    queue = asyncio.Queue()
    clock = FakeClock()
    manager = WALManager(config=config, queue=queue, time_source=clock)

    monkeypatch.setattr(
        "icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets",
        lambda batch, broker_id: (b"data", [{
            "topic": "topic", "partition": 0,
            "base_offset": 0, "last_offset": 0,
            "byte_start": 0, "byte_end": 10
        }])
    )

    item1 = ProduceTopicPartitionData(
        topic="topic",
        partition=0,
        kafka_record_batch=MagicMock(batch_length=60),
        flush_result=asyncio.Future()
    )

    item2 = ProduceTopicPartitionData(
        topic="topic",
        partition=0,
        kafka_record_batch=MagicMock(batch_length=50),
        flush_result=asyncio.Future()
    )
    await queue.put(item1)
    await queue.put(item2)

    await manager.run_once()
    await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)

    assert item1.flush_result.done()
    assert item2.flush_result.done()
    assert item1.flush_result.result() is True

@pytest.mark.asyncio
async def test_flush_triggered_by_timeout(monkeypatch):
    config = MagicMock()
    config.FLUSH_SIZE = 1000
    config.FLUSH_INTERVAL = 5
    config.MAX_IN_FLIGHT_FLUSHES = 1
    config.WAL_BUCKET = "bucket"
    config.WAL_BUCKET_PREFIX = ""
    config.store.put_async = AsyncMock()
    config.async_session_factory = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(),
            __aexit__=AsyncMock(),
        )
    )

    queue = asyncio.Queue()
    fake_clock = FakeClock()

    manager = WALManager(config=config, queue=queue, time_source=fake_clock)

    item = MagicMock()
    item.kafka_record_batch.batch_length = 50
    item.flush_result = asyncio.Future()
    manager.buffer.append(item)
    manager.buffer_size += 50

    monkeypatch.setattr("icestream.kafkaserver.wal.manager.encode_kafka_wal_file_with_offsets", lambda batch, broker_id: (b"data", [{
        "topic": "test",
        "partition": 0,
        "base_offset": 0,
        "last_offset": 0,
        "byte_start": 0,
        "byte_end": 10,
    }]))

    fake_clock.advance(6)
    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        await manager.run_once()
    await asyncio.gather(*manager.pending_flushes)

    assert item.flush_result.done()
    assert item.flush_result.result() is True
