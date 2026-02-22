import asyncio
import time

import pytest
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from sqlalchemy import func, select

from icestream.config import Config
from icestream.models import TopicWALFile, WALFile, WALFileOffset

TEST_TOPIC = "test_topic"


@pytest.mark.asyncio
async def test_produce_single_message(http_client, bootstrap_servers):
    resp = await http_client.post(
        "/topics",
        json={"name": TEST_TOPIC, "num_partitions": 3},
    )
    assert resp.status_code in (200, 201, 400)

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        send_coro = await producer.send(
            TEST_TOPIC, b"hello kafka from test", key=b"test_key"
        )
        record_metadata = await send_coro
        assert record_metadata is not None
    finally:
        await producer.stop()


async def _wait_for_topic_compaction(topic: str, timeout_seconds: float = 20.0) -> None:
    cfg = Config()
    assert cfg.async_session_factory is not None
    start = time.monotonic()
    try:
        while time.monotonic() - start < timeout_seconds:
            async with cfg.async_session_factory() as session:
                topic_wal_count = (
                    await session.execute(
                        select(func.count())
                        .select_from(TopicWALFile)
                        .where(TopicWALFile.topic_name == topic)
                    )
                ).scalar_one()

                uncompacted_wal_count = (
                    await session.execute(
                        select(func.count())
                        .select_from(WALFileOffset)
                        .join(WALFile, WALFile.id == WALFileOffset.wal_file_id)
                        .where(
                            WALFileOffset.topic_name == topic,
                            WALFile.compacted_at.is_(None),
                        )
                    )
                ).scalar_one()

            if topic_wal_count > 0 and uncompacted_wal_count == 0:
                return

            await asyncio.sleep(0.2)
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()

    raise AssertionError("compaction did not materialize topic-wal files in time")


@pytest.mark.asyncio
@pytest.mark.enable_compaction
async def test_reads_work_after_topic_wal_compaction(http_client, bootstrap_servers):
    topic = "topic_wal_compaction_it"
    create_resp = await http_client.post(
        "/topics",
        json={"name": topic, "num_partitions": 1},
    )
    assert create_resp.status_code in (200, 201, 400)

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        base_ts = 1_735_689_600_000
        payloads = [f"msg-{idx}".encode() for idx in range(12)]
        for idx, payload in enumerate(payloads):
            await producer.send_and_wait(
                topic,
                payload,
                key=f"k-{idx}".encode(),
                timestamp_ms=base_ts + idx * 1000,
            )
    finally:
        await producer.stop()

    await _wait_for_topic_compaction(topic)

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        await consumer.seek_to_beginning(tp)

        fetched: list[bytes] = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and len(fetched) < len(payloads):
            records_map = await consumer.getmany(timeout_ms=500, max_records=100)
            for records in records_map.values():
                for record in records:
                    fetched.append(record.value)

        assert fetched == payloads

        offsets = await consumer.offsets_for_times({tp: base_ts + 7 * 1000})
        off_and_ts = offsets[tp]
        assert off_and_ts is not None
        assert off_and_ts.offset == 7
    finally:
        await consumer.stop()
