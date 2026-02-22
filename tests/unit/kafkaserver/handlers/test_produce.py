import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from icestream.kafkaserver.handlers.produce import do_handle_produce_request
from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch


@pytest.mark.asyncio
async def test_produce_rebases_batch_to_partition_offset():
    record = KafkaRecord(
        attributes=0,
        timestamp_delta=0,
        offset_delta=0,
        key=b"k",
        value=b"v",
        headers=[],
    )
    input_batch = KafkaRecordBatch.from_records(offset=0, records=[record])
    req = SimpleNamespace(
        topic_data=[
            SimpleNamespace(
                name="rebased_topic",
                partition_data=[
                    SimpleNamespace(index=0, records=input_batch.to_bytes()),
                ],
            )
        ]
    )

    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(first=lambda: (1, 9, 0)))
    session.commit = AsyncMock()

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)

    config = MagicMock()
    config.async_session_factory = MagicMock(return_value=session_cm)
    config.FLUSH_INTERVAL = 0.1

    produce_queue: asyncio.Queue = asyncio.Queue()
    callback = AsyncMock()
    captured = {}

    async def ack_one():
        item = await produce_queue.get()
        captured["item"] = item
        item.flush_result.set_result(True)

    ack_task = asyncio.create_task(ack_one())
    try:
        await do_handle_produce_request(
            config=config,
            produce_queue=produce_queue,
            req=req,
            api_version=8,
            callback=callback,
        )
    finally:
        await ack_task

    queued = captured["item"]
    assert queued.kafka_record_batch.base_offset == 9
    callback.assert_awaited()
