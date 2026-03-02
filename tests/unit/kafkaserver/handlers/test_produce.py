import asyncio
import os
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kio.schema.errors import ErrorCode
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from icestream.kafkaserver.handlers.produce import do_handle_produce_request
from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch
from icestream.models import (
    Partition,
    ProducerPartitionRecentBatch,
    ProducerPartitionState,
    ProducerSession,
    Topic,
)


def _single_record_batch(
    *,
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
) -> bytes:
    record = KafkaRecord(
        attributes=0,
        timestamp_delta=0,
        offset_delta=0,
        key=b"k",
        value=b"v",
        headers=[],
    )
    batch = KafkaRecordBatch.from_records(offset=0, records=[record])
    batch.producer_id = producer_id
    batch.producer_epoch = producer_epoch
    batch.base_sequence = base_sequence
    return batch.to_bytes()


def _produce_req(topic: str, partition: int, records: bytes):
    return SimpleNamespace(
        topic_data=[
            SimpleNamespace(
                name=topic,
                partition_data=[SimpleNamespace(index=partition, records=records)],
            )
        ]
    )


async def _run_produce(
    config,
    req,
    *,
    expected_queue_items: int,
    flush_exception: Exception | None = None,
):
    produce_queue: asyncio.Queue = asyncio.Queue()
    callback = AsyncMock()
    queued_items = []

    async def ack_items() -> None:
        for _ in range(expected_queue_items):
            item = await asyncio.wait_for(produce_queue.get(), timeout=1.0)
            queued_items.append(item)
            if flush_exception is None:
                item.flush_result.set_result(True)
            else:
                item.flush_result.set_exception(flush_exception)

    ack_task = asyncio.create_task(ack_items())
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

    response = callback.await_args.args[0]
    return response, queued_items


@pytest.mark.asyncio
async def test_produce_rebases_batch_to_partition_offset(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="rebased_topic"))
        session.add(
            Partition(
                topic_name="rebased_topic",
                partition_number=0,
                last_offset=8,
                log_start_offset=0,
            )
        )
        await session.commit()

    req = _produce_req("rebased_topic", 0, _single_record_batch())
    response, queued_items = await _run_produce(config, req, expected_queue_items=1)

    assert response.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(response.responses[0].partition_responses[0].base_offset) == 9
    assert queued_items[0].kafka_record_batch.base_offset == 9


@pytest.mark.asyncio
async def test_idempotent_produce_accepts_and_advances_sequence_state(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_accept_topic"))
        session.add(
            Partition(
                topic_name="idempotent_accept_topic",
                partition_number=0,
                last_offset=4,
                log_start_offset=0,
            )
        )
        producer_session = ProducerSession(transactional_id="txn-accept", producer_epoch=3)
        session.add(producer_session)
        await session.commit()
        producer_id = producer_session.producer_id

    req = _produce_req(
        "idempotent_accept_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=3,
            base_sequence=0,
        ),
    )
    response, _ = await _run_produce(config, req, expected_queue_items=1)

    assert response.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(response.responses[0].partition_responses[0].base_offset) == 5

    async with config.async_session_factory() as session:
        partition = (
            await session.execute(
                select(Partition).where(
                    Partition.topic_name == "idempotent_accept_topic",
                    Partition.partition_number == 0,
                )
            )
        ).scalar_one()
        state = (
            await session.execute(
                select(ProducerPartitionState).where(
                    ProducerPartitionState.producer_id == producer_id,
                    ProducerPartitionState.producer_epoch == 3,
                    ProducerPartitionState.topic_name == "idempotent_accept_topic",
                    ProducerPartitionState.partition_number == 0,
                )
            )
        ).scalar_one()
        recent = (
            await session.execute(
                select(ProducerPartitionRecentBatch).where(
                    ProducerPartitionRecentBatch.producer_id == producer_id,
                    ProducerPartitionRecentBatch.producer_epoch == 3,
                    ProducerPartitionRecentBatch.topic_name == "idempotent_accept_topic",
                    ProducerPartitionRecentBatch.partition_number == 0,
                    ProducerPartitionRecentBatch.base_sequence == 0,
                )
            )
        ).scalar_one()

    assert partition.last_offset == 5
    assert state.next_expected_sequence == 1
    assert recent.first_offset == 5
    assert recent.last_offset == 5


@pytest.mark.asyncio
async def test_idempotent_duplicate_retry_returns_original_offset_without_append(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_duplicate_topic"))
        session.add(
            Partition(
                topic_name="idempotent_duplicate_topic",
                partition_number=0,
                last_offset=9,
                log_start_offset=0,
            )
        )
        producer_session = ProducerSession(transactional_id="txn-duplicate", producer_epoch=1)
        session.add(producer_session)
        await session.flush()

        session.add(
            ProducerPartitionState(
                producer_id=producer_session.producer_id,
                producer_epoch=1,
                topic_name="idempotent_duplicate_topic",
                partition_number=0,
                next_expected_sequence=1,
                last_acked_first_offset=9,
                last_acked_last_offset=9,
            )
        )
        session.add(
            ProducerPartitionRecentBatch(
                producer_id=producer_session.producer_id,
                producer_epoch=1,
                topic_name="idempotent_duplicate_topic",
                partition_number=0,
                base_sequence=0,
                last_sequence=0,
                first_offset=9,
                last_offset=9,
            )
        )
        await session.commit()
        producer_id = producer_session.producer_id

    req = _produce_req(
        "idempotent_duplicate_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=1,
            base_sequence=0,
        ),
    )
    response, queued_items = await _run_produce(config, req, expected_queue_items=0)

    assert response.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(response.responses[0].partition_responses[0].base_offset) == 9
    assert queued_items == []


@pytest.mark.asyncio
async def test_idempotent_out_of_order_sequence_is_rejected(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_oor_topic"))
        session.add(Partition(topic_name="idempotent_oor_topic", partition_number=0))
        producer_session = ProducerSession(transactional_id="txn-oor", producer_epoch=0)
        session.add(producer_session)
        await session.flush()
        session.add(
            ProducerPartitionState(
                producer_id=producer_session.producer_id,
                producer_epoch=0,
                topic_name="idempotent_oor_topic",
                partition_number=0,
                next_expected_sequence=5,
            )
        )
        await session.commit()
        producer_id = producer_session.producer_id

    req = _produce_req(
        "idempotent_oor_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=0,
            base_sequence=7,
        ),
    )
    response, queued_items = await _run_produce(config, req, expected_queue_items=0)

    assert (
        response.responses[0].partition_responses[0].error_code
        == ErrorCode.out_of_order_sequence_number
    )
    assert queued_items == []


@pytest.mark.asyncio
async def test_idempotent_invalid_epoch_is_rejected(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_epoch_topic"))
        session.add(Partition(topic_name="idempotent_epoch_topic", partition_number=0))
        producer_session = ProducerSession(transactional_id="txn-epoch", producer_epoch=2)
        session.add(producer_session)
        await session.commit()
        producer_id = producer_session.producer_id

    req = _produce_req(
        "idempotent_epoch_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=1,
            base_sequence=0,
        ),
    )
    response, queued_items = await _run_produce(config, req, expected_queue_items=0)

    assert (
        response.responses[0].partition_responses[0].error_code
        == ErrorCode.invalid_producer_epoch
    )
    assert queued_items == []


@pytest.mark.asyncio
async def test_idempotent_flush_failure_does_not_advance_offsets_or_sequence(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_flush_fail_topic"))
        session.add(
            Partition(
                topic_name="idempotent_flush_fail_topic",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        producer_session = ProducerSession(
            transactional_id="txn-flush-fail",
            producer_epoch=0,
        )
        session.add(producer_session)
        await session.flush()
        session.add(
            ProducerPartitionState(
                producer_id=producer_session.producer_id,
                producer_epoch=0,
                topic_name="idempotent_flush_fail_topic",
                partition_number=0,
                next_expected_sequence=0,
            )
        )
        await session.commit()
        producer_id = producer_session.producer_id

    req = _produce_req(
        "idempotent_flush_fail_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=0,
            base_sequence=0,
        ),
    )
    response, _ = await _run_produce(
        config,
        req,
        expected_queue_items=1,
        flush_exception=RuntimeError("flush failed"),
    )

    assert (
        response.responses[0].partition_responses[0].error_code
        == ErrorCode.unknown_server_error
    )

    async with config.async_session_factory() as session:
        partition = (
            await session.execute(
                select(Partition).where(
                    Partition.topic_name == "idempotent_flush_fail_topic",
                    Partition.partition_number == 0,
                )
            )
        ).scalar_one()
        state = (
            await session.execute(
                select(ProducerPartitionState).where(
                    ProducerPartitionState.producer_id == producer_id,
                    ProducerPartitionState.producer_epoch == 0,
                    ProducerPartitionState.topic_name == "idempotent_flush_fail_topic",
                    ProducerPartitionState.partition_number == 0,
                )
            )
        ).scalar_one()
        recent = (
            await session.execute(
                select(ProducerPartitionRecentBatch).where(
                    ProducerPartitionRecentBatch.producer_id == producer_id,
                    ProducerPartitionRecentBatch.producer_epoch == 0,
                    ProducerPartitionRecentBatch.topic_name == "idempotent_flush_fail_topic",
                    ProducerPartitionRecentBatch.partition_number == 0,
                )
            )
        ).scalars().all()

    assert partition.last_offset == -1
    assert state.next_expected_sequence == 0
    assert recent == []


@pytest.mark.asyncio
async def test_idempotent_sequence_tracking_is_independent_per_partition(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="idempotent_partition_independence_topic"))
        session.add(
            Partition(
                topic_name="idempotent_partition_independence_topic",
                partition_number=0,
            )
        )
        await session.flush()
        session.add(
            Partition(
                topic_name="idempotent_partition_independence_topic",
                partition_number=1,
            )
        )
        producer_session = ProducerSession(
            transactional_id="txn-partition-independence",
            producer_epoch=0,
        )
        session.add(producer_session)
        await session.commit()
        producer_id = producer_session.producer_id

    req_p0_seq0 = _produce_req(
        "idempotent_partition_independence_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=0,
            base_sequence=0,
        ),
    )
    resp_p0_seq0, _ = await _run_produce(config, req_p0_seq0, expected_queue_items=1)
    assert resp_p0_seq0.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(resp_p0_seq0.responses[0].partition_responses[0].base_offset) == 0

    req_p1_seq0 = _produce_req(
        "idempotent_partition_independence_topic",
        1,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=0,
            base_sequence=0,
        ),
    )
    resp_p1_seq0, _ = await _run_produce(config, req_p1_seq0, expected_queue_items=1)
    assert resp_p1_seq0.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(resp_p1_seq0.responses[0].partition_responses[0].base_offset) == 0

    req_p0_seq1 = _produce_req(
        "idempotent_partition_independence_topic",
        0,
        _single_record_batch(
            producer_id=producer_id,
            producer_epoch=0,
            base_sequence=1,
        ),
    )
    resp_p0_seq1, _ = await _run_produce(config, req_p0_seq1, expected_queue_items=1)
    assert resp_p0_seq1.responses[0].partition_responses[0].error_code == ErrorCode.none
    assert int(resp_p0_seq1.responses[0].partition_responses[0].base_offset) == 1

    async with config.async_session_factory() as session:
        partition0 = (
            await session.execute(
                select(Partition).where(
                    Partition.topic_name == "idempotent_partition_independence_topic",
                    Partition.partition_number == 0,
                )
            )
        ).scalar_one()
        partition1 = (
            await session.execute(
                select(Partition).where(
                    Partition.topic_name == "idempotent_partition_independence_topic",
                    Partition.partition_number == 1,
                )
            )
        ).scalar_one()
        state0 = (
            await session.execute(
                select(ProducerPartitionState).where(
                    ProducerPartitionState.producer_id == producer_id,
                    ProducerPartitionState.producer_epoch == 0,
                    ProducerPartitionState.topic_name
                    == "idempotent_partition_independence_topic",
                    ProducerPartitionState.partition_number == 0,
                )
            )
        ).scalar_one()
        state1 = (
            await session.execute(
                select(ProducerPartitionState).where(
                    ProducerPartitionState.producer_id == producer_id,
                    ProducerPartitionState.producer_epoch == 0,
                    ProducerPartitionState.topic_name
                    == "idempotent_partition_independence_topic",
                    ProducerPartitionState.partition_number == 1,
                )
            )
        ).scalar_one()

    assert partition0.last_offset == 1
    assert partition1.last_offset == 0
    assert state0.next_expected_sequence == 2
    assert state1.next_expected_sequence == 1


@pytest.mark.asyncio
async def test_concurrent_idempotent_produce_multiple_sessions_same_partition(config):
    if os.getenv("ICESTREAM_USING_MOCKGRES") == "1":
        async with config.async_session_factory() as session:
            session.add(Topic(name="idempotent_concurrent_sessions_topic"))
            session.add(
                Partition(
                    topic_name="idempotent_concurrent_sessions_topic",
                    partition_number=0,
                )
            )
            producer_session_a = ProducerSession(
                transactional_id="txn-concurrent-a",
                producer_epoch=0,
            )
            producer_session_b = ProducerSession(
                transactional_id="txn-concurrent-b",
                producer_epoch=0,
            )
            session.add(producer_session_a)
            await session.flush()
            session.add(producer_session_b)
            await session.commit()
            producer_id_a = producer_session_a.producer_id
            producer_id_b = producer_session_b.producer_id

        req_a = _produce_req(
            "idempotent_concurrent_sessions_topic",
            0,
            _single_record_batch(
                producer_id=producer_id_a,
                producer_epoch=0,
                base_sequence=0,
            ),
        )
        req_b = _produce_req(
            "idempotent_concurrent_sessions_topic",
            0,
            _single_record_batch(
                producer_id=producer_id_b,
                producer_epoch=0,
                base_sequence=0,
            ),
        )

        response_a, _ = await _run_produce(config, req_a, expected_queue_items=1)
        response_b, _ = await _run_produce(config, req_b, expected_queue_items=1)

        assert response_a.responses[0].partition_responses[0].error_code == ErrorCode.none
        assert response_b.responses[0].partition_responses[0].error_code == ErrorCode.none
        returned_offsets = sorted(
            [
                int(response_a.responses[0].partition_responses[0].base_offset),
                int(response_b.responses[0].partition_responses[0].base_offset),
            ]
        )
        assert returned_offsets == [0, 1]
        return

    assert config.engine is not None
    original_factory = config.async_session_factory
    config.async_session_factory = async_sessionmaker(
        bind=config.engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    try:
        async with config.async_session_factory() as session:
            session.add(Topic(name="idempotent_concurrent_sessions_topic"))
            session.add(
                Partition(
                    topic_name="idempotent_concurrent_sessions_topic",
                    partition_number=0,
                )
            )
            producer_session_a = ProducerSession(
                transactional_id="txn-concurrent-a",
                producer_epoch=0,
            )
            producer_session_b = ProducerSession(
                transactional_id="txn-concurrent-b",
                producer_epoch=0,
            )
            session.add(producer_session_a)
            await session.flush()
            session.add(producer_session_b)
            await session.commit()
            producer_id_a = producer_session_a.producer_id
            producer_id_b = producer_session_b.producer_id

        produce_queue: asyncio.Queue = asyncio.Queue()
        callback_a = AsyncMock()
        callback_b = AsyncMock()
        stop_acking = asyncio.Event()
        acked_items = []

        async def ack_items() -> None:
            while not stop_acking.is_set() or not produce_queue.empty():
                try:
                    item = await asyncio.wait_for(produce_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                acked_items.append(item)
                item.flush_result.set_result(True)

        req_a = _produce_req(
            "idempotent_concurrent_sessions_topic",
            0,
            _single_record_batch(
                producer_id=producer_id_a,
                producer_epoch=0,
                base_sequence=0,
            ),
        )
        req_b = _produce_req(
            "idempotent_concurrent_sessions_topic",
            0,
            _single_record_batch(
                producer_id=producer_id_b,
                producer_epoch=0,
                base_sequence=0,
            ),
        )

        ack_task = asyncio.create_task(ack_items())
        try:
            await asyncio.gather(
                do_handle_produce_request(
                    config=config,
                    produce_queue=produce_queue,
                    req=req_a,
                    api_version=8,
                    callback=callback_a,
                ),
                do_handle_produce_request(
                    config=config,
                    produce_queue=produce_queue,
                    req=req_b,
                    api_version=8,
                    callback=callback_b,
                ),
            )
        finally:
            stop_acking.set()
            with suppress(asyncio.CancelledError):
                await ack_task

        response_a = callback_a.await_args.args[0]
        response_b = callback_b.await_args.args[0]

        assert response_a.responses[0].partition_responses[0].error_code == ErrorCode.none
        assert response_b.responses[0].partition_responses[0].error_code == ErrorCode.none

        returned_offsets = sorted(
            [
                int(response_a.responses[0].partition_responses[0].base_offset),
                int(response_b.responses[0].partition_responses[0].base_offset),
            ]
        )
        assert returned_offsets == [0, 1]
        assert len(acked_items) == 2

        async with config.async_session_factory() as session:
            partition = (
                await session.execute(
                    select(Partition).where(
                        Partition.topic_name == "idempotent_concurrent_sessions_topic",
                        Partition.partition_number == 0,
                    )
                )
            ).scalar_one()
            state_a = (
                await session.execute(
                    select(ProducerPartitionState).where(
                        ProducerPartitionState.producer_id == producer_id_a,
                        ProducerPartitionState.producer_epoch == 0,
                        ProducerPartitionState.topic_name
                        == "idempotent_concurrent_sessions_topic",
                        ProducerPartitionState.partition_number == 0,
                    )
                )
            ).scalar_one()
            state_b = (
                await session.execute(
                    select(ProducerPartitionState).where(
                        ProducerPartitionState.producer_id == producer_id_b,
                        ProducerPartitionState.producer_epoch == 0,
                        ProducerPartitionState.topic_name
                        == "idempotent_concurrent_sessions_topic",
                        ProducerPartitionState.partition_number == 0,
                    )
                )
            ).scalar_one()

        assert partition.last_offset == 1
        assert state_a.next_expected_sequence == 1
        assert state_b.next_expected_sequence == 1
    finally:
        config.async_session_factory = original_factory
