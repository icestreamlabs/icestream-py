import asyncio
import datetime
import io
import socket
import struct
import time
import uuid
from contextlib import suppress

import httpx
import pytest
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from kio.schema.errors import ErrorCode
from kio.schema.delete_topics.v6.request import (
    DeleteTopicState,
    DeleteTopicsRequest as DeleteTopicsRequestV6,
    RequestHeader as DeleteTopicsRequestHeaderV6,
)
from kio.schema.delete_topics.v6.response import (
    DeleteTopicsResponse as DeleteTopicsResponseV6,
)
from kio.schema.metadata.v1.request import (
    MetadataRequest as MetadataRequestV1,
    MetadataRequestTopic as MetadataRequestTopicV1,
    RequestHeader as MetadataRequestHeaderV1,
)
from kio.schema.metadata.v1.response import MetadataResponse as MetadataResponseV1
from kio.schema.init_producer_id.v5.request import (
    InitProducerIdRequest as InitProducerIdRequestV5,
    RequestHeader as InitProducerIdRequestHeaderV5,
)
from kio.schema.init_producer_id.v5.response import (
    InitProducerIdResponse as InitProducerIdResponseV5,
)
from kio.schema.produce.v8.request import (
    ProduceRequest as ProduceRequestV8,
    RequestHeader as ProduceRequestHeaderV8,
    TopicProduceData as ProduceTopicDataV8,
    PartitionProduceData as ProducePartitionDataV8,
)
from kio.schema.produce.v8.response import ProduceResponse as ProduceResponseV8
from kio.schema.types import TopicName
from kio.serial import entity_reader, entity_writer
from kio.static.primitive import i16, i32, i32Timedelta, i64
from sqlalchemy import func, select

from icestream.__main__ import run as run_icestream
from icestream.config import Config
from icestream.kafkaserver.protocol import KafkaRecord, KafkaRecordBatch
from icestream.models import Partition, Topic, TopicWALFile, WALFile, WALFileOffset

TEST_TOPIC = "test_topic"


async def _delete_topic_via_wire(bootstrap_servers: str, topic: str) -> None:
    host, port_text = bootstrap_servers.split(":")
    port = int(port_text)
    reader, writer = await asyncio.open_connection(host, port)
    try:
        header = DeleteTopicsRequestHeaderV6(
            request_api_key=i16(20),
            request_api_version=i16(6),
            correlation_id=i32(777),
            client_id="integration-delete",
        )
        request = DeleteTopicsRequestV6(
            topics=(DeleteTopicState(name=topic, topic_id=None),),
            timeout=i32Timedelta.parse(datetime.timedelta(seconds=5)),
        )

        request_header_writer = entity_writer(DeleteTopicsRequestHeaderV6)
        request_writer = entity_writer(DeleteTopicsRequestV6)
        request_buf = io.BytesIO()
        request_header_writer(request_buf, header)
        request_writer(request_buf, request)
        payload = request_buf.getvalue()

        writer.write(struct.pack(">i", len(payload)) + payload)
        await writer.drain()

        resp_len = struct.unpack(">i", await reader.readexactly(4))[0]
        resp_payload = io.BytesIO(await reader.readexactly(resp_len))
        response_header_reader = entity_reader(DeleteTopicsResponseV6.__header_schema__)
        response_reader = entity_reader(DeleteTopicsResponseV6)
        _ = response_header_reader(resp_payload)
        response = response_reader(resp_payload)
        assert len(response.responses) == 1
        assert response.responses[0].error_code == ErrorCode.none
    finally:
        writer.close()
        await writer.wait_closed()


async def _metadata_topic_error_code_via_wire(bootstrap_servers: str, topic: str):
    host, port_text = bootstrap_servers.split(":")
    port = int(port_text)
    reader, writer = await asyncio.open_connection(host, port)
    try:
        header = MetadataRequestHeaderV1(
            request_api_key=i16(3),
            request_api_version=i16(1),
            correlation_id=i32(778),
            client_id="integration-metadata",
        )
        request = MetadataRequestV1(
            topics=(MetadataRequestTopicV1(name=topic),),
        )

        header_writer = entity_writer(MetadataRequestHeaderV1)
        request_writer = entity_writer(MetadataRequestV1)
        request_buf = io.BytesIO()
        header_writer(request_buf, header)
        request_writer(request_buf, request)
        payload = request_buf.getvalue()

        writer.write(struct.pack(">i", len(payload)) + payload)
        await writer.drain()

        resp_len = struct.unpack(">i", await reader.readexactly(4))[0]
        resp_payload = io.BytesIO(await reader.readexactly(resp_len))
        response_header_reader = entity_reader(MetadataResponseV1.__header_schema__)
        response_reader = entity_reader(MetadataResponseV1)
        _ = response_header_reader(resp_payload)
        response = response_reader(resp_payload)
        if len(response.topics) == 0:
            return None
        return response.topics[0].topic_error_code
    finally:
        writer.close()
        await writer.wait_closed()


def _reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def _wait_for_port(
    port: int,
    *,
    host: str = "127.0.0.1",
    timeout: float = 10.0,
    startup_task: asyncio.Task | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if startup_task is not None and startup_task.done():
            exc = startup_task.exception()
            if exc is not None:
                raise RuntimeError("icestream exited before becoming ready") from exc
            raise RuntimeError("icestream exited before becoming ready")
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError:
            await asyncio.sleep(0.1)
            continue
        writer.close()
        await writer.wait_closed()
        return
    raise TimeoutError(f"Server not available on {host}:{port}")


async def _start_manual_icestream(*, admin_port: int, kafka_port: int) -> asyncio.Task:
    server_task = asyncio.create_task(run_icestream())
    await asyncio.gather(
        _wait_for_port(admin_port, startup_task=server_task),
        _wait_for_port(kafka_port, startup_task=server_task),
    )
    return server_task


async def _stop_manual_icestream(server_task: asyncio.Task) -> None:
    server_task.cancel()
    with suppress(asyncio.CancelledError):
        await server_task


async def _send_kafka_wire_request(
    *,
    bootstrap_servers: str,
    request_header,
    request_payload,
    response_cls,
):
    host, port_text = bootstrap_servers.split(":")
    port = int(port_text)
    reader, writer = await asyncio.open_connection(host, port)
    try:
        header_writer = entity_writer(type(request_header))
        request_writer = entity_writer(type(request_payload))
        request_buf = io.BytesIO()
        header_writer(request_buf, request_header)
        request_writer(request_buf, request_payload)
        payload = request_buf.getvalue()

        writer.write(struct.pack(">i", len(payload)) + payload)
        await writer.drain()

        resp_len = struct.unpack(">i", await reader.readexactly(4))[0]
        resp_payload = io.BytesIO(await reader.readexactly(resp_len))
        response_header_reader = entity_reader(response_cls.__header_schema__)
        response_reader = entity_reader(response_cls)
        _ = response_header_reader(resp_payload)
        return response_reader(resp_payload)
    finally:
        writer.close()
        await writer.wait_closed()


def _single_record_batch_bytes(
    *,
    value: bytes,
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
) -> bytes:
    record = KafkaRecord(
        attributes=0,
        timestamp_delta=0,
        offset_delta=0,
        key=b"k",
        value=value,
        headers=[],
    )
    batch = KafkaRecordBatch.from_records(offset=0, records=[record])
    batch.producer_id = producer_id
    batch.producer_epoch = producer_epoch
    batch.base_sequence = base_sequence
    return batch.to_bytes()


async def _init_producer_id_via_wire(
    *,
    bootstrap_servers: str,
    transactional_id: str,
) -> tuple[int, int]:
    response = await _send_kafka_wire_request(
        bootstrap_servers=bootstrap_servers,
        request_header=InitProducerIdRequestHeaderV5(
            request_api_key=i16(22),
            request_api_version=i16(5),
            correlation_id=i32(801),
            client_id="integration-init-producer",
        ),
        request_payload=InitProducerIdRequestV5(
            transactional_id=transactional_id,
            transaction_timeout=i32Timedelta.parse(datetime.timedelta(seconds=30)),
            producer_id=i64(-1),
            producer_epoch=i16(-1),
        ),
        response_cls=InitProducerIdResponseV5,
    )
    assert response.error_code == ErrorCode.none
    return int(response.producer_id), int(response.producer_epoch)


async def _produce_via_wire(
    *,
    bootstrap_servers: str,
    topic: str,
    partition: int,
    records: bytes,
    correlation_id: int = 802,
) -> tuple[ErrorCode, int]:
    response = await _send_kafka_wire_request(
        bootstrap_servers=bootstrap_servers,
        request_header=ProduceRequestHeaderV8(
            request_api_key=i16(0),
            request_api_version=i16(8),
            correlation_id=i32(correlation_id),
            client_id="integration-produce",
        ),
        request_payload=ProduceRequestV8(
            transactional_id=None,
            acks=i16(1),
            timeout=i32Timedelta.parse(datetime.timedelta(seconds=5)),
            topic_data=(
                ProduceTopicDataV8(
                    name=TopicName(topic),
                    partition_data=(
                        ProducePartitionDataV8(index=i32(partition), records=records),
                    ),
                ),
            ),
        ),
        response_cls=ProduceResponseV8,
    )
    partition_resp = response.responses[0].partition_responses[0]
    return partition_resp.error_code, int(partition_resp.base_offset)


async def _load_partition_last_offset(topic: str, partition: int) -> int:
    cfg = Config()
    assert cfg.async_session_factory is not None
    try:
        async with cfg.async_session_factory() as session:
            row = (
                await session.execute(
                    select(Partition).where(
                        Partition.topic_name == topic,
                        Partition.partition_number == partition,
                    )
                )
            ).scalar_one()
            return int(row.last_offset)
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()


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


@pytest.mark.asyncio
async def test_topic_lifecycle_create_produce_delete_not_fetchable(bootstrap_servers):
    topic = f"topic_lifecycle_{uuid.uuid4().hex[:8]}"

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        await producer.start()
        try:
            await producer.send_and_wait(topic, b"lifecycle-message")
        finally:
            await producer.stop()

        await _delete_topic_via_wire(bootstrap_servers, topic)

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if topic not in await admin.list_topics():
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("topic did not disappear from metadata after deletion")
    finally:
        await admin.close()

    cfg = Config()
    assert cfg.async_session_factory is not None
    try:
        async with cfg.async_session_factory() as session:
            topic_row = (
                await session.execute(select(Topic).where(Topic.name == topic))
            ).scalar_one_or_none()
            partition_row = (
                await session.execute(
                    select(Partition).where(Partition.topic_name == topic)
                )
            ).scalar_one_or_none()

        assert topic_row is None
        assert partition_row is None

        topic_error_code = await _metadata_topic_error_code_via_wire(
            bootstrap_servers, topic
        )
        assert topic_error_code in (None, ErrorCode.unknown_topic_or_partition)
    finally:
        if cfg.engine is not None:
            await cfg.engine.dispose()


@pytest.mark.asyncio
async def test_idempotent_retry_survives_broker_restart_without_offset_advance(
    monkeypatch,
):
    admin_port = _reserve_tcp_port()
    kafka_port = _reserve_tcp_port()
    bootstrap_servers = f"127.0.0.1:{kafka_port}"
    topic = f"idempotent_restart_{uuid.uuid4().hex[:8]}"
    transactional_id = f"txn-restart-{uuid.uuid4().hex[:8]}"

    monkeypatch.setenv("ICESTREAM_ENABLE_COMPACTION", "false")
    monkeypatch.setenv("ICESTREAM_ADMIN_PORT", str(admin_port))
    monkeypatch.setenv("ICESTREAM_PORT", str(kafka_port))
    monkeypatch.setenv("ICESTREAM_ADVERTISED_HOST", "127.0.0.1")
    monkeypatch.setenv("ICESTREAM_ADVERTISED_PORT", str(kafka_port))

    first_server = await _start_manual_icestream(admin_port=admin_port, kafka_port=kafka_port)
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{admin_port}") as client:
            create_resp = await client.post(
                "/topics",
                json={"name": topic, "num_partitions": 1},
            )
            assert create_resp.status_code in (200, 201)

        producer_id, producer_epoch = await _init_producer_id_via_wire(
            bootstrap_servers=bootstrap_servers,
            transactional_id=transactional_id,
        )
        first_error, first_offset = await _produce_via_wire(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            partition=0,
            records=_single_record_batch_bytes(
                value=b"restart-first",
                producer_id=producer_id,
                producer_epoch=producer_epoch,
                base_sequence=0,
            ),
            correlation_id=901,
        )
        assert first_error == ErrorCode.none
        assert first_offset == 0
    finally:
        await _stop_manual_icestream(first_server)

    second_server = await _start_manual_icestream(admin_port=admin_port, kafka_port=kafka_port)
    try:
        retry_error, retry_offset = await _produce_via_wire(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            partition=0,
            records=_single_record_batch_bytes(
                value=b"restart-first",
                producer_id=producer_id,
                producer_epoch=producer_epoch,
                base_sequence=0,
            ),
            correlation_id=902,
        )
        assert retry_error == ErrorCode.none
        assert retry_offset == first_offset
        assert await _load_partition_last_offset(topic, 0) == first_offset
    finally:
        await _stop_manual_icestream(second_server)


@pytest.mark.asyncio
async def test_idempotent_mixed_partition_workloads_keep_sequence_state_independent(
    http_client,
    bootstrap_servers,
):
    topic = f"idempotent_multi_partition_{uuid.uuid4().hex[:8]}"
    create_resp = await http_client.post(
        "/topics",
        json={"name": topic, "num_partitions": 2},
    )
    assert create_resp.status_code in (200, 201, 400)

    producer_id, producer_epoch = await _init_producer_id_via_wire(
        bootstrap_servers=bootstrap_servers,
        transactional_id=f"txn-mixed-{uuid.uuid4().hex[:8]}",
    )

    err_p0_seq0, off_p0_seq0 = await _produce_via_wire(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition=0,
        records=_single_record_batch_bytes(
            value=b"p0-0",
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=0,
        ),
        correlation_id=911,
    )
    err_p1_seq0, off_p1_seq0 = await _produce_via_wire(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition=1,
        records=_single_record_batch_bytes(
            value=b"p1-0",
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=0,
        ),
        correlation_id=912,
    )
    err_p0_seq1, off_p0_seq1 = await _produce_via_wire(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition=0,
        records=_single_record_batch_bytes(
            value=b"p0-1",
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=1,
        ),
        correlation_id=913,
    )
    err_p0_dup, off_p0_dup = await _produce_via_wire(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition=0,
        records=_single_record_batch_bytes(
            value=b"p0-0",
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=0,
        ),
        correlation_id=914,
    )

    assert err_p0_seq0 == ErrorCode.none
    assert err_p1_seq0 == ErrorCode.none
    assert err_p0_seq1 == ErrorCode.none
    assert err_p0_dup == ErrorCode.none
    assert off_p0_seq0 == 0
    assert off_p1_seq0 == 0
    assert off_p0_seq1 == 1
    assert off_p0_dup == 0

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        tp0 = TopicPartition(topic, 0)
        tp1 = TopicPartition(topic, 1)
        consumer.assign([tp0, tp1])
        await consumer.seek_to_beginning(tp0)
        await consumer.seek_to_beginning(tp1)

        fetched_p0: list[bytes] = []
        fetched_p1: list[bytes] = []
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and (len(fetched_p0) < 2 or len(fetched_p1) < 1):
            records_map = await consumer.getmany(timeout_ms=500, max_records=100)
            for tp, records in records_map.items():
                for record in records:
                    if tp.partition == 0:
                        fetched_p0.append(record.value)
                    elif tp.partition == 1:
                        fetched_p1.append(record.value)

        assert fetched_p0 == [b"p0-0", b"p0-1"]
        assert fetched_p1 == [b"p1-0"]
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_fetch_and_list_offsets_remain_correct_with_mixed_produce_modes(
    http_client,
    bootstrap_servers,
):
    topic = f"mixed_produce_modes_{uuid.uuid4().hex[:8]}"
    create_resp = await http_client.post(
        "/topics",
        json={"name": topic, "num_partitions": 1},
    )
    assert create_resp.status_code in (200, 201, 400)

    producer_id, producer_epoch = await _init_producer_id_via_wire(
        bootstrap_servers=bootstrap_servers,
        transactional_id=f"txn-mixed-modes-{uuid.uuid4().hex[:8]}",
    )
    idempotent_error, idempotent_offset = await _produce_via_wire(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partition=0,
        records=_single_record_batch_bytes(
            value=b"idempotent-0",
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=0,
        ),
        correlation_id=921,
    )
    assert idempotent_error == ErrorCode.none
    assert idempotent_offset == 0

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        metadata = await producer.send_and_wait(topic, b"non-idempotent-1")
        assert metadata.offset == 1
    finally:
        await producer.stop()

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
        while time.monotonic() < deadline and len(fetched) < 2:
            records_map = await consumer.getmany(timeout_ms=500, max_records=100)
            for records in records_map.values():
                for record in records:
                    fetched.append(record.value)

        assert fetched == [b"idempotent-0", b"non-idempotent-1"]

        begin_offsets = await consumer.beginning_offsets([tp])
        end_offsets = await consumer.end_offsets([tp])
        assert begin_offsets[tp] == 0
        assert end_offsets[tp] == 2
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_topic_lifecycle_delete_still_works_after_idempotent_produce(bootstrap_servers):
    topic = f"topic_lifecycle_idempotent_{uuid.uuid4().hex[:8]}"

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

        producer_id, producer_epoch = await _init_producer_id_via_wire(
            bootstrap_servers=bootstrap_servers,
            transactional_id=f"txn-lifecycle-{uuid.uuid4().hex[:8]}",
        )
        produce_error, produce_offset = await _produce_via_wire(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            partition=0,
            records=_single_record_batch_bytes(
                value=b"lifecycle-idempotent",
                producer_id=producer_id,
                producer_epoch=producer_epoch,
                base_sequence=0,
            ),
            correlation_id=931,
        )
        assert produce_error == ErrorCode.none
        assert produce_offset == 0

        await _delete_topic_via_wire(bootstrap_servers, topic)

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if topic not in await admin.list_topics():
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("topic did not disappear from metadata after deletion")
    finally:
        await admin.close()

    topic_error_code = await _metadata_topic_error_code_via_wire(bootstrap_servers, topic)
    assert topic_error_code in (None, ErrorCode.unknown_topic_or_partition)
