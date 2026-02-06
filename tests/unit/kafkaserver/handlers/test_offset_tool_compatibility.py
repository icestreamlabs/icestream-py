import datetime
import struct
from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from kio.schema.fetch.v11.request import (
    FetchPartition as FetchPartitionV11,
    FetchRequest as FetchRequestV11,
    FetchTopic as FetchTopicV11,
)
from kio.schema.list_offsets.v9.request import (
    ListOffsetsPartition,
    ListOffsetsRequest,
    ListOffsetsTopic,
)
from kio.schema.types import BrokerId, TopicName
from kio.static.primitive import i8, i32, i32Timedelta, i64

from icestream.config import Config
from icestream.kafkaserver.handlers.fetch import do_fetch
from icestream.kafkaserver.handlers.list_offsets import do_list_offsets
from icestream.kafkaserver.handlers.offset_commit import do_offset_commit
from icestream.kafkaserver.handlers.offset_fetch import do_offset_fetch
from icestream.kafkaserver.internal_offsets import (
    encode_offset_key,
    internal_offsets_partitions,
)
from icestream.kafkaserver.internal_topics import (
    INTERNAL_OFFSETS_TOPIC,
    ensure_internal_topics,
    partition_for_key,
)
from icestream.kafkaserver.protocol import KafkaRecordBatch, decode_kafka_records
from icestream.models import Partition
from icestream.models.consumer_groups import ConsumerGroup, GroupMember
from tests.unit.conftest import insert_topic_partition

EARLIEST = -2
LATEST = -1


async def _create_group(
    session,
    *,
    group_id: str,
    generation: int = 1,
    state: str = "Stable",
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type="consumer",
        selected_protocol="range",
    )
    session.add(group)
    await session.flush()
    return group


async def _create_member(
    session,
    *,
    group: ConsumerGroup,
    member_id: str,
    generation: int,
) -> GroupMember:
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=datetime.datetime.now(datetime.timezone.utc),
        member_generation=generation,
        join_generation=generation,
        is_in_sync=True,
    )
    session.add(member)
    await session.flush()
    return member


def _mk_list_offsets_req(topic: str, partition: int, ts: int) -> ListOffsetsRequest:
    return ListOffsetsRequest(
        replica_id=BrokerId(-1),
        topics=(
            ListOffsetsTopic(
                name=TopicName(topic),
                partitions=(ListOffsetsPartition(partition_index=i32(partition), timestamp=i64(ts)),),
            ),
        ),
        isolation_level=i8(0),
    )


def _mk_fetch_req(topic: str, partition: int, fetch_offset: int) -> FetchRequestV11:
    return FetchRequestV11(
        replica_id=BrokerId(-1),
        max_wait=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        min_bytes=i32(0),
        max_bytes=i32(1024 * 1024),
        isolation_level=i8(0),
        session_id=i32(0),
        session_epoch=i32(-1),
        topics=(
            FetchTopicV11(
                topic=TopicName(topic),
                partitions=(
                    FetchPartitionV11(
                        partition=i32(partition),
                        current_leader_epoch=i32(-1),
                        fetch_offset=i64(fetch_offset),
                        log_start_offset=i64(-1),
                        partition_max_bytes=i32(1024 * 1024),
                    ),
                ),
            ),
        ),
        forgotten_topics_data=tuple(),
        rack_id="",
    )


def _decode_records(records_blob: bytes) -> list[tuple[int, bytes | None, bytes | None]]:
    out: list[tuple[int, bytes | None, bytes | None]] = []
    pos = 0
    while pos + 12 <= len(records_blob):
        base_offset = struct.unpack_from(">q", records_blob, pos)[0]
        batch_len = struct.unpack_from(">i", records_blob, pos + 8)[0]
        total = 12 + batch_len
        if pos + total > len(records_blob):
            break
        batch = KafkaRecordBatch.from_bytes(records_blob[pos : pos + total])
        records = decode_kafka_records(batch.records)
        for record in records:
            out.append((int(base_offset) + int(record.offset_delta), record.key, record.value))
        pos += total
    return out


def _topic_responses(resp):
    topics = getattr(resp, "topics", None)
    if topics is not None:
        return topics

    responses = getattr(resp, "responses", None)
    if responses is not None:
        return responses

    groups = getattr(resp, "groups", ())
    if groups:
        group = groups[0]
        return getattr(group, "topics", getattr(group, "responses", ()))
    return ()


def _partition_responses(topic_resp):
    return getattr(topic_resp, "partitions", getattr(topic_resp, "partition_responses", ()))


def _partition_id(partition_resp) -> int:
    if hasattr(partition_resp, "partition_index"):
        return int(partition_resp.partition_index)
    if hasattr(partition_resp, "partition"):
        return int(partition_resp.partition)
    return int(getattr(partition_resp, "partition_id"))


def _partition_offset(partition_resp) -> int:
    if hasattr(partition_resp, "committed_offset"):
        return int(partition_resp.committed_offset)
    return int(getattr(partition_resp, "offset"))


@pytest.mark.asyncio
async def test_kafka_console_consumer_style_read_from_internal_offsets(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(session, "compat-console-topic", 0)
            group = await _create_group(session, group_id="cg-console-tool")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-console-tool",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="compat-console-topic",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=11),),
            ),
        ),
    )
    commit_resp = await do_offset_commit(config, commit_req, api_version=9)
    commit_partition = _partition_responses(_topic_responses(commit_resp)[0])[0]
    assert commit_partition.error_code == ErrorCode.none

    key = encode_offset_key("cg-console-tool", "compat-console-topic", 0)
    internal_partition = partition_for_key(key, internal_offsets_partitions(config))

    earliest_resp = await do_list_offsets(
        config,
        _mk_list_offsets_req(INTERNAL_OFFSETS_TOPIC, internal_partition, EARLIEST),
        api_version=9,
    )
    earliest = int(earliest_resp.topics[0].partitions[0].offset)

    fetch_resp = await do_fetch(
        config,
        _mk_fetch_req(INTERNAL_OFFSETS_TOPIC, internal_partition, earliest),
        api_version=11,
    )
    partition_resp = fetch_resp.responses[0].partitions[0]
    assert partition_resp.error_code == ErrorCode.none

    decoded = _decode_records(bytes(partition_resp.records))
    assert decoded
    assert any(record_key == key for _, record_key, _ in decoded)
    assert any(record_value is not None for _, _, record_value in decoded)


@pytest.mark.asyncio
async def test_kafka_consumer_groups_describe_style_lag(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(
                session, "compat-groups-topic", 0, last_offset=120, log_start_offset=0
            )
            group = await _create_group(session, group_id="cg-groups-tool")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-groups-tool",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="compat-groups-topic",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=100),),
            ),
        ),
    )
    await do_offset_commit(config, commit_req, api_version=9)

    fetch_req = SimpleNamespace(
        group_id="cg-groups-tool",
        topics=(
            SimpleNamespace(name="compat-groups-topic", partitions=(SimpleNamespace(partition_index=0),)),
        ),
    )
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=9)
    committed = _partition_offset(_partition_responses(_topic_responses(fetch_resp)[0])[0])
    assert committed == 100

    latest_resp = await do_list_offsets(
        config,
        _mk_list_offsets_req("compat-groups-topic", 0, LATEST),
        api_version=9,
    )
    log_end = int(latest_resp.topics[0].partitions[0].offset)
    lag = log_end - committed
    assert lag == 21
    assert lag >= 0


@pytest.mark.asyncio
async def test_burrow_style_monitoring_handles_missing_commit(config: Config) -> None:
    await ensure_internal_topics(config)

    async with config.async_session_factory() as session:
        async with session.begin():
            await insert_topic_partition(
                session, "compat-burrow-topic", 0, last_offset=50, log_start_offset=0
            )
            session.add(
                Partition(
                    topic_name="compat-burrow-topic",
                    partition_number=1,
                    last_offset=35,
                    log_start_offset=0,
                )
            )
            await session.flush()
            group = await _create_group(session, group_id="cg-burrow-tool")
            await _create_member(session, group=group, member_id="member-1", generation=1)

    commit_req = SimpleNamespace(
        group_id="cg-burrow-tool",
        generation_id=1,
        member_id="member-1",
        topics=(
            SimpleNamespace(
                name="compat-burrow-topic",
                partitions=(SimpleNamespace(partition_index=0, committed_offset=40),),
            ),
        ),
    )
    await do_offset_commit(config, commit_req, api_version=9)

    fetch_req = SimpleNamespace(
        group_id="cg-burrow-tool",
        topics=(
            SimpleNamespace(
                name="compat-burrow-topic",
                partitions=(
                    SimpleNamespace(partition_index=0),
                    SimpleNamespace(partition_index=1),
                ),
            ),
        ),
    )
    fetch_resp = await do_offset_fetch(config, fetch_req, api_version=9)
    partitions = _partition_responses(_topic_responses(fetch_resp)[0])
    committed_by_partition = {_partition_id(p): _partition_offset(p) for p in partitions}
    assert committed_by_partition[0] == 40
    assert committed_by_partition[1] == -1

    latest_p0 = await do_list_offsets(
        config,
        _mk_list_offsets_req("compat-burrow-topic", 0, LATEST),
        api_version=9,
    )
    latest_p1 = await do_list_offsets(
        config,
        _mk_list_offsets_req("compat-burrow-topic", 1, LATEST),
        api_version=9,
    )
    log_end_p0 = int(latest_p0.topics[0].partitions[0].offset)
    log_end_p1 = int(latest_p1.topics[0].partitions[0].offset)

    lag_p0 = log_end_p0 - committed_by_partition[0]
    lag_p1 = None if committed_by_partition[1] < 0 else log_end_p1 - committed_by_partition[1]

    assert lag_p0 == 11
    assert lag_p1 is None
