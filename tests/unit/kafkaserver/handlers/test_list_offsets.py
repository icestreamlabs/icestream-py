import datetime
import pytest
from kio.schema.errors import ErrorCode
from kio.schema.list_offsets.v9 import ListOffsetsResponse
from kio.schema.list_offsets.v9.request import ListOffsetsRequest, ListOffsetsTopic, ListOffsetsPartition
from kio.schema.types import TopicName, BrokerId
from kio.static.primitive import i32, i64, i8

from icestream.kafkaserver.handlers.list_offsets import do_list_offsets

EARLIEST = -2
LATEST = -1


def mk_list_offsets_v9_req(topic: str, partition: int, ts: int) -> ListOffsetsRequest:
    partitions = [
        ListOffsetsPartition(
            partition_index=i32(partition),
            timestamp=i64(ts)
        )
    ]
    topics = [
        ListOffsetsTopic(
            name=TopicName(topic),
            partitions=tuple(partitions)
        )
    ]
    return ListOffsetsRequest(
        replica_id=BrokerId(-1),
        topics=tuple(topics),
        isolation_level=i8(0)
    )

def mk_req_multi(items: list[tuple[str, int, int]]) -> ListOffsetsRequest:
    by_topic: dict[str, list[ListOffsetsPartition]] = {}
    for topic, partition, ts in items:
        by_topic.setdefault(topic, []).append(
            ListOffsetsPartition(
                partition_index=i32(partition),
                timestamp=i64(ts),
            )
        )
    topics = tuple(
        ListOffsetsTopic(
            name=TopicName(t),
            partitions=tuple(parts),
        )
        for t, parts in by_topic.items()
    )
    return ListOffsetsRequest(
        replica_id=BrokerId(-1),
        topics=topics,
        isolation_level=i8(0),
    )


def _resp_map(resp: ListOffsetsResponse):
    t = resp.topics[0]
    p = t.partitions[0]
    return p.error_code, p.timestamp, p.offset, p.leader_epoch

def _index_response(resp: ListOffsetsResponse) -> dict[tuple[str, int, int], tuple[int, int]]:
    out = {}
    for t in resp.topics:
        for p in t.partitions:
            out[(t.name, p.partition_index, p.timestamp)] = (p.error_code, p.offset)
    return out

def index_by_request(req: ListOffsetsRequest, resp: ListOffsetsResponse
) -> dict[tuple[str, int, int], tuple[int, int]]:
    out: dict[tuple[str, int, int], tuple[int, int]] = {}

    for t_req, t_resp in zip(req.topics, resp.topics):
        assert t_req.name == t_resp.name
        for p_req, p_resp in zip(t_req.partitions, t_resp.partitions):
            key = (t_req.name, int(p_req.partition_index), int(p_req.timestamp))
            out[key] = (int(p_resp.error_code), int(p_resp.offset))
    return out


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "topic_key, expected_log_start",
    [
        ("mixed", 0),
        ("compacted_only", 0),
        ("wal_only", 0),
        ("empty", 0),
    ],
    ids=["mixed", "compacted_only", "wal_only", "empty"],
)
async def test_earliest_returns_log_start_offset(config, seeded_topics, topic_key, expected_log_start):
    topic = seeded_topics[topic_key]["topic"]
    partition = seeded_topics[topic_key]["partition"]

    req = mk_list_offsets_v9_req(topic, partition, EARLIEST)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.none
    assert ts == EARLIEST
    assert off == expected_log_start


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "topic_key, expected_log_end",
    [
        ("mixed", 40),           # last_offset=39 -> logEnd=40
        ("compacted_only", 50),  # last_offset=49 -> logEnd=50
        ("wal_only", 30),        # last_offset=29 -> logEnd=30
        ("empty", 0),            # last_offset=-1 -> logEnd=0
    ],
    ids=["mixed", "compacted_only", "wal_only", "empty"],
)
async def test_latest_returns_log_end_offset(config, seeded_topics, topic_key, expected_log_end):
    topic = seeded_topics[topic_key]["topic"]
    partition = seeded_topics[topic_key]["partition"]

    req = mk_list_offsets_v9_req(topic, partition, LATEST)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.none
    assert ts == LATEST
    assert off == expected_log_end

@pytest.mark.asyncio
async def test_timestamp_inside_parquet_range_returns_first_ge_offset(config, seeded_topics):
    topic = seeded_topics["mixed"]["topic"]
    partition = seeded_topics["mixed"]["partition"]
    base_time = seeded_topics["mixed"]["base_time"]

    target_ts = int((base_time + datetime.timedelta(seconds=5)).timestamp() * 1000)

    req = mk_list_offsets_v9_req(topic, partition, target_ts)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.none
    assert ts == target_ts
    assert off == 5

@pytest.mark.asyncio
async def test_timestamp_between_adjacent_files_selects_boundary(config, seeded_topics):
    topic = seeded_topics["mixed"]["topic"]
    partition = seeded_topics["mixed"]["partition"]
    base_time = seeded_topics["mixed"]["base_time"]

    target_ts = int((base_time + datetime.timedelta(seconds=30)).timestamp() * 1000)
    expected_ts = int((base_time + datetime.timedelta(seconds=40)).timestamp() * 1000)  # ts of offset 20

    req = mk_list_offsets_v9_req(topic, partition, target_ts)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.none
    assert ts == expected_ts
    assert off == 20  # first offset of the next Parquet range

@pytest.mark.asyncio
async def test_timestamp_after_max_returns_log_end_offset(config, seeded_topics):
    topic = seeded_topics["mixed"]["topic"]
    partition = seeded_topics["mixed"]["partition"]
    base_time = seeded_topics["mixed"]["base_time"]

    target_ts = int((base_time + datetime.timedelta(hours=1)).timestamp() * 1000)
    last_record_ts = int((base_time + datetime.timedelta(seconds=59)).timestamp() * 1000)

    req = mk_list_offsets_v9_req(topic, partition, target_ts)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)

    assert ec == ErrorCode.none
    assert off == 40  # log_end_offset = last_offset + 1

    assert ts in (last_record_ts, target_ts) or ts >= last_record_ts

@pytest.mark.asyncio
async def test_unknown_topic_or_partition_returns_error(config):
    req = mk_list_offsets_v9_req("no_such_topic", 0, LATEST)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.unknown_topic_or_partition
    assert ts == LATEST
    assert off in (-1, 0)

@pytest.mark.asyncio
async def test_no_data_partition_latest_and_earliest(config, seeded_topics):
    topic = seeded_topics["empty"]["topic"]
    partition = seeded_topics["empty"]["partition"]

    req = mk_req_multi([
        (topic, partition, EARLIEST),
        (topic, partition, LATEST),
    ])
    resp = await do_list_offsets(config, req, api_version=9)
    idx = _index_response(resp)

    assert idx[(topic, partition, EARLIEST)] == (ErrorCode.none, 0)
    assert idx[(topic, partition, LATEST)]   == (ErrorCode.none, 0)

@pytest.mark.asyncio
async def test_uses_walfileoffset_when_no_parquet(config, seeded_topics):
    topic = seeded_topics["wal_only"]["topic"]
    partition = seeded_topics["wal_only"]["partition"]
    base_time = seeded_topics["wal_only"]["base_time"]

    target_ts = int((base_time + datetime.timedelta(seconds=25)).timestamp() * 1000)
    expected_ts = int((base_time + datetime.timedelta(seconds=30)).timestamp() * 1000)

    req = mk_list_offsets_v9_req(topic, partition, target_ts)
    resp = await do_list_offsets(config, req, api_version=9)

    ec, ts, off, _ = _resp_map(resp)
    assert ec == ErrorCode.none
    assert ts == expected_ts
    assert off == 15  # start of next wal file offset range

@pytest.mark.asyncio
async def test_multiple_topics_and_partitions(config, seeded_topics):
    mixed = seeded_topics["mixed"]
    cmpct = seeded_topics["compacted_only"]
    wal   = seeded_topics["wal_only"]

    items = [
        (mixed["topic"], mixed["partition"], LATEST),
        (cmpct["topic"], cmpct["partition"], EARLIEST),
        (mixed["topic"], mixed["partition"], int((mixed["base_time"] + datetime.timedelta(seconds=15)).timestamp() * 1000)),
        (wal["topic"], wal["partition"], int((wal["base_time"] + datetime.timedelta(seconds=12)).timestamp() * 1000)),
        (wal["topic"], wal["partition"], int((wal["base_time"] + datetime.timedelta(seconds=25)).timestamp() * 1000)),
    ]
    req = mk_req_multi(items)
    resp = await do_list_offsets(config, req, api_version=9)
    print(resp)

    idx = _index_response(resp)
    assert idx[(mixed["topic"], mixed["partition"], LATEST)] == (ErrorCode.none, 40)
    assert idx[(cmpct["topic"], cmpct["partition"], EARLIEST)] == (ErrorCode.none, 0)

    t_mid = int((mixed["base_time"] + datetime.timedelta(seconds=15)).timestamp() * 1000)
    assert idx[(mixed["topic"], mixed["partition"], t_mid)] == (ErrorCode.none, 15)

    t_wal_mid = int((wal["base_time"] + datetime.timedelta(seconds=12)).timestamp() * 1000)
    assert idx[(wal["topic"], wal["partition"], t_wal_mid)] == (ErrorCode.none, 12)

    t_wal_gap = int((wal["base_time"] + datetime.timedelta(seconds=30)).timestamp() * 1000)
    assert idx[(wal["topic"], wal["partition"], t_wal_gap)] == (ErrorCode.none, 15)
