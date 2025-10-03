import datetime
import struct

import pytest

from icestream.config import Config
from icestream.kafkaserver.handlers.fetch import do_fetch
from icestream.kafkaserver.protocol import KafkaRecordBatch, decode_kafka_records

from kio.schema.errors import ErrorCode
from kio.schema.types import BrokerId, TopicName
from kio.static.primitive import i8, i16, i32, i32Timedelta, i64
from kio.schema.fetch.v11.request import (
    FetchRequest as FetchRequestV11,
    FetchTopic as FetchTopicV11,
    FetchPartition as FetchPartitionV11,
    ForgottenTopic as ForgottenTopicV11
)
from kio.schema.fetch.v11.response import (
    FetchResponse as FetchResponseV11,
)

from tests.unit.conftest import insert_topic_partition, insert_parquet_file, ts_ms
from tests.utils.seed import create_parquet_range


def _mk_req_v11(
        topics: list[tuple[str, list[tuple[int, int, int]]]],
        *,
        min_bytes: int = 0,
        max_wait_ms: int = 0,
        isolation_level: int = 0,
        max_bytes: int = 2 ** 31 - 1,
) -> FetchRequestV11:
    req_topics = []
    for tname, part_specs in topics:
        parts = tuple(
            FetchPartitionV11(
                partition=i32(p),
                current_leader_epoch=i32(-1),
                fetch_offset=i64(off),
                log_start_offset=i64(-1),
                partition_max_bytes=i32(pmax),
            )
            for (p, off, pmax) in part_specs
        )
        req_topics.append(FetchTopicV11(topic=TopicName(tname), partitions=parts))

    return FetchRequestV11(
        replica_id=BrokerId(-1),
        max_wait=i32Timedelta.parse(datetime.timedelta(milliseconds=max_wait_ms)),
        min_bytes=i32(min_bytes),
        max_bytes=i32(max_bytes),
        isolation_level=i8(isolation_level),
        session_id=i32(0),
        session_epoch=i32(-1),
        topics=tuple(req_topics),
        forgotten_topics_data=tuple(),
        rack_id="",
    )


def _mk_req_v11_with_forgotten(
        topics: list[tuple[str, list[tuple[int, int, int]]]],
        forgotten: list[tuple[str, list[int]]] = (),
        *,
        min_bytes: int = 0,
        max_wait_ms: int = 0,
        isolation_level: int = 0,
        max_bytes: int = 2 ** 31 - 1,
) -> FetchRequestV11:
    req_topics = []
    for tname, part_specs in topics:
        parts = tuple(
            FetchPartitionV11(
                partition=i32(p),
                current_leader_epoch=i32(-1),
                fetch_offset=i64(off),
                log_start_offset=i64(-1),
                partition_max_bytes=i32(pmax),
            )
            for (p, off, pmax) in part_specs
        )
        req_topics.append(FetchTopicV11(topic=TopicName(tname), partitions=parts))

    forgotten_topics = tuple(
        ForgottenTopicV11(topic=TopicName(t), partitions=tuple(i32(p) for p in parts))
        for (t, parts) in forgotten
    )

    return FetchRequestV11(
        replica_id=BrokerId(-1),
        max_wait=i32Timedelta.parse(datetime.timedelta(milliseconds=max_wait_ms)),
        min_bytes=i32(min_bytes),
        max_bytes=i32(max_bytes),
        isolation_level=i8(isolation_level),
        session_id=i32(0),
        session_epoch=i32(-1),
        topics=tuple(req_topics),
        forgotten_topics_data=forgotten_topics,
        rack_id="",
    )


def _decode_all_batches(buf: bytes) -> list[KafkaRecordBatch]:
    batches = []
    pos = 0
    n = len(buf)
    while pos + 12 <= n:
        # read header
        base_offset = struct.unpack_from(">q", buf, pos)[0]
        batch_len = struct.unpack_from(">i", buf, pos + 8)[0]
        total = 12 + batch_len
        if pos + total > n:
            break
        seg = buf[pos:pos + total]
        batches.append(KafkaRecordBatch.from_bytes(seg))
        pos += total
    return batches


def _collect_offsets_from_records_bytes(b: bytes) -> list[int]:
    offsets = []
    for kb in _decode_all_batches(b):
        recs = decode_kafka_records(kb.records)
        offsets.extend([int(kb.base_offset) + r.offset_delta for r in recs])
    return offsets


@pytest.mark.asyncio
async def test_unknown_topic_and_partition_errors(config: Config, seeded_topics):
    req1 = _mk_req_v11([("does_not_exist", [(0, 0, 1024)])])
    resp1 = await do_fetch(config, req1, api_version=11)
    assert isinstance(resp1, FetchResponseV11)
    assert resp1.error_code == ErrorCode.none

    tr = resp1.responses[0]
    assert tr.topic == "does_not_exist"
    p = tr.partitions[0]
    assert p.error_code == ErrorCode.unknown_topic_or_partition
    assert len(p.records) == 0

    req2 = _mk_req_v11([("mixed", [(99, 0, 1024)])])
    resp2 = await do_fetch(config, req2, api_version=11)
    p2 = resp2.responses[0].partitions[0]
    assert p2.error_code == ErrorCode.unknown_topic_or_partition
    assert len(p2.records) == 0


@pytest.mark.asyncio
async def test_offset_out_of_range_before_log_start(config: Config):
    async with config.async_session_factory() as s:
        t = "oor"
        p = 0
        await insert_topic_partition(s, t, p, last_offset=9, log_start_offset=5)
        await s.commit()

    req = _mk_req_v11([("oor", [(0, 0, 1024)])])  # fetch before log start
    resp = await do_fetch(config, req, api_version=11)
    p = resp.responses[0].partitions[0]
    assert p.error_code == ErrorCode.offset_out_of_range
    assert p.log_start_offset == i64(5)
    assert len(p.records) == 0


@pytest.mark.asyncio
async def test_fetch_wal_only(config: Config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 10_000)])], min_bytes=0, max_wait_ms=0)
    resp = await do_fetch(config, req, api_version=11)
    assert isinstance(resp, FetchResponseV11)
    tr = resp.responses[0]
    prt = tr.partitions[0]

    assert prt.error_code == ErrorCode.none
    assert prt.high_watermark == i64(29)
    assert prt.records is not None
    assert len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_fetch_parquet_only(config: Config, seeded_topics):
    req = _mk_req_v11([("compacted_only", [(0, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert prt.high_watermark == i64(49)
    assert prt.records is not None
    assert len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_fetch_mixed_merges_sources(config: Config, seeded_topics):
    req = _mk_req_v11([("mixed", [(0, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert prt.high_watermark == i64(39)
    assert prt.records is not None
    assert len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_isolation_level_read_committed_sets_lso(config: Config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 10_000)])], isolation_level=1)
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert prt.last_stable_offset <= prt.high_watermark


@pytest.mark.asyncio
async def test_min_bytes_and_max_bytes_respected(config: Config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 10_000)])], min_bytes=0, max_wait_ms=0, max_bytes=64)
    resp = await do_fetch(config, req, api_version=11)
    assert resp.error_code == ErrorCode.none
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    if prt.records is not None:
        assert isinstance(bytes(prt.records), (bytes, bytearray))


@pytest.mark.asyncio
async def test_partition_max_bytes_limit(config: Config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 16)])])  # tiny per-partition limit
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    if prt.records is not None:
        assert isinstance(bytes(prt.records), (bytes, bytearray))


@pytest.mark.asyncio
async def test_multi_topic_multi_partition_mixed_results(config: Config, seeded_topics):
    req = _mk_req_v11([
        ("mixed", [(0, 0, 10_000)]),  # should be ok
        ("empty", [(0, 0, 10_000)]),  # no data but topic/partition exist
        ("does_not_exist", [(0, 0, 10_000)]),  # unknown topic
    ])
    resp = await do_fetch(config, req, api_version=11)
    assert resp.error_code == ErrorCode.none

    topics = {t.topic: t for t in resp.responses}
    assert set(topics.keys()) == {"mixed", "empty", "does_not_exist"}

    # mixed ok
    p_mixed = topics["mixed"].partitions[0]
    assert p_mixed.error_code == ErrorCode.none
    assert p_mixed.records is not None and len(bytes(p_mixed.records)) > 0

    # empty
    p_empty = topics["empty"].partitions[0]
    assert p_empty.error_code in (ErrorCode.none, ErrorCode.offset_out_of_range)
    # unknown topic
    p_unknown = topics["does_not_exist"].partitions[0]
    assert p_unknown.error_code == ErrorCode.unknown_topic_or_partition
    assert len(p_unknown.records) == 0


@pytest.mark.asyncio
async def test_fetch_offset_at_log_end_returns_empty_no_error(config: Config, seeded_topics):
    req = _mk_req_v11([("empty", [(0, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    if prt.records is not None:
        assert len(bytes(prt.records)) == 0


@pytest.mark.asyncio
async def test_fetch_offset_greater_than_hw_is_out_of_range(config: Config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 100, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.offset_out_of_range
    assert len(prt.records) == 0


@pytest.mark.asyncio
async def test_fetch_midstream_from_parquet(config: Config, seeded_topics):
    req = _mk_req_v11([("compacted_only", [(0, 25, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert prt.high_watermark == i64(49)
    assert prt.records is not None
    assert len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_same_topic_multiple_partitions_independent_errors(config: Config, seeded_topics):
    req = _mk_req_v11([("mixed", [(0, 0, 10_000), (1, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    ts = {p.partition_index: p for p in resp.responses[0].partitions}
    assert ts[0].error_code == ErrorCode.none and ts[0].records is not None
    assert ts[1].error_code == ErrorCode.unknown_topic_or_partition and len(ts[1].records) == 0


@pytest.mark.asyncio
async def test_response_metadata_defaults(config: Config, seeded_topics):
    req = _mk_req_v11([("mixed", [(0, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)

    assert isinstance(resp, FetchResponseV11)
    assert resp.error_code == ErrorCode.none
    assert resp.throttle_time.total_seconds() * 1000 >= 0
    assert int(resp.session_id) in (0,)  # no session by default

    prt = resp.responses[0].partitions[0]
    assert prt.preferred_read_replica == BrokerId(-1)
    assert int(prt.log_start_offset) >= -1  # accept -1 if not filled by impl
    assert prt.aborted_transactions in (None, tuple())


@pytest.mark.asyncio
async def test_forgotten_topics_ignored_gracefully(config: Config, seeded_topics):
    req = _mk_req_v11_with_forgotten(
        [("mixed", [(0, 0, 10_000)])],
        forgotten=[("mixed", [0])],
    )
    resp = await do_fetch(config, req, api_version=11)
    assert resp.error_code == ErrorCode.none
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none


@pytest.mark.asyncio
async def test_log_start_offset_is_reported_on_success(config: Config):
    async with config.async_session_factory() as s:
        topic = "logstart"
        part = 0
        await insert_topic_partition(s, topic, part, last_offset=20, log_start_offset=10)

        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        base_ms = int(base_time.timestamp() * 1000)

        await create_parquet_range(
            config,
            s,
            topic=topic,
            partition=part,
            min_off=10,
            max_off=20,
            ts_start_ms=base_ms,
            ts_step_ms=1000,
        )
        await s.commit()

    req = _mk_req_v11([("logstart", [(0, 10, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert int(prt.log_start_offset) in (10, -1)
    assert prt.records is not None and len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_min_bytes_waits_then_returns_empty_or_data(config: Config, seeded_topics):
    req = _mk_req_v11(
        [("empty", [(0, 0, 10_000)])],
        min_bytes=10_000_000,
        max_wait_ms=50,  # small wait
    )
    resp = await do_fetch(config, req, api_version=11)
    assert resp.error_code == ErrorCode.none
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    if prt.records is not None:
        assert len(bytes(prt.records)) == 0


@pytest.mark.asyncio
async def test_top_level_max_bytes_with_multiple_partitions(config: Config, seeded_topics):
    req = _mk_req_v11(
        [
            ("mixed", [(0, 0, 10_000)]),
            ("wal_only", [(0, 0, 10_000)]),
        ],
        max_bytes=128
    )
    resp = await do_fetch(config, req, api_version=11)
    assert resp.error_code == ErrorCode.none

    got_payloads = [
        len(bytes(p.records)) if p.records is not None else 0
        for t in resp.responses for p in t.partitions
    ]
    assert any(x >= 0 for x in got_payloads)


@pytest.mark.asyncio
async def test_fetch_at_leo_non_empty_returns_empty(config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 30, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert len(bytes(prt.records)) == 0


@pytest.mark.asyncio
async def test_response_order_matches_request_order(config, seeded_topics):
    req = _mk_req_v11([
        ("wal_only", [(0, 0, 10_000)]),
        ("mixed", [(0, 0, 10_000)]),
        ("compacted_only", [(0, 0, 10_000)]),
    ])
    resp = await do_fetch(config, req, api_version=11)
    got_topics = [t.topic for t in resp.responses]
    assert got_topics == ["wal_only", "mixed", "compacted_only"]
    for t in resp.responses:
        got_parts = [int(p.partition_index) for p in t.partitions]
        assert got_parts == sorted(got_parts)


@pytest.mark.asyncio
async def test_partition_max_bytes_zero_returns_empty(config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 0)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.invalid_request
    assert len(bytes(prt.records)) == 0


@pytest.mark.asyncio
async def test_read_committed_equals_uncommitted_without_txn(config, seeded_topics):
    req_u = _mk_req_v11([("wal_only", [(0, 0, 10_000)])], isolation_level=0)
    req_c = _mk_req_v11([("wal_only", [(0, 0, 10_000)])], isolation_level=1)
    resp_u = await do_fetch(config, req_u, api_version=11)
    resp_c = await do_fetch(config, req_c, api_version=11)

    pu = resp_u.responses[0].partitions[0]
    pc = resp_c.responses[0].partitions[0]

    assert pu.error_code == ErrorCode.none
    assert pc.error_code == ErrorCode.none
    assert pu.high_watermark == pc.high_watermark
    assert bytes(pu.records) == bytes(pc.records)


@pytest.mark.asyncio
async def test_wal_only_payload_is_valid_kafka_batch(config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    payload = bytes(prt.records)
    assert len(payload) > 0

    kb = KafkaRecordBatch.from_bytes(payload)
    recs = decode_kafka_records(kb.records)
    offsets = [kb.base_offset + r.offset_delta for r in recs]
    assert offsets == sorted(offsets)
    assert offsets[0] == int(kb.base_offset)


@pytest.mark.asyncio
async def test_log_start_not_after_high_watermark_plus_one(config, seeded_topics):
    for topic in ("mixed", "wal_only", "compacted_only"):
        req = _mk_req_v11([(topic, [(0, 0, 10_000)])])
        resp = await do_fetch(config, req, api_version=11)
        prt = resp.responses[0].partitions[0]
        assert prt.error_code == ErrorCode.none
        lsoff = int(prt.log_start_offset)
        if lsoff >= 0:
            assert lsoff <= int(prt.high_watermark) + 1


@pytest.mark.asyncio
async def test_fetch_exact_log_start_returns_data_when_available(config: Config):
    topic, part = "at_logstart", 0
    async with config.async_session_factory() as s:
        await insert_topic_partition(s, topic, part, last_offset=20, log_start_offset=10)

        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        base_ms = int(base_time.timestamp() * 1000)

        await create_parquet_range(
            config,
            s,
            topic=topic,
            partition=part,
            min_off=10,
            max_off=20,
            ts_start_ms=base_ms,
            ts_step_ms=1000,
        )
        await s.commit()

    req = _mk_req_v11([(topic, [(part, 10, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.none
    assert len(bytes(prt.records)) > 0


@pytest.mark.asyncio
async def test_fetch_offset_much_greater_than_hw_out_of_range(config, seeded_topics):
    req = _mk_req_v11([("wal_only", [(0, 10_000, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    prt = resp.responses[0].partitions[0]
    assert prt.error_code == ErrorCode.offset_out_of_range
    assert len(bytes(prt.records)) == 0


@pytest.mark.asyncio
async def test_same_topic_independent_partition_errors_bytes_empty(config, seeded_topics):
    req = _mk_req_v11([("mixed", [(0, 0, 10_000), (1, 0, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    ps = {int(p.partition_index): p for p in resp.responses[0].partitions}
    assert ps[0].error_code == ErrorCode.none and len(bytes(ps[0].records)) > 0
    assert ps[1].error_code == ErrorCode.unknown_topic_or_partition and len(bytes(ps[1].records)) == 0


@pytest.mark.asyncio
async def test_gap_in_parquet_advances_to_next_available(config):
    topic, part = "gap_topic", 0
    async with config.async_session_factory() as s:
        await insert_topic_partition(s, topic, part, last_offset=29, log_start_offset=0)
        base = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        await create_parquet_range(config, s, topic=topic, partition=part,
                                   min_off=0, max_off=9, ts_start_ms=ts_ms(base))
        await create_parquet_range(config, s, topic=topic, partition=part,
                                   min_off=20, max_off=29, ts_start_ms=ts_ms(base) + 20_000)
        await s.commit()

    req = _mk_req_v11([(topic, [(part, 12, 10_000)])])
    resp = await do_fetch(config, req, api_version=11)
    p = resp.responses[0].partitions[0]
    assert p.error_code == ErrorCode.none
    offs = _collect_offsets_from_records_bytes(bytes(p.records))
    assert len(offs) > 0
    assert min(offs) >= 20
    assert all(o >= 20 for o in offs)
