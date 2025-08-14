from datetime import datetime, timedelta, UTC
import io
from unittest.mock import AsyncMock, patch

import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from icestream.compaction.types import CompactionContext
from icestream.compaction.wal_to_parquet import WalToParquetProcessor
from icestream.compaction import build_uri
from icestream.kafkaserver.wal import WALFile as DecodedWALFile, WALBatch
from icestream.kafkaserver.protocol import (
    KafkaRecordBatch,
    KafkaRecord,
    KafkaRecordHeader,
)
from icestream.models import ParquetFile, WALFile as WALFileModel


def capture_parquet_rows(session):
    out = {"ParquetFile": [], "ParquetFileSource": []}

    def _add(obj):
        t = type(obj).__name__
        if t in out:
            out[t].append(obj)

    session.add.side_effect = _add
    return out


def patch_decode_and_build(records):
    return patch(
        "icestream.compaction.wal_to_parquet.decode_kafka_records", return_value=records
    ), patch(
        "icestream.compaction.wal_to_parquet.build_uri",
        side_effect=lambda arg, key: build_uri(getattr(arg, "config", arg), key),
    )


def _make_decoded_wal():
    krb = KafkaRecordBatch(
        base_offset=100,
        batch_length=100,
        partition_leader_epoch=0,
        magic=2,
        crc=0,
        attributes=0,
        last_offset_delta=2,
        base_timestamp=1_700_000_000_000,
        max_timestamp=1_700_000_000_010,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        records_count=3,
        records=b"...",
    )
    batch = WALBatch(topic="t1", partition=0, kafka_record_batch=krb)
    wf = DecodedWALFile(version=1, flushed_at=0, broker_id="b1", batches=[batch])
    return [wf]


@pytest.fixture
def wal_to_parquet_proc():
    return WalToParquetProcessor()


@pytest.fixture
def cfg_for_parquet(base_config):
    cfg = base_config
    cfg.PARQUET_PREFIX = "parquet"
    cfg.PARQUET_TARGET_FILE_BYTES = 200  # small to force chunking in one test
    cfg.PARQUET_ROW_GROUP_TARGET_BYTES = 128
    return cfg


@pytest.fixture
def decoded_wal():
    return _make_decoded_wal()


@pytest.fixture
def ctx(cfg_for_parquet, decoded_wal, mock_async_session_factory):
    factory, session = mock_async_session_factory

    class _FakeScalarResult:
        def scalar(self):
            return None

    session.execute = AsyncMock(return_value=_FakeScalarResult())

    wm1 = WALFileModel(uri="s3://bucket/file1", total_bytes=1000, total_messages=1)
    wm1.id = 1
    wm1.compacted_at = None
    wm1.wal_file_offsets = []

    wm2 = WALFileModel(uri="s3://bucket/file2", total_bytes=1000, total_messages=1)
    wm2.id = 2
    wm2.compacted_at = None
    wm2.wal_file_offsets = []

    return CompactionContext(
        config=cfg_for_parquet,
        wal_models=[wm1, wm2],
        wal_decoded=decoded_wal,
        parquet_candidates={},
        now_monotonic=0.0,
    )


@pytest.mark.asyncio
async def test_bucket_records_and_flush_single_chunk(ctx, mock_async_session_factory):
    proc = WalToParquetProcessor()
    records = [
        KafkaRecord(0, 0, 0, b"k0", b"v0", []),
        KafkaRecord(0, 5, 1, b"k1", b"v1", [KafkaRecordHeader("h", b"x")]),
        KafkaRecord(0, 10, 2, None, b"v2" * 50, []),
    ]
    created = capture_parquet_rows(mock_async_session_factory[1])

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await proc.apply(ctx)

    # one upload with expected key
    assert put_wrap.await_count >= 1
    key = put_wrap.await_args_list[0].args[0]
    assert key.startswith("parquet/topics/t1/partition=0/")
    assert key.endswith("-gen0.parquet")

    pf = created["ParquetFile"][0]
    assert pf.topic_name == "t1"
    assert pf.partition_number == 0
    assert pf.row_count == 3
    assert pf.min_offset == 100 and pf.max_offset == 102

    # links to both wal files
    assert {l.wal_file_id for l in created["ParquetFileSource"]} == {1, 2}


@pytest.mark.asyncio
async def test_chunking_multiple_files(ctx):
    ctx.config.PARQUET_TARGET_FILE_BYTES = 80
    proc = WalToParquetProcessor()
    records = [
        KafkaRecord(0, 0, 0, b"k0", b"v0" * 30, []),
        KafkaRecord(0, 1, 1, b"k1", b"v1" * 30, []),
        KafkaRecord(0, 2, 2, None, b"v2" * 30, []),
    ]

    p1, p2 = patch_decode_and_build(records)
    with p1, p2:
        await proc.apply(ctx)

    assert ctx.config.store.put_async.await_count >= 2


@pytest.mark.asyncio
async def test_memorystore_roundtrip_single_chunk(
    ctx, wal_to_parquet_proc, mock_async_session_factory
):
    ctx.config.store = MemoryStore()
    records = [
        KafkaRecord(0, 0, 0, b"k0", b"v0", []),
        KafkaRecord(0, 5, 1, b"k1", b"v1", [KafkaRecordHeader("h", b"x")]),
        KafkaRecord(0, 10, 2, None, b"v2" * 10, []),
    ]
    created = capture_parquet_rows(mock_async_session_factory[1])

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await wal_to_parquet_proc.apply(ctx)

    assert put_wrap.await_count == 1
    key = put_wrap.await_args_list[0].args[0]
    assert key.startswith("parquet/topics/t1/partition=0/") and key.endswith(
        "-gen0.parquet"
    )

    pf = created["ParquetFile"][0]
    assert pf.topic_name == "t1" and pf.partition_number == 0
    assert pf.row_count == 3 and pf.min_offset == 100 and pf.max_offset == 102
    assert {l.wal_file_id for l in created["ParquetFileSource"]} == {1, 2}

    res = await ctx.config.store.get_async(path=key)
    raw = await res.bytes_async()
    table = pq.read_table(io.BytesIO(raw))
    assert table.column("offset").to_pylist() == [100, 101, 102]
    assert set(table.column("partition").to_pylist()) == {0}
    keys = table.column("key").to_pylist()
    vals = table.column("value").to_pylist()
    assert keys[0] == b"k0" and vals[0] == b"v0"
    assert keys[1] == b"k1" and vals[1] == b"v1"
    assert keys[2] is None and len(vals[2]) == len(b"v2" * 10)
    headers = table.column("headers").to_pylist()
    assert headers[1] == [{"key": "h", "value": b"x"}]


@pytest.mark.asyncio
async def test_memorystore_multiple_chunks(ctx, wal_to_parquet_proc):
    ctx.config.store = MemoryStore()
    ctx.config.PARQUET_TARGET_FILE_BYTES = 70
    records = [
        KafkaRecord(0, 0, 0, b"k0", b"v0" * 30, []),
        KafkaRecord(0, 1, 1, b"k1", b"v1" * 30, []),
        KafkaRecord(0, 2, 2, None, b"v2" * 30, []),
        KafkaRecord(0, 3, 3, b"k3", b"v3" * 30, []),
    ]

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await wal_to_parquet_proc.apply(ctx)

    assert put_wrap.await_count >= 2
    keys = [c.args[0] for c in put_wrap.await_args_list]
    assert all(k.startswith("parquet/topics/t1/partition=0/") for k in keys)

    for k in keys:
        buf = await (await ctx.config.store.get_async(path=k)).bytes_async()
        _ = pq.read_table(io.BytesIO(buf))


@pytest.mark.asyncio
async def test_overlap_raises_and_skips_db_insert(
    ctx, wal_to_parquet_proc, mock_async_session_factory
):
    ctx.config.store = MemoryStore()
    records = [
        KafkaRecord(0, 0, 0, b"k0", b"v0", []),
        KafkaRecord(0, 1, 1, b"k1", b"v1", []),
    ]

    class _Hit:
        def scalar(self):
            return 1

    _, session = mock_async_session_factory
    session.execute = AsyncMock(return_value=_Hit())
    created = capture_parquet_rows(session)

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        with pytest.raises(ValueError):
            await wal_to_parquet_proc.apply(ctx)

    assert put_wrap.await_count >= 1
    assert len(created["ParquetFile"]) == 0


@pytest.mark.asyncio
async def test_min_max_timestamp_fields_on_parquetfile(
    ctx, wal_to_parquet_proc, mock_async_session_factory
):
    ctx.config.store = MemoryStore()
    base_ts_ms = 1_700_000_000_000
    krb = ctx.wal_decoded[0].batches[0].kafka_record_batch
    krb.base_timestamp = base_ts_ms

    created = capture_parquet_rows(mock_async_session_factory[1])

    records = [
        KafkaRecord(0, 0, 0, b"a", b"a", []),  # ts = base + 0
        KafkaRecord(0, 5, 1, b"b", b"b", []),  # ts = base + 5
        KafkaRecord(0, 10, 2, b"c", b"c", []),  # ts = base + 10
    ]

    p1, p2 = patch_decode_and_build(records)
    with p1, p2:
        await wal_to_parquet_proc.apply(ctx)

    assert len(created["ParquetFile"]) >= 1
    pf = created["ParquetFile"][0]
    assert pf.min_timestamp is not None and pf.max_timestamp is not None
    expected_min = datetime.fromtimestamp(base_ts_ms / 1000, tz=UTC)
    expected_max = expected_min + timedelta(milliseconds=10)
    assert pf.min_timestamp == expected_min
    assert pf.max_timestamp == expected_max


@pytest.mark.asyncio
async def test_no_wal_decoded_noop(ctx, mock_async_session_factory):
    proc = WalToParquetProcessor()
    ctx.wal_decoded = []

    with patch.object(
        ctx.config.store, "put_async", wraps=ctx.config.store.put_async
    ) as put_wrap:
        _, session = mock_async_session_factory
        added = []
        session.add.side_effect = lambda obj: added.append(type(obj).__name__)
        await proc.apply(ctx)

    assert put_wrap.await_count == 0
    assert not any(name in {"ParquetFile", "ParquetFileSource"} for name in added)


@pytest.mark.asyncio
async def test_batch_without_base_offset_is_skipped(ctx):
    proc = WalToParquetProcessor()

    # create a wal whose batch lacks the .base_offset attr
    krb = KafkaRecordBatch(
        base_offset=999,
        batch_length=100,
        partition_leader_epoch=0,
        magic=2,
        crc=0,
        attributes=0,
        last_offset_delta=0,
        base_timestamp=0,
        max_timestamp=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        records_count=1,
        records=b"...",
    )
    bad_batch = WALBatch(topic="tX", partition=7, kafka_record_batch=krb)

    wf2 = DecodedWALFile(version=1, flushed_at=0, broker_id="b1", batches=[bad_batch])
    setattr(wf2, "id", 999)
    ctx.wal_decoded.append(wf2)

    records = [KafkaRecord(0, 0, 0, b"k", b"v", [])]
    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await proc.apply(ctx)

    # still at least one upload from the original fixture wal
    assert put_wrap.await_count >= 1


@pytest.mark.asyncio
async def test_multiple_topics_and_partitions_grouping(
    ctx, wal_to_parquet_proc, mock_async_session_factory
):
    b0 = ctx.wal_decoded[0].batches[0]
    b1 = WALBatch(topic="t2", partition=1, kafka_record_batch=b0.kafka_record_batch)
    setattr(b1, "base_offset", b0.kafka_record_batch.base_offset)
    ctx.wal_decoded[0].batches.append(b1)

    # 3 per (topic,partition) using deltas 0,5,10
    records = [
        KafkaRecord(0, 0, 0, b"A", b"A", []),
        KafkaRecord(0, 5, 1, b"B", b"B", []),
        KafkaRecord(0, 10, 2, b"C", b"C", []),
    ]

    created = capture_parquet_rows(mock_async_session_factory[1])

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await wal_to_parquet_proc.apply(ctx)

    assert put_wrap.await_count >= 2
    pf_rows = created["ParquetFile"]
    keys = {
        (pf.topic_name, pf.partition_number, pf.min_offset, pf.max_offset)
        for pf in pf_rows
    }
    assert ("t1", 0, 100, 102) in keys
    assert ("t2", 1, 100, 102) in keys


@pytest.mark.asyncio
async def test_exact_target_boundary_splits(ctx, wal_to_parquet_proc):
    # boundary at 128 → expect 2 chunks
    ctx.config.PARQUET_TARGET_FILE_BYTES = 128
    records = [
        KafkaRecord(0, 0, 0, b"", b"", []),
        KafkaRecord(0, 1, 1, b"", b"", []),
        KafkaRecord(0, 2, 2, b"", b"", []),
    ]

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch.object(
            ctx.config.store, "put_async", wraps=ctx.config.store.put_async
        ) as put_wrap,
    ):
        await wal_to_parquet_proc.apply(ctx)

    assert put_wrap.await_count == 2


@pytest.mark.asyncio
async def test_store_put_failure_raises_and_no_db_insert(
    ctx, wal_to_parquet_proc, mock_async_session_factory
):
    ctx.config.PARQUET_TARGET_FILE_BYTES = 10_000
    ctx.config.store.put_async = AsyncMock(side_effect=RuntimeError("boom"))
    records = [KafkaRecord(0, 0, 0, b"k", b"v", [])]

    created = []
    _, session = mock_async_session_factory
    session.add.side_effect = lambda obj: created.append(obj)

    p1, p2 = patch_decode_and_build(records)
    with p1, p2:
        with pytest.raises(RuntimeError, match="boom"):
            await wal_to_parquet_proc.apply(ctx)

    assert not any(isinstance(o, ParquetFile) for o in created)


@pytest.mark.asyncio
async def test_assert_no_overlap_invoked_with_expected_range(ctx, wal_to_parquet_proc):
    # use out-of-order deltas to verify min/max are sorted
    records = [
        KafkaRecord(0, 10, 2, b"k2", b"v2", []),  # 102
        KafkaRecord(0, 0, 0, b"k0", b"v0", []),  # 100
        KafkaRecord(0, 5, 1, b"k1", b"v1", []),  # 101
    ]

    p1, p2 = patch_decode_and_build(records)
    with (
        p1,
        p2,
        patch(
            "icestream.compaction.wal_to_parquet.assert_no_overlap",
            new_callable=AsyncMock,
        ) as overlap,
    ):
        await wal_to_parquet_proc.apply(ctx)

    overlap.assert_awaited()
    args = overlap.await_args_list[0].args
    # (session, topic, partition, min_off, max_off)
    assert args[1] == "t1"
    assert args[2] == 0
    assert args[3] == 100
    assert args[4] == 102
