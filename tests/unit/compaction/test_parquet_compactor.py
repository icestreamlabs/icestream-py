import io
from unittest.mock import AsyncMock, MagicMock, patch

import obstore as obs
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from obstore.store import MemoryStore

from icestream.compaction.parquet_compactor import ParquetCompactor
from icestream.compaction.schema import PARQUET_RECORD_SCHEMA
from icestream.compaction.types import CompactionContext
from icestream.models import ParquetFile, ParquetFileParent


def make_parquet_bytes(rows: list[dict], *, row_group_size: int | None = None) -> bytes:
    table = pa.Table.from_pylist(rows, PARQUET_RECORD_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        row_group_size=row_group_size,
    )
    return buf.getvalue()


async def list_paths(store: MemoryStore, prefix: str) -> list[str]:
    stream = obs.list(store, prefix=prefix)
    metas = await stream.collect_async()
    return [m["path"] for m in metas]


@pytest.fixture
def capture_added():
    def _capture(cfg) -> list[object]:
        added: list[object] = []
        session = cfg.async_session_factory.return_value.__aenter__.return_value
        session.add.side_effect = lambda obj: added.append(obj)
        return added

    return _capture


@pytest.fixture
def run_parquet_compaction():
    async def _run(cfg, candidates, *, overlap_ok: bool = True):
        ctx = CompactionContext(
            config=cfg,
            wal_models=[],
            wal_decoded=[],
            parquet_candidates=candidates,
            now_monotonic=0.0,
        )
        compactor = ParquetCompactor()

        mocked = AsyncMock()
        if overlap_ok:
            mocked.side_effect = None
        else:
            mocked.side_effect = ValueError("overlap")

        with patch(
            "icestream.compaction.parquet_compactor.assert_no_overlap", new=mocked
        ):
            if overlap_ok:
                await compactor.apply(ctx)
            else:
                with pytest.raises(ValueError, match="overlap"):
                    await compactor.apply(ctx)

    return _run


# --------------------------------- tests --------------------------------------


@pytest.mark.asyncio
async def test_apply_no_candidates_uses_no_session(base_config):
    cfg = base_config
    cfg.store = MemoryStore()

    ctx = CompactionContext(
        config=cfg,
        wal_models=[],
        wal_decoded=[],
        parquet_candidates={},  # no candidates
        now_monotonic=0.0,
    )

    compactor = ParquetCompactor()
    await compactor.apply(ctx)

    # with no candidates, we shouldn't even open a db session
    cfg.async_session_factory.assert_not_called()


@pytest.mark.asyncio
async def test_compacts_parents_into_single_output(
    base_config, capture_added, run_parquet_compaction
):
    topic = "orders"
    partition = 3

    # parent 1 offsets 0..4, parent 2 offsets 5..9
    rows1 = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(0, 5)
    ]
    rows2 = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(5, 10)
    ]
    pbytes1 = make_parquet_bytes(rows1)
    pbytes2 = make_parquet_bytes(rows2)

    store = MemoryStore()
    parent_key1 = "parquet/parents/orders/partition=3/0-4-gen0.parquet"
    parent_key2 = "parquet/parents/orders/partition=3/5-9-gen0.parquet"
    await store.put_async(parent_key1, io.BytesIO(pbytes1))
    await store.put_async(parent_key2, io.BytesIO(pbytes2))

    # orm-ish parent objects
    parent1 = MagicMock(spec=ParquetFile)
    parent1.uri = parent_key1
    parent1.min_offset = 0
    parent1.max_offset = 4
    parent1.generation = 0
    parent1.compacted_at = None
    parent1.id = 101

    parent2 = MagicMock(spec=ParquetFile)
    parent2.uri = parent_key2
    parent2.min_offset = 5
    parent2.max_offset = 9
    parent2.generation = 0
    parent2.compacted_at = None
    parent2.id = 102

    cfg = base_config
    cfg.store = store
    cfg.PARQUET_COMPACTION_TARGET_BYTES = 10_000_000  # big so we create a single output
    cfg.PARQUET_PREFIX = "parquet"
    cfg.WAL_BUCKET = "my-bucket"
    cfg.WAL_BUCKET_PREFIX = "wal-prefix"

    added = capture_added(cfg)
    await run_parquet_compaction(
        cfg, {(topic, partition): [parent1, parent2]}, overlap_ok=True
    )

    # child parquet file and lineage rows must be recorded
    child_pfs = [o for o in added if isinstance(o, ParquetFile)]
    lineage = [o for o in added if isinstance(o, ParquetFileParent)]
    assert len(child_pfs) == 1
    child = child_pfs[0]

    assert child.topic_name == topic
    assert child.partition_number == partition
    assert child.min_offset == 0 and child.max_offset == 9
    assert child.generation == 1

    # uri format should be s3-like with bucket and optional prefix
    assert child.uri.startswith(f"s3://{cfg.WAL_BUCKET}/")
    assert f"/{cfg.WAL_BUCKET_PREFIX}/" in child.uri

    # ensure two lineage rows exist and reference both parents
    assert len(lineage) == 2
    parent_ids = {l.parent_parquet_file_id for l in lineage}
    assert parent_ids == {parent1.id, parent2.id}

    # parents should be tombstoned
    assert parent1.compacted_at is not None
    assert parent2.compacted_at is not None

    child_keys = await list_paths(cfg.store, prefix=cfg.PARQUET_PREFIX)
    child_keys = [
        k
        for k in child_keys
        if k.startswith(f"{cfg.PARQUET_PREFIX}/topics/{topic}/partition={partition}/")
        and k.endswith("-gen1.parquet")
    ]
    assert len(child_keys) == 1
    child_key = child_keys[0]

    res = await cfg.store.get_async(child_key)
    raw = await res.bytes_async()
    table = pq.read_table(io.BytesIO(raw))
    assert table.column("offset").to_pylist() == list(range(0, 10))
    assert set(table.column("partition").to_pylist()) == {partition}


@pytest.mark.asyncio
async def test_rollover_when_target_bytes_tiny(
    base_config, capture_added, run_parquet_compaction
):
    topic = "events"
    partition = 0

    # build one parent with 10 rows 0..9, force row_group_size=1 so the compactor can split on rg boundaries
    rows = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(10)
    ]
    pbytes = make_parquet_bytes(rows, row_group_size=1)

    store = MemoryStore()
    parent_key = "parquet/parents/events/partition=0/0-9-gen2.parquet"
    await store.put_async(parent_key, io.BytesIO(pbytes))

    parent = MagicMock(spec=ParquetFile)
    parent.uri = parent_key
    parent.min_offset = 0
    parent.max_offset = 9
    parent.generation = 2
    parent.compacted_at = None
    parent.id = 999

    cfg = base_config
    cfg.store = store
    cfg.PARQUET_COMPACTION_TARGET_BYTES = 1  # tiny to force multiple rollovers
    cfg.PARQUET_PREFIX = "parquet"
    cfg.WAL_BUCKET = "my-bucket"
    cfg.WAL_BUCKET_PREFIX = ""  # none

    added = capture_added(cfg)
    await run_parquet_compaction(cfg, {(topic, partition): [parent]}, overlap_ok=True)

    # look for produced children for gen3 because parent gen is 2
    out_keys = await list_paths(cfg.store, prefix=cfg.PARQUET_PREFIX)
    out_keys = [
        k
        for k in out_keys
        if k.startswith(f"{cfg.PARQUET_PREFIX}/topics/{topic}/partition={partition}/")
        and k.endswith("-gen3.parquet")
    ]
    # with row_group_size=1 and a tiny target, we expect multiple outputs
    assert len(out_keys) >= 2

    # child parquet files in the session should be non-overlapping & ascending
    child_pfs = [o for o in added if isinstance(o, ParquetFile)]
    assert len(child_pfs) >= 2
    child_pfs.sort(key=lambda pf: pf.min_offset)

    prev_max = None
    for pf in child_pfs:
        if prev_max is not None:
            assert pf.min_offset > prev_max
        assert pf.generation == parent.generation + 1
        prev_max = pf.max_offset

    # parent should be tombstoned
    assert parent.compacted_at is not None


@pytest.mark.asyncio
async def test_overlap_error_aborts_child_and_tombstones(
    base_config, capture_added, run_parquet_compaction
):
    cfg = base_config
    cfg.store = MemoryStore()
    cfg.PARQUET_COMPACTION_TARGET_BYTES = 10_000_000
    cfg.PARQUET_PREFIX = "parquet"
    cfg.WAL_BUCKET = "bucket"
    cfg.WAL_BUCKET_PREFIX = ""

    # build one parent [0..4]
    topic, partition = "overlaps", 0
    parent_key = f"parquet/parents/{topic}/partition={partition}/0-4-gen0.parquet"
    rows = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(5)
    ]
    await cfg.store.put_async(parent_key, io.BytesIO(make_parquet_bytes(rows)))

    parent = ParquetFile(
        topic_name=topic,
        partition_number=partition,
        uri=parent_key,
        total_bytes=123,  # unused
        row_count=len(rows),
        min_offset=0,
        max_offset=4,
        min_timestamp=None,
        max_timestamp=None,
        generation=0,
    )
    parent.id = 111

    added = capture_added(cfg)
    # run with overlap_ok=False so the patched assert_no_overlap raises
    await run_parquet_compaction(cfg, {(topic, partition): [parent]}, overlap_ok=False)

    # verify no child parquet file nor lineage rows were added
    assert not any(isinstance(o, ParquetFile) for o in added)
    assert not any(isinstance(o, ParquetFileParent) for o in added)
    # parent should not be tombstoned on error
    assert parent.compacted_at is None

    # parent should still be the only object under parquet/
    keys = await list_paths(cfg.store, prefix="parquet/")
    assert parent_key in keys


@pytest.mark.asyncio
async def test_compacts_multiple_groups_in_one_apply(
    base_config, capture_added, run_parquet_compaction
):
    cfg = base_config
    cfg.store = MemoryStore()
    cfg.PARQUET_COMPACTION_TARGET_BYTES = 10_000_000
    cfg.PARQUET_PREFIX = "parquet"
    cfg.WAL_BUCKET = "bucket"
    cfg.WAL_BUCKET_PREFIX = "pfx"

    # group a: topic=a, partition=0, offsets 0..2
    rows_a = [
        {
            "partition": 0,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(3)
    ]
    key_a = "parquet/parents/a/partition=0/0-2-gen0.parquet"
    await cfg.store.put_async(key_a, io.BytesIO(make_parquet_bytes(rows_a)))
    parent_a = ParquetFile(
        topic_name="a",
        partition_number=0,
        uri=key_a,
        total_bytes=1,
        row_count=3,
        min_offset=0,
        max_offset=2,
        min_timestamp=None,
        max_timestamp=None,
        generation=0,
    )
    parent_a.id = 201

    # group b: topic=b, partition=1, offsets 10..14
    rows_b = [
        {
            "partition": 1,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(10, 15)
    ]
    key_b = "parquet/parents/b/partition=1/10-14-gen0.parquet"
    await cfg.store.put_async(key_b, io.BytesIO(make_parquet_bytes(rows_b)))
    parent_b = ParquetFile(
        topic_name="b",
        partition_number=1,
        uri=key_b,
        total_bytes=1,
        row_count=5,
        min_offset=10,
        max_offset=14,
        min_timestamp=None,
        max_timestamp=None,
        generation=0,
    )
    parent_b.id = 202

    added = capture_added(cfg)
    await run_parquet_compaction(
        cfg, {("a", 0): [parent_a], ("b", 1): [parent_b]}, overlap_ok=True
    )

    # verify we got one child parquet file for each group
    children = [o for o in added if isinstance(o, ParquetFile)]
    assert len(children) == 2
    # verify lineage rows exist for each parent
    lineages = [o for o in added if isinstance(o, ParquetFileParent)]
    assert len(lineages) == 2
    # check min/max offsets and generations
    for c in children:
        if c.topic_name == "a":
            assert c.partition_number == 0
            assert c.min_offset == 0 and c.max_offset == 2
            assert c.generation == 1
        elif c.topic_name == "b":
            assert c.partition_number == 1
            assert c.min_offset == 10 and c.max_offset == 14
            assert c.generation == 1
        else:
            pytest.fail(f"unexpected child topic {c.topic_name}")

    # confirm exactly 2 outputs under 'parquet/topics'
    out_keys = await list_paths(cfg.store, prefix="parquet/topics/")
    out_keys = [k for k in out_keys if k.endswith(".parquet")]
    assert len(out_keys) == 2


@pytest.mark.asyncio
async def test_compacts_non_contiguous_parents_spanning_range(
    base_config, capture_added, run_parquet_compaction
):
    cfg = base_config
    cfg.store = MemoryStore()
    cfg.PARQUET_COMPACTION_TARGET_BYTES = 10_000_000
    cfg.PARQUET_PREFIX = "parquet"
    cfg.WAL_BUCKET = "bucket"
    cfg.WAL_BUCKET_PREFIX = ""

    topic, partition = "span", 2

    # parent1 covers 0..2, parent2 covers 5..7 (gap 3..4): compactor still concatenates in order
    rows_p1 = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(0, 3)
    ]
    rows_p2 = [
        {
            "partition": partition,
            "offset": i,
            "timestamp_ms": None,
            "key": None,
            "value": None,
            "headers": [],
        }
        for i in range(5, 8)
    ]

    key1 = f"parquet/parents/{topic}/partition={partition}/0-2-gen0.parquet"
    key2 = f"parquet/parents/{topic}/partition={partition}/5-7-gen0.parquet"
    await cfg.store.put_async(key1, io.BytesIO(make_parquet_bytes(rows_p1)))
    await cfg.store.put_async(key2, io.BytesIO(make_parquet_bytes(rows_p2)))

    p1 = ParquetFile(
        topic_name=topic,
        partition_number=partition,
        uri=key1,
        total_bytes=1,
        row_count=3,
        min_offset=0,
        max_offset=2,
        min_timestamp=None,
        max_timestamp=None,
        generation=0,
    )
    p1.id = 301
    p2 = ParquetFile(
        topic_name=topic,
        partition_number=partition,
        uri=key2,
        total_bytes=1,
        row_count=3,
        min_offset=5,
        max_offset=7,
        min_timestamp=None,
        max_timestamp=None,
        generation=0,
    )
    p2.id = 302

    added = capture_added(cfg)
    await run_parquet_compaction(cfg, {(topic, partition): [p1, p2]}, overlap_ok=True)

    # a single child covering the union 0..7 should be created (since target is big)
    children = [o for o in added if isinstance(o, ParquetFile)]
    assert len(children) == 1
    child = children[0]
    assert child.topic_name == topic and child.partition_number == partition
    assert child.min_offset == 0 and child.max_offset == 7
    assert child.generation == 1

    # parents should be tombstoned
    assert p1.compacted_at is not None and p2.compacted_at is not None

    out_keys = await list_paths(
        cfg.store, prefix=f"{cfg.PARQUET_PREFIX}/topics/{topic}/partition={partition}/"
    )
    assert len(out_keys) == 1
    out_key = out_keys[0]
    res = await cfg.store.get_async(out_key)
    raw = await res.bytes_async()
    table = pq.read_table(io.BytesIO(raw))
    offsets = table.column("offset").to_pylist()
    # compactor writes batches in order and does not fill gaps
    # we expect concatenated offsets [0,1,2,5,6,7]
    assert offsets == [0, 1, 2, 5, 6, 7]
