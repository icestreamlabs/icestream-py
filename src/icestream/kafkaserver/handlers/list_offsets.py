import datetime
import io
from typing import Optional, Tuple, Sequence

from kio.schema.errors import ErrorCode
from kio.schema.list_offsets.v0.request import (
    ListOffsetsRequest as ListOffsetsRequestV0,
)
from kio.schema.list_offsets.v0.request import (
    RequestHeader as ListOffsetsRequestHeaderV0,
)
from kio.schema.list_offsets.v0.response import (
    ListOffsetsResponse as ListOffsetsResponseV0,
)
from kio.schema.list_offsets.v0.response import (
    ResponseHeader as ListOffsetsResponseHeaderV0,
)
from kio.schema.list_offsets.v1.request import (
    ListOffsetsRequest as ListOffsetsRequestV1,
)
from kio.schema.list_offsets.v1.request import (
    RequestHeader as ListOffsetsRequestHeaderV1,
)
from kio.schema.list_offsets.v1.response import (
    ListOffsetsResponse as ListOffsetsResponseV1,
)
from kio.schema.list_offsets.v1.response import (
    ResponseHeader as ListOffsetsResponseHeaderV1,
)
from kio.schema.list_offsets.v2.request import (
    ListOffsetsRequest as ListOffsetsRequestV2,
)
from kio.schema.list_offsets.v2.request import (
    RequestHeader as ListOffsetsRequestHeaderV2,
)
from kio.schema.list_offsets.v2.response import (
    ListOffsetsResponse as ListOffsetsResponseV2,
)
from kio.schema.list_offsets.v2.response import (
    ResponseHeader as ListOffsetsResponseHeaderV2,
)
from kio.schema.list_offsets.v3.request import (
    ListOffsetsRequest as ListOffsetsRequestV3,
)
from kio.schema.list_offsets.v3.request import (
    RequestHeader as ListOffsetsRequestHeaderV3,
)
from kio.schema.list_offsets.v3.response import (
    ListOffsetsResponse as ListOffsetsResponseV3,
)
from kio.schema.list_offsets.v3.response import (
    ResponseHeader as ListOffsetsResponseHeaderV3,
)
from kio.schema.list_offsets.v4.request import (
    ListOffsetsRequest as ListOffsetsRequestV4,
)
from kio.schema.list_offsets.v4.request import (
    RequestHeader as ListOffsetsRequestHeaderV4,
)
from kio.schema.list_offsets.v4.response import (
    ListOffsetsResponse as ListOffsetsResponseV4,
)
from kio.schema.list_offsets.v4.response import (
    ResponseHeader as ListOffsetsResponseHeaderV4,
)
from kio.schema.list_offsets.v5.request import (
    ListOffsetsRequest as ListOffsetsRequestV5,
)
from kio.schema.list_offsets.v5.request import (
    RequestHeader as ListOffsetsRequestHeaderV5,
)
from kio.schema.list_offsets.v5.response import (
    ListOffsetsResponse as ListOffsetsResponseV5,
)
from kio.schema.list_offsets.v5.response import (
    ResponseHeader as ListOffsetsResponseHeaderV5,
)
from kio.schema.list_offsets.v6.request import (
    ListOffsetsRequest as ListOffsetsRequestV6,
)
from kio.schema.list_offsets.v6.request import (
    RequestHeader as ListOffsetsRequestHeaderV6,
)
from kio.schema.list_offsets.v6.response import (
    ListOffsetsResponse as ListOffsetsResponseV6,
)
from kio.schema.list_offsets.v6.response import (
    ResponseHeader as ListOffsetsResponseHeaderV6,
)
from kio.schema.list_offsets.v7.request import (
    ListOffsetsRequest as ListOffsetsRequestV7,
)
from kio.schema.list_offsets.v7.request import (
    RequestHeader as ListOffsetsRequestHeaderV7,
)
from kio.schema.list_offsets.v7.response import (
    ListOffsetsResponse as ListOffsetsResponseV7,
)
from kio.schema.list_offsets.v7.response import (
    ResponseHeader as ListOffsetsResponseHeaderV7,
)
from kio.schema.list_offsets.v8.request import (
    ListOffsetsRequest as ListOffsetsRequestV8,
)
from kio.schema.list_offsets.v8.request import (
    RequestHeader as ListOffsetsRequestHeaderV8,
)
from kio.schema.list_offsets.v8.response import (
    ListOffsetsResponse as ListOffsetsResponseV8,
)
from kio.schema.list_offsets.v8.response import (
    ResponseHeader as ListOffsetsResponseHeaderV8,
)
from kio.schema.list_offsets.v9.request import (
    ListOffsetsRequest as ListOffsetsRequestV9,
)
from kio.schema.list_offsets.v9.request import (
    RequestHeader as ListOffsetsRequestHeaderV9,
)
from kio.schema.list_offsets.v9.response import (
    ListOffsetsResponse as ListOffsetsResponseV9,
)
from kio.schema.list_offsets.v9.response import (
    ResponseHeader as ListOffsetsResponseHeaderV9,
)
from kio.static.primitive import i32, i64, i32Timedelta

from icestream.config import Config
from icestream.kafkaserver.internal_topics import internal_topics
from icestream.kafkaserver.topic_backends import topic_backend_for_name
from icestream.kafkaserver.protocol import decode_kafka_records
from icestream.kafkaserver.wal.serde import decode_kafka_wal_file
from icestream.models import ParquetFile, WALFile, WALFileOffset, Partition
from icestream.models.consumer_groups import GroupOffsetLog
from icestream.utils import normalize_object_key

ListOffsetsRequestHeader = (
        ListOffsetsRequestHeaderV0
        | ListOffsetsRequestHeaderV1
        | ListOffsetsRequestHeaderV2
        | ListOffsetsRequestHeaderV3
        | ListOffsetsRequestHeaderV4
        | ListOffsetsRequestHeaderV5
        | ListOffsetsRequestHeaderV6
        | ListOffsetsRequestHeaderV7
        | ListOffsetsRequestHeaderV8
        | ListOffsetsRequestHeaderV9
)

ListOffsetsResponseHeader = (
        ListOffsetsResponseHeaderV0
        | ListOffsetsResponseHeaderV1
        | ListOffsetsResponseHeaderV2
        | ListOffsetsResponseHeaderV3
        | ListOffsetsResponseHeaderV4
        | ListOffsetsResponseHeaderV5
        | ListOffsetsResponseHeaderV6
        | ListOffsetsResponseHeaderV7
        | ListOffsetsResponseHeaderV8
        | ListOffsetsResponseHeaderV9
)

ListOffsetsRequest = (
        ListOffsetsRequestV0
        | ListOffsetsRequestV1
        | ListOffsetsRequestV2
        | ListOffsetsRequestV3
        | ListOffsetsRequestV4
        | ListOffsetsRequestV5
        | ListOffsetsRequestV6
        | ListOffsetsRequestV7
        | ListOffsetsRequestV8
        | ListOffsetsRequestV9
)

ListOffsetsResponse = (
        ListOffsetsResponseV0
        | ListOffsetsResponseV1
        | ListOffsetsResponseV2
        | ListOffsetsResponseV3
        | ListOffsetsResponseV4
        | ListOffsetsResponseV5
        | ListOffsetsResponseV6
        | ListOffsetsResponseV7
        | ListOffsetsResponseV8
        | ListOffsetsResponseV9
)

from sqlalchemy import select, asc, Result, Row, func

import kio.schema.list_offsets.v0 as lo_v0
import kio.schema.list_offsets.v1 as lo_v1
import kio.schema.list_offsets.v2 as lo_v2
import kio.schema.list_offsets.v3 as lo_v3
import kio.schema.list_offsets.v4 as lo_v4
import kio.schema.list_offsets.v5 as lo_v5
import kio.schema.list_offsets.v6 as lo_v6
import kio.schema.list_offsets.v7 as lo_v7
import kio.schema.list_offsets.v8 as lo_v8
import kio.schema.list_offsets.v9 as lo_v9

import pyarrow.parquet as pq

EARLIEST = -2
LATEST = -1


async def _find_offset_for_timestamp_parquet(
        config: Config, pf: ParquetFile, ts: int, floor_offset: int
) -> Optional[Tuple[int, int]]:
    if pf.min_timestamp and pf.max_timestamp:
        min_ts = int(pf.min_timestamp.timestamp() * 1000)
        max_ts = int(pf.max_timestamp.timestamp() * 1000)
        if max_ts < ts:
            return None  # skip everything older than target
        # if min_ts >= ts we still need to read to get the first offset >= floor_offset
        # (we can't assume the very first row meets floor_offset).
    obj = await config.store.get_async(normalize_object_key(config, pf.uri))
    blob = await obj.bytes_async()
    pfq = pq.ParquetFile(io.BytesIO(bytes(blob)))

    # TODO can we optimize by using arrow stats?
    cols = ["offset", "timestamp_ms"]
    for rg in range(pfq.num_row_groups):
        tbl = pfq.read_row_group(rg, columns=cols)
        off = tbl.column(tbl.schema.get_field_index("offset"))
        tscol = tbl.column(tbl.schema.get_field_index("timestamp_ms"))
        for i in range(tbl.num_rows):
            o = int(off[i].as_py())
            if o < floor_offset:
                continue
            tsv = tscol[i].as_py()
            if tsv is None:
                # if timestamp is missing, treat as 0
                # Kafka allows unknown timestamps
                tsv = 0
            tsv = int(tsv)
            if tsv >= ts:
                return o, tsv
    return None


async def _find_offset_for_timestamp_wal(
        config: Config, wf: WALFile, ts: int, floor_offset: int
) -> Optional[Tuple[int, int]]:
    obj = await config.store.get_async(normalize_object_key(config, wf.uri))
    data = await obj.bytes_async()
    decoded = decode_kafka_wal_file(bytes(data))

    for b in decoded.batches:
        krb = b.kafka_record_batch
        base = krb.base_offset
        if base is None:
            continue
        lod = krb.last_offset_delta or 0
        last_in_batch = base + lod
        if last_in_batch < floor_offset:
            continue

        base_ts = krb.base_timestamp or 0
        # decode record bodies only when batch may contain candidate records
        recs = decode_kafka_records(krb.records)

        for r in recs:
            a = base + r.offset_delta
            if a < floor_offset:
                continue
            rec_ts = base_ts + (r.timestamp_delta or 0)

            if rec_ts >= ts:
                return a, rec_ts
    return None


async def _resolve_timestamp_to_offset(
        config: Config,
        topic: str,
        partition: int,
        target_ts: int,
        last_offset: int,
        log_start: int,
) -> Tuple[ErrorCode, int, int]:
    if target_ts == LATEST:
        return ErrorCode.none, target_ts, last_offset + 1
    if target_ts == EARLIEST:
        return ErrorCode.none, target_ts, log_start

    async with config.async_session_factory() as session:
        pfiles: Sequence[ParquetFile] = (
            await session.execute(
                select(ParquetFile)
                .where(
                    ParquetFile.topic_name == topic,
                    ParquetFile.partition_number == partition,
                    ParquetFile.compacted_at.is_(None),
                    ParquetFile.max_offset >= log_start,
                    ParquetFile.min_offset <= last_offset,
                )
                .order_by(asc(ParquetFile.min_offset))
            )
        ).scalars().all()

        result: Result[Tuple[WALFileOffset, WALFile]] = await session.execute(
            select(WALFileOffset, WALFile)
            .join(WALFile, WALFile.id == WALFileOffset.wal_file_id)
            .where(
                WALFile.compacted_at.is_(None),
                WALFileOffset.topic_name == topic,
                WALFileOffset.partition_number == partition,
                WALFileOffset.last_offset >= log_start,
                WALFileOffset.base_offset <= last_offset,
            )
            .order_by(asc(WALFileOffset.base_offset))
        )
        wf_rows: Sequence[Tuple[WALFileOffset, WALFile]] = result.all()

    # track global min/max timestamps we see in any segment
    seen_min_ts: Optional[int] = None
    seen_max_ts: Optional[int] = None

    def _bump(tsv: Optional[int]):
        nonlocal seen_min_ts, seen_max_ts
        if tsv is None:
            return
        seen_min_ts = tsv if seen_min_ts is None or tsv < seen_min_ts else seen_min_ts
        seen_max_ts = tsv if seen_max_ts is None or tsv > seen_max_ts else seen_max_ts

    floor = max(log_start, 0)

    for pf in pfiles:
        min_ts_file = int(pf.min_timestamp.timestamp() * 1000) if pf.min_timestamp else None
        max_ts_file = int(pf.max_timestamp.timestamp() * 1000) if pf.max_timestamp else None
        _bump(min_ts_file)
        _bump(max_ts_file)

        if max_ts_file is not None and max_ts_file < target_ts:
            if pf.max_offset is not None:
                floor = max(floor, int(pf.max_offset) + 1)
            continue

        found = await _find_offset_for_timestamp_parquet(config, pf, target_ts, floor)
        if found is not None:
            o, tsv = found
            return ErrorCode.none, tsv, o

        if pf.max_offset is not None:
            floor = max(floor, int(pf.max_offset) + 1)

    for off_row, wf in wf_rows:
        min_ts_file = off_row.min_timestamp
        max_ts_file = off_row.max_timestamp
        _bump(min_ts_file)
        _bump(max_ts_file)

        if max_ts_file is not None and max_ts_file < target_ts:
            floor = max(floor, int(off_row.last_offset) + 1)
            continue

        found = await _find_offset_for_timestamp_wal(config, wf, target_ts, floor)
        if found is not None:
            o, tsv = found
            return ErrorCode.none, tsv, o

        floor = max(floor, int(off_row.last_offset) + 1)

    if seen_max_ts is not None and target_ts > seen_max_ts:
        return ErrorCode.none, target_ts, last_offset + 1

    if seen_min_ts is not None and target_ts <= seen_min_ts:
        return ErrorCode.none, target_ts, log_start

    return ErrorCode.none, target_ts, last_offset + 1


async def do_list_offsets(config: Config, req: ListOffsetsRequest, api_version: int) -> ListOffsetsResponse:
    topic_responses: list[lo_v9.response.ListOffsetsTopicResponse] = []
    print(req)

    internal_specs = {spec.name: spec for spec in internal_topics(config)}

    async with config.async_session_factory() as session:
        for t in req.topics:
            topic_name = t.name
            part_responses: list[lo_v9.response.ListOffsetsPartitionResponse] = []

            backend = topic_backend_for_name(topic_name)
            if backend.is_internal:
                spec = internal_specs.get(topic_name)
                if spec is None:
                    for pr in t.partitions:
                        partition = int(pr.partition_index)
                        part_responses.append(
                            lo_v9.response.ListOffsetsPartitionResponse(
                                partition_index=i32(partition),
                                error_code=ErrorCode.unknown_topic_or_partition,
                                timestamp=i64(-1),
                                offset=i64(-1),
                            )
                        )
                    topic_responses.append(
                        lo_v9.response.ListOffsetsTopicResponse(
                            name=topic_name,
                            partitions=tuple(part_responses),
                        )
                    )
                    continue

            spec = internal_specs.get(topic_name)
            if backend.is_internal:
                for pr in t.partitions:
                    partition = int(pr.partition_index)
                    ts = int(pr.timestamp)
                    if partition < 0 or partition >= spec.partitions:
                        part_responses.append(
                            lo_v9.response.ListOffsetsPartitionResponse(
                                partition_index=i32(partition),
                                error_code=ErrorCode.unknown_topic_or_partition,
                                timestamp=i64(-1),
                                offset=i64(-1),
                            )
                        )
                        continue

                    bounds = (
                        await session.execute(
                            select(
                                func.min(GroupOffsetLog.log_offset),
                                func.max(GroupOffsetLog.log_offset),
                            ).where(GroupOffsetLog.topic_partition == partition)
                        )
                    ).one()
                    min_offset, max_offset = bounds[0], bounds[1]
                    if max_offset is None:
                        log_start = 0
                        log_end = 0
                    else:
                        log_start = int(min_offset) if min_offset is not None else 0
                        log_end = int(max_offset) + 1

                    if ts == LATEST:
                        offset = log_end
                        ret_ts = ts
                    elif ts == EARLIEST:
                        offset = log_start
                        ret_ts = ts
                    else:
                        offset = log_end
                        ret_ts = ts

                    part_responses.append(
                        lo_v9.response.ListOffsetsPartitionResponse(
                            partition_index=i32(partition),
                            error_code=ErrorCode.none,
                            timestamp=i64(ret_ts),
                            offset=i64(offset),
                        )
                    )

                topic_responses.append(
                    lo_v9.response.ListOffsetsTopicResponse(
                        name=topic_name,
                        partitions=tuple(part_responses),
                    )
                )
                continue

            for pr in t.partitions:
                partition = int(pr.partition_index)
                # request timestamp (ms or sentinel)
                ts = int(pr.timestamp)

                row = (
                    await session.execute(
                        select(Partition.last_offset, Partition.log_start_offset)
                        .where(
                            Partition.topic_name == topic_name,
                            Partition.partition_number == partition,
                        )
                    )
                ).one_or_none()

                if row is None:
                    part_responses.append(
                        lo_v9.response.ListOffsetsPartitionResponse(
                            partition_index=i32(partition),
                            error_code=ErrorCode.unknown_topic_or_partition,
                            timestamp=i64(-1),
                            offset=i64(-1),
                        )
                    )
                    continue

                last_offset, log_start = int(row[0]), int(row[1])

                err, ret_ts, offset = await _resolve_timestamp_to_offset(
                    config=config,
                    topic=topic_name,
                    partition=partition,
                    target_ts=ts,
                    last_offset=last_offset,
                    log_start=log_start,
                )

                part_responses.append(
                    lo_v9.response.ListOffsetsPartitionResponse(
                        partition_index=i32(partition),
                        error_code=err,
                        timestamp=i64(ret_ts),
                        offset=i64(offset),
                    )
                )

            topic_responses.append(
                lo_v9.response.ListOffsetsTopicResponse(
                    name=topic_name,
                    partitions=tuple(part_responses),
                )
            )

    resp_v9 = lo_v9.ListOffsetsResponse(
        throttle_time=i32Timedelta.parse(datetime.timedelta(milliseconds=0)),
        topics=tuple(topic_responses),
    )

    if api_version == 9:
        return resp_v9
    else:
        return _list_offsets_response_ladder(resp_v9, api_version)


def _list_offsets_response_ladder(resp_v9: lo_v9.ListOffsetsResponse, api_version: int) -> ListOffsetsResponse:
    """
    Down-convert the canonical v9 response to earlier versions.
    Shapes between versions vary slightly in Kafka; the following mapping keeps the same
    semantics (per-partition {error_code, timestamp, offset}) while adapting field names.
    Adjust inner class names if your generated schema differs.
    """
    if api_version == 8:
        topics = tuple(
            lo_v8.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v8.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v8.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 7:
        topics = tuple(
            lo_v7.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v7.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v7.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 6:
        topics = tuple(
            lo_v6.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v6.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v6.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 5:
        topics = tuple(
            lo_v5.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v5.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v5.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 4:
        topics = tuple(
            lo_v4.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v4.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v4.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 3:
        topics = tuple(
            lo_v3.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v3.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v3.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    if api_version == 2:
        topics = tuple(
            lo_v2.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v2.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v2.ListOffsetsResponse(
            throttle_time=resp_v9.throttle_time,
            topics=topics,
        )

    # v0/v1 historically had arrays of offsets; here we adapt by returning a single-element vector
    if api_version == 1:
        topics = tuple(
            lo_v1.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v1.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        timestamp=p.timestamp,
                        offset=p.offset,  # single element, Kafka brokers also often return 1
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v1.ListOffsetsResponse(
            topics=topics,
        )

    if api_version == 0:
        topics = tuple(
            lo_v0.response.ListOffsetsTopicResponse(
                name=t.name,
                partitions=tuple(
                    lo_v0.response.ListOffsetsPartitionResponse(
                        partition_index=p.partition_index,
                        error_code=p.error_code,
                        old_style_offsets=(p.offset,),
                    ) for p in t.partitions
                )
            ) for t in resp_v9.topics
        )
        return lo_v0.ListOffsetsResponse(
            topics=topics,
        )

    raise ValueError(f"unsupported list_offsets api version: {api_version}")
