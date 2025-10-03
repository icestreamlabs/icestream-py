import io
import struct
import pytest

from icestream.kafkaserver.protocol import (
    KafkaRecord,
    KafkaRecordHeader,
    KafkaRecordBatch,
    decode_kafka_records,
)


def records_equal(a: KafkaRecord, b: KafkaRecord) -> bool:
    return (
        a.attributes == b.attributes
        and a.timestamp_delta == b.timestamp_delta
        and a.offset_delta == b.offset_delta
        and a.key == b.key
        and a.value == b.value
        and len(a.headers) == len(b.headers)
        and all(ha.key == hb.key and ha.value == hb.value for ha, hb in zip(a.headers, b.headers))
    )


@pytest.mark.parametrize(
    "rec",
    [
        KafkaRecord(
            attributes=0,
            timestamp_delta=0,
            offset_delta=0,
            key=None,
            value=None,
            headers=[],
        ),
        KafkaRecord(
            attributes=1,
            timestamp_delta=5,
            offset_delta=2,
            key=b"",
            value=b"",
            headers=[KafkaRecordHeader("x", None)],
        ),
        KafkaRecord(
            attributes=255 & -1,  # signed byte wrap check
            timestamp_delta=123456789,
            offset_delta=98765,
            key=b"k",
            value=b"v" * 10,
            headers=[KafkaRecordHeader("h1", b"val1"), KafkaRecordHeader("h2", b"")],
        ),
        KafkaRecord(
            attributes=3,
            timestamp_delta=-42,
            offset_delta=-7,
            key=None,
            value=b"payload",
            headers=[],
        ),
        KafkaRecord(
            attributes=7,
            timestamp_delta=42,
            offset_delta=7,
            key=b"\x00\x01\x02",
            value=b"\xff\xfe",
            headers=[KafkaRecordHeader("ключ", "значение".encode("utf-8"))],
        ),
    ],
)
def test_kafkarecord_roundtrip(rec: KafkaRecord):
    blob = rec.to_bytes()
    parsed = KafkaRecord.from_bytes(io.BytesIO(blob))
    assert records_equal(rec, parsed)

    assert KafkaRecord.from_bytes(io.BytesIO(blob)).to_bytes() == blob


def test_kafkarecord_length_prefix_matches():
    rec = KafkaRecord(
        attributes=0,
        timestamp_delta=1,
        offset_delta=2,
        key=b"key",
        value=b"value",
        headers=[KafkaRecordHeader("a", b"b")],
    )
    blob = rec.to_bytes()
    parsed = KafkaRecord.from_bytes(io.BytesIO(blob))
    assert parsed.to_bytes() == blob



def make_sample_records():
    return [
        KafkaRecord(
            attributes=0,
            timestamp_delta=0,
            offset_delta=0,
            key=None,
            value=b"hello",
            headers=[],
        ),
        KafkaRecord(
            attributes=1,
            timestamp_delta=10,
            offset_delta=1,
            key=b"k",
            value=None,
            headers=[KafkaRecordHeader("h", b"v")],
        ),
        KafkaRecord(
            attributes=2,
            timestamp_delta=20,
            offset_delta=2,
            key=b"",
            value=b"",
            headers=[KafkaRecordHeader("x", None), KafkaRecordHeader("y", b"")],
        ),
    ]


def test_batch_from_records_sets_metadata_consistently():
    recs = make_sample_records()
    batch = KafkaRecordBatch.from_records(offset=1000, records=recs)

    assert batch.last_offset_delta == len(recs) - 1
    assert batch.records_count == len(recs)

    batch_bytes = batch.to_bytes()
    assert len(batch_bytes) == 12 + batch.batch_length

    parsed = KafkaRecordBatch.from_bytes(batch_bytes)
    assert parsed.base_offset == 1000
    assert parsed.records_count == len(recs)
    assert parsed.last_offset_delta == len(recs) - 1
    assert parsed.batch_length == batch.batch_length
    assert parsed.magic == 2
    assert parsed.attributes == 0
    assert parsed.crc == 0

    parsed_records = decode_kafka_records(parsed.records)
    assert len(parsed_records) == len(recs)
    for a, b in zip(recs, parsed_records):
        assert records_equal(a, b)


def test_batch_idempotency_bytes_to_obj_to_bytes():
    recs = make_sample_records()
    batch = KafkaRecordBatch.from_records(offset=5, records=recs)
    b1 = batch.to_bytes()
    parsed = KafkaRecordBatch.from_bytes(b1)
    b2 = parsed.to_bytes()
    assert b1 == b2


@pytest.mark.parametrize("offset,extra_attr", [(0, 0), (42, 3), (2**31, 7)])
def test_batch_fields_are_serialized_in_order(offset, extra_attr):
    recs = make_sample_records()
    batch = KafkaRecordBatch.from_records(offset=offset, records=recs)
    batch.attributes = extra_attr
    blob = batch.to_bytes()

    buf = io.BytesIO(blob)
    base_offset = struct.unpack(">q", buf.read(8))[0]
    batch_length = struct.unpack(">i", buf.read(4))[0]
    leader_epoch = struct.unpack(">i", buf.read(4))[0]
    magic = struct.unpack(">b", buf.read(1))[0]
    crc = struct.unpack(">I", buf.read(4))[0]
    attributes = struct.unpack(">h", buf.read(2))[0]

    assert base_offset == offset
    assert batch_length == batch.batch_length
    assert leader_epoch == batch.partition_leader_epoch
    assert magic == batch.magic
    assert crc == batch.crc
    assert attributes == extra_attr

    assert len(blob) == 12 + batch_length


def test_batch_records_blob_is_concatenation_of_record_encodings():
    recs = make_sample_records()
    manual = b"".join(r.to_bytes() for r in recs)
    batch = KafkaRecordBatch.from_records(offset=0, records=recs)
    assert batch.records == manual

    parsed = decode_kafka_records(batch.records)
    assert len(parsed) == len(recs)
    for a, b in zip(recs, parsed):
        assert records_equal(a, b)


def test_single_record_batch_edge_cases():
    rec = KafkaRecord(
        attributes=9,
        timestamp_delta=-1,
        offset_delta=0,
        key=None,
        value=None,
        headers=[KafkaRecordHeader("only", b"once")],
    )
    batch = KafkaRecordBatch.from_records(offset=123, records=[rec])
    assert batch.last_offset_delta == 0
    assert batch.records_count == 1
    blob = batch.to_bytes()
    parsed = KafkaRecordBatch.from_bytes(blob)
    assert parsed.last_offset_delta == 0
    assert parsed.records_count == 1
    parsed_records = decode_kafka_records(parsed.records)
    assert len(parsed_records) == 1 and records_equal(parsed_records[0], rec)
