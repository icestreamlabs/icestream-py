import pyarrow as pa

PARQUET_RECORD_SCHEMA = pa.schema(
    [
        pa.field("partition", pa.int32(), nullable=False),
        pa.field("offset", pa.int64(), nullable=False),
        pa.field("timestamp_ms", pa.int64(), nullable=True),
        pa.field("key", pa.binary(), nullable=True),
        pa.field("value", pa.binary(), nullable=True),
        pa.field(
            "headers",
            pa.list_(
                pa.struct(
                    [
                        pa.field("key", pa.string(), nullable=False),
                        pa.field("value", pa.binary(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)
