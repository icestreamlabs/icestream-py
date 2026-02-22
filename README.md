# icestream
The Table-ator-inator is the easiest way to cheaply ingest data into your lakehouse in the whole Tri-State Area.

### Useful commands

```shell
docker run --name icestream-postgres -e POSTGRES_USER=icestream -e POSTGRES_PASSWORD=icestream -e POSTGRES_DB=icestream_dev -p 5432:5432 -d postgres:17
export ICESTREAM_DATABASE_URL="postgresql+asyncpg://icestream:icestream@localhost:5432/icestream_dev"
alembic upgrade head
```

`icestream` is Postgres-only for runtime metadata (`postgresql+asyncpg`).

### Compaction Modes

`icestream` now uses topic-WAL segments as the active compacted read layer.

- `ICESTREAM_COMPACTION_FORMAT=topic_wal` (default): active runtime compacted path.
- `ICESTREAM_COMPACTION_FORMAT=parquet`: legacy fallback compaction path kept for compatibility/debugging.
- `ICESTREAM_COMPACTION_FORMAT=none`: disables compaction worker output.

Active Kafka fetch/list-offsets paths read from raw WAL plus topic-WAL metadata/files. Parquet code remains buildable and test-covered but is not part of the default runtime data flow.
