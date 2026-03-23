# icestream
The Table-ator-inator is the easiest way to cheaply ingest data into your lakehouse in the whole Tri-State Area.

### Useful commands

```shell
docker run --name icestream-postgres -e POSTGRES_USER=icestream -e POSTGRES_PASSWORD=icestream -e POSTGRES_DB=icestream_dev -p 5432:5432 -d postgres:17
export ICESTREAM_DATABASE_URL="postgresql+asyncpg://icestream:icestream@localhost:5432/icestream_dev"
alembic upgrade head
```

`icestream` is Postgres-only for runtime metadata (`postgresql+asyncpg`).

## Admin REST API

The admin API is an operator-facing HTTP surface. It does not replace Kafka-wire
admin APIs for clients.

### Admin API config

- `ICESTREAM_ADMIN_API_ENABLED` (default `true`)
- `ICESTREAM_ADMIN_PORT` (default `8080`)
- `ICESTREAM_ADMIN_MAX_REQUEST_BYTES` (default `1048576`)
- `ICESTREAM_ADMIN_REQUEST_TIMEOUT_SECONDS` (default `10`)

Requests support `X-Request-ID`; responses echo it in both header and JSON
envelope (`request_id`).

### Response contract

Success envelope:

```json
{
  "api_version": "v1",
  "status": "ok",
  "data": {},
  "request_id": "..."
}
```

Error envelope:

```json
{
  "api_version": "v1",
  "status": "error",
  "error": {
    "code": "invalid_request",
    "message": "..."
  },
  "request_id": "..."
}
```

Common error codes: `already_exists`, `not_found`, `invalid_request`,
`internal_error`, `request_too_large`, `request_timeout`.

### Topic lifecycle endpoints

- `POST /topics`
- `GET /topics?limit=<int>&offset=<int>`
- `GET /topics/{topic_name}`
- `DELETE /topics/{topic_name}`
- `GET /topics/{topic_name}/offsets`

Create topic:

```shell
curl -sS -X POST http://127.0.0.1:8080/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders.v1","num_partitions":3}'
```

List topics:

```shell
curl -sS "http://127.0.0.1:8080/topics?limit=25&offset=0"
```

Get topic details:

```shell
curl -sS http://127.0.0.1:8080/topics/orders.v1
```

Delete topic:

```shell
curl -sS -X DELETE http://127.0.0.1:8080/topics/orders.v1
```

### Operational endpoints

- `GET /healthz` (liveness)
- `GET /readyz` (includes DB connectivity)
- `GET /consumer-groups?limit=<int>&offset=<int>`
- `GET /consumer-groups/{group_id}`

### Behavior differences vs Kafka-wire admin APIs

- REST create supports one topic per request and does not expose
  `replication_factor` semantics.
- REST rejects internal topic mutations directly with `invalid_request`.
- REST responses are envelope-based and versioned (`api_version=v1`), while
  Kafka-wire uses protocol-versioned payloads.

### Migration note (for direct-DB tooling)

If your tooling currently inspects metadata tables directly, migrate to REST
reads (`/topics`, `/topics/{topic_name}`, `/topics/{topic_name}/offsets`,
`/consumer-groups`) for stable contracts across schema changes.

### Compaction Modes

`icestream` now uses topic-WAL segments as the active compacted read layer.

- `ICESTREAM_COMPACTION_FORMAT=topic_wal` (default): active runtime compacted path.
- `ICESTREAM_COMPACTION_FORMAT=parquet`: legacy fallback compaction path kept for compatibility/debugging.
- `ICESTREAM_COMPACTION_FORMAT=none`: disables compaction worker output.

Active Kafka fetch/list-offsets paths read from raw WAL plus topic-WAL metadata/files. Parquet code remains buildable and test-covered but is not part of the default runtime data flow.

### Segment Read Cache

`icestream` includes an optional read-through cache for immutable WAL/topic-WAL
object reads.

- Disabled by default (`ICESTREAM_SEGMENT_CACHE_ENABLED=false`).
- Backed by `foyer-py`.
- Read-through behavior: cache miss -> object store fetch -> cache populate.
- Keyed by normalized object key + byte range + optional version token (ETag).
- Cache failures are fail-open for reads (direct object-store fallback).

#### Cache config

- `ICESTREAM_SEGMENT_CACHE_ENABLED` (default `false`)
- `ICESTREAM_SEGMENT_CACHE_MODE` (`memory` or `hybrid`, default `memory`)
- `ICESTREAM_SEGMENT_CACHE_MEMORY_CAPACITY_BYTES` (default `67108864`)
- `ICESTREAM_SEGMENT_CACHE_DISK_CAPACITY_BYTES` (default `268435456`, hybrid only)
- `ICESTREAM_SEGMENT_CACHE_DIR` (default `/tmp/icestream-segment-cache`, hybrid only)
- `ICESTREAM_SEGMENT_CACHE_GET_TIMEOUT_SECONDS` (default `0.05`)
- `ICESTREAM_SEGMENT_CACHE_PUT_TIMEOUT_SECONDS` (default `0.05`)

#### Verification and troubleshooting

- Cache logs:
  - `segment_cache_initialized`
  - `segment_cache_miss_fill`
  - `segment_cache_coalesced_wait`
  - `segment_cache_get_failed` / `segment_cache_put_failed`
- If cache backend init fails, startup logs:
  - `segment_cache_init_failed_fallback_to_direct_reads`
- For debugging read-path behavior, disable cache with:
  - `ICESTREAM_SEGMENT_CACHE_ENABLED=false`

#### Rollout sequence

1. Keep cache disabled in local/dev.
2. Enable in test/staging with `memory` mode and verify hit/miss/fallback logs.
3. Enable in production (`memory` first, then `hybrid` if needed).
4. Monitor fetch latency, cache error logs, and object-store read pressure.
5. Roll back by toggling `ICESTREAM_SEGMENT_CACHE_ENABLED=false` and restarting.
