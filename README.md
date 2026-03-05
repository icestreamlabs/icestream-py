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
