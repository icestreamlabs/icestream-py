# Repository Guidelines

## Project Structure & Module Organization
Source code lives under `src/icestream`:
- `kafkaserver/`: Kafka protocol handling, group coordination, fetch/produce paths, WAL plumbing.
- `kafkaserver/handlers/`: individual API handlers (find coordinator, join/sync/heartbeat, fetch, produce, offsets, etc.).
- `kafkaserver/wal/`: WAL manager and flush behavior.
- `compaction/`: WAL-to-parquet and compaction workers.
- `admin/`: FastAPI admin API.
- `config/`: env-backed runtime config.
- `db/` and `alembic/`: migrations and DB bootstrap.
- `models/`: SQLAlchemy models for topics, WAL metadata, and consumer groups.

Tests:
- `tests/unit/`: fast handler/worker/model/unit coverage.
- `tests/integration/`: boots a real `icestream` process and exercises API + Kafka wire behavior with `aiokafka`.
- `tests/utils/`: shared test utilities/seed helpers.

## Build, Test, and Development Commands
- `uv sync --all-extras` installs project + `dev` extras defined in `pyproject.toml`.
- `uv run icestream` starts the full runtime (Kafka server + admin API + WAL manager + consumer-group reaper; compaction optional).
- `ICESTREAM_DATABASE_URL=postgresql+asyncpg://... uv run alembic upgrade head` applies migrations.
- `uv run pytest tests/unit -q` runs unit tests.
- `uv run pytest tests/integration -q --no-cov` runs integration tests.
- `uv run pytest <path> -q --no-cov` is recommended for focused test runs (global `pytest.ini` enforces coverage >= 80 by default).
- `make tag-patch` creates and pushes the next patch tag (`vX.Y.Z`).

## Coding Style & Naming Conventions
Target Python 3.13, four-space indentation, and type hints (see `icestream.__main__`). Modules, packages, and filenames use `snake_case`; classes remain `PascalCase` while async tasks and queues use descriptive verbs (`run_migrations`, `CompactorWorker`). Favor structured logging via `icestream.logger.log`/`structlog` and keep configuration reads centralized in `Config`. Keep modules under ~300 lines and prefer composable helpers over deep inheritance.

## Testing Guidelines
Pytest + `pytest-asyncio` is the test harness.
- Name tests `test_<behavior>` in `test_<area>.py`.
- Add happy-path and protocol error-path assertions for new handler behavior.
- Consumer-group changes should include both unit tests and integration tests with real `aiokafka` clients.
- If `ICESTREAM_DATABASE_URL` is unset, test setup auto-starts `mockgres`; tests run migrations automatically.
- Integration fixtures in `tests/integration/conftest.py` start `icestream` on ephemeral ports and disable compaction.
- Default pytest options include coverage and a fail-under threshold (`pytest.ini`), so use `--no-cov` for small targeted runs.

## Commit & Pull Request Guidelines
Commits follow short, imperative summaries (e.g., `implement find coordinator`, `fix encoding so fetch works properly`). Keep related changes squashed, push feature branches named `feature/<area>` or `bugfix/<issue-id>`, and describe behavior changes plus rollout notes in the PR body. Reference issues, paste relevant CLI output (migration diff, pytest summary), and include screenshots only when touching admin API responses. Ensure CI can rerun `uv sync`, migrations, and the full pytest matrix without extra secrets.

## Security & Configuration Tips
Do not hardcode credentials; configure via env vars.
- Runtime metadata DB is Postgres (`postgresql+asyncpg`) only.
- `ICESTREAM_DATABASE_URL` should point to a Postgres instance before startup (or tests use mockgres automatically).
- Object store defaults to in-memory; production object-store settings (`ICESTREAM_OBJECT_STORE_PROVIDER`, bucket, region, endpoints) must be explicit.
- Avoid exposing the admin API publicly until authentication/authorization is in place.
