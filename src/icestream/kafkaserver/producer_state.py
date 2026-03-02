from __future__ import annotations

import asyncio
import datetime as dt

from sqlalchemy import and_, delete, or_, select

from icestream.config import Config
from icestream.logger import log
from icestream.models import ProducerPartitionRecentBatch, ProducerSession


async def cleanup_producer_metadata_once(config: Config) -> tuple[int, int]:
    """Delete expired producer-session and recent-batch metadata."""
    assert config.async_session_factory is not None

    now = dt.datetime.now(dt.timezone.utc)
    recent_batch_cutoff = now - dt.timedelta(
        seconds=config.PRODUCER_RECENT_BATCH_TTL_SECONDS
    )

    async with config.async_session_factory() as session:
        async with session.begin():
            expired_recent_batches = await session.execute(
                delete(ProducerPartitionRecentBatch)
                .where(ProducerPartitionRecentBatch.created_at < recent_batch_cutoff)
                .returning(ProducerPartitionRecentBatch.producer_id)
            )
            deleted_recent_batches = len(expired_recent_batches.all())

            expired_sessions = await session.execute(
                delete(ProducerSession)
                .where(
                    or_(
                        ProducerSession.expires_at < now,
                        and_(
                            ProducerSession.expires_at.is_(None),
                            ProducerSession.last_seen_at
                            < now
                            - dt.timedelta(
                                seconds=config.PRODUCER_SESSION_TTL_SECONDS
                            ),
                        ),
                    )
                )
                .returning(ProducerSession.producer_id)
            )
            deleted_sessions = len(expired_sessions.all())

    if deleted_recent_batches or deleted_sessions:
        log.info(
            "producer_state_cleanup",
            deleted_recent_batches=deleted_recent_batches,
            deleted_sessions=deleted_sessions,
        )

    return deleted_recent_batches, deleted_sessions


async def run_producer_state_reaper(config: Config, shutdown_event: asyncio.Event) -> None:
    assert config.async_session_factory is not None

    interval_s = max(1.0, config.PRODUCER_STATE_CLEANUP_INTERVAL_MS / 1000.0)

    while not shutdown_event.is_set():
        try:
            await cleanup_producer_metadata_once(config)
        except Exception:
            log.exception("producer_state_cleanup_failed")

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            continue
