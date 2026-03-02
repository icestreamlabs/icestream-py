import datetime as dt

import pytest
from sqlalchemy import select

from icestream.kafkaserver.producer_state import cleanup_producer_metadata_once
from icestream.models import (
    Partition,
    ProducerPartitionRecentBatch,
    ProducerPartitionState,
    ProducerSession,
    Topic,
)


@pytest.mark.asyncio
async def test_cleanup_producer_metadata_removes_expired_rows(config):
    now = dt.datetime.now(dt.timezone.utc)
    stale = now - dt.timedelta(hours=3)
    fresh = now - dt.timedelta(seconds=10)

    async with config.async_session_factory() as session:
        session.add(Topic(name="producer_cleanup_topic"))
        session.add(Partition(topic_name="producer_cleanup_topic", partition_number=0))
        await session.flush()

        expired_session = ProducerSession(
            transactional_id="txn-expired",
            producer_epoch=0,
            last_seen_at=stale,
        )
        active_session = ProducerSession(
            transactional_id="txn-active",
            producer_epoch=0,
            last_seen_at=fresh,
        )
        session.add(expired_session)
        await session.flush()
        session.add(active_session)
        await session.flush()

        session.add_all(
            [
                ProducerPartitionState(
                    producer_id=expired_session.producer_id,
                    producer_epoch=0,
                    topic_name="producer_cleanup_topic",
                    partition_number=0,
                    next_expected_sequence=0,
                ),
                ProducerPartitionState(
                    producer_id=active_session.producer_id,
                    producer_epoch=0,
                    topic_name="producer_cleanup_topic",
                    partition_number=0,
                    next_expected_sequence=0,
                ),
            ]
        )
        await session.flush()

        session.add_all(
            [
                ProducerPartitionRecentBatch(
                    producer_id=expired_session.producer_id,
                    producer_epoch=0,
                    topic_name="producer_cleanup_topic",
                    partition_number=0,
                    base_sequence=0,
                    last_sequence=0,
                    first_offset=0,
                    last_offset=0,
                    created_at=stale,
                ),
                ProducerPartitionRecentBatch(
                    producer_id=active_session.producer_id,
                    producer_epoch=0,
                    topic_name="producer_cleanup_topic",
                    partition_number=0,
                    base_sequence=0,
                    last_sequence=0,
                    first_offset=0,
                    last_offset=0,
                    created_at=fresh,
                ),
            ]
        )

        await session.commit()

    config.PRODUCER_SESSION_TTL_SECONDS = 60
    config.PRODUCER_RECENT_BATCH_TTL_SECONDS = 60

    deleted_recent_batches, deleted_sessions = await cleanup_producer_metadata_once(
        config
    )

    assert deleted_recent_batches == 1
    assert deleted_sessions == 1

    async with config.async_session_factory() as session:
        sessions = (
            await session.execute(
                select(ProducerSession.transactional_id).order_by(
                    ProducerSession.transactional_id
                )
            )
        ).scalars().all()
        recent_batches = (
            await session.execute(
                select(ProducerPartitionRecentBatch.producer_id).order_by(
                    ProducerPartitionRecentBatch.producer_id
                )
            )
        ).scalars().all()

    assert sessions == ["txn-active"]
    assert len(recent_batches) == 1
