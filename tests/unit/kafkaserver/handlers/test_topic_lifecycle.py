from types import SimpleNamespace

import pytest
from kio.schema.errors import ErrorCode
from sqlalchemy import select

from icestream.kafkaserver.handlers.create_topics import do_handle_create_topics_request
from icestream.kafkaserver.handlers.delete_topics import do_delete_topics
from icestream.models import (
    Partition,
    Topic,
    TopicWALFile,
    TopicWALFileOffset,
    WALFile,
    WALFileOffset,
)


async def _run_create(config, req, api_version: int = 7):
    captured = {}

    async def callback(resp):
        captured["response"] = resp

    await do_handle_create_topics_request(config, req, api_version, callback)
    return captured["response"]


def _create_topic_req(
    *,
    name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    validate_only: bool = False,
):
    return SimpleNamespace(
        topics=(
            SimpleNamespace(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                assignments=(),
                configs=(),
            ),
        ),
        validate_only=validate_only,
    )


@pytest.mark.asyncio
async def test_create_topics_existing_returns_topic_already_exists(config):
    async with config.async_session_factory() as session:
        session.add(Topic(name="existing_topic"))
        session.add(Partition(topic_name="existing_topic", partition_number=0))
        await session.commit()

    response = await _run_create(config, _create_topic_req(name="existing_topic"))
    assert response.topics[0].error_code == ErrorCode.topic_already_exists


@pytest.mark.asyncio
async def test_create_topics_invalid_partition_count_returns_invalid_partitions(config):
    response = await _run_create(
        config,
        _create_topic_req(name="invalid_partitions", num_partitions=0),
    )
    assert response.topics[0].error_code == ErrorCode.invalid_partitions


@pytest.mark.asyncio
async def test_create_topics_internal_name_is_rejected(config):
    response = await _run_create(
        config,
        _create_topic_req(name="__internal_create_blocked"),
    )
    assert response.topics[0].error_code == ErrorCode.topic_authorization_failed

    async with config.async_session_factory() as session:
        row = (
            await session.execute(
                select(Topic).where(Topic.name == "__internal_create_blocked")
            )
        ).scalar_one_or_none()
    assert row is None


@pytest.mark.asyncio
async def test_delete_topics_existing_deletes_topic_and_related_metadata(config):
    topic_name = "delete_me"
    async with config.async_session_factory() as session:
        session.add(Topic(name=topic_name))
        session.add(Partition(topic_name=topic_name, partition_number=0))
        await session.flush()

        wal = WALFile(uri="wal://delete-me", etag=None, total_bytes=1, total_messages=1)
        session.add(wal)
        await session.flush()
        session.add(
            WALFileOffset(
                wal_file_id=wal.id,
                topic_name=topic_name,
                partition_number=0,
                base_offset=0,
                last_offset=0,
                byte_start=0,
                byte_end=1,
                min_timestamp=None,
                max_timestamp=None,
            )
        )

        topic_wal = TopicWALFile(
            topic_name=topic_name,
            uri="topic-wal://delete-me",
            etag=None,
            total_bytes=1,
            total_messages=1,
            compacted_at=None,
        )
        session.add(topic_wal)
        await session.flush()
        session.add(
            TopicWALFileOffset(
                topic_wal_file_id=topic_wal.id,
                topic_name=topic_name,
                partition_number=0,
                base_offset=0,
                last_offset=0,
                byte_start=0,
                byte_end=1,
                min_timestamp=None,
                max_timestamp=None,
            )
        )
        await session.commit()

    req = SimpleNamespace(topic_names=(topic_name,))
    response = await do_delete_topics(config, req, api_version=5)
    assert response.responses[0].error_code == ErrorCode.none

    async with config.async_session_factory() as session:
        assert (
            await session.execute(select(Topic).where(Topic.name == topic_name))
        ).scalar_one_or_none() is None
        assert (
            await session.execute(
                select(Partition).where(Partition.topic_name == topic_name)
            )
        ).scalar_one_or_none() is None
        assert (
            await session.execute(
                select(WALFileOffset).where(WALFileOffset.topic_name == topic_name)
            )
        ).scalar_one_or_none() is None
        assert (
            await session.execute(
                select(TopicWALFile).where(TopicWALFile.topic_name == topic_name)
            )
        ).scalar_one_or_none() is None
        assert (
            await session.execute(
                select(TopicWALFileOffset).where(
                    TopicWALFileOffset.topic_name == topic_name
                )
            )
        ).scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_delete_topics_unknown_returns_unknown_topic_or_partition(config):
    req = SimpleNamespace(topic_names=("unknown_delete_topic",))
    response = await do_delete_topics(config, req, api_version=5)
    assert response.responses[0].error_code == ErrorCode.unknown_topic_or_partition


@pytest.mark.asyncio
async def test_delete_topics_internal_returns_topic_authorization_failed(config):
    req = SimpleNamespace(topic_names=("__consumer_offsets",))
    response = await do_delete_topics(config, req, api_version=5)
    assert response.responses[0].error_code == ErrorCode.topic_authorization_failed


@pytest.mark.asyncio
async def test_delete_topics_duplicate_request_entry_returns_invalid_request(config):
    topic_name = "duplicate_delete_topic"
    async with config.async_session_factory() as session:
        session.add(Topic(name=topic_name))
        session.add(Partition(topic_name=topic_name, partition_number=0))
        await session.commit()

    req = SimpleNamespace(topic_names=(topic_name, topic_name))
    response = await do_delete_topics(config, req, api_version=5)
    assert response.responses[0].error_code == ErrorCode.none
    assert response.responses[1].error_code == ErrorCode.invalid_request

    async with config.async_session_factory() as session:
        deleted = (
            await session.execute(select(Topic).where(Topic.name == topic_name))
        ).scalar_one_or_none()
    assert deleted is None
