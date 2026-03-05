import datetime

import pytest
from sqlalchemy import select

from icestream.admin.errors import AlreadyExistsError, InvalidRequestError, NotFoundError
from icestream.admin.services import AdminService
from icestream.models import (
    Partition,
    Topic,
    TopicWALFile,
    TopicWALFileOffset,
    WALFile,
    WALFileOffset,
)
from icestream.models.consumer_groups import (
    ConsumerGroup,
    GroupAssignment,
    GroupMember,
    GroupOffset,
)


@pytest.mark.asyncio
async def test_create_topic_success(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        result = await service.create_topic(
            session,
            name="orders_v1",
            num_partitions=3,
        )

    assert result.name == "orders_v1"
    assert result.partition_count == 3
    assert result.is_internal is False

    async with config.async_session_factory() as session:
        topic = (
            await session.execute(select(Topic).where(Topic.name == "orders_v1"))
        ).scalar_one_or_none()
        partitions = (
            await session.execute(
                select(Partition).where(Partition.topic_name == "orders_v1")
            )
        ).scalars().all()

    assert topic is not None
    assert len(partitions) == 3
    assert sorted(p.partition_number for p in partitions) == [0, 1, 2]


@pytest.mark.asyncio
async def test_create_topic_conflict_raises_already_exists(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        session.add(Topic(name="existing_topic", is_internal=False))
        await session.flush()
        session.add(
            Partition(
                topic_name="existing_topic",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        await session.commit()

    async with config.async_session_factory() as session:
        with pytest.raises(AlreadyExistsError):
            await service.create_topic(
                session,
                name="existing_topic",
                num_partitions=1,
            )


@pytest.mark.asyncio
async def test_create_topic_rejects_internal_name(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        with pytest.raises(InvalidRequestError):
            await service.create_topic(
                session,
                name="__consumer_offsets",
                num_partitions=1,
            )


@pytest.mark.asyncio
async def test_delete_topic_not_found(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        with pytest.raises(NotFoundError):
            await service.delete_topic(session, name="missing-topic")


@pytest.mark.asyncio
async def test_delete_topic_rejects_internal(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        with pytest.raises(InvalidRequestError):
            await service.delete_topic(session, name="__consumer_offsets")


@pytest.mark.asyncio
async def test_delete_topic_cascades_partitions(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        session.add(Topic(name="delete_me", is_internal=False))
        await session.flush()
        session.add(
            Partition(
                topic_name="delete_me",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        await session.commit()

    async with config.async_session_factory() as session:
        await service.delete_topic(session, name="delete_me")

    async with config.async_session_factory() as session:
        topic = (
            await session.execute(select(Topic).where(Topic.name == "delete_me"))
        ).scalar_one_or_none()
        partition = (
            await session.execute(
                select(Partition).where(Partition.topic_name == "delete_me")
            )
        ).scalar_one_or_none()

    assert topic is None
    assert partition is None


@pytest.mark.asyncio
async def test_list_topics_returns_stable_name_order_with_pagination(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        for topic_name in ("zeta", "alpha", "middle"):
            session.add(Topic(name=topic_name, is_internal=False))
            await session.flush()

        session.add(
            Partition(
                topic_name="zeta",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        await session.flush()
        session.add(
            Partition(
                topic_name="alpha",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        await session.flush()
        await session.commit()

    async with config.async_session_factory() as session:
        page = await service.list_topics(session, limit=2, offset=0)

    assert page.total == 3
    assert [topic.name for topic in page.topics] == ["alpha", "middle"]
    assert [topic.partition_count for topic in page.topics] == [1, 0]


@pytest.mark.asyncio
async def test_get_topic_returns_partition_metadata(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        session.add(Topic(name="details", is_internal=False))
        await session.flush()
        session.add(
            Partition(
                topic_name="details",
                partition_number=1,
                last_offset=12,
                log_start_offset=3,
            )
        )
        await session.flush()
        session.add(
            Partition(
                topic_name="details",
                partition_number=0,
                last_offset=8,
                log_start_offset=0,
            )
        )
        await session.flush()
        await session.commit()

    async with config.async_session_factory() as session:
        topic = await service.get_topic(session, name="details")

    assert topic.name == "details"
    assert [partition.partition_number for partition in topic.partitions] == [0, 1]
    assert [partition.last_offset for partition in topic.partitions] == [8, 12]


@pytest.mark.asyncio
async def test_get_topic_not_found(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        with pytest.raises(NotFoundError):
            await service.get_topic(session, name="missing_topic")


@pytest.mark.asyncio
async def test_get_topic_offsets_reports_wal_and_topic_wal_visibility(config):
    service = AdminService()
    async with config.async_session_factory() as session:
        session.add(Topic(name="offsets_topic", is_internal=False))
        await session.flush()
        session.add(
            Partition(
                topic_name="offsets_topic",
                partition_number=0,
                last_offset=12,
                log_start_offset=2,
            )
        )
        await session.flush()

        wal = WALFile(
            uri="wal://offsets-topic",
            etag=None,
            total_bytes=123,
            total_messages=10,
            compacted_at=None,
        )
        session.add(wal)
        await session.flush()
        session.add(
            WALFileOffset(
                wal_file_id=wal.id,
                topic_name="offsets_topic",
                partition_number=0,
                base_offset=2,
                last_offset=9,
                byte_start=0,
                byte_end=100,
                min_timestamp=None,
                max_timestamp=None,
            )
        )
        await session.flush()

        topic_wal = TopicWALFile(
            topic_name="offsets_topic",
            uri="topic-wal://offsets-topic",
            etag=None,
            total_bytes=64,
            total_messages=5,
            compacted_at=None,
        )
        session.add(topic_wal)
        await session.flush()
        session.add(
            TopicWALFileOffset(
                topic_wal_file_id=topic_wal.id,
                topic_name="offsets_topic",
                partition_number=0,
                base_offset=2,
                last_offset=12,
                byte_start=0,
                byte_end=64,
                min_timestamp=None,
                max_timestamp=None,
            )
        )
        await session.commit()

    async with config.async_session_factory() as session:
        offsets = await service.get_topic_offsets(session, name="offsets_topic")

    assert offsets.topic_name == "offsets_topic"
    assert len(offsets.partitions) == 1
    partition = offsets.partitions[0]
    assert partition.partition_number == 0
    assert partition.log_start_offset == 2
    assert partition.latest_offset == 12
    assert partition.wal_uncompacted_segment_count == 1
    assert partition.wal_uncompacted_min_offset == 2
    assert partition.wal_uncompacted_max_offset == 9
    assert partition.topic_wal_segment_count == 1
    assert partition.topic_wal_min_offset == 2
    assert partition.topic_wal_max_offset == 12


@pytest.mark.asyncio
async def test_list_and_describe_consumer_groups(config):
    service = AdminService()
    now = datetime.datetime.now(datetime.UTC)

    async with config.async_session_factory() as session:
        session.add(Topic(name="topic-x", is_internal=False))
        await session.flush()
        session.add(
            Partition(
                topic_name="topic-x",
                partition_number=0,
                last_offset=-1,
                log_start_offset=0,
            )
        )
        await session.flush()

        group_a = ConsumerGroup(
            group_id="group-a",
            state="Stable",
            generation=3,
            protocol_type="consumer",
            selected_protocol="range",
            updated_at=now,
        )
        session.add(group_a)
        await session.flush()

        group_b = ConsumerGroup(
            group_id="group-b",
            state="Empty",
            generation=0,
            protocol_type="consumer",
            selected_protocol=None,
            updated_at=now,
        )
        session.add(group_b)
        await session.flush()

        member_a1 = GroupMember(consumer_group_id=group_a.id, member_id="member-a1")
        session.add(member_a1)
        await session.flush()
        member_a2 = GroupMember(consumer_group_id=group_a.id, member_id="member-a2")
        session.add(member_a2)
        await session.flush()

        session.add(
            GroupAssignment(
                consumer_group_id=group_a.id,
                generation=3,
                topic_name="topic-x",
                partition_number=0,
                group_member_id=member_a1.id,
            )
        )
        await session.flush()
        session.add(
            GroupOffset(
                consumer_group_id=group_a.id,
                topic_name="topic-x",
                partition_number=0,
                committed_offset=42,
                committed_generation=3,
                committed_member_id="member-a1",
            )
        )
        await session.commit()

    async with config.async_session_factory() as session:
        listed = await service.list_consumer_groups(session, limit=10, offset=0)
    assert listed.total == 2
    assert [group.group_id for group in listed.groups] == ["group-a", "group-b"]
    assert listed.groups[0].member_count == 2
    assert listed.groups[1].member_count == 0

    async with config.async_session_factory() as session:
        described = await service.describe_consumer_group(session, group_id="group-a")
    assert described.summary.group_id == "group-a"
    assert described.summary.member_count == 2
    assert [member.member_id for member in described.members] == ["member-a1", "member-a2"]
    assert len(described.assignments) == 1
    assert described.assignments[0].topic_name == "topic-x"
    assert len(described.offsets) == 1
    assert described.offsets[0].committed_offset == 42
