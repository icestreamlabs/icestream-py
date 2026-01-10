import asyncio
import datetime as dt

import pytest
from sqlalchemy import select

from icestream.kafkaserver.consumer_group_liveness import (
    reap_consumer_groups,
    run_consumer_group_reaper,
)
from icestream.models.consumer_groups import ConsumerGroup, GroupMember


@pytest.mark.asyncio
async def test_reaper_expires_members_and_starts_rebalance(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-reap",
                state="Stable",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
            )
            session.add(group)
            await session.flush()

            active = GroupMember(
                consumer_group_id=group.id,
                member_id="active",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now - dt.timedelta(seconds=2),
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"ok",
            )
            expired = GroupMember(
                consumer_group_id=group.id,
                member_id="expired",
                session_timeout_ms=1_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now - dt.timedelta(seconds=5),
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"old",
            )
            session.add(active)
            await session.flush()
            session.add(expired)

    updated = await reap_consumer_groups(config, now=now)
    assert updated == 1

    async with config.async_session_factory() as session:
        refreshed_group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-reap")
            )
        ).scalar_one()
        members = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == refreshed_group.id
                )
            )
        ).scalars().all()

        assert refreshed_group.state == "PreparingRebalance"
        assert refreshed_group.join_phase_deadline_at is not None
        assert refreshed_group.sync_phase_deadline_at is None
        assert {m.member_id for m in members} == {"active"}


@pytest.mark.asyncio
async def test_reaper_resets_sync_deadline(config):
    now = dt.datetime.now(dt.timezone.utc)

    async with config.async_session_factory() as session:
        async with session.begin():
            group = ConsumerGroup(
                group_id="group-sync-reap",
                state="CompletingRebalance",
                generation=1,
                protocol_type="consumer",
                selected_protocol="range",
                sync_phase_deadline_at=now - dt.timedelta(seconds=1),
            )
            session.add(group)
            await session.flush()

            member = GroupMember(
                consumer_group_id=group.id,
                member_id="member-sync",
                session_timeout_ms=10_000,
                rebalance_timeout_ms=30_000,
                last_heartbeat_at=now - dt.timedelta(seconds=2),
                member_generation=1,
                join_generation=1,
                is_in_sync=True,
                assignment=b"assigned",
            )
            session.add(member)

    updated = await reap_consumer_groups(config, now=now)
    assert updated == 1

    async with config.async_session_factory() as session:
        refreshed_group = (
            await session.execute(
                select(ConsumerGroup).where(ConsumerGroup.group_id == "group-sync-reap")
            )
        ).scalar_one()
        refreshed_member = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id == refreshed_group.id,
                    GroupMember.member_id == "member-sync",
                )
            )
        ).scalar_one()

        assert refreshed_group.state == "PreparingRebalance"
        assert refreshed_group.sync_phase_deadline_at is None
        assert refreshed_member.assignment is None
        assert refreshed_member.is_in_sync is False


@pytest.mark.asyncio
async def test_group_reaper_loop_exits(config):
    shutdown_event = asyncio.Event()
    task = asyncio.create_task(
        run_consumer_group_reaper(config, shutdown_event, interval_ms=10)
    )
    await asyncio.sleep(0.05)
    shutdown_event.set()
    await asyncio.wait_for(task, timeout=1)
