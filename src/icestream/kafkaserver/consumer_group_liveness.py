import asyncio
import datetime as dt
from typing import Iterable, Sequence

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.config import Config
from icestream.logger import log
from icestream.models.consumer_groups import ConsumerGroup, GroupMember

REQUEST_HARD_CAP_MS = 120_000


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def to_aware_utc(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def member_expired(member: GroupMember, now: dt.datetime) -> bool:
    last = to_aware_utc(member.last_heartbeat_at)
    if last is None:
        return True
    elapsed_ms = int((now - last).total_seconds() * 1000)
    return elapsed_ms > max(1, int(member.session_timeout_ms))


def min_rebalance_timeout_ms(members: Iterable[GroupMember], default_ms: int) -> int:
    candidates = [m.rebalance_timeout_ms for m in members]
    return min(candidates, default=default_ms)


def _shorten_join_deadline(group: ConsumerGroup, members: Sequence[GroupMember], now: dt.datetime) -> None:
    if not members:
        return
    active_rb_ms = min(
        min_rebalance_timeout_ms(members, group.rebalance_timeout_ms),
        REQUEST_HARD_CAP_MS,
    )
    recomputed_deadline = now + dt.timedelta(milliseconds=active_rb_ms)
    current_deadline = to_aware_utc(group.join_phase_deadline_at)
    if current_deadline is None or recomputed_deadline < current_deadline:
        group.join_phase_deadline_at = recomputed_deadline


async def reset_after_sync_timeout(
    session: AsyncSession,
    group: ConsumerGroup,
    now: dt.datetime,
) -> bool:
    deadline = to_aware_utc(group.sync_phase_deadline_at)
    if deadline is None or now < deadline:
        return False
    if group.state != "CompletingRebalance":
        return False
    group.state = "PreparingRebalance"
    group.join_phase_deadline_at = None
    group.sync_phase_deadline_at = None
    group.state_version += 1
    await session.execute(
        update(GroupMember)
        .where(GroupMember.consumer_group_id == group.id)
        .values(is_in_sync=False, assignment=None)
    )
    return True


async def expire_dead_members(
    session: AsyncSession,
    group: ConsumerGroup,
    now: dt.datetime,
    members: Sequence[GroupMember] | None = None,
) -> tuple[list[GroupMember], int]:
    if members is None:
        members = list(
            (await session.execute(
                select(GroupMember).where(GroupMember.consumer_group_id == group.id)
            )).scalars()
        )
    expired = [m for m in members if member_expired(m, now)]
    if not expired:
        return list(members), 0

    expired_ids = [m.id for m in expired]
    await session.execute(
        delete(GroupMember).where(
            GroupMember.consumer_group_id == group.id,
            GroupMember.id.in_(expired_ids),
        )
    )

    remaining = [m for m in members if m.id not in expired_ids]
    group.state_version += 1

    if not remaining:
        group.state = "Empty"
        group.selected_protocol = None
        group.leader_member_id = None
        group.join_phase_deadline_at = None
        group.sync_phase_deadline_at = None
        return remaining, len(expired_ids)

    if group.state in ("Stable", "CompletingRebalance", "Empty"):
        group.state = "PreparingRebalance"
    group.sync_phase_deadline_at = None
    _shorten_join_deadline(group, remaining, now)
    return remaining, len(expired_ids)


async def reap_consumer_groups(
    config: Config,
    *,
    now: dt.datetime | None = None,
    group_ids: Iterable[int] | None = None,
) -> int:
    assert config.async_session_factory is not None
    clock = now or utc_now()
    updated_groups = 0

    async with config.async_session_factory() as session:
        async with session.begin():
            if group_ids is None:
                ids = list((await session.execute(select(ConsumerGroup.id))).scalars())
            else:
                ids = list(group_ids)

            for gid in ids:
                group = (
                    await session.execute(
                        select(ConsumerGroup)
                        .where(ConsumerGroup.id == gid)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if group is None:
                    continue

                touched = await reset_after_sync_timeout(session, group, clock)
                _, expired_count = await expire_dead_members(session, group, clock)
                if touched or expired_count:
                    updated_groups += 1

    return updated_groups


async def run_consumer_group_reaper(
    config: Config,
    shutdown_event: asyncio.Event,
    *,
    interval_ms: int | None = None,
) -> None:
    interval = max(0, int(interval_ms or config.GROUP_REAPER_INTERVAL_MS))
    interval_s = max(0.05, interval / 1000)

    while not shutdown_event.is_set():
        try:
            await reap_consumer_groups(config)
        except Exception as exc:  # pragma: no cover - safety net
            log.exception("consumer group reaper failed", error=str(exc))
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            continue
