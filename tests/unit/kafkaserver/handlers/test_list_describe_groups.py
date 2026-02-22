import datetime as dt

import pytest
from kio.schema.describe_groups.v5.request import DescribeGroupsRequest
from kio.schema.errors import ErrorCode
from kio.schema.list_groups.v5.request import ListGroupsRequest

from icestream.config import Config
from icestream.kafkaserver.handlers.describe_groups import do_describe_groups
from icestream.kafkaserver.handlers.list_groups import do_list_groups
from icestream.models.consumer_groups import ConsumerGroup, GroupMember


async def _create_group(
    session,
    *,
    group_id: str,
    state: str,
    generation: int = 1,
    protocol_type: str | None = "consumer",
    selected_protocol: str | None = "range",
    leader_member_id: str | None = None,
) -> ConsumerGroup:
    group = ConsumerGroup(
        group_id=group_id,
        generation=generation,
        state=state,
        protocol_type=protocol_type,
        selected_protocol=selected_protocol,
        leader_member_id=leader_member_id,
    )
    session.add(group)
    await session.flush()
    return group


async def _create_member(
    session,
    *,
    group: ConsumerGroup,
    member_id: str,
    generation: int,
    assignment: bytes = b"",
) -> GroupMember:
    member = GroupMember(
        consumer_group_id=group.id,
        member_id=member_id,
        session_timeout_ms=10_000,
        rebalance_timeout_ms=30_000,
        last_heartbeat_at=dt.datetime.now(dt.timezone.utc),
        member_generation=generation,
        join_generation=generation,
        is_in_sync=True,
        assignment=assignment,
    )
    session.add(member)
    await session.flush()
    return member


@pytest.mark.asyncio
async def test_list_groups_includes_all_consumer_group_states(config: Config) -> None:
    groups = {
        "cg-empty": "Empty",
        "cg-prep": "PreparingRebalance",
        "cg-completing": "CompletingRebalance",
        "cg-stable": "Stable",
    }
    async with config.async_session_factory() as session:
        async with session.begin():
            for group_id, state in groups.items():
                await _create_group(
                    session,
                    group_id=group_id,
                    state=state,
                    selected_protocol=None if state == "Empty" else "range",
                )

    req = ListGroupsRequest(states_filter=tuple(), types_filter=tuple())
    resp = await do_list_groups(config, req, api_version=5)

    assert resp.error_code == ErrorCode.none
    by_group_id = {group.group_id: group for group in resp.groups}
    assert set(by_group_id.keys()) >= set(groups.keys())
    for group_id, state in groups.items():
        assert by_group_id[group_id].group_state == state
        assert by_group_id[group_id].group_type == "classic"


@pytest.mark.asyncio
async def test_describe_groups_covers_all_states_and_missing_group(config: Config) -> None:
    async with config.async_session_factory() as session:
        async with session.begin():
            await _create_group(
                session,
                group_id="dg-empty",
                state="Empty",
                generation=2,
                selected_protocol=None,
                leader_member_id=None,
            )
            prep_group = await _create_group(
                session,
                group_id="dg-prep",
                state="PreparingRebalance",
                generation=3,
                selected_protocol="range",
                leader_member_id="m-prep",
            )
            complete_group = await _create_group(
                session,
                group_id="dg-completing",
                state="CompletingRebalance",
                generation=4,
                selected_protocol="range",
                leader_member_id="m-comp",
            )
            stable_group = await _create_group(
                session,
                group_id="dg-stable",
                state="Stable",
                generation=5,
                selected_protocol="range",
                leader_member_id="m-stable",
            )
            await _create_member(
                session,
                group=prep_group,
                member_id="m-prep",
                generation=3,
                assignment=b"prep",
            )
            await _create_member(
                session,
                group=complete_group,
                member_id="m-comp",
                generation=4,
                assignment=b"comp",
            )
            await _create_member(
                session,
                group=stable_group,
                member_id="m-stable",
                generation=5,
                assignment=b"stable",
            )

    req = DescribeGroupsRequest(
        groups=("dg-empty", "dg-prep", "dg-completing", "dg-stable", "dg-missing"),
        include_authorized_operations=False,
    )
    resp = await do_describe_groups(config, req, api_version=5)
    by_group_id = {group.group_id: group for group in resp.groups}

    assert by_group_id["dg-empty"].error_code == ErrorCode.none
    assert by_group_id["dg-empty"].group_state == "Empty"
    assert by_group_id["dg-empty"].members == tuple()

    assert by_group_id["dg-prep"].error_code == ErrorCode.none
    assert by_group_id["dg-prep"].group_state == "PreparingRebalance"
    assert len(by_group_id["dg-prep"].members) == 1

    assert by_group_id["dg-completing"].error_code == ErrorCode.none
    assert by_group_id["dg-completing"].group_state == "CompletingRebalance"
    assert len(by_group_id["dg-completing"].members) == 1

    assert by_group_id["dg-stable"].error_code == ErrorCode.none
    assert by_group_id["dg-stable"].group_state == "Stable"
    assert len(by_group_id["dg-stable"].members) == 1

    assert by_group_id["dg-missing"].error_code == ErrorCode.group_id_not_found
    assert by_group_id["dg-missing"].group_state == "Dead"
