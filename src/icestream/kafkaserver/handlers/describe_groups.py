from kio.schema.describe_groups.v0.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV0,
)
from kio.schema.describe_groups.v0.request import (
    RequestHeader as DescribeGroupsRequestHeaderV0,
)
from kio.schema.describe_groups.v0.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV0,
)
from kio.schema.describe_groups.v0.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV0,
)
from kio.schema.describe_groups.v1.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV1,
)
from kio.schema.describe_groups.v1.request import (
    RequestHeader as DescribeGroupsRequestHeaderV1,
)
from kio.schema.describe_groups.v1.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV1,
)
from kio.schema.describe_groups.v1.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV1,
)
from kio.schema.describe_groups.v2.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV2,
)
from kio.schema.describe_groups.v2.request import (
    RequestHeader as DescribeGroupsRequestHeaderV2,
)
from kio.schema.describe_groups.v2.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV2,
)
from kio.schema.describe_groups.v2.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV2,
)
from kio.schema.describe_groups.v3.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV3,
)
from kio.schema.describe_groups.v3.request import (
    RequestHeader as DescribeGroupsRequestHeaderV3,
)
from kio.schema.describe_groups.v3.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV3,
)
from kio.schema.describe_groups.v3.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV3,
)
from kio.schema.describe_groups.v4.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV4,
)
from kio.schema.describe_groups.v4.request import (
    RequestHeader as DescribeGroupsRequestHeaderV4,
)
from kio.schema.describe_groups.v4.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV4,
)
from kio.schema.describe_groups.v4.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV4,
)
from kio.schema.describe_groups.v5.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV5,
)
from kio.schema.describe_groups.v5.request import (
    RequestHeader as DescribeGroupsRequestHeaderV5,
)
from kio.schema.describe_groups.v5.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV5,
)
from kio.schema.describe_groups.v5.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV5,
)
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.static.constants import EntityType
from kio.static.primitive import i32
from sqlalchemy import select

from icestream.config import Config
from icestream.models.consumer_groups import ConsumerGroup, GroupMember, GroupMemberProtocol
from icestream.utils import zero_throttle

DescribeGroupsRequestHeader = (
    DescribeGroupsRequestHeaderV0
    | DescribeGroupsRequestHeaderV1
    | DescribeGroupsRequestHeaderV2
    | DescribeGroupsRequestHeaderV3
    | DescribeGroupsRequestHeaderV4
    | DescribeGroupsRequestHeaderV5
)

DescribeGroupsResponseHeader = (
    DescribeGroupsResponseHeaderV0
    | DescribeGroupsResponseHeaderV1
    | DescribeGroupsResponseHeaderV2
    | DescribeGroupsResponseHeaderV3
    | DescribeGroupsResponseHeaderV4
    | DescribeGroupsResponseHeaderV5
)

DescribeGroupsRequest = (
    DescribeGroupsRequestV0
    | DescribeGroupsRequestV1
    | DescribeGroupsRequestV2
    | DescribeGroupsRequestV3
    | DescribeGroupsRequestV4
    | DescribeGroupsRequestV5
)

DescribeGroupsResponse = (
    DescribeGroupsResponseV0
    | DescribeGroupsResponseV1
    | DescribeGroupsResponseV2
    | DescribeGroupsResponseV3
    | DescribeGroupsResponseV4
    | DescribeGroupsResponseV5
)


def _group_element_cls(response_cls):
    groups_field = response_cls.__dataclass_fields__["groups"]
    return groups_field.type.__args__[0]


def _member_element_cls(group_cls):
    members_field = group_cls.__dataclass_fields__["members"]
    return members_field.type.__args__[0]


async def do_describe_groups(
    config: Config,
    req: DescribeGroupsRequest,
    api_version: int,
) -> DescribeGroupsResponse:
    assert config.async_session_factory is not None

    requested_group_ids = [str(group_id) for group_id in getattr(req, "groups", ())]

    async with config.async_session_factory() as session:
        groups = (
            await session.execute(
                select(ConsumerGroup).where(
                    ConsumerGroup.group_id.in_(requested_group_ids or [""])
                )
            )
        ).scalars().all()
        groups_by_id = {group.group_id: group for group in groups}

        members = (
            await session.execute(
                select(GroupMember).where(
                    GroupMember.consumer_group_id.in_([group.id for group in groups] or [-1])
                )
            )
        ).scalars().all()

    members_by_group_id: dict[int, list[GroupMember]] = {}
    for member in members:
        members_by_group_id.setdefault(member.consumer_group_id, []).append(member)

    mod = load_payload_module(15, api_version, EntityType.response)
    response_cls = mod.DescribeGroupsResponse
    response_fields = getattr(response_cls, "__dataclass_fields__", {})
    group_cls = _group_element_cls(response_cls)
    group_fields = getattr(group_cls, "__dataclass_fields__", {})
    member_cls = _member_element_cls(group_cls)
    member_fields = getattr(member_cls, "__dataclass_fields__", {})

    described_groups = []
    for requested_id in requested_group_ids:
        group = groups_by_id.get(requested_id)
        if group is None:
            missing_kwargs = {
                "error_code": ErrorCode.group_id_not_found,
                "group_id": requested_id,
                "group_state": "Dead",
                "protocol_type": "",
                "protocol_data": "",
                "members": tuple(),
            }
            if "authorized_operations" in group_fields:
                missing_kwargs["authorized_operations"] = i32(0)
            described_groups.append(group_cls(**missing_kwargs))
            continue

        group_members = sorted(
            members_by_group_id.get(group.id, ()),
            key=lambda row: row.member_id,
        )
        selected_protocol = group.selected_protocol or ""
        metadata_by_member_id: dict[int, bytes] = {}

        if selected_protocol and group_members:
            member_ids = [member.id for member in group_members]
            async with config.async_session_factory() as session:
                protocol_rows = (
                    await session.execute(
                        select(
                            GroupMemberProtocol.group_member_id,
                            GroupMemberProtocol.protocol_metadata,
                        ).where(
                            GroupMemberProtocol.group_member_id.in_(member_ids),
                            GroupMemberProtocol.name == selected_protocol,
                        )
                    )
                ).all()
            metadata_by_member_id = {
                int(member_id): protocol_metadata or b""
                for member_id, protocol_metadata in protocol_rows
            }

        described_members = []
        for member in group_members:
            member_kwargs = {
                "member_id": member.member_id,
                "client_id": "",
                "client_host": "",
                "member_metadata": metadata_by_member_id.get(member.id, b""),
                "member_assignment": member.assignment or b"",
            }
            if "group_instance_id" in member_fields:
                member_kwargs["group_instance_id"] = member.group_instance_id
            described_members.append(member_cls(**member_kwargs))

        group_kwargs = {
            "error_code": ErrorCode.none,
            "group_id": group.group_id,
            "group_state": group.state,
            "protocol_type": group.protocol_type or "",
            "protocol_data": selected_protocol,
            "members": tuple(described_members),
        }
        if "authorized_operations" in group_fields:
            group_kwargs["authorized_operations"] = i32(0)
        described_groups.append(group_cls(**group_kwargs))

    response_kwargs = {"groups": tuple(described_groups)}
    if "throttle_time" in response_fields:
        response_kwargs["throttle_time"] = zero_throttle()
    return response_cls(**response_kwargs)
