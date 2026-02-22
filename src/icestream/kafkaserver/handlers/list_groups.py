from kio.schema.list_groups.v0.request import (
    ListGroupsRequest as ListGroupsRequestV0,
)
from kio.schema.list_groups.v0.request import (
    RequestHeader as ListGroupsRequestHeaderV0,
)
from kio.schema.list_groups.v0.response import (
    ListGroupsResponse as ListGroupsResponseV0,
)
from kio.schema.list_groups.v0.response import (
    ResponseHeader as ListGroupsResponseHeaderV0,
)
from kio.schema.list_groups.v1.request import (
    ListGroupsRequest as ListGroupsRequestV1,
)
from kio.schema.list_groups.v1.request import (
    RequestHeader as ListGroupsRequestHeaderV1,
)
from kio.schema.list_groups.v1.response import (
    ListGroupsResponse as ListGroupsResponseV1,
)
from kio.schema.list_groups.v1.response import (
    ResponseHeader as ListGroupsResponseHeaderV1,
)
from kio.schema.list_groups.v2.request import (
    ListGroupsRequest as ListGroupsRequestV2,
)
from kio.schema.list_groups.v2.request import (
    RequestHeader as ListGroupsRequestHeaderV2,
)
from kio.schema.list_groups.v2.response import (
    ListGroupsResponse as ListGroupsResponseV2,
)
from kio.schema.list_groups.v2.response import (
    ResponseHeader as ListGroupsResponseHeaderV2,
)
from kio.schema.list_groups.v3.request import (
    ListGroupsRequest as ListGroupsRequestV3,
)
from kio.schema.list_groups.v3.request import (
    RequestHeader as ListGroupsRequestHeaderV3,
)
from kio.schema.list_groups.v3.response import (
    ListGroupsResponse as ListGroupsResponseV3,
)
from kio.schema.list_groups.v3.response import (
    ResponseHeader as ListGroupsResponseHeaderV3,
)
from kio.schema.list_groups.v4.request import (
    ListGroupsRequest as ListGroupsRequestV4,
)
from kio.schema.list_groups.v4.request import (
    RequestHeader as ListGroupsRequestHeaderV4,
)
from kio.schema.list_groups.v4.response import (
    ListGroupsResponse as ListGroupsResponseV4,
)
from kio.schema.list_groups.v4.response import (
    ResponseHeader as ListGroupsResponseHeaderV4,
)
from kio.schema.list_groups.v5.request import (
    ListGroupsRequest as ListGroupsRequestV5,
)
from kio.schema.list_groups.v5.request import (
    RequestHeader as ListGroupsRequestHeaderV5,
)
from kio.schema.list_groups.v5.response import (
    ListGroupsResponse as ListGroupsResponseV5,
)
from kio.schema.list_groups.v5.response import (
    ResponseHeader as ListGroupsResponseHeaderV5,
)
from kio.index import load_payload_module
from kio.schema.errors import ErrorCode
from kio.static.constants import EntityType
from sqlalchemy import select

from icestream.config import Config
from icestream.models.consumer_groups import ConsumerGroup
from icestream.utils import zero_throttle

ListGroupsRequestHeader = (
    ListGroupsRequestHeaderV0
    | ListGroupsRequestHeaderV1
    | ListGroupsRequestHeaderV2
    | ListGroupsRequestHeaderV3
    | ListGroupsRequestHeaderV4
    | ListGroupsRequestHeaderV5
)

ListGroupsResponseHeader = (
    ListGroupsResponseHeaderV0
    | ListGroupsResponseHeaderV1
    | ListGroupsResponseHeaderV2
    | ListGroupsResponseHeaderV3
    | ListGroupsResponseHeaderV4
    | ListGroupsResponseHeaderV5
)

ListGroupsRequest = (
    ListGroupsRequestV0
    | ListGroupsRequestV1
    | ListGroupsRequestV2
    | ListGroupsRequestV3
    | ListGroupsRequestV4
    | ListGroupsRequestV5
)

ListGroupsResponse = (
    ListGroupsResponseV0
    | ListGroupsResponseV1
    | ListGroupsResponseV2
    | ListGroupsResponseV3
    | ListGroupsResponseV4
    | ListGroupsResponseV5
)


def _normalize_filter_values(values: object) -> set[str]:
    if values is None:
        return set()
    return {str(value) for value in values if str(value)}


def _group_element_cls(response_cls):
    groups_field = response_cls.__dataclass_fields__["groups"]
    return groups_field.type.__args__[0]


async def do_list_groups(
    config: Config,
    req: ListGroupsRequest,
    api_version: int,
) -> ListGroupsResponse:
    assert config.async_session_factory is not None

    state_filter = _normalize_filter_values(getattr(req, "states_filter", ()))
    type_filter = _normalize_filter_values(getattr(req, "types_filter", ()))
    group_type = "classic"

    async with config.async_session_factory() as session:
        groups = (
            await session.execute(
                select(ConsumerGroup).order_by(ConsumerGroup.group_id)
            )
        ).scalars().all()

    mod = load_payload_module(16, api_version, EntityType.response)
    response_cls = mod.ListGroupsResponse
    response_fields = getattr(response_cls, "__dataclass_fields__", {})
    group_cls = _group_element_cls(response_cls)
    group_fields = getattr(group_cls, "__dataclass_fields__", {})

    listed = []
    for group in groups:
        if state_filter and group.state not in state_filter:
            continue
        if type_filter and group_type not in type_filter:
            continue

        entry_kwargs = {
            "group_id": group.group_id,
            "protocol_type": group.protocol_type or "",
        }
        if "group_state" in group_fields:
            entry_kwargs["group_state"] = group.state
        if "group_type" in group_fields:
            entry_kwargs["group_type"] = group_type
        listed.append(group_cls(**entry_kwargs))

    response_kwargs = {
        "error_code": ErrorCode.none,
        "groups": tuple(listed),
    }
    if "throttle_time" in response_fields:
        response_kwargs["throttle_time"] = zero_throttle()

    return response_cls(**response_kwargs)
