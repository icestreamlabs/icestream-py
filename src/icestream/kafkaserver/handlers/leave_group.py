from kio.schema.leave_group.v0.request import (
    LeaveGroupRequest as LeaveGroupRequestV0,
)
from kio.schema.leave_group.v0.request import (
    RequestHeader as LeaveGroupRequestHeaderV0,
)
from kio.schema.leave_group.v0.response import (
    LeaveGroupResponse as LeaveGroupResponseV0,
)
from kio.schema.leave_group.v0.response import (
    ResponseHeader as LeaveGroupResponseHeaderV0,
)
from kio.schema.leave_group.v1.request import (
    LeaveGroupRequest as LeaveGroupRequestV1,
)
from kio.schema.leave_group.v1.request import (
    RequestHeader as LeaveGroupRequestHeaderV1,
)
from kio.schema.leave_group.v1.response import (
    LeaveGroupResponse as LeaveGroupResponseV1,
)
from kio.schema.leave_group.v1.response import (
    ResponseHeader as LeaveGroupResponseHeaderV1,
)
from kio.schema.leave_group.v2.request import (
    LeaveGroupRequest as LeaveGroupRequestV2,
)
from kio.schema.leave_group.v2.request import (
    RequestHeader as LeaveGroupRequestHeaderV2,
)
from kio.schema.leave_group.v2.response import (
    LeaveGroupResponse as LeaveGroupResponseV2,
)
from kio.schema.leave_group.v2.response import (
    ResponseHeader as LeaveGroupResponseHeaderV2,
)
from kio.schema.leave_group.v3.request import (
    LeaveGroupRequest as LeaveGroupRequestV3,
)
from kio.schema.leave_group.v3.request import (
    RequestHeader as LeaveGroupRequestHeaderV3,
)
from kio.schema.leave_group.v3.response import (
    LeaveGroupResponse as LeaveGroupResponseV3,
)
from kio.schema.leave_group.v3.response import (
    ResponseHeader as LeaveGroupResponseHeaderV3,
)
from kio.schema.leave_group.v4.request import (
    LeaveGroupRequest as LeaveGroupRequestV4,
)
from kio.schema.leave_group.v4.request import (
    RequestHeader as LeaveGroupRequestHeaderV4,
)
from kio.schema.leave_group.v4.response import (
    LeaveGroupResponse as LeaveGroupResponseV4,
)
from kio.schema.leave_group.v4.response import (
    ResponseHeader as LeaveGroupResponseHeaderV4,
)
from kio.schema.leave_group.v5.request import (
    LeaveGroupRequest as LeaveGroupRequestV5,
)
from kio.schema.leave_group.v5.request import (
    RequestHeader as LeaveGroupRequestHeaderV5,
)
from kio.schema.leave_group.v5.response import (
    LeaveGroupResponse as LeaveGroupResponseV5,
)
from kio.schema.leave_group.v5.response import (
    ResponseHeader as LeaveGroupResponseHeaderV5,
)


LeaveGroupRequestHeader = (
    LeaveGroupRequestHeaderV0
    | LeaveGroupRequestHeaderV1
    | LeaveGroupRequestHeaderV2
    | LeaveGroupRequestHeaderV3
    | LeaveGroupRequestHeaderV4
    | LeaveGroupRequestHeaderV5
)

LeaveGroupResponseHeader = (
    LeaveGroupResponseHeaderV0
    | LeaveGroupResponseHeaderV1
    | LeaveGroupResponseHeaderV2
    | LeaveGroupResponseHeaderV3
    | LeaveGroupResponseHeaderV4
    | LeaveGroupResponseHeaderV5
)

LeaveGroupRequest = (
    LeaveGroupRequestV0
    | LeaveGroupRequestV1
    | LeaveGroupRequestV2
    | LeaveGroupRequestV3
    | LeaveGroupRequestV4
    | LeaveGroupRequestV5
)

LeaveGroupResponse = (
    LeaveGroupResponseV0
    | LeaveGroupResponseV1
    | LeaveGroupResponseV2
    | LeaveGroupResponseV3
    | LeaveGroupResponseV4
    | LeaveGroupResponseV5
)
