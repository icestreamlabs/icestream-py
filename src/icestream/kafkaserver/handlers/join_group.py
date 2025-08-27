from kio.schema.join_group.v0.request import JoinGroupRequest as JoinGroupRequestV0
from kio.schema.join_group.v0.request import (
    RequestHeader as JoinGroupRequestHeaderV0,
)
from kio.schema.join_group.v0.response import (
    JoinGroupResponse as JoinGroupResponseV0,
)
from kio.schema.join_group.v0.response import (
    ResponseHeader as JoinGroupResponseHeaderV0,
)
from kio.schema.join_group.v1.request import JoinGroupRequest as JoinGroupRequestV1
from kio.schema.join_group.v1.request import (
    RequestHeader as JoinGroupRequestHeaderV1,
)
from kio.schema.join_group.v1.response import (
    JoinGroupResponse as JoinGroupResponseV1,
)
from kio.schema.join_group.v1.response import (
    ResponseHeader as JoinGroupResponseHeaderV1,
)
from kio.schema.join_group.v2.request import JoinGroupRequest as JoinGroupRequestV2
from kio.schema.join_group.v2.request import (
    RequestHeader as JoinGroupRequestHeaderV2,
)
from kio.schema.join_group.v2.response import (
    JoinGroupResponse as JoinGroupResponseV2,
)
from kio.schema.join_group.v2.response import (
    ResponseHeader as JoinGroupResponseHeaderV2,
)
from kio.schema.join_group.v3.request import JoinGroupRequest as JoinGroupRequestV3
from kio.schema.join_group.v3.request import (
    RequestHeader as JoinGroupRequestHeaderV3,
)
from kio.schema.join_group.v3.response import (
    JoinGroupResponse as JoinGroupResponseV3,
)
from kio.schema.join_group.v3.response import (
    ResponseHeader as JoinGroupResponseHeaderV3,
)
from kio.schema.join_group.v4.request import JoinGroupRequest as JoinGroupRequestV4
from kio.schema.join_group.v4.request import (
    RequestHeader as JoinGroupRequestHeaderV4,
)
from kio.schema.join_group.v4.response import (
    JoinGroupResponse as JoinGroupResponseV4,
)
from kio.schema.join_group.v4.response import (
    ResponseHeader as JoinGroupResponseHeaderV4,
)
from kio.schema.join_group.v5.request import JoinGroupRequest as JoinGroupRequestV5
from kio.schema.join_group.v5.request import (
    RequestHeader as JoinGroupRequestHeaderV5,
)
from kio.schema.join_group.v5.response import (
    JoinGroupResponse as JoinGroupResponseV5,
)
from kio.schema.join_group.v5.response import (
    ResponseHeader as JoinGroupResponseHeaderV5,
)
from kio.schema.join_group.v6.request import JoinGroupRequest as JoinGroupRequestV6
from kio.schema.join_group.v6.request import (
    RequestHeader as JoinGroupRequestHeaderV6,
)
from kio.schema.join_group.v6.response import (
    JoinGroupResponse as JoinGroupResponseV6,
)
from kio.schema.join_group.v6.response import (
    ResponseHeader as JoinGroupResponseHeaderV6,
)
from kio.schema.join_group.v7.request import JoinGroupRequest as JoinGroupRequestV7
from kio.schema.join_group.v7.request import (
    RequestHeader as JoinGroupRequestHeaderV7,
)
from kio.schema.join_group.v7.response import (
    JoinGroupResponse as JoinGroupResponseV7,
)
from kio.schema.join_group.v7.response import (
    ResponseHeader as JoinGroupResponseHeaderV7,
)
from kio.schema.join_group.v8.request import JoinGroupRequest as JoinGroupRequestV8
from kio.schema.join_group.v8.request import (
    RequestHeader as JoinGroupRequestHeaderV8,
)
from kio.schema.join_group.v8.response import (
    JoinGroupResponse as JoinGroupResponseV8,
)
from kio.schema.join_group.v8.response import (
    ResponseHeader as JoinGroupResponseHeaderV8,
)
from kio.schema.join_group.v9.request import JoinGroupRequest as JoinGroupRequestV9
from kio.schema.join_group.v9.request import (
    RequestHeader as JoinGroupRequestHeaderV9,
)
from kio.schema.join_group.v9.response import (
    JoinGroupResponse as JoinGroupResponseV9,
)
from kio.schema.join_group.v9.response import (
    ResponseHeader as JoinGroupResponseHeaderV9,
)

JoinGroupRequestHeader = (
    JoinGroupRequestHeaderV0
    | JoinGroupRequestHeaderV1
    | JoinGroupRequestHeaderV2
    | JoinGroupRequestHeaderV3
    | JoinGroupRequestHeaderV4
    | JoinGroupRequestHeaderV5
    | JoinGroupRequestHeaderV6
    | JoinGroupRequestHeaderV7
    | JoinGroupRequestHeaderV8
    | JoinGroupRequestHeaderV9
)

JoinGroupResponseHeader = (
    JoinGroupResponseHeaderV0
    | JoinGroupResponseHeaderV1
    | JoinGroupResponseHeaderV2
    | JoinGroupResponseHeaderV3
    | JoinGroupResponseHeaderV4
    | JoinGroupResponseHeaderV5
    | JoinGroupResponseHeaderV6
    | JoinGroupResponseHeaderV7
    | JoinGroupResponseHeaderV8
    | JoinGroupResponseHeaderV9
)

JoinGroupRequest = (
    JoinGroupRequestV0
    | JoinGroupRequestV1
    | JoinGroupRequestV2
    | JoinGroupRequestV3
    | JoinGroupRequestV4
    | JoinGroupRequestV5
    | JoinGroupRequestV6
    | JoinGroupRequestV7
    | JoinGroupRequestV8
    | JoinGroupRequestV9
)

JoinGroupResponse = (
    JoinGroupResponseV0
    | JoinGroupResponseV1
    | JoinGroupResponseV2
    | JoinGroupResponseV3
    | JoinGroupResponseV4
    | JoinGroupResponseV5
    | JoinGroupResponseV6
    | JoinGroupResponseV7
    | JoinGroupResponseV8
    | JoinGroupResponseV9
)
