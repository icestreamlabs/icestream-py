from kio.schema.sync_group.v0.request import (
    SyncGroupRequest as SyncGroupRequestV0,
)
from kio.schema.sync_group.v0.request import (
    RequestHeader as SyncGroupRequestHeaderV0,
)
from kio.schema.sync_group.v0.response import (
    SyncGroupResponse as SyncGroupResponseV0,
)
from kio.schema.sync_group.v0.response import (
    ResponseHeader as SyncGroupResponseHeaderV0,
)
from kio.schema.sync_group.v1.request import (
    SyncGroupRequest as SyncGroupRequestV1,
)
from kio.schema.sync_group.v1.request import (
    RequestHeader as SyncGroupRequestHeaderV1,
)
from kio.schema.sync_group.v1.response import (
    SyncGroupResponse as SyncGroupResponseV1,
)
from kio.schema.sync_group.v1.response import (
    ResponseHeader as SyncGroupResponseHeaderV1,
)
from kio.schema.sync_group.v2.request import (
    SyncGroupRequest as SyncGroupRequestV2,
)
from kio.schema.sync_group.v2.request import (
    RequestHeader as SyncGroupRequestHeaderV2,
)
from kio.schema.sync_group.v2.response import (
    SyncGroupResponse as SyncGroupResponseV2,
)
from kio.schema.sync_group.v2.response import (
    ResponseHeader as SyncGroupResponseHeaderV2,
)
from kio.schema.sync_group.v3.request import (
    SyncGroupRequest as SyncGroupRequestV3,
)
from kio.schema.sync_group.v3.request import (
    RequestHeader as SyncGroupRequestHeaderV3,
)
from kio.schema.sync_group.v3.response import (
    SyncGroupResponse as SyncGroupResponseV3,
)
from kio.schema.sync_group.v3.response import (
    ResponseHeader as SyncGroupResponseHeaderV3,
)
from kio.schema.sync_group.v4.request import (
    SyncGroupRequest as SyncGroupRequestV4,
)
from kio.schema.sync_group.v4.request import (
    RequestHeader as SyncGroupRequestHeaderV4,
)
from kio.schema.sync_group.v4.response import (
    SyncGroupResponse as SyncGroupResponseV4,
)
from kio.schema.sync_group.v4.response import (
    ResponseHeader as SyncGroupResponseHeaderV4,
)
from kio.schema.sync_group.v5.request import (
    SyncGroupRequest as SyncGroupRequestV5,
)
from kio.schema.sync_group.v5.request import (
    RequestHeader as SyncGroupRequestHeaderV5,
)
from kio.schema.sync_group.v5.response import (
    SyncGroupResponse as SyncGroupResponseV5,
)
from kio.schema.sync_group.v5.response import (
    ResponseHeader as SyncGroupResponseHeaderV5,
)

SyncGroupRequestHeader = (
    SyncGroupRequestHeaderV0
    | SyncGroupRequestHeaderV1
    | SyncGroupRequestHeaderV2
    | SyncGroupRequestHeaderV3
    | SyncGroupRequestHeaderV4
    | SyncGroupRequestHeaderV5
)

SyncGroupResponseHeader = (
    SyncGroupResponseHeaderV0
    | SyncGroupResponseHeaderV1
    | SyncGroupResponseHeaderV2
    | SyncGroupResponseHeaderV3
    | SyncGroupResponseHeaderV4
    | SyncGroupResponseHeaderV5
)

SyncGroupRequest = (
    SyncGroupRequestV0
    | SyncGroupRequestV1
    | SyncGroupRequestV2
    | SyncGroupRequestV3
    | SyncGroupRequestV4
    | SyncGroupRequestV5
)

SyncGroupResponse = (
    SyncGroupResponseV0
    | SyncGroupResponseV1
    | SyncGroupResponseV2
    | SyncGroupResponseV3
    | SyncGroupResponseV4
    | SyncGroupResponseV5
)
