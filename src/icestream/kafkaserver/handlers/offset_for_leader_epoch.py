from kio.schema.offset_for_leader_epoch.v0.request import (
    OffsetForLeaderEpochRequest as OffsetForLeaderEpochRequestV0,
)
from kio.schema.offset_for_leader_epoch.v0.request import (
    RequestHeader as OffsetForLeaderEpochRequestHeaderV0,
)
from kio.schema.offset_for_leader_epoch.v0.response import (
    OffsetForLeaderEpochResponse as OffsetForLeaderEpochResponseV0,
)
from kio.schema.offset_for_leader_epoch.v0.response import (
    ResponseHeader as OffsetForLeaderEpochResponseHeaderV0,
)
from kio.schema.offset_for_leader_epoch.v1.request import (
    OffsetForLeaderEpochRequest as OffsetForLeaderEpochRequestV1,
)
from kio.schema.offset_for_leader_epoch.v1.request import (
    RequestHeader as OffsetForLeaderEpochRequestHeaderV1,
)
from kio.schema.offset_for_leader_epoch.v1.response import (
    OffsetForLeaderEpochResponse as OffsetForLeaderEpochResponseV1,
)
from kio.schema.offset_for_leader_epoch.v1.response import (
    ResponseHeader as OffsetForLeaderEpochResponseHeaderV1,
)
from kio.schema.offset_for_leader_epoch.v2.request import (
    OffsetForLeaderEpochRequest as OffsetForLeaderEpochRequestV2,
)
from kio.schema.offset_for_leader_epoch.v2.request import (
    RequestHeader as OffsetForLeaderEpochRequestHeaderV2,
)
from kio.schema.offset_for_leader_epoch.v2.response import (
    OffsetForLeaderEpochResponse as OffsetForLeaderEpochResponseV2,
)
from kio.schema.offset_for_leader_epoch.v2.response import (
    ResponseHeader as OffsetForLeaderEpochResponseHeaderV2,
)
from kio.schema.offset_for_leader_epoch.v3.request import (
    OffsetForLeaderEpochRequest as OffsetForLeaderEpochRequestV3,
)
from kio.schema.offset_for_leader_epoch.v3.request import (
    RequestHeader as OffsetForLeaderEpochRequestHeaderV3,
)
from kio.schema.offset_for_leader_epoch.v3.response import (
    OffsetForLeaderEpochResponse as OffsetForLeaderEpochResponseV3,
)
from kio.schema.offset_for_leader_epoch.v3.response import (
    ResponseHeader as OffsetForLeaderEpochResponseHeaderV3,
)
from kio.schema.offset_for_leader_epoch.v4.request import (
    OffsetForLeaderEpochRequest as OffsetForLeaderEpochRequestV4,
)
from kio.schema.offset_for_leader_epoch.v4.request import (
    RequestHeader as OffsetForLeaderEpochRequestHeaderV4,
)
from kio.schema.offset_for_leader_epoch.v4.response import (
    OffsetForLeaderEpochResponse as OffsetForLeaderEpochResponseV4,
)
from kio.schema.offset_for_leader_epoch.v4.response import (
    ResponseHeader as OffsetForLeaderEpochResponseHeaderV4,
)


OffsetForLeaderEpochRequestHeader = (
    OffsetForLeaderEpochRequestHeaderV0
    | OffsetForLeaderEpochRequestHeaderV1
    | OffsetForLeaderEpochRequestHeaderV2
    | OffsetForLeaderEpochRequestHeaderV3
    | OffsetForLeaderEpochRequestHeaderV4
)

OffsetForLeaderEpochResponseHeader = (
    OffsetForLeaderEpochResponseHeaderV0
    | OffsetForLeaderEpochResponseHeaderV1
    | OffsetForLeaderEpochResponseHeaderV2
    | OffsetForLeaderEpochResponseHeaderV3
    | OffsetForLeaderEpochResponseHeaderV4
)

OffsetForLeaderEpochRequest = (
    OffsetForLeaderEpochRequestV0
    | OffsetForLeaderEpochRequestV1
    | OffsetForLeaderEpochRequestV2
    | OffsetForLeaderEpochRequestV3
    | OffsetForLeaderEpochRequestV4
)

OffsetForLeaderEpochResponse = (
    OffsetForLeaderEpochResponseV0
    | OffsetForLeaderEpochResponseV1
    | OffsetForLeaderEpochResponseV2
    | OffsetForLeaderEpochResponseV3
    | OffsetForLeaderEpochResponseV4
)
