from kio.schema.heartbeat.v0.request import HeartbeatRequest as HeartbeatRequestV0
from kio.schema.heartbeat.v0.request import (
    RequestHeader as HeartbeatRequestHeaderV0,
)
from kio.schema.heartbeat.v0.response import (
    HeartbeatResponse as HeartbeatResponseV0,
)
from kio.schema.heartbeat.v0.response import (
    ResponseHeader as HeartbeatResponseHeaderV0,
)
from kio.schema.heartbeat.v1.request import HeartbeatRequest as HeartbeatRequestV1
from kio.schema.heartbeat.v1.request import (
    RequestHeader as HeartbeatRequestHeaderV1,
)
from kio.schema.heartbeat.v1.response import (
    HeartbeatResponse as HeartbeatResponseV1,
)
from kio.schema.heartbeat.v1.response import (
    ResponseHeader as HeartbeatResponseHeaderV1,
)
from kio.schema.heartbeat.v2.request import HeartbeatRequest as HeartbeatRequestV2
from kio.schema.heartbeat.v2.request import (
    RequestHeader as HeartbeatRequestHeaderV2,
)
from kio.schema.heartbeat.v2.response import (
    HeartbeatResponse as HeartbeatResponseV2,
)
from kio.schema.heartbeat.v2.response import (
    ResponseHeader as HeartbeatResponseHeaderV2,
)
from kio.schema.heartbeat.v3.request import HeartbeatRequest as HeartbeatRequestV3
from kio.schema.heartbeat.v3.request import (
    RequestHeader as HeartbeatRequestHeaderV3,
)
from kio.schema.heartbeat.v3.response import (
    HeartbeatResponse as HeartbeatResponseV3,
)
from kio.schema.heartbeat.v3.response import (
    ResponseHeader as HeartbeatResponseHeaderV3,
)
from kio.schema.heartbeat.v4.request import HeartbeatRequest as HeartbeatRequestV4
from kio.schema.heartbeat.v4.request import (
    RequestHeader as HeartbeatRequestHeaderV4,
)
from kio.schema.heartbeat.v4.response import (
    HeartbeatResponse as HeartbeatResponseV4,
)
from kio.schema.heartbeat.v4.response import (
    ResponseHeader as HeartbeatResponseHeaderV4,
)

HeartbeatRequestHeader = (
    HeartbeatRequestHeaderV0
    | HeartbeatRequestHeaderV1
    | HeartbeatRequestHeaderV2
    | HeartbeatRequestHeaderV3
    | HeartbeatRequestHeaderV4
)

HeartbeatResponseHeader = (
    HeartbeatResponseHeaderV0
    | HeartbeatResponseHeaderV1
    | HeartbeatResponseHeaderV2
    | HeartbeatResponseHeaderV3
    | HeartbeatResponseHeaderV4
)

HeartbeatRequest = (
    HeartbeatRequestV0
    | HeartbeatRequestV1
    | HeartbeatRequestV2
    | HeartbeatRequestV3
    | HeartbeatRequestV4
)

HeartbeatResponse = (
    HeartbeatResponseV0
    | HeartbeatResponseV1
    | HeartbeatResponseV2
    | HeartbeatResponseV3
    | HeartbeatResponseV4
)
