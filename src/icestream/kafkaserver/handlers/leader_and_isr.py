from kio.schema.leader_and_isr.v0.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV0,
)
from kio.schema.leader_and_isr.v0.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV0,
)
from kio.schema.leader_and_isr.v0.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV0,
)
from kio.schema.leader_and_isr.v0.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV0,
)
from kio.schema.leader_and_isr.v1.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV1,
)
from kio.schema.leader_and_isr.v1.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV1,
)
from kio.schema.leader_and_isr.v1.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV1,
)
from kio.schema.leader_and_isr.v1.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV1,
)
from kio.schema.leader_and_isr.v2.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV2,
)
from kio.schema.leader_and_isr.v2.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV2,
)
from kio.schema.leader_and_isr.v2.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV2,
)
from kio.schema.leader_and_isr.v2.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV2,
)
from kio.schema.leader_and_isr.v3.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV3,
)
from kio.schema.leader_and_isr.v3.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV3,
)
from kio.schema.leader_and_isr.v3.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV3,
)
from kio.schema.leader_and_isr.v3.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV3,
)
from kio.schema.leader_and_isr.v4.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV4,
)
from kio.schema.leader_and_isr.v4.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV4,
)
from kio.schema.leader_and_isr.v4.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV4,
)
from kio.schema.leader_and_isr.v4.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV4,
)
from kio.schema.leader_and_isr.v5.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV5,
)
from kio.schema.leader_and_isr.v5.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV5,
)
from kio.schema.leader_and_isr.v5.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV5,
)
from kio.schema.leader_and_isr.v5.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV5,
)
from kio.schema.leader_and_isr.v6.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV6,
)
from kio.schema.leader_and_isr.v6.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV6,
)
from kio.schema.leader_and_isr.v6.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV6,
)
from kio.schema.leader_and_isr.v6.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV6,
)
from kio.schema.leader_and_isr.v7.request import (
    LeaderAndIsrRequest as LeaderAndIsrRequestV7,
)
from kio.schema.leader_and_isr.v7.request import (
    RequestHeader as LeaderAndIsrRequestHeaderV7,
)
from kio.schema.leader_and_isr.v7.response import (
    LeaderAndIsrResponse as LeaderAndIsrResponseV7,
)
from kio.schema.leader_and_isr.v7.response import (
    ResponseHeader as LeaderAndIsrResponseHeaderV7,
)


LeaderAndIsrRequestHeader = (
    LeaderAndIsrRequestHeaderV0
    | LeaderAndIsrRequestHeaderV1
    | LeaderAndIsrRequestHeaderV2
    | LeaderAndIsrRequestHeaderV3
    | LeaderAndIsrRequestHeaderV4
    | LeaderAndIsrRequestHeaderV5
    | LeaderAndIsrRequestHeaderV6
    | LeaderAndIsrRequestHeaderV7
)

LeaderAndIsrResponseHeader = (
    LeaderAndIsrResponseHeaderV0
    | LeaderAndIsrResponseHeaderV1
    | LeaderAndIsrResponseHeaderV2
    | LeaderAndIsrResponseHeaderV3
    | LeaderAndIsrResponseHeaderV4
    | LeaderAndIsrResponseHeaderV5
    | LeaderAndIsrResponseHeaderV6
    | LeaderAndIsrResponseHeaderV7
)

LeaderAndIsrRequest = (
    LeaderAndIsrRequestV0
    | LeaderAndIsrRequestV1
    | LeaderAndIsrRequestV2
    | LeaderAndIsrRequestV3
    | LeaderAndIsrRequestV4
    | LeaderAndIsrRequestV5
    | LeaderAndIsrRequestV6
    | LeaderAndIsrRequestV7
)

LeaderAndIsrResponse = (
    LeaderAndIsrResponseV0
    | LeaderAndIsrResponseV1
    | LeaderAndIsrResponseV2
    | LeaderAndIsrResponseV3
    | LeaderAndIsrResponseV4
    | LeaderAndIsrResponseV5
    | LeaderAndIsrResponseV6
    | LeaderAndIsrResponseV7
)
