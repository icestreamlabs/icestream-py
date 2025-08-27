from kio.schema.find_coordinator.v0.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV0,
)
from kio.schema.find_coordinator.v0.request import (
    RequestHeader as FindCoordinatorRequestHeaderV0,
)
from kio.schema.find_coordinator.v0.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV0,
)
from kio.schema.find_coordinator.v0.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV0,
)
from kio.schema.find_coordinator.v1.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV1,
)
from kio.schema.find_coordinator.v1.request import (
    RequestHeader as FindCoordinatorRequestHeaderV1,
)
from kio.schema.find_coordinator.v1.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV1,
)
from kio.schema.find_coordinator.v1.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV1,
)
from kio.schema.find_coordinator.v2.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV2,
)
from kio.schema.find_coordinator.v2.request import (
    RequestHeader as FindCoordinatorRequestHeaderV2,
)
from kio.schema.find_coordinator.v2.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV2,
)
from kio.schema.find_coordinator.v2.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV2,
)
from kio.schema.find_coordinator.v3.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV3,
)
from kio.schema.find_coordinator.v3.request import (
    RequestHeader as FindCoordinatorRequestHeaderV3,
)
from kio.schema.find_coordinator.v3.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV3,
)
from kio.schema.find_coordinator.v3.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV3,
)
from kio.schema.find_coordinator.v4.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV4,
)
from kio.schema.find_coordinator.v4.request import (
    RequestHeader as FindCoordinatorRequestHeaderV4,
)
from kio.schema.find_coordinator.v4.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV4,
)
from kio.schema.find_coordinator.v4.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV4,
)
from kio.schema.find_coordinator.v5.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV5,
)
from kio.schema.find_coordinator.v5.request import (
    RequestHeader as FindCoordinatorRequestHeaderV5,
)
from kio.schema.find_coordinator.v5.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV5,
)
from kio.schema.find_coordinator.v5.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV5,
)
from kio.schema.find_coordinator.v6.request import (
    FindCoordinatorRequest as FindCoordinatorRequestV6,
)
from kio.schema.find_coordinator.v6.request import (
    RequestHeader as FindCoordinatorRequestHeaderV6,
)
from kio.schema.find_coordinator.v6.response import (
    FindCoordinatorResponse as FindCoordinatorResponseV6,
)
from kio.schema.find_coordinator.v6.response import (
    ResponseHeader as FindCoordinatorResponseHeaderV6,
)

FindCoordinatorRequestHeader = (
    FindCoordinatorRequestHeaderV0
    | FindCoordinatorRequestHeaderV1
    | FindCoordinatorRequestHeaderV2
    | FindCoordinatorRequestHeaderV3
    | FindCoordinatorRequestHeaderV4
    | FindCoordinatorRequestHeaderV5
    | FindCoordinatorRequestHeaderV6
)

FindCoordinatorResponseHeader = (
    FindCoordinatorResponseHeaderV0
    | FindCoordinatorResponseHeaderV1
    | FindCoordinatorResponseHeaderV2
    | FindCoordinatorResponseHeaderV3
    | FindCoordinatorResponseHeaderV4
    | FindCoordinatorResponseHeaderV5
    | FindCoordinatorResponseHeaderV6
)

FindCoordinatorRequest = (
    FindCoordinatorRequestV0
    | FindCoordinatorRequestV1
    | FindCoordinatorRequestV2
    | FindCoordinatorRequestV3
    | FindCoordinatorRequestV4
    | FindCoordinatorRequestV5
    | FindCoordinatorRequestV6
)

FindCoordinatorResponse = (
    FindCoordinatorResponseV0
    | FindCoordinatorResponseV1
    | FindCoordinatorResponseV2
    | FindCoordinatorResponseV3
    | FindCoordinatorResponseV4
    | FindCoordinatorResponseV5
    | FindCoordinatorResponseV6
)
