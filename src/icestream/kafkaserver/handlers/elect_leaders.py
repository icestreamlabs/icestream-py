from kio.schema.elect_leaders.v0.request import (
    ElectLeadersRequest as ElectLeadersRequestV0,
)
from kio.schema.elect_leaders.v0.request import (
    RequestHeader as ElectLeadersRequestHeaderV0,
)
from kio.schema.elect_leaders.v0.response import (
    ElectLeadersResponse as ElectLeadersResponseV0,
)
from kio.schema.elect_leaders.v0.response import (
    ResponseHeader as ElectLeadersResponseHeaderV0,
)
from kio.schema.elect_leaders.v1.request import (
    ElectLeadersRequest as ElectLeadersRequestV1,
)
from kio.schema.elect_leaders.v1.request import (
    RequestHeader as ElectLeadersRequestHeaderV1,
)
from kio.schema.elect_leaders.v1.response import (
    ElectLeadersResponse as ElectLeadersResponseV1,
)
from kio.schema.elect_leaders.v1.response import (
    ResponseHeader as ElectLeadersResponseHeaderV1,
)
from kio.schema.elect_leaders.v2.request import (
    ElectLeadersRequest as ElectLeadersRequestV2,
)
from kio.schema.elect_leaders.v2.request import (
    RequestHeader as ElectLeadersRequestHeaderV2,
)
from kio.schema.elect_leaders.v2.response import (
    ElectLeadersResponse as ElectLeadersResponseV2,
)
from kio.schema.elect_leaders.v2.response import (
    ResponseHeader as ElectLeadersResponseHeaderV2,
)

ElectLeadersRequestHeader = (
    ElectLeadersRequestHeaderV0
    | ElectLeadersRequestHeaderV1
    | ElectLeadersRequestHeaderV2
)

ElectLeadersResponseHeader = (
    ElectLeadersResponseHeaderV0
    | ElectLeadersResponseHeaderV1
    | ElectLeadersResponseHeaderV2
)

ElectLeadersRequest = (
    ElectLeadersRequestV0
    | ElectLeadersRequestV1
    | ElectLeadersRequestV2
)

ElectLeadersResponse = (
    ElectLeadersResponseV0
    | ElectLeadersResponseV1
    | ElectLeadersResponseV2
)
