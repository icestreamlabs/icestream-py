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
