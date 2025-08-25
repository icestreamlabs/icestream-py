from kio.schema.delete_groups.v0.request import (
    DeleteGroupsRequest as DeleteGroupsRequestV0,
)
from kio.schema.delete_groups.v0.request import (
    RequestHeader as DeleteGroupsRequestHeaderV0,
)
from kio.schema.delete_groups.v0.response import (
    DeleteGroupsResponse as DeleteGroupsResponseV0,
)
from kio.schema.delete_groups.v0.response import (
    ResponseHeader as DeleteGroupsResponseHeaderV0,
)
from kio.schema.delete_groups.v1.request import (
    DeleteGroupsRequest as DeleteGroupsRequestV1,
)
from kio.schema.delete_groups.v1.request import (
    RequestHeader as DeleteGroupsRequestHeaderV1,
)
from kio.schema.delete_groups.v1.response import (
    DeleteGroupsResponse as DeleteGroupsResponseV1,
)
from kio.schema.delete_groups.v1.response import (
    ResponseHeader as DeleteGroupsResponseHeaderV1,
)
from kio.schema.delete_groups.v2.request import (
    DeleteGroupsRequest as DeleteGroupsRequestV2,
)
from kio.schema.delete_groups.v2.request import (
    RequestHeader as DeleteGroupsRequestHeaderV2,
)
from kio.schema.delete_groups.v2.response import (
    DeleteGroupsResponse as DeleteGroupsResponseV2,
)
from kio.schema.delete_groups.v2.response import (
    ResponseHeader as DeleteGroupsResponseHeaderV2,
)

DeleteGroupsRequestHeader = (
    DeleteGroupsRequestHeaderV0
    | DeleteGroupsRequestHeaderV1
    | DeleteGroupsRequestHeaderV2
)

DeleteGroupsResponseHeader = (
    DeleteGroupsResponseHeaderV0
    | DeleteGroupsResponseHeaderV1
    | DeleteGroupsResponseHeaderV2
)

DeleteGroupsRequest = (
    DeleteGroupsRequestV0
    | DeleteGroupsRequestV1
    | DeleteGroupsRequestV2
)

DeleteGroupsResponse = (
    DeleteGroupsResponseV0
    | DeleteGroupsResponseV1
    | DeleteGroupsResponseV2
)
