from kio.schema.delete_acls.v0.request import (
    DeleteAclsRequest as DeleteAclsRequestV0,
)
from kio.schema.delete_acls.v0.request import (
    RequestHeader as DeleteAclsRequestHeaderV0,
)
from kio.schema.delete_acls.v0.response import (
    DeleteAclsResponse as DeleteAclsResponseV0,
)
from kio.schema.delete_acls.v0.response import (
    ResponseHeader as DeleteAclsResponseHeaderV0,
)
from kio.schema.delete_acls.v1.request import (
    DeleteAclsRequest as DeleteAclsRequestV1,
)
from kio.schema.delete_acls.v1.request import (
    RequestHeader as DeleteAclsRequestHeaderV1,
)
from kio.schema.delete_acls.v1.response import (
    DeleteAclsResponse as DeleteAclsResponseV1,
)
from kio.schema.delete_acls.v1.response import (
    ResponseHeader as DeleteAclsResponseHeaderV1,
)
from kio.schema.delete_acls.v2.request import (
    DeleteAclsRequest as DeleteAclsRequestV2,
)
from kio.schema.delete_acls.v2.request import (
    RequestHeader as DeleteAclsRequestHeaderV2,
)
from kio.schema.delete_acls.v2.response import (
    DeleteAclsResponse as DeleteAclsResponseV2,
)
from kio.schema.delete_acls.v2.response import (
    ResponseHeader as DeleteAclsResponseHeaderV2,
)
from kio.schema.delete_acls.v3.request import (
    DeleteAclsRequest as DeleteAclsRequestV3,
)
from kio.schema.delete_acls.v3.request import (
    RequestHeader as DeleteAclsRequestHeaderV3,
)
from kio.schema.delete_acls.v3.response import (
    DeleteAclsResponse as DeleteAclsResponseV3,
)
from kio.schema.delete_acls.v3.response import (
    ResponseHeader as DeleteAclsResponseHeaderV3,
)

DeleteAclsRequestHeader = (
    DeleteAclsRequestHeaderV0
    | DeleteAclsRequestHeaderV1
    | DeleteAclsRequestHeaderV2
    | DeleteAclsRequestHeaderV3
)

DeleteAclsResponseHeader = (
    DeleteAclsResponseHeaderV0
    | DeleteAclsResponseHeaderV1
    | DeleteAclsResponseHeaderV2
    | DeleteAclsResponseHeaderV3
)

DeleteAclsRequest = (
    DeleteAclsRequestV0
    | DeleteAclsRequestV1
    | DeleteAclsRequestV2
    | DeleteAclsRequestV3
)

DeleteAclsResponse = (
    DeleteAclsResponseV0
    | DeleteAclsResponseV1
    | DeleteAclsResponseV2
    | DeleteAclsResponseV3
)
