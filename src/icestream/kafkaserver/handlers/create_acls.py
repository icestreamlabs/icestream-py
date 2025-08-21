from kio.schema.create_acls.v0.request import CreateAclsRequest as CreateAclsRequestV0
from kio.schema.create_acls.v0.request import (
    RequestHeader as CreateAclsRequestHeaderV0,
)
from kio.schema.create_acls.v0.response import CreateAclsResponse as CreateAclsResponseV0
from kio.schema.create_acls.v0.response import (
    ResponseHeader as CreateAclsResponseHeaderV0,
)
from kio.schema.create_acls.v1.request import CreateAclsRequest as CreateAclsRequestV1
from kio.schema.create_acls.v1.request import (
    RequestHeader as CreateAclsRequestHeaderV1,
)
from kio.schema.create_acls.v1.response import CreateAclsResponse as CreateAclsResponseV1
from kio.schema.create_acls.v1.response import (
    ResponseHeader as CreateAclsResponseHeaderV1,
)
from kio.schema.create_acls.v2.request import CreateAclsRequest as CreateAclsRequestV2
from kio.schema.create_acls.v2.request import (
    RequestHeader as CreateAclsRequestHeaderV2,
)
from kio.schema.create_acls.v2.response import CreateAclsResponse as CreateAclsResponseV2
from kio.schema.create_acls.v2.response import (
    ResponseHeader as CreateAclsResponseHeaderV2,
)
from kio.schema.create_acls.v3.request import CreateAclsRequest as CreateAclsRequestV3
from kio.schema.create_acls.v3.request import (
    RequestHeader as CreateAclsRequestHeaderV3,
)
from kio.schema.create_acls.v3.response import CreateAclsResponse as CreateAclsResponseV3
from kio.schema.create_acls.v3.response import (
    ResponseHeader as CreateAclsResponseHeaderV3,
)

CreateAclsRequestHeader = (
    CreateAclsRequestHeaderV0
    | CreateAclsRequestHeaderV1
    | CreateAclsRequestHeaderV2
    | CreateAclsRequestHeaderV3
)

CreateAclsResponseHeader = (
    CreateAclsResponseHeaderV0
    | CreateAclsResponseHeaderV1
    | CreateAclsResponseHeaderV2
    | CreateAclsResponseHeaderV3
)

CreateAclsRequest = (
    CreateAclsRequestV0
    | CreateAclsRequestV1
    | CreateAclsRequestV2
    | CreateAclsRequestV3
)

CreateAclsResponse = (
    CreateAclsResponseV0
    | CreateAclsResponseV1
    | CreateAclsResponseV2
    | CreateAclsResponseV3
)
