from kio.schema.describe_acls.v0.request import (
    DescribeAclsRequest as DescribeAclsRequestV0,
)
from kio.schema.describe_acls.v0.request import (
    RequestHeader as DescribeAclsRequestHeaderV0,
)
from kio.schema.describe_acls.v0.response import (
    DescribeAclsResponse as DescribeAclsResponseV0,
)
from kio.schema.describe_acls.v0.response import (
    ResponseHeader as DescribeAclsResponseHeaderV0,
)
from kio.schema.describe_acls.v1.request import (
    DescribeAclsRequest as DescribeAclsRequestV1,
)
from kio.schema.describe_acls.v1.request import (
    RequestHeader as DescribeAclsRequestHeaderV1,
)
from kio.schema.describe_acls.v1.response import (
    DescribeAclsResponse as DescribeAclsResponseV1,
)
from kio.schema.describe_acls.v1.response import (
    ResponseHeader as DescribeAclsResponseHeaderV1,
)
from kio.schema.describe_acls.v2.request import (
    DescribeAclsRequest as DescribeAclsRequestV2,
)
from kio.schema.describe_acls.v2.request import (
    RequestHeader as DescribeAclsRequestHeaderV2,
)
from kio.schema.describe_acls.v2.response import (
    DescribeAclsResponse as DescribeAclsResponseV2,
)
from kio.schema.describe_acls.v2.response import (
    ResponseHeader as DescribeAclsResponseHeaderV2,
)
from kio.schema.describe_acls.v3.request import (
    DescribeAclsRequest as DescribeAclsRequestV3,
)
from kio.schema.describe_acls.v3.request import (
    RequestHeader as DescribeAclsRequestHeaderV3,
)
from kio.schema.describe_acls.v3.response import (
    DescribeAclsResponse as DescribeAclsResponseV3,
)
from kio.schema.describe_acls.v3.response import (
    ResponseHeader as DescribeAclsResponseHeaderV3,
)

DescribeAclsRequestHeader = (
    DescribeAclsRequestHeaderV0
    | DescribeAclsRequestHeaderV1
    | DescribeAclsRequestHeaderV2
    | DescribeAclsRequestHeaderV3
)

DescribeAclsResponseHeader = (
    DescribeAclsResponseHeaderV0
    | DescribeAclsResponseHeaderV1
    | DescribeAclsResponseHeaderV2
    | DescribeAclsResponseHeaderV3
)

DescribeAclsRequest = (
    DescribeAclsRequestV0
    | DescribeAclsRequestV1
    | DescribeAclsRequestV2
    | DescribeAclsRequestV3
)

DescribeAclsResponse = (
    DescribeAclsResponseV0
    | DescribeAclsResponseV1
    | DescribeAclsResponseV2
    | DescribeAclsResponseV3
)
