from kio.schema.describe_groups.v0.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV0,
)
from kio.schema.describe_groups.v0.request import (
    RequestHeader as DescribeGroupsRequestHeaderV0,
)
from kio.schema.describe_groups.v0.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV0,
)
from kio.schema.describe_groups.v0.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV0,
)
from kio.schema.describe_groups.v1.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV1,
)
from kio.schema.describe_groups.v1.request import (
    RequestHeader as DescribeGroupsRequestHeaderV1,
)
from kio.schema.describe_groups.v1.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV1,
)
from kio.schema.describe_groups.v1.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV1,
)
from kio.schema.describe_groups.v2.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV2,
)
from kio.schema.describe_groups.v2.request import (
    RequestHeader as DescribeGroupsRequestHeaderV2,
)
from kio.schema.describe_groups.v2.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV2,
)
from kio.schema.describe_groups.v2.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV2,
)
from kio.schema.describe_groups.v3.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV3,
)
from kio.schema.describe_groups.v3.request import (
    RequestHeader as DescribeGroupsRequestHeaderV3,
)
from kio.schema.describe_groups.v3.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV3,
)
from kio.schema.describe_groups.v3.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV3,
)
from kio.schema.describe_groups.v4.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV4,
)
from kio.schema.describe_groups.v4.request import (
    RequestHeader as DescribeGroupsRequestHeaderV4,
)
from kio.schema.describe_groups.v4.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV4,
)
from kio.schema.describe_groups.v4.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV4,
)
from kio.schema.describe_groups.v5.request import (
    DescribeGroupsRequest as DescribeGroupsRequestV5,
)
from kio.schema.describe_groups.v5.request import (
    RequestHeader as DescribeGroupsRequestHeaderV5,
)
from kio.schema.describe_groups.v5.response import (
    DescribeGroupsResponse as DescribeGroupsResponseV5,
)
from kio.schema.describe_groups.v5.response import (
    ResponseHeader as DescribeGroupsResponseHeaderV5,
)

DescribeGroupsRequestHeader = (
    DescribeGroupsRequestHeaderV0
    | DescribeGroupsRequestHeaderV1
    | DescribeGroupsRequestHeaderV2
    | DescribeGroupsRequestHeaderV3
    | DescribeGroupsRequestHeaderV4
    | DescribeGroupsRequestHeaderV5
)

DescribeGroupsResponseHeader = (
    DescribeGroupsResponseHeaderV0
    | DescribeGroupsResponseHeaderV1
    | DescribeGroupsResponseHeaderV2
    | DescribeGroupsResponseHeaderV3
    | DescribeGroupsResponseHeaderV4
    | DescribeGroupsResponseHeaderV5
)

DescribeGroupsRequest = (
    DescribeGroupsRequestV0
    | DescribeGroupsRequestV1
    | DescribeGroupsRequestV2
    | DescribeGroupsRequestV3
    | DescribeGroupsRequestV4
    | DescribeGroupsRequestV5
)

DescribeGroupsResponse = (
    DescribeGroupsResponseV0
    | DescribeGroupsResponseV1
    | DescribeGroupsResponseV2
    | DescribeGroupsResponseV3
    | DescribeGroupsResponseV4
    | DescribeGroupsResponseV5
)
