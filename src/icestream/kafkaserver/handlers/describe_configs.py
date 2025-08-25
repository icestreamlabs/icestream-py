from kio.schema.describe_configs.v0.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV0,
)
from kio.schema.describe_configs.v0.request import (
    RequestHeader as DescribeConfigsRequestHeaderV0,
)
from kio.schema.describe_configs.v0.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV0,
)
from kio.schema.describe_configs.v0.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV0,
)
from kio.schema.describe_configs.v1.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV1,
)
from kio.schema.describe_configs.v1.request import (
    RequestHeader as DescribeConfigsRequestHeaderV1,
)
from kio.schema.describe_configs.v1.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV1,
)
from kio.schema.describe_configs.v1.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV1,
)
from kio.schema.describe_configs.v2.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV2,
)
from kio.schema.describe_configs.v2.request import (
    RequestHeader as DescribeConfigsRequestHeaderV2,
)
from kio.schema.describe_configs.v2.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV2,
)
from kio.schema.describe_configs.v2.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV2,
)
from kio.schema.describe_configs.v3.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV3,
)
from kio.schema.describe_configs.v3.request import (
    RequestHeader as DescribeConfigsRequestHeaderV3,
)
from kio.schema.describe_configs.v3.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV3,
)
from kio.schema.describe_configs.v3.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV3,
)
from kio.schema.describe_configs.v4.request import (
    DescribeConfigsRequest as DescribeConfigsRequestV4,
)
from kio.schema.describe_configs.v4.request import (
    RequestHeader as DescribeConfigsRequestHeaderV4,
)
from kio.schema.describe_configs.v4.response import (
    DescribeConfigsResponse as DescribeConfigsResponseV4,
)
from kio.schema.describe_configs.v4.response import (
    ResponseHeader as DescribeConfigsResponseHeaderV4,
)


DescribeConfigsRequestHeader = (
    DescribeConfigsRequestHeaderV0
    | DescribeConfigsRequestHeaderV1
    | DescribeConfigsRequestHeaderV2
    | DescribeConfigsRequestHeaderV3
    | DescribeConfigsRequestHeaderV4
)

DescribeConfigsResponseHeader = (
    DescribeConfigsResponseHeaderV0
    | DescribeConfigsResponseHeaderV1
    | DescribeConfigsResponseHeaderV2
    | DescribeConfigsResponseHeaderV3
    | DescribeConfigsResponseHeaderV4
)

DescribeConfigsRequest = (
    DescribeConfigsRequestV0
    | DescribeConfigsRequestV1
    | DescribeConfigsRequestV2
    | DescribeConfigsRequestV3
    | DescribeConfigsRequestV4
)

DescribeConfigsResponse = (
    DescribeConfigsResponseV0
    | DescribeConfigsResponseV1
    | DescribeConfigsResponseV2
    | DescribeConfigsResponseV3
    | DescribeConfigsResponseV4
)
