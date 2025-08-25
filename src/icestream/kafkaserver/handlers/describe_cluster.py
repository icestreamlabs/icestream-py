from kio.schema.describe_cluster.v0.request import (
    DescribeClusterRequest as DescribeClusterRequestV0,
)
from kio.schema.describe_cluster.v0.request import (
    RequestHeader as DescribeClusterRequestHeaderV0,
)
from kio.schema.describe_cluster.v0.response import (
    DescribeClusterResponse as DescribeClusterResponseV0,
)
from kio.schema.describe_cluster.v0.response import (
    ResponseHeader as DescribeClusterResponseHeaderV0,
)
from kio.schema.describe_cluster.v1.request import (
    DescribeClusterRequest as DescribeClusterRequestV1,
)
from kio.schema.describe_cluster.v1.request import (
    RequestHeader as DescribeClusterRequestHeaderV1,
)
from kio.schema.describe_cluster.v1.response import (
    DescribeClusterResponse as DescribeClusterResponseV1,
)
from kio.schema.describe_cluster.v1.response import (
    ResponseHeader as DescribeClusterResponseHeaderV1,
)

DescribeClusterRequestHeader = (
    DescribeClusterRequestHeaderV0
    | DescribeClusterRequestHeaderV1
)

DescribeClusterResponseHeader = (
    DescribeClusterResponseHeaderV0
    | DescribeClusterResponseHeaderV1
)

DescribeClusterRequest = (
    DescribeClusterRequestV0
    | DescribeClusterRequestV1
)

DescribeClusterResponse = (
    DescribeClusterResponseV0
    | DescribeClusterResponseV1
)
