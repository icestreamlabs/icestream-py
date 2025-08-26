from kio.schema.describe_quorum.v0.request import (
    DescribeQuorumRequest as DescribeQuorumRequestV0,
)
from kio.schema.describe_quorum.v0.request import (
    RequestHeader as DescribeQuorumRequestHeaderV0,
)
from kio.schema.describe_quorum.v0.response import (
    DescribeQuorumResponse as DescribeQuorumResponseV0,
)
from kio.schema.describe_quorum.v0.response import (
    ResponseHeader as DescribeQuorumResponseHeaderV0,
)
from kio.schema.describe_quorum.v1.request import (
    DescribeQuorumRequest as DescribeQuorumRequestV1,
)
from kio.schema.describe_quorum.v1.request import (
    RequestHeader as DescribeQuorumRequestHeaderV1,
)
from kio.schema.describe_quorum.v1.response import (
    DescribeQuorumResponse as DescribeQuorumResponseV1,
)
from kio.schema.describe_quorum.v1.response import (
    ResponseHeader as DescribeQuorumResponseHeaderV1,
)
from kio.schema.describe_quorum.v2.request import (
    DescribeQuorumRequest as DescribeQuorumRequestV2,
)
from kio.schema.describe_quorum.v2.request import (
    RequestHeader as DescribeQuorumRequestHeaderV2,
)
from kio.schema.describe_quorum.v2.response import (
    DescribeQuorumResponse as DescribeQuorumResponseV2,
)
from kio.schema.describe_quorum.v2.response import (
    ResponseHeader as DescribeQuorumResponseHeaderV2,
)

DescribeQuorumRequestHeader = (
    DescribeQuorumRequestHeaderV0
    | DescribeQuorumRequestHeaderV1
    | DescribeQuorumRequestHeaderV2
)

DescribeQuorumResponseHeader = (
    DescribeQuorumResponseHeaderV0
    | DescribeQuorumResponseHeaderV1
    | DescribeQuorumResponseHeaderV2
)

DescribeQuorumRequest = (
    DescribeQuorumRequestV0
    | DescribeQuorumRequestV1
    | DescribeQuorumRequestV2
)

DescribeQuorumResponse = (
    DescribeQuorumResponseV0
    | DescribeQuorumResponseV1
    | DescribeQuorumResponseV2
)
