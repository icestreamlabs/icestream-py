from kio.schema.describe_client_quotas.v0.request import (
    DescribeClientQuotasRequest as DescribeClientQuotasRequestV0,
)
from kio.schema.describe_client_quotas.v0.request import (
    RequestHeader as DescribeClientQuotasRequestHeaderV0,
)
from kio.schema.describe_client_quotas.v0.response import (
    DescribeClientQuotasResponse as DescribeClientQuotasResponseV0,
)
from kio.schema.describe_client_quotas.v0.response import (
    ResponseHeader as DescribeClientQuotasResponseHeaderV0,
)
from kio.schema.describe_client_quotas.v1.request import (
    DescribeClientQuotasRequest as DescribeClientQuotasRequestV1,
)
from kio.schema.describe_client_quotas.v1.request import (
    RequestHeader as DescribeClientQuotasRequestHeaderV1,
)
from kio.schema.describe_client_quotas.v1.response import (
    DescribeClientQuotasResponse as DescribeClientQuotasResponseV1,
)
from kio.schema.describe_client_quotas.v1.response import (
    ResponseHeader as DescribeClientQuotasResponseHeaderV1,
)

DescribeClientQuotasRequestHeader = (
    DescribeClientQuotasRequestHeaderV0
    | DescribeClientQuotasRequestHeaderV1
)

DescribeClientQuotasResponseHeader = (
    DescribeClientQuotasResponseHeaderV0
    | DescribeClientQuotasResponseHeaderV1
)

DescribeClientQuotasRequest = (
    DescribeClientQuotasRequestV0
    | DescribeClientQuotasRequestV1
)

DescribeClientQuotasResponse = (
    DescribeClientQuotasResponseV0
    | DescribeClientQuotasResponseV1
)
