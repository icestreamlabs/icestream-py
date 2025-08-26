from kio.schema.describe_log_dirs.v0.request import (
    DescribeLogDirsRequest as DescribeLogDirsRequestV0,
)
from kio.schema.describe_log_dirs.v0.request import (
    RequestHeader as DescribeLogDirsRequestHeaderV0,
)
from kio.schema.describe_log_dirs.v0.response import (
    DescribeLogDirsResponse as DescribeLogDirsResponseV0,
)
from kio.schema.describe_log_dirs.v0.response import (
    ResponseHeader as DescribeLogDirsResponseHeaderV0,
)
from kio.schema.describe_log_dirs.v1.request import (
    DescribeLogDirsRequest as DescribeLogDirsRequestV1,
)
from kio.schema.describe_log_dirs.v1.request import (
    RequestHeader as DescribeLogDirsRequestHeaderV1,
)
from kio.schema.describe_log_dirs.v1.response import (
    DescribeLogDirsResponse as DescribeLogDirsResponseV1,
)
from kio.schema.describe_log_dirs.v1.response import (
    ResponseHeader as DescribeLogDirsResponseHeaderV1,
)
from kio.schema.describe_log_dirs.v2.request import (
    DescribeLogDirsRequest as DescribeLogDirsRequestV2,
)
from kio.schema.describe_log_dirs.v2.request import (
    RequestHeader as DescribeLogDirsRequestHeaderV2,
)
from kio.schema.describe_log_dirs.v2.response import (
    DescribeLogDirsResponse as DescribeLogDirsResponseV2,
)
from kio.schema.describe_log_dirs.v2.response import (
    ResponseHeader as DescribeLogDirsResponseHeaderV2,
)
from kio.schema.describe_log_dirs.v3.request import (
    DescribeLogDirsRequest as DescribeLogDirsRequestV3,
)
from kio.schema.describe_log_dirs.v3.request import (
    RequestHeader as DescribeLogDirsRequestHeaderV3,
)
from kio.schema.describe_log_dirs.v3.response import (
    DescribeLogDirsResponse as DescribeLogDirsResponseV3,
)
from kio.schema.describe_log_dirs.v3.response import (
    ResponseHeader as DescribeLogDirsResponseHeaderV3,
)
from kio.schema.describe_log_dirs.v4.request import (
    DescribeLogDirsRequest as DescribeLogDirsRequestV4,
)
from kio.schema.describe_log_dirs.v4.request import (
    RequestHeader as DescribeLogDirsRequestHeaderV4,
)
from kio.schema.describe_log_dirs.v4.response import (
    DescribeLogDirsResponse as DescribeLogDirsResponseV4,
)
from kio.schema.describe_log_dirs.v4.response import (
    ResponseHeader as DescribeLogDirsResponseHeaderV4,
)


DescribeLogDirsRequestHeader = (
    DescribeLogDirsRequestHeaderV0
    | DescribeLogDirsRequestHeaderV1
    | DescribeLogDirsRequestHeaderV2
    | DescribeLogDirsRequestHeaderV3
    | DescribeLogDirsRequestHeaderV4
)

DescribeLogDirsResponseHeader = (
    DescribeLogDirsResponseHeaderV0
    | DescribeLogDirsResponseHeaderV1
    | DescribeLogDirsResponseHeaderV2
    | DescribeLogDirsResponseHeaderV3
    | DescribeLogDirsResponseHeaderV4
)

DescribeLogDirsRequest = (
    DescribeLogDirsRequestV0
    | DescribeLogDirsRequestV1
    | DescribeLogDirsRequestV2
    | DescribeLogDirsRequestV3
    | DescribeLogDirsRequestV4
)

DescribeLogDirsResponse = (
    DescribeLogDirsResponseV0
    | DescribeLogDirsResponseV1
    | DescribeLogDirsResponseV2
    | DescribeLogDirsResponseV3
    | DescribeLogDirsResponseV4
)
