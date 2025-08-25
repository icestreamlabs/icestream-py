from kio.schema.describe_delegation_token.v0.request import (
    DescribeDelegationTokenRequest as DescribeDelegationTokenRequestV0,
)
from kio.schema.describe_delegation_token.v0.request import (
    RequestHeader as DescribeDelegationTokenRequestHeaderV0,
)
from kio.schema.describe_delegation_token.v0.response import (
    DescribeDelegationTokenResponse as DescribeDelegationTokenResponseV0,
)
from kio.schema.describe_delegation_token.v0.response import (
    ResponseHeader as DescribeDelegationTokenResponseHeaderV0,
)
from kio.schema.describe_delegation_token.v1.request import (
    DescribeDelegationTokenRequest as DescribeDelegationTokenRequestV1,
)
from kio.schema.describe_delegation_token.v1.request import (
    RequestHeader as DescribeDelegationTokenRequestHeaderV1,
)
from kio.schema.describe_delegation_token.v1.response import (
    DescribeDelegationTokenResponse as DescribeDelegationTokenResponseV1,
)
from kio.schema.describe_delegation_token.v1.response import (
    ResponseHeader as DescribeDelegationTokenResponseHeaderV1,
)
from kio.schema.describe_delegation_token.v2.request import (
    DescribeDelegationTokenRequest as DescribeDelegationTokenRequestV2,
)
from kio.schema.describe_delegation_token.v2.request import (
    RequestHeader as DescribeDelegationTokenRequestHeaderV2,
)
from kio.schema.describe_delegation_token.v2.response import (
    DescribeDelegationTokenResponse as DescribeDelegationTokenResponseV2,
)
from kio.schema.describe_delegation_token.v2.response import (
    ResponseHeader as DescribeDelegationTokenResponseHeaderV2,
)
from kio.schema.describe_delegation_token.v3.request import (
    DescribeDelegationTokenRequest as DescribeDelegationTokenRequestV3,
)
from kio.schema.describe_delegation_token.v3.request import (
    RequestHeader as DescribeDelegationTokenRequestHeaderV3,
)
from kio.schema.describe_delegation_token.v3.response import (
    DescribeDelegationTokenResponse as DescribeDelegationTokenResponseV3,
)
from kio.schema.describe_delegation_token.v3.response import (
    ResponseHeader as DescribeDelegationTokenResponseHeaderV3,
)

DescribeDelegationTokenRequestHeader = (
    DescribeDelegationTokenRequestHeaderV0
    | DescribeDelegationTokenRequestHeaderV1
    | DescribeDelegationTokenRequestHeaderV2
    | DescribeDelegationTokenRequestHeaderV3
)

DescribeDelegationTokenResponseHeader = (
    DescribeDelegationTokenResponseHeaderV0
    | DescribeDelegationTokenResponseHeaderV1
    | DescribeDelegationTokenResponseHeaderV2
    | DescribeDelegationTokenResponseHeaderV3
)

DescribeDelegationTokenRequest = (
    DescribeDelegationTokenRequestV0
    | DescribeDelegationTokenRequestV1
    | DescribeDelegationTokenRequestV2
    | DescribeDelegationTokenRequestV3
)

DescribeDelegationTokenResponse = (
    DescribeDelegationTokenResponseV0
    | DescribeDelegationTokenResponseV1
    | DescribeDelegationTokenResponseV2
    | DescribeDelegationTokenResponseV3
)
