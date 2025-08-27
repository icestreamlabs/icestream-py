from kio.schema.expire_delegation_token.v0.request import (
    ExpireDelegationTokenRequest as ExpireDelegationTokenRequestV0,
)
from kio.schema.expire_delegation_token.v0.request import (
    RequestHeader as ExpireDelegationTokenRequestHeaderV0,
)
from kio.schema.expire_delegation_token.v0.response import (
    ExpireDelegationTokenResponse as ExpireDelegationTokenResponseV0,
)
from kio.schema.expire_delegation_token.v0.response import (
    ResponseHeader as ExpireDelegationTokenResponseHeaderV0,
)
from kio.schema.expire_delegation_token.v1.request import (
    ExpireDelegationTokenRequest as ExpireDelegationTokenRequestV1,
)
from kio.schema.expire_delegation_token.v1.request import (
    RequestHeader as ExpireDelegationTokenRequestHeaderV1,
)
from kio.schema.expire_delegation_token.v1.response import (
    ExpireDelegationTokenResponse as ExpireDelegationTokenResponseV1,
)
from kio.schema.expire_delegation_token.v1.response import (
    ResponseHeader as ExpireDelegationTokenResponseHeaderV1,
)
from kio.schema.expire_delegation_token.v2.request import (
    ExpireDelegationTokenRequest as ExpireDelegationTokenRequestV2,
)
from kio.schema.expire_delegation_token.v2.request import (
    RequestHeader as ExpireDelegationTokenRequestHeaderV2,
)
from kio.schema.expire_delegation_token.v2.response import (
    ExpireDelegationTokenResponse as ExpireDelegationTokenResponseV2,
)
from kio.schema.expire_delegation_token.v2.response import (
    ResponseHeader as ExpireDelegationTokenResponseHeaderV2,
)

ExpireDelegationTokenRequestHeader = (
    ExpireDelegationTokenRequestHeaderV0
    | ExpireDelegationTokenRequestHeaderV1
    | ExpireDelegationTokenRequestHeaderV2
)

ExpireDelegationTokenResponseHeader = (
    ExpireDelegationTokenResponseHeaderV0
    | ExpireDelegationTokenResponseHeaderV1
    | ExpireDelegationTokenResponseHeaderV2
)

ExpireDelegationTokenRequest = (
    ExpireDelegationTokenRequestV0
    | ExpireDelegationTokenRequestV1
    | ExpireDelegationTokenRequestV2
)

ExpireDelegationTokenResponse = (
    ExpireDelegationTokenResponseV0
    | ExpireDelegationTokenResponseV1
    | ExpireDelegationTokenResponseV2
)
