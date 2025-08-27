from kio.schema.renew_delegation_token.v0.request import (
    RenewDelegationTokenRequest as RenewDelegationTokenRequestV0,
)
from kio.schema.renew_delegation_token.v0.request import (
    RequestHeader as RenewDelegationTokenRequestHeaderV0,
)
from kio.schema.renew_delegation_token.v0.response import (
    RenewDelegationTokenResponse as RenewDelegationTokenResponseV0,
)
from kio.schema.renew_delegation_token.v0.response import (
    ResponseHeader as RenewDelegationTokenResponseHeaderV0,
)
from kio.schema.renew_delegation_token.v1.request import (
    RenewDelegationTokenRequest as RenewDelegationTokenRequestV1,
)
from kio.schema.renew_delegation_token.v1.request import (
    RequestHeader as RenewDelegationTokenRequestHeaderV1,
)
from kio.schema.renew_delegation_token.v1.response import (
    RenewDelegationTokenResponse as RenewDelegationTokenResponseV1,
)
from kio.schema.renew_delegation_token.v1.response import (
    ResponseHeader as RenewDelegationTokenResponseHeaderV1,
)
from kio.schema.renew_delegation_token.v2.request import (
    RenewDelegationTokenRequest as RenewDelegationTokenRequestV2,
)
from kio.schema.renew_delegation_token.v2.request import (
    RequestHeader as RenewDelegationTokenRequestHeaderV2,
)
from kio.schema.renew_delegation_token.v2.response import (
    RenewDelegationTokenResponse as RenewDelegationTokenResponseV2,
)
from kio.schema.renew_delegation_token.v2.response import (
    ResponseHeader as RenewDelegationTokenResponseHeaderV2,
)

RenewDelegationTokenRequestHeader = (
    RenewDelegationTokenRequestHeaderV0
    | RenewDelegationTokenRequestHeaderV1
    | RenewDelegationTokenRequestHeaderV2
)

RenewDelegationTokenResponseHeader = (
    RenewDelegationTokenResponseHeaderV0
    | RenewDelegationTokenResponseHeaderV1
    | RenewDelegationTokenResponseHeaderV2
)

RenewDelegationTokenRequest = (
    RenewDelegationTokenRequestV0
    | RenewDelegationTokenRequestV1
    | RenewDelegationTokenRequestV2
)

RenewDelegationTokenResponse = (
    RenewDelegationTokenResponseV0
    | RenewDelegationTokenResponseV1
    | RenewDelegationTokenResponseV2
)
