from kio.schema.create_delegation_token.v0.request import CreateDelegationTokenRequest as CreateDelegationTokenRequestV0
from kio.schema.create_delegation_token.v0.request import RequestHeader as CreateDelegationTokenRequestHeaderV0
from kio.schema.create_delegation_token.v0.response import CreateDelegationTokenResponse as CreateDelegationTokenResponseV0
from kio.schema.create_delegation_token.v0.response import ResponseHeader as CreateDelegationTokenResponseHeaderV0
from kio.schema.create_delegation_token.v1.request import CreateDelegationTokenRequest as CreateDelegationTokenRequestV1
from kio.schema.create_delegation_token.v1.request import RequestHeader as CreateDelegationTokenRequestHeaderV1
from kio.schema.create_delegation_token.v1.response import CreateDelegationTokenResponse as CreateDelegationTokenResponseV1
from kio.schema.create_delegation_token.v1.response import ResponseHeader as CreateDelegationTokenResponseHeaderV1
from kio.schema.create_delegation_token.v2.request import CreateDelegationTokenRequest as CreateDelegationTokenRequestV2
from kio.schema.create_delegation_token.v2.request import RequestHeader as CreateDelegationTokenRequestHeaderV2
from kio.schema.create_delegation_token.v2.response import CreateDelegationTokenResponse as CreateDelegationTokenResponseV2
from kio.schema.create_delegation_token.v2.response import ResponseHeader as CreateDelegationTokenResponseHeaderV2
from kio.schema.create_delegation_token.v3.request import CreateDelegationTokenRequest as CreateDelegationTokenRequestV3
from kio.schema.create_delegation_token.v3.request import RequestHeader as CreateDelegationTokenRequestHeaderV3
from kio.schema.create_delegation_token.v3.response import CreateDelegationTokenResponse as CreateDelegationTokenResponseV3
from kio.schema.create_delegation_token.v3.response import ResponseHeader as CreateDelegationTokenResponseHeaderV3

CreateDelegationTokenRequestHeader = (
    CreateDelegationTokenRequestHeaderV0
    | CreateDelegationTokenRequestHeaderV1
    | CreateDelegationTokenRequestHeaderV2
    | CreateDelegationTokenRequestHeaderV3
)

CreateDelegationTokenResponseHeader = (
    CreateDelegationTokenResponseHeaderV0
    | CreateDelegationTokenResponseHeaderV1
    | CreateDelegationTokenResponseHeaderV2
    | CreateDelegationTokenResponseHeaderV3
)

CreateDelegationTokenRequest = (
    CreateDelegationTokenRequestV0
    | CreateDelegationTokenRequestV1
    | CreateDelegationTokenRequestV2
    | CreateDelegationTokenRequestV3
)

CreateDelegationTokenResponse = (
    CreateDelegationTokenResponseV0
    | CreateDelegationTokenResponseV1
    | CreateDelegationTokenResponseV2
    | CreateDelegationTokenResponseV3
)
