from kio.schema.sasl_authenticate.v0.request import (
    SaslAuthenticateRequest as SaslAuthenticateRequestV0,
)
from kio.schema.sasl_authenticate.v0.request import (
    RequestHeader as SaslAuthenticateRequestHeaderV0,
)
from kio.schema.sasl_authenticate.v0.response import (
    SaslAuthenticateResponse as SaslAuthenticateResponseV0,
)
from kio.schema.sasl_authenticate.v0.response import (
    ResponseHeader as SaslAuthenticateResponseHeaderV0,
)
from kio.schema.sasl_authenticate.v1.request import (
    SaslAuthenticateRequest as SaslAuthenticateRequestV1,
)
from kio.schema.sasl_authenticate.v1.request import (
    RequestHeader as SaslAuthenticateRequestHeaderV1,
)
from kio.schema.sasl_authenticate.v1.response import (
    SaslAuthenticateResponse as SaslAuthenticateResponseV1,
)
from kio.schema.sasl_authenticate.v1.response import (
    ResponseHeader as SaslAuthenticateResponseHeaderV1,
)
from kio.schema.sasl_authenticate.v2.request import (
    SaslAuthenticateRequest as SaslAuthenticateRequestV2,
)
from kio.schema.sasl_authenticate.v2.request import (
    RequestHeader as SaslAuthenticateRequestHeaderV2,
)
from kio.schema.sasl_authenticate.v2.response import (
    SaslAuthenticateResponse as SaslAuthenticateResponseV2,
)
from kio.schema.sasl_authenticate.v2.response import (
    ResponseHeader as SaslAuthenticateResponseHeaderV2,
)


SaslAuthenticateRequestHeader = (
    SaslAuthenticateRequestHeaderV0
    | SaslAuthenticateRequestHeaderV1
    | SaslAuthenticateRequestHeaderV2
)

SaslAuthenticateResponseHeader = (
    SaslAuthenticateResponseHeaderV0
    | SaslAuthenticateResponseHeaderV1
    | SaslAuthenticateResponseHeaderV2
)

SaslAuthenticateRequest = (
    SaslAuthenticateRequestV0 | SaslAuthenticateRequestV1 | SaslAuthenticateRequestV2
)

SaslAuthenticateResponse = (
    SaslAuthenticateResponseV0
    | SaslAuthenticateResponseV1
    | SaslAuthenticateResponseV2
)
