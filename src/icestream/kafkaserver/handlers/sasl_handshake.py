from kio.schema.sasl_handshake.v0.request import (
    SaslHandshakeRequest as SaslHandshakeRequestV0,
)
from kio.schema.sasl_handshake.v0.request import (
    RequestHeader as SaslHandshakeRequestHeaderV0,
)
from kio.schema.sasl_handshake.v0.response import (
    SaslHandshakeResponse as SaslHandshakeResponseV0,
)
from kio.schema.sasl_handshake.v0.response import (
    ResponseHeader as SaslHandshakeResponseHeaderV0,
)
from kio.schema.sasl_handshake.v1.request import (
    SaslHandshakeRequest as SaslHandshakeRequestV1,
)
from kio.schema.sasl_handshake.v1.request import (
    RequestHeader as SaslHandshakeRequestHeaderV1,
)
from kio.schema.sasl_handshake.v1.response import (
    SaslHandshakeResponse as SaslHandshakeResponseV1,
)
from kio.schema.sasl_handshake.v1.response import (
    ResponseHeader as SaslHandshakeResponseHeaderV1,
)


SaslHandshakeRequestHeader = (
    SaslHandshakeRequestHeaderV0 | SaslHandshakeRequestHeaderV1
)

SaslHandshakeResponseHeader = (
    SaslHandshakeResponseHeaderV0 | SaslHandshakeResponseHeaderV1
)

SaslHandshakeRequest = SaslHandshakeRequestV0 | SaslHandshakeRequestV1

SaslHandshakeResponse = SaslHandshakeResponseV0 | SaslHandshakeResponseV1
