from kio.schema.alter_client_quotas.v0.request import (
    AlterClientQuotasRequest as AlterClientQuotasRequestV0,
)
from kio.schema.alter_client_quotas.v0.request import (
    RequestHeader as AlterClientQuotasRequestHeaderV0,
)
from kio.schema.alter_client_quotas.v0.response import (
    AlterClientQuotasResponse as AlterClientQuotasResponseV0,
)
from kio.schema.alter_client_quotas.v0.response import (
    ResponseHeader as AlterClientQuotasResponseHeaderV0,
)
from kio.schema.alter_client_quotas.v1.request import (
    AlterClientQuotasRequest as AlterClientQuotasRequestV1,
)
from kio.schema.alter_client_quotas.v1.request import (
    RequestHeader as AlterClientQuotasRequestHeaderV1,
)
from kio.schema.alter_client_quotas.v1.response import (
    AlterClientQuotasResponse as AlterClientQuotasResponseV1,
)
from kio.schema.alter_client_quotas.v1.response import (
    ResponseHeader as AlterClientQuotasResponseHeaderV1,
)

AlterClientQuotasRequestHeader = (
    AlterClientQuotasRequestHeaderV0 | AlterClientQuotasRequestHeaderV1
)

AlterClientQuotasResponseHeader = (
    AlterClientQuotasResponseHeaderV0 | AlterClientQuotasResponseHeaderV1
)

AlterClientQuotasRequest = (
    AlterClientQuotasRequestV0 | AlterClientQuotasRequestV1
)

AlterClientQuotasResponse = (
    AlterClientQuotasResponseV0 | AlterClientQuotasResponseV1
)
