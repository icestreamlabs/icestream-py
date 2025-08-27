from kio.schema.list_transactions.v0.request import (
    ListTransactionsRequest as ListTransactionsRequestV0,
)
from kio.schema.list_transactions.v0.request import (
    RequestHeader as ListTransactionsRequestHeaderV0,
)
from kio.schema.list_transactions.v0.response import (
    ListTransactionsResponse as ListTransactionsResponseV0,
)
from kio.schema.list_transactions.v0.response import (
    ResponseHeader as ListTransactionsResponseHeaderV0,
)
from kio.schema.list_transactions.v1.request import (
    ListTransactionsRequest as ListTransactionsRequestV1,
)
from kio.schema.list_transactions.v1.request import (
    RequestHeader as ListTransactionsRequestHeaderV1,
)
from kio.schema.list_transactions.v1.response import (
    ListTransactionsResponse as ListTransactionsResponseV1,
)
from kio.schema.list_transactions.v1.response import (
    ResponseHeader as ListTransactionsResponseHeaderV1,
)


ListTransactionsRequestHeader = (
    ListTransactionsRequestHeaderV0 | ListTransactionsRequestHeaderV1
)

ListTransactionsResponseHeader = (
    ListTransactionsResponseHeaderV0 | ListTransactionsResponseHeaderV1
)

ListTransactionsRequest = ListTransactionsRequestV0 | ListTransactionsRequestV1

ListTransactionsResponse = ListTransactionsResponseV0 | ListTransactionsResponseV1
