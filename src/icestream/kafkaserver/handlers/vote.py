from kio.schema.vote.v0.request import VoteRequest as VoteRequestV0
from kio.schema.vote.v0.request import RequestHeader as VoteRequestHeaderV0
from kio.schema.vote.v0.response import VoteResponse as VoteResponseV0
from kio.schema.vote.v0.response import ResponseHeader as VoteResponseHeaderV0
from kio.schema.vote.v1.request import VoteRequest as VoteRequestV1
from kio.schema.vote.v1.request import RequestHeader as VoteRequestHeaderV1
from kio.schema.vote.v1.response import VoteResponse as VoteResponseV1
from kio.schema.vote.v1.response import ResponseHeader as VoteResponseHeaderV1

VoteRequestHeader = VoteRequestHeaderV0 | VoteRequestHeaderV1
VoteResponseHeader = VoteResponseHeaderV0 | VoteResponseHeaderV1
VoteRequest = VoteRequestV0 | VoteRequestV1
VoteResponse = VoteResponseV0 | VoteResponseV1
