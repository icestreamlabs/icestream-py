from kio.schema.alter_replica_log_dirs.v0.request import (
    AlterReplicaLogDirsRequest as AlterReplicaLogDirsRequestV0,
)
from kio.schema.alter_replica_log_dirs.v0.request import (
    RequestHeader as AlterReplicaLogDirsRequestHeaderV0,
)
from kio.schema.alter_replica_log_dirs.v0.response import (
    AlterReplicaLogDirsResponse as AlterReplicaLogDirsResponseV0,
)
from kio.schema.alter_replica_log_dirs.v0.response import (
    ResponseHeader as AlterReplicaLogDirsResponseHeaderV0,
)
from kio.schema.alter_replica_log_dirs.v1.request import (
    AlterReplicaLogDirsRequest as AlterReplicaLogDirsRequestV1,
)
from kio.schema.alter_replica_log_dirs.v1.request import (
    RequestHeader as AlterReplicaLogDirsRequestHeaderV1,
)
from kio.schema.alter_replica_log_dirs.v1.response import (
    AlterReplicaLogDirsResponse as AlterReplicaLogDirsResponseV1,
)
from kio.schema.alter_replica_log_dirs.v1.response import (
    ResponseHeader as AlterReplicaLogDirsResponseHeaderV1,
)
from kio.schema.alter_replica_log_dirs.v2.request import (
    AlterReplicaLogDirsRequest as AlterReplicaLogDirsRequestV2,
)
from kio.schema.alter_replica_log_dirs.v2.request import (
    RequestHeader as AlterReplicaLogDirsRequestHeaderV2,
)
from kio.schema.alter_replica_log_dirs.v2.response import (
    AlterReplicaLogDirsResponse as AlterReplicaLogDirsResponseV2,
)
from kio.schema.alter_replica_log_dirs.v2.response import (
    ResponseHeader as AlterReplicaLogDirsResponseHeaderV2,
)


AlterReplicaLogDirsRequestHeader = (
    AlterReplicaLogDirsRequestHeaderV0 | AlterReplicaLogDirsRequestHeaderV1 | AlterReplicaLogDirsRequestHeaderV2
)

AlterReplicaLogDirsResponseHeader = (
    AlterReplicaLogDirsResponseHeaderV0 | AlterReplicaLogDirsResponseHeaderV1 | AlterReplicaLogDirsResponseHeaderV2
)

AlterReplicaLogDirsRequest = (
    AlterReplicaLogDirsRequestV0 | AlterReplicaLogDirsRequestV1 | AlterReplicaLogDirsRequestV2
)

AlterReplicaLogDirsResponse = (
    AlterReplicaLogDirsResponseV0 | AlterReplicaLogDirsResponseV1 | AlterReplicaLogDirsResponseV2
)
