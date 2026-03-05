import asyncio
import time
import uuid
from typing import AsyncGenerator, Callable

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from icestream.admin.errors import (
    AdminApiError,
    AdminErrorCode,
)
from icestream.admin.schemas import (
    ConsumerGroupAssignmentV1,
    ConsumerGroupDetailDataV1,
    ConsumerGroupDetailResponseV1,
    ConsumerGroupListDataV1,
    ConsumerGroupListResponseV1,
    ConsumerGroupMemberV1,
    ConsumerGroupOffsetV1,
    ConsumerGroupSummaryV1,
    ErrorBodyV1,
    ErrorEnvelopeV1,
    HealthDataV1,
    HealthResponseV1,
    ReadinessDataV1,
    ReadinessResponseV1,
    TopicCreateDataV1,
    TopicCreateRequestV1,
    TopicCreateResponseV1,
    TopicDeleteDataV1,
    TopicDeleteResponseV1,
    TopicDetailV1,
    TopicGetDataV1,
    TopicGetResponseV1,
    TopicListDataV1,
    TopicListResponseV1,
    TopicOffsetsDataV1,
    TopicOffsetsResponseV1,
    TopicOffsetPartitionVisibilityV1,
    TopicPartitionMetadataV1,
    TopicSummaryV1,
)
from icestream.admin.services import AdminService
from icestream.config import Config
from icestream.logger import log


class AdminApi:
    def __init__(self, config: Config):
        self.config = config
        self.service = AdminService()
        self.app = FastAPI(
            title="icestream REST API",
            version="v1",
            openapi_tags=[
                {
                    "name": "topics",
                    "description": "Topic lifecycle management for operators.",
                },
                {
                    "name": "health",
                    "description": "Health and readiness endpoints.",
                },
                {
                    "name": "introspection",
                    "description": "Operational visibility endpoints.",
                },
                {
                    "name": "consumer-groups",
                    "description": "Read-only consumer-group introspection.",
                },
            ],
        )
        self._add_exception_handlers()
        self._add_middlewares()
        self._add_routes()

    def _get_session(self) -> Callable[[], AsyncGenerator[AsyncSession, None]]:
        async def _get_session_inner() -> AsyncGenerator[AsyncSession, None]:
            async with self.config.async_session_factory() as session:
                yield session

        return _get_session_inner

    @staticmethod
    def _request_id(request: Request) -> str | None:
        state_value = getattr(request.state, "request_id", None)
        if state_value:
            return str(state_value)
        header_value = request.headers.get("x-request-id")
        if header_value:
            return header_value
        return None

    @staticmethod
    def _error_response(
        request: Request,
        *,
        status_code: int,
        code: AdminErrorCode,
        message: str,
    ) -> JSONResponse:
        body = ErrorEnvelopeV1(
            error=ErrorBodyV1(code=code, message=message),
            request_id=AdminApi._request_id(request),
        )
        return JSONResponse(status_code=status_code, content=body.model_dump())

    def _add_exception_handlers(self) -> None:
        @self.app.exception_handler(AdminApiError)
        async def _handle_admin_error(
            request: Request, exc: AdminApiError
        ) -> JSONResponse:
            return self._error_response(
                request,
                status_code=exc.status_code,
                code=exc.code,
                message=exc.message,
            )

        @self.app.exception_handler(RequestValidationError)
        async def _handle_validation_error(
            request: Request, exc: RequestValidationError
        ) -> JSONResponse:
            detail = exc.errors()
            first_error = detail[0] if detail else {}
            field_path = ".".join(str(value) for value in first_error.get("loc", ()))
            message = first_error.get("msg", "invalid request")
            if field_path:
                message = f"{field_path}: {message}"
            return self._error_response(
                request,
                status_code=400,
                code=AdminErrorCode.INVALID_REQUEST,
                message=message,
            )

        @self.app.exception_handler(Exception)
        async def _handle_internal_error(request: Request, exc: Exception) -> JSONResponse:
            log.exception(
                "admin_unhandled_exception",
                request_id=self._request_id(request),
                method=request.method,
                path=request.url.path,
                error=str(exc),
            )
            return self._error_response(
                request,
                status_code=500,
                code=AdminErrorCode.INTERNAL_ERROR,
                message="internal server error",
            )

    def _add_middlewares(self) -> None:
        @self.app.middleware("http")
        async def _admin_request_middleware(request: Request, call_next):
            request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
            request.state.request_id = request_id
            started = time.perf_counter()

            response: JSONResponse | None = None
            error_category: str | None = None

            content_length = request.headers.get("content-length")
            if content_length is not None:
                try:
                    parsed_length = int(content_length)
                except ValueError:
                    error_category = AdminErrorCode.INVALID_REQUEST.value
                    response = self._error_response(
                        request,
                        status_code=400,
                        code=AdminErrorCode.INVALID_REQUEST,
                        message="content-length must be an integer",
                    )
                else:
                    if parsed_length > self.config.ADMIN_MAX_REQUEST_BYTES:
                        error_category = AdminErrorCode.REQUEST_TOO_LARGE.value
                        response = self._error_response(
                            request,
                            status_code=413,
                            code=AdminErrorCode.REQUEST_TOO_LARGE,
                            message=(
                                "request body exceeds "
                                f"{self.config.ADMIN_MAX_REQUEST_BYTES} bytes"
                            ),
                        )
            else:
                body = await request.body()
                if len(body) > self.config.ADMIN_MAX_REQUEST_BYTES:
                    error_category = AdminErrorCode.REQUEST_TOO_LARGE.value
                    response = self._error_response(
                        request,
                        status_code=413,
                        code=AdminErrorCode.REQUEST_TOO_LARGE,
                        message=(
                            "request body exceeds "
                            f"{self.config.ADMIN_MAX_REQUEST_BYTES} bytes"
                        ),
                    )

            if response is None:
                try:
                    response = await asyncio.wait_for(
                        call_next(request),
                        timeout=self.config.ADMIN_REQUEST_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    error_category = AdminErrorCode.REQUEST_TIMEOUT.value
                    response = self._error_response(
                        request,
                        status_code=408,
                        code=AdminErrorCode.REQUEST_TIMEOUT,
                        message=(
                            "request exceeded timeout of "
                            f"{self.config.ADMIN_REQUEST_TIMEOUT_SECONDS} seconds"
                        ),
                    )

            if error_category is None:
                if response.status_code >= 500:
                    error_category = AdminErrorCode.INTERNAL_ERROR.value
                elif response.status_code >= 400:
                    error_category = AdminErrorCode.INVALID_REQUEST.value

            duration_ms = (time.perf_counter() - started) * 1000
            log.info(
                "admin_http_request",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                latency_ms=round(duration_ms, 2),
                error_category=error_category,
            )

            response.headers["x-request-id"] = request_id
            return response

    def _add_routes(self):
        @self.app.post(
            "/topics",
            response_model=TopicCreateResponseV1,
            status_code=201,
            tags=["topics"],
            summary="Create topic",
            description=(
                "Create a non-internal topic with a fixed number of partitions. "
                "This endpoint is intended for operator tooling."
            ),
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid request payload.",
                },
                409: {
                    "model": ErrorEnvelopeV1,
                    "description": "Topic already exists.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
            openapi_extra={
                "requestBody": {
                    "content": {
                        "application/json": {
                            "examples": {
                                "create_topic": {
                                    "summary": "Create a topic",
                                    "value": {
                                        "name": "orders.v1",
                                        "num_partitions": 3,
                                        "is_internal": False,
                                    },
                                }
                            }
                        }
                    }
                }
            },
        )
        async def create_topic(
            data: TopicCreateRequestV1,
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.create_topic(
                session,
                name=data.name,
                num_partitions=data.num_partitions,
                is_internal=data.is_internal,
            )
            summary = TopicSummaryV1(
                id=result.id,
                name=result.name,
                is_internal=result.is_internal,
                partition_count=result.partition_count,
            )
            return TopicCreateResponseV1(
                data=TopicCreateDataV1(topic=summary),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/topics",
            response_model=TopicListResponseV1,
            tags=["topics"],
            summary="List topics",
            description="List topics with deterministic name ordering.",
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid pagination parameters.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
            openapi_extra={
                "parameters": [
                    {
                        "name": "limit",
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 100},
                        "example": 25,
                    },
                    {
                        "name": "offset",
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 0},
                        "example": 0,
                    },
                ]
            },
        )
        async def list_topics(
            request: Request,
            limit: int = Query(default=100, ge=1, le=1000),
            offset: int = Query(default=0, ge=0),
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.list_topics(
                session,
                limit=limit,
                offset=offset,
            )
            return TopicListResponseV1(
                data=TopicListDataV1(
                    topics=[
                        TopicSummaryV1(
                            id=topic.id,
                            name=topic.name,
                            is_internal=topic.is_internal,
                            partition_count=topic.partition_count,
                        )
                        for topic in result.topics
                    ],
                    limit=result.limit,
                    offset=result.offset,
                    total=result.total,
                ),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/topics/{topic_name}",
            response_model=TopicGetResponseV1,
            tags=["topics"],
            summary="Get topic",
            description="Return topic detail including partition metadata.",
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid topic name.",
                },
                404: {
                    "model": ErrorEnvelopeV1,
                    "description": "Topic not found.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
        )
        async def get_topic(
            topic_name: str,
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.get_topic(session, name=topic_name)
            return TopicGetResponseV1(
                data=TopicGetDataV1(
                    topic=TopicDetailV1(
                        id=result.id,
                        name=result.name,
                        is_internal=result.is_internal,
                        partitions=[
                            TopicPartitionMetadataV1(
                                partition_number=partition.partition_number,
                                log_start_offset=partition.log_start_offset,
                                last_offset=partition.last_offset,
                            )
                            for partition in result.partitions
                        ],
                        created_at=result.created_at,
                        updated_at=result.updated_at,
                    )
                ),
                request_id=self._request_id(request),
            )

        @self.app.delete(
            "/topics/{topic_name}",
            response_model=TopicDeleteResponseV1,
            tags=["topics"],
            summary="Delete topic",
            description="Delete a topic and all dependent metadata via cascade rules.",
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid delete request.",
                },
                404: {
                    "model": ErrorEnvelopeV1,
                    "description": "Topic not found.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
        )
        async def delete_topic(
            topic_name: str,
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            await self.service.delete_topic(session, name=topic_name)
            return TopicDeleteResponseV1(
                data=TopicDeleteDataV1(name=topic_name),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/healthz",
            response_model=HealthResponseV1,
            tags=["health"],
            summary="Liveness probe",
            description="Simple process liveness endpoint.",
        )
        async def healthz(request: Request):
            return HealthResponseV1(
                data=HealthDataV1(),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/readyz",
            response_model=ReadinessResponseV1,
            tags=["health"],
            summary="Readiness probe",
            description="Readiness endpoint including database connectivity.",
            responses={
                503: {
                    "model": ErrorEnvelopeV1,
                    "description": "Service not ready.",
                }
            },
        )
        async def readyz(
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            ready = await self.service.ping_database(session)
            if not ready:
                return self._error_response(
                    request,
                    status_code=503,
                    code=AdminErrorCode.INTERNAL_ERROR,
                    message="database is unavailable",
                )
            return ReadinessResponseV1(
                data=ReadinessDataV1(),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/topics/{topic_name}/offsets",
            response_model=TopicOffsetsResponseV1,
            tags=["introspection"],
            summary="Get topic offsets",
            description=(
                "Inspect per-partition log-start/latest offsets and "
                "WAL/topic-WAL visibility metadata."
            ),
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid topic name.",
                },
                404: {
                    "model": ErrorEnvelopeV1,
                    "description": "Topic not found.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
        )
        async def topic_offsets(
            topic_name: str,
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.get_topic_offsets(session, name=topic_name)
            return TopicOffsetsResponseV1(
                data=TopicOffsetsDataV1(
                    topic_name=result.topic_name,
                    partitions=[
                        TopicOffsetPartitionVisibilityV1(
                            partition_number=partition.partition_number,
                            log_start_offset=partition.log_start_offset,
                            latest_offset=partition.latest_offset,
                            wal_uncompacted_segment_count=partition.wal_uncompacted_segment_count,
                            wal_uncompacted_min_offset=partition.wal_uncompacted_min_offset,
                            wal_uncompacted_max_offset=partition.wal_uncompacted_max_offset,
                            topic_wal_segment_count=partition.topic_wal_segment_count,
                            topic_wal_min_offset=partition.topic_wal_min_offset,
                            topic_wal_max_offset=partition.topic_wal_max_offset,
                        )
                        for partition in result.partitions
                    ],
                ),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/consumer-groups",
            response_model=ConsumerGroupListResponseV1,
            tags=["consumer-groups"],
            summary="List consumer groups",
            description="List consumer groups and current summary state.",
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid pagination parameters.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
        )
        async def list_consumer_groups(
            request: Request,
            limit: int = Query(default=100, ge=1, le=1000),
            offset: int = Query(default=0, ge=0),
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.list_consumer_groups(
                session,
                limit=limit,
                offset=offset,
            )
            return ConsumerGroupListResponseV1(
                data=ConsumerGroupListDataV1(
                    groups=[
                        ConsumerGroupSummaryV1(
                            group_id=group.group_id,
                            state=group.state,
                            generation=group.generation,
                            protocol_type=group.protocol_type,
                            selected_protocol=group.selected_protocol,
                            member_count=group.member_count,
                            updated_at=group.updated_at,
                        )
                        for group in result.groups
                    ],
                    limit=result.limit,
                    offset=result.offset,
                    total=result.total,
                ),
                request_id=self._request_id(request),
            )

        @self.app.get(
            "/consumer-groups/{group_id}",
            response_model=ConsumerGroupDetailResponseV1,
            tags=["consumer-groups"],
            summary="Describe consumer group",
            description="Describe one consumer group with members, assignments and offsets.",
            responses={
                400: {
                    "model": ErrorEnvelopeV1,
                    "description": "Invalid group id.",
                },
                404: {
                    "model": ErrorEnvelopeV1,
                    "description": "Consumer group not found.",
                },
                500: {
                    "model": ErrorEnvelopeV1,
                    "description": "Internal server error.",
                },
            },
        )
        async def describe_consumer_group(
            group_id: str,
            request: Request,
            session: AsyncSession = Depends(self._get_session()),
        ):
            result = await self.service.describe_consumer_group(
                session,
                group_id=group_id,
            )
            return ConsumerGroupDetailResponseV1(
                data=ConsumerGroupDetailDataV1(
                    group=ConsumerGroupSummaryV1(
                        group_id=result.summary.group_id,
                        state=result.summary.state,
                        generation=result.summary.generation,
                        protocol_type=result.summary.protocol_type,
                        selected_protocol=result.summary.selected_protocol,
                        member_count=result.summary.member_count,
                        updated_at=result.summary.updated_at,
                    ),
                    members=[
                        ConsumerGroupMemberV1(
                            member_id=member.member_id,
                            group_instance_id=member.group_instance_id,
                            is_in_sync=member.is_in_sync,
                            last_heartbeat_at=member.last_heartbeat_at,
                        )
                        for member in result.members
                    ],
                    assignments=[
                        ConsumerGroupAssignmentV1(
                            member_id=assignment.member_id,
                            topic_name=assignment.topic_name,
                            partition_number=assignment.partition_number,
                            generation=assignment.generation,
                        )
                        for assignment in result.assignments
                    ],
                    offsets=[
                        ConsumerGroupOffsetV1(
                            topic_name=offset.topic_name,
                            partition_number=offset.partition_number,
                            committed_offset=offset.committed_offset,
                            committed_metadata=offset.committed_metadata,
                            commit_timestamp=offset.commit_timestamp,
                            committed_member_id=offset.committed_member_id,
                            committed_generation=offset.committed_generation,
                        )
                        for offset in result.offsets
                    ],
                ),
                request_id=self._request_id(request),
            )
