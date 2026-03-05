from __future__ import annotations

from enum import StrEnum


class AdminErrorCode(StrEnum):
    ALREADY_EXISTS = "already_exists"
    NOT_FOUND = "not_found"
    INVALID_REQUEST = "invalid_request"
    INTERNAL_ERROR = "internal_error"
    REQUEST_TOO_LARGE = "request_too_large"
    REQUEST_TIMEOUT = "request_timeout"


class AdminApiError(Exception):
    def __init__(
        self,
        *,
        code: AdminErrorCode,
        message: str,
        status_code: int,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class InvalidRequestError(AdminApiError):
    def __init__(self, message: str) -> None:
        super().__init__(
            code=AdminErrorCode.INVALID_REQUEST,
            message=message,
            status_code=400,
        )


class AlreadyExistsError(AdminApiError):
    def __init__(self, message: str) -> None:
        super().__init__(
            code=AdminErrorCode.ALREADY_EXISTS,
            message=message,
            status_code=409,
        )


class NotFoundError(AdminApiError):
    def __init__(self, message: str) -> None:
        super().__init__(
            code=AdminErrorCode.NOT_FOUND,
            message=message,
            status_code=404,
        )


class InternalError(AdminApiError):
    def __init__(self, message: str = "internal server error") -> None:
        super().__init__(
            code=AdminErrorCode.INTERNAL_ERROR,
            message=message,
            status_code=500,
        )
