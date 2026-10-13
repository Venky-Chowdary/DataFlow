from __future__ import annotations

from collections.abc import Mapping
from typing import Any


_REQUEST_ID_HEADERS = (
    "x-request-id",
    "x-correlation-id",
    "request-id",
    "x-amzn-requestid",
)


def safe_exception_context(exc: BaseException) -> str:
    details = [type(exc).__name__]
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if not isinstance(status, int) or isinstance(status, bool):
        status = getattr(exc, "status", None)
    if isinstance(status, int) and not isinstance(status, bool):
        details.append(f"HTTP {status}")

    request_id = ""
    headers = getattr(response, "headers", None)
    if isinstance(headers, Mapping):
        for name in _REQUEST_ID_HEADERS:
            value = next(
                (
                    item
                    for key, item in headers.items()
                    if str(key).lower() == name and item not in (None, "")
                ),
                "",
            )
            if value:
                request_id = str(value)
                break
    if not request_id:
        request_id = str(getattr(exc, "request_id", "") or "")
    request_id = "".join(char for char in request_id if char.isprintable())[:128]
    if request_id:
        details.append(f"request_id={request_id}")
    return f" (cause: {'; '.join(details)})"


class ConnectorError(Exception):
    def __init__(
        self,
        message: str,
        *,
        request_id: str = "",
        stream: str = "",
        status: int | None = None,
        url: str = "",
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.stream = stream
        self.status = status
        self.url = url

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": type(self).__name__,
            "message": str(self),
            "request_id": self.request_id,
            "stream": self.stream,
            "status": self.status,
        }


class ConnectorAuthError(ConnectorError):
    pass


class ConnectorRequestError(ConnectorError):
    pass


class RateLimitExhausted(ConnectorError):
    pass


class TransientExhausted(ConnectorError):
    pass


class ResponseShapeError(ConnectorError):
    pass


class PaginationError(ConnectorError):
    pass


class SchemaDriftError(ConnectorError):
    pass


class ManifestError(ConnectorError, ValueError):
    def __init__(self, message: str, *, path: str = "") -> None:
        super().__init__(message)
        self.path = path

    def to_dict(self) -> dict[str, Any]:
        return {**super().to_dict(), "path": self.path}
