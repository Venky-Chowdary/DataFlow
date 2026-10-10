from __future__ import annotations

from typing import Any


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
