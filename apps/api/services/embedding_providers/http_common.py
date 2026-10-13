from __future__ import annotations

from typing import Any

from services.embedding_providers.base import (
    EmbeddingAuthError,
    EmbeddingProviderUnavailable,
    EmbeddingRateLimited,
    EmbeddingRequestError,
)


def response_error(response: Any) -> Exception | None:
    status = int(getattr(response, "status_code", 200))
    if status < 400:
        return None
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After")
    if status in (401, 403):
        return EmbeddingAuthError(
            f"Embedding provider authentication failed (HTTP {status})",
            status_code=status,
        )
    if status == 429:
        return EmbeddingRateLimited(
            "Embedding provider rate limited the request",
            retry_after=str(retry_after) if retry_after is not None else None,
        )
    if status >= 500:
        return EmbeddingProviderUnavailable(
            f"Embedding provider unavailable (HTTP {status})",
            retry_after=str(retry_after) if retry_after is not None else None,
            status_code=status,
        )
    return EmbeddingRequestError(
        f"Embedding request rejected (HTTP {status})", status_code=status
    )


def transport_error(exc: BaseException) -> EmbeddingProviderUnavailable:
    if isinstance(exc, TimeoutError):
        label = "timed out"
    else:
        label = "connection failed"
    return EmbeddingProviderUnavailable(f"Embedding provider {label}")
