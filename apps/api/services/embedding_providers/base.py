from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass
class ProviderResponse:
    vectors: list[list[float]]
    input_tokens: int | None = None


@runtime_checkable
class EmbeddingProvider(Protocol):
    provider_name: str
    model: str
    max_batch_texts: int
    max_batch_tokens: int | None

    def count_tokens(self, texts: list[str]) -> tuple[int, bool]: ...

    def embed_batch(self, texts: list[str]) -> ProviderResponse: ...


@dataclass
class EmbeddingUsage:
    provider: str
    model: str
    calls: int = 0
    retries: int = 0
    input_tokens: int = 0
    token_count_estimated: bool = False
    cache_hits: int = 0
    cache_misses: int = 0
    estimated_cost_usd: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "model": self.model,
            "calls": self.calls,
            "retries": self.retries,
            "input_tokens": self.input_tokens,
            "token_count_estimated": self.token_count_estimated,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "estimated_cost_usd": self.estimated_cost_usd,
        }


def usage_for_provider(provider: EmbeddingProvider) -> EmbeddingUsage:
    from services.embedding_providers.pricing import estimated_cost_usd

    name = str(provider.provider_name)
    model = str(provider.model)
    base_model = getattr(provider, "base_model", None)
    known = estimated_cost_usd(name, model, 0, base_model) is not None
    return EmbeddingUsage(
        provider=name,
        model=model,
        estimated_cost_usd=0.0 if known else None,
    )


class EmbeddingProviderError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.attempts: int | None = None


class EmbeddingAuthError(EmbeddingProviderError):
    pass


class EmbeddingRequestError(EmbeddingProviderError):
    pass


class EmbeddingRateLimited(EmbeddingProviderError):
    def __init__(
        self, message: str = "Embedding provider rate limited the request",
        *, retry_after: str | None = None, status_code: int = 429,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after


class EmbeddingProviderUnavailable(EmbeddingProviderError):
    def __init__(
        self, message: str = "Embedding provider is unavailable",
        *, retry_after: str | None = None, status_code: int | None = None,
        retryable: bool = True,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after = retry_after
        self.retryable = retryable


class EmbeddingResponseError(EmbeddingProviderError):
    pass


class EmbeddingConfigError(EmbeddingProviderError):
    pass
