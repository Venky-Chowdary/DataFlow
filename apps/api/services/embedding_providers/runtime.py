from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.brand_env import getenv_brand
from services.embedding_providers.base import (
    EmbeddingConfigError,
    EmbeddingUsage,
    usage_for_provider,
)
from services.embedding_providers.config import resolve_provider

EMBEDDING_EXTRA_FIELDS = (
    "embedding_api_key",
    "embedding_base_url",
    "embedding_endpoint",
    "embedding_api_version",
    "embedding_base_model",
    "embedding_region",
    "embedding_dimensions",
    "embedding_requests_per_minute",
    "embedding_tokens_per_minute",
)


def provider_extra_from_options(options: Mapping[str, Any]) -> dict[str, Any]:
    out = {
        name: options[name]
        for name in EMBEDDING_EXTRA_FIELDS
        if name in options and options[name] is not None
    }
    for name in ("_embedding_session", "_bedrock_client"):
        if name in options:
            out[name] = options[name]
    return out


def create_embedding_usage(
    model: str | None,
    extra: Mapping[str, Any],
    embedding_column: str | None = None,
) -> EmbeddingUsage:
    if embedding_column:
        return EmbeddingUsage(
            "source_embedding", f"column:{embedding_column}",
            estimated_cost_usd=0.0,
        )
    resolved = model or getenv_brand(
        "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
    )
    return usage_for_provider(resolve_provider(resolved, extra))


def provider_rate_limit_kwargs(extra: Mapping[str, Any]) -> dict[str, int]:
    limits: dict[str, int] = {}
    for option, argument in (
        ("embedding_requests_per_minute", "requests_per_minute"),
        ("embedding_tokens_per_minute", "tokens_per_minute"),
    ):
        value = extra.get(option)
        if value is None:
            continue
        try:
            limit = int(value)
        except (TypeError, ValueError):
            raise EmbeddingConfigError(
                f"{option} must be a positive integer"
            ) from None
        if limit <= 0:
            raise EmbeddingConfigError(f"{option} must be a positive integer")
        limits[argument] = limit
    return limits
