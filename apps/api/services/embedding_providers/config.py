from __future__ import annotations

import os
from collections.abc import Mapping
from urllib.parse import urlparse

from services.embedding_providers.base import EmbeddingConfigError
from services.embedding_providers.bedrock import BedrockProvider
from services.embedding_providers.cohere import CohereProvider
from services.embedding_providers.local import LocalProvider
from services.embedding_providers.openai_compat import OpenAICompatibleProvider


def _key(extra: Mapping, env_name: str, *, required: bool = True) -> str:
    value = str(extra.get("embedding_api_key") or os.environ.get(env_name) or "")
    if required and not value:
        raise EmbeddingConfigError(f"Missing API key; configure {env_name}")
    return value


def _validate_url(value: str, field: str) -> str:
    parsed = urlparse(value)
    local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local):
        raise EmbeddingConfigError(f"{field} must use HTTPS (HTTP is allowed for localhost)")
    if not parsed.netloc:
        raise EmbeddingConfigError(f"{field} must be an absolute URL")
    return value.rstrip("/")


def _dimensions(extra: Mapping) -> int | None:
    value = extra.get("embedding_dimensions")
    if value is None:
        return None
    try:
        dimension = int(value)
    except (TypeError, ValueError) as exc:
        raise EmbeddingConfigError("embedding_dimensions must be a positive integer") from exc
    if dimension <= 0:
        raise EmbeddingConfigError("embedding_dimensions must be a positive integer")
    return dimension


def resolve_provider(model: str, extra: Mapping) -> object:
    name = str(model or "").strip()
    dimensions = _dimensions(extra)
    session = extra.get("_embedding_session")
    if name.startswith("deterministic/"):
        name = "hash/" + name.split("/", 1)[1]
    if name.startswith("hash/"):
        suffix = name.split("/", 1)[1]
        if not suffix.isdigit() or int(suffix) <= 0:
            raise EmbeddingConfigError("hash model dimension must be a positive integer")
        from services.vectorization import _get_embedder

        return LocalProvider(name, _get_embedder(name), "hash")
    if name.startswith("sentence-transformers/"):
        from services.vectorization import _get_embedder

        embedder = _get_embedder(name)
        return LocalProvider(name, embedder, str(getattr(embedder, "backend", "sentence_transformers")))
    if name.startswith("openai/") or name.startswith("text-embedding-"):
        canonical = name.split("/", 1)[1] if name.startswith("openai/") else name
        return OpenAICompatibleProvider(
            canonical, api_key=_key(extra, "OPENAI_API_KEY"),
            session=session, dimensions=dimensions,
        )
    if name.startswith("azure/"):
        deployment = name.split("/", 1)[1]
        endpoint = _validate_url(str(extra.get("embedding_endpoint") or ""), "embedding_endpoint")
        return OpenAICompatibleProvider(
            deployment, api_key=_key(extra, "AZURE_OPENAI_API_KEY"),
            provider_name="azure_openai", azure_endpoint=endpoint,
            deployment=deployment, api_version=str(extra.get("embedding_api_version") or ""),
            base_model=extra.get("embedding_base_model"), session=session, dimensions=dimensions,
        )
    if name.startswith("openai-compatible/"):
        canonical = name.split("/", 1)[1]
        base = _validate_url(str(extra.get("embedding_base_url") or ""), "embedding_base_url")
        return OpenAICompatibleProvider(
            canonical, api_key=_key(extra, "OPENAI_API_KEY", required=False),
            base_url=base, provider_name="openai_compatible",
            session=session, dimensions=dimensions,
        )
    if name.startswith("cohere/"):
        return CohereProvider(
            name.split("/", 1)[1], api_key=_key(extra, "COHERE_API_KEY"), session=session
        )
    if name.startswith("bedrock/"):
        return BedrockProvider(
            name.split("/", 1)[1],
            region=extra.get("embedding_region"), dimensions=dimensions,
            client=extra.get("_bedrock_client"),
        )
    raise EmbeddingConfigError(f"Unsupported embedding model prefix: {name.split('/', 1)[0]}")
