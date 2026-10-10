from __future__ import annotations

from typing import Any

from services.embedding_providers.base import ProviderResponse


class LocalProvider:
    max_batch_texts = 2048
    max_batch_tokens = None

    def __init__(self, model: str, embedder: Any, provider_name: str) -> None:
        self.model = model
        self.embedder = embedder
        self.provider_name = provider_name

    def count_tokens(self, texts: list[str]) -> tuple[int, bool]:
        return sum(max(1, (len(text.encode("utf-8")) + 3) // 4) for text in texts), True

    def embed_batch(self, texts: list[str]) -> ProviderResponse:
        return ProviderResponse(self.embedder.embed(texts))
