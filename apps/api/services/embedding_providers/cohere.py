from __future__ import annotations

from typing import Any

from services.embedding_providers.base import EmbeddingResponseError, ProviderResponse
from services.embedding_providers.http_common import response_error, transport_error


class CohereProvider:
    provider_name = "cohere"
    max_batch_texts = 96
    max_batch_tokens = 300_000
    url = "https://api.cohere.com/v2/embed"

    def __init__(self, model: str, *, api_key: str, session: Any = None) -> None:
        self.model = model
        self.api_key = api_key
        if session is None:
            import requests

            session = requests.Session()
        self.session = session

    def count_tokens(self, texts: list[str]) -> tuple[int, bool]:
        return sum(max(1, (len(text.encode("utf-8")) + 3) // 4) for text in texts), True

    def embed_batch(self, texts: list[str]) -> ProviderResponse:
        try:
            response = self.session.post(
                self.url,
                json={
                    "model": self.model,
                    "texts": texts,
                    "input_type": "search_document",
                    "embedding_types": ["float"],
                },
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
        except Exception as exc:
            raise transport_error(exc) from None
        error = response_error(response)
        if error:
            raise error
        try:
            payload = response.json()
            vectors = payload["embeddings"]["float"]
            tokens = payload.get("meta", {}).get("billed_units", {}).get("input_tokens")
        except (KeyError, TypeError, AttributeError):
            raise EmbeddingResponseError(
                "Embedding provider returned an invalid response"
            ) from None
        return ProviderResponse(vectors, tokens)
