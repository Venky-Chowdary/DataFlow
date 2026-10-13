from __future__ import annotations

from typing import Any
from urllib.parse import quote

from services.embedding_providers.base import (
    EmbeddingConfigError,
    EmbeddingResponseError,
    ProviderResponse,
)
from services.embedding_providers.http_common import response_error, transport_error


def _count_tokens(texts: list[str]) -> tuple[int, bool]:
    return sum(max(1, (len(text.encode("utf-8")) + 3) // 4) for text in texts), True


class OpenAICompatibleProvider:
    provider_name = "openai"
    max_batch_texts = 2048
    max_batch_tokens = 300_000

    def __init__(
        self, model: str, *, api_key: str, base_url: str = "https://api.openai.com/v1",
        session: Any = None, dimensions: int | None = None,
        provider_name: str = "openai", azure_endpoint: str | None = None,
        deployment: str | None = None, api_version: str | None = None,
        base_model: str | None = None,
    ) -> None:
        self.model = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.dimensions = dimensions
        self.provider_name = provider_name
        self.base_model = base_model
        if provider_name == "azure_openai":
            if not api_version:
                raise EmbeddingConfigError("embedding_api_version is required for Azure OpenAI")
            if not azure_endpoint or not deployment:
                raise EmbeddingConfigError("Azure OpenAI endpoint and deployment are required")
            self.url = (
                f"{azure_endpoint.rstrip('/')}/openai/deployments/"
                f"{quote(deployment, safe='')}/embeddings?api-version={quote(api_version, safe='')}"
            )
            self.headers = {"api-key": api_key, "Content-Type": "application/json"}
        else:
            self.url = f"{self.base_url}/embeddings"
            self.headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        if session is None:
            try:
                import requests
            except ImportError:
                from services.embedding_providers.base import EmbeddingProviderUnavailable

                raise EmbeddingProviderUnavailable(
                    "Embedding HTTP providers require requests; install it or choose another provider",
                    retryable=False,
                ) from None
            session = requests.Session()
        self.session = session

    def count_tokens(self, texts: list[str]) -> tuple[int, bool]:
        return _count_tokens(texts)

    def embed_batch(self, texts: list[str]) -> ProviderResponse:
        body: dict[str, Any] = {"input": texts}
        if self.provider_name != "azure_openai":
            body["model"] = self.model
        if self.dimensions:
            body["dimensions"] = self.dimensions
        try:
            response = self.session.post(
                self.url, json=body, headers=self.headers, timeout=60
            )
        except Exception as exc:
            raise transport_error(exc) from None
        error = response_error(response)
        if error:
            raise error
        try:
            payload = response.json()
            data = sorted(payload["data"], key=lambda item: int(item["index"]))
            vectors = [item["embedding"] for item in data]
            input_tokens = payload.get("usage", {}).get("prompt_tokens")
        except (KeyError, TypeError, ValueError, AttributeError):
            raise EmbeddingResponseError(
                "Embedding provider returned an invalid response"
            ) from None
        return ProviderResponse(vectors, input_tokens)
