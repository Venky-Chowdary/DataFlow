from __future__ import annotations

import json
from typing import Any

from services.embedding_providers.base import (
    EmbeddingAuthError,
    EmbeddingProviderUnavailable,
    EmbeddingRateLimited,
    EmbeddingRequestError,
    EmbeddingResponseError,
    ProviderResponse,
)


class BedrockProvider:
    provider_name = "bedrock"
    max_batch_tokens = None

    def __init__(
        self, model: str, *, region: str | None = None, dimensions: int | None = None,
        client: Any = None,
    ) -> None:
        self.model = model
        self.dimensions = dimensions
        self.is_titan = model.startswith("amazon.titan-embed-text-v2")
        self.max_batch_texts = 1 if self.is_titan else 96
        if client is None:
            try:
                import boto3
            except ImportError:
                raise EmbeddingProviderUnavailable(
                    "bedrock provider requires boto3; install it or choose another provider",
                    retryable=False,
                ) from None
            client = boto3.client("bedrock-runtime", region_name=region)
        self.client = client

    def count_tokens(self, texts: list[str]) -> tuple[int, bool]:
        return sum(max(1, (len(text.encode("utf-8")) + 3) // 4) for text in texts), True

    def embed_batch(self, texts: list[str]) -> ProviderResponse:
        if self.is_titan:
            body: dict[str, Any] = {"inputText": texts[0]}
            if self.dimensions:
                body["dimensions"] = self.dimensions
        else:
            body = {"texts": texts, "input_type": "search_document", "truncate": "END"}
        try:
            response = self.client.invoke_model(
                modelId=self.model, body=json.dumps(body),
                contentType="application/json", accept="application/json",
            )
            raw = response["body"].read()
        except Exception as exc:
            status = None
            error_response = getattr(exc, "response", None)
            if isinstance(error_response, dict):
                metadata = error_response.get("ResponseMetadata")
                if isinstance(metadata, dict):
                    status = metadata.get("HTTPStatusCode")
            if status in (401, 403):
                raise EmbeddingAuthError(
                    f"Bedrock embedding authentication failed (HTTP {status})",
                    status_code=int(status),
                ) from None
            if status in (400, 404, 413, 422):
                raise EmbeddingRequestError(
                    f"Bedrock embedding request rejected (HTTP {status})",
                    status_code=int(status),
                ) from None
            if status == 429:
                raise EmbeddingRateLimited(status_code=429) from None
            raise EmbeddingProviderUnavailable(
                "Bedrock embedding request failed"
            ) from None
        try:
            payload = json.loads(raw)
            if self.is_titan:
                vectors = [payload["embedding"]]
                tokens = payload.get("inputTextTokenCount")
            else:
                vectors = payload["embeddings"]
                tokens = payload.get("inputTextTokenCount")
        except (KeyError, TypeError, ValueError, AttributeError):
            raise EmbeddingResponseError(
                "Bedrock embedding provider returned an invalid response"
            ) from None
        return ProviderResponse(vectors, tokens)
