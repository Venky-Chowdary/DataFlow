from __future__ import annotations

import builtins
import json
from typing import Any

import pytest

from services.embedding_providers import (
    EmbeddingAuthError,
    EmbeddingConfigError,
    EmbeddingProviderUnavailable,
    EmbeddingRateLimited,
    EmbeddingRequestError,
    EmbeddingResponseError,
    EmbeddingRunner,
    EmbeddingUsage,
    ProviderResponse,
)
from services.embedding_providers.bedrock import BedrockProvider
from services.embedding_providers.cohere import CohereProvider
from services.embedding_providers.config import resolve_provider
from services.embedding_providers.openai_compat import OpenAICompatibleProvider
from services.embedding_providers.pricing import estimated_cost_usd
from services.embedding_providers.runner import _BUCKETS


class FakeResponse:
    def __init__(
        self, status: int = 200, body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status
        self.body = body or {}
        self.headers = headers or {}
        self.text = "sentinel response body"

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


class FakeProvider:
    provider_name = "openai"
    model = "text-embedding-3-small"
    max_batch_texts = 10
    max_batch_tokens = 5

    def __init__(self, vectors=None):
        self.vectors = vectors
        self.batches: list[list[str]] = []

    def count_tokens(self, texts):
        return sum(len(text) for text in texts), True

    def embed_batch(self, texts):
        self.batches.append(list(texts))
        vectors = self.vectors or [[0.1, 0.2] for _ in texts]
        return ProviderResponse(vectors, input_tokens=None)


def _openai_body(n: int = 1, tokens: int = 3):
    return {
        "data": [
            {"index": i, "embedding": [float(i), 0.25]} for i in range(n)
        ],
        "usage": {"prompt_tokens": tokens},
    }


def test_retry_after_and_transient_retries():
    session = FakeSession([
        FakeResponse(429, headers={"Retry-After": "2"}),
        FakeResponse(body=_openai_body()),
    ])
    provider = OpenAICompatibleProvider(
        "text-embedding-3-small", api_key="sentinel-key", session=session
    )
    usage = EmbeddingUsage("openai", provider.model, estimated_cost_usd=0.0)
    delays = []
    vectors = EmbeddingRunner(
        provider, usage=usage, sleep=delays.append
    ).embed(["safe input"])
    assert vectors == [[0.0, 0.25]]
    assert delays == [2.0]
    assert (usage.calls, usage.retries, usage.input_tokens) == (2, 1, 3)

    session = FakeSession([
        FakeResponse(503), FakeResponse(503), FakeResponse(body=_openai_body())
    ])
    provider.session = session
    delays.clear()
    import services.embedding_providers.runner as runner_module

    original = runner_module.random.uniform
    runner_module.random.uniform = lambda low, high: high
    try:
        EmbeddingRunner(provider, usage=usage, sleep=delays.append).embed(["safe"])
    finally:
        runner_module.random.uniform = original
    assert delays[-2:] == [0.5, 1.0]


@pytest.mark.parametrize(
    ("status", "error_type"),
    [(401, EmbeddingAuthError), (400, EmbeddingRequestError)],
)
def test_auth_and_validation_errors_do_not_retry(status, error_type):
    session = FakeSession([FakeResponse(status)])
    provider = OpenAICompatibleProvider("model", api_key="secret", session=session)
    usage = EmbeddingUsage("openai", "model")
    with pytest.raises(error_type):
        EmbeddingRunner(provider, usage=usage, sleep=lambda _delay: None).embed(["safe"])
    assert usage.calls == 1
    assert usage.retries == 0


def test_exhausted_rate_limit_has_attempt_count_and_redacts_key_and_text(caplog):
    key = "APIKEY_SENTINEL_8192"
    text = "PROMPT_SENTINEL_5634"
    session = FakeSession([FakeResponse(429), FakeResponse(429)])
    provider = OpenAICompatibleProvider("model", api_key=key, session=session)
    with pytest.raises(EmbeddingRateLimited) as raised:
        EmbeddingRunner(
            provider,
            usage=EmbeddingUsage("openai", "model"),
            retry_max_attempts=2,
            sleep=lambda _delay: None,
        ).embed([text])
    assert raised.value.attempts == 2
    assert "2 attempt" in str(raised.value)
    assert key not in str(raised.value) + caplog.text
    assert text not in str(raised.value) + caplog.text


def test_cohere_batches_200_texts_and_sends_search_document():
    class CohereSession:
        def __init__(self):
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append(kwargs["json"])
            count = len(kwargs["json"]["texts"])
            return FakeResponse(
                body={
                    "embeddings": {"float": [[0.5] for _ in range(count)]},
                    "meta": {"billed_units": {"input_tokens": count}},
                }
            )

    session = CohereSession()
    provider = CohereProvider("embed-english-v3.0", api_key="key", session=session)
    usage = EmbeddingUsage("cohere", provider.model, estimated_cost_usd=0.0)
    vectors = EmbeddingRunner(provider, usage=usage).embed(
        [f"document {i}" for i in range(200)]
    )
    assert [len(call["texts"]) for call in session.calls] == [96, 96, 8]
    assert all(call["input_type"] == "search_document" for call in session.calls)
    assert all(call["embedding_types"] == ["float"] for call in session.calls)
    assert len(vectors) == 200


def test_token_batches_split_and_oversized_text_is_refused_without_echo():
    provider = FakeProvider()
    runner = EmbeddingRunner(provider, usage=EmbeddingUsage("openai", "model"))
    runner.embed(["aa", "bb", "ccc"])
    assert provider.batches == [["aa", "bb"], ["ccc"]]
    with pytest.raises(EmbeddingRequestError, match="index 0") as raised:
        runner.embed(["too-long"])
    assert "too-long" not in str(raised.value)


def test_dimension_and_response_count_validation():
    class DriftingProvider(FakeProvider):
        max_batch_texts = 1

        def embed_batch(self, texts):
            self.batches.append(list(texts))
            vector = [0.1, 0.2] if len(self.batches) == 1 else [0.1, 0.2, 0.3]
            return ProviderResponse([vector])

    with pytest.raises(EmbeddingResponseError, match="dimension drift"):
        EmbeddingRunner(
            DriftingProvider(), usage=EmbeddingUsage("openai", "model")
        ).embed(["a", "b"])

    provider = FakeProvider(vectors=[[0.1, 0.2]])
    with pytest.raises(EmbeddingResponseError, match="returned 1 vectors"):
        EmbeddingRunner(provider, usage=EmbeddingUsage("openai", "model")).embed(
            ["a", "b", "c", "d", "e", "f"]
        )


def test_cost_uses_provider_tokens_or_estimate_and_unknown_is_none():
    session = FakeSession([FakeResponse(body=_openai_body(tokens=100))])
    provider = OpenAICompatibleProvider(
        "text-embedding-3-small", api_key="key", session=session
    )
    usage = EmbeddingUsage("openai", provider.model, estimated_cost_usd=0.0)
    EmbeddingRunner(provider, usage=usage).embed(["some content"])
    assert usage.input_tokens == 100
    assert usage.token_count_estimated is False
    assert usage.estimated_cost_usd == pytest.approx(0.02 * 100 / 1_000_000)
    assert estimated_cost_usd("openai", "unknown-model", 10) is None

    provider = FakeProvider()
    usage = EmbeddingUsage("openai", "unknown-model")
    EmbeddingRunner(provider, usage=usage).embed(["est"])
    assert usage.token_count_estimated is True
    assert usage.input_tokens == len("est")
    assert usage.estimated_cost_usd is None


def test_azure_url_header_and_body_shape():
    session = FakeSession([FakeResponse(body=_openai_body())])
    provider = OpenAICompatibleProvider(
        "deployment-a",
        api_key="azure-secret",
        provider_name="azure_openai",
        azure_endpoint="https://example.azure.com",
        deployment="deployment-a",
        api_version="2024-02-01",
        session=session,
    )
    provider.embed_batch(["safe"])
    call = session.calls[0]
    assert call["url"] == (
        "https://example.azure.com/openai/deployments/deployment-a/embeddings"
        "?api-version=2024-02-01"
    )
    assert call["headers"]["api-key"] == "azure-secret"
    assert "Authorization" not in call["headers"]
    assert call["json"] == {"input": ["safe"]}


def test_bedrock_dependency_and_request_shapes(monkeypatch):
    real_import = builtins.__import__

    def missing_boto3(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_boto3)
    with pytest.raises(EmbeddingProviderUnavailable, match="requires boto3") as raised:
        BedrockProvider("amazon.titan-embed-text-v2:0")
    assert raised.value.retryable is False
    monkeypatch.setattr(builtins, "__import__", real_import)

    class Body:
        def __init__(self, value):
            self.value = value

        def read(self):
            return json.dumps(self.value).encode()

    class Client:
        def __init__(self, response):
            self.response = response
            self.calls = []

        def invoke_model(self, **kwargs):
            self.calls.append(kwargs)
            return {"body": Body(self.response)}

    titan_client = Client({"embedding": [0.1, 0.2], "inputTextTokenCount": 4})
    titan = BedrockProvider(
        "amazon.titan-embed-text-v2:0", dimensions=256, client=titan_client
    )
    titan.embed_batch(["x"])
    assert titan_client.calls[0]["modelId"] == "amazon.titan-embed-text-v2:0"
    assert json.loads(titan_client.calls[0]["body"]) == {
        "inputText": "x", "dimensions": 256
    }

    cohere_client = Client({"embeddings": [[0.1]], "inputTextTokenCount": 2})
    bedrock_cohere = BedrockProvider("cohere.embed-english-v3", client=cohere_client)
    bedrock_cohere.embed_batch(["x"])
    assert json.loads(cohere_client.calls[0]["body"]) == {
        "texts": ["x"], "input_type": "search_document", "truncate": "END"
    }


def test_rate_limiter_uses_injected_clock():
    _BUCKETS.clear()
    provider = FakeProvider()
    provider.max_batch_texts = 1
    now = [0.0]
    delays = []

    def sleep(delay):
        delays.append(delay)
        now[0] += delay

    EmbeddingRunner(
        provider,
        usage=EmbeddingUsage("openai", "rate-limit-test"),
        requests_per_minute=1,
        sleep=sleep,
        clock=lambda: now[0],
    ).embed(["a", "b"])
    assert delays == [60.0]
    _BUCKETS.clear()


def test_vectorization_passes_validated_rate_limits_to_runner(monkeypatch):
    from services import embedding_providers, vectorization
    from services.embedding_providers.base import EmbeddingConfigError

    observed = {}

    class Runner:
        def __init__(self, provider, *, usage, **options):
            observed.update(options)

        def embed(self, texts):
            return [[0.25] * 8 for _ in texts]

    monkeypatch.setattr(embedding_providers, "EmbeddingRunner", Runner)
    vectors = vectorization.embed(
        ["rate-limited text"],
        model="hash/8",
        use_cache=False,
        durable=False,
        embedding_extra={
            "embedding_requests_per_minute": "12",
            "embedding_tokens_per_minute": 240,
        },
    )
    assert len(vectors) == 1
    assert observed == {"requests_per_minute": 12, "tokens_per_minute": 240}

    with pytest.raises(EmbeddingConfigError, match="positive integer"):
        vectorization.embed(
            ["rate-limit validation"],
            model="hash/8",
            use_cache=False,
            durable=False,
            embedding_extra={"embedding_requests_per_minute": 0},
        )


def test_config_validation_errors():
    with pytest.raises(EmbeddingConfigError, match="OPENAI_API_KEY"):
        resolve_provider("openai/text-embedding-3-small", {})
    with pytest.raises(EmbeddingConfigError, match="HTTPS"):
        resolve_provider(
            "openai-compatible/model", {"embedding_base_url": "http://example.com"}
        )
    with pytest.raises(EmbeddingConfigError, match="positive integer"):
        resolve_provider(
            "openai-compatible/model",
            {
                "embedding_base_url": "http://localhost:1234",
                "embedding_dimensions": 0,
            },
        )
    with pytest.raises(EmbeddingConfigError, match="embedding_api_version"):
        resolve_provider(
            "azure/deployment",
            {
                "embedding_endpoint": "https://azure.example",
                "embedding_api_key": "key",
            },
        )


def test_embedding_cache_hits_skip_provider_and_are_counted():
    from services.vectorization import clear_memory_cache, embed

    clear_memory_cache()
    usage = EmbeddingUsage("hash", "hash/8", estimated_cost_usd=0.0)
    first = embed(["cache hit check"], "hash/8", durable=False, usage=usage)
    assert len(first) == 1
    assert (usage.cache_misses, usage.cache_hits, usage.calls) == (1, 0, 1)
    second = embed(["cache hit check"], "hash/8", durable=False, usage=usage)
    assert second == first
    assert (usage.cache_misses, usage.cache_hits, usage.calls) == (1, 1, 1)
    clear_memory_cache()
