from __future__ import annotations

import logging
import math
import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

from services.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingProviderError,
    EmbeddingProviderUnavailable,
    EmbeddingRateLimited,
    EmbeddingRequestError,
    EmbeddingResponseError,
    EmbeddingUsage,
    ProviderResponse,
)
from services.embedding_providers.pricing import estimated_cost_usd

logger = logging.getLogger(__name__)
_BUCKETS: dict[tuple[str, str], tuple[float, float, float]] = {}
_BUCKET_LOCK = threading.Lock()


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _acquire_rate(
    provider: str, model: str, tokens: int, rpm: int | None, tpm: int | None,
    clock: Callable[[], float], sleep: Callable[[float], None],
) -> None:
    if not rpm and not tpm:
        return
    key = (provider, model)
    while True:
        now = clock()
        with _BUCKET_LOCK:
            last, requests, token_bucket = _BUCKETS.get(
                key, (now, float(rpm or 1), float(tpm or 0))
            )
            elapsed = max(0.0, now - last)
            if rpm:
                requests = min(float(rpm), requests + elapsed * rpm / 60)
            if tpm:
                token_bucket = min(float(tpm), token_bucket + elapsed * tpm / 60)
            wait = 0.0
            if rpm and requests < 1:
                wait = max(wait, (1 - requests) * 60 / rpm)
            if tpm and tokens > tpm:
                raise EmbeddingRequestError(
                    "Embedding batch exceeds the configured tokens-per-minute limit"
                )
            if tpm and token_bucket < tokens:
                wait = max(wait, (tokens - token_bucket) * 60 / tpm)
            if not wait:
                _BUCKETS[key] = (
                    now, requests - (1 if rpm else 0),
                    token_bucket - (tokens if tpm else 0),
                )
                return
            _BUCKETS[key] = (now, requests, token_bucket)
        sleep(wait)


class EmbeddingRunner:
    def __init__(
        self, provider: EmbeddingProvider, *, usage: EmbeddingUsage,
        retry_max_attempts: int = 5, base_delay: float = 0.5,
        max_delay: float = 60, requests_per_minute: int | None = None,
        tokens_per_minute: int | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.provider = provider
        self.usage = usage
        self.retry_max_attempts = max(1, int(retry_max_attempts))
        self.base_delay = max(0.0, base_delay)
        self.max_delay = max(0.0, max_delay)
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self.sleep = sleep
        self.clock = clock
        self._dimension: int | None = None

    def embed(self, texts: list[str]) -> list[list[float]]:
        batches: list[tuple[int, list[str], int, bool]] = []
        current: list[str] = []
        current_tokens = 0
        estimated = False
        offset = 0
        for index, text in enumerate(texts):
            count, is_estimated = self.provider.count_tokens([text])
            limit = self.provider.max_batch_tokens
            if limit and count > limit:
                raise EmbeddingRequestError(
                    f"Text at index {index} exceeds the provider token limit"
                )
            if current and (
                len(current) >= self.provider.max_batch_texts
                or (limit and current_tokens + count > limit)
            ):
                batches.append((offset, current, current_tokens, estimated))
                offset += len(current)
                current, current_tokens, estimated = [], 0, False
            current.append(text)
            current_tokens += count
            estimated = estimated or is_estimated
        if current:
            batches.append((offset, current, current_tokens, estimated))

        output: list[list[float]] = []
        for _, batch, estimate, _count_estimated in batches:
            vectors, input_tokens, is_estimated = self._embed_batch(batch, estimate)
            if len(vectors) != len(batch):
                raise EmbeddingResponseError(
                    f"Embedding response returned {len(vectors)} vectors for {len(batch)} texts"
                )
            for vector in vectors:
                if not isinstance(vector, (list, tuple)) or not vector:
                    raise EmbeddingResponseError("Embedding response contains invalid vector data")
                try:
                    values = [float(value) for value in vector]
                except (TypeError, ValueError, OverflowError) as exc:
                    raise EmbeddingResponseError("Embedding response contains non-numeric data") from exc
                if not all(math.isfinite(value) for value in values):
                    raise EmbeddingResponseError("Embedding response contains non-numeric data")
                if self._dimension is None:
                    self._dimension = len(values)
                elif len(values) != self._dimension:
                    raise EmbeddingResponseError("Embedding response dimension drift detected")
                output.append(values)
            self.usage.input_tokens += input_tokens
            self.usage.token_count_estimated |= is_estimated
            cost = estimated_cost_usd(
                self.usage.provider, self.usage.model, input_tokens,
                getattr(self.provider, "base_model", None),
            )
            if cost is None:
                self.usage.estimated_cost_usd = None
            else:
                self.usage.estimated_cost_usd = (
                    self.usage.estimated_cost_usd or 0.0
                ) + cost
        return output

    def _embed_batch(
        self, texts: list[str], estimated_tokens: int
    ) -> tuple[list[list[float]], int, bool]:
        for attempt in range(1, self.retry_max_attempts + 1):
            _acquire_rate(
                self.usage.provider, self.usage.model, estimated_tokens,
                self.requests_per_minute, self.tokens_per_minute, self.clock, self.sleep,
            )
            self.usage.calls += 1
            try:
                response = self.provider.embed_batch(texts)
                if not isinstance(response, ProviderResponse):
                    response = ProviderResponse(**response)
                provided_tokens = response.input_tokens is not None
                return (
                    response.vectors,
                    int(response.input_tokens) if provided_tokens else estimated_tokens,
                    not provided_tokens,
                )
            except (EmbeddingRateLimited, EmbeddingProviderUnavailable) as exc:
                retryable = not isinstance(exc, EmbeddingProviderUnavailable) or exc.retryable
                if not retryable or attempt >= self.retry_max_attempts:
                    exc.attempts = attempt
                    exc.args = (f"{exc} (after {attempt} attempt(s))",)
                    raise
                self.usage.retries += 1
                ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
                server_delay = _retry_after(exc.retry_after)
                delay = min(
                    self.max_delay,
                    server_delay if server_delay is not None else random.uniform(0, ceiling),  # nosec B311
                )
                logger.warning(
                    "Embedding retry provider=%s model=%s batch_size=%d attempt=%d status=%s delay=%.2f",
                    self.usage.provider, self.usage.model, len(texts), attempt,
                    exc.status_code or "transport", delay,
                )
                self.sleep(delay)
            except EmbeddingProviderError:
                raise
            except (TimeoutError, ConnectionError, OSError):
                wrapped = EmbeddingProviderUnavailable("Embedding provider connection failed")
                if attempt >= self.retry_max_attempts:
                    wrapped.attempts = attempt
                    wrapped.args = (f"{wrapped} (after {attempt} attempt(s))",)
                    raise wrapped from None
                self.usage.retries += 1
                ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
                delay = random.uniform(0, ceiling)  # nosec B311
                logger.warning(
                    "Embedding retry provider=%s model=%s batch_size=%d attempt=%d status=transport delay=%.2f",
                    self.usage.provider, self.usage.model, len(texts), attempt, delay,
                )
                self.sleep(delay)
        raise EmbeddingProviderUnavailable("Embedding retry budget exhausted")
