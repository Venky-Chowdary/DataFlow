from __future__ import annotations

import logging
import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import requests

from connectors.sdk.declarative.errors import (
    ConnectorAuthError,
    ConnectorRequestError,
    RateLimitExhausted,
    ResponseShapeError,
    TransientExhausted,
)
from services.error_handling import RetryBudget, with_retry
from services.mcp_rate_limit import TokenBucketStore
from services.secret_config import redact_config, redact_url
from services.value_serializer import load_http_json

logger = logging.getLogger(__name__)

_REQUEST_ID_RESPONSE_HEADERS = (
    "x-request-id",
    "x-correlation-id",
    "request-id",
    "x-amzn-requestid",
)


class _SensitiveURLLogFilter(logging.Filter):
    _query_path = re.compile(r'''(?P<path>/[^?\s'"]*)\?(?P<query>[^'"\s]*)''')

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except (TypeError, ValueError):
            return True

        def redact(match: re.Match[str]) -> str:
            safe = urlsplit(redact_url(f"https://log.invalid{match.group(0)}"))
            return safe.path + (f"?{safe.query}" if safe.query else "")

        record.msg = self._query_path.sub(redact, message)
        record.args = ()
        return True


_http_pool_logger = logging.getLogger("urllib3.connectionpool")
if not any(isinstance(item, _SensitiveURLLogFilter) for item in _http_pool_logger.filters):
    _http_pool_logger.addFilter(_SensitiveURLLogFilter())


@dataclass(frozen=True)
class HttpResult:
    response: requests.Response
    request_id: str
    payload: Any = None


def _header_value(headers: Mapping[str, Any], name: str) -> str:
    for key, value in headers.items():
        if str(key).lower() == name.lower() and value not in (None, ""):
            return str(value)
    return ""


def _redact_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in headers.items():
        normalized = str(key).lower().replace("-", "_")
        sensitive_name = any(
            marker in normalized
            for marker in (
                "authorization",
                "api_key",
                "apikey",
                "token",
                "secret",
                "password",
                "credential",
                "cookie",
            )
        )
        redacted[str(key)] = (
            "***" if sensitive_name else redact_config({normalized: value})[normalized]
        )
    return redacted


class HttpRequester:
    """Single HTTP path for declarative connectors with bounded retries."""

    def __init__(
        self,
        *,
        timeout_s: float = 60.0,
        max_attempts: int = 5,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 60.0,
        jitter: bool = True,
        rate_limit_per_second: float | None = None,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if base_delay_seconds < 0 or max_delay_seconds < 0:
            raise ValueError("retry delays cannot be negative")
        if rate_limit_per_second is not None and rate_limit_per_second <= 0:
            raise ValueError("rate_limit_per_second must be positive")
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.jitter = jitter
        self.rate_limit_per_second = rate_limit_per_second
        self.clock = clock or __import__("time").monotonic
        self.sleep = sleep or __import__("time").sleep
        self.session = session if session is not None else requests.Session()
        self._buckets = TokenBucketStore(clock=self.clock)
        self._bucket_scope = uuid.uuid4().hex

    def _wait_for_rate_limit(self, url: str) -> None:
        if self.rate_limit_per_second is None:
            return
        host = urlsplit(url).netloc.lower()
        while True:
            result = self._buckets.consume(
                f"{self._bucket_scope}:{host}",
                capacity=1.0,
                refill_per_sec=self.rate_limit_per_second,
            )
            if result.get("allowed"):
                return
            self.sleep(float(result.get("retry_after_sec", 0.1)))

    @staticmethod
    def _server_request_id(response: requests.Response | None) -> str:
        if response is None:
            return ""
        for header in _REQUEST_ID_RESPONSE_HEADERS:
            value = _header_value(response.headers, header)
            if value:
                return value
        return ""

    @staticmethod
    def _merge_headers(
        existing: Mapping[str, Any],
        replacement: Mapping[str, Any],
    ) -> dict[str, Any]:
        merged = dict(existing)
        replacement_names = {str(key).lower() for key in replacement}
        for key in list(merged):
            if str(key).lower() in replacement_names:
                del merged[key]
        merged.update(replacement)
        return merged

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        timeout_s: float | None = None,
        stream: str = "",
        refresh_auth: Callable[[], Mapping[str, str]] | None = None,
    ) -> HttpResult:
        request_headers = dict(headers or {})
        request_id = _header_value(request_headers, "X-Request-ID") or str(uuid.uuid4())
        if not _header_value(request_headers, "X-Request-ID"):
            request_headers["X-Request-ID"] = request_id
        safe_url = redact_url(url)
        logger.debug(
            "declarative HTTP request method=%s url=%s stream=%s request_id=%s headers=%s",
            method.upper(),
            safe_url,
            stream,
            request_id,
            _redact_headers(request_headers),
        )
        current_headers = request_headers
        refreshed = False
        latest_response: requests.Response | None = None
        latest_request_id = request_id

        while True:
            def perform_request() -> requests.Response:
                nonlocal latest_response, latest_request_id
                self._wait_for_rate_limit(url)
                request_kwargs = {
                    "headers": current_headers,
                    "params": params,
                    "json": json,
                    "data": data,
                    "timeout": timeout_s or self.timeout_s,
                }
                response = self.session.request(method.upper(), url, **request_kwargs)
                latest_response = response
                latest_request_id = self._server_request_id(response) or request_id
                status_code = getattr(response, "status_code", None)
                if isinstance(status_code, int) and status_code >= 400:
                    raise requests.HTTPError(
                        f"HTTP {status_code} {response.reason or 'Error'}",
                        response=response,
                    )
                return response

            try:
                response = with_retry(
                    perform_request,
                    budget=RetryBudget(
                        max_attempts=self.max_attempts,
                        base_delay_seconds=self.base_delay_seconds,
                        max_delay_seconds=self.max_delay_seconds,
                        jitter=self.jitter,
                    ),
                    sleep=self.sleep,
                )
                return HttpResult(
                    response=response,
                    request_id=self._server_request_id(response) or request_id,
                )
            except Exception as exc:
                response = getattr(exc, "response", None) or latest_response
                status = getattr(response, "status_code", None)
                if not isinstance(status, int) or isinstance(status, bool):
                    status = None
                request_id_for_error = (
                    self._server_request_id(response) or latest_request_id or request_id
                )
                if status == 401 and refresh_auth is not None and not refreshed:
                    refreshed = True
                    try:
                        current_headers = self._merge_headers(current_headers, refresh_auth())
                    except Exception:
                        raise ConnectorAuthError(
                            f"Authentication refresh failed for {safe_url}",
                            request_id=request_id_for_error,
                            stream=stream,
                            status=status,
                            url=safe_url,
                        ) from None
                    continue

                message = f"HTTP request failed for {safe_url}"
                common = {
                    "request_id": request_id_for_error,
                    "stream": stream,
                    "status": status,
                    "url": safe_url,
                }
                if status == 401 or status == 403 and not _github_rate_limited(response):
                    raise ConnectorAuthError(message, **common) from None
                if status == 429 or status == 403 and _github_rate_limited(response):
                    raise RateLimitExhausted(message, **common) from None
                if status == 408 or status is not None and 500 <= status <= 599:
                    raise TransientExhausted(message, **common) from None
                if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
                    raise TransientExhausted(message, **common) from None
                if status is not None:
                    raise ConnectorRequestError(message, **common) from None
                raise TransientExhausted(message, **common) from None

    def request_json(
        self,
        method: str,
        url: str,
        *,
        stream: str = "",
        refresh_auth: Callable[[], Mapping[str, str]] | None = None,
        **request_kwargs: Any,
    ) -> HttpResult:
        result = self.request(
            method,
            url,
            stream=stream,
            refresh_auth=refresh_auth,
            **request_kwargs,
        )
        try:
            payload = load_http_json(result.response)
        except (ValueError, TypeError, UnicodeError):
            raise ResponseShapeError(
                f"Invalid JSON response from {redact_url(url)}",
                request_id=result.request_id,
                stream=stream,
                status=result.response.status_code,
                url=redact_url(url),
            ) from None
        return HttpResult(result.response, result.request_id, payload)


def _github_rate_limited(response: requests.Response | None) -> bool:
    if response is None or response.status_code != 403:
        return False
    return _header_value(response.headers, "X-RateLimit-Remaining").strip() == "0"
