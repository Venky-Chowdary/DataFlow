"""Shared helpers for SaaS source connectors (Salesforce, HubSpot, Stripe, etc.)."""

from __future__ import annotations

import re
from typing import Any, NoReturn

import requests

from connectors.base import ReadBatch
from services.error_handling import RetryBudget, with_retry
from services.value_serializer import cell_to_string


def base_url(host: str, default: str) -> str:
    host = (host or default).strip()
    if not host:
        host = default
    if "://" not in host:
        host = f"https://{host}"
    return host.rstrip("/")


def token(
    api_key: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
) -> str:
    """Extract a bearer token from the first non-empty credential field."""
    for value in (api_key, connection_string):
        v = (value or "").strip()
        if v:
            return v
    if username and password:
        return f"{username.strip()}:{password.strip()}"
    return ""


def request(
    *,
    method: str,
    url: str,
    token: str = "",
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    timeout: float = 30.0,
    retry_budget: RetryBudget | None = None,
) -> requests.Response:
    """Make an HTTP request with retriable transient handling (429 / 5xx / timeouts)."""
    h = dict(headers or {})
    if token:
        h.setdefault("Authorization", f"Bearer {token}")
    h.setdefault("Accept", "application/json")
    h.setdefault("User-Agent", "DataFlow/1.0")

    def _call() -> requests.Response:
        resp = requests.request(
            method=method,
            url=url,
            headers=h,
            params=params,
            json=data,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp

    return with_retry(_call, budget=retry_budget or RetryBudget())


def humanize_http_error(exc: Exception, driver: str) -> str:
    text = str(exc).lower()
    if "401" in text or "unauthorized" in text:
        return f"{driver.title()} authentication failed. Check your API token/key and that it is active."
    if "403" in text or "forbidden" in text:
        return f"{driver.title()} permission denied. The token does not have the required scopes/permissions for this object."
    if "404" in text or "not found" in text:
        return f"{driver.title()} resource not found. Check the object/table name and API endpoint."
    if "429" in text or "rate limit" in text:
        return f"{driver.title()} rate limit hit. Please wait and try again."
    if "timeout" in text or "timed out" in text:
        return "Connection timed out. Check the host/URL and network."
    if "connection" in text and "refused" in text:
        return "Could not reach the API host. Check the URL and network."
    if re.search(r"no module named|cannot import", text):
        return "The requests library is not installed in this environment."
    return f"{driver.title()} API error: {exc}"


def extract_records(records: list[dict[str, Any]]) -> ReadBatch:
    if not records:
        return ReadBatch(headers=[], rows=[], offset=0, total_rows=0)
    headers = list(records[0].keys())
    rows = [[cell_to_string(r.get(h, "")) for h in headers] for r in records]
    return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=len(rows))


def object_name(cfg: dict[str, Any], default: str) -> str:
    return (
        (cfg.get("table") or "").strip()
        or (cfg.get("database") or "").strip()
        or default
    )


def write_not_supported(**_kwargs: Any) -> NoReturn:
    """Placeholder writer for source-only SaaS connectors."""
    raise RuntimeError("This SaaS connector is currently source-only.")


# Keep an alias under the canonical writer name so the registry stays consistent.
write_mapped_rows = write_not_supported
