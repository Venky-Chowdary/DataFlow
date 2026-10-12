"""Canonical redaction for local and exported application logs."""

from __future__ import annotations

import re
from typing import Any

REDACTED = "***"

_SENSITIVE_EXACT = frozenset(
    {
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "apikey",
        "xapikey",
        "privatekey",
        "credential",
        "credentials",
        "connectionstring",
        "connstr",
        "dsn",
        "accountkey",
        "sig",
        "signature",
        "xamzsignature",
        "xamzsecuritytoken",
        "samlresponse",
        "codeverifier",
        "clientassertion",
        "payload",
        "requestbody",
        "responsebody",
        "rawdata",
        "rowdata",
    }
)

_SAFE_SUFFIXES = (
    "count",
    "id",
    "ids",
    "name",
    "names",
    "type",
    "ttl",
    "prefix",
    "length",
    "len",
    "ms",
    "seconds",
    "version",
    "expiresat",
    "expiry",
    "hint",
    "configured",
    "present",
    "enabled",
    "required",
    "scope",
    "scopes",
    "mode",
    "urlkind",
)

_SENSITIVE_STEMS = (
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "privatekey",
    "accesskey",
    "accountkey",
    "credential",
    "cookie",
    "authorization",
    "signature",
    "connectionstring",
    "samlresponse",
    "codeverifier",
    "clientassertion",
    "webhook",
)

_BEARER_OR_BASIC = re.compile(
    r"(?i)\b(bearer|basic)\s+[a-z0-9._~+/=-]+"
)
_JWT = re.compile(
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)
_DATAFLOW_KEY = re.compile(r"\bdfk_[A-Za-z0-9_-]{16,}")
_WEBHOOK_URLS = (
    re.compile(
        r"""(?i)(https://hooks\.slack\.com)/(?:services|workflows|triggers)/[^\s"'<>]+"""
    ),
    re.compile(
        r"""(?i)(https://[a-z0-9.-]+\.webhook\.office\.com)/[^\s"'<>]+"""
    ),
    re.compile(
        r"""(?i)(https://(?:www\.|ptb\.|canary\.)?discord(?:app)?\.com)/api/webhooks/[^\s"'<>]+"""
    ),
)
_URI_USERINFO = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://[^:/\s@]*:)[^@\s/]+(@)"
)
_SENSITIVE_QUERY = re.compile(r"(?i)([?&](?:code|session_state)=)[^&\s#\"']+")
_KEY_VALUE_ASSIGNMENT = re.compile(
    r"""(?P<key_open>["']?)(?P<key>[A-Za-z0-9_.-]+)(?P<key_close>["']?)"""
    r"""(?P<separator>\s*[:=]\s*)(?P<value_open>["']?)"""
    r"""(?P<value>[^\s,;&"'}]+)"""
)


def is_sensitive_key(key: object) -> bool:
    """Return whether a normalized field name can carry sensitive material."""
    try:
        normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    except Exception:
        return True
    if normalized in _SENSITIVE_EXACT:
        return True
    if any(normalized.endswith(suffix) for suffix in _SAFE_SUFFIXES):
        return False
    return any(stem in normalized for stem in _SENSITIVE_STEMS)


def _redacts_value(value: Any) -> bool:
    """Scalars retain their diagnostic value; compound or textual values do not."""
    return not (
        value is None
        or isinstance(value, (bool, int, float))
    )


def _redact_assignment(match: re.Match[str]) -> str:
    prefix = (
        match.group("key_open")
        + match.group("key")
        + match.group("key_close")
        + match.group("separator")
        + match.group("value_open")
    )
    if not is_sensitive_key(match.group("key")):
        nested = _KEY_VALUE_ASSIGNMENT.sub(_redact_assignment, match.group("value"))
        return prefix + nested
    if (
        re.sub(r"[^a-z0-9]", "", match.group("key").lower())
        in {"authorization", "proxyauthorization"}
        and match.group("value").lower() in {"bearer", "basic"}
    ):
        return match.group(0)
    return (
        prefix
        + REDACTED
    )


def redact_log_text(value: str) -> str:
    """Redact common credentials from a free-form log line without raising."""
    try:
        redacted = value if isinstance(value, str) else str(value)
        redacted = _BEARER_OR_BASIC.sub(r"\1 ***", redacted)
        redacted = _JWT.sub(REDACTED, redacted)
        redacted = _DATAFLOW_KEY.sub("dfk_" + REDACTED, redacted)
        for pattern in _WEBHOOK_URLS:
            redacted = pattern.sub(r"\1/***", redacted)
        redacted = _URI_USERINFO.sub(r"\1***\2", redacted)
        redacted = _SENSITIVE_QUERY.sub(r"\1***", redacted)
        return _KEY_VALUE_ASSIGNMENT.sub(_redact_assignment, redacted)
    except Exception:
        return REDACTED


def redact_log_value(key: object, value: Any) -> Any:
    """Redact a structured value recursively using its field name as context."""
    if is_sensitive_key(key) and _redacts_value(value):
        return REDACTED
    if isinstance(value, str):
        return redact_log_text(value)
    if isinstance(value, dict):
        return {
            str(nested_key): redact_log_value(str(nested_key), nested_value)
            for nested_key, nested_value in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_log_value("", item) for item in value]
    return value
