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
        "serviceaccount",
        "serviceaccountjson",
        "serviceaccountkey",
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

_SAFE_SUFFIXES = frozenset(
    {
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
    }
)
_SAFE_SUFFIX_PAIRS = (("expires", "at"), ("url", "kind"))

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
    "passcode",
    "webhook",
)

_BEARER_OR_BASIC = re.compile(
    r"(?i)\b(bearer|basic)\s+[a-z0-9._~+/=-]+"
)
_JWT = re.compile(
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
)
_DATAFLOW_KEY = re.compile(r"\b(dfk|dwk)_[A-Za-z0-9_-]{16,}")
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
_VENDOR_TOKEN_PATTERNS = (
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>gh[pousr]_)[A-Za-z0-9]{36,}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>github_pat_)[A-Za-z0-9_]{50,}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>(?:sk|rk)_(?:live|test)_)[A-Za-z0-9]{16,}"
        r"(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>whsec_)[A-Za-z0-9]{24,}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>xox[abprs]-)[A-Za-z0-9-]{10,}"
        r"(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>AKIA|ASIA)[A-Z0-9]{16}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?P<prefix>AIza)[0-9A-Za-z_-]{35}(?![A-Za-z0-9_])"
    ),
)
_SENSITIVE_QUERY = re.compile(
    r"""(?i)([?&](?:code|session_state|key|apikey|api_key|access_token)=)[^&\s#"']+"""
)
_KEY_VALUE_ASSIGNMENT = re.compile(
    r"""(?P<key_open>["']?)(?P<key>[A-Za-z0-9_.-]+)(?P<key_close>["']?)"""
    r"""(?P<separator>\s*[:=]\s*)(?P<value_open>["']?)"""
    r"""(?P<value>[^\s,;&"'})\]]+)"""
)


def _key_tokens(key: object) -> tuple[str, ...]:
    value = str(key)
    value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value)
    value = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", value)
    return tuple(token.lower() for token in re.split(r"[_\s.-]+", value) if token)


def _has_safe_suffix(tokens: tuple[str, ...]) -> bool:
    if len(tokens) >= 2 and tokens[-2:] in _SAFE_SUFFIX_PAIRS:
        return True
    if not tokens or tokens[-1] not in _SAFE_SUFFIXES:
        return False
    if tokens[-1] == "ms" and len(tokens) > 1:
        previous = tokens[-2]
        if previous in _SENSITIVE_EXACT or any(stem in previous for stem in _SENSITIVE_STEMS):
            return False
    return True


def is_sensitive_key(key: object) -> bool:
    """Return whether a normalized field name can carry sensitive material."""
    try:
        normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
        tokens = _key_tokens(key)
    except Exception:
        return True
    if normalized in _SENSITIVE_EXACT:
        return True
    if _has_safe_suffix(tokens):
        return False
    return any(stem in normalized for stem in _SENSITIVE_STEMS) or any(
        token in {"pin", "otp"} for token in tokens
    )


def _numeric_is_secret(key: object) -> bool:
    try:
        normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
        tokens = _key_tokens(key)
    except Exception:
        return True
    if normalized in _SENSITIVE_EXACT:
        return True
    if any(stem in normalized for stem in ("password", "passwd", "pwd", "secret", "passcode")):
        return True
    return any(token in {"pin", "otp"} for token in tokens)


def is_redactable_value(value: Any, key: object | None = None) -> bool:
    """Whether this value should be replaced under the supplied field key."""
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return key is not None and is_sensitive_key(key) and _numeric_is_secret(key)
    return True


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
    numeric_value = re.fullmatch(r"\d+(?:\.\d+)?", match.group("value"))
    if match.group("value").lower() in {"true", "false", "yes", "no", "none", "null"}:
        return match.group(0)
    if numeric_value and not _numeric_is_secret(match.group("key")):
        return match.group(0)
    return prefix + REDACTED


def redact_secret_shapes(value: str) -> str:
    """Redact recognized secret shapes without interpreting arbitrary assignments."""
    try:
        redacted = value if isinstance(value, str) else str(value)
        redacted = _BEARER_OR_BASIC.sub(r"\1 ***", redacted)
        redacted = _JWT.sub(REDACTED, redacted)
        redacted = _DATAFLOW_KEY.sub(lambda m: m.group(1) + "_" + REDACTED, redacted)
        for pattern in _WEBHOOK_URLS:
            redacted = pattern.sub(r"\1/***", redacted)
        redacted = _URI_USERINFO.sub(r"\1***\2", redacted)
        redacted = _SENSITIVE_QUERY.sub(r"\1***", redacted)
        for pattern in _VENDOR_TOKEN_PATTERNS:
            redacted = pattern.sub(lambda match: match.group("prefix") + REDACTED, redacted)
        return redacted
    except Exception:
        return REDACTED


def redact_log_text(value: str) -> str:
    """Redact common credentials from a free-form log line without raising."""
    try:
        return _KEY_VALUE_ASSIGNMENT.sub(_redact_assignment, redact_secret_shapes(value))
    except Exception:
        return REDACTED


def redact_log_value(key: object, value: Any) -> Any:
    """Redact a structured value recursively using its field name as context."""
    if is_sensitive_key(key) and is_redactable_value(value, key):
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
