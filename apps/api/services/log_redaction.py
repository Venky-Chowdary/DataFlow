"""Canonical redaction for local and exported application logs."""

from __future__ import annotations

import re
from typing import Any

SECRET_KEY = re.compile(
    r"(?i)^(authorization|cookie|set-cookie|password|passwd|secret|token|"
    r"api[_-]?key|private[_-]?key|credential|payload|request[_-]?body|"
    r"response[_-]?body|raw[_-]?data|row[_-]?data)$"
)
_SECRET_TEXT = (
    re.compile(r"(?i)\b(bearer|basic)\s+[a-z0-9._~+/=-]+"),
    re.compile(
        r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization)"
        r"(\s*[:=]\s*)([^\s,;]+)"
    ),
    re.compile(r"(?i)(https?://[^:/\s]+:)[^@\s]+(@)"),
)


def redact_log_text(value: str) -> str:
    redacted = value
    redacted = _SECRET_TEXT[0].sub(lambda match: f"{match.group(1)} ***", redacted)
    redacted = _SECRET_TEXT[1].sub(
        lambda match: f"{match.group(1)}{match.group(2)}***",
        redacted,
    )
    return _SECRET_TEXT[2].sub(r"\1***\2", redacted)


def redact_log_value(key: str, value: Any) -> Any:
    if SECRET_KEY.match(key):
        return "***"
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
