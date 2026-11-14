"""One sentence for a warning, whatever shape a check produced it in.

Checks emit warnings as plain strings or as dicts ({"id", "message", ...});
``str(dict)`` put a Python repr (``{'id': 'x', 'message': ...}``) on screen
(UX-34). The message is the sentence; a dict without one becomes
"key: value" clauses, internal ids and severities left out.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_TEXT_KEYS = ("message", "reason", "detail", "text", "note")
_HIDDEN_KEYS = frozenset({"id", "code", "severity", "level", "kind"})


def warning_text(warning: Any) -> str:
    if warning is None:
        return ""
    if isinstance(warning, str):
        return warning.strip()
    if isinstance(warning, Mapping):
        for key in _TEXT_KEYS:
            value = warning.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        parts = [
            f"{str(k).replace('_', ' ')}: {warning_text(v) if isinstance(v, (Mapping, list, tuple)) else v}"
            for k, v in warning.items()
            if str(k) not in _HIDDEN_KEYS and v not in (None, "", [], {})
        ]
        return "; ".join(parts)
    if isinstance(warning, (list, tuple, set)):
        return ", ".join(t for t in (warning_text(w) for w in warning) if t)
    return str(warning).strip()
