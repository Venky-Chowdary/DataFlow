"""Two REST error responses that must not let a caller tell them apart (U2 / R11).

An existence-oracle test asks a foreign object and a missing one the same
question and demands the same answer. Since U2 every error body carries a
fresh per-response ``error.reference`` (and the correlation id that equals
it), so the raw bodies never compare equal. A random reference reveals
nothing about the object, so it is the one thing set aside; everything else
(status, detail, code, message, action, object) must be identical, and each
response must still carry a reference for support.
"""

from __future__ import annotations

from typing import Any

#: Per-response identifiers; random by design, never about the object.
_PER_RESPONSE_KEYS = ("reference", "correlation_id")


def _references(body: dict) -> set[str]:
    error = body.get("error") if isinstance(body.get("error"), dict) else {}
    found = {str(error.get("reference") or "")}
    found |= {str(body.get(key) or "") for key in _PER_RESPONSE_KEYS}
    return {value for value in found if value}


def _scrub(value: Any, references: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            key: _scrub(item, references)
            for key, item in value.items()
            if key not in _PER_RESPONSE_KEYS
        }
    if isinstance(value, list):
        return [_scrub(item, references) for item in value]
    if isinstance(value, str):
        # "... Reference: <id>" in a detail names this response, not the object.
        for reference in references:
            value = value.replace(reference, "<reference>")
    return value


def without_references(response: Any) -> dict:
    """The error body minus its per-response reference; asserts it had one."""
    body = response.json()
    assert isinstance(body, dict), body
    error = body.get("error")
    assert isinstance(error, dict) and error.get("reference"), (
        f"U2: every REST error carries error.reference: {body}"
    )
    return _scrub(body, _references(body))


def assert_indistinguishable(a: Any, b: Any) -> None:
    """Same status and the same body except each response's own reference."""
    assert a.status_code == b.status_code, (a.status_code, a.text, b.status_code, b.text)
    assert without_references(a) == without_references(b), (a.text, b.text)
