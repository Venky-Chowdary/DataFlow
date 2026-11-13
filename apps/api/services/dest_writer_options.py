"""One validator for a destination's file-writer options (G7-F2 / G7-F3).

``formula_protection`` (``"off"`` | ``"prefix"``) and ``tsv_null`` (``"empty"`` |
``"\\N"``) ride a destination's ``extra`` (or sit top-level on an endpoint dict).
An unknown value is refused when the request is made — every entry point that
accepts a destination or its extras (transfer JSON and multipart runs, plans,
contracts, preflight, shape and fidelity probes) calls this — never later, when
the writer meets it at run time.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from services.csv_safety import formula_protection_mode
from services.export_dialect import tsv_null_mode

#: Option -> vocabulary check (raises ``ValueError`` naming the option).
WRITER_OPTION_CHECKS: dict[str, Callable[[object], str]] = {
    "formula_protection": formula_protection_mode,
    "tsv_null": tsv_null_mode,
}


def validate_dest_writer_options(*payloads: Any) -> None:
    """Raise ``ValueError`` for an unknown writer option in any payload.

    A payload is an endpoint dict (its own keys and its nested ``extra``) or an
    ``extra`` dict; anything else is ignored. Absent options are fine.
    """
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        nested = payload.get("extra")
        for scope in (payload, nested if isinstance(nested, dict) else {}):
            for key, check in WRITER_OPTION_CHECKS.items():
                value = scope.get(key)
                if value is not None:
                    check(value)


def checked_dest_payload(value: Any) -> Any:
    """Pydantic ``field_validator`` body: the payload unchanged, or a ``ValueError`` (422)."""
    validate_dest_writer_options(value)
    return value


def refuse_invalid_dest_writer_options(*payloads: Any) -> None:
    """Router helper for payloads parsed by hand (multipart forms): 422 on an unknown option."""
    try:
        validate_dest_writer_options(*payloads)
    except ValueError as exc:
        from fastapi import HTTPException

        raise HTTPException(status_code=422, detail=str(exc)) from exc
