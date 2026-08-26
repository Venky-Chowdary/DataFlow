"""Array wire parsing shared by the writers.

Split out of ``writer_common.py`` (a god module over its size budget). An array
cell arrives as a JSON list, a Postgres ``{a,b,NULL}`` literal, or a bare scalar
that may legitimately be an engine-native payload; the parser answers only what
is unambiguous, so an ambiguous cell is never quarantined on a guess.
"""

from __future__ import annotations

import json
from typing import Any


def parse_array_wire_elements(value: Any) -> tuple[list[Any] | None, str | None]:
    """Parse array wire into elements for element-level fidelity checks.

    Returns ``(elements, error)``. ``(None, None)`` means *ambiguous* — a bare
    scalar that may legitimately be a SET joiner payload or engine-native
    literal. Ambiguity is never quarantined; only unambiguous breakage is,
    so this gate cannot produce false holdouts.
    """
    if value is None:
        return None, None
    if isinstance(value, (list, tuple)):
        return list(value), None
    if isinstance(value, dict):
        return None, "object/dict payload cannot populate an ARRAY column"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None, "binary payload cannot populate an ARRAY column"
    if not isinstance(value, str):
        return None, None

    text = value.strip()
    if not text:
        return None, None
    if text.startswith("[") and text.endswith("]"):
        try:
            from services.value_serializer import json_loads_exact

            # stdlib json.loads rounds every non-integer through binary64.
            # 0.1 and 1.234567890123456789 lost digits before element fit.
            parsed = json_loads_exact(text)
        except Exception:
            return None, "malformed JSON array payload"
        if not isinstance(parsed, list):
            return None, "JSON payload is not an array"
        return parsed, None
    if text.startswith("{") and text.endswith("}"):
        # Postgres array literal ``{a,b,NULL}`` (unquoted NULL is a real NULL;
        # quoted "NULL" is the literal string) — PG docs 8.15.
        try:
            parsed_obj = json.loads(text)
        except Exception:
            return _parse_pg_array_literal(text), None
        if isinstance(parsed_obj, dict):
            return None, "JSON object payload cannot populate an ARRAY column"
        return _parse_pg_array_literal(text), None
    return None, None


def _parse_pg_array_literal(text: str) -> list[Any]:
    """Split a Postgres ``{a,b,"c,d",NULL}`` literal into elements."""
    body = text[1:-1]
    if not body.strip():
        return []
    elements: list[Any] = []
    buf: list[str] = []
    in_quotes = False
    escaped = False
    for ch in body:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == "," and not in_quotes:
            elements.append(_pg_array_element(("".join(buf)).strip()))
            buf = []
            continue
        buf.append(ch)
    elements.append(_pg_array_element(("".join(buf)).strip()))
    return elements


def _pg_array_element(raw: str) -> Any:
    if raw.upper() == "NULL":
        return None
    if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw


def _is_numeric_wire(value: Any) -> bool:
    """True when the write path binds this cell as a finite number.

    ``Decimal(text)`` invented Auto ``1.234`` / ``1.000`` as numeric and
    missed ``$1,234`` / ``€1.234`` that INTEGER and DECIMAL bind store.
    Wordy ``true`` is not a decimal wire — ``coerce_integer_wire`` still
    refuses it — so this stays False for that token.
    """
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(value) is not None


def _is_temporal_wire(value: Any) -> bool:
    """True when the write path binds this cell as date, datetime, or time.

    ``time.fromisoformat("1704067200")`` invented ``17:04:06.720000``.
    Unambiguous ``31/12/2024`` / ``12/31/2024`` bind on the write path but
    ISO-only parsing refused them. Auto ``01/02/2024`` still refuses.
    """
    from datetime import date, datetime, time

    if isinstance(value, (datetime, date, time)):
        return True
    text = str(value).strip()
    if not text:
        return True
    from services.transform_engine import apply_transform

    for kind in ("date", "datetime", "time"):
        parsed, err = apply_transform(text, kind)
        if parsed is not None and not err:
            return True
    return False

