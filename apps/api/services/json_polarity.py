"""JSON scalar polarity is a type, not a pretty-print.

RFC 8259 numbers have no precision bound; JavaScript ``Number`` is IEEE-754
binary64. Airbyte and many Python pipelines ``json.loads`` a cell and
``str()`` it back, so the JSON string ``"1"`` and the JSON number ``1``
collapse, ``true`` and the string ``"true"`` collapse, and integers past
``2**53`` round off through float. Checksums of the *text* can still match
after a re-parse. That is silent type change, not transfer.

The algorithm:

1. JSON/JSONB columns travel as **engine JSON text** (``col::text`` /
   ``CAST(col AS CHAR)``), never as a deserialized Python tree. SQL NULL
   stays SQL NULL; JSON ``null`` stays the text ``null``.
2. Classify each scalar into a polarity class (number / string / boolean /
   null / array / object). MySQL ``INTEGER``/``DOUBLE``/``DECIMAL`` are the
   same class as PostgreSQL ``number``.
3. Bind the text as JSON. Never ``json.loads`` a VARCHAR-looking cell that
   was a JSON string scalar — that is how ``"1"`` becomes ``1``.
4. Certify from the destination engine (``jsonb_typeof`` / ``JSON_TYPE``),
   not from Python's type after a second parse.

Integers beyond the IEEE-754 mantissa (``2**53``) stay JSON numbers with
their digits; we do not stringify them to "save" JavaScript.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

JsonKind = Literal["number", "string", "boolean", "null", "array", "object"]

# MySQL JSON_TYPE splits numbers (INTEGER / UNSIGNED INTEGER / DOUBLE /
# DECIMAL); PostgreSQL jsonb_typeof does not.
_NUMBER_SPELLINGS: frozenset[str] = frozenset(
    {"number", "integer", "unsigned integer", "double", "decimal"}
)

IEEE754_SAFE_INT: int = 2**53


@dataclass(frozen=True)
class JsonPolarity:
    """What a JSON value *is*, independent of how a driver decoded it."""

    kind: JsonKind
    integer: bool = False
    beyond_ieee: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "integer": self.integer,
            "beyond_ieee": self.beyond_ieee,
        }


def is_json_catalog_type(data_type: str, udt_name: str = "") -> bool:
    """True for a JSON/JSONB column in information_schema (not json[])."""
    tokens = {
        (data_type or "").strip().lower(),
        (udt_name or "").strip().lower(),
    }
    return bool(tokens & {"json", "jsonb"})


def normalize_json_kind(kind: str) -> str:
    """Map dest-engine type names onto the portable polarity class."""
    token = " ".join((kind or "").split()).lower()
    if token in _NUMBER_SPELLINGS:
        return "number"
    if token in {"string", "boolean", "null", "array", "object"}:
        return token
    return token


def polarities_match(source_kind: str, dest_kind: str) -> bool:
    """True when dest JSON_TYPE / jsonb_typeof preserves source polarity."""
    return normalize_json_kind(source_kind) == normalize_json_kind(dest_kind)


def classify_json_value(value: Any) -> JsonPolarity:
    """Polarity of a Python value as it would appear in JSON."""
    if value is None:
        return JsonPolarity(kind="null")
    if isinstance(value, bool):
        return JsonPolarity(kind="boolean")
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        as_int = isinstance(value, int) or (
            isinstance(value, Decimal) and value == value.to_integral_value()
        )
        magnitude = int(value) if as_int else 0
        return JsonPolarity(
            kind="number",
            integer=bool(as_int),
            beyond_ieee=bool(as_int and abs(magnitude) > IEEE754_SAFE_INT),
        )
    if isinstance(value, str):
        return JsonPolarity(kind="string")
    if isinstance(value, (list, tuple)):
        return JsonPolarity(kind="array")
    if isinstance(value, dict):
        return JsonPolarity(kind="object")
    return JsonPolarity(kind="string")


def classify_json_text(text: str) -> JsonPolarity:
    """Polarity of canonical JSON text (engine ``::text`` / CAST AS CHAR)."""
    from services.value_serializer import json_loads_exact

    parsed = json_loads_exact((text or "").strip())
    return classify_json_value(parsed)


def json_object_path_kind(document: str, path: str) -> JsonPolarity:
    """Polarity of one object member, from canonical JSON text.

    ``path`` is a dotted JSON key (no arrays) — the certify helper uses the
    same keys the live fixture plants.
    """
    from services.value_serializer import json_loads_exact

    parsed = json_loads_exact(document)
    if not isinstance(parsed, dict):
        raise ValueError("json_object_path_kind requires a JSON object")
    if path not in parsed:
        raise KeyError(path)
    return classify_json_value(parsed[path])


def json_document_wire(value: Any) -> str:
    """Canonical JSON text from a driver-native cell.

    Prefer engine ``::text`` so this is only the fallback when a driver has
    already decoded the tree. A Python ``str`` that is valid JSON is treated
    as already-canonical engine text; any other ``str`` is a JSON string
    scalar and is quoted.
    """
    from services.value_serializer import (
        SQL_NULL_SENTINEL,
        json_document_text,
        json_loads_exact,
    )

    if value is None:
        return SQL_NULL_SENTINEL
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(
                "empty string cannot coerce to JSON — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        try:
            json_loads_exact(text)
        except (json.JSONDecodeError, ValueError, TypeError):
            return json.dumps(value, ensure_ascii=False)
        return text
    return json_document_text(value)
