"""Which JSON numbers a destination's native JSON type stores exactly (D206).

docs/ZERO_LOSS_PROPERTIES.md, Property 8 ("JSON number spelling"): a JSON
document written to a *text* carrier keeps every token, but a native JSON type
re-encodes numbers:

* MySQL ``JSON`` - integers that fit int64/uint64 exactly, every other number
  as an IEEE double. ``1.10`` -> ``1.1`` is the same value (Gate-8 compares
  value-equal there); ``12345678901234567890.123`` -> ``1.2345678901234567e19``
  is not, and Gate-8 failed the run *after* the write.
* MongoDB (BSON) - the writer parses with ``json_loads_exact``: a number a
  double holds by value is a double; any other non-integer goes to
  ``Decimal128`` (34 significant digits, inexact refused) and past that the
  digits fall back to a *string*; an integer outside int64 cannot be encoded.
* Snowflake ``VARIANT`` / ``OBJECT`` / ``ARRAY`` - ``PARSE_JSON`` keeps a
  plain decimal of up to 38 digits as fixed-point; exponent notation and
  anything wider become a double.

MariaDB ``JSON`` is a ``LONGTEXT`` alias and PostgreSQL ``jsonb`` keeps
numeric digits, so neither has a limit here. The rule is "the value the engine
stores equals the value the source wrote" - the same equality Gate-8 applies
on these engines - so preflight refuses exactly what Gate-8 would fail.
"""

from __future__ import annotations

import json
import math
from decimal import Decimal, InvalidOperation
from typing import Any

_INT64 = (-(2**63), 2**63 - 1)
_UINT64_MAX = 2**64 - 1
_DECIMAL128_DIGITS = 34
_DECIMAL128_EXP = (-6176, 6111)
_SNOWFLAKE_DIGITS = 38
_SNOWFLAKE_SCALE = 37

_MYSQL_FAMILY = frozenset({"mysql", "tidb", "aurora_mysql", "aurora-mysql", "singlestore"})
_SNOWFLAKE_JSON = frozenset({"VARIANT", "OBJECT", "ARRAY"})


def json_number_capacity(engine: Any, ddl_type: Any) -> str | None:
    """``"double"`` / ``"bson"`` / ``"snowflake"``, or ``None`` when exact."""
    eng = str(engine or "").strip().lower()
    base = str(ddl_type or "").strip().upper().split("(", 1)[0].strip()
    if eng in _MYSQL_FAMILY and base == "JSON":
        return "double"
    if eng in {"mongodb", "mongo", "documentdb", "cosmosdb_mongo"}:
        return "bson"
    if eng == "snowflake" and base in _SNOWFLAKE_JSON:
        return "snowflake"
    return None


def _double_holds(token: str) -> bool:
    try:
        exact = Decimal(token)
        as_float = float(exact)
    except (InvalidOperation, ValueError, OverflowError):
        return False
    return math.isfinite(as_float) and Decimal(repr(as_float)) == exact


def _is_integer_token(token: str) -> bool:
    return not any(ch in token for ch in ".eE")


def _significant_digits(exact: Decimal) -> tuple[int, int]:
    """(precision, scale) of a finite decimal as written, leading zeros dropped."""
    _sign, digits, exp = exact.as_tuple()
    text = "".join(str(d) for d in digits).lstrip("0") or "0"
    scale = max(0, -int(exp))
    return max(len(text), scale), scale


def number_is_held(token: str, capacity: str) -> bool:
    """True when ``capacity`` stores the JSON number ``token`` as that value."""
    if capacity == "double":
        if _is_integer_token(token):
            value = int(token)
            if _INT64[0] <= value <= _UINT64_MAX:
                return True
        return _double_holds(token)
    if capacity == "bson":
        if _is_integer_token(token):
            return _INT64[0] <= int(token) <= _INT64[1]
        if _double_holds(token):
            return True
        try:
            exact = Decimal(token)
        except InvalidOperation:
            return False
        if not exact.is_finite():
            return False
        # Count digits by hand: ``normalize()`` rounds to the context's 28.
        _sign, digits, exp = exact.as_tuple()
        text = "".join(str(d) for d in digits)
        stripped = text.rstrip("0")
        exp = int(exp) + (len(text) - len(stripped))
        stripped = stripped.lstrip("0") or "0"
        return (
            len(stripped) <= _DECIMAL128_DIGITS
            and _DECIMAL128_EXP[0] <= exp <= _DECIMAL128_EXP[1]
        )
    if capacity == "snowflake":
        if "e" not in token.lower():
            try:
                precision, scale = _significant_digits(Decimal(token))
            except InvalidOperation:
                return False
            if precision <= _SNOWFLAKE_DIGITS and scale <= _SNOWFLAKE_SCALE:
                return True
        return _double_holds(token)
    return True


def _number_tokens(document: Any) -> list[str] | None:
    """Every number in a JSON document, as the source spelled it."""
    tokens: list[str] = []

    def _keep(raw: str) -> str:
        tokens.append(raw)
        return raw

    if isinstance(document, (bytes, bytearray, memoryview)):
        document = bytes(document).decode("utf-8", errors="replace")
    if isinstance(document, str):
        try:
            json.loads(document, parse_float=_keep, parse_int=_keep)
        except (ValueError, TypeError):
            return None
        return tokens

    def _walk(value: Any) -> None:
        if isinstance(value, bool) or value is None:
            return
        if isinstance(value, (int, Decimal)):
            tokens.append(str(value))
        elif isinstance(value, float):
            tokens.append(repr(value))
        elif isinstance(value, dict):
            for v in value.values():
                _walk(v)
        elif isinstance(value, (list, tuple)):
            for v in value:
                _walk(v)

    _walk(document)
    return tokens


def json_numbers_not_held(document: Any, *, engine: Any, ddl_type: Any) -> list[str]:
    """Number tokens in ``document`` the destination JSON type would change."""
    capacity = json_number_capacity(engine, ddl_type)
    if capacity is None or document is None:
        return []
    tokens = _number_tokens(document) or []
    return [t for t in tokens if not number_is_held(t, capacity)]


_CARRIER_HINT = {
    "double": ("MySQL JSON stores it as a double", "LONGTEXT"),
    "bson": ("BSON has no exact carrier for it", "a string field"),
    "snowflake": ("Snowflake VARIANT stores it as a double", "VARCHAR"),
}


def json_number_refusal(document: Any, *, engine: Any, ddl_type: Any, column: str = "") -> str:
    """The refusal text for a cell the destination JSON type would round; ``""`` if none.

    Kept short: the gate summary truncates near 250 characters, and the value
    and the remedy must survive that.
    """
    lost = json_numbers_not_held(document, engine=engine, ddl_type=ddl_type)
    if not lost:
        return ""
    capacity = json_number_capacity(engine, ddl_type) or ""
    why, carrier = _CARRIER_HINT.get(capacity, ("the JSON type re-encodes it", "text"))
    shown = ", ".join(v[:40] for v in lost[:2]) + (f" (+{len(lost) - 2})" if len(lost) > 2 else "")
    where = f"{column}: " if column else ""
    return (
        f"JSON_NUMBER_NOT_EXACT: {where}{shown} would be rounded ({why}). "
        f"Remedy: a text carrier ({carrier}) or a risk contract "
        f"(CAST_AND_CONTINUE + quarantine). Never stored rounded."
    )
