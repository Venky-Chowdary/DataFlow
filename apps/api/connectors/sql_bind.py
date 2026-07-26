"""Shared SQL bind normalization — Mongo/quarantine wire → driver-safe values.

MySQL, Postgres, and generic_sql writers must share one algorithm for:
- boolean string wire (``cell_to_string`` → ``"true"``/``"false"``)
- JSON empty / scalar / nested bind (MySQL 3140 class, JSONB dict bind)

Callers choose representation via ``bool_as_int`` / ``json_as_text``.
"""

from __future__ import annotations

import base64
import json
from typing import Any

_TRUE_TOKENS = frozenset({"true", "t", "yes", "y", "1", "on"})
_FALSE_TOKENS = frozenset({"false", "f", "no", "n", "0", "off"})


def coerce_boolean_wire(value: Any, *, as_int: bool = False) -> Any:
    """Normalize Mongo/CSV boolean wire. Unrecognized values pass through."""
    if value is None:
        return None
    if isinstance(value, bool):
        return (1 if value else 0) if as_int else value
    if isinstance(value, (int, float)) and value in (0, 1):
        bit = int(value)
        return bit if as_int else bool(bit)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _TRUE_TOKENS:
            return 1 if as_int else True
        if token in _FALSE_TOKENS:
            return 0 if as_int else False
    return value


def coerce_json_wire(value: Any, *, as_text: bool = True) -> Any:
    """Normalize JSON/JSONB bind. Empty → NULL; invalid scalars wrapped as JSON text."""
    if value is None:
        return None
    from services.value_serializer import json_default

    if isinstance(value, (dict, list, tuple)):
        if as_text:
            return json.dumps(
                value, ensure_ascii=False, separators=(",", ":"), default=json_default
            )
        return list(value) if isinstance(value, tuple) else value
    if isinstance(value, (bool, int, float)):
        return json.dumps(value, allow_nan=False) if as_text else value
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        # Lossless wrap so scalars still load into JSON columns.
        return json.dumps(text, ensure_ascii=False) if as_text else text
    if as_text:
        return text
    return parsed


def coerce_binary_wire(value: Any) -> Any:
    """Normalize BYTEA/BLOB wire (base64 string → bytes)."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except Exception:
            return value.encode("utf-8")
    return value


def normalize_sql_bind_value(
    value: Any,
    ddl_type: str,
    *,
    engine: str = "",
) -> Any:
    """Route value through shared boolean/JSON/binary bind for ``ddl_type``.

    Temporal coercion stays in ``sql_temporal.coerce_sql_temporal`` — callers
    should apply that first (or via writer-specific helpers).
    """
    if value is None:
        return None
    from connectors.sql_temporal import coerce_sql_temporal, sql_base_type

    temporal = coerce_sql_temporal(value, ddl_type)
    if temporal is not value:
        return temporal

    upper = sql_base_type(ddl_type)
    eng = (engine or "").strip().lower()
    if upper in {"BINARY", "BLOB", "LONGBLOB", "VARBINARY", "BYTEA"}:
        return coerce_binary_wire(value)
    if upper in {"JSON", "JSONB"}:
        # MySQL/MariaDB bind JSON as text; Postgres/SA prefer native structures.
        as_text = eng in {"mysql", "mariadb", ""}
        return coerce_json_wire(value, as_text=as_text)
    if upper in {"BOOLEAN", "BOOL"}:
        return coerce_boolean_wire(value, as_int=eng in {"mysql", "mariadb"})
    if upper == "TINYINT" and eng in {"mysql", "mariadb"}:
        # MySQL TINYINT(1) convention — same as BOOLEAN wire.
        return coerce_boolean_wire(value, as_int=True)
    return value
