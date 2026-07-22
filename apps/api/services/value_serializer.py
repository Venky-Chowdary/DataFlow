"""Canonical typed-value serialization for DataFlow.

All source readers, the string matrix builder, and file-export paths should
convert Python values into the intermediate string form through `cell_to_string`
so that databases and object stores do not lose bytes, datetime, Decimal, UUID,
ObjectId, or nested-structure fidelity.
"""

from __future__ import annotations

import base64
import json
import math
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, Overflow
from enum import Enum
from typing import Any

# Fixed-point expansion past type_system DECIMAL budgets is unsafe
# (memory / driver Overflow). Prefer short scientific form for Redis/JSON/CSV.
from services.type_system import decimal_needs_scientific_wire


def safe_decimal_text(value: Decimal) -> str | None:
    """Serialize a Decimal per type_system DECIMAL wire policy.

    Modest values → fixed-point (exact scale). Extreme exponents → scientific
    text (Informatica-class: preserve digits as text when platform DECIMAL
    cannot hold them). Never expand into multi-megabyte strings; never raise
    decimal.Overflow into the transfer loop.
    """
    if not isinstance(value, Decimal):
        try:
            value = Decimal(value)
        except (InvalidOperation, Overflow, ValueError, TypeError):
            return None
    try:
        if value.is_nan() or value.is_infinite():
            return None
        _sign, digits, exp = value.as_tuple()
        if not isinstance(exp, int):
            return str(value)
        if decimal_needs_scientific_wire(digit_count=len(digits), abs_exponent=abs(exp)):
            return format(value, "e")
        return format(value, "f")
    except (Overflow, InvalidOperation, ValueError, TypeError):
        try:
            return str(value)
        except Exception:
            return None


def _is_na(value: Any) -> bool:
    """Detect pandas/numpy missing-like values without importing pandas."""
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def _is_decimal(obj: Any) -> bool:
    return isinstance(obj, Decimal)


def _is_objectid(obj: Any) -> bool:
    return obj.__class__.__name__ == "ObjectId"


def _is_decimal128(obj: Any) -> bool:
    return obj.__class__.__name__ == "Decimal128"


def _is_binary(obj: Any) -> bool:
    return obj.__class__.__name__ == "Binary"


def _format_timedelta(value: timedelta) -> str:
    """Format a timedelta as a SQL-compatible [H]HH:MM:SS[.ffffff] string."""
    total_seconds = value.total_seconds()
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds % 60
    if seconds == int(seconds):
        return f"{sign}{hours:02d}:{minutes:02d}:{int(seconds):02d}"
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds:09.6f}".rstrip("0").rstrip(".")


def _decimal_to_json(value: Decimal) -> Any:
    """Convert a Decimal to a JSON-compatible value.

    JSON has no native Decimal type, so we emit the exact decimal text as a
    string. Converting to float would lose precision for values that are not
    exactly representable in binary64 (e.g. 0.1, 1.2345, large integers). A
    string preserves every digit and can be parsed back to an exact numeric
    value by any downstream consumer.

    Extreme exponents stay scientific — never expand into a multi-megabyte
    fixed-point string (that path raised decimal.Overflow mid-transfer).
    """
    return safe_decimal_text(value)


def _json_default(value: Any) -> Any:
    """Fallback for values that the stdlib json encoder does not understand.

    This function never returns a non-serializable value; it recursively
    resolves numpy/pandas scalars, boto3 Binary, bson ObjectId/Decimal128,
    UUID, bytes, datetime, Decimal, and containers.
    """
    if value is None:
        return None

    # Missing-like values (pd.NA, np.nan, etc.) where value != value.
    if _is_na(value):
        return None

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return _decimal_to_json(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _format_timedelta(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, uuid.UUID):
        return str(value)

    cls_name = value.__class__.__name__

    # bson ObjectId / PyObjectId
    if cls_name == "ObjectId":
        return str(value)

    # bson.decimal128.Decimal128 — to_decimal() can raise decimal.Overflow
    if cls_name == "Decimal128":
        try:
            return _json_default(value.to_decimal())
        except (Overflow, InvalidOperation, ValueError, TypeError):
            try:
                return str(value)
            except Exception:
                return None

    # boto3 DynamoDB Binary
    if cls_name == "Binary":
        return base64.b64encode(value.value).decode("ascii")

    # numpy / pandas scalars and arrays
    if hasattr(value, "ndim") and hasattr(value, "tolist"):
        if value.ndim == 0 and hasattr(value, "item") and callable(value.item):
            return _json_default(value.item())
        return _json_default(value.tolist())

    if cls_name in {"NAType", "NaTType"}:
        return None

    if isinstance(value, (set, tuple, frozenset)):
        return [_json_default(v) for v in value]

    # Last resort: never emit repr() artifacts such as "b'...'".
    return str(value)


def cell_to_string(value: Any) -> str:
    """Convert a typed Python value into a canonical intermediate string.

    * None, NaN-like, and missing values -> ""
    * bool -> "true" / "false" (lowercase)
    * bytes / bytearray / memoryview -> base64
    * datetime / date / time -> ISO 8601
    * timedelta -> SQL TIME interval string
    * Decimal -> normalized string, or "" for NaN/Infinity
    * UUID -> string
    * ObjectId -> string
    * Decimal128 -> string
    * dict / list / tuple / set / frozenset -> compact JSON
    * numpy / pandas scalars -> their scalar .item() representation
    * unknown -> str(value) (never repr)
    """
    if value is None:
        return ""

    # Missing-like values (pd.NA, np.nan, etc.) where value != value.
    if _is_na(value):
        return ""

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, Decimal):
        return safe_decimal_text(value) or ""

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, time):
        return value.isoformat()

    if isinstance(value, timedelta):
        return _format_timedelta(value)

    if isinstance(value, uuid.UUID):
        return str(value)

    cls_name = value.__class__.__name__

    # bson ObjectId
    if cls_name == "ObjectId":
        return str(value)

    # bson.decimal128.Decimal128 — never let Overflow abort a whole batch
    if cls_name == "Decimal128":
        try:
            return cell_to_string(value.to_decimal())
        except (Overflow, InvalidOperation, ValueError, TypeError):
            try:
                return str(value)
            except Exception:
                return ""

    # boto3 DynamoDB Binary
    if cls_name == "Binary":
        return base64.b64encode(value.value).decode("ascii")

    # numpy / pandas scalars and arrays (convert to native Python first)
    if hasattr(value, "ndim") and hasattr(value, "tolist"):
        if value.ndim == 0 and hasattr(value, "item") and callable(value.item):
            return cell_to_string(value.item())
        return cell_to_string(value.tolist())

    if cls_name in {"NAType", "NaTType"}:
        return ""

    if isinstance(value, Enum):
        return sanitize_json_value(value.value)

    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return json.dumps(
            sanitize_json_value(value),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=json_default,
            allow_nan=False,
        )

    # Fallback to str() — never repr().
    return str(value)


def sanitize_json_value(value: Any) -> Any:
    """Recursively convert a value into a JSON-serializable Python object.

    Unlike `_json_default`, this is a pre-processor: it returns values that
    `json.dumps` can serialize without needing a `default` callback. It replaces
    `NaN` / `Infinity` / missing values with `None`, converts `Decimal` to numbers
    (or strings when they overflow float), encodes bytes as base64, and normalizes
    datetime / UUID / ObjectId / Binary / numpy values. Strings are left as-is.
    """
    if value is None:
        return None
    if _is_na(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return _decimal_to_json(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _format_timedelta(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if _is_objectid(value):
        return str(value)
    if _is_decimal128(value):
        try:
            return sanitize_json_value(value.to_decimal())
        except (Overflow, InvalidOperation, ValueError, TypeError):
            try:
                return str(value)
            except Exception:
                return None
    if _is_binary(value):
        return base64.b64encode(value.value).decode("ascii")
    if isinstance(value, Enum):
        return sanitize_json_value(value.value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if hasattr(value, "ndim") and hasattr(value, "tolist"):
        if value.ndim == 0 and hasattr(value, "item") and callable(value.item):
            return sanitize_json_value(value.item())
        return sanitize_json_value(value.tolist())
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        return None
    if isinstance(value, (set, tuple, frozenset)):
        return [sanitize_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(v) for v in value]
    # Last resort: never emit repr() artifacts.
    return str(value)


def json_default(value: Any) -> Any:
    """Public JSON-default helper for json.dumps() callers."""
    return _json_default(value)
