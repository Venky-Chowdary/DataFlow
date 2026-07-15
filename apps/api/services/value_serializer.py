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
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


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
    """
    if value.is_nan() or value.is_infinite():
        return None
    # Use fixed-point notation so values like 1E-13 are emitted as
    # "0.0000000000001" and trailing zeros are preserved.
    return format(value, "f")


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

    # bson.decimal128.Decimal128
    if cls_name == "Decimal128":
        return _json_default(value.to_decimal())

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
        if value.is_nan() or value.is_infinite():
            return ""
        # Fixed-point notation preserves scale and avoids scientific notation
        # in CSV/JSON/JSONL exports (e.g. 1E-13 becomes "0.0000000000001").
        return format(value, "f")

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

    # bson.decimal128.Decimal128
    if cls_name == "Decimal128":
        return cell_to_string(value.to_decimal())

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
        return sanitize_json_value(value.to_decimal())
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
