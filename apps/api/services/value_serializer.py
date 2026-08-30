"""Canonical typed-value serialization for Datawrap.

All source readers, the string matrix builder, and file-export paths should
convert Python values into the intermediate string form through `cell_to_string`
so that databases and object stores do not lose bytes, datetime, Decimal, UUID,
ObjectId, or nested-structure fidelity.
"""

from __future__ import annotations

import base64
import json
import math
import re
import uuid
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, Overflow
from enum import Enum
from typing import Any

# Fixed-point expansion past type_system DECIMAL budgets is unsafe
# (memory / driver Overflow). Prefer short scientific form for Redis/JSON/CSV.
from services.type_system import decimal_needs_scientific_wire

# Explicit SQL NULL on the transfer string wire — distinct from empty string "".
# SQL readers use ``cell_to_string(..., preserve_sql_null=True)``; apply_transform
# maps the sentinel back to Python None → destination SQL NULL.
SQL_NULL_SENTINEL = "__DF_SQL_NULL__"

# Document field absent (Mongo/Dynamo schemaless) — distinct from explicit null.
# Sparse CDC upsert: omit the key from SET (never wipe destination with NULL).
# Dense INSERT/COPY/full-refresh / coerce_null: materialize as SQL NULL.
#
# In-memory cells use the ``Missing`` singleton (never a customer-visible string).
# ``DF_MISSING_SENTINEL`` remains the quarantine/wire spelling for durable JSON;
# ``is_missing_sentinel`` accepts both. Mapped-row public APIs must never return
# the bare string (audit §2.4).
class _MissingType:
    __slots__ = ()

    def __repr__(self) -> str:
        return "Missing"

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return other is self or other == "__DF_MISSING__"

    def __hash__(self) -> int:
        return hash("__DF_MISSING__")


Missing = _MissingType()
DF_MISSING_SENTINEL = "__DF_MISSING__"


def is_missing_sentinel(value: Any) -> bool:
    return value is Missing or value == DF_MISSING_SENTINEL


# Wire spellings that mean "no value here" — a DuckDB reader adds its own.
NULL_WIRE_SENTINELS: frozenset[str] = frozenset(
    {SQL_NULL_SENTINEL, DF_MISSING_SENTINEL, "__df_ddb_null__"}
)


def is_null_evidence(value: Any) -> bool:
    """True when a cell carries no type evidence (NULL / absent / blank)."""
    if value is None or value is Missing:
        return True
    text = str(value).strip()
    return not text or text in NULL_WIRE_SENTINELS


def is_reader_null_cell(value: Any) -> bool:
    """Reader-wired SQL NULL / Missing — not empty or whitespace text.

    Extract emits ``SQL_NULL_SENTINEL`` for a database NULL. Empty ``""`` is
    a present-but-unfit specialty payload (not WKT, not an interval), so
    GEOGRAPHY / INTERVAL bind must not treat it as SQL NULL.
    """
    if value is None or is_missing_sentinel(value):
        return True
    if not isinstance(value, str):
        return False
    return value.strip() in NULL_WIRE_SENTINELS


def absent_sql_bind(value: Any) -> tuple[bool, Any]:
    """Return ``(True, bind)`` when the cell is absence.

    Missing stays Missing (sparse omit). Reader-wired SQL NULL / None /
    DuckDB null bind as SQL NULL. Empty string is not absence — specialty
    and temporal coerces refuse it instead of inventing NULL.
    """
    if is_missing_sentinel(value):
        return True, value
    if is_reader_null_cell(value):
        return True, None
    return False, value


def present_cell_text(value: Any) -> str | None:
    """One present cell on the reader wire.

    SQL NULL / Missing / blank are not a unique key, FK, or collision token.
    Typed cells use ``cell_to_string`` so ``True`` and dest ``"true"`` match.
    """
    if is_null_evidence(value):
        return None
    if isinstance(value, str):
        text = value.strip()
        return None if is_null_evidence(text) else text
    text = cell_to_string(value, preserve_sql_null=True)
    if is_null_evidence(text):
        return None
    return text


def evidence_samples(values: Any, *, limit: int | None = None) -> list[str]:
    """Sample values usable as type evidence.

    A NULL is the *absence* of evidence, never evidence of text. Feeding the
    wire sentinel to inference made an all-NULL ``DECIMAL(7,3)`` column look
    like non-numeric strings, so Map invented a lossy ``<col>_text`` LONGTEXT
    destination for a column whose declared type was perfectly representable.
    """
    out = [str(v).strip() for v in (values or []) if not is_null_evidence(v)]
    return out[:limit] if limit else out


def public_mapped_cell(value: Any, *, dense_null: bool = False) -> Any:
    """Normalize a mapped cell for public / writer consumption.

    ``dense_null=True`` (INSERT / coerce_null): Missing → None.
    Otherwise keep ``Missing`` singleton (omit-from-SET); never the wire string.
    """
    if value is Missing:
        return None if dense_null else Missing
    if value == DF_MISSING_SENTINEL:
        return None if dense_null else Missing
    return value


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


def _bq_day_second_wire(
    days: int,
    hours: int,
    minutes: int,
    seconds: Decimal,
    *,
    sign: str = "",
) -> str:
    """Normalize DS parts and emit BigQuery ``0-0 D H:M:S[.F]``.

    ISO ``PT…S`` seconds stay Decimal identity. ``float(seconds)`` +
    ``timedelta`` overflowed ``2**53+1`` and rounded long fractions.
    """
    extra_min, seconds = divmod(seconds, Decimal(60))
    minutes += int(extra_min)
    extra_hr, minutes = divmod(minutes, 60)
    hours += extra_hr
    extra_d, hours = divmod(hours, 24)
    days += extra_d
    if seconds == seconds.to_integral_value():
        sec_s = f"{int(seconds):02d}"
    else:
        # Fixed-point identity — never scientific, never IEEE :09.6f.
        text = format(seconds, "f")
        whole, _, frac = text.partition(".")
        frac = frac.rstrip("0")
        sec_s = f"{int(whole):02d}.{frac}" if frac else f"{int(whole):02d}"
    return f"{sign}0-0 {days} {hours}:{minutes:02d}:{sec_s}"


def format_bigquery_interval(value: Any) -> str:
    """Canonical BigQuery INTERVAL wire: ``Y-M D H:M:S[.F]`` (day-to-second).

    Accepts timedelta, ISO-8601 duration (``P…T…``), HH:MM:SS, or already
    canonical BQ form. Unparseable input is returned stripped for quarantine.
    """
    if value is None:
        return ""
    if isinstance(value, timedelta):
        # Python timedelta is IEEE via total_seconds — not claimed exact.
        total = value.total_seconds()
        sign = "-" if total < 0 else ""
        total = abs(total)
        days = int(total // 86400)
        rem = total - days * 86400
        hours = int(rem // 3600)
        rem -= hours * 3600
        minutes = int(rem // 60)
        seconds = rem - minutes * 60
        if seconds == int(seconds):
            sec_s = f"{int(seconds):02d}"
        else:
            sec_s = f"{seconds:09.6f}".rstrip("0").rstrip(".")
            if "." in sec_s:
                whole, frac = sec_s.split(".", 1)
                sec_s = f"{int(whole):02d}.{frac}"
            else:
                sec_s = f"{int(sec_s):02d}"
        return f"{sign}0-0 {days} {hours}:{minutes:02d}:{sec_s}"

    text = str(value).strip()
    if not text:
        return ""
    if re.match(r"^-?\d+-\d+\s+-?\d+\s+-?\d+:\d{2}:\d{2}", text):
        return text
    m = re.fullmatch(
        r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$",
        text,
        re.I,
    )
    if m:
        days = int(m.group("days") or 0)
        hours = int(m.group("hours") or 0)
        minutes = int(m.group("minutes") or 0)
        sec_raw = m.group("seconds")
        if sec_raw is None:
            seconds = Decimal(0)
        else:
            try:
                seconds = Decimal(sec_raw)
            except (InvalidOperation, Overflow):
                return text
            if not seconds.is_finite() or seconds < 0:
                return text
        # ISO uses ``.`` as the decimal separator (PT1.234S is 1.234s,
        # not Auto grouping). Do not route through decimal_wire_value.
        return _bq_day_second_wire(days, hours, minutes, seconds)
    m2 = re.fullmatch(r"^(-)?(\d+):(\d{2}):(\d{2}(?:\.\d+)?)$", text)
    if m2:
        sign, h, mi, s = m2.groups()
        return f"{sign or ''}0-0 0 {int(h)}:{mi}:{s}"
    return text


def _decimal_to_json(value: Decimal) -> Any:
    """Convert a Decimal to a JSON-compatible value.

    JSON has no native Decimal type, so we emit the exact decimal text as a
    string. Converting to float would lose precision for values that are not
    exactly representable in binary64 (e.g. 0.1, 1.2345, large integers). A
    string preserves every digit and can be parsed back to an exact numeric
    value by any downstream consumer.

    Extreme exponents stay scientific — never expand into a multi-megabyte
    fixed-point string (that path raised decimal.Overflow mid-transfer).

    Exported *files* are the exception: a reader expects a numeric column to
    be a JSON number, so exporters serialize through
    ``json_dumps_exact_numbers``, which writes the same exact digits as an
    unquoted literal.
    """
    return safe_decimal_text(value)


def _demote_exactly_representable(value: Any) -> Any:
    """Return floats where binary64 is exact, Decimals where it is not."""
    if isinstance(value, Decimal):
        try:
            as_float = float(value)
        except (OverflowError, ValueError, InvalidOperation):
            return value
        if math.isfinite(as_float) and Decimal(repr(as_float)) == value:
            return as_float
        return value
    if isinstance(value, list):
        return [_demote_exactly_representable(v) for v in value]
    if isinstance(value, dict):
        return {k: _demote_exactly_representable(v) for k, v in value.items()}
    return value


def json_loads_exact(text: str, *, parse_constant: Any = None) -> Any:
    """``json.loads`` that does not round numbers off through binary64.

    The stdlib parses every non-integer JSON number into a float, so
    ``12345678901234567890.123456789`` comes back as ``1.2345678901234567e+19``
    — digits gone, silently, on a value the source stated exactly. Numbers that
    binary64 *can* hold exactly are still returned as floats so JSON output
    keeps its usual shape; only the ones that would lose digits stay ``Decimal``,
    which ``_decimal_to_json`` then writes as exact text per this module's
    documented policy.
    """
    parsed = json.loads(text, parse_float=Decimal, parse_constant=parse_constant)
    return _demote_exactly_representable(parsed)


def load_http_json(resp: Any) -> Any:
    """HTTP JSON body. Numbers match ``json_loads_exact``.

    ``Response.json()`` is stdlib ``json.loads``, so a long fraction in an
    API cell collapses to IEEE before flatten/bind. Invalid bodies raise.
    Test doubles that only stub ``.json()`` keep their already-built tree.
    """
    text = getattr(resp, "text", None)
    if isinstance(text, (bytes, bytearray, memoryview)):
        text = bytes(text).decode("utf-8")
    if isinstance(text, str) and text.strip():
        return json_loads_exact(text)
    content = getattr(resp, "content", None)
    if isinstance(content, (bytes, bytearray, memoryview)):
        raw = bytes(content).decode("utf-8")
        if raw.strip():
            return json_loads_exact(raw)
    json_fn = getattr(resp, "json", None)
    if callable(json_fn):
        payload = json_fn()
        if isinstance(payload, str):
            return json_loads_exact(payload) if payload.strip() else {}
        return payload if payload is not None else {}
    return {}


def demote_exact_json(value: Any) -> Any:
    """Same IEEE-exact number demotion ``json_loads_exact`` applies.

    ijson default ``use_float=False`` yields ``Decimal`` for every fraction,
    including ``1.5``. Streaming ingest must share this demote so DOM and
    StAX never disagree on a leaf the write path already binds as float.
    """
    return _demote_exactly_representable(value)


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


def project_row_cells(
    row: Mapping[str, Any], headers: list[str], *, preserve_sql_null: bool = False
) -> list[str]:
    """Project a record onto ``headers`` — absent key ≠ empty string.

    Schemaless and sparse sources (Mongo documents, DynamoDB items, NDJSON,
    API payloads) omit keys entirely. Defaulting those to ``""`` made Validate
    report ``Empty value cannot coerce to decimal`` for a field the document
    simply does not carry, blocking a transfer whose write path would have
    omitted the key (sparse upsert) or written SQL NULL (dense insert). The
    missing sentinel keeps that distinction all the way to the writer.
    """
    out: list[str] = []
    for h in headers:
        if h not in row:
            out.append(DF_MISSING_SENTINEL)
            continue
        cell = row[h]
        # ``cell_to_string`` flattens the sentinel to "" so exports never leak
        # it; a reader that already marked the field absent must keep it here.
        if is_missing_sentinel(cell):
            out.append(DF_MISSING_SENTINEL)
            continue
        out.append(cell_to_string(cell, preserve_sql_null=preserve_sql_null))
    return out


def cell_to_string(value: Any, *, preserve_sql_null: bool = False) -> str:
    """Convert a typed Python value into a canonical intermediate string.

    * None → "" by default, or ``SQL_NULL_SENTINEL`` when ``preserve_sql_null``
      (SQL readers) so empty string and SQL NULL stay distinct on the wire
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
        return SQL_NULL_SENTINEL if preserve_sql_null else ""

    # Sparse CDC / STOP_COLUMN / coerce omit — never serialize the sentinel into
    # CSV/JSON/export wires (would look like a real client value).
    if is_missing_sentinel(value):
        return ""

    # Missing-like values (pd.NA, np.nan, etc.) where value != value.
    if _is_na(value):
        return SQL_NULL_SENTINEL if preserve_sql_null else ""

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


def sanitize_json_value(value: Any, *, refuse_nonfinite: bool = True) -> Any:
    """Recursively convert a value into a JSON-serializable Python object.

    Unlike `_json_default`, this is a pre-processor: it returns values that
    `json.dumps` can serialize without needing a `default` callback. It converts
    ``Decimal`` to exact text (or numbers when safe), encodes bytes as base64, and
    normalizes datetime / UUID / ObjectId / Binary / numpy values. Strings are
    left as-is.

    Non-finite floats/Decimals and NA-like values raise by default (write path
    must quarantine, never invent JSON null). Pass ``refuse_nonfinite=False``
    for read/display surfaces that need a null placeholder.
    """
    if value is None:
        return None
    if is_missing_sentinel(value):
        if refuse_nonfinite:
            raise ValueError(
                "DF_MISSING sentinel refused for JSON sanitize — omit the key "
                "before serialize (STOP_COLUMN / sparse CDC)"
            )
        return None
    if _is_na(value):
        if refuse_nonfinite:
            raise ValueError("non-finite / NA value refused for JSON write")
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if refuse_nonfinite:
            raise ValueError(f"non-finite float refused for JSON write: {value!r}")
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            if refuse_nonfinite:
                raise ValueError(f"non-finite Decimal refused for JSON write: {value!r}")
            return None
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
            return sanitize_json_value(
                value.to_decimal(), refuse_nonfinite=refuse_nonfinite
            )
        except (Overflow, InvalidOperation, ValueError, TypeError):
            try:
                return str(value)
            except Exception:
                return None
    if _is_binary(value):
        return base64.b64encode(value.value).decode("ascii")
    if isinstance(value, Enum):
        return sanitize_json_value(value.value, refuse_nonfinite=refuse_nonfinite)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if hasattr(value, "ndim") and hasattr(value, "tolist"):
        if value.ndim == 0 and hasattr(value, "item") and callable(value.item):
            return sanitize_json_value(
                value.item(), refuse_nonfinite=refuse_nonfinite
            )
        return sanitize_json_value(value.tolist(), refuse_nonfinite=refuse_nonfinite)
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        if refuse_nonfinite:
            raise ValueError("pandas NA/NaT refused for JSON write")
        return None
    if isinstance(value, (set, tuple, frozenset)):
        return [
            sanitize_json_value(v, refuse_nonfinite=refuse_nonfinite) for v in value
        ]
    if isinstance(value, dict):
        return {
            str(k): sanitize_json_value(v, refuse_nonfinite=refuse_nonfinite)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            sanitize_json_value(v, refuse_nonfinite=refuse_nonfinite) for v in value
        ]
    # Last resort: never emit repr() artifacts.
    return str(value)


def json_default(value: Any) -> Any:
    """Public JSON-default helper for json.dumps() callers."""
    return _json_default(value)


_JSON_NUMBER_RE = re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def _mark_exact_numbers(value: Any, mark: str) -> Any:
    """Tag Decimals so the dump can restore them as unquoted literals."""
    if isinstance(value, Decimal):
        text = safe_decimal_text(value)
        return mark + text if _JSON_NUMBER_RE.match(text) else text
    if isinstance(value, dict):
        return {k: _mark_exact_numbers(v, mark) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mark_exact_numbers(v, mark) for v in value]
    return value


def json_dumps_exact_numbers(value: Any, **kwargs: Any) -> str:
    """``json.dumps`` that writes Decimals as exact JSON *numbers*.

    JSON numbers carry arbitrary precision, so an exported ``NUMERIC(12,2)``
    cell belongs in the file as ``1000.00`` — not as the quoted ``"1000.00"``
    the string policy in ``_decimal_to_json`` produces, which retypes a whole
    numeric column to text for every downstream reader. Digits, scale and
    exponent are the exact ones the source stated; ``float`` is never involved.

    Values JSON has no number grammar for (NaN, Infinity) keep the string form.
    """
    kwargs.setdefault("default", json_default)
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("allow_nan", False)
    # Per-dump token: a *string* cell that happened to carry the marker text
    # must never be unquoted into a bare literal.
    mark = f"\x00df{uuid.uuid4().hex}#"
    dumped = json.dumps(_mark_exact_numbers(value, mark), **kwargs)
    pattern = re.escape(json.dumps(mark)[1:-1])
    return re.sub(
        rf'"{pattern}(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"',
        lambda m: m.group(1),
        dumped,
    )
