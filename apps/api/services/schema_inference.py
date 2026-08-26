"""Top-level schema intelligence engine for Datawrap.

Universal contract
------------------
Every connector path (Mongo, SQL, files, warehouses) must type columns through
this module before Map / preflight / CREATE TABLE. Rules are fail-safe:

1. **Values beat name guesses.** A typed logical type is emitted only when
   every non-empty sample parses as that type.
2. **Booleans are write-path tokens only** (true/false/t/f/1/0 — the same
   set ``transform_engine.CANONICAL_BOOLEAN_TOKENS`` binds). Informal
   yes/no/y/n/on/off stay VARCHAR: inventing BOOLEAN dest quarantines every
   informal row. Words like ``active`` / ``inactive`` / ``pending`` are
   **string enums**, never booleans.
3. **Name heuristics only disambiguate** (e.g. 0/1 on ``is_active`` → BOOLEAN;
   epoch digits on ``created_at`` → TIMESTAMP). Names never invent a type that
   samples contradict.
4. **Widen to VARCHAR/TEXT** on mixed or ambiguous columns — never invent a
   tight type that will fail dry-run on the next unseen value.

Public API: ``infer_type``, ``infer_column``, ``infer_columns_from_rows``.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from services.decimal_observe import observe_source_numeric_samples
from services.transform_engine import (
    CANONICAL_BOOLEAN_TOKENS,
    NULL_SENTINELS,
    _active_date_locale,
    _parse_date,
    _parse_datetime,
    _parse_decimal,
    vector_component_carrier,
)
from services.value_serializer import evidence_samples, is_null_evidence

# Logical types emitted to mapping / preflight / DDL layers
LOGICAL_TYPES = frozenset({
    "INTEGER", "DECIMAL", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME",
    "VARCHAR", "TEXT", "UUID", "JSON", "ARRAY", "BINARY",
    "INTERVAL", "GEOGRAPHY", "VECTOR",
})

# Status / lifecycle vocabulary — never treat as boolean literals.
_STATUS_ENUM_TOKENS = frozenset({
    "active", "inactive", "enabled", "disabled", "pending", "invalidated",
    "approved", "rejected", "completed", "cancelled", "canceled", "draft",
    "published", "archived", "deleted", "suspended", "locked", "unlocked",
    "open", "closed", "new", "old", "success", "failure", "failed", "passed",
    "processing", "processed", "queued", "running", "stopped", "paused",
    "ok", "error", "warning", "info", "unknown", "n/a", "na", "none",
    "positive", "negative", "aye", "nope",
})


def _is_boolean_field_name(name: str) -> bool:
    """True only for flag-shaped names — not bare status/lifecycle words.

    Matches: is_active, has_flag, deviceVerified, email_verified, enabled, *_bool.
    Rejects: status, state, active, completed, approved (those are usually enums).
    """
    n = (name or "").strip()
    if not n:
        return False
    # camelCase / snake prefix flags
    if re.search(r"(?:^|_)(is|has|can|should|was|are|do|does|did)(?:[A-Z_]|_)", n):
        return True
    if re.search(r"(?:^|_)(?:is|has)_[a-z0-9]", n, re.I):
        return True
    # Explicit flag/bool suffix or enabled/disabled/verified/confirmed as the
    # whole name or trailing token (deviceVerified, email_verified).
    if re.search(r"(?:^|_|[a-z])(?:flag|bool|enabled|disabled|verified|confirmed)$", n, re.I):
        return True
    if re.fullmatch(r"(?:enabled|disabled|verified|confirmed|flag|bool)", n, re.I):
        return True
    return False


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]+={0,2}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ENUM_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-./]{0,63}$")

# WKT geometry — identity payload; no invented SRID/cast.
_WKT_RE = re.compile(
    r"^\s*(?:SRID=\d+;)?\s*"
    r"(MULTI)?(POINT|LINESTRING|POLYGON|GEOMETRYCOLLECTION)\s*"
    r"(Z|M|ZM)?\s*\(",
    re.I,
)
_GEOJSON_TYPES = frozenset({
    "Point", "MultiPoint", "LineString", "MultiLineString",
    "Polygon", "MultiPolygon", "GeometryCollection", "Feature", "FeatureCollection",
})
# ISO-8601 durations (P1D, PT15M, P1Y2M3DT4H) — identity INTERVAL.
# Require at least one numeric component (reject bare "P" / "PT").
_ISO_INTERVAL_RE = re.compile(
    r"^P(?=\d|T\d)"
    r"(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?"
    r"(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$",
    re.I,
)
_SQL_INTERVAL_RE = re.compile(
    r"^\d+\s+(?:year|years|month|months|day|days|hour|hours|minute|minutes|second|seconds)"
    r"(?:\s+\d+\s+(?:year|years|month|months|day|days|hour|hours|minute|minutes|second|seconds))*$",
    re.I,
)
_VECTOR_FIELD_RE = re.compile(
    r"(?:^|_)(?:embed(?:ding)?|vector|vec|latent|encoding)(?:$|_)",
    re.I,
)

# Minimum homogeneous float-array length to treat as VECTOR without a vector-ish name.
_VECTOR_MIN_DIM_DEFAULT = 8
# Absolute floor even with a vector-ish name (avoids [lat,lon] → VECTOR(2) by accident).
_VECTOR_MIN_DIM_NAMED = 3


def _is_base64(value: str) -> bool:
    s = value.strip()
    if len(s) < 12 or len(s) % 4 != 0:
        return False
    if not _BASE64_RE.match(s):
        return False
    if len(s) > 64 and len(set(s)) <= 3:
        return False
    if s.isalpha() and len(s) > 32:
        return False
    if all(c in "0123456789abcdefABCDEF" for c in s):
        return False
    return True


def _looks_like_binary_payload(value: str, *, field_name: str | None = None) -> bool:
    """Promote to BINARY only with name evidence or strong payload evidence.

    Short base64-looking tokens (session ids, opaque keys) must stay VARCHAR —
    never invent BINARY DDL from a single 12–20 char sample (Airbyte trap).
    """
    if not _is_base64(value):
        return False
    if _is_binary_field_name(field_name or ""):
        return True
    s = value.strip()
    # Strong evidence without a binary-ish name: longer payload + padding or high entropy.
    if len(s) < 32:
        return False
    if s.endswith("=") or s.endswith("=="):
        return True
    return len(set(s)) >= 12


# Binary payloads only — not generic "data"/"key"/"token" (those are often IDs/JWTs).
_BINARY_FIELD_RE = re.compile(
    r"(?:^|_)(?:payload|binary|blob|bytea|bytes|b64|base64|image|audio|video|pdf|"
    r"attachment|thumbnail|avatar_bytes|file_bytes|raw_bytes)(?:_?\d*)?(?:$|_)",
    re.I,
)


def _is_binary_field_name(name: str) -> bool:
    return bool(_BINARY_FIELD_RE.search(name or ""))


_TIMESTAMP_FIELD_RE = re.compile(
    r"(?:^|_)(?:time|date|timestamp|datetime|epoch|unix|created|updated|modified|"
    r"logged|expires|occurred|scheduled|started|ended|birth)"
    r"\d*(?:$|_)"
    r"|(?:^|_)(?:created|updated|modified|logged|expires|occurred|started|ended)_(?:at|on)$"
    r"|_at$|_dt$",
    re.I,
)

_DATE_FIELD_RE = re.compile(
    r"(?:^|_)(?:date|day|dob|birth|yyyymmdd|txn_dt|pay_dt|trans_dt)(?:$|_)"
    r"|_date$|_dt$",
    re.I,
)


def _is_timestamp_field_name(name: str) -> bool:
    return bool(_TIMESTAMP_FIELD_RE.search(name or ""))


def _is_date_field_name(name: str) -> bool:
    return bool(_DATE_FIELD_RE.search(name or "")) or _is_timestamp_field_name(name)


def _valid_yyyymmdd(text: str) -> bool:
    if not re.fullmatch(r"\d{8}", text):
        return False
    try:
        dt = datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return False
    return 1900 <= dt.year <= 2100


def _looks_like_string_enum(samples: list[str]) -> bool:
    """Low-cardinality symbolic tokens that are not strict booleans."""
    vals = [s.strip() for s in samples if s and str(s).strip()]
    if len(vals) < 1:
        return False
    distinct = {v.lower() for v in vals}
    if not distinct:
        return False
    if distinct <= CANONICAL_BOOLEAN_TOKENS:
        return False
    if len(distinct) > 32:
        return False
    if not all(_ENUM_TOKEN_RE.match(v) for v in vals):
        return False
    # Any status-vocabulary token, or >2 distinct labels → enum
    if distinct & _STATUS_ENUM_TOKENS:
        return True
    if len(distinct) >= 2 and all(not v.isdigit() for v in distinct):
        # Short alphabetic labels (pending/active/…)
        if all(len(v) <= 32 and v.replace("_", "").replace("-", "").isalpha() for v in distinct):
            return True
    return False


def _is_vector_field_name(name: str) -> bool:
    return bool(_VECTOR_FIELD_RE.search(name or ""))


def _parse_vector_array(value: str) -> list[float] | None:
    """Return float list when value is a homogeneous numeric JSON array; else None."""
    s = (value or "").strip()
    if not (s.startswith("[") and s.endswith("]")):
        return None
    try:
        from services.value_serializer import json_loads_exact

        parsed = json_loads_exact(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list) or len(parsed) < 2:
        return None
    out: list[float] = []
    for item in parsed:
        # JSON numbers only — string components are ARRAY / write-path, not
        # inferred VECTOR. bool ⊂ int must not invent a 1.0 dimension.
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return None
        bound = vector_component_carrier(item)
        if bound is None:
            return None
        out.append(bound)
    return out


def _looks_like_geojson(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    t = obj.get("type")
    if not isinstance(t, str) or t not in _GEOJSON_TYPES:
        return False
    if t in {"Feature", "FeatureCollection"}:
        return True
    return "coordinates" in obj or "geometries" in obj


def _looks_like_interval(value: str) -> bool:
    s = (value or "").strip()
    if not s:
        return False
    if _ISO_INTERVAL_RE.match(s) and s.upper() != "P":
        return True
    if _SQL_INTERVAL_RE.match(s):
        return True
    # Python timedelta wire from cell_to_string: "1 day, 0:00:01" / "0:00:01"
    if re.fullmatch(r"\d+:\d{2}:\d{2}(?:\.\d+)?", s):
        # Ambiguous with TIME — only treat as INTERVAL when hours can exceed 23
        # or when prefixed with day count elsewhere. Keep TIME for HH:MM:SS.
        parts = s.split(":")
        try:
            return int(parts[0]) > 23
        except ValueError:
            return False
    if " day" in s.lower() or " days" in s.lower():
        return True
    return False


def is_geography_wire(value: Any) -> bool:
    """True when a cell can travel as GEOGRAPHY/GEOMETRY without inventing a cast.

    Accepts WKT, EWKT (SRID=…;…), GeoJSON text/objects, and raw EWKB bytes.
    Rejects empty / clearly non-spatial strings so writers can quarantine fail-closed
    instead of letting the driver invent NULLs or abort mid-batch.
    """
    if value is None:
        return True
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value) > 0
    if isinstance(value, dict):
        return _looks_like_geojson(value)
    text = str(value).strip()
    if not text:
        return False
    if _WKT_RE.match(text):
        return True
    if text[0] in "{[":
        try:
            import json

            parsed = json.loads(text)
        except Exception:
            return False
        return _looks_like_geojson(parsed)
    # Hex EWKB (PostGIS / MySQL common wire) — even-length hex, WKB byte order 0/1.
    if len(text) >= 10 and len(text) % 2 == 0 and re.fullmatch(r"[0-9a-fA-F]+", text):
        return text[:2].lower() in {"00", "01"}
    return False


def is_interval_wire(value: Any) -> bool:
    """True when a cell looks like an INTERVAL identity payload (ISO-8601 / SQL)."""
    if value is None:
        return True
    if isinstance(value, (int, float)):
        # Raw numeric seconds/days is ambiguous — refuse inventing INTERVAL.
        return False
    # datetime.timedelta travels as string via serializers; accept native too.
    try:
        from datetime import timedelta

        if isinstance(value, timedelta):
            return True
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return False
    return _looks_like_interval(text)


def interval_wire_family(value: Any) -> str | None:
    """Return ``ym`` / ``ds`` when the wire payload is family-specific, else None.

    Used by write quarantine so YEAR-MONTH values never bind into DAY-SECOND DDL
    (and vice versa) — ANSI/Oracle/Snowflake family polarity.
    """
    if value is None:
        return None
    try:
        from datetime import timedelta

        if isinstance(value, timedelta):
            return "ds"
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    # ISO-8601: P1Y2M (ym) vs P1DT2H / PT15M (ds). Mixed Y/M + T… is rare; prefer ds
    # when a time designator is present, else ym when only Y/M, else ds on D/W.
    if _ISO_INTERVAL_RE.match(text):
        has_ym = bool(re.search(r"\d+Y|\d+M", upper.split("T", 1)[0]))
        has_ds = "T" in upper or bool(re.search(r"\d+[DWS]", upper))
        if has_ym and not has_ds:
            return "ym"
        if has_ds and not has_ym:
            return "ds"
        if has_ym and has_ds:
            # Mixed calendar+time — not a pure YM bind target.
            return "ds"
        return None
    if re.search(r"\b(?:year|years|month|months)\b", text, re.I) and not re.search(
        r"\b(?:day|days|hour|hours|minute|minutes|second|seconds)\b", text, re.I
    ):
        return "ym"
    if re.search(
        r"\b(?:day|days|hour|hours|minute|minutes|second|seconds)\b", text, re.I
    ):
        return "ds"
    # Oracle / SQL literal shapes: '1-2' (YM) vs '1 02:03:04' (DS).
    if re.fullmatch(r"[+-]?\d+-\d{1,2}", text):
        return "ym"
    if re.fullmatch(r"[+-]?\d+\s+\d{1,2}:\d{2}:\d{2}(?:\.\d+)?", text):
        return "ds"
    if _looks_like_interval(text):
        return None
    return None


def geography_wire_srid(value: Any) -> int | None:
    """Extract SRID from EWKT (``SRID=4326;POINT(...)``) when present."""
    if value is None or isinstance(value, (bytes, bytearray, memoryview, dict)):
        return None
    text = str(value).strip()
    m = re.match(r"^\s*SRID\s*=\s*(\d+)\s*;", text, re.I)
    if m:
        return int(m.group(1))
    return None


def _classify_jsonish(value: str, *, field_name: str | None = None) -> str | None:
    """Classify JSON / array / GeoJSON / VECTOR candidates. None → not JSON-shaped."""
    s = value.strip()
    if not ((s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))):
        return None
    try:
        from services.value_serializer import json_loads_exact

        parsed = json_loads_exact(s)
    except json.JSONDecodeError:
        return None

    if isinstance(parsed, dict):
        if _looks_like_geojson(parsed):
            return "GEOGRAPHY"
        return "JSON"

    if isinstance(parsed, list):
        vec = _parse_vector_array(s)
        if vec is not None:
            min_dim = _VECTOR_MIN_DIM_NAMED if _is_vector_field_name(field_name or "") else _VECTOR_MIN_DIM_DEFAULT
            if len(vec) >= min_dim:
                # Provisional — infer_column promotes to VECTOR(n) when dims agree.
                return "VECTOR"
        return "ARRAY"
    return "JSON"


def _classify_value(value: str, *, field_name: str | None = None) -> str:
    s = value.strip()
    if not s or s.lower() in NULL_SENTINELS:
        return "VARCHAR"

    # Status vocabulary is always text — never boolean/date.
    if s.lower() in _STATUS_ENUM_TOKENS:
        return "VARCHAR"

    # Specialty identity types — detect before generic JSON / string widen.
    if _WKT_RE.match(s):
        return "GEOGRAPHY"
    if _looks_like_interval(s):
        return "INTERVAL"

    jsonish = _classify_jsonish(s, field_name=field_name)
    if jsonish:
        return jsonish

    if _UUID_RE.match(s):
        return "UUID"

    if _looks_like_binary_payload(s, field_name=field_name):
        return "BINARY"

    low = s.strip().lower()
    if low in CANONICAL_BOOLEAN_TOKENS:
        # Defer 0/1 disambiguation to infer_type (field name known).
        if low in {"0", "1"}:
            return "INTEGER"
        return "BOOLEAN"

    # YYYYMMDD only when calendar-valid and field looks temporal (avoids SKUs/zips).
    if re.fullmatch(r"\d{8}", s):
        if _valid_yyyymmdd(s) and (field_name is None or _is_date_field_name(field_name) or "yyyymmdd" in (field_name or "").lower()):
            return "DATE"
        # Without a date-ish name, keep as integer/string later
        if _valid_yyyymmdd(s) and field_name is None:
            return "DATE"

    # Honor the active transfer date_locale (DMY/MDY/Auto) so inference matches
    # the parser that will be used at write time. Without this, ambiguous dates
    # like 5/8/1967 are classified as VARCHAR even when the locale is resolved.
    active_locale = _active_date_locale()
    if _parse_date(s, date_locale=active_locale) is not None and not re.fullmatch(r"\d{8}", s):
        return "DATE"

    if _parse_datetime(s, date_locale=active_locale) is not None:
        # Preserve TZ awareness when the sample carries Z / offset — never invent SRID/cast.
        if re.search(r"(Z|[+-]\d{2}:?\d{2})$", s, re.I):
            return "TIMESTAMPTZ"
        return "TIMESTAMP"

    for fmt in ("%H:%M:%S", "%H:%M:%S.%f", "%H:%M:%S%z"):
        try:
            datetime.strptime(s.replace("Z", "+0000"), fmt.replace("Z", "+0000"))
            return "TIME"
        except ValueError:
            continue

    decimal_parsed = _parse_decimal(s)
    if decimal_parsed is not None:
        if "." in decimal_parsed or "e" in s.lower():
            return "DECIMAL"
        try:
            iv = int(decimal_parsed)
        except (ValueError, TypeError):
            return "VARCHAR"
        if iv > 2**63 - 1 or iv < -(2**63):
            return "VARCHAR"
        return "INTEGER"

    if len(s) > 255:
        return "TEXT"
    if _EMAIL_RE.match(s):
        return "VARCHAR"
    return "VARCHAR"


@contextmanager
def _column_date_locale(samples: list[str]):
    """Resolve one date ordering for the whole column before classifying cells.

    ``12/31/2024`` is unambiguously MDY, but ``5/8/1967`` beside it is not, and a
    cell judged on its own becomes VARCHAR. One such cell made the column mixed,
    so a date column landed as text even though the write path went on to parse
    every value as MDY correctly — the type and the values disagreed.

    Reading the ordering from the column and classifying under it is what a
    reader does with a CSV: the unambiguous rows settle the ambiguous ones. An
    explicit transfer locale still wins, and a column with no unambiguous member
    resolves to nothing and stays text rather than guessing an ordering.
    """
    from services.transform_engine import (
        _active_date_locale,
        infer_date_locale,
        reset_active_date_locale,
        set_active_date_locale,
    )

    token = None
    if not _active_date_locale():
        resolved = infer_date_locale(samples)
        if resolved:
            token = set_active_date_locale(resolved)
    try:
        yield
    finally:
        if token is not None:
            reset_active_date_locale(token)


def infer_type(
    samples: list[str], *, threshold: float = 0.85, field_name: str | None = None
) -> str:
    """Infer a single logical type for a column from sample values."""
    return str(infer_column(samples, field_name=field_name)["logical_type"])


def infer_schema_map(
    samples_by_field: dict[str, list[str]],
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Canonical choke point: field → logical type + full intelligence records.

    All introspect paths (Mongo, SQL sample refine, files) should call this
    instead of ad-hoc ``infer_type`` loops so semantic_role / notes stay attached.
    """
    schema: dict[str, str] = {}
    intel: dict[str, dict[str, Any]] = {}
    for field, samples in samples_by_field.items():
        rec = infer_column(samples, field_name=field)
        schema[field] = str(rec["logical_type"])
        intel[field] = rec
    return schema, intel


def _samples_fit_declared_numeric(samples: list[str], logical_type: str) -> bool | None:
    """Do the samples fit a declared exact-numeric carrier?

    ``None`` when the declared type is not an exact numeric one with a known
    width, so the caller falls back to inference. Otherwise the verdict is
    arithmetic: every sample must parse and fit the declared precision/scale,
    with no rounding invented (a value needing more scale than the column keeps
    does not fit — the write path refuses to quantize silently).
    """
    from decimal import Decimal, InvalidOperation

    from services.numeric_fit import integer_storage_bounds
    from services.transform_engine import apply_transform
    from services.type_system import (
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    logical = normalize_logical_type(logical_type)
    if logical not in {"integer", "decimal"}:
        return None
    precision, scale = parse_numeric_precision_scale(logical_type)
    bounds = integer_storage_bounds(logical_type) if logical == "integer" else None
    if precision is None and bounds is None:
        if logical != "decimal":
            # Unqualified INTEGER: the physical width is engine-dependent, so
            # only the destination-resolved carrier can answer.
            return None
        # Unqualified DECIMAL asks one question — are these numbers? — and
        # arithmetic answers it. Deferring to ``infer_type`` said "no" for long
        # digit runs, which is how a value the destination stores exactly ended
        # up diverted into a text column.
        precision, scale = None, None
    for raw in samples[:200]:
        text = str(raw).strip()
        if not text:
            continue
        digits_only = text[1:] if text[:1] in "+-" else text
        if len(digits_only) > 1 and digits_only[:1] == "0" and digits_only[1:2] != ".":
            # A leading zero is data (zip codes, account numbers) and no numeric
            # carrier keeps it — this is a text column, whatever it parses as.
            return False
        # Parse through the write path's own coercion so locale forms
        # ("1,000.00", "2.000,50") are read exactly as the writer will read
        # them — a second parser here would disagree with what actually lands.
        coerced, err = apply_transform(text, "decimal")
        if err:
            return False
        try:
            value = Decimal(str(coerced))
        except (InvalidOperation, ValueError, ArithmeticError):
            return False
        if not value.is_finite():
            return False
        if bounds is not None:
            if value != value.to_integral_value():
                return False
            if not (bounds[0] <= int(value) <= bounds[1]):
                return False
            continue
        if precision is None:
            # Width unknown — the value being a finite number is the verdict.
            continue
        exponent = value.as_tuple().exponent
        used_scale = -int(exponent) if isinstance(exponent, int) and exponent < 0 else 0
        declared_scale = int(scale or 0)
        if used_scale > declared_scale:
            return False
        digits = len(value.as_tuple().digits)
        integral_digits = max(digits - used_scale, 1)
        if integral_digits > int(precision or 0) - declared_scale:
            return False
    return True


def samples_fit_logical_type(samples: list[str], logical_type: str, *, field_name: str | None = None) -> bool:
    """True when every non-empty sample coerces cleanly to ``logical_type``."""
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    non_empty = evidence_samples(samples)
    if not non_empty:
        return True
    lt = (logical_type or "VARCHAR").upper()
    if lt in {"VARCHAR", "TEXT", "STRING", "CHAR"}:
        return True
    # A declared numeric carrier is answered by arithmetic, not by inference.
    # ``infer_type`` keeps long digit runs as VARCHAR so account numbers and zip
    # codes are not turned into integers; taking that as "does not fit
    # DECIMAL(38,0)" made Map invent a shadow ``wide_num_text`` column beside a
    # destination DECIMAL(38,0) that plainly holds the value — and the real
    # column stayed NULL for every row.
    numeric_verdict = _samples_fit_declared_numeric(non_empty, lt)
    if numeric_verdict is not None:
        return numeric_verdict
    # Re-infer; if engine widens away from proposed type, samples do not fit.
    inferred = infer_type(non_empty, field_name=field_name)
    if inferred in {"VARCHAR", "TEXT"} and lt not in {"VARCHAR", "TEXT", "STRING"}:
        return False
    transform = infer_transform_for_mapping(
        field_name or "col",
        field_name or "col",
        inferred,
        lt,
    )
    typed = {"boolean", "integer", "decimal", "date", "datetime", "time", "uuid", "json", "binary"}
    if transform not in typed and lt in {"BOOLEAN", "INTEGER", "DECIMAL", "DATE", "TIMESTAMP", "TIME", "UUID", "JSON", "BINARY"}:
        # Explicit typed DDL with only trim — verify via apply_transform alias
        engine_t = {
            "BOOLEAN": "boolean",
            "INTEGER": "integer",
            "DECIMAL": "decimal",
            "DATE": "date",
            "TIMESTAMP": "datetime",
            "TIME": "time",
            "UUID": "uuid",
            "JSON": "json",
            "BINARY": "binary",
        }.get(lt)
        if not engine_t:
            return True
        transform = engine_t
    if transform not in typed:
        return True
    for raw in non_empty[:200]:
        _val, err = apply_transform(raw, transform)
        if err:
            return False
    return True


def safe_ddl_logical_type(
    proposed: str,
    samples: list[str] | None,
    *,
    field_name: str | None = None,
    source_type: str | None = None,
    honor_explicit: bool = False,
) -> str:
    """For new-table DDL: never emit a tight type samples cannot all coerce to.

    Destination-native DDL (e.g. Postgres ``TIMESTAMPTZ``, Snowflake ``TIMESTAMP_TZ``)
    is canonicalized to a logical type first. Without that, identity mappings that
    already projected ``ddl_type(dest, TIMESTAMP)`` were treated as unknown and
    wrongly widened to VARCHAR — CREATE TABLE then stored ISO strings as TEXT.

    When ``honor_explicit`` is True (operator / Map set ``target_type``), preserve
    the **physical stamp** unchanged (``TIMESTAMP_LTZ``, ``CHAR(36)``, ``INET``,
    ``BOOLEAN``, …). Migration Assurance: Map≡CREATE — never rewrite approved
    DDL from sample inference. Values that do not coerce quarantine on write;
    they must not mutate the approved schema.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        ddl_carrier_type,
        normalize_logical_type,
        parse_numeric_precision_scale,
        parse_vector_dimension,
    )

    original = (proposed or source_type or "VARCHAR").strip() or "VARCHAR"
    proposed_u = original.upper()
    if proposed_u in {"STRING", "CHAR", "CHARACTER", "CHARACTER VARYING"}:
        proposed_u = "VARCHAR"
    # Preserve DECIMAL(p,s) / VECTOR(n) / FLOAT carriers before class-level collapse.
    carrier_src = ddl_carrier_type(proposed or source_type or "VARCHAR")
    if normalize_logical_type(carrier_src) == LOGICAL_DECIMAL:
        precision, _scale = parse_numeric_precision_scale(carrier_src)
        if precision is not None:
            if not samples or samples_fit_logical_type(
                samples, "DECIMAL", field_name=field_name
            ):
                return carrier_src
    if normalize_logical_type(carrier_src) == "vector":
        dim = parse_vector_dimension(carrier_src)
        # Keep declared width even when samples are opaque float arrays as text.
        if dim is not None:
            return carrier_src
    # Float carriers: never collapse REAL / DOUBLE PRECISION / HALF → bare FLOAT
    # before honor_explicit — that destroys Map create-new stamps (writer then
    # invents DOUBLE PRECISION from FLOAT). Soften only when not honoring stamp.
    if normalize_logical_type(carrier_src) == "float" and not honor_explicit:
        if not samples or samples_fit_logical_type(samples, "FLOAT", field_name=field_name):
            return "FLOAT"

    # Explicit Map / operator target_type: Map≡CREATE. Never infer_type-replace.
    if honor_explicit:
        return original

    # Map dest-native / alias DDL → canonical logical vocabulary used by writers.
    _NORM_TO_LOGICAL = {
        "integer": "INTEGER",
        "decimal": "DECIMAL",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "time": "TIME",
        "string": "VARCHAR",
        "text": "TEXT",
        "uuid": "UUID",
        "json": "JSON",
        "array": "ARRAY",
        "binary": "BINARY",
        "interval": "INTERVAL",
        "geography": "GEOGRAPHY",
        "vector": "VECTOR",
        "timestamptz": "TIMESTAMPTZ",
    }

    canonical = _NORM_TO_LOGICAL.get(normalize_logical_type(proposed_u))
    if canonical:
        proposed_u = canonical
    if not samples:
        # Prefer source type when no samples; still avoid BOOLEAN without evidence.
        if proposed_u == "BOOLEAN" and source_type and str(source_type).upper() in {"VARCHAR", "TEXT", "STRING"}:
            return "VARCHAR"
        return proposed_u if proposed_u in LOGICAL_TYPES else "VARCHAR"
    # Loose text proposals: upgrade when every sample coerces to a tighter type.
    # Without this, CREATE TABLE stays VARCHAR and writers skip type transforms
    # (e.g. "true"/"false"/"1" never become BOOLEAN).
    if proposed_u in {"VARCHAR", "TEXT", "STRING", "CHAR"}:
        inferred = infer_type(samples, field_name=field_name)
        if inferred not in {"VARCHAR", "TEXT"} and samples_fit_logical_type(
            samples, inferred, field_name=field_name
        ):
            return inferred if inferred in LOGICAL_TYPES else "VARCHAR"
    if samples_fit_logical_type(samples, proposed_u, field_name=field_name):
        return proposed_u if proposed_u in LOGICAL_TYPES else "VARCHAR"
    # Widen using fresh inference (string enums → VARCHAR, etc.)
    return infer_type(samples, field_name=field_name)


def infer_column(
    samples: list[str], *, field_name: str | None = None
) -> dict[str, Any]:
    """Full schema-intelligence record for one column.

    Returns keys: logical_type, semantic_role, confidence, notes, samples.
    """
    non_empty = evidence_samples(samples)
    notes: list[str] = []
    if not non_empty:
        return {
            "name": field_name or "",
            "logical_type": "VARCHAR",
            "semantic_role": "unknown",
            "confidence": 0.5,
            "notes": ["no samples — default VARCHAR"],
            "samples": [],
        }

    # Explicit string-enum short-circuit (status=active/invalidated, state=pending, …)
    if _looks_like_string_enum(non_empty):
        notes.append("string enum vocabulary — VARCHAR (not BOOLEAN)")
        return {
            "name": field_name or "",
            "logical_type": "VARCHAR",
            "semantic_role": "string_enum",
            "confidence": 0.95,
            "notes": notes,
            "samples": non_empty[:8],
        }

    # Write-path tokens only. Informal yes/on mixed with true/1 must stay
    # VARCHAR — classifying them BOOLEAN invents a dest Execute cannot bind.
    # Canonical true/false mixed with 0/1 must stay BOOLEAN (classifying "1"
    # as INTEGER alone would widen and skip bool coercion).
    if all(v.lower() in CANONICAL_BOOLEAN_TOKENS for v in non_empty):
        only_01 = all(v.strip() in {"0", "1"} for v in non_empty)
        if (not only_01) or _is_boolean_field_name(field_name or ""):
            notes.append("canonical boolean wire (true/false/t/f/0/1) → BOOLEAN")
            return {
                "name": field_name or "",
                "logical_type": "BOOLEAN",
                "semantic_role": "boolean_flag",
                "confidence": 0.98,
                "notes": notes,
                "samples": non_empty[:8],
            }

    with _column_date_locale(non_empty):
        counts: Counter[str] = Counter(
            _classify_value(s, field_name=field_name) for s in non_empty
        )
    types = set(counts.keys())

    if types <= {"INTEGER", "DECIMAL"}:
        # Sample-aware DECIMAL(p,s) / FLOAT invent — never bare DECIMAL → (38,15).
        obs = observe_source_numeric_samples(non_empty)
        inferred = str(obs.get("carrier") or ("DECIMAL" if "DECIMAL" in types else "INTEGER"))
        role = "numeric"
        if obs.get("kind") == "ieee_float":
            notes.append(
                "IEEE/Excel float residue — invent FLOAT (not fake money DECIMAL)"
            )
        elif obs.get("kind") == "fixed_decimal":
            notes.append(
                f"observed DECIMAL({obs.get('precision')},{obs.get('scale')}) "
                f"from samples (max_int={obs.get('max_int_digits')})"
            )
        elif obs.get("kind") == "integer":
            notes.append("all integral samples")
    elif types <= {"DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME"}:
        tz_count = counts.get("TIMESTAMPTZ", 0)
        # Promote to TIMESTAMPTZ only when the column is unanimously TZ-aware,
        # or when a temporal field name has at least one TZ sample.
        if tz_count > 0 and (tz_count == len(non_empty) or (field_name and _is_timestamp_field_name(field_name))):
            inferred = "TIMESTAMPTZ"
        elif counts.get("TIMESTAMP", 0) >= counts.get("DATE", 0) and counts.get("TIMESTAMP", 0) >= counts.get("TIME", 0):
            inferred = "TIMESTAMP"
        elif counts.get("DATE", 0) >= counts.get("TIME", 0):
            inferred = "DATE"
        else:
            inferred = "TIME"
        role = "temporal"
    elif len(types) == 1:
        inferred = next(iter(types))
        role = {
            "BOOLEAN": "boolean_flag",
            "UUID": "identifier",
            "JSON": "semi_structured",
            "ARRAY": "semi_structured",
            "BINARY": "binary",
            "TEXT": "text",
            "VARCHAR": "text",
            "VECTOR": "embedding",
            "GEOGRAPHY": "geography",
            "INTERVAL": "duration",
            "TIMESTAMPTZ": "temporal",
        }.get(inferred, "unknown")
    else:
        # Specialty + JSON/ARRAY: prefer specialty when mixed with structural leftovers.
        specialty = types & {"VECTOR", "GEOGRAPHY", "INTERVAL"}
        if len(specialty) == 1 and types <= specialty | {"JSON", "ARRAY", "VARCHAR"}:
            inferred = next(iter(specialty))
            role = {
                "VECTOR": "embedding",
                "GEOGRAPHY": "geography",
                "INTERVAL": "duration",
            }[inferred]
            notes.append(f"majority specialty {inferred} — identity payload")
        elif types <= {"JSON", "ARRAY"}:
            inferred = "ARRAY" if counts.get("ARRAY", 0) >= counts.get("JSON", 0) else "JSON"
            role = "semi_structured"
        elif "TEXT" in counts and max(len(s) for s in non_empty if _classify_value(s, field_name=field_name) == "TEXT") > 255:
            inferred = "TEXT"
            role = "text"
            notes.append("mixed sample types — widened to lossless text")
        else:
            inferred = "VARCHAR"
            role = "text"
            notes.append("mixed sample types — widened to lossless text")

    # VECTOR: promote to VECTOR(n) only when every sample agrees on dims — never invent.
    if inferred == "VECTOR":
        dims: list[int] = []
        ok = True
        for s in non_empty:
            vec = _parse_vector_array(s)
            if vec is None:
                ok = False
                break
            dims.append(len(vec))
        if ok and dims and len(set(dims)) == 1:
            n = dims[0]
            inferred = f"VECTOR({n})"
            role = "embedding"
            notes.append(f"homogeneous float array → VECTOR({n}) (dims from samples)")
        else:
            inferred = "ARRAY"
            role = "semi_structured"
            notes.append("float arrays with disagreeing/invalid dims — ARRAY (no invented VECTOR dim)")

    # GEOGRAPHY / INTERVAL stay identity — no invented SRID or cast.
    if inferred in {"GEOGRAPHY", "INTERVAL"}:
        notes.append(f"{inferred} — identity payload (no invented cast)")

    # 0/1 → BOOLEAN only on flag-shaped names (canonical wire, not yes/no).
    if (
        inferred in {"INTEGER", "VARCHAR"}
        and field_name
        and _is_boolean_field_name(field_name)
        and all(v.strip() in {"0", "1"} for v in non_empty)
    ):
        inferred = "BOOLEAN"
        role = "boolean_flag"
        notes.append("0/1 on flag-shaped field name → BOOLEAN")

    # Never keep BOOLEAN if any sample is status vocabulary
    if inferred == "BOOLEAN" and any(v.lower() in _STATUS_ENUM_TOKENS for v in non_empty):
        inferred = "VARCHAR"
        role = "string_enum"
        notes.append("status vocabulary present — demoted BOOLEAN → VARCHAR")

    if field_name and _is_binary_field_name(field_name):
        valid = 0
        for v in non_empty:
            s = v.strip()
            if len(s) >= 4 and len(s) % 4 == 0 and _BASE64_RE.match(s):
                try:
                    import base64

                    base64.b64decode(s, validate=True)
                    valid += 1
                except (ValueError, TypeError):
                    # Invalid base64 padding/alphabet — treat as non-binary below.
                    continue
        if valid == len(non_empty):
            inferred = "BINARY"
            role = "binary"

    # Bare epoch-shaped digits (10 or 13 chars) classify as TIMESTAMP per value.
    # Two ways that misreads an ordinary integer column:
    #   unanimous — every sample epoch-shaped, so the column reads as TIMESTAMP;
    #   mixed     — ordinary short integers alongside 10-digit ones give
    #               {INTEGER, TIMESTAMP}, which no widening rule covers, so the
    #               column fell through to VARCHAR and an integer key landed as
    #               text on Mongo/CSV → SQL routes.
    # The unanimous case needs a name to judge (a nameless all-epoch column is
    # more likely a real timestamp); the mixed case is already self-evidently
    # not a timestamp column, so it recovers even unnamed.
    epoch_mixed = inferred == "VARCHAR" and types == {"INTEGER", "TIMESTAMP"}
    if (
        (inferred == "TIMESTAMP" and field_name) or epoch_mixed
    ) and not _is_timestamp_field_name(field_name or ""):
        if all(re.match(r"^[+\-]?\d+$", v) for v in non_empty):
            try:
                for v in non_empty:
                    int(v)
                # Re-observe so a value beyond int64 still widens to DECIMAL
                # rather than being forced into INTEGER.
                obs = observe_source_numeric_samples(non_empty)
                inferred = str(obs.get("carrier") or "INTEGER")
                role = "numeric"
                notes.append("long digits without temporal name — numeric not TIMESTAMP")
            except ValueError:
                inferred = "VARCHAR"
                role = "text"

    # YYYYMMDD without date-ish name → INTEGER/VARCHAR, not DATE
    if inferred == "DATE" and field_name and not _is_date_field_name(field_name):
        if all(re.fullmatch(r"\d{8}", v) for v in non_empty):
            inferred = "INTEGER"
            role = "numeric"
            notes.append("8-digit values without date-ish name — not DATE")

    confidence = 0.99 if len(types) == 1 and not notes else 0.85
    if role == "string_enum":
        confidence = 0.95

    return {
        "name": field_name or "",
        "logical_type": inferred,
        "semantic_role": role,
        "confidence": confidence,
        "notes": notes,
        "samples": non_empty[:8],
    }


def infer_columns_from_rows(headers: list[str], rows: list[list[Any]], *, max_samples: int = 50) -> list[dict]:
    columns = []
    sample_rows = rows[:max_samples]
    for i, name in enumerate(headers):
        samples = [str(row[i]) if i < len(row) else "" for row in sample_rows]
        intel = infer_column(samples, field_name=name)
        columns.append(
            {
                "name": name.strip() or f"column_{i + 1}",
                "inferred_type": intel["logical_type"],
                "semantic_role": intel["semantic_role"],
                "confidence": intel["confidence"],
                "notes": intel["notes"],
                "nullable": any(is_null_evidence(s) for s in samples),
                "samples": evidence_samples(samples[:5]),
            }
        )
    return columns
