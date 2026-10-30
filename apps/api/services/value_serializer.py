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
from decimal import Decimal, DecimalException, InvalidOperation, Overflow
from enum import Enum
from typing import Any

# Fixed-point expansion past type_system DECIMAL budgets is unsafe
# (memory / driver Overflow). Prefer short scientific form for Redis/JSON/CSV.
from services.type_system import decimal_needs_scientific_wire

# Explicit SQL NULL on the transfer string wire — distinct from empty string "".
# SQL readers use ``cell_to_string(..., preserve_sql_null=True)``; apply_transform
# maps the sentinel back to Python None → destination SQL NULL.
SQL_NULL_SENTINEL = "__DF_SQL_NULL__"

# Escape prefix for *real string values* that would collide with a wire
# sentinel (D109). ``cell_to_string(..., preserve_sql_null=True)`` emits
# ``__DF_ESC__`` + the literal so the in-band sentinel stays a proof of
# SQL NULL. ``unescape_wire_text`` strips one prefix at each decode layer.
ESCAPED_WIRE_PREFIX = "__DF_ESC__"

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
    if isinstance(value, WireLiteral):
        # Decoded "__DF_MISSING__" literal — present user text, never omit.
        return False
    return value is Missing or value == DF_MISSING_SENTINEL


class _ExplicitNullType:
    """A store's explicit NULL value (DynamoDB ``{"NULL": true}``), not an absent field."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "ExplicitNull"


#: The DynamoDB reader returns this for ``{"NULL": true}`` so a source read can
#: tell it from a missing attribute. Every verifier reads it as SQL NULL (D139:
#: Gate-8 read-back stringified the bare ``object()`` and failed a correct load).
EXPLICIT_NULL = _ExplicitNullType()


def is_explicit_null(value: Any) -> bool:
    return value is EXPLICIT_NULL


# Wire spellings that mean "no value here" — a DuckDB reader adds its own.
NULL_WIRE_SENTINELS: frozenset[str] = frozenset(
    {SQL_NULL_SENTINEL, DF_MISSING_SENTINEL, "__df_ddb_null__"}
)

# Decoders compare ``strip().lower()`` (apply_transform), so the collision
# domain is the sentinel set under the same normalization.
_NULL_WIRE_LOWER: frozenset[str] = frozenset(s.lower() for s in NULL_WIRE_SENTINELS)


class WireLiteral(str):
    """A decoded wire literal that spells a sentinel — always a real value.

    ``unescape_wire_text("__DF_ESC____DF_SQL_NULL__")`` yields the text
    ``"__DF_SQL_NULL__"`` — a user literal, not absence. A bare ``str`` cannot
    carry that fact, so every downstream sentinel check (``is_reader_null_cell``,
    ``_copy_text_value``'s ``\\N``, ``apply_transform``'s sentinel decode)
    re-classified the literal as NULL — the D109/D113 decode-order defect.
    Carrying the decoded text as a ``str`` subclass keeps every consumer
    working (psycopg2/sqlalchemy bind, COPY text, JSON) while the absence
    classifiers below refuse to treat it as absence. ``str(x)`` / ``x.strip()``
    degrade it to a plain str — only wire-decoding helpers preserve the marker.
    """


class WireCell(str):
    """A cell a reader already encoded for the transfer wire (D174).

    ``cell_to_string(None, preserve_sql_null=True)`` is ``"__DF_SQL_NULL__"`` and
    a user text that spells a sentinel is escaped. Encoding that result again
    escapes the NULL sentinel into the *text* ``"__DF_SQL_NULL__"`` and escapes a
    user literal twice. A bare ``str`` cannot say which side of the encode it is
    on, so a reader that hands the engine wire cells marks them and
    :func:`cell_to_string` passes a marked cell through unchanged. Raw values (a
    ``None``, a ``Decimal``, a user ``str``) stay unmarked and are encoded once.
    """


def wire_cell(value: Any) -> WireCell:
    """Encode ``value`` once for the wire and mark the result as encoded."""
    return WireCell(cell_to_string(value, preserve_sql_null=True))


def reader_wire_cell(value: Any) -> Any:
    """Mark a cell taken from a connector reader's ``ReadBatch`` as wire.

    ``ReadBatch.rows`` are wire by contract (``cell_to_string(...,
    preserve_sql_null=True)``): NULL is the ``__DF_SQL_NULL__`` sentinel and a
    colliding text is already escaped. A consumer that otherwise sees raw
    driver values (Gate-8's destination sample) must not encode it again.
    """
    if isinstance(value, str) and not isinstance(value, (WireCell, WireLiteral)):
        return WireCell(value)
    return value


def document_bind_value(value: Any) -> Any:
    """A present wire cell as the value a document store keeps.

    The wire escapes text that spells a sentinel (``__DF_ESC____DF_SQL_NULL__``
    is the user's text ``__DF_SQL_NULL__``). SQL binds decode that layer
    (``sql_bind``); the Elasticsearch / MongoDB / DynamoDB writers did not, so
    the escape itself was stored and read back as different text. Callers
    handle absence first (``absent_sql_bind``); this decodes one layer. A
    decoded sentinel spelling stays a :class:`WireLiteral` (a ``str``), so
    later absence checks in the writer keep it as text, not NULL.
    """
    if isinstance(value, str):
        return unescape_wire_text(value)
    return value


def driver_bind_value(value: Any) -> Any:
    """The value a database driver binds: a :class:`WireLiteral` leaves as ``str``.

    The marker must survive binding — writers classify bound rows again (dense
    NULL materialize, COPY ``\\N``, checksums), and a plain ``str`` spelling a
    sentinel would turn back into NULL there. A driver is past every classifier,
    and one that dispatches on the exact class name (Snowflake's converter looks
    up ``_wireliteral_to_snowflake``) refuses the subclass outright (D120b).
    """
    if isinstance(value, WireLiteral):
        return str(value)
    return value


def _wire_unsafe(text: str) -> bool:
    return text.strip().lower() in _NULL_WIRE_LOWER or text.startswith(ESCAPED_WIRE_PREFIX)


def escape_wire_text(text: str) -> str:
    """Escape a wire-unsafe string: collides with a NULL/missing sentinel or
    already carries the escape prefix (recursive, so decode stays total)."""
    if _wire_unsafe(text):
        return f"{ESCAPED_WIRE_PREFIX}{text}"
    return text


def payload_cell_wire(value: Any) -> Any:
    """A parsed file/object payload cell → its transfer-wire form.

    File parsers return what the payload spells; the engine reads every
    source cell as wire, the way SQL readers emit it with
    ``cell_to_string(..., preserve_sql_null=True)``. A CSV/JSON text cell
    that really says ``__DF_SQL_NULL__`` therefore decoded as NULL and a
    ``__DF_ESC__`` prefix was stripped, on both Gate-8 sides alike (D121).
    Only text is escaped: ``None`` already is NULL on the wire, and typed
    values (JSON numbers, Parquet decimals) never collide.
    """
    # A reader's batch cell already crossed the encode (D174): its
    # ``__DF_SQL_NULL__`` is NULL, and escaping it again turned a real NULL
    # into the literal text ``__DF_SQL_NULL__``.
    if isinstance(value, WireCell):
        return value
    # Every wire spelling contains ``__`` — the cheap test keeps the
    # strip/lower in ``escape_wire_text`` off the per-cell hot path.
    if isinstance(value, str) and "__" in value:
        return escape_wire_text(value)
    return value


def payload_record_wire(record: dict[str, Any]) -> dict[str, Any]:
    """Escape a parsed payload record's text cells in place (see above)."""
    for key, value in record.items():
        wired = payload_cell_wire(value)
        if wired is not value:
            record[key] = wired
    return record


def unescape_wire_text(value: Any) -> Any:
    """Strip one ``__DF_ESC__`` layer from a wire string; non-strings pass.

    When the decoded text is itself wire-unsafe — it spells a NULL/missing
    sentinel or carries the escape prefix — the result is a
    :class:`WireLiteral`, so no downstream check re-reads it as absence and no
    later decode strips the literal's own ``__DF_ESC__`` prefix (D121).
    """
    if isinstance(value, WireLiteral):
        return value
    if isinstance(value, str) and value.startswith(ESCAPED_WIRE_PREFIX):
        decoded = value[len(ESCAPED_WIRE_PREFIX) :]
        if _wire_unsafe(decoded):
            return WireLiteral(decoded)
        return decoded
    return value


def wire_cell_text(value: Any) -> str:
    """Re-serialize a cell that may already carry wire encoding.

    ``cell_to_string(..., preserve_sql_null=True)`` is the RAW-source leaf: it
    escapes a bare literal that collides with a sentinel so a database string
    ``'__DF_SQL_NULL__'`` never decodes as NULL. Mid-wire passes (verify,
    quarantine replay, shape materialize, dedupe probes, dry-run samples) see
    cells that already crossed that boundary — a str is either a sentinel
    spelling or an ``__DF_ESC__``-escaped literal, so re-serializing it
    re-escapes the encoded NULL into ``__DF_ESC____DF_SQL_NULL__`` text, which
    typed casts then refuse ("Invalid decimal: '__DF_ESC__...'"). A str input
    is therefore returned verbatim; only typed values get canonical wire.
    """
    if isinstance(value, str):
        return value
    return cell_to_string(value, preserve_sql_null=True)


def is_null_evidence(value: Any) -> bool:
    """True when a cell carries no type evidence (NULL / absent / blank)."""
    if value is None or value is Missing:
        return True
    if isinstance(value, WireLiteral):
        # Decoded sentinel-spelling literal — presence proven upstream.
        return False
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
    if isinstance(value, WireLiteral):
        # Decoded sentinel-spelling literal — a real stored value, not NULL.
        return False
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


def operator_cell(value: Any) -> Any:
    """One wire cell as an operator reads it in a sample or preview.

    SQL NULL / an absent field is ``None`` (JSON null; the UI says NULL) —
    never the internal ``__DF_SQL_NULL__`` token and never ``''``. Text is the
    user's literal: ``''`` and whitespace stay exactly as stored and an
    escaped sentinel-spelling literal is decoded. Typed cells pass through.
    Every sampler that shows rows to a person renders through this (D62).
    """
    if value is None or is_missing_sentinel(value) or is_frame_missing(value):
        return None
    if isinstance(value, str):
        if is_reader_null_cell(value):
            return None
        return str(unescape_wire_text(value))
    return value


def present_cell_text(value: Any) -> str | None:
    """One present cell on the reader wire.

    SQL NULL / Missing / blank are not a unique key, FK, or collision token.
    Typed cells use ``cell_to_string`` so ``True`` and dest ``"true"`` match.
    """
    if is_null_evidence(value) or is_frame_missing(value):
        return None
    if isinstance(value, str):
        # Decoded text is the user's literal — never re-test it: an escaped
        # ``__DF_ESC__`` + sentinel spells ``__DF_SQL_NULL__`` after decode and
        # a second ``is_null_evidence`` here silently turned it into absence
        # (D109 literal → NULL). Real absence was already classified above on
        # the wire spelling.
        return unescape_wire_text(value).strip()
    text = cell_to_string(value, preserve_sql_null=True)
    if is_null_evidence(text):
        return None
    return text


def key_identity_text(value: Any) -> str | None:
    """Exact text identity of one upsert / census key cell.

    Unlike :func:`present_cell_text`, only SQL NULL / Missing is "no key": ``''``
    is a value (a quoted file ``""`` and every database's ``''`` decode to it,
    an unquoted empty file cell is already NULL) and text is never stripped, so
    ``'a'`` and ``'a '`` stay two keys wherever the destination distinguishes
    them (D53, D70). Collation folding belongs to the destination engine.
    """
    if is_reader_null_cell(value) or is_frame_missing(value):
        return None
    if isinstance(value, str):
        return unescape_wire_text(value)
    text = cell_to_string(value, preserve_sql_null=True)
    return None if is_null_evidence(text) else text


def evidence_samples(values: Any, *, limit: int | None = None) -> list[str]:
    """Sample values usable as type evidence.

    A NULL is the *absence* of evidence, never evidence of text. Feeding the
    wire sentinel to inference made an all-NULL ``DECIMAL(7,3)`` column look
    like non-numeric strings, so Map invented a lossy ``<col>_text`` LONGTEXT
    destination for a column whose declared type was perfectly representable.
    """
    out = [evidence_text(v) for v in (values or []) if not is_null_evidence(v)]
    return out[:limit] if limit else out


def evidence_text(value: Any) -> str:
    """One sample cell as the text a type parser reads.

    A driver hands a JSON / JSONB / document cell over already parsed, and
    ``str()`` of a dict is a Python repr (``{'k': 1}``): not JSON, so the sample
    validator failed every cell of a column whose values were fine and demoted
    DataFlow's own embedded-document field on run 2 (D179). Containers carry as
    the JSON document they are.
    """
    if isinstance(value, (dict, list, tuple)):
        try:
            return json_dumps_exact_numbers(value)
        except (TypeError, ValueError):
            pass
    return str(value).strip()


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


def canonical_numeric_key_text(value: Any) -> str | None:
    """Collapse numeric objects to the canonical text of their numeric value."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        decimal_value = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
        if not decimal_value.is_finite():
            return None
        if decimal_value.is_zero():
            return "0"
        normalized = decimal_value.normalize()
    except (DecimalException, ValueError, TypeError):
        return None
    return safe_decimal_text(normalized)


def _is_na(value: Any) -> bool:
    """Detect pandas/numpy missing-like values without importing pandas."""
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def is_frame_missing(value: Any) -> bool:
    """pandas/numpy missing marker (``np.nan``, ``pd.NA``, ``pd.NaT``).

    A DataFrame spells SQL NULL this way, so at a NULL-polarity boundary
    (quarantine wire, present-token checks) it is NULL, not the text ``nan``.
    Text, bytes, bool and :class:`~decimal.Decimal` are never frame markers:
    ``Decimal('NaN')`` is a stored numeric value.
    """
    if value is None or isinstance(value, (str, bytes, bytearray, bool, Decimal)):
        return False
    # ``pd.NA != pd.NA`` is ``pd.NA`` and its truth value raises, so the
    # self-inequality probe alone misses it.
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    return _is_na(value)


def nonfinite_wire_token(value: Any) -> str | None:
    """``NaN`` / ``Infinity`` text for a non-finite float or Decimal.

    Pandas missing values stay on :func:`_is_na`. IEEE NaN must not take that
    path: ``value != value`` turned a source NaN into SQL NULL before the
    writer could quarantine it.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        try:
            if value.is_nan():
                return "NaN"
            if value.is_infinite():
                return "Infinity" if value.copy_abs() == value else "-Infinity"
        except (InvalidOperation, ValueError):
            return None
        return None
    type_name = type(value).__name__
    if isinstance(value, float) or type_name in {"float64", "float32", "float16"}:
        try:
            if value != value:
                return "NaN"
            if value == float("inf"):
                return "Infinity"
            if value == float("-inf"):
                return "-Infinity"
        except (TypeError, ValueError):
            return None
    return None


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


# Integral Decimals wider than this stay exact text rather than becoming a
# JSON number: 1E+1000000 would otherwise expand to a million digits.
_JSON_EXACT_DIGIT_LIMIT = 512


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


_JSON_INSIGNIFICANT_WS = frozenset(" \t\n\r")


def compact_json_text(text: str) -> str:
    """Valid JSON text with only the insignificant whitespace removed.

    A JSON document that arrives as *text* (a PG ``json`` / ``jsonb::text``
    read, a CSV cell, a MySQL JSON read) carries its numbers as the source
    spelled them. Parsing it and dumping it again rewrote them through
    binary64: ``1.10`` became ``1.1``, ``1e2`` became ``100.0`` (D192). Every
    token — numbers, string escapes, key order, duplicate keys — is kept
    byte-for-byte here; only whitespace *between* tokens goes. The caller has
    already validated ``text`` as JSON.
    """
    out: list[str] = []
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
            out.append(ch)
        elif ch not in _JSON_INSIGNIFICANT_WS:
            out.append(ch)
    return "".join(out)


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


def json_document_text(value: Any, *, sort_keys: bool = False) -> str:
    """Exact JSON text for a *document* a driver already decoded.

    ``json.dumps(..., default=json_default)`` renders a ``Decimal`` as a JSON
    **string** — correct for a DECIMAL column travelling on the string wire,
    wrong inside a JSON document: python-oracledb decodes OSON numbers as
    ``Decimal``, so ``{"i": 0}`` was rewritten as ``{"i": "0"}`` and the
    document changed type on the way to the destination. Numbers stay numbers
    with every digit they were stated with (``services.json_polarity``: we do
    not stringify past 2**53 either).
    """
    return _document_text(value, sort_keys=sort_keys)


def _document_text(value: Any, *, sort_keys: bool = False) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return json.dumps(value, allow_nan=False)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"non-finite Decimal refused for JSON write: {value!r}")
        text = safe_decimal_text(value)
        if text is None:
            raise ValueError(f"Decimal not representable as JSON: {value!r}")
        return text
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_document_text(v, sort_keys=sort_keys) for v in value]
        return "[" + ",".join(items) + "]"
    if isinstance(value, Mapping):
        pairs = [(str(k), v) for k, v in value.items()]
        if sort_keys:
            pairs.sort(key=lambda kv: kv[0])
        body = ",".join(
            f"{json.dumps(k, ensure_ascii=False)}:"
            f"{_document_text(v, sort_keys=sort_keys)}"
            for k, v in pairs
        )
        return "{" + body + "}"
    return _document_text(_json_default(value), sort_keys=sort_keys)


def _canonical_number_text(value: Any) -> str | None:
    """One canonical spelling per real number — ``3.0`` ≡ ``3`` ≡ ``3e0``.

    JSON has one numeric domain: engines that store documents as real numbers
    (Snowflake VARIANT, Mongo, jsonb) read ``3.0`` back as ``3``. A semantic
    compare must not call that a mismatch, and must not collapse ``3.0001``
    into ``3`` either — only the spelling is folded, not the value (D73).
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        # Decimal.normalize() folds 3.0/3.00/3e0 to one digit string.
        return format(value.normalize(), "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value.is_integer() and abs(value) < 2**63:
            return str(int(value))
        return repr(value)
    return None


def _scaled_number_text(value: Any) -> str | None:
    """A JSON number as PostgreSQL ``numeric`` stores it: value *and* scale.

    ``1.10`` and ``1.1`` are different stored values on an engine that keeps
    numeric scale (PG ``json``/``jsonb``, a JSON-as-text column): the
    fingerprint must tell them apart (D192). The exponent is folded into
    fixed-point (``1e2`` -> ``100``, ``2.50e1`` -> ``25.0``) and zero loses
    its sign (``-0.0`` -> ``0.0``), exactly as ``jsonb`` renders them.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        if value.is_zero():
            value = value.copy_abs()
        return format(value, "f")
    return _canonical_number_text(value)


def canonical_json_text(document_text: str, *, keep_scale: bool = False) -> str | None:
    """Canonical form of a JSON document for fingerprint/semantic compare.

    Parses exactly (``json_loads_exact`` — no binary64 rounding), then renders
    every number through ``_canonical_number_text`` and sorts object keys, so
    ``{"n": 3.0}`` and ``{"n": 3}`` — the same JSON value — compare equal while
    ``{"n": 3.0001}`` still differs. Returns ``None`` when the text is not a
    JSON document a strict parser accepts.

    ``keep_scale`` is for destinations that store a JSON number with its
    scale (PostgreSQL ``json``/``jsonb``, JSON written to a text column):
    ``{"n": 1.10}`` and ``{"n": 1.1}`` then fingerprint differently, so a
    write that dropped the trailing zero cannot pass Gate-8 (D192). Engines
    that store numbers as doubles (MySQL JSON, BSON, VARIANT) keep the
    value-only compare.
    """
    try:
        if keep_scale:
            parsed = json.loads(document_text, parse_float=Decimal)
        else:
            parsed = json_loads_exact(document_text)
    except Exception:
        return None
    number_text = _scaled_number_text if keep_scale else _canonical_number_text

    def render(v: Any) -> str:
        num = number_text(v)
        if num is not None:
            return num
        if v is None or isinstance(v, JsonNullToken):
            return "null"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, str):
            return json.dumps(v, ensure_ascii=False)
        if isinstance(v, (list, tuple)):
            return "[" + ",".join(render(i) for i in v) + "]"
        if isinstance(v, Mapping):
            return "{" + ",".join(
                f"{json.dumps(str(k), ensure_ascii=False)}:{render(val)}"
                for k, val in sorted(v.items(), key=lambda kv: str(kv[0]))
            ) + "}"
        return render(_json_default(v))

    return render(parsed)


class JsonNullToken:
    """JSON null as engine text ``null``, distinct from SQL NULL.

    ``json.dumps`` has no other way to emit a bare null that is not Python
    ``None``. ``_json_default`` returns ``None`` for this token, which the
    encoder writes as ``null``.
    """

    def __repr__(self) -> str:
        return "null"

    def __str__(self) -> str:
        return "null"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, JsonNullToken)

    def __hash__(self) -> int:
        return hash(JsonNullToken)


JSON_NULL = JsonNullToken()


def _json_default(value: Any) -> Any:
    """Fallback for values that the stdlib json encoder does not understand.

    This function never returns a non-serializable value; it recursively
    resolves numpy/pandas scalars, boto3 Binary, bson ObjectId/Decimal128,
    UUID, bytes, datetime, Decimal, and containers.
    """
    if value is None or isinstance(value, JsonNullToken):
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
    from services.column_case import lookup_row_value

    for h in headers:
        if h in row:
            cell = row[h]
        else:
            cell = lookup_row_value(row, str(h), DF_MISSING_SENTINEL)
            if cell is DF_MISSING_SENTINEL:
                out.append(DF_MISSING_SENTINEL)
                continue
        # ``cell_to_string`` flattens the sentinel to "" so exports never leak
        # it; a reader that already marked the field absent must keep it here.
        if is_missing_sentinel(cell):
            out.append(DF_MISSING_SENTINEL)
            continue
        # Database rows already crossed the reader leaf: re-escaping their
        # NULL sentinel turned every NULL into ``__DF_ESC____DF_SQL_NULL__``
        # text that typed casts refuse (D118).
        out.append(wire_cell_text(cell) if preserve_sql_null else cell_to_string(cell))
    return out


def transform_input_cell(value: Any) -> str | None:
    """Text a write-path transform should see.

    ``None`` stays ``None``. A sparse omit (``DF_MISSING``) stays the sentinel.
    ``cell_to_string`` flattens that sentinel to ``""`` for export wires. Gate-8
    used that flatten, so a Mongo document that simply lacks ``balance`` failed
    as ``Empty value cannot coerce to decimal``. Absence is not an empty cell.
    """
    if value is None:
        return None
    if is_missing_sentinel(value):
        return DF_MISSING_SENTINEL
    return cell_to_string(value)


def operator_binary_cell_text(text: str | None, ddl_type: str = "") -> str | None:
    """Operator spelling of a binary wire cell (X-47).

    Binary values ride the wire as base64, which no database tool shows. For a
    column whose declared type resolves to logical ``binary``, render the
    payload as ``0x`` hex — the same spelling psql/sqlcmd/Workbench use. Other
    columns, and payloads that are not valid base64, pass through unchanged.
    """
    if text is None:
        return None
    from services.type_system import normalize_logical_type

    if normalize_logical_type(ddl_type) != "binary":
        return text
    # A NULL/absent cell must render as NULL, not as hex. JSON read-backs can
    # hand the literal "null" to a binary column, and "null" is valid base64 —
    # it decoded to \x9e\xe9\x65 and displayed as 0x9ee965 (X-131).
    if text.strip() in NULL_WIRE_SENTINELS or text.strip().lower() == "null":
        return "NULL"
    if text == "":
        return "0x"  # zero-length bytes — real value, distinct from NULL
    try:
        return "0x" + base64.b64decode(text, validate=True).hex()
    except Exception:
        return text


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
    if isinstance(value, WireCell):
        return str(value)

    if value is None:
        return SQL_NULL_SENTINEL if preserve_sql_null else ""

    # Sparse CDC / STOP_COLUMN / coerce omit — never serialize the sentinel into
    # CSV/JSON/export wires (would look like a real client value).
    # The missing sentinel IS the string ``__DF_MISSING__`` — escaping it would
    # turn every absent field into literal text on the wire and poison schema
    # inference, so only the fallback path below escapes colliding literals.
    if is_missing_sentinel(value):
        return ""

    # IEEE NaN / Infinity are values. They are not SQL NULL.
    nonfinite = nonfinite_wire_token(value)
    if nonfinite:
        return nonfinite

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
        # Numbers inside a document stay numbers — the string-wire policy for a
        # DECIMAL column does not apply to a JSON document's leaves, so ``{"pi":
        # 3.14}`` must not be rewritten as ``{"pi": "3.14"}`` (D74).
        return json_document_text(value, sort_keys=True)

    # Fallback to str() — never repr(). Escape wire-colliding spellings so the
    # in-memory sentinel stays a proof of SQL NULL, not a literal (D109).
    text = str(value)
    if preserve_sql_null and isinstance(value, str):
        return escape_wire_text(text)
    return text


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
    if value is None or isinstance(value, JsonNullToken):
        # JSON null encodes as bare null. SQL NULL is omitted before sanitize.
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


#: Scalars the BSON encoder carries natively. ``datetime`` is here but ``date``
#: is not — BSON has no date-only wire — and ``bool`` is matched before ``int``
#: by ``isinstance`` anyway, so both are listed for the reader, not the check.
_BSON_NATIVE_SCALARS: tuple[type, ...] = (
    str,
    bool,
    int,
    float,
    bytes,
    bytearray,
    datetime,
    uuid.UUID,
)


def decimal_to_bson(value: Decimal) -> Any:
    """Exact BSON carrier for one ``Decimal``.

    ``Decimal128`` holds 34 significant digits and does not round through
    float. A value wider than that, or one ``Decimal128`` refuses, keeps its
    digits as text. ``None`` is the carrier for NaN and infinity, which BSON
    cannot store and which must not be invented as ``0``.
    """
    text = safe_decimal_text(value)
    if text is None:
        return None
    try:
        from bson.decimal128 import Decimal128
    except ImportError:
        return text
    try:
        return Decimal128(text)
    except (DecimalException, ValueError, TypeError):
        return text


_DECIMAL_CODEC: Any = None


def _decimal_codec() -> Any:
    """One codec instance. ``get_database`` runs on every job write."""
    global _DECIMAL_CODEC
    if _DECIMAL_CODEC is not None:
        return _DECIMAL_CODEC
    from bson.codec_options import TypeCodec
    from bson.decimal128 import Decimal128

    class DecimalCodec(TypeCodec):
        python_type = Decimal
        bson_type = Decimal128

        def transform_python(self, value: Decimal) -> Any:
            return decimal_to_bson(value)

        def transform_bson(self, value: Any) -> Decimal:
            return value.to_decimal()

    _DECIMAL_CODEC = DecimalCodec()
    return _DECIMAL_CODEC


def control_plane_codec_options(base: Any | None = None) -> Any:
    """Codec options that make every control-plane write accept ``Decimal``.

    Profiling, numeric keys, and boolean-to-decimal coercion all produce
    ``Decimal``. A job or approval document that still holds one raises
    ``InvalidDocument: cannot encode object: Decimal('1')`` and the transfer
    never starts. ``Decimal128`` is the carrier on the way out and ``Decimal``
    on the way back — the same rule as :func:`decimal_to_bson`.

    ``base`` is the client's existing options. Only the decimal codec is added,
    so timezone and UUID representation stay whatever the client already used.
    """
    from bson.codec_options import CodecOptions, TypeRegistry

    codec = _decimal_codec()
    if base is None:
        return CodecOptions(type_registry=TypeRegistry([codec]))
    prior = list(getattr(base.type_registry, "codecs", []) or [])
    fallback = getattr(base.type_registry, "fallback_encoder", None)
    merged = TypeRegistry([*prior, codec], fallback)
    return base.with_options(type_registry=merged)


def bson_safe_document(value: Any) -> Any:
    """Rewrite a metadata document into types BSON can encode.

    BSON has no wire for Python ``Decimal``, so a contract/checkpoint document
    that carries one raises ``InvalidDocument`` and the whole write is lost to a
    fallback path. ``Decimal128`` is the exact carrier for it — 34 significant
    digits, no float — and a value wider than that keeps its exact digits as
    text rather than being rounded into the wire.

    Constraint values are taken from real column data, so a document can also
    hold ``date``/``time``/``UUID``/``bytes``/enum members. The ones BSON encodes
    itself are left alone; the rest settle through ``json_default``, the same
    owner the JSON surfaces use, instead of raising ``InvalidDocument``.
    """
    if isinstance(value, Decimal):
        return decimal_to_bson(value)
    if isinstance(value, dict):
        return {str(k): bson_safe_document(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [bson_safe_document(v) for v in value]
    if isinstance(value, set):
        return [bson_safe_document(v) for v in sorted(value, key=repr)]
    if value is None or isinstance(value, _BSON_NATIVE_SCALARS):
        return value
    if type(value).__module__.split(".")[0] == "bson":
        # ObjectId / Decimal128 / Binary / Timestamp: already a BSON carrier.
        return value
    return json_default(value)


def python_document_from_bson(value: Any) -> Any:
    """Inverse of ``bson_safe_document`` for a document read back from Mongo.

    ``Decimal128`` is the BSON carrier for ``Decimal``; a document that keeps it
    past the store boundary cannot be rendered by any JSON surface (pydantic,
    ``json.dumps``) and the whole response is lost to a 500. Every other BSON
    carrier with no Python equivalent settles through ``json_default``.
    """
    if isinstance(value, dict):
        return {str(k): python_document_from_bson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [python_document_from_bson(v) for v in value]
    if _is_decimal128(value):
        return value.to_decimal()
    if value is None or isinstance(value, _BSON_NATIVE_SCALARS):
        return value
    if type(value).__module__.split(".")[0] == "bson":
        return json_default(value)
    return value


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
