"""CE1: the canonical encoding both sides of Parity at Scale hash.

This module is the **Python reference** of CE1 (spec:
``docs/reference/parity-canonical-encoding.md``). Every SQL dialect must
produce byte-identical field encodings for the values it can hold; the SQLite
dialect calls this module directly through UDFs, and the PostgreSQL dialect is
checked against it value by value on a live engine.

Soundness invariant (the rule that must never break)
----------------------------------------------------
``CE1(a) == CE1(b)``  ⇒  ``fingerprint_v2(a) == fingerprint_v2(b)``

for every value pair under one column plan (the destination's declared type
and engine), where ``fingerprint_v2`` is Gate-8's
:func:`services.reconciliation.fingerprint_for_reconcile`. CE1 may be
*stricter* than the fingerprint (two values the fingerprint folds together can
encode apart: a case-insensitive collation, a JSON key order, a SQLite
separator spelling); the cell judge then rules on those few rows with the
fingerprint itself. CE1 may never be *looser*: a looser encoding would let a
scan call two populations equal that Gate-8 would fail. The property tests in
``tests/test_parity_scale_ce1_soundness.py`` enforce the invariant against the
real fingerprint and, for approved lossy casts, against the real writer.

Field grammar (a row is the concatenation of its fields in plan order)::

    field := 'N'                                  SQL NULL (out of band)
           | 'V' <utf8 byte length> ':' <text>    inline canonical text
           | 'D' <64 lowercase hex>               SHA-256 of the canonical bytes
           | 'X' <class> ':' <byte length> ':' <text>
                                                  a value whose storage class is
                                                  not the column kind's own

Every alternative is self-delimiting (a tag, then a length or a fixed width),
so no value can forge a field boundary and the row encoding is injective.
``X`` exists for engines without enforced column types (SQLite) and for
values the plan did not expect: it keeps the storage class in the bytes, so
an integer ``5`` in a text column never encodes like the text ``'5'`` — a
stricter-than-fingerprint choice, sound by construction.
"""

from __future__ import annotations

import hashlib
import math
import re
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Final

#: Version of the encoding. Evidence carries it; two scans are only comparable
#: when both ran the same version.
CE_VERSION: Final[str] = "CE1"
#: Scan algorithm identifier recorded in evidence.
ALGORITHM: Final[str] = "PSD1"
#: Hash construction recorded in evidence: SHA-256, two 60-bit words, summed.
HASH_SPEC: Final[str] = "sha256/2x60/sum"

MASK60: Final[int] = (1 << 60) - 1

#: Column kinds CE1 encodes in-engine. ``native`` is the same-engine rendering
#: (§1.6.1 of the design): only legal when both sides are one dialect.
KINDS: Final[tuple[str, ...]] = (
    "integer",
    "boolean",
    "decimal",
    "text",
    "char",
    "date",
    "timestamp",
    "timestamptz",
    "time",
    "uuid",
    "binary",
    "native",
)

#: Width threshold: text declared wider than this many characters is encoded
#: by digest (``D``) so a row stays bounded (64 chars ≤ 256 UTF-8 bytes).
INLINE_MAX_CHARS: Final[int] = 64

_EPOCH_DATE: Final[date] = date(1970, 1, 1)
_EPOCH_NAIVE: Final[datetime] = datetime(1970, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
_UUID_RE: Final[re.Pattern[str]] = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_INFINITY_TOKENS: Final[dict[str, str]] = {
    "infinity": "+inf",
    "+infinity": "+inf",
    "-infinity": "-inf",
}


class _Held:
    """Sentinel: the write contract held this row out of the destination."""

    _instance: "_Held | None" = None

    def __new__(cls) -> "_Held":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "HELD"


#: Returned by :func:`project_source_value` when the writer would not have
#: written the row (quarantine / skip): the row leaves the expected population.
HELD: Final[_Held] = _Held()


#: The classes a value meets against ``NUMERIC(p,s)``; each non-fitting class
#: has its own writer action, read from the real writer.
FIT_CLASSES: Final[tuple[str, ...]] = ("scale", "int", "both", "nan", "inf")


@dataclass(frozen=True)
class DecimalFit:
    """The approved lossy decimal contract the writer ran (fit-or-contract).

    ``actions`` maps each non-fitting class — ``scale`` (only the fraction is
    too long), ``int`` (only the integer part is too long), ``both``, ``nan``,
    ``inf`` — to what the writer did with such a value: ``keep`` (written
    unchanged), ``null`` (the cell written as NULL) or ``hold`` (the row held
    out). The actions are read from the real writer at plan time
    (:func:`services.parity_scale.plan.probe_decimal_fit`), never assumed, and
    the property tests check the classes replay the writer value by value. The
    writer never rounds, so neither does CE1.
    """

    precision: int
    scale: int
    dest_db: str
    actions: dict[str, str] = field(default_factory=dict)

    def action(self, outcome: str) -> str:
        return "keep" if outcome == "fits" else self.actions.get(outcome, "hold")

    def spec(self) -> str:
        acts = ";".join(f"{k}={self.actions.get(k, 'hold')}" for k in FIT_CLASSES)
        return f"fit({self.precision},{self.scale});{acts}"


@dataclass(frozen=True)
class FieldSpec:
    """How one column encodes: the part of a column plan CE1 needs."""

    kind: str
    #: ``inline`` or ``digest``.
    width: str = "inline"
    #: Destination declared type and engine (the fingerprint basis).
    dest_type: str = ""
    dest_engine: str = ""
    #: Comparison precision of instants/times (fractional digits, 0..6).
    digits: int = 6
    #: The destination stores ``''`` as NULL (Oracle): ``''`` encodes ``N``.
    empty_is_null: bool = False
    #: Source-side decimal contract; ``None`` for an exact carry.
    decimal_fit: DecimalFit | None = None


# ── field primitives ────────────────────────────────────────────────────────


def null_field() -> bytes:
    return b"N"


def inline_field(text: str) -> bytes:
    raw = text.encode("utf-8", "surrogatepass")
    return b"V" + str(len(raw)).encode("ascii") + b":" + raw


def digest_field(payload: bytes) -> bytes:
    return b"D" + hashlib.sha256(payload).hexdigest().encode("ascii")


def foreign_field(storage_class: str, text: str) -> bytes:
    raw = text.encode("utf-8", "surrogatepass")
    return (
        b"X"
        + storage_class.encode("ascii")
        + b":"
        + str(len(raw)).encode("ascii")
        + b":"
        + raw
    )


def foreign(value: Any) -> bytes:
    """``X`` field for a value whose storage class is not the column kind's own.

    Each storage class renders injectively within itself: integers in decimal,
    doubles by ``repr`` (shortest round trip, so ``-0.0`` and ``0.0`` stay
    apart), text as itself, bytes as hex, Decimal by its exact tuple.
    """
    if isinstance(value, bool):
        return foreign_field("bool", "1" if value else "0")
    if isinstance(value, int):
        return foreign_field("integer", str(value))
    if isinstance(value, float):
        return foreign_field("real", repr(value))
    if isinstance(value, str):
        return foreign_field("text", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return foreign_field("blob", bytes(value).hex())
    if isinstance(value, Decimal):
        return foreign_field("decimal", repr(value.as_tuple()))
    if isinstance(value, datetime):
        return foreign_field("datetime", value.isoformat())
    if isinstance(value, date):
        return foreign_field("date", value.isoformat())
    if isinstance(value, time):
        return foreign_field("time", value.isoformat())
    if isinstance(value, timedelta):
        return foreign_field("timedelta", repr((value.days, value.seconds, value.microseconds)))
    return foreign_field("other:" + type(value).__name__, repr(value))


def _sqlite_engine(engine: str) -> bool:
    return str(engine or "").strip().lower() in {"sqlite", "sqlite3"}


# ── canonical texts per kind ────────────────────────────────────────────────


def decimal_text(value: Decimal) -> str:
    """Minimal exact decimal text: no exponent, no trailing fractional zeros.

    ``12.3400`` → ``12.34``; ``1E+2`` → ``100``; ``-0.00`` → ``0``;
    ``NaN`` → ``NaN``; ``±Infinity`` → ``Infinity`` / ``-Infinity``. The same
    text PostgreSQL's ``trim_scale(x)::text`` renders.
    """
    if value.is_nan():
        return "NaN"
    if value.is_infinite():
        return "-Infinity" if value.is_signed() else "Infinity"
    sign, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    text_digits = "".join(str(d) for d in digits)
    if exponent >= 0:
        int_part = (text_digits + "0" * exponent).lstrip("0") or "0"
        frac_part = ""
    else:
        cut = len(text_digits) + exponent
        if cut > 0:
            int_part = text_digits[:cut].lstrip("0") or "0"
            frac_part = text_digits[cut:]
        else:
            int_part = "0"
            frac_part = "0" * (-cut) + text_digits
        frac_part = frac_part.rstrip("0")
    if int_part == "0" and not frac_part:
        return "0"
    body = int_part + ("." + frac_part if frac_part else "")
    return ("-" if sign else "") + body


def _epoch_microseconds(value: datetime) -> int:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    delta = value - _EPOCH_NAIVE
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _units_field(microseconds: int, digits: int, tick: int = 0) -> bytes:
    """Integer epoch units at ``digits`` fractional digits, never a fold.

    A value carrying finer digits than the comparison precision cannot be
    rendered at that precision without losing them, so it is encoded at
    microseconds under an ``X`` tag instead: stricter, never looser.
    """
    if digits >= 7:
        # 100 ns units (SQL Server datetime2(7)); ``tick`` is the seventh digit.
        return inline_field(str(microseconds * 10 + tick))
    if tick:
        # A 100 ns tick is finer than the comparison precision.
        return foreign_field("tick", f"{microseconds * 10 + tick}")
    step = 10 ** (6 - digits)
    if microseconds % step:
        return foreign_field("us", str(microseconds))
    return inline_field(str(microseconds // step))


def project_instant(value: Any, spec: FieldSpec) -> Any:
    """Replay the destination carrier's instant quantisation (the real function)."""
    from services.carrier_instant import quantize_instant_for_carrier

    if not spec.dest_type or not spec.dest_engine:
        return value
    return quantize_instant_for_carrier(value, ddl_type=spec.dest_type, engine=spec.dest_engine)


def decimal_fit_outcome(value: Any, fit: DecimalFit) -> str:
    """``fits`` or the class of a value that does not fit ``NUMERIC(p,s)``.

    ``fits`` is the writer's own :func:`connectors.writer_common.fits_decimal`
    for finite values. Integer digits and scale are the writer's own
    ``write_int_digits_and_scale`` (trailing zeros are not scale; zero counts
    one integer digit).
    """
    from connectors.writer_common import fits_decimal
    from services.decimal_observe import write_int_digits_and_scale

    d = value if isinstance(value, Decimal) else Decimal(int(value))
    if d.is_nan():
        return "nan"
    if d.is_infinite():
        return "inf"
    if fits_decimal(d, fit.precision, fit.scale, dest_db=fit.dest_db):
        return "fits"
    int_digits, cell_scale = write_int_digits_and_scale(d)
    scale_over = cell_scale > fit.scale
    int_over = int_digits > max(fit.precision - fit.scale, 0)
    if scale_over and int_over:
        return "both"
    return "scale" if scale_over else "int"


def project_source_value(value: Any, spec: FieldSpec) -> Any:
    """The value the write contract stored for a source ``value`` (or HELD).

    Projections replay the writer; they never round a decimal and never invent
    a value the writer did not write.
    """
    if value is None:
        return None
    if (
        spec.decimal_fit is not None
        and spec.kind == "decimal"
        and isinstance(value, (Decimal, int))
        and not isinstance(value, bool)
    ):
        # Only exact numbers are projected: the writer's classes are proven for
        # them. Another class (a SQLite REAL) is encoded raw under ``X``.
        action = spec.decimal_fit.action(decimal_fit_outcome(value, spec.decimal_fit))
        if action == "keep":
            return value
        return None if action == "null" else HELD
    if _is_own_instant(value, spec.kind):
        # Only the kind's own class is projected; a value of another class is
        # encoded raw under an ``X`` tag, never quantised into a match.
        return project_instant(value, spec)
    return value


def _is_own_instant(value: Any, kind: str) -> bool:
    if kind in {"timestamp", "timestamptz"}:
        return isinstance(value, datetime) and (value.tzinfo is not None) == (kind == "timestamptz")
    if kind == "time":
        return isinstance(value, time) and value.tzinfo is None
    return False


def encode_value(value: Any, spec: FieldSpec) -> bytes:
    """CE1 field for a value *as stored* (destination side, or after projection)."""
    if value is None:
        return null_field()
    if type(value).__name__ == "JsonNullToken":
        # D144: a file's JSON null is SQL NULL on a scalar column and the JSON
        # value ``null`` on a document column.
        from services.file_json_null import json_null_reads_as_sql_null

        if json_null_reads_as_sql_null(value, spec.dest_type, spec.dest_engine):
            return null_field()
        return inline_field("null")
    kind = spec.kind
    if kind == "integer":
        if isinstance(value, int):
            return inline_field(str(int(value)))
        return foreign(value)
    if kind == "boolean":
        if isinstance(value, bool):
            return inline_field("1" if value else "0")
        if isinstance(value, int):
            # A boolean carrier holding an integer keeps the integer: 2 stays 2.
            return inline_field(str(value))
        return foreign(value)
    if kind == "decimal":
        if isinstance(value, Decimal):
            return inline_field(decimal_text(value))
        if isinstance(value, int) and not isinstance(value, bool):
            return inline_field(str(value))
        if isinstance(value, float) and _sqlite_engine(spec.dest_engine) and math.isfinite(value):
            # SQLite has no exact decimal: NUMERIC(p,s) stores REAL. The
            # double's shortest round-trip text is the number Gate-8 reads
            # (the bind parses str(float)), and distinct doubles have distinct
            # texts, so a REAL encodes as that decimal: equal to an exact
            # source value exactly when the fingerprint calls them equal.
            return inline_field(decimal_text(Decimal(repr(value))))
        return foreign(value)
    if kind in {"text", "char"}:
        if not isinstance(value, str):
            return foreign(value)
        if spec.empty_is_null and value == "":
            return null_field()
        text = value.rstrip(" ") if kind == "char" else value
        if spec.width == "digest":
            return digest_field(text.encode("utf-8", "surrogatepass"))
        return inline_field(text)
    if kind == "date":
        if isinstance(value, datetime) or not isinstance(value, date):
            if isinstance(value, str) and value.strip().lower() in _INFINITY_TOKENS:
                return inline_field(_INFINITY_TOKENS[value.strip().lower()])
            return foreign(value)
        return inline_field(str((value - _EPOCH_DATE).days))
    if kind in {"timestamp", "timestamptz"}:
        if isinstance(value, datetime) and (value.tzinfo is not None) == (kind == "timestamptz"):
            # A naive carrier holds naive instants and an aware one aware
            # instants; the other kind is not this column's own (Gate-8 binds
            # an aware value into a naive column by its wall clock, not UTC).
            from connectors.sqlserver_tick import drop_tick, tick_of

            tick = tick_of(value)
            return _units_field(_epoch_microseconds(drop_tick(value)), spec.digits, tick)
        if isinstance(value, str) and value.strip().lower() in _INFINITY_TOKENS:
            return inline_field(_INFINITY_TOKENS[value.strip().lower()])
        return foreign(value)
    if kind == "time":
        if isinstance(value, time) and value.tzinfo is None:
            micro = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000
            return _units_field(micro + value.microsecond, spec.digits)
        return foreign(value)
    if kind == "uuid":
        if isinstance(value, _uuid.UUID):
            return inline_field(str(value))
        if isinstance(value, str) and _UUID_RE.match(value):
            return inline_field(value.lower())
        return foreign(value)
    if kind == "binary":
        if isinstance(value, (bytes, bytearray, memoryview)):
            return digest_field(bytes(value))
        return foreign(value)
    if kind == "native":
        # Same-engine rendering. In Python the native rendering of a driver
        # value is its storage class rendered exactly (the ``X`` forms), which
        # is injective per class: equal encodings mean equal values.
        return foreign(value)
    raise ValueError(f"CE1 has no kind {kind!r}")


def encode_source_value(value: Any, spec: FieldSpec) -> bytes | _Held:
    """CE1 field for a *source* value: projection first, then the encoding."""
    projected = project_source_value(value, spec)
    if projected is HELD:
        return HELD
    return encode_value(projected, spec)


# ── rows, keys and words ────────────────────────────────────────────────────


def encode_row(
    values: list[Any] | tuple[Any, ...],
    specs: list[FieldSpec] | tuple[FieldSpec, ...],
    *,
    source: bool,
) -> bytes | _Held:
    """Concatenated CE1 fields of one row (``HELD`` when the writer held it out)."""
    parts: list[bytes] = []
    for value, spec in zip(values, specs, strict=True):
        field = encode_source_value(value, spec) if source else encode_value(value, spec)
        if field is HELD:
            return HELD
        parts.append(field)  # type: ignore[arg-type]
    return b"".join(parts)


def row_hash(row_bytes: bytes) -> bytes:
    """``H = SHA-256(CE1 row bytes)``."""
    return hashlib.sha256(row_bytes).digest()


def words(h: bytes) -> tuple[int, int]:
    """``(a, b)``: the low 60 bits of the first and second big-endian 64-bit words."""
    return (
        int.from_bytes(h[0:8], "big") & MASK60,
        int.from_bytes(h[8:16], "big") & MASK60,
    )


def key_bucket(key_hash: bytes, level: int) -> int:
    """Bucket (0..1023) of a keyed row at ``level``: bits of the key hash's first word.

    Level ``i`` reads bits ``63-10i .. 54-10i`` of the key hash's first 64-bit
    word, so each level splits a bucket 1024 ways and the levels are disjoint.
    """
    top = int.from_bytes(key_hash[0:8], "big")
    return (top >> (54 - 10 * level)) & 1023


def row_bucket(h: bytes, level: int) -> int:
    """Bucket of a keyless row: bits of the row hash's *third* word.

    The words ``a`` and ``b`` use the first two words; bucketing on them would
    fix some of their bits inside a bucket and weaken the false-match bound.
    """
    top = int.from_bytes(h[16:24], "big")
    return (top >> (54 - 10 * level)) & 1023


def hash_words(row_bytes: bytes) -> tuple[bytes, int, int]:
    h = row_hash(row_bytes)
    a, b = words(h)
    return h, a, b
