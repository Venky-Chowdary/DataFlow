"""Parity Studio: the deterministic difference classifier.

Every difference Gate-8 names between a source row and its destination row is
given exactly one cause by the pure functions below — one function per cause
code, each answering "is this pair explained by *my* cause?" and returning the
cause or ``None``. Nothing here reads a database, calls a model or looks at the
clock: the same pair always gets the same cause, which is what lets a verdict be
signed. The AI layer only words what this module decided.

A cause carries ``explained``: whether the difference is *expected* given what
the run was configured to do, as opposed to merely *diagnosed*. A scale cut is
diagnosed (``numeric_scale``) yet unexplained: the destination lost digits the
source holds. Representation-only differences (the same instant in another
offset, the same text in another Unicode normal form, a trailing zero, the same
binary64) are explained; so are the run's own transforms, quarantined rows, rows
a concurrent source update moved, rows outside this run's population and
differences an operator accepted. Preflight-side root causes live in
``services.root_cause_engine``; this module is the post-load counterpart and
keeps its own stable cause ids.

Values arrive as the transfer wire text Gate-8 and the keyed reads produce
(``str`` or ``None`` for SQL NULL).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal
from typing import Any, Callable, Iterable

# ── stable cause ids ─────────────────────────────────────────────────────────
WHITESPACE = "whitespace"
LETTER_CASE = "letter_case"
UNICODE_NORMALIZATION = "unicode_normalization"
NUMERIC_SCALE = "numeric_scale"
FLOAT_PRECISION = "float_precision"
TIMEZONE_SHIFT = "timezone_shift"
NULL_VS_EMPTY = "null_vs_empty"
TRUNCATION = "truncation"
ENCODING = "encoding"
TRANSFORM_APPLIED = "transform_applied"
QUARANTINED = "quarantined"
ACCEPTED_BY_OPERATOR = "accepted_by_operator"
CONCURRENT_SOURCE_UPDATE = "concurrent_source_update"
OUT_OF_SCOPE_POPULATION = "out_of_scope_population"
UNEXPLAINED = "unexplained"

CAUSE_LABELS: dict[str, str] = {
    WHITESPACE: "Whitespace",
    LETTER_CASE: "Letter case",
    UNICODE_NORMALIZATION: "Unicode normalization",
    NUMERIC_SCALE: "Numeric scale",
    FLOAT_PRECISION: "Float precision",
    TIMEZONE_SHIFT: "Time zone",
    NULL_VS_EMPTY: "NULL versus empty",
    TRUNCATION: "Truncation",
    ENCODING: "Character encoding",
    TRANSFORM_APPLIED: "Your transform",
    QUARANTINED: "Quarantined",
    ACCEPTED_BY_OPERATOR: "Accepted by operator",
    CONCURRENT_SOURCE_UPDATE: "Changed in source during the run",
    OUT_OF_SCOPE_POPULATION: "Not written by this run",
    UNEXPLAINED: "Unexplained",
}

#: Causes a person can only resolve by changing data or accepting the risk.
#: ``widen_column`` is proposed for these when the destination column is narrow.
NARROW_COLUMN_CAUSES = frozenset({NUMERIC_SCALE, TRUNCATION})

_FIXED_WIDTH_CHAR = re.compile(r"^\s*(n?char|character|bpchar|nchar)\b(?!\s*varying)", re.I)
_FLOAT_FAMILY = re.compile(
    r"\b(float|real|double|binary_float|binary_double|float4|float8)\b", re.I
)
_FLOAT32 = re.compile(r"\b(real|float4|binary_float)\b|^\s*float\s*\(\s*([1-9]|1\d|2[0-4])\s*\)", re.I)
_DATETIME = re.compile(r"^\s*\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_ORACLE = frozenset({"oracle", "oracle_db"})
_REPLACEMENT_CHARS = frozenset({"?", "�"})


def cause(code: str, detail: str, *, explained: bool) -> dict[str, Any]:
    """One cause record: ``{code, label, explained, detail}``. ``detail`` never holds a value."""
    return {
        "code": code,
        "label": CAUSE_LABELS.get(code, code),
        "explained": bool(explained),
        "detail": detail,
    }


@dataclass(frozen=True)
class CellContext:
    """Catalog metadata for one column pair; no row values."""

    source_type: str = ""
    dest_type: str = ""
    dest_engine: str = ""
    transforms: tuple[str, ...] = field(default_factory=tuple)


# ── value helpers ────────────────────────────────────────────────────────────


def same_value(left: str | None, right: str | None, ctx: CellContext | None = None) -> bool:
    """Gate-8 equality for two wire cells.

    With the destination column type known this is the sample compare's own
    rule (``fingerprint_for_reconcile`` under that type). Without it, only an
    identical cell or the same exact number counts as equal — never a guess
    that would hide a difference.
    """
    if left == right:
        return True
    if left is None or right is None:
        return False
    ddl = ctx.dest_type if ctx is not None else ""
    if ddl:
        from services.reconciliation import fingerprint_for_reconcile

        engine = ctx.dest_engine if ctx is not None else ""
        return fingerprint_for_reconcile(left, ddl_type=ddl, engine=engine) == (
            fingerprint_for_reconcile(right, ddl_type=ddl, engine=engine)
        )
    a, b = _decimal(left), _decimal(right)
    return a is not None and b is not None and a == b


_NUMBER = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")


def _decimal(text: str | None) -> Decimal | None:
    """A canonical wire number (``-12.50``, ``1E+3``); never a locale guess."""
    if not isinstance(text, str) or not _NUMBER.match(text.strip()):
        return None
    value = Decimal(text.strip())
    return value if value.is_finite() else None


def _scale(text: str) -> int:
    body = text.strip().lower()
    if "e" in body:
        exp = Decimal(body).as_tuple().exponent
        return max(0, -int(exp)) if isinstance(exp, int) else 0
    if "." not in body:
        return 0
    return len(body.split(".", 1)[1])


def _datetime(text: str | None) -> datetime | None:
    if text is None or not _DATETIME.match(text):
        return None
    raw = text.strip().replace("Z", "+00:00").replace("z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _both_text(source: str | None, dest: str | None) -> bool:
    return isinstance(source, str) and isinstance(dest, str) and source != dest


# ── one function per cause code ──────────────────────────────────────────────


def null_vs_empty(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """One side is SQL NULL, the other the empty string.

    Explained only where the destination engine stores ``''`` as NULL (Oracle).
    """
    if source == "" and dest is None:
        oracle = ctx.dest_engine.lower() in _ORACLE
        return cause(
            NULL_VS_EMPTY,
            "The source holds an empty string; the destination holds NULL"
            + (" (Oracle stores an empty string as NULL)." if oracle else "."),
            explained=oracle,
        )
    if source is None and dest == "":
        return cause(
            NULL_VS_EMPTY, "The source holds NULL; the destination holds an empty string.",
            explained=False,
        )
    return None


def whitespace(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """Equal once runs of whitespace are collapsed and the ends trimmed.

    Explained only for fixed-width CHAR padding on the destination.
    """
    if not _both_text(source, dest):
        return None
    if " ".join(source.split()) != " ".join(dest.split()):
        return None
    padded = bool(_FIXED_WIDTH_CHAR.match(ctx.dest_type or "")) and (
        dest.rstrip(" ") == source.rstrip(" ") and len(dest) >= len(source)
    )
    if padded:
        return cause(WHITESPACE, "The fixed-width CHAR column pads with trailing spaces.", explained=True)
    if source.strip() == dest.strip():
        where = "Trailing" if source.lstrip() == dest.lstrip() else (
            "Leading" if source.rstrip() == dest.rstrip() else "Leading and trailing"
        )
        return cause(WHITESPACE, f"{where} whitespace differs.", explained=False)
    return cause(WHITESPACE, "Whitespace inside the value differs.", explained=False)


def letter_case(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """Equal ignoring case (Unicode case folding), and not merely another normal form."""
    if not _both_text(source, dest) or source.casefold() != dest.casefold():
        return None
    if unicodedata.normalize("NFC", source) == unicodedata.normalize("NFC", dest):
        return None
    return cause(LETTER_CASE, "Only the letter case differs.", explained=False)


def unicode_normalization(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """Equal under Unicode normalization.

    NFC-equal text is the same text (explained); NFKC-only equality folds
    compatibility characters (``ﬁ`` to ``fi``) and changes meaning (unexplained).
    """
    if not _both_text(source, dest):
        return None
    if unicodedata.normalize("NFC", source) == unicodedata.normalize("NFC", dest):
        return cause(
            UNICODE_NORMALIZATION,
            "The same text in another Unicode normal form (composed versus decomposed).",
            explained=True,
        )
    if unicodedata.normalize("NFKC", source) == unicodedata.normalize("NFKC", dest):
        return cause(
            UNICODE_NORMALIZATION,
            "Compatibility characters were folded (NFKC); the stored text changed.",
            explained=False,
        )
    return None


def encoding(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """UTF-8 read as a single-byte code page (mojibake), or characters replaced by ``?``."""
    if not _both_text(source, dest):
        return None
    for codec in ("latin-1", "cp1252"):
        try:
            if source.encode("utf-8").decode(codec) == dest:
                return cause(ENCODING, f"UTF-8 bytes were read as {codec}.", explained=False)
        except UnicodeDecodeError:
            pass
        try:
            if dest.encode("utf-8").decode(codec) == source:
                return cause(ENCODING, f"UTF-8 bytes were read as {codec} on the source side.", explained=False)
        except UnicodeDecodeError:
            pass
    if len(source) == len(dest):
        diffs = [(s, d) for s, d in zip(source, dest) if s != d]
        if diffs and all(ord(s) > 127 and d in _REPLACEMENT_CHARS for s, d in diffs):
            return cause(
                ENCODING,
                f"{len(diffs)} non-ASCII character(s) were replaced: the destination "
                "column's code page cannot hold them.",
                explained=False,
            )
    return None


def truncation(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """The destination holds a strict prefix of the source text (by characters or bytes)."""
    if not _both_text(source, dest) or not dest or len(dest) >= len(source):
        return None
    if _decimal(source) is not None and _decimal(dest) is not None:
        return None  # a number cut short is a scale question, not a text cut
    if source.startswith(dest):
        return cause(TRUNCATION, f"Cut at {len(dest)} characters.", explained=False)
    cut = source.encode("utf-8")[: len(dest.encode("utf-8"))].decode("utf-8", "ignore")
    if cut == dest:
        return cause(TRUNCATION, f"Cut at {len(dest.encode('utf-8'))} bytes.", explained=False)
    return None


def timezone_shift(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """Both sides are timestamps that differ only by a UTC offset."""
    a, b = _datetime(source), _datetime(dest)
    if a is None or b is None or source == dest:
        return None
    if a.tzinfo is not None and b.tzinfo is not None:
        if a == b:
            return cause(TIMEZONE_SHIFT, "The same instant written with another UTC offset.", explained=True)
        if a.replace(tzinfo=None) == b.replace(tzinfo=None):
            return cause(
                TIMEZONE_SHIFT, "The wall-clock time was kept but the UTC offset changed.",
                explained=False,
            )
        return None
    if a.tzinfo is not None or b.tzinfo is not None:
        aware, naive = (a, b) if a.tzinfo is not None else (b, a)
        offset = aware.utcoffset() or timedelta(0)
        if naive == (aware - offset).replace(tzinfo=None):
            return cause(
                TIMEZONE_SHIFT, "Stored as UTC without its offset (the same instant).",
                explained=True,
            )
        if naive == aware.replace(tzinfo=None):
            return cause(
                TIMEZONE_SHIFT, "The UTC offset was dropped; read as UTC the instant moved.",
                explained=False,
            )
        return None
    delta = abs(a - b)
    if delta and delta <= timedelta(hours=14) and delta % timedelta(minutes=15) == timedelta(0):
        hours = delta.total_seconds() / 3600
        return cause(
            TIMEZONE_SHIFT, f"Shifted by {hours:g} hour(s), the size of a UTC offset.",
            explained=False,
        )
    return None


def numeric_scale(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """The destination holds the source number rounded or cut to fewer decimal places.

    The same number with trailing zeros (``10.10`` vs ``10.1``) is explained.
    """
    a, b = _decimal(source), _decimal(dest)
    if a is None or b is None:
        return None
    if a == b:
        if source != dest:
            return cause(NUMERIC_SCALE, "The same number written with a different scale.", explained=True)
        return None
    s_scale, d_scale = _scale(source), _scale(dest)
    if d_scale >= s_scale or float(a) == float(b):
        return None  # the same binary64 is a float question
    step = Decimal(1).scaleb(-d_scale)
    for rounding in (ROUND_HALF_UP, ROUND_HALF_EVEN, ROUND_DOWN):
        if a.quantize(step, rounding=rounding) == b:
            how = "cut" if rounding == ROUND_DOWN and a.quantize(step, ROUND_HALF_UP) != b else "rounded"
            return cause(
                NUMERIC_SCALE,
                f"Scale {how} from {s_scale} to {d_scale} decimal place(s).",
                explained=False,
            )
    return None


def float_precision(source: str | None, dest: str | None, ctx: CellContext) -> dict | None:
    """The two numbers agree to binary floating-point precision.

    Explained when they are the same binary64, or within binary32 precision on
    a single-precision destination column; otherwise digits were lost.
    """
    a, b = _decimal(source), _decimal(dest)
    if a is None or b is None or a == b:
        return None
    fa, fb = float(a), float(b)
    if fa == fb:
        return cause(FLOAT_PRECISION, "Both values are the same binary64 number.", explained=True)
    scale = max(abs(fa), abs(fb))
    rel = abs(fa - fb) / scale if scale else 0.0
    if rel > 2.0 ** -23:
        return None
    single = bool(_FLOAT32.search(ctx.dest_type or ""))
    floaty = bool(_FLOAT_FAMILY.search(ctx.dest_type or ""))
    if rel <= 2.0 ** -51 and floaty:
        return cause(FLOAT_PRECISION, "Rounded to double precision by the float column.", explained=True)
    if single:
        return cause(FLOAT_PRECISION, "Rounded to single precision by the REAL column.", explained=True)
    return cause(
        FLOAT_PRECISION, "Digits were lost to floating-point rounding on a non-float column.",
        explained=False,
    )


def transform_applied(
    source_raw: str | None, dest: str | None, replayed: str | None, ctx: CellContext
) -> dict | None:
    """The run's own transform, replayed on the raw source value, yields the destination value."""
    if same_value(source_raw, dest, ctx) or not same_value(replayed, dest, ctx):
        return None
    names = ", ".join(t for t in ctx.transforms if t) or "the run's transform"
    return cause(TRANSFORM_APPLIED, f"Produced by {names}, as configured.", explained=True)


def quarantined(key: str | None, quarantined_keys: Iterable[str]) -> dict | None:
    """The row was held out to quarantine on purpose."""
    if key is None or key not in set(quarantined_keys):
        return None
    return cause(QUARANTINED, "The row failed validation and was held in quarantine.", explained=True)


def concurrent_source_update(key: str | None, drifted_keys: Iterable[str]) -> dict | None:
    """The write witness proved the source changed this key after the run wrote it."""
    if key is None or key not in set(drifted_keys):
        return None
    return cause(
        CONCURRENT_SOURCE_UPDATE,
        "The source changed after this run wrote the row; the destination holds what was written.",
        explained=True,
    )


def out_of_scope_population(kind: str, *, outside_this_run: bool) -> dict | None:
    """A destination-only row this run never wrote (append or upsert into an occupied table)."""
    if kind != "extra_in_destination" or not outside_this_run:
        return None
    return cause(
        OUT_OF_SCOPE_POPULATION,
        "The destination already held this row; this run did not write it.",
        explained=True,
    )


def accepted_by_operator(acceptance: dict[str, Any] | None) -> dict | None:
    """An approver accepted this difference, with a note."""
    if not acceptance:
        return None
    who = str(acceptance.get("by") or "an approver")
    note = str(acceptance.get("note") or "").strip()
    return cause(
        ACCEPTED_BY_OPERATOR,
        f"Accepted by {who}" + (f": {note}" if note else "."),
        explained=True,
    )


def unexplained(kind: str) -> dict[str, Any]:
    detail = {
        "missing_in_destination": "The row is in the source but not in the destination.",
        "extra_in_destination": "The row is in the destination but not in the source.",
        "duplicate_key": "The destination holds more than one row for a key.",
    }.get(kind, "The values differ and no rule explains how.")
    return cause(UNEXPLAINED, detail, explained=False)


#: Value rules in the order they are tried; the first match names the cause.
VALUE_RULES: tuple[Callable[[str | None, str | None, CellContext], dict | None], ...] = (
    null_vs_empty,
    whitespace,
    unicode_normalization,
    letter_case,
    encoding,
    truncation,
    timezone_shift,
    numeric_scale,
    float_precision,
)


def classify_value(
    *,
    expected: str | None,
    dest: str | None,
    ctx: CellContext,
    source_raw: str | None = None,
    raw_known: bool = False,
    key: str | None = None,
    quarantined_keys: Iterable[str] = (),
    drifted_keys: Iterable[str] = (),
) -> dict[str, Any] | None:
    """Cause of one differing cell, or ``None`` when the cells are equal.

    ``expected`` is the value the run should have written (the source value with
    the run's transforms replayed); ``source_raw`` the untransformed source
    value when it was re-read (``raw_known``).
    """
    held = quarantined(key, quarantined_keys) or concurrent_source_update(key, drifted_keys)
    if held:
        return held
    if same_value(expected, dest, ctx):
        if raw_known and ctx.transforms:
            return transform_applied(source_raw, dest, expected, ctx)
        return None
    for rule in VALUE_RULES:
        found = rule(expected, dest, ctx)
        if found:
            return found
    return unexplained("value")


def classify_row(
    kind: str,
    *,
    key: str | None,
    quarantined_keys: Iterable[str] = (),
    drifted_keys: Iterable[str] = (),
    outside_this_run: bool = False,
) -> dict[str, Any]:
    """Cause of a missing, extra or duplicate row."""
    return (
        quarantined(key, quarantined_keys) if kind == "missing_in_destination" else None
    ) or (
        concurrent_source_update(key, drifted_keys) if kind != "duplicate_key" else None
    ) or out_of_scope_population(kind, outside_this_run=outside_this_run) or unexplained(kind)
