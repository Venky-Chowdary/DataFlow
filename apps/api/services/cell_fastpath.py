"""Per-column fast paths for the Gate-8 cell canonicalizers.

``fingerprint_for_reconcile`` and ``normalize_cell`` run once per cell per
pass — the write-pass digest, the source re-read and the destination read-back
each call them for every cell of every row — and each call walks the whole
generic dispatch (sentinels, carried kinds, locale pins, bind routing, carrier
quantization, the declared-type rules) to arrive, for an ordinary typed cell,
at ``str(int)`` or an ISO wall clock. On a mixed-type table that dispatch was
most of the transfer's CPU.

This module answers the same question for the ordinary cases only, and says
"not mine" (``MISS``) for everything else, which then takes the full path:

* The column's *plan* is compiled once per ``(ddl_type, engine)`` from the very
  predicates the slow path consults (``bind_route``, ``split_carried``,
  ``instant_date_carrier``, ``is_text_family``, ``_text_fold_plan``, the
  float-width and decimal-exactness rules). A column any of them treats
  specially — a carried kind, a collation fold, a single-precision float, a
  document engine, an engine this module was not proven on — gets no plan.
* Within a plan, a value is handled only when its exact Python type and, for
  text, its exact shape are ones whose slow-path result is a fixed rendering:
  ``int`` → ``str``; a finite ``Decimal`` within the column scale →
  ``_canonicalize_number``; a nonzero finite ``float`` → the same; a naive
  ``datetime`` / ``date`` → the wall-clock text; ``bool`` → ``1``/``0``;
  ordinary text → itself. Every other value — sentinels, ``__DF_ESC__``
  escapes, JSON-looking text, clock fractions, negative zero, NaN, aware or
  out-of-range instants, sub-scale decimals, subclasses — is a ``MISS``.
* Before a plan is used it is self-checked: a fixed probe set goes through
  both paths and the plan is dropped for good on any disagreement.

``tests/test_cell_fastpath_equivalence.py`` compares both paths over large
generated corpora on every plan kind, engine, transform and locale pin.
``DATAFLOW_CELL_FASTPATH=0`` switches the fast paths off.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, time as _time
from decimal import Decimal
from typing import Any

MISS: Any = object()

#: Engines whose plans the equivalence suite proves.
_ENGINES = frozenset({"postgresql", "postgres", "mysql", "mariadb"})
#: Transforms that leave the cell to the bind (``fingerprint_for_reconcile``).
_IDENTITY = frozenset({"", "none", "identity", "passthrough"})

_DECIMAL_DDL_RE = re.compile(r"^(?:NUMERIC|DECIMAL)\s*\(\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)$", re.I)
_TIMESTAMP_DDL_RE = re.compile(
    r"^(TIMESTAMP|DATETIME)(?:\s*\(\s*(\d)\s*\))?(\s+WITHOUT\s+TIME\s+ZONE)?$", re.I
)
_INT_TEXT_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,17})\Z")
_DECIMAL_TEXT_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,30})(?:\.([0-9]{1,30}))?\Z")
_ISO_DATE_TEXT_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
_ISO_STAMP_TEXT_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\Z")
_BOOL_TEXT = {"true": "1", "false": "0", "1": "1", "0": "0"}


def enabled() -> bool:
    """``DATAWRAP_CELL_FASTPATH`` / ``DATAFLOW_CELL_FASTPATH`` = 0 turns the fast paths off."""
    from services.brand_env import getenv_brand

    raw = (getenv_brand("CELL_FASTPATH", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


@dataclass(frozen=True)
class CellPlan:
    """What one destination column's cells canonicalize as."""

    kind: str  # int | decimal | double | timestamp | date | bool | text
    scale: int = 0
    precision: int = 0
    instant_digits: int = 6
    exact: bool = True
    #: Integer bind: the range ``coerce_integer_wire`` holds the column to, or
    #: ``None`` when it checks none (BIGINT). ``bind_int`` False: no int fast bind.
    int_lo: int | None = None
    int_hi: int | None = None
    bind_int: bool = False
    #: Boolean bind: MySQL stores 1 / 0.
    as_int: bool = False


_plans: dict[tuple[str, str], CellPlan | None] = {}
_plans_lock = threading.RLock()
_building = threading.local()
_MAX_PLANS = 4096


def column_plan(ddl_type: str, engine: str) -> CellPlan | None:
    """The plan for ``ddl_type`` on ``engine``, or ``None`` (always the full path)."""
    key = (ddl_type, engine)
    try:
        return _plans[key]
    except KeyError:
        pass
    except TypeError:
        return None
    if getattr(_building, "active", False):
        # The self-check below runs the full path, which asks for plans again:
        # it must see the full path all the way down, and must not cache that.
        return None
    with _plans_lock:
        if key in _plans:
            return _plans[key]
        _building.active = True
        try:
            plan = _compile(str(ddl_type or ""), str(engine or ""))
        finally:
            _building.active = False
        if len(_plans) >= _MAX_PLANS:
            _plans.clear()
        _plans[key] = plan
        return plan


def _compile(ddl_type: str, engine: str) -> CellPlan | None:
    if not enabled():
        return None
    try:
        plan = _build_plan(ddl_type, engine)
    except Exception:  # noqa: BLE001 - an unknown column never gets a fast path
        return None
    if plan is None:
        return None
    try:
        return plan if _self_check(plan, ddl_type, engine) else None
    except Exception:  # noqa: BLE001 - a probe the full path cannot take: no plan
        return None


def reset_plans() -> None:
    """Forget compiled plans (tests that change the environment switch)."""
    with _plans_lock:
        _plans.clear()


def _build_plan(ddl_type: str, engine: str) -> CellPlan | None:
    eng = engine.strip().lower()
    ddl = ddl_type.strip()
    if eng not in _ENGINES or not ddl or ddl != ddl_type:
        return None
    from connectors.sql_bind_route import bind_route
    from services.document_instant import instant_date_carrier
    from services.fingerprint_rules import is_exact_decimal_carrier, is_text_family, split_carried
    from services.numeric_fit import float_mantissa_bits
    from services.reconciliation import (
        _INTERVAL_DDL_RE,
        _single_precision_carrier,
        _text_fold_plan,
        destination_empty_string_is_null,
    )
    from services.type_system import normalize_logical_type

    if split_carried(ddl)[1] or instant_date_carrier(eng, ddl) != ddl:
        return None
    if _INTERVAL_DDL_RE.search(ddl) or _single_precision_carrier(ddl, eng):
        return None
    route = bind_route(ddl, eng)
    logical = normalize_logical_type(ddl)
    upper = ddl.upper()
    if route.kind == "integer" and logical == "integer" and not route.is_temporal_candidate:
        if upper.startswith("TINYINT") or "UNSIGNED" in upper:
            return None
        base = upper.split("(", 1)[0].strip()
        lo: int | None = None
        hi: int | None = None
        bind_int = True
        if base in {"BIGINT", "INT8"}:
            pass  # coerce_integer_wire checks no range for these
        elif base == "SMALLINT":
            lo, hi = -32768, 32767
        elif base == "MEDIUMINT":
            lo, hi = -8388608, 8388607
        elif base == "INT4":
            lo, hi = -2147483648, 2147483647
        elif base in {"INT", "INTEGER"}:
            from services.numeric_fit import integer_storage_bounds

            bounds = integer_storage_bounds(base, dest_db=eng)
            if bounds is None:
                bind_int = False
            else:
                lo, hi = bounds
        else:
            bind_int = False
        return CellPlan("int", exact=is_exact_decimal_carrier(ddl), int_lo=lo, int_hi=hi,
                        bind_int=bind_int)
    if route.kind == "decimal" and logical == "decimal":
        m = _DECIMAL_DDL_RE.match(ddl)
        if not m:
            return None
        precision, scale = int(m.group(1)), int(m.group(2))
        if not 0 <= scale <= precision or precision < 2:
            return None  # NUMBER(1)/DECIMAL(1,0) is a boolean carrier
        return CellPlan("decimal", scale=scale, precision=precision,
                        exact=is_exact_decimal_carrier(ddl))
    if route.kind == "float" and float_mantissa_bits(ddl, dest_db=eng) == 53:
        return CellPlan("double", exact=is_exact_decimal_carrier(ddl))
    if route.is_temporal_candidate and route.kind == "passthrough":
        if upper == "DATE" and logical == "date":
            return CellPlan("date")
        m = _TIMESTAMP_DDL_RE.match(ddl)
        if m and logical == "datetime":
            base = m.group(1).upper()
            if eng in {"mysql", "mariadb"} and base != "DATETIME":
                return None  # MySQL TIMESTAMP converts through the session zone
            if eng in {"postgresql", "postgres"} and base != "TIMESTAMP":
                return None
            digits = int(m.group(2)) if m.group(2) is not None else (6 if base == "TIMESTAMP" else 0)
            return CellPlan("timestamp", instant_digits=digits)
        return None
    if route.kind == "boolean" and upper == "BOOLEAN":
        return CellPlan("bool", as_int=bool(route.as_int))
    if (
        route.kind == "passthrough"
        and not route.is_temporal_candidate
        and not route.oracle_empty_is_null
        and is_text_family(ddl)
        and logical in {"string", "text"}
        and not destination_empty_string_is_null(eng)
    ):
        fold = _text_fold_plan(ddl)
        if (
            fold.keep_trailing_spaces
            and not fold.rstrip_blank_pad
            and not (fold.fold_width or fold.fold_kana or fold.fold_variation)
            and not (fold.fold_accent or fold.casefold or fold.uuid_carrier)
        ):
            return CellPlan("text")
    return None


def _plain_text(value: str) -> bool:
    """Text whose fingerprint is itself under a plain character plan."""
    from services.reconciliation import _CLOCK_FRACTION_RE

    if "__" in value or "\x00" in value:
        return False  # sentinels, escapes, Missing, the NUL text escape
    if value.lstrip()[:1] in ("{", "["):
        return False  # a JSON document compares as the document
    return not _CLOCK_FRACTION_RE.match(value)


def normalize_typed(value: Any, plan: CellPlan) -> Any:
    """``normalize_cell(value, ddl_type, engine)`` for a planned column, or ``MISS``."""
    from services.reconciliation import _NULL_SENTINEL

    if value is None:
        return _NULL_SENTINEL
    t = type(value)
    kind = plan.kind
    if kind == "text":
        if t is str and _plain_text(value):
            return value
        return MISS
    if kind == "bool":
        if t is bool:
            return "1" if value else "0"
        if t is int and value in (0, 1):
            return str(value)
        return MISS
    if t is int and kind in ("int", "decimal", "double"):
        return str(value)
    if kind == "decimal":
        if t is Decimal and value.is_finite():
            return _canonical_decimal(value, plan)
        return MISS
    if kind == "double":
        if t is float and value == value and value not in (_INF, _NINF) and value != 0.0:
            from services.reconciliation import _NAN_TOKEN, _canonicalize_number

            return _canonicalize_number(value) or _NAN_TOKEN
        return MISS
    if kind == "timestamp":
        if t is datetime and value.tzinfo is None and 1000 <= value.year <= 9999:
            from services.reconciliation import _wall_clock_text

            return _wall_clock_text(value, 0)
        return MISS
    if kind == "date":
        if t is date and 1000 <= value.year <= 9999:
            return datetime.combine(value, _time.min).strftime("%Y-%m-%dT%H:%M:%S")
        return MISS
    return MISS


_INF = float("inf")
_NINF = float("-inf")


def _canonical_decimal(value: Decimal, plan: CellPlan) -> Any:
    from services.reconciliation import _NAN_TOKEN, _canonicalize_number

    return _canonicalize_number(value, exact=plan.exact) or _NAN_TOKEN


def fingerprint(value: Any, plan: CellPlan, tname: str, source_locale: str = "") -> Any:
    """``fingerprint_for_reconcile`` for a planned column, or ``MISS``."""
    kind = plan.kind
    if value is None:
        if tname in _IDENTITY:
            from services.reconciliation import _NULL_SENTINEL

            return _NULL_SENTINEL
        return MISS
    if type(value) is not str:
        if tname not in _IDENTITY:
            return MISS
        if kind == "decimal" and type(value) is Decimal:
            # The bind quantizes to the column scale; inside it, that is the value.
            if not value.is_finite():
                return MISS
            exp = value.as_tuple().exponent
            if not isinstance(exp, int) or exp < -plan.scale or exp > 0:
                return MISS
            if value.adjusted() >= plan.precision - plan.scale:
                return MISS
        elif kind == "timestamp" and type(value) is datetime:
            if plan.instant_digits < 6 and value.microsecond:
                return MISS
        elif kind == "int" and type(value) is int:
            if not -(2**63) <= value < 2**63:
                return MISS
        elif kind == "double" and type(value) is not float:
            return MISS  # the bind turns an int into the double it stores
        elif kind == "decimal" and type(value) is int:
            if not -(10 ** (plan.precision - plan.scale)) < value < 10 ** (plan.precision - plan.scale):
                return MISS
        return normalize_typed(value, plan)
    # Text cells: the shapes every transform and locale pin read the same way.
    if kind == "text":
        if tname in _IDENTITY and _plain_text(value):
            return value
        return MISS
    if kind == "int":
        if tname in _IDENTITY or tname == "integer":
            if _INT_TEXT_RE.match(value) and value != "-0":
                return value
        return MISS
    if kind == "decimal":
        if tname in _IDENTITY or tname == "decimal":
            m = _DECIMAL_TEXT_RE.match(value)
            if m is None:
                return MISS
            frac = m.group(1) or ""
            if len(frac) == 3 or len(frac) > plan.scale:
                return MISS
            if len(value.lstrip("-").split(".")[0]) > plan.precision - plan.scale:
                return MISS
            return _canonical_decimal(Decimal(value), plan)
        return MISS
    if kind == "double":
        if tname in _IDENTITY:
            # The fingerprint binds under ``source_locale or WIRE``.
            number = _plain_float_text(value, wire=not source_locale)
            if number is None or number == 0.0:
                return MISS
            from services.reconciliation import _NAN_TOKEN, _canonicalize_number

            return _canonicalize_number(number) or _NAN_TOKEN
        return MISS
    if kind == "date":
        if (tname in _IDENTITY or tname == "date") and _ISO_DATE_TEXT_RE.match(value):
            try:
                day = date.fromisoformat(value)
            except ValueError:
                return MISS
            if day.year < 1000:
                return MISS
            return value + "T00:00:00"
        return MISS
    if kind == "timestamp":
        if (tname in _IDENTITY or tname == "datetime") and _ISO_STAMP_TEXT_RE.match(value):
            try:
                stamp = datetime.fromisoformat(value)
            except ValueError:
                return MISS
            if stamp.year < 1000:
                return MISS
            return value
        return MISS
    if kind == "bool":
        if tname in _IDENTITY or tname == "boolean":
            return _BOOL_TEXT.get(value, MISS)
        return MISS
    return MISS


_ISO_STAMP_FRACTION_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?\Z"
)
_FLOAT_LO = 1e-300
_FLOAT_HI = 1e300


_PLAIN_FLOAT_TEXT_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,16})\.([0-9]{1,17})\Z")


def _wire_locale() -> bool:
    """True when the number locale pinned on this context is WIRE."""
    from services.transform_engine import NUMBER_LOCALE_WIRE, _NUMBER_LOCALE_VAR

    return _NUMBER_LOCALE_VAR.get() == NUMBER_LOCALE_WIRE


def _plain_float_text(value: str, *, wire: bool = False) -> float | None:
    """The double a plain ``d.ddd`` text spells, when the locale reads it so.

    Exactly three fraction digits is a grouping under some locale pins
    (``1.234``) — unless the text is read as WIRE, where a dot is always the
    decimal point. Anything ``repr`` would not print back is not this shape.
    """
    m = _PLAIN_FLOAT_TEXT_RE.match(value)
    if m is None or (len(m.group(1)) == 3 and not wire):
        return None
    number = float(value)
    if repr(number) != value:
        return None
    return number


def bind(value: Any, plan: CellPlan) -> Any:
    """``normalize_sql_bind_value(value, ddl_type, engine=...)`` for a planned column, or ``MISS``.

    Only cells the full bind returns unchanged (or parses from one exact text
    shape) are answered; every refusal, range edge and coercion is the full
    path's.
    """
    if value is None:
        return None
    t = type(value)
    kind = plan.kind
    if kind == "text":
        if t is str and "__" not in value:
            return value
        return MISS
    if kind == "int":
        if t is int and plan.bind_int:
            if plan.int_lo is not None and not plan.int_lo <= value <= plan.int_hi:
                return MISS
            return value
        return MISS
    if kind == "decimal":
        if t is Decimal and value.is_finite():
            exp = value.as_tuple().exponent
            if isinstance(exp, int) and -plan.scale <= exp <= 0 and (
                value.is_zero() or value.adjusted() < plan.precision - plan.scale
            ):
                return value
        return MISS
    if kind == "double":
        if t is float:
            number = value
        elif t is str:
            number = _plain_float_text(value, wire=_wire_locale())
            if number is None:
                return MISS
        else:
            return MISS
        if number != number or not _FLOAT_LO < abs(number) < _FLOAT_HI:
            return MISS
        return number
    if kind == "timestamp":
        if t is datetime:
            if value.tzinfo is None and 1000 <= value.year <= 9999:
                return value
            return MISS
        if t is str and _ISO_STAMP_FRACTION_RE.match(value):
            if "." in value and plan.instant_digits < 6:
                return MISS
            try:
                stamp = datetime.fromisoformat(value)
            except ValueError:
                return MISS
            if 1000 <= stamp.year <= 9999:
                return stamp
        return MISS
    if kind == "date":
        if t is date:
            return value if 1000 <= value.year <= 9999 else MISS
        if t is str and _ISO_DATE_TEXT_RE.match(value):
            try:
                day = date.fromisoformat(value)
            except ValueError:
                return MISS
            if 1000 <= day.year <= 9999:
                return day
        return MISS
    if kind == "bool":
        if t is bool:
            return (1 if value else 0) if plan.as_int else value
        return MISS
    return MISS


def fast_float(value: Any, ddl_type: str, engine: str) -> Any:
    """``coerce_float_wire(value, ddl_type=..., engine=...)`` for a planned DOUBLE, or ``MISS``."""
    if value is None or getattr(_off, "on", False):
        return MISS
    plan = column_plan(ddl_type, engine)
    if plan is None or plan.kind != "double":
        return MISS
    return bind(value, plan)


def fast_bind(value: Any, ddl_type: str, engine: str) -> Any:
    """Entry point used by ``normalize_sql_bind_value``."""
    if not ddl_type or getattr(_off, "on", False):
        return MISS
    plan = column_plan(ddl_type, engine)
    if plan is None:
        return MISS
    return bind(value, plan)


# --------------------------------------------------------------------------
# untyped cells (no declared type: the verification ladder's L2 cells)

_UNTYPED_DECIMAL_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,14})\.([0-9]{1,12})\Z")


def normalize_untyped(value: Any) -> Any:
    """``normalize_cell(value)`` (no declared type) for a plain text cell, or ``MISS``.

    ISO date / ``T`` timestamp text (no zone, no fraction), a plain integer,
    a plain decimal (not three fraction digits, at most 15 significant
    digits), ``true`` / ``false``. Everything else takes the full path.
    """
    if type(value) is not str or getattr(_off, "on", False):
        return MISS
    if _INT_TEXT_RE.match(value):
        return MISS if value == "-0" else value
    m = _UNTYPED_DECIMAL_RE.match(value)
    if m is not None:
        if len(m.group(1)) == 3 or len(value.lstrip("-").replace(".", "").lstrip("0")) > 15:
            return MISS
        from services.reconciliation import _canonicalize_number

        out = _canonicalize_number(Decimal(value), exact=False)
        return MISS if out is None else out
    if _ISO_STAMP_TEXT_RE.match(value):
        try:
            stamp = datetime.fromisoformat(value)
        except ValueError:
            return MISS
        return value if 1000 <= stamp.year <= 9999 else MISS
    if _ISO_DATE_TEXT_RE.match(value):
        try:
            day = date.fromisoformat(value)
        except ValueError:
            return MISS
        return value + "T00:00:00" if 1000 <= day.year <= 9999 else MISS
    if value == "true":
        return "1"
    if value == "false":
        return "0"
    text = value.strip()
    if text[:1].isalpha() and "__" not in value:
        # Text that starts with a letter is never a clock, a date, JSON or a
        # negative zero; it is itself unless it spells a boolean, a non-finite
        # number or a currency amount, which the full path folds.
        from services.fingerprint_rules import nonfinite_token
        from services.reconciliation import _looks_like_numeric_wire
        from services.transform_engine import _STRICT_BOOL_FALSE, _STRICT_BOOL_TRUE

        lowered = text.lower()
        if (
            lowered in _STRICT_BOOL_TRUE
            or lowered in _STRICT_BOOL_FALSE
            or nonfinite_token(text) is not None
            or _looks_like_numeric_wire(text)
        ):
            return MISS
        return text
    return MISS


_NOT_A_NUMBER_CHARS = frozenset(":;/\\#@!?\"'<>=[]{}|^~`&*")
_LETTER_NOT_E_RE = re.compile(r"[A-DF-Za-df-z]")
_SPACE_RE = re.compile(r"\s")
_DASHED_DIGITS_RE = re.compile(r"[0-9]+-[0-9-]*[0-9]\Z")


def text_is_never_a_decimal(text: str) -> bool:
    """True when ``decimal_wire_value(text)`` is ``None`` whatever the locale.

    Only ASCII text, where NFKC changes nothing: a character no number
    spelling, marker or separator rule removes (``:``, ``{``, ``/`` …); a
    letter other than an exponent ``e`` in text with no whitespace (a
    currency code is only stripped as a whitespace-delimited word, and a bare
    one leaves nothing to parse); digits joined by inner dashes (a date).
    """
    if getattr(_off, "on", False):
        return False
    if not text.isascii():
        return _non_ascii_never_a_decimal(text)
    if not _NOT_A_NUMBER_CHARS.isdisjoint(text):
        return True
    if _SPACE_RE.search(text):
        # A currency code / regional mark is stripped only as a whole
        # whitespace-delimited word; a word with any other letter stays.
        return any(
            _LETTER_NOT_E_RE.search(word) and word.upper() not in _removable_words()
            for word in text.split()
        )
    if _LETTER_NOT_E_RE.search(text):
        return True
    return _DASHED_DIGITS_RE.match(text) is not None


_REMOVABLE: frozenset[str] | None = None


def _removable_words() -> frozenset[str]:
    """Upper-cased words ``_CURRENCY_RE`` may strip as a whole word (and a superset)."""
    global _REMOVABLE
    if _REMOVABLE is None:
        from services.transform_engine import _CURRENCY_CODES

        words = {w.upper() for w in _CURRENCY_CODES.split("|")}
        words |= {"US$", "A$", "C$", "HK$", "NZ$", "S$", "MX$", "R$", "CA$", "AU$", "SG$",
                  "RS", "RS.", "RP", "RM", "KR", "FT", "LEI", "CHF", "FR", "FR.", "SFR"}
        _REMOVABLE = frozenset(words)
    return _REMOVABLE


def _non_ascii_never_a_decimal(text: str) -> bool:
    """Non-ASCII text without whitespace holding a letter nothing strips.

    After NFKC (what the parser applies first) a letter that is not an
    exponent ``e`` and not one of the currency symbols (some symbols are
    letters, e.g. ``Ξ``) survives every rule; a currency code is only removed
    as a whitespace-delimited word, so without whitespace it can only be the
    whole text, which then leaves nothing to parse.
    """
    import unicodedata

    from services.transform_engine import _CURRENCY_SYMBOLS

    folded = unicodedata.normalize("NFKC", text)
    if any(ch.isspace() for ch in folded):
        return False
    return any(ch.isalpha() and ch not in "eE" and ch not in _CURRENCY_SYMBOLS for ch in folded)


# --------------------------------------------------------------------------
# write-path transform and temporal fit

_TRANSFORM_KINDS = frozenset({"integer", "decimal", "date", "datetime", "boolean"})
_BOOL_TRANSFORM = {"true": True, "false": False, "1": True, "0": False}
_DECIMAL_TRANSFORM_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,17})(?:\.([0-9]{1,10}))?\Z")


def fast_transform(raw: Any, transform: Any) -> Any:
    """``apply_transform(raw, transform)`` for a plain text cell, or ``MISS``.

    Only the canonical cast names and only text shapes every locale pin,
    strict mode and boolean policy reads one way: a plain integer, a plain
    decimal (never exactly three fraction digits), an ISO date, an ISO
    ``T`` timestamp without zone or fraction, and ``true``/``false``/``1``/``0``.
    """
    if type(raw) is not str or getattr(_off, "on", False):
        return MISS
    if transform is None or transform == "" or transform == "none":
        # The identity cast carries the text; only a sentinel (every one is
        # ``__``-delimited) is read as something else.
        if "__" in raw:
            return MISS
        return raw, None
    if type(transform) is not str or transform not in _TRANSFORM_KINDS:
        return MISS
    if transform == "integer":
        if _INT_TEXT_RE.match(raw) and raw != "-0":
            return int(raw), None
        return MISS
    if transform == "decimal":
        m = _DECIMAL_TRANSFORM_RE.match(raw)
        if m is None or len(m.group(1) or "") == 3:
            return MISS
        return Decimal(raw), None
    if transform == "boolean":
        flag = _BOOL_TRANSFORM.get(raw)
        return MISS if flag is None else (flag, None)
    if transform == "date":
        if _ISO_DATE_TEXT_RE.match(raw):
            try:
                day = date.fromisoformat(raw)
            except ValueError:
                return MISS
            if day.year >= 1000:
                return raw, None
        return MISS
    if _ISO_STAMP_TEXT_RE.match(raw):
        try:
            stamp = datetime.fromisoformat(raw)
        except ValueError:
            return MISS
        if stamp.year >= 1000:
            return raw, None
    return MISS


def temporal_fits(raw: Any, typ: str, dest_db: str) -> bool:
    """True when ``_temporal_unfit_reason`` has no reason for this cell.

    A plain ISO date / ``T`` timestamp text (or naive ``date`` / ``datetime``)
    into a planned DATE / TIMESTAMP column with no zone and no fraction past
    the column's digits. ``False`` means "ask the full check", never "unfit".
    """
    if getattr(_off, "on", False):
        return False
    plan = column_plan(typ, dest_db)
    if plan is None:
        return False
    t = type(raw)
    if plan.kind == "timestamp":
        if t is datetime:
            return (raw.tzinfo is None and 1000 <= raw.year <= 9999
                    and (raw.microsecond == 0 or plan.instant_digits == 6))
        if t is str and _ISO_STAMP_TEXT_RE.match(raw):
            try:
                stamp = datetime.fromisoformat(raw)
            except ValueError:
                return False
            return 1000 <= stamp.year <= 9999
        return False
    if plan.kind == "date":
        if t is date:
            return 1000 <= raw.year <= 9999
        if t is str and _ISO_DATE_TEXT_RE.match(raw):
            try:
                day = date.fromisoformat(raw)
            except ValueError:
                return False
            return 1000 <= day.year <= 9999
    return False


# --------------------------------------------------------------------------
# self-check

_PROBES: dict[str, tuple[Any, ...]] = {
    "int": (0, 1, -1, 42, -42081, 2**31, -(2**31), 2**62, "0", "7", "-15", "123456789"),
    "decimal": (Decimal("0"), Decimal("1"), Decimal("-1.5"), Decimal("20.9458"), Decimal("100.00"),
                Decimal("-0.0001"), 7, "0", "12.5", "-3.25", "1000"),
    "double": (0.125, -2.5, 1e-7, 123456.789, 1.0, 3, "0.375", "-1.25", "1e-07"),
    "timestamp": (datetime(2020, 1, 1, 0, 0, 37), datetime(1999, 12, 31, 23, 59, 59),
                  datetime(2024, 2, 29, 12, 0, 0, 500000), "2020-01-01T00:01:14",
                  "2024-02-29T23:59:59"),
    "date": (date(2000, 1, 2), date(2024, 2, 29), "2000-01-02", "2024-12-31"),
    "bool": (True, False, 0, 1, "true", "false", "1", "0"),
    "text": ("", "label-1", "Δοκιμή", "🙂 emoji", "tab\tand\nnewline\\back", " lead", "trail ",
             "note 5d41402abc4b2a76b9719d911017c592", "2024-01-01", "12.50", "true"),
}
_TRANSFORMS = {
    "int": (None, "none", "integer"),
    "decimal": (None, "none", "decimal"),
    "double": (None, "none"),
    "timestamp": (None, "none", "datetime"),
    "date": (None, "none", "date"),
    "bool": (None, "none", "boolean"),
    "text": (None, "none"),
}
self_check_failures: list[dict[str, Any]] = []


def _self_check(plan: CellPlan, ddl_type: str, engine: str) -> bool:
    """Both paths on the probe set must agree, or the plan is not used."""
    from services.reconciliation import _fingerprint_for_reconcile_full, _normalize_cell_full

    for value in _PROBES.get(plan.kind, ()):
        for transform in _TRANSFORMS.get(plan.kind, (None,)):
            tname = (transform or "").strip().lower()
            fast = fingerprint(value, plan, tname)
            if fast is not MISS:
                slow = _fingerprint_for_reconcile_full(
                    value, ddl_type=ddl_type, engine=engine, transform=transform
                )
                if slow != fast:
                    self_check_failures.append({"ddl": ddl_type, "engine": engine,
                                                "value": repr(value), "transform": transform,
                                                "fast": fast, "slow": slow})
                    return False
        fast = bind(value, plan)
        if fast is not MISS:
            from connectors.sql_bind import _normalize_sql_bind_value_full

            try:
                slow = _normalize_sql_bind_value_full(value, ddl_type, engine=engine)
            except Exception:  # noqa: BLE001 - the full bind refused what the fast one took
                slow = MISS
            if slow != fast or type(slow) is not type(fast):
                self_check_failures.append({"ddl": ddl_type, "engine": engine,
                                            "value": repr(value), "fast": repr(fast),
                                            "slow": repr(slow), "fn": "bind"})
                return False
        if type(value) is not str or plan.kind == "text":
            fast = normalize_typed(value, plan)
            if fast is not MISS:
                slow = _normalize_cell_full(value, ddl_type=ddl_type, engine=engine)
                if slow != fast:
                    self_check_failures.append({"ddl": ddl_type, "engine": engine,
                                                "value": repr(value), "fast": fast,
                                                "slow": slow, "fn": "normalize_cell"})
                    return False
    return True


_off = threading.local()


class full_path_only:
    """Context manager: this thread takes the full path (equivalence tests)."""

    def __enter__(self) -> "full_path_only":
        self._prev = getattr(_off, "on", False)
        _off.on = True
        return self

    def __exit__(self, *exc: Any) -> None:
        _off.on = self._prev


def fast_fingerprint(
    value: Any, ddl_type: str, engine: str, transform: str | None, source_locale: str = ""
) -> Any:
    """Entry point used by ``fingerprint_for_reconcile``."""
    if not ddl_type or getattr(_off, "on", False):
        return MISS
    plan = column_plan(ddl_type, engine)
    if plan is None:
        return MISS
    return fingerprint(value, plan, (transform or "").strip().lower(), source_locale)


def fast_normalize(value: Any, ddl_type: str, engine: str) -> Any:
    """Entry point used by ``normalize_cell``."""
    if not ddl_type or getattr(_off, "on", False):
        return MISS
    plan = column_plan(ddl_type, engine)
    if plan is None:
        return MISS
    return normalize_typed(value, plan)


def install(full_fingerprint: Any, full_normalize: Any) -> tuple[Any, Any]:
    """Front ``fingerprint_for_reconcile`` / ``normalize_cell`` with the fast paths.

    ``services.reconciliation`` keeps its full functions as
    ``_fingerprint_for_reconcile_full`` / ``_normalize_cell_full`` and binds
    these wrappers under the public names, so every caller — the module's own
    full path included — reaches the fast path first and the full path for
    anything it does not answer.
    """
    import functools

    @functools.wraps(full_fingerprint)
    def fingerprint_for_reconcile(
        value: Any,
        *,
        ddl_type: str = "",
        engine: str = "",
        transform: str | None = None,
        source_locale: str = "",
    ) -> str:
        if ddl_type:
            fast = fast_fingerprint(value, ddl_type, engine, transform, source_locale)
            if fast is not MISS:
                return fast
        return full_fingerprint(value, ddl_type=ddl_type, engine=engine,
                                transform=transform, source_locale=source_locale)

    @functools.wraps(full_normalize)
    def normalize_cell(value: Any, *, ddl_type: str = "", engine: str = "") -> str:
        if ddl_type:
            fast = fast_normalize(value, ddl_type, engine)
        elif not engine:
            fast = normalize_untyped(value)
        else:
            fast = MISS
        if fast is not MISS:
            return fast
        return full_normalize(value, ddl_type=ddl_type, engine=engine)

    return fingerprint_for_reconcile, normalize_cell


__all__: list[str] = [
    "MISS", "CellPlan", "column_plan", "fast_bind", "fast_fingerprint", "fast_normalize",
    "fast_transform", "install", "normalize_untyped", "temporal_fits",
    "text_is_never_a_decimal",
]
