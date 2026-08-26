"""Deterministic transform execution for dry-run and write paths."""

from __future__ import annotations

import base64
import contextvars
import functools
import hashlib
import hmac
import json
import os
from services.brand_env import getenv_brand
import re
import unicodedata
import uuid as uuid_lib
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, Overflow
from collections.abc import Iterable
from typing import Any

from services.pii_guard import mask as pii_mask
from services.semantic_types import (
    SemanticType,
    detect_semantic_type,
    normalize_value_for_target,
)
from services.value_serializer import json_default, json_loads_exact

#: An unadorned ASCII integer — no sign but ``-``, no separator, no exponent.
_PLAIN_ASCII_INT_RE = re.compile(r"^-?[0-9]{1,38}$")
#: Already-canonical ISO calendar date, which needs validation but no pattern search.
_ISO_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

_MONTH_NAME_RE = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
_DATE_LIKE_RE = re.compile(
    r"\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|"
    r"\d{8}|"
    r"\d{1,2}\s+" + _MONTH_NAME_RE + r"\s+\d{2,4}|"
    r"" + _MONTH_NAME_RE + r"\s+\d{1,2},?\s+\d{2,4}",
    re.IGNORECASE,
)

DATE_PATTERNS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y%m%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%m/%d/%y",
    "%d/%m/%y",
    "%m-%d-%Y",
    "%d-%m-%Y",
    "%m-%d-%y",
    "%d-%m-%y",
    "%m.%d.%Y",
    "%d.%m.%Y",
    "%m.%d.%y",
    "%d.%m.%y",
    "%d-%b-%Y",
    "%d-%b-%y",
    "%d-%B-%Y",
    "%d-%B-%y",
    "%b %d, %Y",
    "%b %d, %y",
    "%B %d, %Y",
    "%B %d, %y",
    "%d %b %Y",
    "%d %b %y",
    "%d %B %Y",
    "%d %B %y",
    "%Y-%b-%d",
    "%y-%b-%d",
)

# Additional patterns that represent a full date but may contain time.
# Used only for the "date" transform so it can parse a datetime string and
# return the date portion without widening schema inference to classify
# datetime values as plain DATE.
DATE_WITH_TIME_PATTERNS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%y %H:%M:%S",
    "%d/%m/%y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%m-%d-%y %H:%M:%S",
    "%d-%m-%y %H:%M:%S",
    "%m.%d.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%m.%d.%y %H:%M:%S",
    "%d.%m.%y %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %I:%M:%S %p",
    "%Y-%m-%d %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m-%d-%Y %I:%M %p",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%y %H:%M:%S",
    "%d-%B-%Y %H:%M:%S",
    "%d-%B-%y %H:%M:%S",
)

DATETIME_PATTERNS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%y %H:%M:%S",
    "%d/%m/%y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%m-%d-%y %H:%M:%S",
    "%d-%m-%y %H:%M:%S",
    "%m.%d.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%m.%d.%y %H:%M:%S",
    "%d.%m.%y %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %I:%M:%S %p",
    "%Y-%m-%d %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m-%d-%Y %I:%M %p",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%y %H:%M:%S",
    "%d-%B-%Y %H:%M:%S",
    "%d-%B-%y %H:%M:%S",
)

# Values that are unambiguously empty/missing for non-string types.
NULL_SENTINELS = frozenset({
    "null", "none", "nil", "undefined", "n/a", "na", "nan", "",
    "-", "--", "—", "empty", "blank", "missing", "not available",
    "not applicable", "not_applicable",
    # Dynamo / SQL explicit NULL — distinct from missing attr / empty string.
    "__df_ddb_null__",
    "__df_sql_null__",
    # NOTE: __df_missing__ is NOT a null — writers must omit the field (sparse CDC).
})

# Transform-classification sets used by the per-cell coercion path. These are
# module-level because that path runs once per cell: rebuilding three frozensets
# inside it cost a set construction per cell, which is billions of avoidable
# allocations on a wide, large table.

#: Identity / text transforms where an empty string is a real value, not a null.
_KEEP_EMPTY_TRANSFORMS = frozenset({
    "none", "identity", "passthrough", "string", "varchar", "text",
    "upper", "lower", "trim", "trim_id",
    "strip_controls", "normalize_unicode",
})

#: NaN / ±Infinity are not SQL null for JSON/vector — the typed parsers reject
#: them rather than inventing JSON null or an empty embedding.
_NONFINITE_TOKENS = frozenset({"nan", "infinity", "+infinity", "-infinity"})

#: Transforms that parse into a concrete type, so null sentinels mean null.
#: Includes currency/percentage — empty must not silent-NULL into numeric sinks.
_TYPED_TRANSFORMS = frozenset({
    "decimal", "integer", "boolean", "date", "datetime", "time",
    "json", "uuid", "binary", "vector",
    "currency", "percentage",
})

# Per-request date locale for ambiguous MDY/DMY parsing.  The engine and
# preflight service set this via :func:`set_active_date_locale` so every
# coerce / dry-run / preview path resolves dates with the operator-chosen
# locale without threading the value through every helper signature.
_DATE_LOCALE_VAR: contextvars.ContextVar[str] = contextvars.ContextVar("date_locale", default="")


@functools.lru_cache(maxsize=1)
def _env_date_locale() -> str:
    """Deployment-level date order from the environment.

    The env var is process configuration, so it is read once — a per-cell read
    cost two ``os.environ`` lookups on every date column of every row. Per-run
    and per-request overrides travel through ``set_active_date_locale``; call
    ``_env_date_locale.cache_clear()`` if the environment is changed in place.
    """
    return (getenv_brand("DATE_ORDER") or "").strip().upper()


def _active_date_locale(explicit: str = "") -> str:
    """Return 'DMY' or 'MDY' from explicit > context > env, or '' if unset."""
    loc = (explicit or _DATE_LOCALE_VAR.get() or "").strip().upper() or _env_date_locale()
    return loc if loc in {"DMY", "MDY"} else ""


def set_active_date_locale(locale: str) -> contextvars.Token[str]:
    """Set the active date locale for the current request context."""
    return _DATE_LOCALE_VAR.set((locale or "").strip().upper())


def reset_active_date_locale(token: contextvars.Token[str]) -> None:
    """Restore the previous date locale."""
    _DATE_LOCALE_VAR.reset(token)


# Number grouping: 'US' (1,234.56), 'EU' (1.234,56), or '' fail-closed on
# a lone 3-digit group (1,234 / 1.234). Same contract shape as date_locale —
# never guess US vs EU. Currency marks and both separators still parse.
_NUMBER_LOCALE_VAR: contextvars.ContextVar[str] = contextvars.ContextVar(
    "number_locale", default=""
)


def _active_number_locale(explicit: str = "") -> str:
    loc = (explicit or _NUMBER_LOCALE_VAR.get() or "").strip().upper()
    return loc if loc in {"US", "EU"} else ""


def set_active_number_locale(locale: str) -> contextvars.Token[str]:
    """Pin the number locale for this transfer. Returns a reset token."""
    return _NUMBER_LOCALE_VAR.set(_active_number_locale(locale))


def reset_active_number_locale(token: contextvars.Token[str]) -> None:
    """Restore the previous number locale."""
    _NUMBER_LOCALE_VAR.reset(token)


def _implied_number_locale_from_currency(raw: str) -> str:
    """Currency marks that carry a grouping convention, or '' if mixed/absent.

    ``$`` / USD / ``£`` / GBP use 1,234.56. ``€`` / EUR use 1.234,56.
    A mark is evidence for that *cell*, not a column-wide guess.
    """
    text = unicodedata.normalize("NFKC", str(raw or ""))
    us = bool(re.search(r"(?<![A-Za-z])(?:USD|GBP|US\$)(?![A-Za-z])|[$£]", text, re.I))
    eu = bool(re.search(r"(?<![A-Za-z])EUR(?![A-Za-z])|[€]", text, re.I))
    if us and not eu:
        return "US"
    if eu and not us:
        return "EU"
    return ""


def infer_number_locale(
    rows: list[dict] | None,
    columns: list[str] | None = None,
    existing_locale: str = "",
) -> str:
    """US or EU when every marked/both-separator sample agrees; else ''."""
    pinned = _active_number_locale(existing_locale)
    if pinned:
        return pinned
    if not rows:
        return ""
    cols = columns or (list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [])
    votes = {"US": 0, "EU": 0}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for col in cols:
            raw = str(row.get(col) or "").strip()
            if not raw:
                continue
            implied = _implied_number_locale_from_currency(raw)
            if implied:
                votes[implied] += 1
                continue
            if "." in raw and "," in raw:
                if raw.rfind(".") > raw.rfind(","):
                    votes["US"] += 1
                else:
                    votes["EU"] += 1
    if votes["US"] and not votes["EU"]:
        return "US"
    if votes["EU"] and not votes["US"]:
        return "EU"
    return ""


def _looks_like_grouped_number(raw: str) -> bool:
    text = str(raw or "").strip()
    if not text or not any(ch.isdigit() for ch in text):
        return False
    return "," in text or "." in text


def ambiguous_number_columns(
    rows: list[dict] | None,
    columns: list[str] | None = None,
    number_locale: str = "",
) -> list[dict[str, Any]]:
    """Columns whose samples fail Auto grouping and look numeric.

    Operator next action: set number locale US or EU. Never invent a parse.
    """
    if _active_number_locale(number_locale) or not rows:
        return []
    cols = columns or (list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [])
    findings: list[dict[str, Any]] = []
    for col in cols:
        samples: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw = str(row.get(col) or "").strip()
            if not raw or not _looks_like_grouped_number(raw):
                continue
            if _parse_decimal(raw) is None:
                samples.append(raw)
        if samples:
            findings.append({
                "column": col,
                "samples": samples[:5],
                "next_action": "Set number locale US or EU in Destination → Advanced",
            })
    return findings


# Currency symbols and codes that are safe to strip from numeric values.
_CURRENCY_SYMBOLS = "".join({
    "$", "€", "£", "¥", "₹", "₩", "₽", "₺", "₴", "₱", "₫", "₭", "₦", "₲",
    "₮", "₣", "₤", "₨", "₪", "₸", "₾", "₼", "₿", "Ξ", "Ð", "₳", "✕", "Ł",
    "⚛", "∞", "Ȧ", "฿", "﷼", "؋", "৳",
})

# ISO / common letter codes and regional dollar notations.
_CURRENCY_CODES = "|".join(sorted({
    "USD", "EUR", "GBP", "INR", "JPY", "CNY", "CAD", "AUD", "CHF", "SEK",
    "DKK", "NOK", "NZD", "SGD", "HKD", "MXN", "BRL", "ZAR", "SAR", "AED",
    "KRW", "RUB", "TRY", "PLN", "THB", "IDR", "MYR", "PHP", "VND", "CZK",
    "HUF", "ILS", "CLP", "PEN", "COP", "ARS", "PKR", "BDT", "EGP", "NGN",
    "KES", "GHS", "XOF", "XAF", "XCD", "XPF", "XDR", "USDC", "USDT", "BUSD",
    "DAI", "BTC", "ETH", "DOGE", "ADA", "SOL", "XRP", "LTC", "BCH", "BNB",
    "DOT", "MATIC", "LINK", "UNI", "AAVE", "MKR", "COMP", "CRV", "SUSHI",
    "1INCH", "YFI", "BAL", "GRT", "SNX", "ZRX", "KNC", "BNT", "REN", "ANT",
    "BAND", "KAVA", "SC", "OCEAN", "STORJ", "FET", "AGIX", "RNDR", "COTI",
    "CELO", "NEAR", "ALGO", "XLM", "VET", "TRX", "EOS", "XTZ", "AVAX", "LDO",
    "ATOM", "IMX", "GALA", "MANA", "SAND", "ENJ", "AXS", "GODS", "BICO", "ANKR",
}, key=len, reverse=True))

_CURRENCY_RE = re.compile(
    rf"(?:^|\s)({_CURRENCY_CODES})(?:\s|$)|"
    rf"(?:^|\s)(US\$|A\$|C\$|HK\$|NZ\$|S\$|MX\$|R\$|CA\$|AU\$|SG\$)(?:\s|$)|"
    rf"(?:^|\s)(Rs\.?|Rp|RM|kr|Ft|Kč|zł|lei|лв|ден|ман|Нэм|CHF|Fr\.?|SFr)(?:\s|$)|"
    rf"[{re.escape(_CURRENCY_SYMBOLS)}]",
    re.IGNORECASE,
)


def _format_datetime(dt: datetime) -> str:
    """Canonical datetime wire form.

    - Naive values stay naive — no ``Z``, no offset.
    - UTC-aware values use ``Z`` (same instant, canonical form).
    - Non-UTC aware values keep their original offset (instant + offset fidelity).
      Destination NTZ writers (MySQL DATETIME, Snowflake TIMESTAMP_NTZ) normalize
      at bind time — never erase offset on the shared transform wire.

    Naive used to be stamped with ``Z``. That is the "UTC invent" the write
    quarantine in ``writer_common`` exists to prevent, so the two rules fought
    each other: a Postgres ``TIMESTAMP WITHOUT TIME ZONE`` picked up a zone it
    never had, and the MySQL ``DATETIME`` writer then quarantined every row for
    being timezone-aware. Postgres→MySQL moved zero rows as a result. A value
    with no zone carries no zone downstream; only a real offset survives.

    Sub-second precision survives too. Every branch here used to render seconds
    and nothing finer, and the mapper stamps this transform on *every* timestamp
    column by default, so a PostgreSQL ``timestamp(6)`` copied to an identical
    ``timestamp(6)`` arrived with its microseconds gone — on every route, for
    every row, with no finding raised. ``isoformat`` omits the fractional part
    when it is zero, so whole-second values render exactly as they did.
    """
    if dt.tzinfo is None:
        return dt.isoformat()
    offset = dt.utcoffset()
    if offset is not None and offset.total_seconds() == 0:
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return dt.isoformat()


def _to_utc_z(dt: datetime) -> str:
    """Backward-compatible alias — prefer :func:`_format_datetime` for new code."""
    return _format_datetime(dt)


def _detect_dayfirst(text: str, date_locale: str = "") -> bool | None:
    """Return True for day-first ordering, False for month-first, or None if ambiguous.

    Looks at the first two numeric fields of slash/dash/dot-delimited dates.
    A value like 31/12/2024 or 31.12.2024 is unambiguously day-first;
    12/31/2024 or 12-31-24 is month-first.  When both fields are <= 12 and
    unequal, locale is ambiguous — callers must fail closed (no silent MDY)
    unless an explicit ``date_locale`` (or ``DATAFLOW_DATE_ORDER`` env var) is
    set to ``DMY`` or ``MDY``.
    When both fields are equal (05/05/2024) either locale yields the same date.
    """
    order = _active_date_locale(date_locale)
    if order == "DMY":
        return True
    if order == "MDY":
        return False
    m = re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$", text)
    if not m:
        return None
    first, second = int(m.group(1)), int(m.group(2))
    if first > 12:
        return True
    if second > 12:
        return False
    if first == second:
        return False  # same calendar date either way — prefer month-first patterns
    return None


def _is_ambiguous_mdy_dmy(text: str, date_locale: str = "") -> bool:
    """True when slash/dash/dot date could be either MDY or DMY with different results."""
    return _detect_dayfirst(text, date_locale) is None and bool(
        re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$", text.strip())
    )


def ambiguous_date_columns(
    rows: list[dict] | None,
    columns: list[str] | None = None,
    date_locale: str = "",
) -> list[dict[str, Any]]:
    """Columns whose slash/dash/dot dates are MDY/DMY-ambiguous under Auto.

    Operator next action: set date locale DMY or MDY. Never invent Jan 2 vs Feb 1.
    """
    if _active_date_locale(date_locale) or not rows:
        return []
    cols = columns or (list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [])
    findings: list[dict[str, Any]] = []
    for col in cols:
        samples: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw = str(row.get(col) or "").strip()
            if not raw or not _is_ambiguous_mdy_dmy(raw):
                continue
            samples.append(raw)
        if samples:
            findings.append({
                "column": col,
                "samples": samples[:5],
                "next_action": "Set date locale DMY or MDY in Destination → Advanced",
            })
    return findings


def infer_date_locale(
    records: Iterable[Any],
    columns: list[str] | None = None,
    *,
    existing_locale: str = "",
    max_rows: int = 1000,
) -> str:
    """Infer DMY or MDY from unambiguous slash/dash/dot dates in a sample.

    A value like 31/12/2024 is unambiguously DMY; 12/31/2024 is MDY.
    Ambiguous values (both fields <= 12 and unequal) are ignored.
    If an explicit locale is already set, it is returned unchanged.
    """
    order = _active_date_locale(existing_locale)
    if order:
        return order
    dmy = 0
    mdy = 0
    pattern = re.compile(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$")
    checked = 0
    for rec in records:
        if checked >= max_rows:
            break
        checked += 1
        if isinstance(rec, dict):
            vals = rec.values() if columns is None else (rec.get(c) for c in columns)
        elif isinstance(rec, (list, tuple)) and columns is not None:
            vals = (rec[columns.index(c)] for c in columns if c in columns)
        else:
            vals = [rec]
        for v in vals:
            if not isinstance(v, str):
                continue
            v = v.strip()
            if not _DATE_LIKE_RE.search(v):
                continue
            m = pattern.match(v)
            if not m:
                continue
            first, second = int(m.group(1)), int(m.group(2))
            if first > 12 and second <= 12:
                dmy += 1
            elif second > 12 and first <= 12:
                mdy += 1
            # if both > 12 it's invalid; if equal it's locale-independent
    if dmy and dmy >= mdy:
        return "DMY"
    if mdy and mdy > dmy:
        return "MDY"
    return ""


def _reorder_date_patterns(text: str, patterns: tuple[str, ...], date_locale: str = "") -> list[str]:
    """Move the most likely day/month ordering patterns to the front.

    Year-first patterns only use 4-digit years (`%Y`).  Two-digit year-last
    patterns (`%m/%d/%y`, `%d/%m/%y`, etc.) are grouped with their leading
    month/day letter so that day-first vs month-first disambiguation works
    correctly and two-digit years cannot be mistaken for the first field.
    """
    dayfirst = _detect_dayfirst(text, date_locale)
    if dayfirst is None:
        return list(patterns)
    year_first = [p for p in patterns if p.startswith("%Y")]
    day_first = [p for p in patterns if p.startswith("%d")]
    month_first = [p for p in patterns if p.startswith("%m")]
    if dayfirst:
        return year_first + day_first + month_first
    return year_first + month_first + day_first


def _is_lossless_temporal_normalize(raw: str, out: str, transform: str) -> bool:
    """True when coerce is only canonical ISO formatting of the same instant/date."""
    locale = _active_date_locale()
    try:
        if transform == "datetime":
            a = _parse_datetime(raw, date_locale=locale)
            b = _parse_datetime(out, date_locale=locale)
            return bool(a and b and a == b)
        if transform == "date":
            a = _parse_date(raw, with_time=True, date_locale=locale) or _parse_date(raw, date_locale=locale)
            b = _parse_date(out, date_locale=locale)
            return bool(a and b and a == b)
        if transform == "time":
            return raw.strip()[:8] == out.strip()[:8] or raw.strip() == out.strip()
    except Exception:
        return False
    return False


_EPOCH_MS_RE = re.compile(r"^\d{13}$")
_EPOCH_S_RE = re.compile(r"^\d{10}$")


@functools.lru_cache(maxsize=4096)
def _parse_datetime_worker(value: str, date_locale: str) -> str | None:
    text = value.strip()
    if not _DATE_LIKE_RE.search(text):
        return None
    # Fail closed on ambiguous MDY/DMY — including timestamps with a time-of-day.
    # Inventing DMY for "06/05/2024 14:30" while AM/PM paths fell through to MDY
    # silently corrupted calendars across US/EU/IN feeds. Require date_locale.
    if _is_ambiguous_mdy_dmy(text, date_locale):
        return None
    if _EPOCH_MS_RE.match(text):
        ms = int(text)
        return _to_utc_z(datetime.fromtimestamp(ms / 1000, tz=timezone.utc))
    if _EPOCH_S_RE.match(text):
        return _to_utc_z(datetime.fromtimestamp(int(text), tz=timezone.utc))
    try:
        iso = text.replace("Z", "+00:00")
        return _to_utc_z(datetime.fromisoformat(iso))
    except ValueError:
        pass
    for fmt in _reorder_date_patterns(text, DATETIME_PATTERNS, date_locale):
        try:
            parsed = datetime.strptime(text, fmt)
            return _to_utc_z(parsed)
        except ValueError:
            continue
    parsed = _parse_date(text, date_locale=date_locale)
    # Date-only → datetime: attach midnight without inventing a timezone.
    # Stamping Z implied UTC and silently shifted warehouse TIMESTAMP_NTZ binds.
    return f"{parsed}T00:00:00" if parsed else None


def _parse_datetime(value: str, date_locale: str = "") -> str | None:
    return _parse_datetime_worker(value, _active_date_locale(date_locale))


@functools.lru_cache(maxsize=4096)
def _parse_date_worker(value: str, with_time: bool, date_locale: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    # ISO 8601 calendar dates are unambiguous under every locale and already in
    # the output form, so they need one validation instead of a pattern sweep.
    if _ISO_DATE_RE.match(text):
        try:
            date.fromisoformat(text)
        except ValueError:
            return None
        return text
    if not _DATE_LIKE_RE.search(text):
        return None
    if text.lower() in NULL_SENTINELS:
        return None
    # Fail closed when 05/06/2024 could be May 6 or June 5 depending on locale.
    if _is_ambiguous_mdy_dmy(text, date_locale):
        return None
    # Plain YYYYMMDD integer
    if re.match(r"^\d{8}$", text):
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    patterns = _reorder_date_patterns(text, DATE_PATTERNS, date_locale)
    if with_time:
        patterns += _reorder_date_patterns(text, DATE_WITH_TIME_PATTERNS, date_locale)
    for fmt in patterns:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_date(value: str, *, with_time: bool = False, date_locale: str = "") -> str | None:
    return _parse_date_worker(value, with_time, _active_date_locale(date_locale))

def _normalize_numeric_text(value: str) -> str:
    """Normalize unicode spaces, currency markers, accounting negatives, and percent signs."""
    text = unicodedata.normalize("NFKC", value)
    for ch in ("\u00a0", "\u2007", "\u202f", "\u2009", "\u2002", "\u2003",
               "\u2000", "\u2001", "\u2004", "\u2005", "\u2006", "\u2008",
               "\u200a", "\u205f", "\u3000"):
        text = text.replace(ch, "")
    text = text.strip()
    if text.endswith("%"):
        text = text[:-1].strip()
    # Accounting negative: (1,234.56) or 1,234.56-
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1].strip()}"
    if text.endswith("-") and text[:-1].strip():
        text = f"-{text[:-1].strip()}"
    # Remove currency symbols and common codes.
    text = _CURRENCY_RE.sub("", text)
    return text.strip()


def _digit_parts(parts: list[str]) -> bool:
    if not parts or not parts[0]:
        return False
    head = parts[0][1:] if parts[0].startswith("-") else parts[0]
    if not head.isdigit():
        return False
    return all(part.isdigit() for part in parts[1:])


def _normalize_locale_separators(text: str, number_locale: str = "") -> str | None:
    """Resolve . / , / space separators, or None when the grouping is ambiguous.

    Auto (no locale): both separators, 3+ thousand groups, and a 1–2 digit
    last group still parse. A lone 3-digit group (``1,234`` / ``1.234``)
    fails closed — US thousands and EU decimals share that shape.
    """
    if text.lower() in NULL_SENTINELS:
        return None
    if not text:
        return None

    locale = _active_number_locale(number_locale)
    # Remove ASCII spaces used as thousands separators (e.g. "1 000 000").
    text = text.replace(" ", "").replace("\t", "")

    if "." in text and "," in text:
        last_dot = text.rfind(".")
        last_comma = text.rfind(",")
        if last_dot > last_comma:
            candidate = text.replace(",", "")
            if candidate.count(".") <= 1:
                return candidate
            return None
        text = text.replace(".", "")
        last_comma = text.rfind(",")
        candidate = text[:last_comma] + "." + text[last_comma + 1:]
        if "," in candidate or candidate.count(".") > 1:
            return None
        return candidate

    if "," in text:
        parts = text.split(",")
        if not _digit_parts(parts):
            return None
        if locale == "US":
            if all(len(part) == 3 for part in parts[1:]):
                return "".join(parts)
            return None
        if locale == "EU":
            if len(parts) == 2:
                return parts[0] + "." + parts[1]
            if (
                len(parts) >= 2
                and all(len(part) == 3 for part in parts[1:-1])
                and 1 <= len(parts[-1]) <= 2
            ):
                return "".join(parts[:-1]) + "." + parts[-1]
            return None
        if (
            len(parts) >= 3
            and parts[0]
            and not parts[0].lstrip("-").startswith("0")
            and all(len(part) == 3 for part in parts[1:])
        ):
            return "".join(parts)
        if (
            len(parts) >= 2
            and all(len(part) == 3 for part in parts[1:-1])
            and 1 <= len(parts[-1]) <= 2
        ):
            return "".join(parts[:-1]) + "." + parts[-1]
        # A last group longer than 3 cannot be thousands — it is a decimal scale.
        if len(parts) == 2 and len(parts[1]) > 3:
            return parts[0] + "." + parts[1]
        return None

    if "." in text:
        parts = text.split(".")
        if not _digit_parts(parts):
            return text
        if locale == "US":
            if len(parts) == 2:
                return text
            return None
        if locale == "EU":
            if all(len(part) == 3 for part in parts[1:]):
                return "".join(parts)
            if len(parts) == 2 and 1 <= len(parts[1]) <= 2:
                return text
            if len(parts) == 2 and len(parts[1]) > 3:
                return text
            return None
        if (
            len(parts) >= 3
            and parts[0]
            and not parts[0].lstrip("-").startswith("0")
            and all(len(part) == 3 for part in parts[1:])
        ):
            return "".join(parts)
        if (
            len(parts) >= 2
            and all(len(part) == 3 for part in parts[1:-1])
            and 1 <= len(parts[-1]) <= 2
        ):
            return "".join(parts[:-1]) + "." + parts[-1]
        if len(parts) == 2 and 1 <= len(parts[1]) <= 2:
            return text
        # A last group longer than 3 cannot be thousands — IEEE/Excel residue
        # and money scales (52.310500000000000, 1.2345) stay decimals.
        if len(parts) == 2 and len(parts[1]) > 3:
            return text
        return None
    return text


def _parse_decimal(value: str) -> str | None:
    text = value.strip()
    # Tuple / point / coordinate strings such as (1,2) or (1, 2) are not numbers.
    if text.startswith("(") and text.endswith(")") and "," in text and "." not in text:
        return None
    implied = _implied_number_locale_from_currency(text)
    text = _normalize_numeric_text(text)
    text = _normalize_locale_separators(text, implied)
    if text is None or text == "":
        return None
    try:
        dec = Decimal(text)
    except (InvalidOperation, Overflow):
        return None
    if not dec.is_finite():
        return None

    from services.value_serializer import safe_decimal_text

    # Scientific / extreme magnitudes: keep a short exact form (never expand
    # 1e+1000000 into a million-character fixed-point string mid-transfer).
    rendered = safe_decimal_text(dec)
    if rendered is None:
        return None
    # Preserve Decimal scale (e.g. 1000.00 stays 1000.00). Stripping trailing
    # zeros caused money-fidelity regressions and false INTEGER inferences.
    return rendered

def currency_samples_carry_markers(samples: list[str] | None) -> bool:
    """True when sample values carry real currency / locale-money formatting.

    Distinguishes a money column (``$1,000.00`` / ``€2.000,50`` / ``USD 100``)
    from a plain numeric column that merely *shares a money-ish name* (``amount``
    holding ``100``). Only the former is safe to normalise into a DECIMAL carrier
    on create-new — a plain integer column must keep the engine's no-invent
    honesty. Evidence is a currency symbol/code OR a locale grouping that the
    numeric parser can still disambiguate to a finite decimal.
    """
    if not samples:
        return False
    parseable = 0
    with_marker = 0
    for raw in samples:
        text = str(raw or "").strip()
        if not text or text.lower() in NULL_SENTINELS:
            continue
        if _parse_decimal(text) is None:
            continue
        parseable += 1
        stripped = _CURRENCY_RE.sub("", unicodedata.normalize("NFKC", text)).strip()
        has_currency = stripped != text.strip()
        has_grouping = ("," in text) or ("." in text and text.count(".") > 1)
        if has_currency or has_grouping:
            with_marker += 1
    return parseable > 0 and with_marker > 0




def _parse_integer(value: str) -> int | None:
    plain = value.strip()
    # An unadorned ASCII integer has nothing for NFKC, currency, accounting or
    # separator resolution to change, and its digit count is inside the wire
    # budget by construction — the common integer column skips all of it.
    if _PLAIN_ASCII_INT_RE.match(plain):
        return int(plain)
    implied = _implied_number_locale_from_currency(plain)
    text = _normalize_numeric_text(plain)
    text = _normalize_locale_separators(text, implied)
    if text is None or text == "":
        return None
    try:
        if re.match(r"^-?\d+(\.\d+)?[eE][+-]?\d+$", text):
            dec = Decimal(text)
            if not dec.is_finite():
                return None
            if dec != dec.to_integral_value():
                return None
            from services.type_system import integer_within_wire_budget

            _sign, digits, exp = dec.as_tuple()
            if isinstance(exp, int) and not integer_within_wire_budget(
                digit_count=len(digits), exponent=exp
            ):
                return None
            return int(dec)
        dec = Decimal(text)
    except (InvalidOperation, Overflow, ValueError):
        return None
    if not dec.is_finite():
        return None
    if dec != dec.to_integral_value():
        return None
    from services.type_system import integer_within_wire_budget

    _sign, digits, exp = dec.as_tuple()
    if isinstance(exp, int) and not integer_within_wire_budget(
        digit_count=len(digits), exponent=exp
    ):
        return None
    try:
        return int(dec)
    except (Overflow, ValueError, InvalidOperation):
        return None


def decimal_wire_value(value: Any) -> Decimal | None:
    """The decimal the write path would bind for ``value``, or ``None``.

    The one parser profilers, preflight, reconcile, and shape must consult.
    Auto fails closed on ``1,234`` / ``1.234``. Currency marks and both-separator
    forms still parse. ``Decimal(text.replace(",", ""))`` is a second algorithm
    and is forbidden at call sites.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, Overflow, ValueError):
            return None
        return parsed if parsed.is_finite() else None
    text = str(value).strip()
    if not text:
        return None
    rendered = _parse_decimal(text)
    if rendered is None:
        return None
    try:
        dec = Decimal(rendered)
    except (InvalidOperation, Overflow, ValueError):
        return None
    return dec if dec.is_finite() else None


def integer_wire_value(value: str) -> int | None:
    """The integer the write path would bind for ``value``, or ``None``.

    The one parser a fit check must consult. ``Decimal(text)`` answers a
    different question in both directions: it refuses ``$1,000`` and ``true``,
    which this write coerces, and accepts ``NaN``/``Infinity``, which it does
    not. A fit check that asked ``Decimal`` alone therefore called values
    writable that the writer refuses at row 1.
    """
    text = str(value).strip()
    if not text:
        return None
    as_number = canonical_boolean_as_number(text)
    if as_number is not None:
        return as_number
    return _parse_integer(text)


def integer_parse_failure_reason(value: str) -> str:
    """Why the integer transform refused ``value``, in remediation terms.

    ``Invalid integer: '22.433332'`` names the value but not the fix, so an
    operator reviewing quarantine cannot tell a fractional value (widen the
    carrier, or round explicitly) from unparseable text (repair the source).
    The refusal keeps its prefix so existing reason grouping still holds.
    """
    stem = f"Invalid integer: {value!r}"
    try:
        dec = Decimal(value.strip())
    except (InvalidOperation, Overflow, ValueError, TypeError):
        return stem
    if dec.is_finite() and dec != dec.to_integral_value():
        return (
            f"{stem} — fractional value is not an integer; widen the "
            "destination to DECIMAL/DOUBLE, or round it explicitly before "
            "the write"
        )
    return stem


# Canonical boolean wire only (SSOT with type_system.boolean_value_fits).
# Informal "yes"/"on"/"y" invents truth (Airbyte-class); refuse — operator
# must remap or transform. Schema inference keeps a wider informal set for
# flag-name detection only.
_STRICT_BOOL_TRUE = frozenset({"true", "t", "1"})
_STRICT_BOOL_FALSE = frozenset({"false", "f", "0"})

#: Every token the write path can actually coerce to a boolean. A column typed
#: BOOLEAN off ``Y``/``N`` samples is rejected here on every row ("Invalid
#: boolean: 'Y'"), so any caller routing values through the boolean transform
#: must first check the destination can hold the outcome.
CANONICAL_BOOLEAN_TOKENS: frozenset[str] = _STRICT_BOOL_TRUE | _STRICT_BOOL_FALSE


def _parse_boolean(value: str) -> bool | None:
    text = value.strip().lower()
    if text in NULL_SENTINELS:
        return None
    if text in _STRICT_BOOL_TRUE:
        return True
    if text in _STRICT_BOOL_FALSE:
        return False
    return None


def canonical_boolean_as_number(value: str) -> int | None:
    """Canonical boolean wire → 1/0 for a numeric target, else ``None``.

    Boolean-carrying sources (SQL Server ``BIT``, PostgreSQL ``BOOLEAN``, MySQL
    ``TINYINT(1)``) serialize to ``"true"``/``"false"``, and engines without a
    boolean type (Oracle before 23ai, DB2, most warehouses) receive them as
    ``NUMBER(1)``/``SMALLINT``. That mapping is total and lossless, so refusing
    it as ``Invalid integer`` blocked every boolean column on those routes. Only
    the strict canonical wire converts — informal ``yes``/``on`` still refuses.
    """
    text = value.strip().lower()
    if text in {"true", "t"}:
        return 1
    if text in {"false", "f"}:
        return 0
    return None


def boolean_carrier_numeric_value(
    value: object, precision: int | None, scale: int | None
) -> int | None:
    """1/0 when a boolean lands on an engine's boolean carrier, else ``None``.

    Engines without a native boolean (Oracle, DB2) carry one as ``NUMBER(1)``,
    so a ``BIT``/``BOOLEAN`` source arriving as ``"true"`` is in range there —
    quarantining it as "decimal does not fit DECIMAL(1,0)" held out every
    boolean column on those routes. Anything wider than a single integer digit
    is a real numeric column and keeps refusing boolean wire.
    """
    if precision is None or int(precision) > 1 or int(scale or 0) != 0:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        return canonical_boolean_as_number(value)
    return None


def _json_reject_nonfinite(name: str) -> None:
    """Refuse NaN/Infinity → null (silent data loss on JSON/VARIANT/SUPER)."""
    raise ValueError(f"non-finite JSON constant: {name}")


def _parse_json(value: Any) -> str | None:
    """Normalize a cell into JSON-valid text for a semi-structured target.

    Valid JSON (objects, arrays, numbers, booleans, quoted strings) is preserved
    and re-serialized compactly. Native Python containers are also serialized so
    database drivers that return parsed JSON objects round-trip deterministically.
    A bare scalar that is not valid JSON on its own is losslessly wrapped as a
    JSON string literal so it still loads into a VARIANT / JSON / SUPER column.

    Non-finite constants (NaN / Infinity) are rejected — never mapped to JSON null.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            # JSONDecodeError subclasses ValueError — catch it first so bare
            # scalars wrap as JSON string literals (Mongo mixed fields → VARIANT).
            # Numbers parse exactly: the stdlib routes every non-integer through
            # binary64, which silently dropped digits off a DECIMAL landing in a
            # JSON / JSONB / VARIANT / SUPER column. Values binary64 holds exactly
            # still serialize as JSON numbers; the rest keep every digit as exact
            # text, per this codebase's Decimal-to-JSON policy.
            parsed = json_loads_exact(value, parse_constant=_json_reject_nonfinite)
        except json.JSONDecodeError:
            parsed = value  # wrap the raw scalar as a JSON string literal
        except ValueError:
            # Non-finite NaN/Infinity via parse_constant — refuse silent null.
            raise
        except TypeError:
            parsed = value
    elif isinstance(value, (dict, list, tuple, set, frozenset)):
        parsed = value
    else:
        parsed = value
    return json.dumps(
        parsed,
        ensure_ascii=False,
        separators=(",", ":"),
        default=json_default,
        allow_nan=False,
    )


def _parse_vector(value: str) -> list[float] | None:
    """Parse a vector literal into a float list; reject dim-mismatched later."""
    text = value.strip()
    if not text:
        return None
    try:
        if text.startswith("["):
            parsed = json.loads(text, parse_constant=_json_reject_nonfinite)
        else:
            parsed = [float(x.strip()) for x in text.split(",") if x.strip()]
        if not isinstance(parsed, list) or not parsed:
            return None
        out: list[float] = []
        for x in parsed:
            f = float(x)
            if f != f or f in (float("inf"), float("-inf")):  # NaN / Inf
                return None
            out.append(f)
        return out
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _parse_uuid(value: str) -> str | None:
    text = value.strip()
    try:
        return str(uuid_lib.UUID(text))
    except ValueError:
        return None


def _hash_pii(value: str) -> str:
    """HMAC-SHA256 digest for PII masking. Requires DATAFLOW_PII_HASH_KEY in prod."""
    secret = getenv_brand("PII_HASH_KEY") or getenv_brand("SECRET")
    if not secret:
        # Fail closed — never hash with a shared public default (would be reversible
        # across tenants that ship the same binary).
        raise ValueError(
            "hash_pii requires DATAFLOW_PII_HASH_KEY (or DATAFLOW_SECRET) — "
            "refusing insecure default key"
        )
    digest = hmac.new(
        secret.encode("utf-8"),
        value.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:32]


TIME_PATTERNS = (
    "%H:%M:%S.%f%z",
    "%H:%M:%S%z",
    "%H:%M:%S.%f",
    "%H:%M:%S",
    "%H:%M%z",
    "%H:%M",
    "%I:%M:%S %p",
    "%I:%M:%S%p",
    "%I:%M %p",
    "%I:%M%p",
)


def _parse_time(value: str) -> str | None:
    """Parse a time string and return a canonical ISO 8601 time.

    Accepts 24-hour and 12-hour forms, with optional microseconds, time-zone
    offsets, and AM/PM markers. Offset / ``Z`` polarity is preserved — never
    strip then re-invent UTC on TIMETZ binds (silent clock corruption).
    """
    from datetime import time as time_cls

    text = value.strip()
    if not text:
        return None
    # Prefer fromisoformat so ``15:30:00+05:30`` keeps tzinfo.
    iso_text = text
    if iso_text.upper().endswith("Z"):
        iso_text = iso_text[:-1] + "+00:00"
    elif iso_text.upper().endswith(" UTC"):
        iso_text = iso_text[:-4].strip() + "+00:00"
    try:
        tm = time_cls.fromisoformat(iso_text)
        return tm.isoformat()
    except ValueError:
        pass
    # strptime fallback — keep tzinfo via timetz() when %z matched.
    stamped = text.upper().replace("Z", "+0000")
    # ``+05:30`` → ``+0530`` for %z on older parsers.
    if len(stamped) >= 6 and stamped[-3] == ":" and stamped[-6] in "+-":
        stamped = stamped[:-3] + stamped[-2:]
    for fmt in TIME_PATTERNS:
        try:
            parsed = datetime.strptime(stamped, fmt)
            tm = parsed.timetz() if parsed.tzinfo is not None else parsed.time()
            return tm.isoformat()
        except ValueError:
            continue
    return None


KNOWN_TRANSFORMS = frozenset({
    "decimal", "integer", "boolean", "date", "datetime", "time", "json", "binary",
    "trim", "trim_id", "uuid", "upper", "lower", "hash_pii", "mask_pii", "none", "identity",
    # Logical-type aliases Studio / DDL inference sometimes stamp as the transform id.
    "passthrough", "string", "varchar", "text",
    "phone", "email", "url", "iban", "currency", "percentage", "postal", "base64",
    "strip_controls", "normalize_unicode",
})

#: Rename-only transforms — must not mutate wire (no strip). Trim is opt-in.
_IDENTITY_TRANSFORMS = frozenset({
    "none", "identity", "passthrough", "string", "varchar", "text",
})


def _strip_format_controls(text: str) -> str:
    """Remove format/control chars warehouses reject; keep tab/newline/carriage return."""
    cleaned: list[str] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat == "Cf":
            continue
        if cat == "Cc" and ch not in "\t\n\r":
            continue
        cleaned.append(ch)
    return "".join(cleaned)


def _parse_binary(value: str) -> str | None:
    """Normalize binary wire to base64 text for the transform pipeline.

    Accepts already-base64 payloads and common hex carriers (Postgres ``\\x…``,
    ``0x…``). Refuse silent UTF-8→base64 invent — that mutates operator data
    (Airbyte historically did this; destinations then write wrong bytes).
    """
    text = value.strip()
    if not text:
        return None
    try:
        base64.b64decode(text, validate=True)
        return text
    except Exception:
        pass
    hex_body: str | None = None
    if text.lower().startswith("\\x"):
        hex_body = text[2:]
    elif text.lower().startswith("0x"):
        hex_body = text[2:]
    if hex_body is not None:
        try:
            raw = bytes.fromhex(hex_body)
        except ValueError:
            return None
        return base64.b64encode(raw).decode("ascii")
    return None

def infer_transform(source_col: str, target_col: str, inferred_type: str) -> str:
    return infer_transform_for_mapping(source_col, target_col, inferred_type, None)


def _samples_prefer_boolean_over_integer(samples: list[str] | None) -> bool:
    """True when samples parse as booleans but not as plain integers (true/false).

    Used when the destination DDL is INTEGER (SQLite bool affinity) so we coerce
    with the boolean transform instead of inventing a create-new text column.
    """
    if not samples:
        return False
    checked = 0
    bool_ok = 0
    int_ok = 0
    for raw in samples[:8]:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        checked += 1
        if _parse_boolean(text) is not None:
            bool_ok += 1
        try:
            int(text, 10)
            int_ok += 1
        except ValueError:
            pass
    if checked < 2:
        return False
    return (bool_ok / checked) >= 0.9 and (int_ok / checked) < 0.5


def _samples_look_temporal(source_samples: list[str] | None) -> bool:
    """True when most non-empty samples look date/time-like (not status/enum text)."""
    if not source_samples:
        return False
    hits = 0
    checked = 0
    for raw in source_samples[:25]:
        text = str(raw or "").strip()
        if not text:
            continue
        checked += 1
        if _DATE_LIKE_RE.search(text):
            hits += 1
    return checked >= 2 and (hits / checked) >= 0.8


_YEAR_COLUMN_NAMES = frozenset({"year", "fiscalyear", "calendaryear", "yr"})


def _is_calendar_year_number(
    source_col: str, src_logical: str, source_samples: list[str] | None
) -> bool:
    """True for a column that *holds a year number*, not merely one named "Year".

    The name alone is not evidence. Spreadsheet exports routinely put a real
    instant (``2019-01-01T00:00:00``) in a column called ``Year``; forcing the
    integer transform there makes every row fail ``Invalid integer`` at Validate
    with no remap that can clear it, because the declared pair is already
    TIMESTAMP → TIMESTAMP. A temporal source type or temporal-looking samples
    therefore veto the heuristic.
    """
    if re.sub(r"[^a-z0-9]", "", (source_col or "").lower()) not in _YEAR_COLUMN_NAMES:
        return False
    if src_logical in {"datetime", "date", "timestamp", "time"}:
        return False
    return not _samples_look_temporal(source_samples)


def _samples_need_numeric_parse(source_samples: list[str] | None) -> bool:
    """True when declared-numeric samples still look like text that must parse."""
    if not source_samples:
        return False
    for raw in source_samples[:12]:
        text = str(raw).strip()
        if any(mark in text for mark in ("$", "€", "£", "¥", "%", ",")):
            return True
    return False


def infer_transform_for_mapping(
    source_col: str,
    target_col: str,
    source_type: str,
    target_type: str | None = None,
    source_samples: list[str] | None = None,
    destination_db_type: str = "",
) -> str:
    """Pick transform from source/target logical types, column semantics, and samples.

    ``destination_db_type`` disambiguates carriers whose name understates what
    they hold — MongoDB's BSON ``date`` is a full millisecond instant, not a
    calendar day, so a datetime source must not be truncated into it.
    """
    from services.type_system import normalize_logical_type

    from services.type_system import parse_numeric_precision_scale, temporal_carrier_holds_time

    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if target_type else None
    tgt_name = target_col.lower()

    semantic = detect_semantic_type(source_col, source_samples)
    samples_temporal = _samples_look_temporal(source_samples)
    src_temporal = src in {"datetime", "date", "timestamp", "time"}

    # Native numeric wire already is a number — do not invent a string parse.
    # Parse integer/decimal is reserved for text/unknown sources, or dirty
    # CSV/Excel cells that still carry currency / grouping marks.
    _native_numeric = src in {"integer", "decimal", "float"} and not _samples_need_numeric_parse(
        source_samples
    )
    if tgt == "decimal" and target_type:
        _p, _s = parse_numeric_precision_scale(target_type)
        if _s == 0 and src == "integer":
            return "none" if _native_numeric else "integer"

    # Explicit, non-generic target type wins; if the source is already numeric
    # use a direct numeric transform, otherwise apply semantic transforms.
    if tgt and tgt not in {"string", "text"}:
        if tgt == "integer":
            # SQLite/MySQL/etc. store BOOLEAN as INTEGER. Coerce boolean sources
            # (and true/false text samples) with the boolean transform so remaps
            # do not invent active_text / null out the existing flag column.
            if src == "boolean" or _samples_prefer_boolean_over_integer(source_samples):
                return "boolean"
            return "none" if _native_numeric else "integer"
        if tgt == "decimal":
            if src in {"string", "text", "unknown"} and semantic == "currency":
                return "currency"
            if src in {"string", "text", "unknown"} and semantic == "percentage":
                return "percentage"
            return "none" if _native_numeric else "decimal"
        if tgt == "float":
            # Native float/decimal/int already numeric — IEEE DDL is type_system.
            return "none" if _native_numeric else "decimal"
        if tgt == "boolean":
            return "boolean"
        if tgt in {"json", "array"}:
            return "json"
        if tgt == "binary":
            return "binary"
        if tgt == "datetime":
            # Calendar year number columns must stay integer (FSI "Year"), not
            # invent datetime coerce that then FAIL_JOBs on blank Excel cells.
            if _is_calendar_year_number(source_col, src, source_samples):
                return "integer"
            # Never force a date cast on non-temporal VARCHAR (status → posted_date).
            # Let G3/G5 declare the type mismatch instead of lucky-parse corruption.
            if src_temporal or samples_temporal:
                return "datetime"
            return "none"
        if tgt == "date":
            if _is_calendar_year_number(source_col, src, source_samples):
                return "integer"
            # Narrowing a datetime into a date-only column drops the time of day.
            # Only do it when the destination genuinely cannot hold a time;
            # document stores map both logical types onto one instant carrier.
            if src == "datetime" and temporal_carrier_holds_time(destination_db_type):
                return "datetime"
            if src_temporal or samples_temporal:
                return "date"
            return "none"
        if tgt == "time":
            if src_temporal or samples_temporal:
                return "time"
            return "none"
        if tgt == "uuid":
            return "uuid"
        if tgt == "vector":
            return "vector"
        # Specialty types travel as identity text/binary payloads — never invent a cast.
        if tgt in {"interval", "geography"}:
            return "none"

    # Source type is the pivot when the target is generic (e.g., VARCHAR).
    # Native numerics stay identity; but a numeric-typed source whose textual
    # values still carry currency symbols / locale grouping ($1,000.00, €2.000,50)
    # must be normalised even into a text sink — otherwise the raw formatted
    # string is written verbatim and money fidelity is lost. ``_native_numeric``
    # is False exactly when the samples still need a parse.
    if src == "integer":
        return "none" if _native_numeric else "integer"
    if src == "decimal":
        return "none" if _native_numeric else "decimal"
    if src == "float":
        return "none" if _native_numeric else "decimal"
    if src == "boolean":
        return "boolean"
    if src in {"json", "array"}:
        return "json"
    if src == "binary":
        # Binary→text sinks must not force base64 rewrite as identity.
        # Keep bytes as identity payload; Map/G3 treat domain polarity via
        # Accept risk (hex/base64 mutate is not "preserve").
        if tgt in {"string", "text", "json", "unknown"} or not target_type:
            return "none"
        return "binary"
    if src == "datetime":
        return "datetime"
    if src == "date":
        return "date"
    if src == "time":
        return "time"
    if src == "uuid":
        return "uuid"
    if src == "vector":
        return "vector"
    if src in {"interval", "geography"}:
        return "none"

    # Semantic column names drive the transform for generic string targets.
    # For string/unknown targets, preserve currency/percentage/email/url/phone
    # as text to avoid data loss / false quarantine (e.g. empty image→url on
    # TEXT, '$100' stripped to 100). Typed sinks still get semantic casts below.
    if semantic in {
        "currency",
        "percentage",
        "phone",
        "email",
        "url",
        "iban",
        "postal",
        "base64",
    }:
        if tgt in {"string", "text", "unknown"} or not tgt:
            return "none"
        if semantic == "phone":
            return "phone"
        if semantic == "email":
            return "email"
        if semantic == "url":
            return "url"
        if semantic == "iban":
            return "iban"
        if semantic == "postal":
            return "postal"
        if semantic == "base64":
            return "base64"
        # currency / percentage on numeric tgt already handled above when tgt set
        return "none"
    if semantic == "timestamp":
        return "datetime"

    src_col = source_col.upper()
    # Calendar year number (FSI "Year", fiscal_year) is INTEGER — never invent
    # datetime coerce for a 4-digit year field (empty cells then FAIL_JOB).
    if _is_calendar_year_number(source_col, src, source_samples):
        if tgt in {"datetime", "timestamp", "timestamptz", "date"}:
            return "integer"
        if not tgt or tgt in {"string", "text", "unknown", "integer", "bigint"}:
            return "integer"
    # Name-heuristic decimal only when the destination is numeric — never invent
    # a cast that strips currency markers into a VARCHAR/TEXT sink.
    if tgt not in {"string", "text", "unknown", None} and (
        "amount" in tgt_name
        or "total" in tgt_name
        or "weight" in tgt_name
        or src_col in {
            "AMT", "PAY_AMT", "PAYMENT_AMT", "VALUE",
        }
    ):
        return "decimal"
    src_lower = source_col.lower()
    # Only apply date transforms when the SOURCE looks temporal — never because
    # the target name alone contains "date" (status → posted_date_estimated).
    source_looks_temporal = (
        semantic == "timestamp"
        or src in {"date", "datetime", "time"}
        or "date" in src_lower
        or "time" in src_lower
        or src_lower.endswith("_at")
        or src_lower.endswith("_dt")
        or src_col.endswith("_DT")
        or src_col in {"TXN_DT", "PAY_DT", "PAYMENT_DT", "TRANS_DT"}
    )
    if source_looks_temporal and (
        "date" in tgt_name
        or tgt_name.endswith("_dt")
        or src_col.endswith("_DT")
        or src_col in {"TXN_DT", "PAY_DT", "PAYMENT_DT", "TRANS_DT"}
    ):
        return "datetime" if src == "datetime" or "epoch" in src_lower else "date"
    if tgt_name.endswith("_id") or tgt_name.endswith("id") or src_col.endswith("_ID"):
        return "trim_id"
    if "qty" in tgt_name or "quantity" in tgt_name:
        return "integer" if src == "integer" else "decimal"
    # Prefer preserve/identity over trim — operators who want strip choose Trim.
    return "none"


#: A zoneless source column carries wall-clock digits and no zone, so writing it
#: to an instant carrier has to pick one. Guessing UTC is what silently shifts a
#: business day; this transform is the operator supplying the fact the source
#: never recorded, so the instant is asserted rather than invented.
ASSUME_TIMEZONE_PREFIX = "assume_timezone:"


def _apply_assume_timezone(text: str, transform: str) -> tuple[Any, str | None]:
    """Attach a declared zone to a zoneless datetime.

    A value that already carries an offset is left alone: it has an instant, and
    overriding it with a declared zone would move a timestamp the source was
    explicit about.
    """
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    zone_name = transform[len(ASSUME_TIMEZONE_PREFIX):].strip()
    if not zone_name:
        return None, "assume_timezone needs a zone, e.g. assume_timezone:UTC"
    try:
        zone = ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return None, f"Unknown timezone: {zone_name!r}"

    parsed = _parse_datetime(text)
    if parsed is None:
        return None, f"Invalid datetime: {text!r}"
    try:
        moment = datetime.fromisoformat(str(parsed).replace("Z", "+00:00"))
    except ValueError:
        return None, f"Invalid datetime: {text!r}"
    # An offset the source stated is an instant already; a declared zone must not
    # move it.
    if moment.tzinfo is not None:
        return _format_datetime(moment), None
    try:
        return _format_datetime(moment.replace(tzinfo=zone)), None
    except (ValueError, OverflowError) as exc:
        return None, f"Could not apply zone {zone_name!r}: {exc}"


def apply_transform(raw: str | None, transform: str) -> tuple[Any, str | None]:
    """Returns (value, error).

    Explicit SQL/Dynamo NULL sentinels → None. Empty string is preserved for
    identity/string transforms so ``''`` ≠ SQL NULL on VARCHAR round-trips;
    typed transforms still coerce empty → None.
    """
    if raw is None:
        return None, None
    raw_s = raw if type(raw) is str else str(raw)
    # Every engine sentinel is underscore-delimited, so a value without an
    # underscore cannot be one. The check keeps the identity path — the most
    # common transform on the widest tables — off ``strip().lower()`` of every
    # cell it carries through unchanged.
    if "_" in raw_s:
        lowered = raw_s.strip().lower()
        # Sparse CDC / schemaless absent field — never coerce to SQL NULL or identity text.
        if lowered == "__df_missing__" or raw_s == "__DF_MISSING__":
            from services.value_serializer import DF_MISSING_SENTINEL

            return DF_MISSING_SENTINEL, None
        # Explicit NULL sentinels must never land as literal strings in any dest.
        if lowered in {"__df_sql_null__", "__df_ddb_null__"}:
            return None, None

    transform_l = (transform or "none").strip().lower()
    # Identity aliases preserve the exact wire, so they need nothing below.
    if transform_l in _IDENTITY_TRANSFORMS:
        return raw_s, None

    text = raw_s.strip()
    if transform_l in {"omit", "intentional_omit", "drop", "exclude"}:
        return None, "intentional omit — mapping should not project"

    # Identity / text transforms: empty string is a real value.
    if text == "" and transform_l in _KEEP_EMPTY_TRANSFORMS:
        return "", None
    # Typed + semantic transforms: empty/whitespace must not silently become SQL NULL.
    # That bypassed Risk Contracts (no err → no CAST/QUARANTINE path) and looked
    # like a successful write. Empty → error; continue policies may quarantine or
    # coerce_null only when the contract/job policy says so.
    if text == "" and transform_l in _TYPED_TRANSFORMS:
        return None, f"Empty value cannot coerce to {transform_l}"
    if text == "":
        return None, f"Empty value cannot coerce to {transform_l or 'transform'}"

    # Null/missing sentinels for typed transforms must surface as coerce errors
    # (Risk Contract / quarantine path) — never silent NULL invent.
    # Exception: NaN / ±Infinity are NOT SQL null for JSON/vector — reject as
    # non-finite (never invent JSON null / empty embedding).
    if transform_l in _TYPED_TRANSFORMS:
        low = text.lower()
        if low in NULL_SENTINELS:
            if transform_l in {"json", "vector"} and low in _NONFINITE_TOKENS:
                pass  # fall through to typed parsers that reject non-finite
            else:
                return None, f"Null sentinel {text!r} cannot coerce to {transform_l}"

    if transform == "decimal":
        bool_as_number = canonical_boolean_as_number(text)
        if bool_as_number is not None:
            return Decimal(bool_as_number), None
        parsed = _parse_decimal(text)
        if parsed is None:
            return None, f"Invalid decimal: {text!r}"
        return parsed, None

    if transform == "integer":
        bool_as_number = canonical_boolean_as_number(text)
        if bool_as_number is not None:
            return bool_as_number, None
        parsed_int = _parse_integer(text)
        if parsed_int is None:
            return None, integer_parse_failure_reason(text)
        return parsed_int, None

    if transform == "boolean":
        parsed_bool = _parse_boolean(text)
        if parsed_bool is None:
            return None, f"Invalid boolean: {text!r}"
        return parsed_bool, None

    if transform == "date":
        parsed = _parse_date(text, with_time=True)
        if parsed is None:
            return None, f"Invalid date: {text!r}"
        return parsed, None

    if transform == "datetime":
        parsed = _parse_datetime(text)
        if parsed is None:
            return None, f"Invalid datetime: {text!r}"
        return parsed, None

    if transform.startswith(ASSUME_TIMEZONE_PREFIX):
        return _apply_assume_timezone(text, transform)

    if transform == "time":
        parsed = _parse_time(text)
        if parsed is None:
            return None, f"Invalid time: {text!r}"
        return parsed, None

    if transform == "json":
        json_input = raw if isinstance(raw, (dict, list, tuple, set, frozenset)) else text
        try:
            parsed_json = _parse_json(json_input)
        except ValueError as exc:
            return None, str(exc)
        if parsed_json is None:
            return None, f"Invalid JSON: {text!r}"
        return parsed_json, None

    if transform == "vector":
        parsed_vec = _parse_vector(text)
        if parsed_vec is None:
            return None, f"Invalid vector: {text!r}"
        return parsed_vec, None

    if transform == "binary":
        parsed_binary = _parse_binary(text)
        if parsed_binary is None:
            return None, f"Invalid binary: {text!r}"
        return parsed_binary, None

    if transform in {"trim", "trim_id"}:
        cleaned = re.sub(r"\s+", " ", text)
        return cleaned, None

    if transform == "strip_controls":
        cleaned = _strip_format_controls(text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned, None

    if transform == "normalize_unicode":
        cleaned = unicodedata.normalize("NFKC", _strip_format_controls(text))
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned, None

    if transform == "uuid":
        parsed = _parse_uuid(text)
        if parsed is None:
            return None, f"Invalid UUID: {text!r}"
        return parsed, None

    if transform == "upper":
        return text.upper(), None

    if transform == "lower":
        return text.lower(), None

    if transform == "hash_pii":
        try:
            return _hash_pii(text), None
        except ValueError as exc:
            return None, str(exc)

    if transform == "mask_pii":
        return pii_mask(text), None

    semantic_transform_map = {
        "phone": SemanticType.PHONE,
        "email": SemanticType.EMAIL,
        "url": SemanticType.URL,
        "iban": SemanticType.IBAN,
        "currency": SemanticType.CURRENCY,
        "percentage": SemanticType.PERCENTAGE,
        "postal": SemanticType.POSTAL,
        "base64": SemanticType.BASE64,
    }
    if transform in semantic_transform_map:
        st = semantic_transform_map[transform]
        # Currency and percentage are numeric; convert to a fixed-point string so
        # destinations that serialize rows as JSON are safe. Other semantic types
        # stay string-safe.
        target_string = st not in {SemanticType.CURRENCY, SemanticType.PERCENTAGE}
        converted = normalize_value_for_target(text, st, "decimal" if not target_string else "string")
        if not target_string and not isinstance(converted, Decimal):
            # Fail-closed: never invent the raw text as a "usable" currency/percentage.
            return None, f"Invalid {transform}: {text!r}"
        if target_string:
            out = str(converted)
            # Explicit semantic transforms must fail-closed on garbage — never
            # silently "normalize" invalid email/url/iban/phone into the primary table.
            from services.semantic_types import (
                _EMAIL_RE,
                _IBAN_RE,
                _URL_RE,
                _digits_only,
            )

            if transform == "email" and not _EMAIL_RE.match(out):
                return None, f"Invalid email: {text!r}"
            if transform == "url" and not _URL_RE.match(out):
                return None, f"Invalid url: {text!r}"
            if transform == "iban":
                compact = out.upper().replace(" ", "")
                if not _IBAN_RE.match(compact):
                    return None, f"Invalid iban: {text!r}"
                return compact, None
            if transform == "phone":
                digits = _digits_only(out)
                # E.164-ish: at least 7 digits; refuse embedded letters.
                if len(digits.replace("+", "")) < 7:
                    return None, f"Invalid phone: {text!r}"
                if re.search(r"[A-Za-z]", out):
                    return None, f"Invalid phone: {text!r}"
                # Preserve operator-visible formatting on string targets (Map Ready).
                return out, None
            if transform == "postal":
                compact = out.upper().replace(" ", "")
                # Accept national formats (US ZIP, CA, UK outward+inward) — refuse
                # empty / punctuation-only garbage that normalize would soft-pass.
                if not re.match(r"^[A-Z0-9]{3,12}$", compact):
                    return None, f"Invalid postal: {text!r}"
                return compact, None
            if transform == "base64":
                try:
                    import base64 as _b64

                    _b64.b64decode(out, validate=True)
                except Exception:
                    return None, f"Invalid base64: {text!r}"
                return out, None
            return out, None
        return str(converted) if isinstance(converted, Decimal) else converted, None

    if transform not in KNOWN_TRANSFORMS:
        return None, f"Unknown transform: {transform!r}"

    return text, None


def dry_run_sample(
    *,
    headers: list[str],
    sample_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    sample_size: int = 100,
    max_errors_per_mapping: int = 5,
) -> tuple[bool, list[str]]:
    """Apply write-path transforms to the sample window.

    Collects up to ``max_errors_per_mapping`` failures per column so sporadic
    bad values mid-sample cannot slip past Validate while early rows look clean.
    """
    if not sample_rows:
        return False, ["No sample rows available for dry-run validation"]

    errors: list[str] = []
    source_idx = {h: i for i, h in enumerate(headers)}

    from services.mapping_constraints import write_mappings
    from services.transform_resolver import resolve_transform

    # A declared omission has no destination carrier, so there is no write-path
    # transform to dry-run. Probing it reported the omission itself as a cast
    # failure — the honest operator action was punished with a data error.
    mappings = write_mappings(mappings)

    dest_types = {
        str(m.get("target")): str(m.get("target_type"))
        for m in mappings
        if m.get("target") and m.get("target_type")
    }

    for m in mappings:
        idx = source_idx.get(m["source"])
        if idx is None:
            errors.append(f"Source column missing: {m['source']}")
            continue
        # Resolve UI aliases (cast_number → decimal) before dry-run — never leave
        # Unknown transform: 'cast_number' as a false quarantine signal.
        transform = resolve_transform(m, column_types=column_types, dest_types=dest_types)
        mapping_errors = 0
        scanned = 0
        for row in sample_rows[:sample_size]:
            scanned += 1
            raw = row[idx] if idx < len(row) else ""
            _, err = apply_transform(raw, transform)
            if err:
                errors.append(f"{m['source']}→{m['target']}: {err}")
                mapping_errors += 1
                if mapping_errors >= max_errors_per_mapping:
                    remaining = max(0, min(len(sample_rows), sample_size) - scanned)
                    if remaining:
                        errors.append(
                            f"{m['source']}→{m['target']}: "
                            f"+{remaining} sample row(s) not fully reported "
                            f"(stopped after {max_errors_per_mapping} errors)"
                        )
                    break

    return len(errors) == 0, errors[:40]


def preview_quarantine_cells(
    *,
    headers: list[str],
    sample_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str] | None = None,
    sample_size: int = 25,
    max_cells: int = 120,
) -> dict:
    """Cell-level preview: which sample values will quarantine / coerce before run.

    Operators use this so Validate feels trustworthy vs silent Airbyte/Fivetran loads.
    """
    column_types = column_types or {}
    source_idx = {h: i for i, h in enumerate(headers)}
    cells: list[dict] = []
    quarantine_count = 0
    coerce_count = 0
    ok_count = 0

    from services.mapping_constraints import write_mappings
    from services.transform_resolver import resolve_transform

    # Omitted columns are never written, so they have no cell to quarantine.
    mappings = write_mappings(mappings)

    for m in mappings:
        src = m.get("source") or ""
        tgt = m.get("target") or src
        idx = source_idx.get(src)
        if idx is None:
            continue
        transform = resolve_transform(m, column_types=column_types)
        for row_i, row in enumerate(sample_rows[:sample_size]):
            if len(cells) >= max_cells:
                break
            raw = row[idx] if idx < len(row) else ""
            raw_s = "" if raw is None else str(raw)
            out, err = apply_transform(raw_s, transform)
            if err:
                quarantine_count += 1
                cells.append({
                    "row": row_i,
                    "source": src,
                    "target": tgt,
                    "raw": raw_s[:200],
                    "status": "quarantine",
                    "message": err,
                    "transform": transform,
                })
            elif out is not None and str(out) != raw_s:
                # Lossless datetime/date normalization (ISO Z ↔ same instant) is
                # expected for CSV→SQL — do not flood Validate/Run with coerce noise.
                if transform in {"datetime", "date", "time"} and _is_lossless_temporal_normalize(
                    raw_s, str(out), transform
                ):
                    ok_count += 1
                else:
                    coerce_count += 1
                    ok_count += 1
                    cells.append({
                        "row": row_i,
                        "source": src,
                        "target": tgt,
                        "raw": raw_s[:200],
                        "coerced": str(out)[:200],
                        "status": "coerced",
                        "transform": transform,
                    })
            else:
                ok_count += 1
        if len(cells) >= max_cells:
            break

    # Prefer surfacing quarantine/coerced cells; drop pure-ok noise.
    interesting = [c for c in cells if c["status"] != "ok"]
    return {
        "quarantine_count": quarantine_count,
        "coerce_count": coerce_count,
        "ok_count": ok_count,
        "cells": interesting[:max_cells],
        "sample_rows_scanned": min(sample_size, len(sample_rows)),
    }
