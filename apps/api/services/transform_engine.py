"""Deterministic transform execution for dry-run and write paths."""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import os
import re
import unicodedata
import uuid as uuid_lib
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, Overflow
from typing import Any

from services.pii_guard import mask as pii_mask
from services.semantic_types import (
    SemanticType,
    detect_semantic_type,
    normalize_value_for_target,
)
from services.value_serializer import json_default

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
    # Schemaless source field absent (Mongo/Dynamo/Couchbase unions).
    "__df_missing__",
})

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

    - Naive values are treated as UTC and emitted with ``Z``.
    - UTC-aware values use ``Z`` (same instant, canonical form).
    - Non-UTC aware values keep their original offset (instant + offset fidelity).
      Destination NTZ writers (MySQL DATETIME, Snowflake TIMESTAMP_NTZ) normalize
      at bind time — never erase offset on the shared transform wire.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    offset = dt.utcoffset()
    if offset is not None and offset.total_seconds() == 0:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.isoformat(timespec="seconds")


def _to_utc_z(dt: datetime) -> str:
    """Backward-compatible alias — prefer :func:`_format_datetime` for new code."""
    return _format_datetime(dt)


def _detect_dayfirst(text: str) -> bool | None:
    """Return True for day-first ordering, False for month-first, or None if ambiguous.

    Looks at the first two numeric fields of slash/dash/dot-delimited dates.
    A value like 31/12/2024 or 31.12.2024 is unambiguously day-first;
    12/31/2024 or 12-31-24 is month-first.  When both fields are <= 12 and
    unequal, locale is ambiguous — callers must fail closed (no silent MDY)
    unless ``DATAFLOW_DATE_ORDER`` is set to ``DMY`` or ``MDY``.
    When both fields are equal (05/05/2024) either locale yields the same date.
    """
    order = (os.getenv("DATAFLOW_DATE_ORDER") or "").strip().upper()
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


def _is_ambiguous_mdy_dmy(text: str) -> bool:
    """True when slash/dash/dot date could be either MDY or DMY with different results."""
    return _detect_dayfirst(text) is None and bool(
        re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$", text.strip())
    )

def _reorder_date_patterns(text: str, patterns: tuple[str, ...]) -> list[str]:
    """Move the most likely day/month ordering patterns to the front.

    Year-first patterns only use 4-digit years (`%Y`).  Two-digit year-last
    patterns (`%m/%d/%y`, `%d/%m/%y`, etc.) are grouped with their leading
    month/day letter so that day-first vs month-first disambiguation works
    correctly and two-digit years cannot be mistaken for the first field.
    """
    dayfirst = _detect_dayfirst(text)
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
    try:
        if transform == "datetime":
            a = _parse_datetime(raw)
            b = _parse_datetime(out)
            return bool(a and b and a == b)
        if transform == "date":
            a = _parse_date(raw, with_time=True) or _parse_date(raw)
            b = _parse_date(out)
            return bool(a and b and a == b)
        if transform == "time":
            return raw.strip()[:8] == out.strip()[:8] or raw.strip() == out.strip()
    except Exception:
        return False
    return False


@functools.lru_cache(maxsize=4096)
def _parse_datetime(value: str) -> str | None:
    text = value.strip()
    if not _DATE_LIKE_RE.search(text):
        return None
    # Fail closed on ambiguous MDY/DMY — silent US-default corrupts EU dates.
    # Exception: ambiguous timestamps that also carry a time-of-day are far more
    # likely to be day-first event data (EU/IN/AU convention) than a US date with
    # a time; default to DMY so real-world logistics/banking fixtures parse.
    if _is_ambiguous_mdy_dmy(text):
        if re.search(r"[ T]\d{1,2}:\d{2}(?::\d{2})?(?:\s*[APap][Mm])?\b", text):
            dayfirst_patterns = (
                [p for p in DATETIME_PATTERNS if p.startswith("%d")]
                + [p for p in DATETIME_PATTERNS if p.startswith("%Y")]
                + [p for p in DATETIME_PATTERNS if p.startswith("%m")]
            )
            for fmt in dayfirst_patterns:
                try:
                    parsed = datetime.strptime(text, fmt)
                    return _to_utc_z(parsed)
                except ValueError:
                    continue
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
    for fmt in _reorder_date_patterns(text, DATETIME_PATTERNS):
        try:
            parsed = datetime.strptime(text, fmt)
            return _to_utc_z(parsed)
        except ValueError:
            continue
    parsed = _parse_date(text)
    return f"{parsed}T00:00:00Z" if parsed else None


_EPOCH_MS_RE = re.compile(r"^\d{13}$")
_EPOCH_S_RE = re.compile(r"^\d{10}$")


@functools.lru_cache(maxsize=4096)
def _parse_date(value: str, *, with_time: bool = False) -> str | None:
    text = value.strip()
    if not text:
        return None
    if not _DATE_LIKE_RE.search(text):
        return None
    if text.lower() in NULL_SENTINELS:
        return None
    # Fail closed when 05/06/2024 could be May 6 or June 5 depending on locale.
    if _is_ambiguous_mdy_dmy(text):
        return None
    # Plain YYYYMMDD integer
    if re.match(r"^\d{8}$", text):
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    patterns = _reorder_date_patterns(text, DATE_PATTERNS)
    if with_time:
        patterns += _reorder_date_patterns(text, DATE_WITH_TIME_PATTERNS)
    for fmt in patterns:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

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


def _normalize_locale_separators(text: str) -> str | None:
    """Resolve . / , / space separator ambiguity into a decimal string.

    Returns None for unambiguous null sentinels and values that are not
    parseable numbers.
    """
    if text.lower() in NULL_SENTINELS:
        return None
    if not text:
        return None

    # Remove ASCII spaces used as thousands separators (e.g. "1 000 000").
    text = text.replace(" ", "").replace("\t", "")

    if "." in text and "," in text:
        last_dot = text.rfind(".")
        last_comma = text.rfind(",")
        if last_dot > last_comma:
            # Dot is the decimal separator; commas are thousands separators.
            candidate = text.replace(",", "")
            if candidate.count(".") <= 1:
                return candidate
            return None
        # Comma is the decimal separator; thousand dots are removed.
        text = text.replace(".", "")
        last_comma = text.rfind(",")
        candidate = text[:last_comma] + "." + text[last_comma + 1:]
        if "," in candidate or candidate.count(".") > 1:
            return None
        return candidate
    if "," in text:
        parts = text.split(",")
        if (
            len(parts) >= 2
            and parts[0]
            and not parts[0].startswith("0")
            and all(part.isdigit() and len(part) == 3 for part in parts[1:])
        ):
            return text.replace(",", "")
        # European decimal / decimal with 3-digit groups and a short final group.
        if (
            len(parts) >= 2
            and parts[0].isdigit()
            and all(part.isdigit() and len(part) == 3 for part in parts[1:-1])
            and parts[-1].isdigit()
            and 1 <= len(parts[-1]) <= 2
            and len(parts[0]) <= 3
        ):
            return "".join(parts[:-1]) + "." + parts[-1]
        if len(parts) == 2:
            return parts[0] + "." + parts[1]
        return None
    if "." in text:
        parts = text.split(".")
        # Multi-dot US/ISO thousands: 1.234.567 (but not 1.234, which is decimal).
        if (
            len(parts) > 2
            and parts[0]
            and not parts[0].startswith("0")
            and all(part.isdigit() and len(part) == 3 for part in parts[1:])
        ):
            return text.replace(".", "")
        if (
            len(parts) >= 2
            and parts[0].isdigit()
            and all(part.isdigit() and len(part) == 3 for part in parts[1:-1])
            and parts[-1].isdigit()
            and 1 <= len(parts[-1]) <= 2
            and len(parts[0]) <= 3
        ):
            return "".join(parts[:-1]) + "." + parts[-1]
        # Single dot is a decimal point (e.g. 1.234), not a thousands separator.
        return text
    return text


def _parse_decimal(value: str) -> str | None:
    text = value.strip()
    # Tuple / point / coordinate strings such as (1,2) or (1, 2) are not numbers.
    if text.startswith("(") and text.endswith(")") and "," in text and "." not in text:
        return None
    text = _normalize_numeric_text(text)
    text = _normalize_locale_separators(text)
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


def _parse_integer(value: str) -> int | None:
    text = _normalize_numeric_text(value.strip())
    text = _normalize_locale_separators(text)
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


# Strict boolean tokens only. Words like "active"/"inactive"/"enabled" are
# status *enums* in real datasets (Mongo sessions, CRM, auth) — treating them
# as booleans caused new Snowflake tables to CREATE status BOOLEAN, then
# hard-fail on values like "invalidated".
_STRICT_BOOL_TRUE = frozenset({"true", "t", "yes", "y", "1", "on"})
_STRICT_BOOL_FALSE = frozenset({"false", "f", "no", "n", "0", "off"})


def _parse_boolean(value: str) -> bool | None:
    text = value.strip().lower()
    if text in NULL_SENTINELS:
        return None
    if text in _STRICT_BOOL_TRUE:
        return True
    if text in _STRICT_BOOL_FALSE:
        return False
    return None


def _parse_json(value: Any) -> str | None:
    """Normalize a cell into JSON-valid text for a semi-structured target.

    Valid JSON (objects, arrays, numbers, booleans, quoted strings) is preserved
    and re-serialized compactly. Native Python containers are also serialized so
    database drivers that return parsed JSON objects round-trip deterministically.
    A bare scalar that is not valid JSON on its own is losslessly wrapped as a
    JSON string literal so it still loads into a VARIANT / JSON / SUPER column.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value, parse_constant=lambda v: None)
        except (json.JSONDecodeError, ValueError):
            parsed = value  # wrap the raw scalar as a JSON string literal
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


def _parse_uuid(value: str) -> str | None:
    text = value.strip()
    try:
        return str(uuid_lib.UUID(text))
    except ValueError:
        return None


def _hash_pii(value: str) -> str:
    """HMAC-SHA256 digest for PII masking. Requires DATAFLOW_PII_HASH_KEY in prod."""
    secret = os.getenv("DATAFLOW_PII_HASH_KEY") or os.getenv("DATAFLOW_SECRET")
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
    offsets, and AM/PM markers.
    """
    text = value.strip()
    if not text:
        return None
    text = text.upper().replace("Z", "+0000")
    for fmt in TIME_PATTERNS:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.time().isoformat()
        except ValueError:
            continue
    return None


KNOWN_TRANSFORMS = frozenset({
    "decimal", "integer", "boolean", "date", "datetime", "time", "json", "binary",
    "trim", "trim_id", "uuid", "upper", "lower", "hash_pii", "mask_pii", "none", "identity",
    "phone", "email", "url", "iban", "currency", "percentage", "postal", "base64",
    "strip_controls", "normalize_unicode",
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
    text = value.strip()
    if not text:
        return None
    try:
        base64.b64decode(text, validate=True)
        return text
    except Exception:
        return base64.b64encode(text.encode("utf-8")).decode("ascii")


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


def infer_transform_for_mapping(
    source_col: str,
    target_col: str,
    source_type: str,
    target_type: str | None = None,
    source_samples: list[str] | None = None,
) -> str:
    """Pick transform from source/target logical types, column semantics, and samples."""
    from services.type_system import normalize_logical_type

    from services.type_system import parse_numeric_precision_scale

    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if target_type else None
    tgt_name = target_col.lower()

    semantic = detect_semantic_type(source_col, source_samples)

    # Zero-scale DECIMAL/NUMBER targets (e.g. Snowflake NUMBER(38,0)) are integer
    # carriers, so an integer source should be coerced with the integer transform.
    if tgt == "decimal" and target_type:
        _p, _s = parse_numeric_precision_scale(target_type)
        if _s == 0 and src == "integer":
            return "integer"

    # Explicit, non-generic target type wins; if the source is already numeric
    # use a direct numeric transform, otherwise apply semantic transforms.
    if tgt and tgt not in {"string", "text"}:
        if tgt == "integer":
            # SQLite/MySQL/etc. store BOOLEAN as INTEGER. Coerce boolean sources
            # (and true/false text samples) with the boolean transform so remaps
            # do not invent active_text / null out the existing flag column.
            if src == "boolean" or _samples_prefer_boolean_over_integer(source_samples):
                return "boolean"
            return "integer"
        if tgt == "decimal":
            if src in {"string", "text", "unknown"} and semantic == "currency":
                return "currency"
            if src in {"string", "text", "unknown"} and semantic == "percentage":
                return "percentage"
            return "decimal"
        if tgt == "float":
            # Wire as decimal transform — IEEE float DDL is chosen by type_system.
            return "decimal"
        if tgt == "boolean":
            return "boolean"
        if tgt in {"json", "array"}:
            return "json"
        if tgt == "binary":
            return "binary"
        if tgt == "datetime":
            return "datetime"
        if tgt == "date":
            return "date"
        if tgt == "time":
            return "time"
        if tgt == "uuid":
            return "uuid"
        # Specialty types travel as identity text/binary payloads — never invent a cast.
        if tgt in {"interval", "geography", "vector"}:
            return "none"

    # Source type is the pivot when the target is generic (e.g., VARCHAR).
    if src == "integer":
        return "integer"
    if src == "decimal":
        return "decimal"
    if src == "float":
        return "decimal"
    if src == "boolean":
        return "boolean"
    if src in {"json", "array"}:
        return "json"
    if src == "binary":
        return "binary"
    if src == "datetime":
        return "datetime"
    if src == "date":
        return "date"
    if src == "time":
        return "time"
    if src == "uuid":
        return "uuid"
    if src in {"interval", "geography", "vector"}:
        return "none"

    # Semantic column names drive the transform for generic string targets.
    # For string/unknown targets, preserve currency/percentage as text to avoid
    # data loss (e.g. '$100' should not be silently stripped to 100).
    if semantic in {"currency", "percentage"}:
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
    if semantic == "timestamp":
        return "datetime"

    src_col = source_col.upper()
    if "amount" in tgt_name or "total" in tgt_name or "weight" in tgt_name or src_col in {
        "AMT", "PAY_AMT", "PAYMENT_AMT", "VALUE",
    }:
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


def apply_transform(raw: str | None, transform: str) -> tuple[Any, str | None]:
    """Returns (value, error).

    Explicit SQL/Dynamo NULL sentinels → None. Empty string is preserved for
    identity/string transforms so ``''`` ≠ SQL NULL on VARCHAR round-trips;
    typed transforms still coerce empty → None.
    """
    if raw is None:
        return None, None
    raw_s = str(raw)
    lowered = raw_s.strip().lower()
    # Explicit NULL sentinels must never land as literal strings in any dest.
    if lowered in {"__df_sql_null__", "__df_ddb_null__"}:
        return None, None

    text = raw_s.strip()
    transform_l = (transform or "none").strip().lower()

    # Identity / text transforms: empty string is a real value.
    _KEEP_EMPTY = frozenset({
        "none", "identity", "passthrough", "string", "varchar", "text",
        "upper", "lower", "trim", "trim_id",
        "strip_controls", "normalize_unicode",
    })
    if text == "" and transform_l in _KEEP_EMPTY:
        return "", None
    if text == "":
        return None, None

    # Null/missing sentinels for typed transforms are treated as None.
    if transform_l in {"decimal", "integer", "boolean", "date", "datetime", "time", "json", "uuid", "binary"}:
        if text.lower() in NULL_SENTINELS:
            return None, None

    if transform == "decimal":
        parsed = _parse_decimal(text)
        if parsed is None:
            return None, f"Invalid decimal: {text!r}"
        return parsed, None

    if transform == "integer":
        parsed_int = _parse_integer(text)
        if parsed_int is None:
            return None, f"Invalid integer: {text!r}"
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

    if transform == "time":
        parsed = _parse_time(text)
        if parsed is None:
            return None, f"Invalid time: {text!r}"
        return parsed, None

    if transform == "json":
        json_input = raw if isinstance(raw, (dict, list, tuple, set, frozenset)) else text
        parsed_json = _parse_json(json_input)
        if parsed_json is None:
            return None, f"Invalid JSON: {text!r}"
        return parsed_json, None

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

    if transform in {"none", "identity"}:
        return text, None

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
            return text, f"Invalid {transform}: {text!r}"
        return str(converted) if not target_string and isinstance(converted, Decimal) else converted, None

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

    from services.transform_resolver import resolve_transform

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

    from services.transform_resolver import resolve_transform

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
