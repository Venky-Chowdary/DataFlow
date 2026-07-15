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
from decimal import Decimal, InvalidOperation
from typing import Any

from services.value_serializer import json_default
from services.semantic_types import SemanticType, normalize_value_for_target, detect_semantic_type

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


def _to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _detect_dayfirst(text: str) -> bool | None:
    """Return True for day-first ordering, False for month-first, or None if ambiguous.

    Looks at the first two numeric fields of slash/dash/dot-delimited dates.
    A value like 31/12/2024 or 31.12.2024 is unambiguously day-first;
    12/31/2024 or 12-31-24 is month-first.  When both fields are <= 12 we keep
    the default (month-first) to stay compatible with existing data.
    """
    m = re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})(?:[ T].*)?$", text)
    if not m:
        return None
    first, second = int(m.group(1)), int(m.group(2))
    if first > 12:
        return True
    if second > 12:
        return False
    return None


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


@functools.lru_cache(maxsize=4096)
def _parse_datetime(value: str) -> str | None:
    text = value.strip()
    if not _DATE_LIKE_RE.search(text):
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
    except InvalidOperation:
        return None

    # Percent is parsed as the numeric value (50% -> 50) to preserve magnitude.
    # Scientific notation: 1.5e3, 2E-4. Convert to fixed-point form so
    # downstream numeric checks stay stable and comparable across formats.
    if "e" in text.lower():
        fixed = format(dec, "f")
        if "." in fixed:
            fixed = fixed.rstrip("0").rstrip(".")
        return fixed or "0"
    return str(dec)


def _parse_integer(value: str) -> int | None:
    text = _normalize_numeric_text(value.strip())
    text = _normalize_locale_separators(text)
    if text is None or text == "":
        return None
    if re.match(r"^-?\d+(\.\d+)?[eE][+-]?\d+$", text):
        try:
            dec = Decimal(text)
            if dec != dec.to_integral_value():
                return None
            return int(dec)
        except (InvalidOperation, ValueError):
            return None
    try:
        dec = Decimal(text)
    except InvalidOperation:
        return None
    if dec != dec.to_integral_value():
        return None
    return int(dec)


def _parse_boolean(value: str) -> bool | None:
    text = value.strip().lower()
    if text in NULL_SENTINELS:
        return None
    if text in {"true", "t", "yes", "y", "1", "on", "enabled", "active", "ok", "aye", "positive"}:
        return True
    if text in {"false", "f", "no", "n", "0", "off", "disabled", "inactive", "nope", "negative"}:
        return False
    return None


def _parse_json(value: str) -> str | None:
    try:
        parsed = json.loads(value, parse_constant=lambda v: None)
    except json.JSONDecodeError:
        return None
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"), default=json_default, allow_nan=False)


def _parse_uuid(value: str) -> str | None:
    text = value.strip()
    try:
        return str(uuid_lib.UUID(text))
    except ValueError:
        return None


def _hash_pii(value: str) -> str:
    secret = os.getenv("DATAFLOW_PII_HASH_KEY", os.getenv("DATAFLOW_SECRET", "dataflow-dev-key"))
    digest = hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest[:32]


KNOWN_TRANSFORMS = frozenset({
    "decimal", "integer", "boolean", "date", "datetime", "json", "binary",
    "trim", "trim_id", "uuid", "upper", "lower", "hash_pii", "none", "identity",
    "phone", "email", "url", "iban", "currency", "percentage", "postal", "base64",
})


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


def infer_transform_for_mapping(
    source_col: str,
    target_col: str,
    source_type: str,
    target_type: str | None = None,
    source_samples: list[str] | None = None,
) -> str:
    """Pick transform from source/target logical types, column semantics, and samples."""
    from services.type_system import normalize_logical_type

    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if target_type else None
    tgt_name = target_col.lower()

    semantic = detect_semantic_type(source_col, source_samples)

    # Explicit, non-generic target type wins; if the source is already numeric
    # use a direct numeric transform, otherwise apply semantic transforms.
    if tgt and tgt not in {"string", "text"}:
        if tgt == "integer":
            return "integer"
        if tgt == "decimal":
            if src in {"string", "text", "unknown"} and semantic == "currency":
                return "currency"
            if src in {"string", "text", "unknown"} and semantic == "percentage":
                return "percentage"
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
        if tgt == "uuid":
            return "uuid"

    # Source type is the pivot when the target is generic (e.g., VARCHAR).
    if src == "integer":
        return "integer"
    if src == "decimal":
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
    if src == "uuid":
        return "uuid"

    # Semantic column names drive the transform for generic string targets.
    # For string/unknown targets, preserve currency/percentage as text to avoid
    # data loss (e.g. '$100' should not be silently stripped to 100).
    if semantic in {"currency", "percentage"}:
        return "trim"
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
    if (
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
    return "trim"


def apply_transform(raw: str | None, transform: str) -> tuple[Any, str | None]:
    """Returns (value, error). Empty string becomes None for nullable columns."""
    if raw is None:
        return None, None
    text = str(raw).strip()
    if text == "":
        return None, None

    # Null/missing sentinels for typed transforms are treated as None.
    if transform in {"decimal", "integer", "boolean", "date", "datetime", "json", "uuid", "binary"}:
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

    if transform == "json":
        parsed_json = _parse_json(text)
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
        return _hash_pii(text), None

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
) -> tuple[bool, list[str]]:
    if not sample_rows:
        return False, ["No sample rows available for dry-run validation"]

    errors: list[str] = []
    source_idx = {h: i for i, h in enumerate(headers)}

    for m in mappings:
        idx = source_idx.get(m["source"])
        if idx is None:
            errors.append(f"Source column missing: {m['source']}")
            continue
        transform = m.get("transform") or infer_transform_for_mapping(
            m["source"],
            m["target"],
            column_types.get(m["source"], "VARCHAR"),
            m.get("target_type") or column_types.get(m["target"]),
            source_samples=[str(r[idx]) for r in sample_rows[:sample_size] if idx < len(r)],
        )
        for row in sample_rows[:sample_size]:
            raw = row[idx] if idx < len(row) else ""
            _, err = apply_transform(raw, transform)
            if err:
                errors.append(f"{m['source']}→{m['target']}: {err}")
                break

    return len(errors) == 0, errors[:25]
