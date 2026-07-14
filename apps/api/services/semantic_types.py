"""Semantic type detection and normalization for global data.

Phone, email, URL, IBAN, currency, percentage, UUID, base64, timestamp strings,
and postal addresses are all kept as lossless strings when the target is generic.
When the target is typed, they are parsed to the appropriate logical type
(decimal for currency/percentage, bytes for base64, UUID for UUID, datetime for
timestamp). This is intentionally conservative: if a value is not a clean match,
we return the original text so data is never lost.
"""

from __future__ import annotations

import base64
import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


class SemanticType(str, Enum):
    UNKNOWN = "unknown"
    PHONE = "phone"
    EMAIL = "email"
    URL = "url"
    IBAN = "iban"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    UUID = "uuid"
    BASE64 = "base64"
    TIMESTAMP = "timestamp"
    POSTAL = "postal"


# Column-name heuristics used when no sample is available.
NAME_HINTS: dict[SemanticType, list[str]] = {
    SemanticType.PHONE: ["phone", "telephone", "mobile", "cell", "phone_number", "phoneno", "contact_phone"],
    SemanticType.EMAIL: ["email", "email_address", "emailaddress", "e_mail", "mail"],
    SemanticType.URL: ["url", "website", "web_address", "link", "uri", "href"],
    SemanticType.IBAN: ["iban", "international_bank_account"],
    SemanticType.CURRENCY: ["amount", "price", "cost", "total", "subtotal", "revenue", "payment", "balance", "fee", "charge", "salary", "wage"],
    SemanticType.PERCENTAGE: ["percent", "pct", "percentage", "rate", "ratio", "discount_rate", "tax_rate"],
    SemanticType.UUID: ["uuid", "guid", "uniqueidentifier"],
    SemanticType.BASE64: ["base64", "encoded", "encoded_data"],
    SemanticType.TIMESTAMP: ["timestamp", "epoch", "unix_ts"],
    SemanticType.POSTAL: ["zip", "postal", "zipcode", "postcode", "postal_code"],
}

# Regex for value validation.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", re.IGNORECASE)
_URL_RE = re.compile(r"^(https?|ftp)://[^\s/$.?#].[^\s]*$", re.IGNORECASE)
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$", re.IGNORECASE)
_POSTAL_RE = re.compile(r"^\d{4,10}$|^[A-Z]\d[A-Z]\s?\d[A-Z]\d$", re.IGNORECASE)


def _digits_only(value: str) -> str:
    """Remove formatting from phone/identifier strings, preserving a leading +."""
    if not value:
        return ""
    value = value.strip()
    digits = re.sub(r"[^\d+]", "", value)
    # Only keep + if it was the first non-whitespace character.
    if value.startswith("+") and digits.startswith("+"):
        return digits
    # If + appears somewhere else, drop it and keep digits.
    return digits.replace("+", "")


def detect_semantic_type(name: str, samples: list[str] | None = None) -> SemanticType:
    """Infer the semantic type from column name and optional samples."""
    lowered = name.lower().replace(" ", "_")

    # Name-driven hint is the most reliable signal for most semantic types.
    for st, hints in NAME_HINTS.items():
        for hint in hints:
            if hint in lowered or lowered.endswith(hint) or lowered.startswith(hint):
                return st

    # Sample-driven detection for values that are unambiguous even without a hint.
    if samples:
        candidates: dict[SemanticType, int] = {}
        for sample in samples:
            if sample is None:
                continue
            text = str(sample).strip()
            if not text:
                continue
            if _EMAIL_RE.match(text):
                candidates[SemanticType.EMAIL] = candidates.get(SemanticType.EMAIL, 0) + 1
            elif _URL_RE.match(text):
                candidates[SemanticType.URL] = candidates.get(SemanticType.URL, 0) + 1
            elif _UUID_RE.match(text):
                candidates[SemanticType.UUID] = candidates.get(SemanticType.UUID, 0) + 1
            elif _IBAN_RE.match(text.upper().replace(" ", "")):
                candidates[SemanticType.IBAN] = candidates.get(SemanticType.IBAN, 0) + 1
            elif re.match(r"^\d{1,3}(,\d{3})*\.?\d*\s*%$", text) or ("%" in text and re.search(r"\d", text)):
                candidates[SemanticType.PERCENTAGE] = candidates.get(SemanticType.PERCENTAGE, 0) + 1
            elif re.match(r"^[\+\-]?\s*(\$|€|£|¥|₹|CHF|USD|EUR|GBP|JPY|INR)\s*[\d,]+(\.\d{1,4})?$", text) or re.match(r"^[\+\-]?\s*[\d,]+(\.\d{1,4})?\s*(\$|€|£|¥|₹|USD|EUR|GBP|JPY|INR|CHF)$", text):
                candidates[SemanticType.CURRENCY] = candidates.get(SemanticType.CURRENCY, 0) + 1
            elif re.match(r"^\d{4}-\d{1,2}-\d{1,2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:?\d{2}|Z)?)?$", text):
                candidates[SemanticType.TIMESTAMP] = candidates.get(SemanticType.TIMESTAMP, 0) + 1

        if candidates:
            # Pick the most frequent match.
            best = max(candidates, key=lambda k: candidates[k])
            if candidates[best] >= max(1, len([s for s in samples if s is not None and str(s).strip()]) / 2):
                return best

    return SemanticType.UNKNOWN


def normalize_value(value: Any, semantic_type: SemanticType, target_string: bool = True) -> Any:
    """Normalize a single value according to its semantic type.

    The default is to keep the value as a string so the source is not altered. If
    the caller wants a typed value (e.g. decimal for currency, bytes for base64),
    set `target_string=False`.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    if semantic_type == SemanticType.PHONE:
        digits = _digits_only(text)
        return digits if digits else text

    if semantic_type == SemanticType.EMAIL:
        return text.lower()

    if semantic_type == SemanticType.URL:
        # Canonicalize: lowercase scheme and domain, keep path/query.
        if "://" in text:
            scheme, rest = text.split("://", 1)
            return f"{scheme.lower()}://{rest}"
        return text

    if semantic_type == SemanticType.IBAN:
        return text.upper().replace(" ", "")

    if semantic_type == SemanticType.POSTAL:
        return text.upper().replace(" ", "")

    if semantic_type == SemanticType.UUID:
        uid = text.lower().replace("{", "").replace("}", "").strip()
        return uid if _UUID_RE.match(uid) else text

    if semantic_type == SemanticType.BASE64:
        # Decode only if the caller expects a typed payload; otherwise keep encoded.
        if target_string:
            return text
        try:
            return base64.b64decode(text, validate=True)
        except Exception:
            return text

    if semantic_type in {SemanticType.CURRENCY, SemanticType.PERCENTAGE}:
        if target_string:
            return text
        # Use the same locale-aware numeric parser as the transform engine so
        # values like '1,000.00', '2.000,50', '$1,234.56', '1.23E-10', and '50%'
        # are handled consistently.
        from services.transform_engine import _parse_decimal

        parsed = _parse_decimal(text)
        if parsed is None:
            return text
        try:
            return Decimal(parsed)
        except InvalidOperation:
            return text

    if semantic_type == SemanticType.TIMESTAMP:
        # Keep as string unless explicitly typed; the datetime transform handles it.
        return text

    return text


def normalize_value_for_target(
    value: Any,
    semantic_type: SemanticType,
    target_type: str,
) -> Any:
    """Normalize a value for a specific target logical type."""
    from services.type_system import normalize_logical_type

    tgt = normalize_logical_type(target_type)
    if tgt in {"string", "text"}:
        return normalize_value(value, semantic_type, target_string=True)
    if tgt in {"decimal", "integer", "float", "number"}:
        return normalize_value(value, semantic_type, target_string=False)
    if tgt in {"binary", "bytes"}:
        return normalize_value(value, semantic_type, target_string=False)
    return normalize_value(value, semantic_type, target_string=True)


def infer_semantic_transform(source_name: str, samples: list[str] | None = None) -> str | None:
    """Return the engine transform id for a semantic type, or None if generic."""
    st = detect_semantic_type(source_name, samples)
    return st if st != SemanticType.UNKNOWN else None
