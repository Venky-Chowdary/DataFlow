"""Deterministic PII and compliance-risk detection for data mappings.

This is a rule-based safety layer: it flags sensitive fields by name and by
sample-value pattern, then returns an auditable score that can gate transfer
execution or require human review.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from decimal import Decimal
from typing import Any

_logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# Digits plus the separators printed numbers use; a leading ``(`` is an area
# code, ``(555) 123-4567`` (D94). The digit count is checked separately
# against E.164's 7..15 so the shape alone never decides.
PHONE_RE = re.compile(r"^\+?[0-9(][0-9\s().-]{5,22}[0-9]$")
# Four dot-separated 1-3 digit groups: an IPv4 (validated) or a version /
# malformed address — never a phone, though it has phone-like digit count.
_DOTTED_QUAD_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
# ICAO 9303 TD3 (passport) machine-readable zone, 44 characters per line.
_MRZ_LINE1_RE = re.compile(r"^P[A-Z<][A-Z][A-Z<]{2}[A-Z<]{39}$")
_MRZ_LINE2_RE = re.compile(r"^[A-Z0-9<]{9}\d[A-Z][A-Z<]{2}\d{7}[MFX<]\d{7}[A-Z0-9<]{14}[0-9<]\d$")
SSN_RE = re.compile(r"^\d{3}-\d{2}-\d{4}$")
# Calendar shapes (ISO / slash) are not PII by themselves. DOB is name-gated
# in ``_NAME_PATTERN_GROUPS`` — Fivetran / Airbyte / Collibra classifiers treat
# date-of-birth as a semantic class, not "any 01/02/2024". Value-only matching
# invented a HIPAA hold on ``event_date`` / ``created_at``.
# PCI PAN is its own class: a bare 8-19 digit run is NOT an account number
# (every order id matched). Cards are Luhn-valid 13-19 digits, tolerant of
# space/dash separators — same classifier as services.pii_guard (C28).
ACCOUNT_RE = re.compile(r"^[A-Z]{2}\d{12,30}$")

_ACCOUNT_SURROGATE_NAME_RE = re.compile(r"account_?id\b")

_TEMPORAL_VALUE_TYPES = frozenset({"DATE", "TIMESTAMP", "TIMESTAMPTZ"})

_NAME_PATTERN_GROUPS: dict[str, tuple[tuple[str, ...], float]] = {
    "email": ((r"email", r"e_mail", r"\bmail\b"), 0.22),
    "phone": ((r"phone", r"mobile", r"\btel\b", r"contact_?(?:phone|number|no)"), 0.22),
    "name": ((
        r"\bname\b",
        r"first_?name",
        r"last_?name",
        r"full_?name",
        r"contact_?name",
        r"customer_?name",
        r"user_?name",
        r"display_?name",
    ), 0.18),
    "address": ((
        r"address",
        r"street",
        r"\bcity\b",
        r"\bzip\b",
        r"postal",
        r"country(?:_?code)?\b",
    ), 0.18),
    "dob": ((r"\bdob\b", r"birth", r"date_of_birth"), 0.42),
    # Online identifier under GDPR. ``ip`` must be a whole snake_case token:
    # ``zip`` / ``tip`` / ``skip_count`` are not addresses.
    "ip_address": ((
        r"(?:^|_)ip(?:_|$)",
        r"ip_?addr",
        r"\bipv[46]\b",
        r"remote_?addr",
    ), 0.18),
    # Government travel-document number: as regulated as an SSN (D94).
    "passport": ((r"passport", r"travel_?doc"), 0.5),
    "ssn": ((r"\bssn\b", r"social_security", r"tax_id", r"\btin\b"), 0.5),
    # PCI/financial account — NOT boolean flags like accountLocked / accountActive.
    "account": ((
        r"account_?(?:number|no|num|id)\b",
        r"bank_?account",
        r"routing_?(?:number|no|num)?\b",
        r"\biban\b",
        r"\bswift\b",
    ), 0.4),
    # PCI card data — own class (C28). Card number / PAN / CVV columns are
    # PCI-DSS scope regardless of sampled values.
    "pci_pan": ((
        r"card_?(?:number|no|num)\b",
        r"credit_?card",
        r"debit_?card",
        r"\bpan\b",
        r"\bcvv\b",
        r"card_?holder",
        r"expir(?:y|ation)_?(?:date|month|year)?\b",
    ), 0.45),
    # High-signal regulated IDs only — bare ``id`` / ``*Id`` matches every CRM field.
    # Technical UUID/GUID surrogate keys are not PCI/HIPAA identifiers.
    "identifier": ((
        r"policy_?(?:no|number|num)\b",
        r"member_?id\b",
        r"\bmrn\b",
        r"record_?id\b",
        r"patient_?id\b",
    ), 0.32),
    # Compensation is regulated the same way a date of birth is: a load of
    # full_name + salary must not skip the acknowledgement a birth_date load
    # already requires (QA RT5-06 / R2-036).
    "salary": ((
        r"\bsalary\b",
        r"compensation",
        r"\bwage\b",
        r"pay_?rate",
    ), 0.4),
}

# Categories that alone force compliance review.
_HIGH_RISK_CATEGORIES = frozenset({"ssn", "passport", "dob", "account", "salary", "pci_pan"})

# Personal names, not technical surrogates. ``username`` matches the name
# category via user_name and must stay a medium signal — forcing review on
# every login column is a different policy.
_HIGH_RISK_NAME_RE = re.compile(
    r"(?:^|_)(?:full_?name|first_?name|last_?name|contact_?name|customer_?name|display_?name)(?:_|$)"
)


def _field_hits(name: str) -> list[str]:
    lowered = name.lower()
    hits: list[str] = []
    for field, (patterns, _) in _NAME_PATTERN_GROUPS.items():
        if any(re.search(pat, lowered) for pat in patterns):
            hits.append(field)
    return hits


def _is_ip_address(text: str) -> bool:
    """Canonical IPv4 (octets 0-255, no leading zeros) or IPv6 literal."""
    if "." not in text and ":" not in text:
        return False
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def _is_phone(text: str) -> bool:
    if not PHONE_RE.match(text) or _DOTTED_QUAD_RE.match(text):
        return False
    from services.pii_guard import _is_decimal_literal

    # ``12345.67`` is an amount, not a phone (X-26 for the log masker).
    if _is_decimal_literal(text):
        return False
    return 7 <= sum(ch.isdigit() for ch in text) <= 15


def _is_bare_digits(text: str) -> bool:
    """``5551234567`` — a phone, an order number and a count all look like this."""
    return text.isascii() and text.isdigit()


#: Share of non-empty sampled values that must be phone-shaped before a column
#: of bare digit runs counts as phones without a name hint (QA round 11).
_BARE_PHONE_SHARE = 0.8

_NUMERIC_TYPE_RE = re.compile(
    r"^(?:big|small|tiny|medium)?int(?:eger)?\d*\b|^(?:numeric|decimal|number|"
    r"float\d*|double|real|money|serial|bigserial|smallserial)\b|^u?int\d+$",
    re.I,
)


def _numeric_column(raw_values: list[Any], declared_type: str = "") -> bool:
    """The column holds numbers: a declared numeric type, or native numeric cells."""
    if declared_type and _NUMERIC_TYPE_RE.match(declared_type.strip()):
        return True
    present = [v for v in raw_values if v is not None and not (isinstance(v, str) and not v.strip())]
    return bool(present) and all(
        isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in present
    )


def _bare_digits_may_be_phone(
    name_hits: list[str], values: list[str], raw_values: list[Any], declared_type: str
) -> bool:
    """Whether a bare digit run in this column is evidence of a phone number.

    A single 7-15 digit value used to flag the column, so counts
    (``num_sequences = 12345678``) and order numbers read as phones and forced
    a PII acknowledgment (QA round 11, RNAcentral). Bare digits now count only
    under a phone-hinting name, or when most non-empty values look like a phone
    and the column does not hold numbers. Formatted values (``+1 555 …``,
    ``(555) 123-4567``) are phones on their own.
    """
    if "phone" in name_hits:
        return True
    if _numeric_column(raw_values, declared_type):
        return False
    non_empty = [v for v in values if v]
    if not non_empty:
        return False
    phone_like = sum(1 for v in non_empty if _is_bare_digits(v) and _is_phone(v))
    return phone_like / len(non_empty) >= _BARE_PHONE_SHARE


def _mrz_check_digit(field: str) -> int:
    """ICAO 9303 check digit: weights 7-3-1, A-Z = 10..35, ``<`` = 0."""
    total = 0
    for i, ch in enumerate(field):
        if ch.isdigit():
            value = int(ch)
        elif "A" <= ch <= "Z":
            value = ord(ch) - 55
        else:
            value = 0
        total += value * (7, 3, 1)[i % 3]
    return total % 10


def _is_passport_mrz(text: str) -> bool:
    """A TD3 MRZ line. Line 2 must pass every check digit, so a 44-character
    code that merely has the shape is not a passport. A bare passport number
    (``K1234567``) is indistinguishable from a SKU and is name-gated."""
    if _MRZ_LINE1_RE.match(text):
        return "<<" in text[5:]
    if not _MRZ_LINE2_RE.match(text):
        return False
    digit = _mrz_check_digit
    if text[42] == "<":
        optional_ok = set(text[28:42]) == {"<"}
    else:
        optional_ok = digit(text[28:42]) == int(text[42])
    return (
        optional_ok
        and digit(text[0:9]) == int(text[9])
        and digit(text[13:19]) == int(text[19])
        and digit(text[21:27]) == int(text[27])
        and digit(text[0:10] + text[13:20] + text[21:43]) == int(text[43])
    )


def _value_hits(values: list[str], *, bare_digit_phones: bool = True) -> list[str]:
    hits: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if EMAIL_RE.match(text):
            hits.append("email")
        elif SSN_RE.match(text):
            hits.append("ssn")
        elif _is_ip_address(text):
            hits.append("ip_address")
        elif _is_passport_mrz(text):
            hits.append("passport")
        else:
            from services.pii_guard import luhn_valid

            # A Luhn-valid 13-19 digit value is a PAN, not a phone/account —
            # the phone shape matches card numbers, so PCI wins precedence.
            if luhn_valid(text):
                hits.append("pci_pan")
            elif _is_phone(text) and (bare_digit_phones or not _is_bare_digits(text)):
                hits.append("phone")
            elif ACCOUNT_RE.match(text):
                hits.append("account")
    return list(dict.fromkeys(hits))


def _temporal_column(col: str, values: list[str]) -> bool:
    """Every sampled value is a date / instant, so its digits are not a phone.

    ``1990-05-20``, ``20240115`` and epoch-ms ``1705312200000`` all match the
    phone/account digit shapes. Formatted dates count as temporal on their
    own; bare digits only under a temporal column name, because a 10-digit
    phone number is epoch-shaped too. Name rules (``birth_date`` → dob) still
    apply to the column.
    """
    from services.schema_inference import _is_date_field_name, classify_value

    non_empty = [v for v in values if v]
    if not non_empty:
        return False
    temporal_name = _is_date_field_name(col)
    for value in non_empty:
        if value.lstrip("+-").isdigit() and not temporal_name:
            return False
        if classify_value(value, field_name=col) not in _TEMPORAL_VALUE_TYPES:
            return False
    return True


def detect_pii_fields(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
    column_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Identify columns that are likely sensitive or regulated.

    ``column_types`` (source column → declared type) lets a numeric column's
    digits be read as numbers even when the sample carries them as text.

    Returns a structured report with the sensitive fields, their detected risk
    categories, and a coarse risk level suitable for preflight gating.
    """
    sample_rows = (rows or [])[:sample_limit]
    sensitive_fields: list[str] = []
    field_risk: dict[str, list[str]] = {}
    high_risk_fields: list[str] = []

    field_evidence: dict[str, list[dict[str, str]]] = {}

    for col in columns:
        name_hits = _field_hits(col)
        raw_values = [row.get(col) for row in sample_rows if col in row]
        sample_values = [
            "" if value is None else str(value).strip() for value in raw_values
        ]
        value_hits = _value_hits(
            sample_values,
            bare_digit_phones=_bare_digits_may_be_phone(
                name_hits, sample_values, raw_values, str((column_types or {}).get(col) or "")
            ),
        )
        if value_hits and _temporal_column(col, sample_values):
            _logger.info(
                "PII value pattern(s) %s on column %r ignored: every sampled value "
                "is a date/timestamp",
                value_hits,
                col,
            )
            value_hits = []
        non_empty = [v for v in sample_values if v]
        if (
            "account" in name_hits
            and "account" not in value_hits
            and non_empty
            and _ACCOUNT_SURROGATE_NAME_RE.search(col.lower())
            and not any(ACCOUNT_RE.match(v) for v in non_empty)
        ):
            # ``account_id`` is usually a surrogate key. Sampled values that do
            # not look like a bank/card number keep it a regulated identifier
            # (PCI tag, visible) instead of a high-risk account number that
            # forces review on a synthetic numeric table.
            name_hits = ["identifier" if h == "account" else h for h in name_hits]
        categories = list(dict.fromkeys(name_hits + value_hits))
        if not categories:
            continue
        sensitive_fields.append(col)
        field_risk[col] = categories
        field_evidence[col] = [
            {"category": cat, "rule": "column_name"} for cat in dict.fromkeys(name_hits)
        ] + [
            # Content is evidence in its own right, also where the name agrees:
            # "phone by column name" alone read as a name-only detector (D94).
            {"category": cat, "rule": "sample_value"}
            for cat in value_hits
        ]
        personal_name = "name" in categories and bool(
            _HIGH_RISK_NAME_RE.search(col.lower())
        )
        if personal_name or any(
            cat in _HIGH_RISK_CATEGORIES
            for cat in categories
        ):
            high_risk_fields.append(col)

    risk_level = "low"
    if high_risk_fields:
        risk_level = "high"
    elif sensitive_fields:
        risk_level = "medium"

    return {
        "sensitive_fields": sorted(sensitive_fields),
        "field_risk": field_risk,
        "high_risk_fields": sorted(high_risk_fields),
        "risk_level": risk_level,
        "sensitive_count": len(sensitive_fields),
        "field_evidence": field_evidence,
    }


def score_compliance_risk(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
    column_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Score the compliance and privacy risk of a dataset.

    The score is deterministic and bounded to [0.0, 1.0]. The result is meant
    for preflight review, not for claiming legal compliance.
    """
    pii_report = detect_pii_fields(
        columns, rows, sample_limit=sample_limit, column_types=column_types
    )
    sensitive_fields = pii_report["sensitive_fields"]
    field_risk = pii_report["field_risk"]

    weight_by_category = {
        "email": 0.18,
        "phone": 0.18,
        "name": 0.14,
        "address": 0.16,
        "dob": 0.42,
        "ssn": 0.5,
        "passport": 0.5,
        "ip_address": 0.18,
        "account": 0.4,
        "pci_pan": 0.45,
        "identifier": 0.28,
        "salary": 0.4,
    }

    total_weight = 0.0
    high_risk_weight = 0.0
    for col in sensitive_fields:
        for category in field_risk.get(col, []):
            weight = weight_by_category.get(category, 0.12)
            total_weight += weight
            if category in _HIGH_RISK_CATEGORIES:
                high_risk_weight += weight

    if not sensitive_fields:
        risk_score = 0.0
    else:
        base = total_weight / max(len(sensitive_fields), 1)
        uplift = min(0.18, high_risk_weight / max(len(sensitive_fields), 1) * 0.4)
        risk_score = min(1.0, round(base + uplift, 3))

    risk_level = "low"
    if risk_score >= 0.45 or pii_report["high_risk_fields"]:
        risk_level = "high" if risk_score >= 0.6 or pii_report["high_risk_fields"] else "medium"

    compliance_tags: list[str] = []
    if any("ssn" in field_risk.get(col, []) or "dob" in field_risk.get(col, []) for col in sensitive_fields):
        compliance_tags.append("HIPAA")
    if any(
        {"account", "identifier", "pci_pan"} & set(field_risk.get(col, []))
        for col in sensitive_fields
    ):
        compliance_tags.append("PCI-DSS")
    if sensitive_fields:
        compliance_tags.append("PII")

    requires_review = risk_score >= 0.45 or bool(pii_report["high_risk_fields"])
    evidence = pii_report.get("field_evidence") or {}
    findings = [
        {
            "column": col,
            "categories": list(field_risk.get(col, [])),
            "high_risk": col in pii_report["high_risk_fields"],
            "evidence": list(evidence.get(col, [])),
        }
        for col in sensitive_fields
    ]

    return {
        "risk_score": risk_score,
        "risk_level": risk_level,
        "requires_review": requires_review,
        "sensitive_fields": sensitive_fields,
        "high_risk_fields": pii_report["high_risk_fields"],
        "field_risk": field_risk,
        "findings": findings,
        "compliance_tags": sorted(set(compliance_tags)),
    }
