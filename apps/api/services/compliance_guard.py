"""Deterministic PII and compliance-risk detection for data mappings.

This is a rule-based safety layer: it flags sensitive fields by name and by
sample-value pattern, then returns an auditable score that can gate transfer
execution or require human review.
"""

from __future__ import annotations

import re
from typing import Any

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
PHONE_RE = re.compile(r"^\+?[0-9][0-9\s().-]{6,18}[0-9]$")
SSN_RE = re.compile(r"^\d{3}-\d{2}-\d{4}$")
# Calendar shapes (ISO / slash) are not PII by themselves. DOB is name-gated
# in ``_NAME_PATTERN_GROUPS`` — Fivetran / Airbyte / Collibra classifiers treat
# date-of-birth as a semantic class, not "any 01/02/2024". Value-only matching
# invented a HIPAA hold on ``event_date`` / ``created_at``.
ACCOUNT_RE = re.compile(r"^(?:\d{8,19}|[A-Z]{2}\d{12,30})$")

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
    "ssn": ((r"\bssn\b", r"social_security", r"tax_id", r"\btin\b"), 0.5),
    # PCI/financial account — NOT boolean flags like accountLocked / accountActive.
    "account": ((
        r"account_?(?:number|no|num|id)\b",
        r"bank_?account",
        r"routing_?(?:number|no|num)?\b",
        r"\biban\b",
        r"\bswift\b",
        r"\bcvv\b",
        r"card_?(?:number|no|num)\b",
        r"credit_?card",
        r"debit_?card",
        r"pan\b",
    ), 0.4),
    # High-signal regulated IDs only — bare ``id`` / ``*Id`` matches every CRM field.
    "identifier": ((
        r"\buuid\b",
        r"\bguid\b",
        r"policy_?(?:no|number|num)\b",
        r"member_?id\b",
        r"\bmrn\b",
        r"record_?id\b",
        r"patient_?id\b",
    ), 0.32),
}


def _field_hits(name: str) -> list[str]:
    lowered = name.lower()
    hits: list[str] = []
    for field, (patterns, _) in _NAME_PATTERN_GROUPS.items():
        if any(re.search(pat, lowered) for pat in patterns):
            hits.append(field)
    return hits


def _value_hits(values: list[str]) -> list[str]:
    hits: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if EMAIL_RE.match(text):
            hits.append("email")
        elif PHONE_RE.match(text):
            hits.append("phone")
        elif SSN_RE.match(text):
            hits.append("ssn")
        elif ACCOUNT_RE.match(text):
            hits.append("account")
    return list(dict.fromkeys(hits))


def detect_pii_fields(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
) -> dict[str, Any]:
    """Identify columns that are likely sensitive or regulated.

    Returns a structured report with the sensitive fields, their detected risk
    categories, and a coarse risk level suitable for preflight gating.
    """
    sample_rows = (rows or [])[:sample_limit]
    sensitive_fields: list[str] = []
    field_risk: dict[str, list[str]] = {}
    high_risk_fields: list[str] = []

    for col in columns:
        name_hits = _field_hits(col)
        sample_values = [str(row.get(col, "")).strip() for row in sample_rows if col in row]
        value_hits = _value_hits(sample_values)
        categories = list(dict.fromkeys(name_hits + value_hits))
        if not categories:
            continue
        sensitive_fields.append(col)
        field_risk[col] = categories
        if any(cat in {"ssn", "dob", "account"} for cat in categories):
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
    }


def score_compliance_risk(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
) -> dict[str, Any]:
    """Score the compliance and privacy risk of a dataset.

    The score is deterministic and bounded to [0.0, 1.0]. The result is meant
    for preflight review, not for claiming legal compliance.
    """
    pii_report = detect_pii_fields(columns, rows, sample_limit=sample_limit)
    sensitive_fields = pii_report["sensitive_fields"]
    field_risk = pii_report["field_risk"]

    weight_by_category = {
        "email": 0.18,
        "phone": 0.18,
        "name": 0.14,
        "address": 0.16,
        "dob": 0.42,
        "ssn": 0.5,
        "account": 0.4,
        "identifier": 0.28,
    }

    total_weight = 0.0
    high_risk_weight = 0.0
    for col in sensitive_fields:
        for category in field_risk.get(col, []):
            weight = weight_by_category.get(category, 0.12)
            total_weight += weight
            if category in {"ssn", "dob", "account"}:
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
    if any("account" in field_risk.get(col, []) or "identifier" in field_risk.get(col, []) for col in sensitive_fields):
        compliance_tags.append("PCI-DSS")
    if sensitive_fields:
        compliance_tags.append("PII")

    requires_review = risk_score >= 0.45 or bool(pii_report["high_risk_fields"])

    return {
        "risk_score": risk_score,
        "risk_level": risk_level,
        "requires_review": requires_review,
        "sensitive_fields": sensitive_fields,
        "high_risk_fields": pii_report["high_risk_fields"],
        "field_risk": field_risk,
        "compliance_tags": sorted(set(compliance_tags)),
    }
