"""PII / PHI detection and masking for the universal transfer orchestrator.

Detects sensitive values in samples and masks/de-identifies them in logs,
telemetry, and prompt payloads.  This is a defensive guard, not a data loss
prevention replacement; it makes sure DataFlow never leaks sensitive data in
observability or prompts.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

# Simplistic but fast patterns.  For production, integrate with a DLP or ML service.
PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b"),
    "ip_address": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}

SENSITIVE_NAME_HINTS: set[str] = {
    "email", "phone", "mobile", "ssn", "dob", "birth", "passport", "license",
    "credit", "card", "iban", "account_number", "name", "first_name", "last_name",
    "address", "zip", "postal", "city", "country", "gender", "race", "ethnicity",
    "religion", "sexual", "orientation", "disability", "health", "diagnosis",
    "medication", "condition", "patient", "doctor", "mrn", "ssn", "sin",
}


def is_sensitive_name(name: str) -> bool:
    lower = name.lower()
    return any(hint in lower for hint in SENSITIVE_NAME_HINTS)


def detect_pii(value: Any) -> dict[str, Any]:
    """Detect PII patterns in a single value."""
    text = str(value) if value is not None else ""
    findings: dict[str, int] = {}
    for label, pattern in PII_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            findings[label] = len(matches)
    return {"has_pii": bool(findings), "findings": findings, "sample": mask(text)}


def mask(value: Any) -> str:
    """Mask a sensitive value for safe logging."""
    if value is None:
        return ""
    text = str(value)
    if len(text) <= 4:
        return "*" * len(text)
    return text[:2] + "***" + text[-2:]


def hash_token(value: Any, salt: str = "") -> str:
    """Produce a deterministic one-way hash for a sensitive value."""
    return hashlib.sha256((salt + str(value)).encode("utf-8")).hexdigest()[:16]


def mask_record(record: dict[str, Any], sensitive_columns: set[str] | None = None) -> dict[str, Any]:
    """Return a copy of the record with sensitive columns masked."""
    if sensitive_columns is None:
        sensitive_columns = {k for k in record if is_sensitive_name(k)}
    out: dict[str, Any] = {}
    for k, v in record.items():
        if k in sensitive_columns:
            out[k] = mask(v)
        else:
            out[k] = v
    return out


def redact_sample(sample: dict[str, Any]) -> dict[str, Any]:
    """Redact any PII detected in a sample row."""
    out: dict[str, Any] = {}
    for k, v in sample.items():
        if is_sensitive_name(k) or detect_pii(v)["has_pii"]:
            out[k] = mask(v)
        else:
            out[k] = v
    return out


def classify_columns(columns: list[str]) -> dict[str, str]:
    """Classify columns by sensitivity risk."""
    return {c: "sensitive" if is_sensitive_name(c) else "low" for c in columns}
