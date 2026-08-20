"""PII / PHI detection and masking for the universal transfer orchestrator.

Detects sensitive values in samples and masks/de-identifies them in logs,
telemetry, and prompt payloads.  This is a defensive guard, not a data loss
prevention replacement; it makes sure Datawrap never leaks sensitive data in
observability or prompts.
"""

from __future__ import annotations

import copy
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

# Union of every pattern above: a value matches no label iff it fails this.
# Used as an exact one-pass gate so a clean column costs one scan, not five.
_PII_UNION: re.Pattern[str] = re.compile(
    "|".join(f"(?:{p.pattern})" for p in PII_PATTERNS.values())
)
# Every pattern needs either an ``@`` (email) or a digit (phone/ssn/card/ip),
# so a value holding neither cannot match any label.
_PII_REQUIRED_CHARS: frozenset[str] = frozenset("@0123456789")

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


def _looks_structured(text: str) -> bool:
    """JSON / array wire forms — must never be replaced by a single PII match."""
    s = (text or "").lstrip()
    return s.startswith("{") or s.startswith("[")


def _mask_email_address(raw: str) -> str:
    """Shape-preserving email mask — keep local first char + TLD only."""
    local, _, domain = raw.partition("@")
    if not local or not domain:
        return "*" * max(1, len(raw))
    local_mask = f"{local[0]}{'*' * max(1, len(local) - 1)}"
    if "." in domain:
        name, _, tld = domain.rpartition(".")
        domain_mask = f"{'*' * max(3, len(name))}.{tld}"
    else:
        domain_mask = "*" * max(3, len(domain))
    return f"{local_mask}@{domain_mask}"


def _mask_matched_token(raw: str) -> str:
    """Mask one regex match without recursing into structured redact."""
    stripped = (raw or "").strip()
    if PII_PATTERNS["email"].fullmatch(stripped):
        return _mask_email_address(stripped)
    if len(raw) <= 4:
        return "*" * len(raw)
    if len(raw) <= 12:
        return raw[:2] + "*" * (len(raw) - 4) + raw[-2:]
    return raw[:6] + "…" + raw[-4:]


def _mask_without_pii(text: str) -> str:
    """``mask`` for text already known to match no PII pattern.

    Same output as ``mask``, minus the pattern sweep the caller just ran: no
    pattern matched, so the email branch and the embedded-PII branch cannot fire.
    """
    if _looks_structured(text):
        return _redact_text(text)
    if len(text) <= 4:
        return "*" * len(text)
    if len(text) <= 12:
        return text[:2] + "*" * (len(text) - 4) + text[-2:]
    return text[:6] + "…" + text[-4:]


def pii_findings(value: Any) -> dict[str, int]:
    """Pattern label → match count for one value; empty when nothing matched.

    Callers that only classify a column (audits, routing) use this instead of
    ``detect_pii`` so they do not pay for a masked sample they discard.
    """
    text = str(value) if value is not None else ""
    if not _PII_REQUIRED_CHARS.intersection(text):
        return {}
    if _PII_UNION.search(text) is None:
        return {}
    findings: dict[str, int] = {}
    for label, pattern in PII_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            findings[label] = len(matches)
    return findings


def detect_pii(value: Any) -> dict[str, Any]:
    """Detect PII patterns in a single value."""
    text = str(value) if value is not None else ""
    findings = pii_findings(text)
    sample = mask(text) if findings else _mask_without_pii(text)
    return {"has_pii": bool(findings), "findings": findings, "sample": sample}


def mask(value: Any) -> str:
    """Mask a sensitive value for safe logging and operator previews.

    Honesty: JSON/array (and any multi-token string with embedded PII) is
    redacted **in place**. Never replace an entire notifications/referrals
    payload with a single masked email — that destroyed operator preview fidelity.
    """
    if value is None:
        return ""
    text = str(value)
    stripped = text.strip()
    # Pure email scalar.
    if PII_PATTERNS["email"].fullmatch(stripped):
        return _mask_email_address(stripped)
    # Structured wire or embedded PII — preserve surrounding structure.
    if _looks_structured(text) or any(p.search(text) for p in PII_PATTERNS.values()):
        return _redact_text(text)
    if len(text) <= 4:
        return "*" * len(text)
    if len(text) <= 12:
        return text[:2] + "*" * (len(text) - 4) + text[-2:]
    # Longer tokens (IDs, nested paths): keep head/tail for correlation without full reveal.
    return text[:6] + "…" + text[-4:]


def mask_preview_value(value: Any, *, column: str = "", force: bool = False) -> str:
    """Mask preview cells when the column is sensitive or the value looks like PII."""
    if value is None:
        return ""
    text = str(value)
    if force or is_sensitive_name(column) or detect_pii(text).get("has_pii"):
        return mask(text)
    return text


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


def _redact_text(text: str) -> str:
    """Substitute PII/PHI patterns in a string with a safe mask (structure-preserving)."""
    if not isinstance(text, str):
        return text
    for _label, pattern in PII_PATTERNS.items():
        text = pattern.sub(lambda m: _mask_matched_token(m.group(0)), text)
    return text


def _sensitive_source_columns(mappings: list[dict]) -> set[str]:
    """Source columns that the operator has explicitly chosen to mask/hash or
    whose names are inherently sensitive."""
    return {
        m["source"]
        for m in mappings
        if m.get("source")
        and (
            m.get("transform") in {"mask_pii", "hash_pii"}
            or is_sensitive_name(m.get("source") or "")
        )
    }


def redact_records(rows: list[dict], mappings: list[dict]) -> list[dict]:
    """Return a copy of row dicts with sensitive source columns masked."""
    sensitive = _sensitive_source_columns(mappings)
    return [mask_record(row, sensitive) for row in rows]


def redact_destination_summary(
    summary: dict[str, Any], mappings: list[dict]
) -> dict[str, Any]:
    """Mask PII in the operator-facing destination summary before persistence."""
    out = copy.deepcopy(summary)
    sensitive = _sensitive_source_columns(mappings)

    sample = out.get("reconcile_sample")
    if isinstance(sample, list):
        out["reconcile_sample"] = [mask_record(row, sensitive) for row in sample]

    details = out.get("rejected_details")
    if isinstance(details, list):
        redacted_details: list[dict[str, Any]] = []
        for d in details:
            nd: dict[str, Any] = dict(d)
            col = str(nd.get("column") or nd.get("source") or "")
            if col in sensitive or is_sensitive_name(col):
                if "value" in nd:
                    nd["value"] = mask(nd["value"])
            values = nd.get("values")
            if isinstance(values, dict):
                nd["values"] = mask_record(values, sensitive)
            # Dual-stamped Wave 32/34 payloads — must mask both shapes for Theater/export.
            source_values = nd.get("source_values")
            if isinstance(source_values, dict):
                nd["source_values"] = mask_record(source_values, sensitive)
            target_values = nd.get("target_values")
            if isinstance(target_values, dict):
                nd["target_values"] = mask_record(target_values, sensitive)
            redacted_details.append(nd)
        out["rejected_details"] = redacted_details

    if out.get("warnings"):
        out["warnings"] = [_redact_text(w) for w in out["warnings"]]
    if isinstance(out.get("error"), str):
        out["error"] = _redact_text(out["error"])

    return out


def _redact_mismatch_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Mask source/target values in a mismatch detail dict."""
    for key, col_key in (("source_value", "source"), ("target_value", "target")):
        val = entry.get(key)
        if val is None:
            continue
        col = entry.get(col_key) or ""
        if is_sensitive_name(col) or detect_pii(val)["has_pii"]:
            entry[key] = mask(val)
    return entry


def redact_reconciliation(
    recon: dict[str, Any] | None, mappings: list[dict]
) -> dict[str, Any] | None:
    """Mask PII in the reconciliation report before it is returned or persisted."""
    if not recon:
        return recon
    out = copy.deepcopy(recon)
    sample_compare = out.get("sample_compare")
    if isinstance(sample_compare, dict):
        for key in ("source_only", "target_only"):
            rows = sample_compare.get(key)
            if isinstance(rows, list):
                sample_compare[key] = [redact_sample(row) for row in rows]
        # The real sample_compare shape stores mismatch details under
        # ``mismatches`` (each item has source_value / target_value).
        mm = sample_compare.get("mismatches")
        if isinstance(mm, list):
            sample_compare["mismatches"] = [_redact_mismatch_entry(dict(row)) for row in mm]
    mismatches = out.get("mismatches")
    if isinstance(mismatches, list):
        for mm in mismatches:
            _redact_mismatch_entry(mm)
    if isinstance(out.get("message"), str):
        out["message"] = _redact_text(out["message"])
    return out
