"""Domain-specific column intelligence — logistics, healthcare, finance, insurance."""

from __future__ import annotations

import re
from typing import Any

DOMAINS: dict[str, dict[str, Any]] = {
    "logistics": {
        "label": "Logistics & supply chain",
        "compliance": [],
        "patterns": [
            r"ship(ping|ment)?", r"freight", r"carrier", r"warehouse", r"sku",
            r"tracking", r"origin", r"destination", r"bol", r"container", r"pallet",
            r"delivery", r"cust_id", r"order_id", r"manifest", r"route",
        ],
        "required_fields": ["order_id", "tracking_no", "ship_date"],
        "type_overrides": {
            "weight": "DECIMAL",
            "freight_class": "VARCHAR",
            "ship_date": "TIMESTAMP",
        },
    },
    "healthcare": {
        "label": "Healthcare & life sciences",
        "compliance": ["HIPAA", "HITECH"],
        "patterns": [
            r"patient", r"diagnosis", r"icd", r"npi", r"physician", r"provider",
            r"medical", r"clinical", r"procedure", r"rx", r"prescription", r"mrn",
            r"encounter", r"admission", r"discharge", r"lab_", r"cpt",
        ],
        "required_fields": ["patient_id", "encounter_id"],
        "type_overrides": {
            "dob": "DATE",
            "mrn": "VARCHAR",
            "icd10": "VARCHAR",
        },
        "pii_fields": ["patient_id", "mrn", "dob", "ssn", "npi"],
    },
    "finance": {
        "label": "Financial services",
        "compliance": ["PCI-DSS", "SOX"],
        "patterns": [
            r"payment", r"transaction", r"ledger", r"account", r"balance",
            r"currency", r"amt", r"amount", r"txn", r"swift", r"iban",
            r"routing", r"settlement", r"fee", r"interest", r"portfolio", r"trade",
        ],
        "required_fields": ["txn_id", "amount", "currency"],
        "type_overrides": {
            "amount": "DECIMAL",
            "amt": "DECIMAL",
            "balance": "DECIMAL",
            "txn_date": "TIMESTAMP",
        },
    },
    "insurance": {
        "label": "Insurance",
        "compliance": ["NAIC", "SOC2"],
        "patterns": [
            r"policy", r"premium", r"claim", r"claimant", r"underwrit",
            r"coverage", r"deductible", r"beneficiary", r"insured", r"loss",
            r"adjuster", r"renewal",
        ],
        "required_fields": ["policy_no", "claim_id"],
        "type_overrides": {
            "premium": "DECIMAL",
            "deductible": "DECIMAL",
            "effective_date": "DATE",
        },
    },
}


def detect_data_domain(columns: list[str]) -> dict[str, Any]:
    """Score columns against domain pattern libraries."""
    col_lower = [c.lower() for c in columns]
    col_blob = " ".join(col_lower)

    best_domain = "general"
    best_score = 0.0
    best_signals: list[str] = []

    for domain_id, spec in DOMAINS.items():
        signals: list[str] = []
        for col in columns:
            cl = col.lower()
            for pat in spec["patterns"]:
                if re.search(pat, cl, re.I):
                    signals.append(col)
                    break
        unique = list(dict.fromkeys(signals))[:8]
        if not unique:
            continue
        score = min(0.98, len(unique) / max(3, len(columns) * 0.12))
        if score > best_score:
            best_score = score
            best_domain = domain_id
            best_signals = unique

    if best_score < 0.35:
        return {
            "domain": "general",
            "label": "General enterprise data",
            "confidence": 0.5,
            "signals": [],
            "compliance": [],
        }

    spec = DOMAINS[best_domain]
    return {
        "domain": best_domain,
        "label": spec["label"],
        "confidence": round(best_score, 2),
        "signals": best_signals,
        "compliance": spec.get("compliance", []),
    }


def domain_type_hints(domain: str, column: str, inferred: str) -> str:
    """Apply domain-specific type overrides for warehouse DDL."""
    if domain == "general" or domain not in DOMAINS:
        return inferred
    overrides = DOMAINS[domain].get("type_overrides", {})
    col_lower = column.lower()
    for key, dtype in overrides.items():
        if key in col_lower or col_lower == key:
            return dtype
    return inferred


def domain_pii_columns(domain: str, columns: list[str]) -> list[str]:
    if domain not in DOMAINS:
        return []
    pii_patterns = DOMAINS[domain].get("pii_fields", [])
    found = []
    for col in columns:
        cl = col.lower()
        if cl in pii_patterns or any(p in cl for p in pii_patterns):
            found.append(col)
    return found


def enrich_mapping_with_domain(
    mappings: list[dict],
    columns: list[str],
) -> tuple[list[dict], dict[str, Any]]:
    """Augment mappings with domain detection metadata."""
    profile = detect_data_domain(columns)
    domain = profile["domain"]
    if domain == "general":
        return mappings, profile

    pii_cols = set(domain_pii_columns(domain, columns))
    enriched = []
    for m in mappings:
        row = dict(m)
        src = m.get("source", "")
        if src in pii_cols and not row.get("transform"):
            row["transform"] = "hash_pii"
            row["domain_hint"] = f"{domain}: PII field — hash recommended"
        enriched.append(row)
    return enriched, profile
