"""Value-aware column semantic analysis — names + samples → canonical roles."""

from __future__ import annotations

import re
from typing import Any

# Canonical semantic roles used for cross-column matching (AMT, AMOUNT, value → payment_amount)
SEMANTIC_ROLES: dict[str, list[str]] = {
    "payment_amount": [
        "amount", "payment_amount", "pay_amt", "amt", "txn_amt", "transaction_amount",
        "value", "total", "order_total", "payment", "pmt", "pymt", "fld_07",
    ],
    "payment_date": [
        "date", "payment_date", "pay_date", "txn_dt", "trans_dt", "transaction_date",
        "value_date", "dtpmt", "order_date", "created_at",
    ],
    "customer_id": [
        "customer_id", "cust_id", "customerid", "buyer_id", "client_id", "cust_no",
    ],
    "account_number": [
        "account_number", "acct_no", "acct_num", "account_no", "iban", "beneficiary_account",
        "bank_account",
    ],
    "currency_code": ["currency_code", "currency", "ccy", "curr", "iso_currency"],
    "reference_number": ["reference_number", "ref_no", "ref", "reference", "txn_ref", "invoice_number"],
    "description": ["description", "desc", "descr", "memo", "narrative", "details", "notes"],
    "status": ["status", "sts", "stat", "state", "payment_status"],
    "email": ["email", "e_mail", "email_address"],
    "phone": ["phone", "telephone", "mobile", "phone_number"],
    "name": ["name", "full_name", "customer_name", "contact_name"],
    "quantity": ["quantity", "qty", "units", "count"],
    "sku": ["sku", "product_sku", "item_code", "product_id"],
    "address": ["address", "addr", "street", "shipping_address"],
    "postal_code": ["postal_code", "zip", "zipcode", "postcode"],
    "origin_city": ["origin_city", "orig_city", "from_city", "pickup_city", "origin"],
    "destination_city": ["destination_city", "dest_city", "to_city", "delivery_city", "destination"],
    "shipment_weight_kg": ["shipment_weight_kg", "weight_kg", "weight", "pkg_weight", "wt_kg"],
    "tracking_number": ["tracking_number", "track_no", "tracking_id", "awb", "consignment_no"],
}


def _normalize(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _role_from_name(name: str) -> tuple[str | None, float]:
    norm = _normalize(name)
    tokens = norm.split("_")
    best_role: str | None = None
    best_score = 0.0

    for role, aliases in SEMANTIC_ROLES.items():
        for alias in aliases:
            alias_norm = _normalize(alias)
            if norm == alias_norm:
                return role, 0.98
            if alias_norm in tokens or norm.endswith(alias_norm) or alias_norm in norm:
                score = 0.88 if len(alias_norm) > 3 else 0.75
                if score > best_score:
                    best_role, best_score = role, score
    return best_role, best_score


def _role_from_samples(samples: list[str], inferred_type: str) -> tuple[str | None, float]:
    non_empty = [s.strip() for s in samples if s and str(s).strip()]
    if not non_empty:
        return None, 0.0

    numeric = 0
    date_like = 0
    currency_like = 0
    id_like = 0

    for s in non_empty[:20]:
        cleaned = s.replace(",", "").replace("$", "").strip()
        if re.match(r"^-?\d+(\.\d+)?$", cleaned):
            numeric += 1
        if re.match(r"^\d{4}-\d{2}-\d{2}", s) or re.match(r"^\d{8}$", s):
            date_like += 1
        if re.match(r"^[A-Z]{3}$", s.upper()) and len(s) == 3:
            currency_like += 1
        if re.match(r"^(ACC|TXN|CUST|INV|REF)[-_]?\w+", s.upper()) or re.match(r"^[A-Z0-9]{8,}$", s):
            id_like += 1

    n = len(non_empty)
    if date_like / n >= 0.5:
        return "payment_date", 0.82
    if currency_like / n >= 0.5:
        return "currency_code", 0.8
    if numeric / n >= 0.7 and inferred_type.upper() in {"INTEGER", "DECIMAL", "NUMBER", "FLOAT", "NUMERIC"}:
        return "payment_amount", 0.78
    if id_like / n >= 0.5:
        return "customer_id", 0.72
    return None, 0.0


def analyze_column(name: str, inferred_type: str = "VARCHAR", samples: list[str] | None = None) -> dict[str, Any]:
    """Detect semantic role for a column using header + sample values."""
    samples = samples or []
    role_name, name_conf = _role_from_name(name)
    role_sample, sample_conf = _role_from_samples(samples, inferred_type)

    if role_sample and sample_conf > name_conf:
        role, confidence, source = role_sample, sample_conf, "value_pattern"
    elif role_name:
        role, confidence, source = role_name, name_conf, "header_lexicon"
    elif role_sample:
        role, confidence, source = role_sample, sample_conf, "value_pattern"
    else:
        role = f"field_{_normalize(name)}"
        confidence = 0.55
        source = "unknown"

    return {
        "name": name,
        "inferred_type": inferred_type,
        "semantic_role": role,
        "confidence": round(confidence, 3),
        "detection_source": source,
        "description": _role_description(role),
        "samples": samples[:5],
    }


def _role_description(role: str) -> str:
    descriptions = {
        "payment_amount": "Monetary amount / payment value",
        "payment_date": "Transaction or payment date",
        "customer_id": "Customer or client identifier",
        "account_number": "Bank or beneficiary account",
        "currency_code": "ISO currency code",
        "reference_number": "Transaction or invoice reference",
        "description": "Free-text description or memo",
        "status": "Status or state code",
        "email": "Email address",
        "phone": "Phone number",
        "name": "Person or entity name",
        "quantity": "Quantity or count",
        "sku": "Product SKU or item code",
        "address": "Street or mailing address",
        "postal_code": "Postal / ZIP code",
        "origin_city": "Shipment origin city",
        "destination_city": "Shipment destination city",
        "shipment_weight_kg": "Package weight in kilograms",
        "tracking_number": "Carrier tracking or AWB number",
    }
    return descriptions.get(role, role.replace("_", " ").title())


def analyze_schema(columns: list[dict]) -> list[dict]:
    """Analyze all columns in an uploaded or introspected schema."""
    return [
        analyze_column(
            c.get("name", ""),
            c.get("inferred_type", "VARCHAR"),
            c.get("samples", []),
        )
        for c in columns
    ]


def role_match_boost(source_role: str, target_role: str) -> float | None:
    """Boost mapping score when semantic roles align."""
    if source_role == target_role:
        return 0.97
    src_base = source_role.replace("field_", "")
    tgt_base = target_role.replace("field_", "")
    if src_base == tgt_base:
        return 0.94
    # payment_amount ↔ order_total etc.
    amount_roles = {"payment_amount", "order_total"}
    if source_role in amount_roles and target_role in amount_roles:
        return 0.9
    date_roles = {"payment_date", "order_date"}
    if source_role in date_roles and target_role in date_roles:
        return 0.9
    return None
