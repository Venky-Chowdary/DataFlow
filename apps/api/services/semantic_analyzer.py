"""Value-aware column semantic analysis — names + samples → canonical roles."""

from __future__ import annotations

import re
from typing import Any

# Canonical semantic roles used for cross-column matching (AMT, AMOUNT, value → payment_amount)
SEMANTIC_ROLES: dict[str, list[str]] = {
    "payment_amount": [
        "amount", "payment_amount", "pay_amt", "amt", "txn_amt", "transaction_amount",
        "payment", "pmt", "pymt", "payment_amt", "paid_amount", "fld_07",
    ],
    "payment_date": [
        "payment_date", "pay_date", "txn_dt", "trans_dt", "transaction_date",
        "value_date", "dtpmt", "payment_dt", "paid_date", "settlement_date",
    ],
    "order_total": ["order_total", "cart_total", "invoice_total", "subtotal", "grand_total", "total"],
    "order_date": ["order_date", "purchase_date", "placed_at", "ordered_at"],
    "created_timestamp": ["created_at", "created_on", "created_ts", "created_dt", "inserted_at"],
    "updated_timestamp": ["updated_at", "updated_on", "modified_at", "modified_on", "last_updated", "updated_ts"],
    "date_value": ["date", "event_date", "effective_date", "business_date"],
    "numeric_value": ["value", "metric_value", "score", "measure", "count", "measure_value"],
    "identifier": ["id", "record_id", "row_id", "object_id", "external_id"],
    "order_number": ["order_number", "order_no", "order_num", "order_id", "purchase_order"],
    "invoice_number": ["invoice_number", "invoice_no", "invoice_num", "inv_no", "invoice_id"],
    "transaction_id": ["transaction_id", "txn_id", "trans_id", "txn_no", "transaction_no"],
    "customer_id": [
        "customer_id", "cust_id", "customerid", "buyer_id", "client_id", "cust_no",
        "user_id", "userid", "user", "account_id", "accountid", "account_no",
        "member_id", "memberid", "subscriber_id", "subscriberid", "clientid",
    ],
    "first_name": ["first_name", "firstname", "fname", "given_name", "forename"],
    "last_name": ["last_name", "lastname", "lname", "surname", "family_name"],
    "full_name": ["full_name", "fullname", "complete_name", "display_name", "name"],
    "email_address": ["email", "e_mail", "email_address", "email_addr", "mail"],
    "phone_number": ["phone", "telephone", "phone_number", "contact_number", "tel"],
    "mobile_number": ["mobile", "mobile_number", "cell", "cell_phone", "cell_number"],
    "country_code": ["country_code", "country_cd", "country_iso", "ctry_code", "country"],
    "state_code": ["state_code", "state_cd", "province_code", "province_cd", "st", "state"],
    "province_code": ["province_code", "province_cd", "prov_code", "prov_cd"],
    "city_name": ["city", "city_name", "town", "municipality"],
    "region_code": ["region_code", "region_cd", "region", "territory"],
    "department": ["department", "dept", "division", "business_unit", "cost_center", "org_unit"],
    "hire_date": ["hire_date", "hired_date", "start_date", "employment_date", "join_date", "onboarding_date"],
    "ship_date": ["ship_date", "shipped_date", "shipping_date", "dispatch_date"],
    "delivery_date": ["delivery_date", "delivered_date", "del_date", "receipt_date"],
    "account_number": [
        "account_number", "acct_no", "acct_num", "account_no", "iban", "beneficiary_account",
        "bank_account",
    ],
    "currency_code": ["currency_code", "currency", "ccy", "curr", "iso_currency"],
    "reference_number": ["reference_number", "ref_no", "ref", "reference", "txn_ref", "reference_num"],
    "description": ["description", "desc", "descr", "memo", "narrative", "details", "notes"],
    "status": ["status", "sts", "stat", "state", "payment_status", "order_status"],
    "quantity": ["quantity", "qty", "units", "count", "quantity_ordered", "qty_ordered"],
    "quantity_ordered": ["quantity_ordered", "qty_ordered", "order_qty", "qty_ord"],
    "unit_price": ["unit_price", "unit_cost", "unit_prc", "price", "cost", "list_price", "sale_price"],
    "unit_cost": ["unit_cost", "cost", "cost_per_unit", "unit_prc"],
    "tax_amount": ["tax_amount", "tax_amt", "tax", "vat", "gst", "sales_tax"],
    "discount_amount": ["discount_amount", "discount_amt", "disc", "disc_amt", "rebate", "promo_amount"],
    "line_amount": ["line_amount", "line_amt", "line_total", "line_item_amount"],
    "net_amount": ["net_amount", "net_amt", "net"],
    "gross_amount": ["gross_amount", "gross_amt", "gross"],
    "salary_amount": ["salary", "salary_amount", "salary_amt", "compensation", "pay", "wage"],
    "commission_amount": ["commission", "commission_amount", "commission_amt", "comm"],
    "bonus_amount": ["bonus", "bonus_amount", "bonus_amt"],
    "sku": ["sku", "product_sku", "item_code", "product_id"],
    "address": ["address", "addr", "street", "shipping_address", "billing_address"],
    "postal_code": ["postal_code", "zip", "zipcode", "postcode"],
    "origin_city": ["origin_city", "orig_city", "from_city", "pickup_city", "origin"],
    "destination_city": ["destination_city", "dest_city", "to_city", "delivery_city", "destination"],
    "shipment_weight_kg": ["shipment_weight_kg", "weight_kg", "weight", "pkg_weight", "wt_kg"],
    "tracking_number": ["tracking_number", "track_no", "tracking_id", "awb", "consignment_no"],
    "long_text": ["body", "content", "story", "narrative", "comment", "remarks", "blob_text", "clob"],
    "binary_data": ["binary", "blob", "bytea", "raw_bytes", "image_data", "file_data", "attachment"],
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
    avg_len = sum(len(str(s)) for s in non_empty) / n
    if avg_len > 256 or inferred_type.upper() in {"TEXT", "CLOB", "LONGTEXT"}:
        return "long_text", 0.85
    if inferred_type.upper() in {"BINARY", "BLOB", "BYTEA"}:
        return "binary_data", 0.88
    base64_hits = sum(1 for s in non_empty[:10] if re.match(r"^[A-Za-z0-9+/=]{32,}$", str(s).strip()))
    if base64_hits / min(n, 10) >= 0.6:
        return "binary_data", 0.82
    if date_like / n >= 0.5:
        return "date_value", 0.72
    if currency_like / n >= 0.5:
        return "currency_code", 0.8
    if numeric / n >= 0.7 and inferred_type.upper() in {"INTEGER", "DECIMAL", "NUMBER", "FLOAT", "NUMERIC"}:
        return "numeric_value", 0.66
    if id_like / n >= 0.5:
        return "identifier", 0.66
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
        "order_total": "Order, invoice, or cart total",
        "order_date": "Order or purchase date",
        "created_timestamp": "Creation timestamp",
        "updated_timestamp": "Update or modification timestamp",
        "date_value": "Generic date value",
        "numeric_value": "Generic numeric value",
        "identifier": "Generic record identifier",
        "order_number": "Order number or purchase order identifier",
        "invoice_number": "Invoice number or billing identifier",
        "transaction_id": "Transaction identifier",
        "customer_id": "Customer or client identifier",
        "first_name": "Person first / given name",
        "last_name": "Person last / family name",
        "full_name": "Person or entity full name",
        "email_address": "Email address",
        "phone_number": "Phone number",
        "mobile_number": "Mobile / cell phone number",
        "country_code": "Country code",
        "state_code": "State or province code",
        "province_code": "Province code",
        "city_name": "City name",
        "region_code": "Region or territory code",
        "department": "Department or business unit",
        "hire_date": "Employee hire / start date",
        "ship_date": "Shipment dispatch date",
        "delivery_date": "Delivery or receipt date",
        "account_number": "Bank or beneficiary account",
        "currency_code": "ISO currency code",
        "reference_number": "Transaction or invoice reference",
        "description": "Free-text description or memo",
        "status": "Status or state code",
        "quantity": "Quantity or count",
        "quantity_ordered": "Quantity ordered",
        "unit_price": "Unit price or sale price",
        "unit_cost": "Unit cost",
        "tax_amount": "Tax amount",
        "discount_amount": "Discount or rebate amount",
        "line_amount": "Line item amount",
        "net_amount": "Net amount",
        "gross_amount": "Gross amount",
        "salary_amount": "Salary or compensation amount",
        "commission_amount": "Commission amount",
        "bonus_amount": "Bonus amount",
        "sku": "Product SKU or item code",
        "address": "Street or mailing address",
        "postal_code": "Postal / ZIP code",
        "origin_city": "Shipment origin city",
        "destination_city": "Shipment destination city",
        "shipment_weight_kg": "Package weight in kilograms",
        "tracking_number": "Carrier tracking or AWB number",
        "long_text": "Long-form text, narrative, or CLOB content",
        "binary_data": "Binary blob, bytea, or base64-encoded payload",
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

    # Monetary amount roles are interchangeable.
    amount_roles = {
        "payment_amount", "order_total", "line_amount", "net_amount",
        "gross_amount", "tax_amount", "discount_amount", "salary_amount",
        "commission_amount", "bonus_amount", "unit_price", "unit_cost",
        "numeric_value",
    }
    if source_role in amount_roles and target_role in amount_roles:
        return 0.9

    # Date / timestamp roles are interchangeable.
    date_roles = {
        "payment_date", "order_date", "date_value", "created_timestamp",
        "updated_timestamp", "hire_date", "ship_date", "delivery_date",
    }
    if source_role in date_roles and target_role in date_roles:
        return 0.9

    # Identifier roles are interchangeable.
    id_roles = {
        "identifier", "customer_id", "order_number", "invoice_number",
        "transaction_id", "sku", "reference_number", "account_number",
    }
    if source_role in id_roles and target_role in id_roles:
        return 0.88

    # Name / contact roles are interchangeable.
    name_roles = {
        "name", "full_name", "first_name", "last_name", "email_address",
        "phone_number", "mobile_number",
    }
    if source_role in name_roles and target_role in name_roles:
        return 0.87

    # Location / address roles are interchangeable.
    location_roles = {
        "address", "city_name", "state_code", "province_code", "country_code",
        "region_code", "postal_code", "origin_city", "destination_city",
    }
    if source_role in location_roles and target_role in location_roles:
        return 0.86

    return None
