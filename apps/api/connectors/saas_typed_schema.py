"""Typed schema extraction for thin SaaS sources (Airtable / Notion / Stripe).

Honesty: these helpers improve Map/Validate type fidelity. They do **not**
promote Planned connectors to TRANSFER_READY / PRODUCTION_SKU — certification
stays in ``connector_capabilities`` until live matrices pass.
"""

from __future__ import annotations

import json
from typing import Any

from services.value_serializer import cell_to_string, json_default

# Notion property type → DataFlow logical type
_NOTION_LOGICAL: dict[str, str] = {
    "title": "string",
    "rich_text": "string",
    "number": "decimal",
    "select": "string",
    "multi_select": "array",
    "status": "string",
    "date": "datetime",
    "people": "array",
    "files": "array",
    "checkbox": "boolean",
    "url": "string",
    "email": "string",
    "phone_number": "string",
    "relation": "array",
    "formula": "string",
    "rollup": "string",
    "created_time": "datetime",
    "created_by": "string",
    "last_edited_time": "datetime",
    "last_edited_by": "string",
    "unique_id": "string",
}

# Stripe common fields (object-agnostic leaf names)
_STRIPE_LOGICAL: dict[str, str] = {
    "id": "string",
    "object": "string",
    "created": "integer",
    "livemode": "boolean",
    "amount": "integer",
    "amount_due": "integer",
    "amount_paid": "integer",
    "amount_remaining": "integer",
    "balance": "integer",
    "currency": "string",
    "customer": "string",
    "email": "string",
    "description": "string",
    "status": "string",
    "paid": "boolean",
    "refunded": "boolean",
    "delinquent": "boolean",
}


def _infer_logical(value: Any) -> str:
    if value is None:
        return "string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "json"
    return "string"


def flatten_airtable_record(rec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Promote ``fields.*`` to top-level columns with inferred logical types."""
    out: dict[str, Any] = {}
    schema: dict[str, str] = {}
    if rec.get("id") is not None:
        out["id"] = rec["id"]
        schema["id"] = "string"
    if rec.get("createdTime") is not None:
        out["createdTime"] = rec["createdTime"]
        schema["createdTime"] = "datetime"
    fields = rec.get("fields") if isinstance(rec.get("fields"), dict) else {}
    for key, value in fields.items():
        out[str(key)] = value
        schema[str(key)] = _infer_logical(value)
    # Fallback: non-standard shapes
    if not fields and not out:
        for key, value in rec.items():
            if key in {"id", "createdTime", "fields"}:
                continue
            out[str(key)] = value
            schema[str(key)] = _infer_logical(value)
    return out, schema


def _notion_property_value(prop: dict[str, Any]) -> Any:
    """Unwrap a Notion property object into a scalar / JSON-friendly value."""
    if not isinstance(prop, dict):
        return prop
    typ = str(prop.get("type") or "")
    payload = prop.get(typ)
    if typ in {"title", "rich_text"}:
        parts = []
        for block in payload or []:
            if isinstance(block, dict):
                parts.append(block.get("plain_text") or (block.get("text") or {}).get("content") or "")
        return "".join(parts)
    if typ == "number":
        return payload
    if typ == "checkbox":
        return bool(payload)
    if typ in {"url", "email", "phone_number", "status"}:
        if typ == "status" and isinstance(payload, dict):
            return payload.get("name")
        return payload
    if typ == "select" and isinstance(payload, dict):
        return payload.get("name")
    if typ == "multi_select" and isinstance(payload, list):
        return [p.get("name") for p in payload if isinstance(p, dict)]
    if typ == "date" and isinstance(payload, dict):
        return payload.get("start") or payload.get("end")
    if typ in {"people", "relation", "files"} and isinstance(payload, list):
        return payload
    if typ in {"created_time", "last_edited_time"}:
        return payload
    if typ == "unique_id" and isinstance(payload, dict):
        prefix = payload.get("prefix") or ""
        number = payload.get("number")
        return f"{prefix}-{number}" if prefix else number
    if typ == "formula" and isinstance(payload, dict):
        ftype = payload.get("type")
        return payload.get(ftype) if ftype else payload
    return payload


def flatten_notion_record(rec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Flatten a Notion page/database row with property types."""
    out: dict[str, Any] = {}
    schema: dict[str, str] = {}
    if rec.get("id") is not None:
        out["id"] = rec["id"]
        schema["id"] = "string"
    if rec.get("created_time") is not None:
        out["created_time"] = rec["created_time"]
        schema["created_time"] = "datetime"
    if rec.get("last_edited_time") is not None:
        out["last_edited_time"] = rec["last_edited_time"]
        schema["last_edited_time"] = "datetime"
    props = rec.get("properties") if isinstance(rec.get("properties"), dict) else {}
    for name, prop in props.items():
        if not isinstance(prop, dict):
            out[str(name)] = prop
            schema[str(name)] = _infer_logical(prop)
            continue
        typ = str(prop.get("type") or "")
        out[str(name)] = _notion_property_value(prop)
        schema[str(name)] = _NOTION_LOGICAL.get(typ) or _infer_logical(out[str(name)])
    if not props and not out:
        # Generic Notion object — shallow flatten
        for key, value in rec.items():
            if isinstance(value, (dict, list)):
                out[str(key)] = json.dumps(value, default=json_default)
                schema[str(key)] = "json"
            else:
                out[str(key)] = value
                schema[str(key)] = _infer_logical(value)
    return out, schema


def flatten_stripe_record(rec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Shallow flatten Stripe objects with known field types."""
    out: dict[str, Any] = {}
    schema: dict[str, str] = {}

    def _walk(obj: dict[str, Any], prefix: str = "") -> None:
        for key, value in obj.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict) and key in {"metadata", "address", "billing_details"}:
                _walk(value, name)
            elif isinstance(value, (dict, list)):
                out[name] = value
                schema[name] = "json" if isinstance(value, dict) else "array"
            else:
                out[name] = value
                leaf = str(key)
                schema[name] = _STRIPE_LOGICAL.get(leaf) or _STRIPE_LOGICAL.get(name) or _infer_logical(value)

    if isinstance(rec, dict):
        _walk(rec)
    return out, schema


def flatten_saas_record(
    catalog_id: str,
    rec: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Dispatch brand-aware flatten; returns (row_dict, schema)."""
    brand = (catalog_id or "").strip().lower()
    if brand == "airtable":
        return flatten_airtable_record(rec)
    if brand == "notion":
        return flatten_notion_record(rec)
    if brand == "stripe":
        return flatten_stripe_record(rec)
    # Generic REST — leave to caller
    return {}, {}


def rows_and_schema_from_saas(
    catalog_id: str,
    records: list[dict[str, Any]],
) -> tuple[list[str], list[list[str]], dict[str, str]]:
    """Build string matrix + union schema for thin SaaS sources."""
    brand = (catalog_id or "").strip().lower()
    if brand not in {"airtable", "notion", "stripe"}:
        return [], [], {}

    keys: list[str] = []
    seen: set[str] = set()
    schema: dict[str, str] = {}
    flattened: list[dict[str, str]] = []

    for rec in records:
        if not isinstance(rec, dict):
            continue
        row, types = flatten_saas_record(brand, rec)
        for k, t in types.items():
            if k not in schema:
                schema[k] = t
        wire = {k: cell_to_string(v, preserve_sql_null=True) for k, v in row.items()}
        for k in wire:
            if k not in seen:
                seen.add(k)
                keys.append(k)
        flattened.append(wire)

    matrix = [[r.get(k, "") for k in keys] for r in flattened]
    return keys, matrix, schema
