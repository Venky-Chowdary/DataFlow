"""SaaS introspection: Salesforce / HubSpot Describe, thin JSON SaaS (Stripe, Airtable, Notion, REST), Shopify, Zendesk.

Dispatched from ``services.schema_introspect.introspect_schema`` by ``db_type``.
These probes read an API's own schema description (Describe, Properties,
documented carriers) or a bounded typed page, and return the same
``{"ok", "columns", "tables", "schema"}`` shape every catalog introspector
returns, so Map and preflight treat a CRM object like a table.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

def salesforce_field_to_logical(
    field_type: str,
    *,
    precision: int | None = None,
    scale: int | None = None,
    length: int | None = None,
) -> str:
    """Map Salesforce describe field type → Datawrap logical carrier.

    Currency/percent without Describe precision still get honest DECIMAL defaults
    (never bare DECIMAL that invents warehouse NUMBER). Datetime is UTC → TIMESTAMPTZ.
    String fields with Describe ``length`` stamp ``VARCHAR(n)`` (writer SSOT).
    """
    t = (field_type or "string").strip().lower()
    if t in {"boolean"}:
        return "BOOLEAN"
    if t in {"long"}:
        return "BIGINT"
    if t in {"int", "integer"}:
        return "INTEGER"
    if t in {"double", "currency", "percent"}:
        if precision is not None and scale is not None:
            p, s = int(precision), int(scale)
            if s == 0 and p <= 18:
                return "BIGINT"
            return f"DECIMAL({p},{s})"
        if t == "currency":
            return "DECIMAL(18,2)"
        if t == "percent":
            return "DECIMAL(18,2)"
        return "DOUBLE"
    if t == "date":
        return "DATE"
    if t == "datetime":
        return "TIMESTAMPTZ"
    if t == "time":
        return "TIME"
    if t == "base64":
        return "BINARY"
    if t in {"address", "location", "complexvalue", "json"}:
        return "JSON"
    if t == "id":
        # Salesforce Ids are 15/18-char strings — bound when length absent.
        if length is not None and int(length) > 0:
            return f"VARCHAR({int(length)})"
        return "VARCHAR(18)"
    # string, textarea, phone, url, email, picklist, reference, …
    if length is not None:
        try:
            n = int(length)
            if n > 0:
                return f"VARCHAR({n})"
        except (TypeError, ValueError):
            pass
    return "TEXT"


def hubspot_property_to_logical(
    prop_type: str,
    *,
    field_type: str = "",
    number_display_hint: str = "",
    name: str = "",
) -> str:
    """Map HubSpot property type → Datawrap logical carrier.

    Uses ``fieldType`` / ``numberDisplayHint`` when present so currency and
    whole-number properties do not all collapse to bare DECIMAL.
    """
    t = (prop_type or "string").strip().lower()
    ft = (field_type or "").strip().lower()
    hint = (number_display_hint or "").strip().lower()
    n = (name or "").strip().lower()
    if t == "bool" or ft == "booleancheckbox":
        return "BOOLEAN"
    if t == "number":
        if hint in {"currency"}:
            return "DECIMAL(18,2)"
        if hint in {"percentage", "percent"}:
            return "DECIMAL(18,2)"
        if hint == "duration":
            return "INTEGER"
        if ft in {"calculation_equation", "calculation_score", "calculation_read_time"}:
            return "FLOAT"
        if (
            n.endswith(("_count", "count"))
            or n.endswith("_num")
            or n in {"num_employees", "numberofemployees", "hs_object_id"}
            or n.endswith("numberofemployees")
        ):
            return "INTEGER"
        return "DECIMAL"
    if t == "date":
        return "DATE"
    if t == "datetime":
        return "TIMESTAMPTZ"
    if t == "json" or ft in {"html", "calculation_equation"}:
        return "JSON" if t == "json" else "TEXT"
    # string, enumeration, phone_number, …
    return "TEXT"


def _saas_cfg(**kwargs: Any) -> dict[str, Any]:
    return {
        "host": kwargs.get("host") or "",
        "database": kwargs.get("database") or "",
        "table": kwargs.get("table") or "",
        "connection_string": kwargs.get("connection_string") or "",
        "api_key": kwargs.get("api_key") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
    }


def _introspect_salesforce(**kwargs: Any) -> dict[str, Any]:
    """Salesforce Describe → typed columns for Map / preflight."""
    try:
        from connectors.salesforce import describe_sobject, list_sobjects
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    cfg = _saas_cfg(**kwargs)
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    try:
        tables = list_sobjects(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    if not table:
        return {"ok": True, "columns": [], "tables": tables, "schema": ""}

    try:
        fields = describe_sobject(cfg, table)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": tables}

    columns = []
    for f in fields:
        if not f.get("name"):
            continue
        # Prefer writer SSOT carrier (VARCHAR(n) from Describe length) so Map
        # chips and reverse-ETL quarantine share one honesty bar.
        try:
            from connectors.salesforce_writer import salesforce_field_to_carrier

            inferred = salesforce_field_to_carrier(f)
        except Exception:
            inferred = salesforce_field_to_logical(
                str(f.get("type") or "string"),
                precision=f.get("precision") if isinstance(f.get("precision"), int) else None,
                scale=f.get("scale") if isinstance(f.get("scale"), int) else None,
                length=f.get("length") if isinstance(f.get("length"), int) else None,
            )
        columns.append(
            {
                "name": f["name"],
                "inferred_type": inferred,
                "nullable": bool(f.get("nillable", True)),
                "data_type": f.get("type") or "string",
                "label": f.get("label") or "",
                "is_primary_key": f.get("name") == "Id" or bool(f.get("externalId")),
                "updateable": bool(f.get("updateable", True)),
                "createable": bool(f.get("createable", True)),
                "calculated": bool(f.get("calculated", False)),
                "externalId": bool(f.get("externalId", False)),
                "idLookup": bool(f.get("idLookup", False)),
            }
        )
    pk_cols = ["Id"] if any(c.get("name") == "Id" for c in columns) else []
    unique_keys: list[dict[str, Any]] = []
    for c in columns:
        if c.get("externalId") or (c.get("idLookup") and c.get("name") != "Id"):
            unique_keys.append(
                {
                    "name": f"sf_ext_{c['name']}",
                    "columns": [str(c["name"])],
                    "primary": False,
                    "enforced": True,
                    "external_id": bool(c.get("externalId")),
                }
            )
    return {
        "ok": True,
        "columns": columns,
        "tables": tables,
        "schema": table,
        "primary_key_columns": pk_cols,
        "unique_keys": unique_keys,
    }


def _thin_saas_logical_to_carrier(logical: str) -> str:
    """Map thin-SaaS logical types to Map/DDL carrier names."""
    lt = (logical or "string").strip().lower()
    return {
        "boolean": "BOOLEAN",
        "integer": "INTEGER",
        "decimal": "DECIMAL",
        "float": "FLOAT",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "json": "JSON",
        "array": "ARRAY",
        "string": "TEXT",
        "text": "TEXT",
    }.get(lt, "TEXT")


def _introspect_thin_saas(brand: str, **kwargs: Any) -> dict[str, Any]:
    """Sample-based typed introspect for Airtable/Notion/Stripe/REST.

    Honesty: improves Map types via ``native_types``; does **not** certify the
    connector as TRANSFER_READY / PRODUCTION_SKU.
    """
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    cfg = {
        **_saas_cfg(**kwargs),
        "type": brand,
        "format": brand,
    }
    if not table and brand == "stripe":
        # The catalog is the documented list endpoints; the key is proven by
        # the same /v1/account call Test makes (D272).
        from connectors.saas_write_carriers import stripe_catalog_objects
        from connectors.stripe import test_stripe

        ok, message = test_stripe(**{k: v for k, v in cfg.items() if k not in {"type", "format"}})
        if not ok:
            return {"ok": False, "error": message, "columns": [], "tables": []}
        return {"ok": True, "columns": [], "tables": stripe_catalog_objects(), "schema": ""}
    if not table:
        return {"ok": True, "columns": [], "tables": [], "schema": ""}
    try:
        if brand == "stripe":
            from connectors.stripe import read_object
        else:
            from connectors.rest_api import read_object

        batch = read_object(cfg=cfg, object=table, limit=25, offset=0)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": [table] if table else [],
        }

    meta = getattr(batch, "meta", None) or {}
    native = meta.get("native_types") or meta.get("schema") or {}
    if not isinstance(native, dict):
        native = {}
    headers = list(batch.headers or [])
    columns = [
        {
            "name": name,
            "inferred_type": _thin_saas_logical_to_carrier(str(native.get(name) or "string")),
            "nullable": True,
            "data_type": str(native.get(name) or "string"),
            "label": name,
        }
        for name in headers
        if name
    ]
    columns = _stamp_thin_saas_write_carriers(brand, table, columns, cfg)
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": meta.get("certification") or "planned_typed_read",
        "saas_typed": bool(meta.get("saas_typed")),
    }


def _stamp_thin_saas_write_carriers(
    brand: str,
    table: str,
    columns: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """Overlay writer SSOT carriers (VARCHAR(n)/DECIMAL) onto thin-SaaS Map types."""
    if not columns:
        return columns
    names = [str(c.get("name") or "") for c in columns if c.get("name")]
    live: dict[str, str] = {}
    try:
        if brand == "stripe":
            from connectors.saas_write_carriers import stripe_live_types_for_columns

            live = stripe_live_types_for_columns(table, names)
        elif brand == "airtable":
            from connectors.airtable_writer import (
                airtable_field_to_carrier,
                _fetch_table_fields,
            )
            from connectors.saas_common import token as saas_token

            access = saas_token(
                str(cfg.get("api_key") or ""),
                str(cfg.get("connection_string") or ""),
                str(cfg.get("username") or ""),
                str(cfg.get("password") or ""),
            )
            base_id = str(cfg.get("database") or "").strip()
            if access and base_id:
                meta_fields, meta_exc = _fetch_table_fields(base_id, table, access)
                if meta_exc is None and meta_fields:
                    for f in meta_fields:
                        if isinstance(f, dict) and f.get("name"):
                            live[str(f["name"])] = airtable_field_to_carrier(f)
        elif brand == "notion":
            from connectors.notion_writer import (
                notion_property_to_carrier,
                _fetch_database_properties,
                _database_id,
            )
            from connectors.saas_common import token as saas_token

            access = saas_token(
                str(cfg.get("api_key") or ""),
                str(cfg.get("connection_string") or ""),
                str(cfg.get("username") or ""),
                str(cfg.get("password") or ""),
            )
            db_id = _database_id(table or str(cfg.get("database") or ""))
            if access and db_id:
                props, options = _fetch_database_properties(db_id, access)
                for name, typ in (props or {}).items():
                    live[name] = notion_property_to_carrier(
                        typ,
                        option_names=(
                            (options or {}).get(str(name).lower())
                            or (options or {}).get(name)
                        ),
                    )
    except Exception:
        live = {}
    if not live:
        return columns
    live_l = {str(k).lower(): v for k, v in live.items()}
    out: list[dict[str, Any]] = []
    for c in columns:
        name = str(c.get("name") or "")
        stamped = live.get(name) or live_l.get(name.lower())
        if stamped:
            out.append({**c, "inferred_type": stamped})
        else:
            out.append(c)
    return out


def _introspect_hubspot(**kwargs: Any) -> dict[str, Any]:
    """HubSpot Properties API → typed columns for Map / preflight."""
    try:
        from connectors.hubspot import describe_properties, list_object_types
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    cfg = _saas_cfg(**kwargs)
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    notes: list[str] = []
    try:
        tables = list_object_types(cfg, notes=notes)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    note = {"warning": " ".join(notes)} if notes else {}

    if not table:
        return {"ok": True, "columns": [], "tables": tables, "schema": "", **note}

    try:
        props = describe_properties(cfg, table)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": tables, **note}

    columns = [
        {
            "name": "id",
            "inferred_type": "VARCHAR(65536)",
            "nullable": False,
            "data_type": "string",
            "label": "Record ID",
        }
    ]
    try:
        from connectors.hubspot_writer import hubspot_property_to_carrier
    except Exception:
        hubspot_property_to_carrier = None  # type: ignore[assignment]
    for p in props:
        if not p.get("name") or p["name"] == "id":
            continue
        if hubspot_property_to_carrier is not None:
            inferred = hubspot_property_to_carrier(p)
        else:
            inferred = hubspot_property_to_logical(
                str(p.get("type") or "string"),
                field_type=str(p.get("fieldType") or ""),
                number_display_hint=str(p.get("numberDisplayHint") or ""),
                name=str(p.get("name") or ""),
            )
        columns.append(
            {
                "name": p["name"],
                "inferred_type": inferred,
                "nullable": True,
                "data_type": p.get("type") or "string",
                "label": p.get("label") or "",
            }
        )
    return {"ok": True, "columns": columns, "tables": tables, "schema": table, **note}


def _introspect_shopify(**kwargs: Any) -> dict[str, Any]:
    """Shopify Admin core + live metafield definitions → Map carriers."""
    table = (kwargs.get("table") or kwargs.get("database") or "customers").strip()
    cfg = _saas_cfg(**kwargs)
    cfg["shop"] = cfg.get("host") or ""
    metafield_defs: list[dict[str, Any]] = []
    try:
        from connectors.shopify import describe_metafield_definitions

        metafield_defs = describe_metafield_definitions(cfg, table) or []
    except Exception:
        metafield_defs = []
    try:
        from connectors.saas_write_carriers import (
            shopify_core_field_carriers,
            shopify_live_types_for_columns,
        )

        # Core Admin fields always stamp (note 5000, email 255) even without samples.
        core = shopify_core_field_carriers(table)
        names = list(
            dict.fromkeys(
                [
                    *core.keys(),
                    *[
                        f"{d.get('namespace')}.{d.get('key')}"
                        for d in metafield_defs
                        if d.get("namespace") and d.get("key")
                    ],
                    *[str(d.get("key")) for d in metafield_defs if d.get("key")],
                ]
            )
        )
        live = shopify_live_types_for_columns(
            table, names, metafield_defs=metafield_defs
        )
        for k, v in core.items():
            live.setdefault(k, v)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": [table]}

    columns = [
        {
            "name": name,
            "inferred_type": carrier,
            "nullable": name.lower() != "id",
            "data_type": carrier,
            "label": name,
        }
        for name, carrier in live.items()
        if name
    ]
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": "planned_typed_read",
        "saas_typed": True,
    }


def _introspect_zendesk(**kwargs: Any) -> dict[str, Any]:
    """Zendesk ticket/user/org fields → Map carriers (writer SSOT)."""
    table = (kwargs.get("table") or kwargs.get("database") or "tickets").strip()
    cfg = _saas_cfg(**kwargs)
    try:
        from connectors.zendesk import describe_fields
        from connectors.zendesk_writer import zendesk_field_to_carrier

        fields = describe_fields(cfg, table) or []
    except Exception as exc:
        # Fail closed — never invent seed VARCHAR carriers as saas_typed truth.
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": [],
        }

    seen: set[str] = set()
    columns: list[dict[str, Any]] = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        carrier = zendesk_field_to_carrier(f)
        for key in (f.get("name"), f.get("title")):
            name = str(key or "").strip()
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            columns.append(
                {
                    "name": name,
                    "inferred_type": carrier,
                    "nullable": True,
                    "data_type": str(f.get("type") or carrier),
                    "label": str(f.get("title") or name),
                }
            )
    if not columns:
        return {
            "ok": False,
            "error": (
                f"Zendesk schema Describe returned no fields for {table!r} — "
                "refuse Map VARCHAR invent (empty→null invent risk)."
            ),
            "columns": [],
            "tables": [table],
        }
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": "planned_typed_read",
        "saas_typed": True,
    }
