"""Documented SaaS reverse-ETL write carriers (Stripe / Shopify).

These catalogs encode **platform-published** field limits (Stripe API reference,
Shopify Admin / metafield type docs). They are not mocks — writers merge them
as ``live_types`` into ``resolve_mapping_dest_types`` so quarantine catches
overflow before REST invents bad CRM/commerce rows.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Shared nested address leaves (Stripe Customer.address / Shopify
# default_address / billing_address / shipping_address).
# Only documented Admin/API leaves — never soft-VARCHAR invent for unknowns.
# ---------------------------------------------------------------------------

_ADDRESS_LEAF_CARRIERS: dict[str, str] = {
    # Shopify Admin address shape
    "address1": "VARCHAR(255)",
    "address2": "VARCHAR(255)",
    # Stripe Customer.address / PaymentMethod.billing_details.address
    "line1": "VARCHAR(255)",
    "line2": "VARCHAR(255)",
    "city": "VARCHAR(255)",
    "province": "VARCHAR(255)",
    "province_code": "VARCHAR(16)",
    "state": "VARCHAR(255)",
    "state_code": "VARCHAR(16)",
    "country": "VARCHAR(2)",
    "country_code": "VARCHAR(2)",
    "country_name": "VARCHAR(255)",
    "zip": "VARCHAR(20)",
    "postal_code": "VARCHAR(20)",
    "phone": "VARCHAR(50)",
    "company": "VARCHAR(255)",
    "name": "VARCHAR(255)",
    "first_name": "VARCHAR(255)",
    "last_name": "VARCHAR(255)",
    "latitude": "DECIMAL(10,7)",
    "longitude": "DECIMAL(10,7)",
}

_ADDRESS_NEST_PREFIXES: tuple[str, ...] = (
    "billing_details.address.",
    "billing_details_address_",
    "shipping_details.address.",
    "shipping_details_address_",
    "default_address.",
    "default_address_",
    "billing_address.",
    "billing_address_",
    "shipping_address.",
    "shipping_address_",
    "address.",
    "address_",
)


def address_leaf_carrier(column: str) -> str | None:
    """Return typed carrier for a flattened address leaf, or ``None`` if unknown.

    Unknown leaves must NOT soft-bind VARCHAR — callers refuse Map invent unless
    Studio provides ``destination_column_types``.
    """
    low = str(column or "").strip().lower()
    if not low:
        return None
    leaf: str | None = None
    # Longer prefixes first (default_address_ before address_).
    for prefix in _ADDRESS_NEST_PREFIXES:
        if low.startswith(prefix):
            leaf = low[len(prefix) :]
            break
    if leaf is None:
        return None
    return _ADDRESS_LEAF_CARRIERS.get(leaf)


# ---------------------------------------------------------------------------
# Stripe — OpenAPI / API reference maximum lengths (customers, products, …)
# ---------------------------------------------------------------------------

# Shared leaf names across many Stripe objects (amounts in smallest currency unit).
_STRIPE_COMMON: dict[str, str] = {
    "id": "VARCHAR(255)",
    "object": "VARCHAR(64)",
    "created": "INTEGER",
    "livemode": "BOOLEAN",
    "currency": "VARCHAR(3)",
    "customer": "VARCHAR(255)",
    "status": "VARCHAR(64)",
    # Default description bound; subscriptions override to 500 (API docs).
    "description": "VARCHAR(10000)",
    "metadata": "VARCHAR(500)",  # per-value limit when flattened
    "amount": "INTEGER",
    "amount_due": "INTEGER",
    "amount_paid": "INTEGER",
    "amount_remaining": "INTEGER",
    "amount_capturable": "INTEGER",
    "amount_received": "INTEGER",
    "balance": "INTEGER",
    "paid": "BOOLEAN",
    "refunded": "BOOLEAN",
    "delinquent": "BOOLEAN",
    "captured": "BOOLEAN",
    "disputed": "BOOLEAN",
    "quantity": "INTEGER",
    "unit_amount": "INTEGER",
    "unit_amount_decimal": "DECIMAL(38,12)",
}

_STRIPE_BY_OBJECT: dict[str, dict[str, str]] = {
    "customers": {
        "email": "VARCHAR(512)",
        "name": "VARCHAR(256)",
        "phone": "VARCHAR(20)",
        "business_name": "VARCHAR(150)",
        "individual_name": "VARCHAR(150)",
        "invoice_prefix": "VARCHAR(12)",
        "tax_exempt": "ENUM('none','exempt','reverse')",
        "next_invoice_sequence": "INTEGER",
    },
    "customer": {},  # alias filled below
    "products": {
        "name": "VARCHAR(250)",
        "active": "BOOLEAN",
        "type": "VARCHAR(32)",
        "unit_label": "VARCHAR(12)",
        "url": "VARCHAR(2048)",
        "statement_descriptor": "VARCHAR(22)",
    },
    "product": {},
    "prices": {
        "unit_amount": "INTEGER",
        "unit_amount_decimal": "DECIMAL(38,12)",
        "billing_scheme": "ENUM('per_unit','tiered')",
        "nickname": "VARCHAR(250)",
        "active": "BOOLEAN",
        "type": "ENUM('one_time','recurring')",
        "lookup_key": "VARCHAR(200)",
        "tax_behavior": "ENUM('inclusive','exclusive','unspecified')",
    },
    "price": {},
    "payment_intents": {
        "receipt_email": "VARCHAR(512)",
        "statement_descriptor": "VARCHAR(22)",
        "statement_descriptor_suffix": "VARCHAR(22)",
        "capture_method": "VARCHAR(32)",
        "confirmation_method": "VARCHAR(32)",
    },
    "payment_intent": {},
    "charges": {
        "receipt_email": "VARCHAR(512)",
        "statement_descriptor": "VARCHAR(22)",
        "failure_code": "VARCHAR(64)",
        "failure_message": "VARCHAR(2000)",
    },
    "charge": {},
    "invoices": {
        "number": "VARCHAR(64)",
        "footer": "VARCHAR(5000)",
        "collection_method": "VARCHAR(32)",
        "customer_email": "VARCHAR(512)",
        "customer_name": "VARCHAR(256)",
        "statement_descriptor": "VARCHAR(22)",
    },
    "invoice": {},
    # Subscription description max 500 (Stripe subscriptions/create).
    "subscriptions": {
        "description": "VARCHAR(500)",
        "collection_method": "VARCHAR(32)",
        "billing_cycle_anchor": "INTEGER",
        "cancel_at": "INTEGER",
        "canceled_at": "INTEGER",
        "current_period_end": "INTEGER",
        "current_period_start": "INTEGER",
        "days_until_due": "INTEGER",
        "default_payment_method": "VARCHAR(255)",
        "trial_end": "INTEGER",
        "trial_start": "INTEGER",
        "cancel_at_period_end": "BOOLEAN",
    },
    "subscription": {},
    "subscription_items": {
        "price": "VARCHAR(255)",
        "quantity": "INTEGER",
        "subscription": "VARCHAR(255)",
    },
    "subscription_item": {},
    "refunds": {
        "reason": "VARCHAR(64)",
        "receipt_number": "VARCHAR(64)",
        "payment_intent": "VARCHAR(255)",
        "charge": "VARCHAR(255)",
    },
    "refund": {},
    "payment_methods": {
        "type": "VARCHAR(64)",
        "billing_details_email": "VARCHAR(512)",
        "billing_details_name": "VARCHAR(256)",
        "billing_details_phone": "VARCHAR(20)",
    },
    "payment_method": {},
    "coupons": {
        "name": "VARCHAR(40)",
        "duration": "ENUM('forever','once','repeating')",
        "percent_off": "DECIMAL(38,10)",
        "amount_off": "INTEGER",
        "max_redemptions": "INTEGER",
        "redeem_by": "INTEGER",
        "valid": "BOOLEAN",
    },
    "coupon": {},
    "promotion_codes": {
        "code": "VARCHAR(500)",
        "active": "BOOLEAN",
        "max_redemptions": "INTEGER",
        "expires_at": "INTEGER",
        "coupon": "VARCHAR(255)",
    },
    "promotion_code": {},
}


def _normalize_stripe_object(object_type: str) -> str:
    obj = (object_type or "customers").strip().lower().lstrip("/")
    if obj.endswith("s") and obj not in _STRIPE_BY_OBJECT:
        # already plural preferred
        pass
    aliases = {
        "customer": "customers",
        "product": "products",
        "price": "prices",
        "payment_intent": "payment_intents",
        "charge": "charges",
        "invoice": "invoices",
        "subscription": "subscriptions",
        "subscription_item": "subscription_items",
        "refund": "refunds",
        "payment_method": "payment_methods",
        "coupon": "coupons",
        "promotion_code": "promotion_codes",
    }
    return aliases.get(obj, obj)


def stripe_field_carriers(object_type: str = "customers") -> dict[str, str]:
    """Return documented Stripe write carriers for an object collection."""
    key = _normalize_stripe_object(object_type)
    out = dict(_STRIPE_COMMON)
    out.update(_STRIPE_BY_OBJECT.get(key) or {})
    # Fill empty singular stubs from plurals (defensive).
    if not _STRIPE_BY_OBJECT.get(key) and key.endswith("s"):
        out.update(_STRIPE_BY_OBJECT.get(key[:-1]) or {})
    return out


def stripe_live_types_for_columns(
    object_type: str,
    target_cols: list[str],
) -> dict[str, str]:
    """Map target columns → carriers; metadata.* / metadata_ → VARCHAR(500)."""
    catalog = stripe_field_carriers(object_type)
    live: dict[str, str] = {}
    for col in target_cols:
        low = str(col).lower()
        if low in catalog:
            live[col] = catalog[low]
            continue
        if low.startswith("metadata.") or low.startswith("metadata_"):
            live[col] = "VARCHAR(500)"
            continue
        # Nested address leaves — typed documented leaves only (no soft invent).
        addr = address_leaf_carrier(col)
        if addr:
            live[col] = addr
    return live


def merge_stripe_catalog_types(
    object_type: str,
    target_cols: list[str],
    *,
    studio_types: dict[str, Any] | None = None,
) -> tuple[dict[str, str], str | None]:
    """Catalog∩Studio coverage gate for Stripe Map bind (no live Describe API).

    Returns ``(live_types, None)`` when every mapped column has a documented
    OpenAPI carrier or a Studio-typed destination carrier. Uncatalogued Map
    columns without Studio types return an error — never soft-bind VARCHAR
    (empty→null / overflow invent risk on create).
    """
    live = stripe_live_types_for_columns(object_type, target_cols)
    studio = studio_types if isinstance(studio_types, dict) else {}
    studio_l = {
        str(k).lower(): str(v).strip()
        for k, v in studio.items()
        if k and str(v or "").strip()
    }
    merged = dict(live)
    missing: list[str] = []
    for col in target_cols:
        if not col:
            continue
        if col in live:
            continue
        st = studio_l.get(str(col).lower())
        if st:
            merged[col] = st
            continue
        missing.append(col)
    if missing:
        sample = ", ".join(repr(c) for c in missing[:12])
        more = f" (+{len(missing) - 12} more)" if len(missing) > 12 else ""
        return merged, (
            f"Stripe OpenAPI catalog has no carrier for mapped field(s) "
            f"{sample}{more} — refuse Map VARCHAR invent (empty→null / "
            "overflow risk). Map documented Stripe fields or provide Studio "
            "destination_column_types for custom/metadata leaves."
        )
    return merged, None


# ---------------------------------------------------------------------------
# Shopify — Admin core fields + metafield type → carrier
# ---------------------------------------------------------------------------

_SHOPIFY_NOTE = 5_000
_SHOPIFY_SINGLE_LINE = 255
# number_decimal: +/-9999999999999.999999999 → DECIMAL(22,9)
_SHOPIFY_DECIMAL = "DECIMAL(22,9)"

# Shopify Admin metafield measurement / structured types store JSON
# {"value":…,"unit":…} (or money/rating/link/rich_text shapes).
# https://shopify.dev/docs/apps/build/metafields/list-of-data-types
_SHOPIFY_MEASUREMENT_JSON_TYPES = frozenset(
    {
        "antenna_gain",
        "area",
        "battery_charge_capacity",
        "battery_energy_capacity",
        "capacitance",
        "concentration",
        "data_storage_capacity",
        "data_transfer_rate",
        "dimension",
        "display_density",
        "distance",
        "duration",
        "electric_current",
        "electrical_resistance",
        "energy",
        "frequency",
        "illuminance",
        "inductance",
        "luminous_flux",
        "mass",
        "mass_flow_rate",
        "power",
        "pressure",
        "resolution",
        "rotational_speed",
        "sound_level",
        "speed",
        "temperature",
        "thermal_power",
        "viscosity",
        "voltage",
        "volume",
        "volume_of_liquid",
        "volumetric_flow_rate",
        "weight",
        "length",
        "density",
    }
)

_SHOPIFY_BY_OBJECT: dict[str, dict[str, str]] = {
    "customers": {
        "email": "VARCHAR(255)",
        "first_name": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "last_name": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "phone": "VARCHAR(50)",
        "note": f"VARCHAR({_SHOPIFY_NOTE})",
        "tags": "VARCHAR(65535)",
        "verified_email": "BOOLEAN",
        "tax_exempt": "BOOLEAN",
        "accepts_marketing": "BOOLEAN",
        "multipass_identifier": "VARCHAR(255)",
        "currency": "VARCHAR(3)",
    },
    "products": {
        "title": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "body_html": "VARCHAR(65535)",
        "vendor": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "product_type": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "handle": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "tags": "VARCHAR(65535)",
        "published": "BOOLEAN",
        "template_suffix": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "status": "VARCHAR(32)",
    },
    "orders": {
        "note": f"VARCHAR({_SHOPIFY_NOTE})",
        "email": "VARCHAR(255)",
        "phone": "VARCHAR(50)",
        "tags": "VARCHAR(65535)",
        "currency": "VARCHAR(3)",
        "financial_status": "VARCHAR(32)",
        "fulfillment_status": "VARCHAR(32)",
        "total_price": _SHOPIFY_DECIMAL,
        "subtotal_price": _SHOPIFY_DECIMAL,
        "total_tax": _SHOPIFY_DECIMAL,
        "total_discounts": _SHOPIFY_DECIMAL,
        "name": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "gateway": "VARCHAR(64)",
        "source_name": "VARCHAR(64)",
    },
    "variants": {
        "title": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "sku": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "barcode": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "price": _SHOPIFY_DECIMAL,
        "compare_at_price": _SHOPIFY_DECIMAL,
        "weight": _SHOPIFY_DECIMAL,
        "inventory_quantity": "INTEGER",
        "taxable": "BOOLEAN",
        "requires_shipping": "BOOLEAN",
    },
    # Draft orders share note 5000 / tag semantics with orders (Admin + Help Center).
    "draft_orders": {
        "note": f"VARCHAR({_SHOPIFY_NOTE})",
        "email": "VARCHAR(255)",
        "phone": "VARCHAR(50)",
        "tags": "VARCHAR(65535)",
        "currency": "VARCHAR(3)",
        "name": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "status": "VARCHAR(32)",
        "total_price": _SHOPIFY_DECIMAL,
        "subtotal_price": _SHOPIFY_DECIMAL,
        "total_tax": _SHOPIFY_DECIMAL,
        # Shopify Admin returns RFC3339 instants — TIMESTAMPTZ polarity.
        "invoice_sent_at": "TIMESTAMPTZ",
        "completed_at": "TIMESTAMPTZ",
    },
    "draft_order": {},
    "draftorders": {},
    "collections": {
        "title": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "body_html": "VARCHAR(65535)",
        "handle": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "sort_order": "VARCHAR(64)",
        "template_suffix": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "published": "BOOLEAN",
    },
    "collection": {},
    "custom_collections": {},
    "smart_collections": {},
    "discounts": {
        "title": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "code": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "value": _SHOPIFY_DECIMAL,
        "value_type": "VARCHAR(32)",
        "target_type": "VARCHAR(32)",
        "status": "VARCHAR(32)",
    },
    "discount": {},
    "price_rules": {
        "title": f"VARCHAR({_SHOPIFY_SINGLE_LINE})",
        "value": _SHOPIFY_DECIMAL,
        "value_type": "VARCHAR(32)",
        "target_type": "VARCHAR(32)",
        "allocation_method": "VARCHAR(32)",
        "customer_selection": "VARCHAR(32)",
    },
    "price_rule": {},
}


def _normalize_shopify_object(object_type: str) -> str:
    obj = (object_type or "customers").strip().lower()
    aliases = {
        "customer": "customers",
        "product": "products",
        "order": "orders",
        "variant": "variants",
        "product_variants": "variants",
        "draft_order": "draft_orders",
        "draftorder": "draft_orders",
        "draftorders": "draft_orders",
        "collection": "collections",
        "custom_collection": "collections",
        "custom_collections": "collections",
        "smart_collection": "collections",
        "smart_collections": "collections",
        "discount": "discounts",
        "price_rule": "price_rules",
        "pricerule": "price_rules",
    }
    return aliases.get(obj, obj)


# Admin REST identity + common leaves present on every resource payload.
_SHOPIFY_COMMON: dict[str, str] = {
    "id": "VARCHAR(64)",
    "admin_graphql_api_id": "VARCHAR(255)",
    "created_at": "TIMESTAMPTZ",
    "updated_at": "TIMESTAMPTZ",
}


def shopify_core_field_carriers(object_type: str = "customers") -> dict[str, str]:
    """Return documented Shopify Admin core-field carriers for a resource."""
    key = _normalize_shopify_object(object_type)
    out = dict(_SHOPIFY_COMMON)
    out.update(_SHOPIFY_BY_OBJECT.get(key) or {})
    if len(out) == len(_SHOPIFY_COMMON) and key.endswith("s"):
        out.update(_SHOPIFY_BY_OBJECT.get(key[:-1]) or {})
    # Product Map often flattens variant sku onto the product object.
    if key == "products":
        out.setdefault("sku", f"VARCHAR({_SHOPIFY_SINGLE_LINE})")
    return out


def shopify_metafield_type_to_carrier(
    metafield_type: str,
    *,
    max_validation: int | None = None,
) -> str:
    """Map Shopify metafield type name → quarantine carrier.

    Research: Shopify Admin metafield types
    (https://shopify.dev/docs/apps/build/metafields/list-of-data-types).
    Values are always API strings, but Datawrap carriers preserve polarity so
    Map/Validate/quarantine do not invent TEXT for booleans, decimals, lists,
    or measurement JSON objects.
    """
    t = (metafield_type or "").strip().lower()
    if t.startswith("list."):
        inner = shopify_metafield_type_to_carrier(
            t[5:], max_validation=max_validation
        )
        # Unknown element type — refuse soft ARRAY<TEXT> invent (Studio gate).
        if not str(inner or "").strip():
            return ""
        # List payloads are JSON arrays of the element type.
        if inner == "BOOLEAN":
            return "ARRAY<BOOLEAN>"
        if inner == "BIGINT" or inner.startswith("DECIMAL"):
            return f"ARRAY<{inner}>"
        if inner in {"DATE", "TIMESTAMPTZ"}:
            return f"ARRAY<{inner}>"
        if inner == "JSON":
            return "ARRAY<JSON>"
        if inner.startswith("VARCHAR") or inner == "ARRAY<TEXT>":
            return "ARRAY<TEXT>"
        return "ARRAY<TEXT>"
    if t in {"boolean"}:
        return "BOOLEAN"
    if t in {"number_integer"}:
        return "BIGINT"
    if t in {"number_decimal"}:
        return _SHOPIFY_DECIMAL
    if t in {"json", "rich_text_field", "link", "money", "rating"}:
        # money/rating/link/rich_text are JSON objects per Shopify Admin docs.
        return "JSON"
    if t in _SHOPIFY_MEASUREMENT_JSON_TYPES:
        # {\"value\":…,\"unit\":…} measurement polarity — never collapse to TEXT.
        return "JSON"
    if t == "date":
        return "DATE"
    if t == "date_time":
        return "TIMESTAMPTZ"
    if t == "url":
        return "VARCHAR(2048)"
    if t == "color":
        return "VARCHAR(16)"
    if t == "language":
        return "VARCHAR(16)"
    if t in {
        "product_reference",
        "variant_reference",
        "collection_reference",
        "file_reference",
        "page_reference",
        "blog_reference",
        "article_reference",
        "metaobject_reference",
        "customer_reference",
        "company_reference",
        "order_reference",
    } or t.endswith("_reference"):
        # Shopify GID / resource id string.
        return "VARCHAR(255)"
    if t == "multi_line_text_field":
        if max_validation and max_validation > 0:
            return f"VARCHAR({max_validation})"
        return "VARCHAR(65535)"
    if t in {"single_line_text_field", "id"}:
        if max_validation and max_validation > 0:
            return f"VARCHAR({min(max_validation, 65535)})"
        return f"VARCHAR({_SHOPIFY_SINGLE_LINE})"
    # Unknown / new Shopify Admin types — refuse soft VARCHAR invent; Studio or
    # a documented carrier must cover the column (merge_shopify_catalog_types).
    return ""


def shopify_live_types_for_columns(
    object_type: str,
    target_cols: list[str],
    *,
    metafield_defs: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Core catalog + optional live metafield definitions → column carriers."""
    live: dict[str, str] = {}
    catalog = shopify_core_field_carriers(object_type)
    for col in target_cols:
        low = str(col).lower()
        if low in catalog:
            live[col] = catalog[low]
            continue
        # Flattened default_address / billing_address / shipping_address leaves.
        addr = address_leaf_carrier(col)
        if addr:
            live[col] = addr

    for d in metafield_defs or []:
        if not isinstance(d, dict):
            continue
        ns = str(d.get("namespace") or "").strip()
        key = str(d.get("key") or "").strip()
        typ = str(d.get("type") or d.get("typeName") or "").strip()
        max_v: int | None = None
        for v in d.get("validations") or []:
            if not isinstance(v, dict):
                continue
            if str(v.get("name") or "").lower() == "max":
                try:
                    max_v = int(float(str(v.get("value"))))
                except (TypeError, ValueError):
                    max_v = None
        if not typ.strip():
            # Empty metafield type from Describe — do not invent VARCHAR(2048).
            continue
        carrier = shopify_metafield_type_to_carrier(typ, max_validation=max_v)
        if not str(carrier or "").strip():
            # Unknown Admin type token — leave uncatalogued for Studio gate.
            continue
        names = []
        if ns and key:
            names.extend([f"{ns}.{key}", f"{ns}_{key}", key])
        if key:
            names.append(key)
        for name in names:
            live[name] = carrier
    return live


def merge_shopify_catalog_types(
    object_type: str,
    target_cols: list[str],
    *,
    metafield_defs: list[dict[str, Any]] | None = None,
    studio_types: dict[str, Any] | None = None,
) -> tuple[dict[str, str], str | None]:
    """Admin catalog∩metafield∩Studio gate (Stripe-class Map invent refuse).

    Every mapped column must hit Admin core, a live metafield definition, or
    Studio ``destination_column_types`` — never soft-bind Map VARCHAR.
    """
    live = shopify_live_types_for_columns(
        object_type, target_cols, metafield_defs=metafield_defs
    )
    live_l = {str(k).lower(): str(v) for k, v in live.items() if k and v}
    studio = studio_types if isinstance(studio_types, dict) else {}
    studio_l = {
        str(k).lower(): str(v).strip()
        for k, v in studio.items()
        if k and str(v or "").strip()
    }
    merged = dict(live)
    missing: list[str] = []
    for col in target_cols:
        if not col:
            continue
        if live_l.get(str(col).lower()):
            continue
        st = studio_l.get(str(col).lower())
        if st:
            merged[col] = st
            continue
        missing.append(col)
    if missing:
        sample = ", ".join(repr(c) for c in missing[:12])
        more = f" (+{len(missing) - 12} more)" if len(missing) > 12 else ""
        return merged, (
            f"Shopify Admin catalog has no carrier for mapped field(s) "
            f"{sample}{more} — refuse Map VARCHAR invent (empty→null / "
            "overflow risk). Map documented Admin fields, refresh metafield "
            "Describe, or provide Studio destination_column_types."
        )
    return merged, None
