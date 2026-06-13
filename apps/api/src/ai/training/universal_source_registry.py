"""
DataTransfer.space — Universal Source Registry

Expands training data from 620+ connector catalog entries, industry templates,
and category-specific schema patterns — simulating millions of data source profiles.
"""

from __future__ import annotations

from ..knowledge.industry_schemas import INDUSTRY_SCHEMAS
from ..training.universal_data_feeder import UniversalSchema

# Typical column sets per connector category (used for synthetic universal schemas)
CATEGORY_COLUMN_TEMPLATES: dict[str, list[str]] = {
    "database": [
        "id", "created_at", "updated_at", "name", "status", "metadata_json",
        "owner_id", "version", "is_active", "tags",
    ],
    "warehouse": [
        "record_id", "event_timestamp", "dimension_key", "measure_value",
        "source_system", "load_batch_id", "partition_date", "currency_code",
    ],
    "file": [
        "row_id", "file_name", "sheet_name", "column_a", "column_b",
        "parsed_at", "checksum", "encoding",
    ],
    "saas": [
        "object_id", "external_id", "email", "company_name", "created_at",
        "modified_at", "owner_email", "subscription_tier", "api_version",
    ],
    "marketing": [
        "campaign_id", "utm_source", "utm_medium", "utm_campaign", "clicks",
        "impressions", "conversions", "spend_usd", "audience_segment",
    ],
    "analytics": [
        "event_id", "user_id", "session_id", "event_name", "event_time",
        "page_url", "device_type", "country_code", "properties_json",
    ],
    "storage": [
        "object_key", "bucket", "size_bytes", "content_type", "last_modified",
        "etag", "storage_class", "region",
    ],
    "messaging": [
        "message_id", "topic", "partition", "offset", "payload_json",
        "headers_json", "produced_at", "consumer_group",
    ],
    "other": [
        "id", "name", "type", "value", "timestamp", "source", "status",
    ],
}

# Industry suffix columns merged into connector schemas for cross-domain training
INDUSTRY_OVERLAY_COLUMNS: dict[str, list[str]] = {
    key: list(schema["columns"].keys())[:6]
    for key, schema in INDUSTRY_SCHEMAS.items()
}


def _connector_columns(connector: dict) -> list[str]:
    """Build a realistic column list for a catalog connector."""
    category = connector.get("category", "other")
    base = list(CATEGORY_COLUMN_TEMPLATES.get(category, CATEGORY_COLUMN_TEMPLATES["other"]))
    cid = connector.get("id", "source").replace("___", "_")
    prefix = cid.split("_")[0][:12]
    prefixed = [f"{prefix}_{c}" if c in ("id", "name", "status") else c for c in base[:5]]
    cols = prefixed + base[5:]

    # Blend industry columns for richer universal profiles
    industry_key = list(INDUSTRY_SCHEMAS.keys())[hash(cid) % len(INDUSTRY_SCHEMAS)]
    overlay = INDUSTRY_OVERLAY_COLUMNS.get(industry_key, [])[:4]
    for col in overlay:
        if col not in cols:
            cols.append(col)

    return cols[:14]


def load_connector_schemas() -> list[UniversalSchema]:
    """One universal schema profile per catalog connector (620+ sources)."""
    from ...services.catalog_service import load_catalog

    schemas: list[UniversalSchema] = []
    for connector in load_catalog().get("connectors", []):
        cols = _connector_columns(connector)
        cid = connector["id"]
        samples: dict[str, list[str]] = {}
        for col in cols[:6]:
            sem = col.replace("_", " ")
            samples[col] = [f"sample_{sem[:20]}", f"example_{cid[:8]}"]

        schemas.append(UniversalSchema(
            name=f"connector_{cid}",
            source="catalog",
            columns=cols,
            samples=samples,
            row_count=1000 + (hash(cid) % 9000),
            industry=list(INDUSTRY_SCHEMAS.keys())[hash(cid) % len(INDUSTRY_SCHEMAS)],
            file_type=connector.get("category", ""),
        ))
    return schemas


def expand_schema_variants(schemas: list[UniversalSchema], max_variants: int = 3) -> list[UniversalSchema]:
    """
    Generate variant schemas (staging/raw/curated) per source for deeper training coverage.
    Caps variants to keep embedding ingest bounded while scaling conversation examples.
    """
    expanded: list[UniversalSchema] = []
    for schema in schemas:
        expanded.append(schema)
        for suffix in ("_raw", "_staging", "_curated")[:max_variants - 1]:
            variant_cols = [f"{suffix.strip('_')}_{c}" if c == "id" else c for c in schema.columns]
            expanded.append(UniversalSchema(
                name=f"{schema.name}{suffix}",
                source=schema.source,
                columns=variant_cols,
                samples=schema.samples,
                row_count=schema.row_count,
                industry=schema.industry,
                file_type=schema.file_type,
            ))
    return expanded


def get_universal_schema_count() -> dict:
    """Stats for training status UI."""
    connector_count = len(load_connector_schemas())
    industry_count = len(INDUSTRY_SCHEMAS)
    variant_count = connector_count * 3 + industry_count
    return {
        "connector_sources": connector_count,
        "industry_templates": industry_count,
        "estimated_schema_profiles": variant_count,
        "description": "620+ connectors × variants + industry templates",
    }
