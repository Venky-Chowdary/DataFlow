"""Connector capability manifest — single source of truth for catalog honesty."""

from __future__ import annotations

from typing import Any

# Driver-level capabilities (implemented in connectors/ + adapters.py)
_DRIVER_CAPS: dict[str, dict[str, bool]] = {
    "postgresql": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "mysql": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "mongodb": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "snowflake": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "bigquery": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "dynamodb": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "redis": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "s3": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "elasticsearch": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "redshift": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
    "gcs": {"test": True, "read": True, "write": True, "introspect": True, "preflight": True},
}

# File format capabilities (FileParser + registry)
_FILE_CAPS: dict[str, dict[str, bool]] = {
    "csv": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "tsv": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "json": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "jsonl": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "ndjson": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "excel": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
    "parquet": {"test": True, "read": True, "write": True, "file_source": True, "file_export": True},
}

# Catalog marketplace id → driver / format type
CATALOG_ID_ALIASES: dict[str, str] = {
    "csv___tsv": "csv",
    "amazon_s3": "s3",
    "aws_s3": "s3",
    "google_cloud_storage": "gcs",
    "gcs": "gcs",
    "minio": "s3",
    "wasabi": "s3",
    "backblaze_b2": "s3",
    "digitalocean_spaces": "s3",
    "cloudflare_r2": "s3",
    "amazon_redshift": "redshift",
    "google_bigquery": "bigquery",
    "opensearch": "elasticsearch",
    "amazon_elasticsearch": "elasticsearch",
    "elastic_cloud": "elasticsearch",
    "amazon_dynamodb": "dynamodb",
    "planetscale": "mysql",
    "mariadb": "mysql",
    "percona_mysql": "mysql",
    "amazon_aurora_mysql": "mysql",
    "amazon_aurora_postgresql": "postgresql",
    "amazon_rds_postgresql": "postgresql",
    "amazon_rds_mysql": "mysql",
    "google_cloud_sql_postgresql": "postgresql",
    "google_cloud_sql_mysql": "mysql",
    "azure_database_postgresql": "postgresql",
    "azure_database_mysql": "mysql",
    "supabase": "postgresql",
    "neon": "postgresql",
    "timescaledb": "postgresql",
    "cockroachdb": "postgresql",
    "jsonl": "jsonl",
    "ndjson": "ndjson",
}

# Suggested lists — only connectors users can configure today
SUGGESTED_SOURCES = [
    "postgresql", "mongodb", "mysql", "snowflake", "bigquery", "redshift",
    "csv___tsv", "json", "jsonl", "excel", "parquet",
    "dynamodb", "amazon_s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
]

# Catalog entry ids that map to implemented drivers — blocks false "Full transfer" on aliases
TRANSFER_READY_CATALOG_IDS = frozenset({
    "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
    "dynamodb", "amazon_s3", "s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
    "csv___tsv", "json", "jsonl", "ndjson", "excel", "parquet",
})

SUGGESTED_DESTINATIONS = [
    "postgresql", "mongodb", "mysql", "snowflake", "bigquery", "redshift",
    "dynamodb", "amazon_s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
]


def default_port(driver_type: str) -> int:
    return {
        "mongodb": 27017,
        "mysql": 3306,
        "postgresql": 5432,
        "redis": 6379,
        "elasticsearch": 9200,
        "snowflake": 443,
        "bigquery": 443,
        "dynamodb": 443,
        "s3": 443,
        "gcs": 443,
        "redshift": 5439,
    }.get((driver_type or "").lower(), 5432)


def resolve_driver_type(catalog_id: str) -> str:
    """Map catalog entry id to implemented driver or file format key."""
    cid = (catalog_id or "").lower().strip()
    if not cid:
        return "unknown"
    if cid in CATALOG_ID_ALIASES:
        return CATALOG_ID_ALIASES[cid]
    if cid in _DRIVER_CAPS:
        return cid
    if cid in _FILE_CAPS:
        return cid

    # Strict substring match — only if target is a known implemented type
    for needle, driver in [
        ("postgres", "postgresql"),
        ("mongo", "mongodb"),
        ("documentdb", "mongodb"),
        ("mysql", "mysql"),
        ("mariadb", "mysql"),
        ("aurora", "mysql"),
        ("planetscale", "mysql"),
        ("vitess", "mysql"),
        ("tidb", "mysql"),
        ("oceanbase", "mysql"),
        ("polardb", "mysql"),
        ("singlestore", "mysql"),
        ("gaussdb", "mysql"),
        ("goldendb", "mysql"),
        ("percona", "mysql"),
        ("snowflake", "snowflake"),
        ("bigquery", "bigquery"),
        ("dynamodb", "dynamodb"),
        ("redis", "redis"),
        ("elastic", "elasticsearch"),
        ("opensearch", "elasticsearch"),
        ("redshift", "redshift"),
        ("timescale", "postgresql"),
        ("cockroach", "postgresql"),
        ("yugabyte", "postgresql"),
        ("alloydb", "postgresql"),
        ("supabase", "postgresql"),
        ("neon", "postgresql"),
        ("minio", "s3"),
        ("wasabi", "s3"),
        ("backblaze", "s3"),
        ("spaces", "s3"),
        ("object_storage", "s3"),
    ]:
        if needle in cid and driver in _DRIVER_CAPS:
            return driver
    if "gcs" in cid or "google_cloud_storage" in cid or ("cloud_storage" in cid and "google" in cid):
        return "gcs"
    if "parquet" in cid:
        return "parquet"
    if "s3" in cid or "aws_s3" in cid:
        return "s3"
    if cid.startswith("jsonl") or cid.endswith("jsonl"):
        return "jsonl"
    if "json" in cid and "jsonl" not in cid:
        return "json"
    if "csv" in cid or "tsv" in cid:
        return "csv"

    base = cid.replace("___", "_").split("_")[0]
    if base in _DRIVER_CAPS or base in _FILE_CAPS:
        return base
    return base


def get_capabilities(driver_type: str) -> dict[str, bool]:
    if driver_type in _DRIVER_CAPS:
        return dict(_DRIVER_CAPS[driver_type])
    if driver_type in _FILE_CAPS:
        return dict(_FILE_CAPS[driver_type])
    return {"test": False, "read": False, "write": False, "introspect": False, "preflight": False}


def transfer_ready(caps: dict[str, bool]) -> bool:
    """True when connector supports production read+write transfer."""
    if caps.get("file_source"):
        return True
    return bool(caps.get("read") and caps.get("write"))


def connect_only(caps: dict[str, bool]) -> bool:
    return bool(caps.get("test") and not transfer_ready(caps))


def effective_status(caps: dict[str, bool], catalog_status: str = "") -> str:
    if transfer_ready(caps):
        return "live"
    if connect_only(caps):
        return "connect_only"
    if catalog_status in ("live", "beta") and not caps.get("test"):
        return "planned"
    return "planned"


def capability_label(caps: dict[str, bool]) -> str:
    if transfer_ready(caps):
        if caps.get("file_source"):
            return "File transfer"
        return "Full transfer"
    if connect_only(caps):
        return "Connection test only"
    return "Roadmap"


def _catalog_transfer_ready(catalog_id: str, driver: str, caps: dict[str, bool]) -> bool:
    """True only for native catalog ids — aliases resolve for routing but stay roadmap."""
    if not transfer_ready(caps):
        return False
    cid = (catalog_id or "").lower().strip()
    if not cid:
        return False
    # Native driver/format ids only — no alias inflation (e.g. amazon_rds_postgresql → planned)
    if cid in TRANSFER_READY_CATALOG_IDS:
        return True
    if cid in _DRIVER_CAPS or cid in _FILE_CAPS:
        return True
    return False


def enrich_catalog_entry(entry: dict[str, Any]) -> dict[str, Any]:
    catalog_id = (entry.get("id") or "").lower().strip()
    driver = resolve_driver_type(catalog_id)
    caps = get_capabilities(driver)
    ready = _catalog_transfer_ready(catalog_id, driver, caps)
    eff = "live" if ready else (
        "connect_only" if connect_only(caps) else effective_status(caps, entry.get("status", "planned"))
    )
    out = dict(entry)
    out["driver_type"] = driver
    out["capabilities"] = caps
    out["effective_status"] = eff
    out["transfer_ready"] = ready
    out["connect_only"] = connect_only(caps) and not ready
    out["capability_label"] = capability_label(caps) if ready else (
        "Connection test only" if connect_only(caps) else "Roadmap"
    )
    return out


def transfer_live_driver_types() -> list[str]:
    live = []
    for k, caps in {**_DRIVER_CAPS, **_FILE_CAPS}.items():
        if transfer_ready(caps):
            live.append(k)
    return sorted(set(live))


def manifest_summary() -> dict[str, Any]:
    from .registry import LIVE_MATRIX

    transfer_live = len(transfer_live_driver_types())
    connect_only_count = sum(1 for k in _DRIVER_CAPS if connect_only(_DRIVER_CAPS[k]))
    return {
        "transfer_live_drivers": transfer_live_driver_types(),
        "transfer_live_count": transfer_live,
        "connect_only_count": connect_only_count,
        "live_route_combinations": len(LIVE_MATRIX),
    }
