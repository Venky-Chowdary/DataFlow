"""PRODUCTION_SKU sold-now classification — one owner.

A route is sold only when ``validate_transfer`` accepts it **and** the
required driver package is loadable on this host. Missing optional packages
are an environment gap, not a Planned tile and not a silent green.

Catalog tiles are a larger number and are never this list.
"""

from __future__ import annotations

from typing import Any

from src.transfer.connector_capabilities import driver_available, resolve_driver_type
from src.transfer.registry import PRODUCTION_SKU, validate_transfer

# Destinations / sources that require optional packages; classify as
# driver_missing (not refused, not sold) when the package is absent.
OPTIONAL_DRIVERS = frozenset({
    "sqlserver",
    "oracle",
    "sftp",
    "adls",
    "snowflake",
    "bigquery",
    "s3",
    "salesforce",
    "hubspot",
    "pgvector",
    "qdrant",
    "weaviate",
    "pinecone",
    "milvus",
    "kafka",
})

_CORE_SOURCES = frozenset({
    "csv", "json", "jsonl", "tsv", "parquet", "ndjson", "excel", "avro", "orc", "xml",
    "sqlite", "postgresql", "mysql", "mongodb", "rest_api", "iceberg",
})

_CORE_DESTS = frozenset({
    "sqlite", "postgresql", "mysql", "mongodb",
    "csv", "json", "jsonl", "tsv", "excel", "parquet", "ndjson", "avro", "orc", "xml",
    "iceberg", "rest_api",
})

SkuRoute = tuple[str, str, str, str]


def route_driver_gap(src_fmt: str, dst_fmt: str) -> str | None:
    """Why this host cannot execute the route, or None when drivers are present."""
    src = resolve_driver_type(src_fmt)
    dst = resolve_driver_type(dst_fmt)
    if src in OPTIONAL_DRIVERS and not driver_available(src, src_fmt):
        return f"source driver {src} not installed"
    if dst in OPTIONAL_DRIVERS and not driver_available(dst, dst_fmt):
        return f"destination driver {dst} not installed"
    if src not in _CORE_SOURCES and src not in OPTIONAL_DRIVERS and not driver_available(src, src_fmt):
        return f"source driver {src} not installed"
    if dst not in _CORE_DESTS and dst not in OPTIONAL_DRIVERS and not driver_available(dst, dst_fmt):
        return f"destination driver {dst} not installed"
    return None


def classify_sku_route(route: SkuRoute) -> dict[str, Any]:
    """Classify one committed SKU route for this host.

    Status:
      sold — validate_transfer accepts and drivers are present (sell this)
      driver_missing — certified/committed but package not loadable here
      refused — validate_transfer rejects even with drivers present (do not sell)
    """
    src_kind, src_fmt, dst_kind, dst_fmt = route
    gap = route_driver_gap(src_fmt, dst_fmt)
    ok, msg = validate_transfer(src_kind, src_fmt, dst_kind, dst_fmt)
    if gap:
        status = "driver_missing"
    elif ok:
        status = "sold"
    else:
        status = "refused"
    return {
        "source_kind": src_kind,
        "source_format": src_fmt,
        "dest_kind": dst_kind,
        "dest_format": dst_fmt,
        "route": f"{src_kind}/{src_fmt} → {dst_kind}/{dst_fmt}",
        "status": status,
        "validate_ok": bool(ok),
        "validate_msg": msg,
        "driver_gap": gap,
        "sold": status == "sold",
    }


def classify_production_sku(
    routes: list[SkuRoute] | None = None,
) -> list[dict[str, Any]]:
    return [classify_sku_route(route) for route in (routes or PRODUCTION_SKU)]


def sku_honesty_summary(
    classified: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = classified if classified is not None else classify_production_sku()
    sold = [r for r in rows if r["status"] == "sold"]
    missing = [r for r in rows if r["status"] == "driver_missing"]
    refused = [r for r in rows if r["status"] == "refused"]
    return {
        "production_sku_claimed": len(rows),
        "production_sku_sold": len(sold),
        "production_sku_driver_missing": len(missing),
        "production_sku_refused": len(refused),
        "sold_routes": [r["route"] for r in sold],
        "driver_missing_routes": [r["route"] for r in missing],
        "refused_routes": [r["route"] for r in refused],
        "note": (
            f"{len(sold)} of {len(rows)} PRODUCTION_SKU routes are sold on this host "
            f"(validate_transfer + driver present). {len(missing)} driver-missing, "
            f"{len(refused)} refused. Catalog tiles are not this list."
        ),
    }
