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
# Optional warehouses / CDC-adjacent packages. Classification now uses
# ``driver_available`` for every endpoint; this set remains the operator-facing
# list of packages that are commonly absent on demo hosts.
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

SkuRoute = tuple[str, str, str, str]

# Customer-handover SKU: sqlite / PostgreSQL / MySQL plus file formats that
# write those engines (or file_export). Warehouse, SaaS, vector, and emulator
# routes can be PRODUCTION_SKU sold on a desktop lab host without being a
# tenant cutover.
HANDOVER_CORE_ENGINES = frozenset({
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "mariadb",
})
HANDOVER_FILE_FORMATS = frozenset({"csv", "json", "yaml", "fixed_width"})


def _handover_endpoint(kind: str, fmt: str) -> bool:
    family = (fmt or "").strip().lower()
    slot = (kind or "").strip().lower()
    if family in HANDOVER_CORE_ENGINES:
        return True
    if slot in {"file", "file_export"} and family in HANDOVER_FILE_FORMATS:
        return True
    return False


def route_is_customer_handover(src_kind: str, src_fmt: str, dst_kind: str, dst_fmt: str) -> bool:
    """True when both ends are the relational/file core a tenant cutover can run."""
    return _handover_endpoint(src_kind, src_fmt) and _handover_endpoint(dst_kind, dst_fmt)


def route_driver_gap(src_fmt: str, dst_fmt: str) -> str | None:
    """Why this host cannot execute the route, or None when drivers are present.

    Core OLTP packages (psycopg2 / pymysql / pymongo) are the same class of
    gap as optional warehouses: environment, not a refused SKU and not Planned.
    """
    src = resolve_driver_type(src_fmt)
    dst = resolve_driver_type(dst_fmt)
    if not driver_available(src, src_fmt):
        return f"source driver {src} not installed"
    if not driver_available(dst, dst_fmt):
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
    eligible = route_is_customer_handover(src_kind, src_fmt, dst_kind, dst_fmt)
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
        "customer_handover_eligible": eligible,
        "customer_handover": status == "sold" and eligible,
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
    handover = [r for r in rows if r.get("customer_handover")]
    handover_eligible = [r for r in rows if r.get("customer_handover_eligible")]
    return {
        "production_sku_claimed": len(rows),
        "production_sku_sold": len(sold),
        "production_sku_driver_missing": len(missing),
        "production_sku_refused": len(refused),
        "sold_routes": [r["route"] for r in sold],
        "driver_missing_routes": [r["route"] for r in missing],
        "refused_routes": [r["route"] for r in refused],
        "customer_handover_sold": len(handover),
        "customer_handover_eligible": len(handover_eligible),
        "customer_handover_routes": [r["route"] for r in handover],
        "note": (
            f"{len(sold)} of {len(rows)} PRODUCTION_SKU routes are sold on this host "
            f"(validate_transfer + driver present). {len(missing)} driver-missing, "
            f"{len(refused)} refused. Customer handover is {len(handover)} sold "
            f"sqlite/PostgreSQL/MySQL/file routes — warehouse/SaaS/vector SKU is "
            f"desktop-lab, not a tenant cutover. Catalog tiles are not this list."
        ),
    }
