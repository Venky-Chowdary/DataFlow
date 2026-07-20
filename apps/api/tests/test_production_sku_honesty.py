"""PRODUCTION_SKU honesty: every committed route validates or skips with reason.

Routes whose optional DBAPI / cloud drivers are missing must not silently fail —
they skip with an explicit driver-unavailable message. Planned brands must never
appear in PRODUCTION_SKU as live.
"""

from __future__ import annotations

import pytest

from src.transfer.connector_capabilities import driver_available, resolve_driver_type
from src.transfer.registry import PRODUCTION_SKU, validate_transfer

# Destinations that require optional packages; skip (not fail) when absent.
_OPTIONAL_DRIVERS = {
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
}


def _route_skip_reason(src_fmt: str, dst_fmt: str) -> str | None:
    src = resolve_driver_type(src_fmt)
    dst = resolve_driver_type(dst_fmt)
    if src in _OPTIONAL_DRIVERS and not driver_available(src, src_fmt):
        return f"source driver {src} not installed"
    if dst in _OPTIONAL_DRIVERS and not driver_available(dst, dst_fmt):
        return f"destination driver {dst} not installed"
    # File formats always "available"
    if src in {"csv", "json", "jsonl", "tsv", "parquet", "ndjson", "excel", "avro", "orc", "xml"}:
        pass
    elif src not in {"sqlite", "postgresql", "mysql", "mongodb", "rest_api", "iceberg"} and not driver_available(
        src, src_fmt
    ):
        return f"source driver {src} not installed"
    if dst not in {
        "sqlite",
        "postgresql",
        "mysql",
        "mongodb",
        "csv",
        "json",
        "jsonl",
        "tsv",
        "excel",
        "parquet",
        "ndjson",
        "avro",
        "orc",
        "xml",
        "iceberg",
        "rest_api",
    } and dst not in _OPTIONAL_DRIVERS and not driver_available(dst, dst_fmt):
        return f"destination driver {dst} not installed"
    return None


@pytest.mark.parametrize(
    "route",
    PRODUCTION_SKU,
    ids=lambda r: f"{r[0]}_{r[1]}_to_{r[2]}_{r[3]}",
)
def test_production_sku_validate_or_explicit_skip(route: tuple[str, str, str, str]) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    skip = _route_skip_reason(src_fmt, dst_fmt)
    ok, msg = validate_transfer(src_kind, src_fmt, dst_kind, dst_fmt)
    if skip:
        if not ok:
            pytest.skip(f"{skip}; validate_transfer={msg}")
        # Driver missing but route still in LIVE_MATRIX — document skip for execute tests.
        pytest.skip(skip)
    assert ok, f"PRODUCTION_SKU route must validate when drivers present: {route} → {msg}"
    assert "Planned" not in msg, f"PRODUCTION_SKU must not include Planned brands: {route} → {msg}"


def test_production_sku_has_no_planned_rest_stubs() -> None:
    for route in PRODUCTION_SKU:
        _, src_fmt, _, dst_fmt = route
        for fmt in (src_fmt, dst_fmt):
            driver = resolve_driver_type(fmt)
            assert fmt not in {"zendesk", "shopify", "netsuite", "servicenow"}, route
            if driver == "rest_api" and fmt != "rest_api":
                pytest.fail(f"REST brand stub in PRODUCTION_SKU: {route}")
