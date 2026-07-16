"""Every live catalog ID must resolve to a runnable driver and a valid transfer route."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from src.services.catalog_service import search_catalog  # noqa: E402
from src.transfer.connector_capabilities import default_port, dest_ready, get_capabilities, resolve_driver_type, source_ready  # noqa: E402
from src.transfer.registry import validate_transfer  # noqa: E402


def _all_live_catalog_ids() -> list[str]:
    return [c["id"] for c in search_catalog(status="live", limit=1000).get("connectors", [])]


@pytest.fixture(scope="module")
def live_catalog_ids() -> list[str]:
    return _all_live_catalog_ids()


def test_all_live_catalog_ids_resolve_to_driver(live_catalog_ids: list[str]):
    """No catalog ID should fall through to an unknown driver."""
    for cid in live_catalog_ids:
        driver = resolve_driver_type(cid)
        assert driver, f"{cid} resolved to an empty driver"
        # Generic SQL engines resolve to generic_sql; aliases resolve to first-class drivers.
        assert driver in (
            "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
            "dynamodb", "s3", "gcs", "adls", "redis", "elasticsearch", "sqlite",
            "sftp", "email",
            "salesforce", "hubspot", "stripe",
            "generic_sql", "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet",
        ), f"{cid} -> {driver} is not a known driver"


def test_all_live_catalog_ids_have_default_port(live_catalog_ids: list[str]):
    for cid in live_catalog_ids:
        driver = resolve_driver_type(cid)
        port = default_port(driver)
        assert port is not None, f"{cid} -> {driver} has no default port"


def test_all_live_db_catalog_ids_have_valid_db_to_db_route(live_catalog_ids: list[str]):
    db_ids = [cid for cid in live_catalog_ids if resolve_driver_type(cid) not in (
        "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"
    )]
    source_ids = [cid for cid in db_ids if get_capabilities(resolve_driver_type(cid)).get("read")]
    dest_ids = [cid for cid in db_ids if dest_ready(get_capabilities(resolve_driver_type(cid)))]
    for src in source_ids:
        for dst in dest_ids:
            ok, msg = validate_transfer("database", src, "database", dst)
            assert ok, f"database/{src} -> database/{dst}: {msg}"


def test_all_live_file_catalog_ids_have_valid_db_route(live_catalog_ids: list[str]):
    file_ids = [cid for cid in live_catalog_ids if resolve_driver_type(cid) in (
        "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"
    )]
    dest_ids = [cid for cid in live_catalog_ids if resolve_driver_type(cid) not in (
        "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"
    ) and dest_ready(get_capabilities(resolve_driver_type(cid)))]
    for fid in file_ids:
        for did in dest_ids:
            ok, msg = validate_transfer("file", fid, "database", did)
            assert ok, f"file/{fid} -> database/{did}: {msg}"


def test_all_live_db_catalog_ids_have_valid_db_to_file_route(live_catalog_ids: list[str]):
    file_ids = [cid for cid in live_catalog_ids if resolve_driver_type(cid) in (
        "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"
    )]
    db_ids = [cid for cid in live_catalog_ids if resolve_driver_type(cid) not in (
        "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"
    )]
    source_ids = [cid for cid in db_ids if get_capabilities(resolve_driver_type(cid)).get("read")]
    for sid in source_ids:
        for fid in file_ids:
            ok, msg = validate_transfer("database", sid, "file_export", fid)
            assert ok, f"database/{sid} -> file_export/{fid}: {msg}"


def test_live_catalog_count_matches_health_manifest():
    live = _all_live_catalog_ids()
    assert len(live) >= 130, f"Expected at least 130 live catalog IDs, got {len(live)}"
