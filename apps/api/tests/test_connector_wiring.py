"""Smoke tests — every transfer-live driver is wired in registry, adapters, and probes."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from transfer.connector_capabilities import (  # noqa: E402
    _DRIVER_CAPS,
    _FILE_CAPS,
    get_capabilities,
    transfer_live_driver_types,
    transfer_ready,
)
from transfer.connector_registry import CONNECTOR_MODULES, assert_registry_matches_capabilities  # noqa: E402
from transfer.registry import (  # noqa: E402
    LIVE_DEST_DATABASES,
    LIVE_SOURCE_DATABASES,
    LIVE_SOURCE_FORMATS,
    validate_transfer,
)

DB_PROBES = {k: v.probe for k, v in CONNECTOR_MODULES.items()}
DB_READERS = {k: v.reader for k, v in CONNECTOR_MODULES.items() if v.reader}
DB_WRITERS = {k: v.writer for k, v in CONNECTOR_MODULES.items()}
DB_READER_FNS = {k: v.reader_fn for k, v in CONNECTOR_MODULES.items() if v.reader}
DB_WRITER_FNS = {k: v.writer_fn for k, v in CONNECTOR_MODULES.items()}


def test_transfer_live_drivers_have_full_caps():
    assert_registry_matches_capabilities()
    for driver in transfer_live_driver_types():
        caps = get_capabilities(driver)
        assert transfer_ready(caps), f"{driver} should be transfer-ready"
        if driver in _DRIVER_CAPS:
            assert caps.get("read") and caps.get("write") and caps.get("test")


def test_registry_includes_all_db_drivers():
    for driver in _DRIVER_CAPS:
        caps = get_capabilities(driver)
        if not transfer_ready(caps):
            continue
        assert driver in LIVE_SOURCE_DATABASES, driver
        assert driver in LIVE_DEST_DATABASES, driver


def test_file_formats_in_registry():
    for fmt in _FILE_CAPS:
        caps = get_capabilities(fmt)
        if not transfer_ready(caps):
            continue
        assert fmt in LIVE_SOURCE_FORMATS, fmt


@pytest.mark.parametrize("driver", sorted(_DRIVER_CAPS.keys()))
def test_db_probe_modules_importable(driver: str):
    spec = DB_PROBES.get(driver)
    if spec is None:
        import pymongo  # noqa: F401
        return
    mod_name, fn_name = spec
    mod = importlib.import_module(mod_name)
    assert callable(getattr(mod, fn_name))


@pytest.mark.parametrize("driver", sorted(DB_READERS.keys()))
def test_db_reader_modules_importable(driver: str):
    mod = importlib.import_module(DB_READERS[driver])
    fn_name = DB_READER_FNS[driver]
    assert callable(getattr(mod, fn_name))


@pytest.mark.parametrize("driver", sorted(DB_WRITERS.keys()))
def test_db_writer_has_write_mapped_rows(driver: str):
    mod = importlib.import_module(DB_WRITERS[driver])
    fn_name = DB_WRITER_FNS[driver]
    assert callable(getattr(mod, fn_name))


def test_catalog_allowlist_honesty():
    from transfer.connector_capabilities import TRANSFER_READY_CATALOG_IDS, enrich_catalog_entry

    fake_rds = enrich_catalog_entry({
        "id": "amazon_rds_postgresql",
        "name": "Amazon RDS PostgreSQL",
        "status": "planned",
    })
    assert fake_rds["driver_type"] == "postgresql"
    assert fake_rds["transfer_ready"] is False

    real_pg = enrich_catalog_entry({"id": "postgresql", "name": "PostgreSQL", "status": "live"})
    assert real_pg["transfer_ready"] is True
    assert "postgresql" in TRANSFER_READY_CATALOG_IDS


def test_redshift_and_gcs_routes():
    ok, _ = validate_transfer("database", "redshift", "database", "postgresql")
    assert ok
    ok, _ = validate_transfer("file", "parquet", "database", "gcs")
    assert ok
    ok, _ = validate_transfer("database", "gcs", "database", "s3")
    assert ok


def test_excel_and_parquet_in_transfer_live():
    for fmt in ("excel", "parquet"):
        caps = get_capabilities(fmt)
        assert caps.get("file_source") and caps.get("read")
        assert fmt in transfer_live_driver_types()


def test_core_route_matrix_samples():
    ok, _ = validate_transfer("file", "csv", "database", "mongodb")
    assert ok
    ok, _ = validate_transfer("file", "csv", "database", "snowflake")
    assert ok
    ok, _ = validate_transfer("database", "postgresql", "database", "dynamodb")
    assert ok
    ok, _ = validate_transfer("database", "s3", "database", "elasticsearch")
    assert ok
    ok, _ = validate_transfer("file", "json", "file_export", "csv")
    assert ok


def test_catalog_native_transfer_ready_only():
    import json
    from pathlib import Path

    from transfer.connector_capabilities import enrich_catalog_entry, transfer_live_driver_types

    catalog_path = Path(__file__).resolve().parents[1] / "data" / "connector_catalog.json"
    data = json.loads(catalog_path.read_text(encoding="utf-8"))
    enriched = [enrich_catalog_entry(c) for c in data.get("connectors", [])]
    live = [c for c in enriched if c.get("transfer_ready")]
    # Honest count: native drivers only, not alias inflation
    assert len(live) >= len(transfer_live_driver_types())
    assert len(live) < 100, f"alias inflation detected: {len(live)} marked transfer_ready"
