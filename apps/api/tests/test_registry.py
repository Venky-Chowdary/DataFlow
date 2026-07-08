"""Transfer registry includes MySQL and BigQuery."""

import importlib.util
from pathlib import Path


def _load_registry():
    path = Path(__file__).resolve().parents[1] / "src" / "transfer" / "registry.py"
    spec = importlib.util.spec_from_file_location("transfer_registry", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


_registry = _load_registry()
LIVE_DEST_DATABASES = _registry.LIVE_DEST_DATABASES
validate_transfer = _registry.validate_transfer


def test_mysql_file_upload_live():
    ok, _ = validate_transfer("file", "csv", "database", "mysql")
    assert ok


def test_bigquery_file_upload_live():
    ok, _ = validate_transfer("file", "json", "database", "bigquery")
    assert ok


def test_live_dest_includes_new_connectors():
    assert "mysql" in LIVE_DEST_DATABASES
    assert "bigquery" in LIVE_DEST_DATABASES
