"""Embedded-database path confinement (SQLite / DuckDB connector paths).

A workspace member controls a connector's ``database`` field; the API must not
open arbitrary host files (service state, other tenants' files) for it.
"""

from __future__ import annotations

import os
import sqlite3

import pytest

from connectors.generic_sql import _build_url
from connectors.sqlite import test_sqlite as probe_sqlite
from connectors.sqlite_common import duckdb_file_path, sqlite_file_path
from services import platform_config


@pytest.fixture
def dev_env(monkeypatch, tmp_path):
    for name in (
        "DATAFLOW_SQLITE_ROOT", "DATAWRAP_SQLITE_ROOT",
        "DATAFLOW_DUCKDB_ROOT", "DATAWRAP_DUCKDB_ROOT",
        "DATAWRAP_ENV", "DATAWRAP_DATA_DIR",
        "RAILWAY_ENVIRONMENT", "RAILWAY_SERVICE_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    state = tmp_path / "service_state"
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(state))
    return tmp_path


def _probe(path: str):
    return probe_sqlite(
        host="", port=0, database=path, username="", password="",
        schema="", connection_string="", ssl=False,
    )


def test_service_data_dir_refused_without_root(dev_env):
    target = platform_config.data_dir() / "proof_ledger.db"
    sqlite3.connect(target).close()
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path(str(target), "", "")
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path("", f"sqlite:///{target}", "")
    result = _probe(str(target))
    assert not result.ok and "service data directory" in (result.error or "")


def test_traversal_and_symlink_into_data_dir_refused(dev_env):
    outside = dev_env / "outside"
    outside.mkdir()
    sneaky = outside / ".." / "service_state" / "x.db"
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path(str(sneaky), "", "")
    if hasattr(os, "symlink"):
        platform_config.data_dir()
        link = outside / "link.db"
        os.symlink(platform_config.data_dir() / "x.db", link)
        with pytest.raises(ValueError, match="service data directory"):
            sqlite_file_path(str(link), "", "")


def test_dev_without_root_keeps_local_files(dev_env):
    db = dev_env / "local.db"
    assert sqlite_file_path(str(db), "", "") == str(db)
    assert sqlite_file_path(":memory:", "", "") == ":memory:"
    assert _probe(str(db)).ok


def test_production_without_root_confines_to_the_volume(dev_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    db = dev_env / "any.db"
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        sqlite_file_path(str(db), "", "")
    result = _probe(str(db))
    assert not result.ok and "DATAFLOW_SQLITE_ROOT" in (result.error or "")
    assert not db.exists(), "probe must not create files when refused"
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        _build_url({"type": "sqlite", "database": str(db)})
    with pytest.raises(ValueError, match="DATAFLOW_DUCKDB_ROOT"):
        duckdb_file_path(str(dev_env / "any.duckdb"), "")
    assert sqlite_file_path(":memory:", "", "") == ":memory:"
    assert duckdb_file_path("md:my_db", "") == "md:my_db"


def test_root_allowlist_enforced(dev_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    root_a = dev_env / "tenant_a"
    root_b = dev_env / "tenant_b"
    root_a.mkdir()
    root_b.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", os.pathsep.join([str(root_a), str(root_b)]))
    assert sqlite_file_path(str(root_b / "ok.db"), "", "") == str((root_b / "ok.db").resolve())
    # A relative name is anchored at the first root (item 54), not the working dir.
    assert sqlite_file_path("relative.db", "", "") == str((root_a / "relative.db").resolve())
    for bad in (dev_env / "outside.db", root_a / ".." / "outside.db"):
        with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
            sqlite_file_path(str(bad), "", "")
    assert not _probe(str(dev_env / "outside.db")).ok
    assert not (dev_env / "outside.db").exists()
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        _build_url({"type": "sqlite", "connection_string": f"sqlite:///{dev_env}/outside.db"})
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        _build_url({"connection_string": f"sqlite:////{dev_env}/outside.db"})
    assert str(_build_url({"type": "sqlite", "database": str(root_a / "ok.db")})).startswith("sqlite:")


def test_data_dir_refused_even_inside_root(dev_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(dev_env))
    with pytest.raises(ValueError, match="service data directory"):
        sqlite_file_path(str(platform_config.data_dir() / "state.db"), "", "")


def test_duckdb_root_and_fallback(dev_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    root = dev_env / "duck"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    assert duckdb_file_path(str(root / "a.duckdb"), "") == str((root / "a.duckdb").resolve())
    with pytest.raises(ValueError, match="DATAFLOW_DUCKDB_ROOT"):
        duckdb_file_path(f"duckdb:///{dev_env}/b.duckdb", "")
    with pytest.raises(ValueError, match="DATAFLOW_DUCKDB_ROOT"):
        _build_url({"type": "duckdb", "database": str(dev_env / "b.duckdb")})
    assert duckdb_file_path(":memory:", "") == ":memory:"


def test_production_default_volume_dirs_are_allowed_but_not_the_rest_of_the_data_dir(dev_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    data = platform_config.data_dir()
    assert sqlite_file_path("tenant.db", "", "") == str((data / "sqlite" / "tenant.db").resolve())
    assert duckdb_file_path("tenant.duckdb", "") == str((data / "duckdb" / "tenant.duckdb").resolve())
    with pytest.raises(ValueError, match="persistent data volume"):
        sqlite_file_path(str(data / "proof_ledger.db"), "", "")
    with pytest.raises(ValueError, match="persistent data volume"):
        duckdb_file_path(str(data / "sqlite" / "x.db"), "")


def test_embedded_raw_connectors_are_confined(dev_env, monkeypatch):
    """Sites that used the raw ``database`` field now go through the confinement."""
    from services.dest_precount import _sqlite_cfg_path

    state_db = platform_config.data_dir() / "jobs.db"
    with pytest.raises(ValueError, match="service data directory"):
        _sqlite_cfg_path({"database": str(state_db)})
    with pytest.raises(ValueError, match="service data directory"):
        _build_url({"type": "sqlite", "database": str(state_db)})


def test_duckdb_ignores_the_sqlite_root_unless_confinement_is_on(dev_env, monkeypatch):
    """Production never confined DuckDB; SQLITE_ROOT must not start doing so by itself."""
    monkeypatch.delenv("DATAFLOW_CONFINE_LOCAL_FILES", raising=False)
    root = dev_env / "sqlite_only"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    outside = str(dev_env / "b.duckdb")
    assert duckdb_file_path(outside, "") == outside
    monkeypatch.setenv("DATAFLOW_DUCKDB_ROOT", str(root))
    with pytest.raises(ValueError, match="DATAFLOW_DUCKDB_ROOT"):
        duckdb_file_path(outside, "")
    monkeypatch.delenv("DATAFLOW_DUCKDB_ROOT")
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    with pytest.raises(ValueError, match="DATAFLOW_DUCKDB_ROOT"):
        duckdb_file_path(outside, "")
