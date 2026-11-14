"""Security follow-up 1: SQLite/DuckDB file confinement is on by default in production.

``DATAFLOW_CONFINE_LOCAL_FILES`` used to default off everywhere, so a tenant's
SQLite or DuckDB connector in production could open any file the API process
can read (``/etc/hosts``, another product's database, a path outside every
workspace). In production the confinement is now the default: a connector opens
only files in the platform's data area (with PR 403's per-workspace partition),
its upload area (an upload owned by the caller's workspace) and its export area
(the caller's own export folder), plus any directory the operator lists in
``DATAFLOW_SOURCE_PATH_ROOTS``. Since SEC-9 SQLite and DuckDB are confined
in development and tests too; Iceberg's local root stays production-only.
An explicit ``DATAFLOW_CONFINE_LOCAL_FILES=0`` is honoured, warned about at
startup and recorded in the security-settings snapshot.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from pathlib import Path

import pytest

from connectors.sqlite_common import duckdb_file_path, sqlite_file_path

WS_A = str(uuid.uuid4())
WS_B = str(uuid.uuid4())

_CLEARED = (
    "SQLITE_ROOT", "DUCKDB_ROOT", "ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "SOURCE_PATH_ROOTS",
    "ENV", "UPLOAD_DIR", "VOLUME_PATH",
)


@pytest.fixture
def prod(tmp_path, monkeypatch):
    for name in _CLEARED:
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
    for name in ("RAILWAY_ENVIRONMENT", "RAILWAY_SERVICE_ID", "RAILWAY_VOLUME_MOUNT_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    (tmp_path / "uploads").mkdir()
    return tmp_path


@pytest.fixture
def multi_tenant(prod, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    return prod


def _sqlite_db(path: Path, marker: str = "row") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("create table t(v text)")
    conn.execute("insert into t values (?)", (marker,))
    conn.commit()
    conn.close()
    return path


def _area(kind: str, workspace: str) -> Path:
    from services.embedded_volume import volume_workspace_dir
    from services.platform_config import data_dir

    return volume_workspace_dir(data_dir().resolve() / kind, workspace)


def _open_as(workspace: str, path: str, kind: str = "sqlite") -> str:
    from services.export_paths import connector_export_scope

    with connector_export_scope(workspace):
        return sqlite_file_path(path, "", "") if kind == "sqlite" else duckdb_file_path(path, "")


def _register_upload(monkeypatch, prod: Path, workspace: str, name: str) -> Path:
    from services import file_parser

    file_id = uuid.uuid4().hex[:16]
    path = _sqlite_db(prod / "uploads" / f"{file_id}_{name}", f"upload-of-{workspace}")
    monkeypatch.setitem(
        file_parser._file_registry,
        file_id,
        {"file_id": file_id, "filename": name, "path": str(path), "workspace_id": workspace},
    )
    return path


def _assert_actionable_without_host_paths(message: str, prod: Path) -> None:
    assert "data folder" in message
    assert "DATAFLOW_SOURCE_PATH_ROOTS" in message
    assert "DATAFLOW_CONFINE_LOCAL_FILES=0" in message
    assert str(prod) not in message, "a host path outside the request leaked into the message"
    assert "/etc" not in message


# --- production default ------------------------------------------------------


@pytest.mark.parametrize("kind", ["sqlite", "duckdb"])
def test_production_default_refuses_host_files_with_an_actionable_message(prod, kind):
    outside = prod / "elsewhere" / f"x.{kind}"
    for path in ("/etc/hosts", str(outside), f"/{'/'.join(['..'] * 8)}/etc/passwd"):
        with pytest.raises(ValueError) as caught:
            sqlite_file_path(path, "", "") if kind == "sqlite" else duckdb_file_path(path, "")
        _assert_actionable_without_host_paths(str(caught.value), prod)
    assert not outside.exists()


def test_production_default_refuses_through_the_connector_probe_and_url_builder(prod):
    from connectors.generic_sql import _build_url
    from connectors.sqlite import test_sqlite as probe_sqlite

    result = probe_sqlite(
        host="", port=0, database="/etc/hosts", username="", password="",
        schema="", connection_string="", ssl=False,
    )
    assert not result.ok
    assert "DATAFLOW_SOURCE_PATH_ROOTS" in (result.error or "")
    with pytest.raises(ValueError, match="DATAFLOW_CONFINE_LOCAL_FILES=0"):
        _build_url({"type": "sqlite", "connection_string": "sqlite:////etc/hosts"})
    with pytest.raises(ValueError, match="DATAFLOW_CONFINE_LOCAL_FILES=0"):
        _build_url({"type": "duckdb", "database": "/etc/hosts"})


def test_production_default_anchors_relative_names_in_the_data_area(prod):
    from services.platform_config import data_dir

    assert Path(sqlite_file_path("rel.db", "", "")) == data_dir().resolve() / "sqlite" / "rel.db"
    assert Path(duckdb_file_path("rel.duckdb", "")) == data_dir().resolve() / "duckdb" / "rel.duckdb"


def test_production_default_opens_upload_and_operator_declared_areas(prod, monkeypatch):
    """Single operator (auth off): uploads, the own export area and declared roots open."""
    upload = _register_upload(monkeypatch, prod, "", "mine.sqlite")
    assert Path(sqlite_file_path(str(upload), "", "")) == upload.resolve()
    declared = _sqlite_db(prod / "declared" / "shared.db")
    with pytest.raises(ValueError):
        sqlite_file_path(str(declared), "", "")
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(prod / "declared"))
    assert Path(sqlite_file_path(str(declared), "", "")) == declared.resolve()


def test_production_default_refuses_another_workspaces_file(multi_tenant, monkeypatch):
    prod = multi_tenant
    for kind in ("sqlite", "duckdb"):
        a_file = _area(kind, WS_A) / f"a.{kind}"
        a_file.parent.mkdir(parents=True, exist_ok=True)
        a_file.touch()
        with pytest.raises(ValueError, match="(?i)another workspace"):
            _open_as(WS_B, str(a_file), kind)
    a_upload = _register_upload(monkeypatch, prod, WS_A, "a_rows.sqlite")
    with pytest.raises(ValueError) as caught:
        _open_as(WS_B, str(a_upload))
    assert "upload" in str(caught.value).lower()
    assert str(prod) not in str(caught.value)
    unregistered = _sqlite_db(prod / "uploads" / "hydrate_0123456789ab_x.sqlite")
    with pytest.raises(ValueError):
        _open_as(WS_B, str(unregistered))
    from services.export_paths import workspace_exports_dir

    a_export = _sqlite_db(workspace_exports_dir(WS_A) / "a_export.sqlite")
    with pytest.raises(ValueError):
        _open_as(WS_B, str(a_export))


def test_production_default_loads_a_file_in_the_workspaces_own_area(multi_tenant, monkeypatch):
    prod = multi_tenant
    own = _sqlite_db(_area("sqlite", WS_A) / "mine.sqlite", "A-OWN-ROWS")
    opened = _open_as(WS_A, str(own))
    assert Path(opened) == own
    conn = sqlite3.connect(opened)
    try:
        assert conn.execute("select v from t").fetchall() == [("A-OWN-ROWS",)]
    finally:
        conn.close()
    upload = _register_upload(monkeypatch, prod, WS_A, "own_upload.sqlite")
    assert Path(_open_as(WS_A, str(upload))) == upload.resolve()
    assert Path(_open_as(WS_A, "new.db")) == _area("sqlite", WS_A) / "new.db"


def test_explicit_opt_in_still_confines(prod, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    with pytest.raises(ValueError, match="DATAFLOW_SOURCE_PATH_ROOTS"):
        sqlite_file_path("/etc/hosts", "", "")


def test_production_default_confines_local_iceberg_warehouses(prod):
    from connectors.iceberg_catalog import iceberg_local_root
    from services.platform_config import data_dir

    assert iceberg_local_root() == (data_dir() / "iceberg").resolve()


# --- development: SQLite/DuckDB confined too (SEC-9) -----------------------


@pytest.mark.parametrize("env", ["development", "test"])
def test_development_default_confines_sqlite_and_duckdb_too(prod, monkeypatch, env):
    monkeypatch.setenv("DATAFLOW_ENV", env)
    outside = str(prod / "elsewhere" / "x.db")
    with pytest.raises(ValueError):
        sqlite_file_path(outside, "", "")
    with pytest.raises(ValueError):
        sqlite_file_path("/etc/hosts", "", "")
    with pytest.raises(ValueError):
        duckdb_file_path(outside.replace(".db", ".duckdb"), "")
    from connectors.iceberg_catalog import iceberg_local_root
    from services.platform_config import data_dir

    assert sqlite_file_path("local.db", "", "").startswith(str(data_dir().resolve() / "sqlite"))
    # Iceberg's local warehouse root stays a production default.
    assert iceberg_local_root() is None


# --- operator opt-out ---------------------------------------------------------


@pytest.mark.parametrize("value", ["0", "false", "off", "OFF", "no"])
def test_opt_out_is_honoured_audited_and_warned(prod, monkeypatch, caplog, value):
    import services.audit_log as audit_log
    from services import would_apply_audit
    from services.local_file_policy import local_file_settings, warn_if_local_file_confinement_off

    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", value)
    would_apply_audit.reset_dedupe()
    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw))
    outside = str(prod / "elsewhere" / "x.db")
    assert sqlite_file_path(outside, "", "") == outside
    assert [e["action"] for e in events] == ["local_file.would_confine"]

    logger = logging.getLogger("datawrap.security.test")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_local_file_confinement_off(logger)
    warned = [r.getMessage() for r in caplog.records if r.name == logger.name]
    assert len(warned) == 1 and "DATAFLOW_CONFINE_LOCAL_FILES" in warned[0]
    assert local_file_settings() == {"local_file_confinement": "opted_out"}


def test_no_startup_warning_and_snapshot_values_without_opt_out(prod, monkeypatch, caplog):
    from services.local_file_policy import local_file_settings, warn_if_local_file_confinement_off

    logger = logging.getLogger("datawrap.security.test")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_local_file_confinement_off(logger)
        monkeypatch.setenv("DATAFLOW_ENV", "development")
        warn_if_local_file_confinement_off(logger)
    assert not [r for r in caplog.records if r.name == logger.name]
    assert local_file_settings() == {"local_file_confinement": "enforced"}
    # SEC-9: an opt-out is an opt-out in every environment, and is said so.
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "0")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_local_file_confinement_off(logger)
    assert [r for r in caplog.records if r.name == logger.name]
    assert local_file_settings() == {"local_file_confinement": "opted_out"}

