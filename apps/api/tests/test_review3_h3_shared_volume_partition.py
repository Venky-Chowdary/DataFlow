"""Security review round 3 (H3): the shared SQLite/DuckDB volumes are partitioned per workspace.

``<data_dir>/sqlite`` and ``<data_dir>/duckdb`` were one namespace for every
workspace: workspace B's editor saved a SQLite connector on
``<data_dir>/sqlite/a_destination.sqlite`` and ``/query/execute`` returned
workspace A's rows. Each volume now has one area per workspace
(``<volume>/_workspaces/<ws>``) with the exports tree's binding (the job's
workspace, else the stored connector's), a one-time idempotent migration of
flat files with a provable owner, and single-operator mode (auth off)
unchanged.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

import pytest

from tenancy_world import tenancy_world  # noqa: F401


def _db(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("create table secret(v text)")
    conn.execute("insert into secret values (?)", (marker,))
    conn.commit()
    conn.close()


def _volume(kind: str = "sqlite") -> Path:
    from services.platform_config import data_dir

    return data_dir().resolve() / kind


def _area(workspace: str, kind: str = "sqlite") -> Path:
    from services.embedded_volume import volume_workspace_dir

    return volume_workspace_dir(_volume(kind), workspace)


@pytest.fixture
def world(tenancy_world, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-h3-secrets-key-distinct-from-auth-0001")
    return tenancy_world


def _open(path: str, workspace: str, kind: str = "sqlite") -> str:
    from connectors.sqlite_common import duckdb_file_path, sqlite_file_path
    from services.export_paths import connector_export_scope

    with connector_export_scope(workspace):
        if kind == "sqlite":
            return sqlite_file_path(path, "", "")
        return duckdb_file_path(path, "")


def _query(client, path: str):
    made = client.post("/api/v1/connectors/saved", json={"name": f"v{time.time_ns()}", "type": "sqlite", "database": path})
    if made.status_code not in (200, 201):
        return made
    body = made.json()
    cid = (body.get("connector") or body).get("id")
    return client.post("/api/v1/query/execute", json={"connector_id": cid, "query": "select v from secret", "language": "sql"})


def test_b_cannot_read_a_flat_volume_file_over_http(world):
    """The reviewer's probe: A's destination in the flat volume, B's editor names it."""
    path = _volume() / "a_destination.sqlite"
    _db(path, "A-SQLITE-DESTINATION-ROWS")
    r = _query(world.b, str(path))
    assert "A-SQLITE-DESTINATION-ROWS" not in r.text
    assert r.status_code != 200 or r.json().get("success") is False or not r.json().get("rows")


def test_b_cannot_read_a_file_in_as_area_over_http_and_a_can(world):
    path = _area(world.workspace_a) / "dest.sqlite"
    _db(path, "A-AREA-ROWS")
    leaked = _query(world.b, str(path))
    assert "A-AREA-ROWS" not in leaked.text
    own = _query(world.a, str(path))
    assert own.status_code == 200, own.text
    assert "A-AREA-ROWS" in own.text


@pytest.mark.parametrize("kind", ["sqlite", "duckdb"])
def test_another_workspaces_area_is_refused_and_the_own_area_opens(world, kind):
    a_file = _area(world.workspace_a, kind) / f"a.{kind}"
    a_file.parent.mkdir(parents=True, exist_ok=True)
    a_file.touch()
    with pytest.raises(ValueError, match="(?i)another workspace"):
        _open(str(a_file), world.workspace_b, kind)
    assert _open(str(a_file), world.workspace_a, kind) == str(a_file)


@pytest.mark.parametrize("kind", ["sqlite", "duckdb"])
def test_a_new_file_named_in_the_flat_volume_is_created_in_the_own_area(world, kind):
    flat = _volume(kind) / f"new.{kind}"
    opened = _open(str(flat), world.workspace_b, kind)
    assert opened == str(_area(world.workspace_b, kind) / f"new.{kind}")
    assert Path(opened).parent.is_dir()


def test_a_relative_name_under_confinement_lands_in_the_own_area(world, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "1")
    monkeypatch.setattr("services.platform_config.is_production", lambda: True)
    for name in ("SQLITE_ROOT", "DUCKDB_ROOT"):
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
    from connectors.sqlite_common import sqlite_root

    root, explicit = sqlite_root()
    assert not explicit and Path(root).resolve() == _volume()
    assert _open("rel.sqlite", world.workspace_b) == str(_area(world.workspace_b) / "rel.sqlite")


def test_with_no_workspace_in_play_the_flat_part_opens_but_no_area(world):
    """System code, an admin's all-workspace view, a deployment without workspaces."""
    flat = _volume() / "legacy.sqlite"
    _db(flat, "LEGACY")
    a_file = _area(world.workspace_a) / "a.sqlite"
    _db(a_file, "A")
    from connectors.sqlite_common import sqlite_file_path

    assert sqlite_file_path(str(flat), "", "") == str(flat)
    with pytest.raises(ValueError, match="(?i)workspace area"):
        sqlite_file_path(str(a_file), "", "")


def test_an_unowned_flat_file_is_out_of_every_workspaces_reach(world):
    flat = _volume() / "legacy.sqlite"
    _db(flat, "LEGACY")
    for workspace in (world.workspace_a, world.workspace_b):
        with pytest.raises(ValueError, match="(?i)no provable owner"):
            _open(str(flat), workspace)


def test_the_requests_verified_workspace_binds_when_no_job_or_connector_does(world):
    from connectors.sqlite_common import sqlite_file_path
    from services.workspace_filter import bound_workspace_filter

    a_file = _area(world.workspace_a) / "a.sqlite"
    _db(a_file, "A")
    with bound_workspace_filter(None, world.workspace_b):
        with pytest.raises(ValueError, match="(?i)another workspace"):
            sqlite_file_path(str(a_file), "", "")
    with bound_workspace_filter(None, world.workspace_a):
        assert sqlite_file_path(str(a_file), "", "") == str(a_file)


def test_single_operator_mode_keeps_todays_paths(world, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    flat = _volume() / "local.sqlite"
    _db(flat, "LOCAL")
    a_file = _area(world.workspace_a) / "a.sqlite"
    _db(a_file, "A")
    from connectors.sqlite_common import sqlite_file_path

    assert sqlite_file_path(str(flat), "", "") == str(flat)
    assert _open(str(flat), world.workspace_b) == str(flat)
    assert _open(str(a_file), world.workspace_b) == str(a_file)


def _connector(workspace: str, path: Path, kind: str = "sqlite"):
    from services import connector_store

    return connector_store.create_connector({"name": f"c{time.time_ns()}", "type": kind,
                                            "database": str(path), "workspace_id": workspace})


def _backdate(connector_id: str) -> None:
    """A connector saved before the volume was partitioned."""
    from services import connector_store

    connector_store.update_connector(connector_id, {"created_at": "2020-01-01T00:00:00+00:00"})


def test_migration_moves_owned_files_home_and_is_idempotent(world):
    from services.embedded_volume import mark_partitioned, migrate_flat_volume_files

    owned = _volume() / "owned.sqlite"
    _db(owned, "OWNED")
    Path(f"{owned}-wal").write_bytes(b"")
    shared = _volume() / "shared.sqlite"
    _db(shared, "SHARED")
    orphan = _volume() / "orphan.sqlite"
    _db(orphan, "ORPHAN")
    _backdate(_connector(world.workspace_a, owned).id)
    _backdate(_connector(world.workspace_a, shared).id)
    _backdate(_connector(world.workspace_b, shared).id)
    late = _volume() / "late.sqlite"
    _db(late, "LATE")
    mark_partitioned()
    _connector(world.workspace_b, late)  # saved after the partition: proves nothing

    first = migrate_flat_volume_files()
    assert first["moved"] == 1
    home = _area(world.workspace_a) / "owned.sqlite"
    assert home.is_file() and not owned.exists()
    assert Path(f"{home}-wal").exists() and not Path(f"{owned}-wal").exists()
    assert shared.exists() and orphan.exists() and late.exists()
    assert migrate_flat_volume_files()["moved"] == 0
    # A's stored connector still names the flat path; it opens the moved file.
    assert _open(str(owned), world.workspace_a) == str(home)
    # B naming the old flat path gets a new, empty file in B's own area.
    assert _open(str(owned), world.workspace_b) == str(_area(world.workspace_b) / "owned.sqlite")
    with pytest.raises(ValueError, match="(?i)no provable owner"):
        _open(str(late), world.workspace_b)


def test_an_owner_opening_a_not_yet_migrated_file_moves_it_home(world):
    from services.embedded_volume import mark_partitioned

    owned = _volume() / "pending.sqlite"
    _db(owned, "PENDING")
    _backdate(_connector(world.workspace_a, owned).id)
    mark_partitioned()
    with pytest.raises(ValueError):
        _open(str(owned), world.workspace_b)
    home = _area(world.workspace_a) / "pending.sqlite"
    assert _open(str(owned), world.workspace_a) == str(home)
    assert home.is_file() and not owned.exists()


def test_no_partition_marker_means_no_connector_proves_ownership(world):
    owned = _volume() / "early.sqlite"
    _db(owned, "EARLY")
    _backdate(_connector(world.workspace_a, owned).id)
    with pytest.raises(ValueError, match="(?i)no provable owner"):
        _open(str(owned), world.workspace_a)
    assert owned.exists()


def test_the_startup_pass_records_the_partition_before_requests(world):
    from src import main

    source = Path(main.__file__).read_text(encoding="utf-8")
    assert "mark_partitioned()" in source
    assert os.path.exists(main.__file__)
