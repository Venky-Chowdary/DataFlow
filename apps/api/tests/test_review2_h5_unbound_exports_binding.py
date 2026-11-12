"""Security review round 2 (H5): an unbound caller never opens the flat exports folder.

``connector_may_open_export_path`` took its binding from the running job or,
failing that, the request's ``X-Workspace-Id`` stamp. A member who simply
omitted the header read as unbound, and unbound opened the flat exports
folder (the legacy files the M3 migration leaves "platform administrators
only"). So did any ``ThreadPoolExecutor`` thread, where contextvars are not
copied.

Now a path in the exports tree opens only for:
- the running job's workspace (``export_job_scope``), its own folder only;
- the saved connector's own workspace (``connector_export_scope``, bound by
  the routes that open a stored connector), its own folder only;
- with nothing bound, the flat folder when authentication is off (the single
  local operator) or the caller is a platform administrator.
A request header alone never binds it, and a lost binding refuses.
"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from tenancy_world import tenancy_world  # noqa: F401

from connectors.sqlite_common import confine_sqlite_path

WS_A = "0b5c3f8e-0000-4000-8000-00000000000a"
WS_B = "0b5c3f8e-0000-4000-8000-00000000000b"


def _db(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("create table secret(v text)")
    conn.execute("insert into secret values (?)", (marker,))
    conn.commit()
    conn.close()


def _opens(path: Path) -> bool:
    try:
        confine_sqlite_path(str(path))
    except ValueError as exc:
        assert "export" in str(exc)
        return False
    return True


@pytest.fixture
def roots(tmp_path, monkeypatch):
    for name in ("SQLITE_ROOT", "DUCKDB_ROOT", "CONFINE_LOCAL_FILES", "EXPORTS_DIR", "DATA_DIR", "REQUIRE_AUTH"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return tmp_path


@pytest.fixture
def multi_tenant(roots, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    return roots


def _flat(root: Path) -> Path:
    return root / "exports" / "legacy_orders.sqlite"


def _area(root: Path, ws: str) -> Path:
    from services.export_paths import _workspace_segment

    return root / "exports" / "_workspaces" / _workspace_segment(ws) / "orders.sqlite"


# --------------------------------------------------------------- function level


def test_multi_tenant_unbound_caller_is_refused_the_flat_folder(multi_tenant):
    assert not _opens(_flat(multi_tenant))


def test_a_request_header_alone_never_binds_an_export_area(multi_tenant):
    """The request stamp (``X-Workspace-Id``) is not a binding for the exports tree."""
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter

    with bound_workspace_filter(WorkspaceFilter.only(WS_B), stamp=WS_B):
        assert not _opens(_area(multi_tenant, WS_B))
        assert not _opens(_flat(multi_tenant))


def test_the_connectors_own_workspace_binds_its_folder_only(multi_tenant):
    from services.export_paths import connector_export_scope

    with connector_export_scope(WS_B):
        assert _opens(_area(multi_tenant, WS_B))
        assert not _opens(_area(multi_tenant, WS_A))
        assert not _opens(_flat(multi_tenant))
    # An unscoped (legacy) connector binds nothing.
    with connector_export_scope(""):
        assert not _opens(_flat(multi_tenant))


def test_a_workspace_job_keeps_its_own_export_area(multi_tenant):
    from services.export_object_store import export_job_scope

    with export_job_scope("job-a", WS_A):
        assert _opens(_area(multi_tenant, WS_A))
        assert not _opens(_area(multi_tenant, WS_B))
        assert not _opens(_flat(multi_tenant))


def test_a_lost_binding_in_a_pool_thread_refuses(multi_tenant):
    """contextvars are not copied into ThreadPoolExecutor threads: losing the binding refuses."""
    from services.export_object_store import export_job_scope
    from services.export_paths import connector_export_scope

    with export_job_scope("job-a", WS_A), connector_export_scope(WS_A):
        with ThreadPoolExecutor(1) as pool:
            assert pool.submit(_opens, _flat(multi_tenant)).result() is False
            assert pool.submit(_opens, _area(multi_tenant, WS_A)).result() is False


def test_platform_admin_unbound_opens_the_flat_folder_only(multi_tenant):
    from services.connector_access import ConnectorCaller, bind_connector_caller

    with bind_connector_caller(ConnectorCaller(actor="root@example.com", platform_admin=True)):
        assert _opens(_flat(multi_tenant))
        assert not _opens(_area(multi_tenant, WS_A))
    with bind_connector_caller(ConnectorCaller(actor="member@example.com")):
        assert not _opens(_flat(multi_tenant))


def test_single_operator_keeps_the_flat_folder(roots):
    """Authentication off: the single local operator's SQLite export opens, as before."""
    assert _opens(_flat(roots))
    assert not _opens(_area(roots, WS_A))
    with ThreadPoolExecutor(1) as pool:
        assert pool.submit(_opens, _flat(roots)).result() is True


# ------------------------------------------------------------------- HTTP level


@pytest.fixture
def world(tenancy_world, tmp_path_factory, monkeypatch):
    """Exports outside the data directory, as in a deployment (the data-dir
    refusal would otherwise answer first and hide the exports rule)."""
    w = tenancy_world
    w.exp_root = tmp_path_factory.mktemp("h5")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(w.exp_root / "exports"))
    _db(_flat(w.exp_root), "LEGACY-FLAT-EXPORT-OF-A")
    _db(_area(w.exp_root, w.workspace_b), "B-OWN-EXPORT")
    return w


def _query(client, connector_id: str):
    return client.post(
        "/api/v1/query/execute", json={"connector_id": connector_id, "query": "select v from secret"}
    )


def test_http_member_without_header_cannot_read_the_flat_folder(world):
    w = world
    saved = w.b.post(
        "/api/v1/connectors/saved",
        json={"name": "legacy", "type": "sqlite", "database": str(_flat(w.exp_root))},
    )
    assert saved.status_code == 200, saved.text
    cid = saved.json()["id"]
    single = w.member("single@tenancy.example.com", w.workspace_b, "editor")
    single.headers.pop("X-Workspace-Id")
    r = _query(single, cid)
    assert "LEGACY-FLAT-EXPORT-OF-A" not in r.text and r.status_code != 200, r.text
    multi = w.member("multi@tenancy.example.com", w.workspace_b, "editor")
    w.add_membership("multi@tenancy.example.com", w.workspace_a, "viewer")
    multi.headers.pop("X-Workspace-Id")
    r = _query(multi, cid)
    assert "LEGACY-FLAT-EXPORT-OF-A" not in r.text, r.text
    r = _query(w.b, cid)
    assert "LEGACY-FLAT-EXPORT-OF-A" not in r.text and r.status_code != 200, r.text


def test_http_connector_on_its_own_workspace_export_opens_with_or_without_header(world):
    """The binding is the connector's workspace, so B's own export is B's to read."""
    w = world
    saved = w.b.post(
        "/api/v1/connectors/saved",
        json={"name": "own", "type": "sqlite", "database": str(_area(w.exp_root, w.workspace_b))},
    )
    assert saved.status_code == 200, saved.text
    cid = saved.json()["id"]
    r = _query(w.b, cid)
    assert r.status_code == 200 and "B-OWN-EXPORT" in r.text, r.text
    single = w.member("single2@tenancy.example.com", w.workspace_b, "editor")
    single.headers.pop("X-Workspace-Id")
    r = _query(single, cid)
    assert r.status_code == 200 and "B-OWN-EXPORT" in r.text, r.text


def test_a_pooled_engine_is_confined_again_on_every_call(multi_tenant, monkeypatch):
    """The engine cache is keyed by the address, not the caller: a cache hit
    must not hand A's export database to a caller bound to B."""
    from connectors.generic_sql import get_sqlalchemy_engine
    from services import engine_pool
    from services.export_object_store import export_job_scope
    from services.export_paths import connector_export_scope

    monkeypatch.setenv("DATAFLOW_ENGINE_CACHE", "1")
    engine_pool.reset_for_tests()
    a_file = _area(multi_tenant, WS_A)
    _db(a_file, "A-WORKSPACE-EXPORT")
    cfg = {"type": "sqlite", "database": str(a_file)}
    try:
        with export_job_scope("job-a", WS_A):
            engine_pool.release_engine(get_sqlalchemy_engine(dict(cfg)))
        with connector_export_scope(WS_B), pytest.raises(ValueError, match="export"):
            get_sqlalchemy_engine(dict(cfg))
        with pytest.raises(ValueError, match="export"):
            get_sqlalchemy_engine(dict(cfg))
    finally:
        engine_pool.reset_for_tests()
