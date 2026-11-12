"""Security review (low): an embedded-database connector never opens the exports tree.

``confine_sqlite_path`` / ``confine_duckdb_path`` always refuse the service's
own state directory. With ``CONFINE_LOCAL_FILES`` off, a SQLite or DuckDB
connector could still name ``<exports>/_workspaces/<other workspace>/x.sqlite``
and read or write another workspace's export. A path into the exports tree
now opens only when it is the caller's own export area -- the rule
``resolve_export_path`` applies to an export name: with a workspace bound,
that workspace's folder; with none, the flat folder but never
``_workspaces``. The platform's own SQLite export (``kind: file, format:
sqlite``) writes there, so it keeps working (``test_r11_not_yet_live_routes_e2e``).
"""

from __future__ import annotations

import pytest

from connectors.sqlite_common import confine_duckdb_path, confine_sqlite_path


@pytest.fixture
def roots(tmp_path, monkeypatch):
    for name in ("SQLITE_ROOT", "DUCKDB_ROOT", "CONFINE_LOCAL_FILES", "EXPORTS_DIR", "DATA_DIR"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return tmp_path


WS_A = "0b5c3f8e-0000-4000-8000-00000000000a"
WS_B = "0b5c3f8e-0000-4000-8000-00000000000b"


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
@pytest.mark.parametrize("relative", [
    f"exports/_workspaces/{WS_B}/orders.sqlite",
    "exports/sub/../_workspaces/x/y.duckdb",
])
def test_unbound_callers_never_open_a_workspace_folder(roots, confine, relative):
    with pytest.raises(ValueError, match="export"):
        confine(str(roots / relative))


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
@pytest.mark.parametrize("relative", [
    f"exports/_workspaces/{WS_B}/orders.sqlite",
    "exports/orders.sqlite",
    f"exports/_workspaces/{WS_A}/../{WS_B}/orders.sqlite",
])
def test_a_workspace_job_never_opens_another_area(roots, confine, relative):
    from services.export_object_store import export_job_scope

    with export_job_scope("job-a", WS_A):
        with pytest.raises(ValueError, match="export"):
            confine(str(roots / relative))


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
def test_a_workspace_job_opens_its_own_export_area(roots, confine):
    from services.export_object_store import export_job_scope

    path = str(roots / "exports" / "_workspaces" / WS_A / "orders.sqlite")
    with export_job_scope("job-a", WS_A):
        assert confine(path) == path


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
def test_an_unbound_caller_opens_the_flat_export_folder(roots, confine):
    """The single local operator's SQLite export lands in the flat folder."""
    path = str(roots / "exports" / "orders.sqlite")
    assert confine(path) == path


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
def test_a_symlink_into_the_exports_tree_is_refused(roots, confine):
    (roots / "exports" / "_workspaces" / "other").mkdir(parents=True)
    (roots / "elsewhere").symlink_to(roots / "exports" / "_workspaces" / "other")
    with pytest.raises(ValueError, match="export"):
        confine(str(roots / "elsewhere" / "orders.sqlite"))


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
def test_a_file_outside_it_still_opens(roots, confine):
    path = str(roots / "warehouse" / "local.db")
    assert confine(path) == path
    assert not (roots / "exports").exists(), "checking a path must not create the exports folder"
