"""Security review (low): an embedded-database connector never opens the exports tree.

``confine_sqlite_path`` / ``confine_duckdb_path`` always refuse the service's
own state directory. With ``CONFINE_LOCAL_FILES`` off, a SQLite or DuckDB
connector could still name ``<exports>/_workspaces/<other workspace>/x.sqlite``
and read or write another workspace's export. The exports tree is now refused
always, like the state directory.
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


@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
@pytest.mark.parametrize("relative", [
    "exports/_workspaces/0b5c3f8e-0000-4000-8000-000000000000/orders.sqlite",
    "exports/orders.sqlite",
    "exports/sub/../_workspaces/x/y.duckdb",
])
def test_the_exports_tree_is_refused(roots, confine, relative):
    with pytest.raises(ValueError, match="export"):
        confine(str(roots / relative))


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
