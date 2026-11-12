"""Security review round 2 (C1): a SQLite ``file:`` URI never escapes path confinement.

``_confine_embedded_file_url`` judged ``make_url(url).database``. For
``sqlite:///file:/abs/x.sqlite?mode=ro&uri=true`` that value is
``file:/abs/x.sqlite``, which the confinement resolved as a *relative* name
under the working directory, so neither the exports check nor the
data-directory check fired, while SQLAlchemy (``uri=true``) opened the real
absolute path. A workspace-B editor read workspace A's export over
``/query/execute``.

Now a SQLite or DuckDB database value that starts with ``file:`` (any case)
is refused, and so is an embedded-database URL whose query carries a
URI-filename parameter (``uri``, ``mode``, ``vfs``, ``immutable``,
``nolock``), at every place a path or URL reaches a driver.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from tenancy_world import tenancy_world  # noqa: F401

from connectors.sqlite_common import (
    confine_duckdb_path,
    confine_sqlite_path,
    duckdb_file_path,
    sqlite_file_path,
    sqlite_unconfined_path,
)


def _db(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("create table secret(v text)")
    conn.execute("insert into secret values (?)", (marker,))
    conn.commit()
    conn.close()


@pytest.fixture
def roots(tmp_path, monkeypatch):
    for name in ("SQLITE_ROOT", "DUCKDB_ROOT", "CONFINE_LOCAL_FILES", "EXPORTS_DIR", "DATA_DIR"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return tmp_path


def test_http_b_editor_cannot_read_a_workspace_export_through_a_file_uri(tenancy_world, monkeypatch):
    """The review's reproduction: B's editor, header B, ``/query/execute`` on a ``file:`` URI."""
    w = tenancy_world
    from services.export_paths import _workspace_segment

    a_file = w.data_dir / "exports" / "_workspaces" / _workspace_segment(w.workspace_a) / "a.sqlite"
    _db(a_file, "A-WORKSPACE-EXPORT")
    cs = f"sqlite:///file:{a_file}?mode=ro&uri=true"
    saved = w.b.post(
        "/api/v1/connectors/saved", json={"name": "uri", "type": "sqlite", "connection_string": cs}
    )
    if saved.status_code == 200:
        r = w.b.post(
            "/api/v1/query/execute",
            json={"connector_id": saved.json()["id"], "query": "select v from secret"},
        )
        assert r.status_code != 200, r.text
        assert "A-WORKSPACE-EXPORT" not in r.text
    else:
        assert saved.status_code in (400, 422), saved.text
    # The same file as a ``database`` value (the form field) is refused too.
    saved = w.b.post(
        "/api/v1/connectors/saved",
        json={"name": "uri2", "type": "sqlite", "database": f"FILE:{a_file}?mode=ro"},
    )
    if saved.status_code == 200:
        r = w.b.post(
            "/api/v1/query/execute",
            json={"connector_id": saved.json()["id"], "query": "select v from secret"},
        )
        assert r.status_code != 200, r.text
        assert "A-WORKSPACE-EXPORT" not in r.text


@pytest.mark.parametrize("prefix", ["file:", "FILE:", "File:"])
@pytest.mark.parametrize("confine", [confine_sqlite_path, confine_duckdb_path])
def test_a_file_uri_database_value_is_refused(roots, confine, prefix):
    target = roots / "exports" / "_workspaces" / "other" / "a.sqlite"
    with pytest.raises(ValueError, match="URI"):
        confine(f"{prefix}{target}?mode=ro")


@pytest.mark.parametrize("prefix", ["file:", "FILE:"])
def test_sqlite_file_path_refuses_a_file_uri(roots, prefix):
    with pytest.raises(ValueError, match="URI"):
        sqlite_file_path(f"{prefix}{roots}/state/jobs.sqlite?mode=rw", "", "")
    with pytest.raises(ValueError, match="URI"):
        sqlite_file_path("", f"sqlite:///{prefix}{roots}/state/jobs.sqlite", "")


@pytest.mark.parametrize("param", ["uri=true", "mode=rw", "vfs=unix-none", "immutable=1", "nolock=1", "URI=True"])
def test_build_url_refuses_uri_filename_query_parameters(roots, param):
    from connectors import generic_sql

    plain = roots / "plain.sqlite"
    for cs in (f"sqlite:///{plain}?{param}", f"sqlite:////{plain}?timeout=5&{param}"):
        with pytest.raises(ValueError, match="URI"):
            generic_sql._build_url({"type": "sqlite", "connection_string": cs})
    with pytest.raises(ValueError, match="URI"):
        generic_sql._build_url({"type": "duckdb", "connection_string": f"duckdb:///{plain}?{param}"})
    with pytest.raises(ValueError, match="URI"):
        sqlite_unconfined_path("", f"sqlite:///{plain}?{param}")


@pytest.mark.parametrize(
    "cs",
    [
        "sqlite:///file:{state}/probe_state.sqlite?mode=rw&uri=true",
        "sqlite:///file:{state}/probe_state.sqlite",
        "sqlite:///FILE:{exports}/legacy.sqlite?uri=true",
    ],
)
def test_build_url_refuses_a_file_uri_into_the_data_dir_or_exports(roots, cs):
    from connectors import generic_sql

    text = cs.format(state=roots / "state", exports=roots / "exports")
    with pytest.raises(ValueError, match="URI"):
        generic_sql._build_url({"type": "sqlite", "connection_string": text})


def test_duckdb_file_uri_is_refused(roots):
    with pytest.raises(ValueError, match="URI"):
        duckdb_file_path(f"file:{roots}/state/x.duckdb", "")
    from connectors import generic_sql

    with pytest.raises(ValueError, match="URI"):
        generic_sql._build_url({"type": "duckdb", "connection_string": f"duckdb:///file:{roots}/x.duckdb"})


@pytest.mark.parametrize(
    "value",
    [
        "{root}/plain.sqlite",
        "sqlite:////{root_rel}/plain.sqlite",
        "sqlite:///{root}/plain.sqlite",
        "sqlite:///{root}/plain.sqlite?timeout=5",
        "sqlite:///:memory:",
        ":memory:",
    ],
)
def test_ordinary_sqlite_values_still_open(roots, value):
    """Control: a plain path or a SQLAlchemy URL without URI parameters still builds."""
    from connectors import generic_sql

    text = value.format(root=roots, root_rel=str(roots).lstrip("/"))
    if "://" in text:
        assert generic_sql._build_url({"type": "sqlite", "connection_string": text})
    else:
        assert confine_sqlite_path(text) == text
    assert confine_duckdb_path(f"{roots}/plain.duckdb") == f"{roots}/plain.duckdb"


def test_readonly_uri_escapes_query_and_fragment_characters(roots):
    """A file name that contains ``?`` or ``#`` can never append URI parameters."""
    from connectors.sqlite_common import sqlite_readonly_uri

    uri = sqlite_readonly_uri(f"{roots}/a.sqlite?vfs=x#/../b.sqlite")
    assert uri.startswith("file:")
    assert uri.endswith("?mode=ro")
    assert uri.count("?") == 1 and "#" not in uri
    real = roots / "real.sqlite"
    _db(real, "ok")
    conn = sqlite3.connect(sqlite_readonly_uri(str(real)), uri=True)
    try:
        assert conn.execute("select v from secret").fetchone() == ("ok",)
    finally:
        conn.close()
