"""Query playground must not read host files or URLs through DuckDB.

DuckDB file binds are exempt from the egress guard, and DuckDB reads files
and URLs from a plain SELECT (reader table functions, replacement scans).
"""

from __future__ import annotations

from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("duckdb_sqlalchemy")

from fastapi.testclient import TestClient

from services import connector_store
from src.routers import query_router
from src.routers.query_router import _duckdb_reads_external

_SECRET = "S3NT1NEL-host-file-content"


@pytest.fixture
def client():
    from src.main import app

    return TestClient(app)


@pytest.fixture
def duck(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    connector_store._backend_choice = None
    db_path = tmp_path / "play.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
    conn.close()
    host_file = tmp_path / "secret.csv"
    host_file.write_text(f"k\n{_SECRET}\n")
    saved = connector_store.create_connector({
        "name": "Play DuckDB",
        "type": "duckdb",
        "role": "both",
        "database": str(db_path),
        "workspace_id": "",
    })
    return {"id": saved.id, "db": db_path, "file": host_file}


def _execute(client, connector_id: str, query: str):
    return client.post("/api/v1/query/execute", json={
        "connector_id": connector_id, "query": query, "limit": 100,
    })


def test_duckdb_table_query_still_runs(client, duck):
    response = _execute(client, duck["id"], "SELECT * FROM users ORDER BY id")
    assert response.status_code == 200, response.text
    assert [r["name"] for r in response.json()["rows"]] == ["alice", "bob"]


@pytest.mark.parametrize("template", [
    "SELECT * FROM read_text('{f}')",
    "SELECT * FROM read_csv('{f}')",
    "SELECT * FROM read_csv_auto('{f}')",
    "SELECT * FROM read_blob('{f}')",
    "SELECT * FROM glob('{d}/*')",
    "SELECT * FROM '{f}'",
    'SELECT * FROM "{f}"',
    "SELECT * FROM users, (FROM '{f}')",
    "SELECT * FROM users JOIN /* c */ '{f}' ON true",
    "SELECT * FROM \"read_csv\"('{f}')",
    "SELECT * FROM main.read_csv /* c */ ('{f}')",
    "SELECT * FROM query('SELECT * FROM read_csv(''{f}'')')",
    "SELECT * FROM sqlite_scan('{f}', 't')",
    "SELECT * FROM read_csv('http://169.254.169.254/latest/meta-data/')",
])
def test_duckdb_external_readers_are_refused(client, duck, template):
    query = template.format(f=duck["file"], d=duck["file"].parent)
    response = _execute(client, duck["id"], query)
    assert response.status_code == 400, response.text
    assert _SECRET not in response.text


def test_connection_disables_external_access_when_sql_check_is_bypassed(
    client, duck, monkeypatch
):
    monkeypatch.setattr(query_router, "_duckdb_reads_external", lambda _sql: False)
    for query in (
        f"SELECT * FROM read_text('{duck['file']}')",
        f"SELECT * FROM '{duck['file']}'",
        f"SELECT * FROM glob('{duck['file'].parent}/*')",
    ):
        response = _execute(client, duck["id"], query)
        assert response.status_code == 400, response.text
        assert _SECRET not in response.text
    # Table reads still work on the guarded connection.
    assert _execute(client, duck["id"], "SELECT count(*) AS n FROM users").json()["rows"] == [{"n": 2}]


def test_guarded_connection_does_not_leak_to_other_connections(client, duck):
    assert _execute(client, duck["id"], "SELECT * FROM users").status_code == 200
    conn = duckdb.connect(str(duck["db"]))
    try:
        assert conn.execute(
            "SELECT current_setting('enable_external_access')"
        ).fetchone()[0] is True
    finally:
        conn.close()


@pytest.mark.parametrize("query", [
    "SELECT * FROM users",
    'SELECT * FROM "users"',
    'SELECT * FROM "My Table"',
    "SELECT name || '/' || id FROM users",
    "SELECT concat(name, '/', id) FROM users WHERE name <> '/etc/passwd'",
    "SELECT * FROM users WHERE name = 'read_csv(x)'",
    "SELECT thread_count FROM users",
])
def test_duckdb_reader_check_allows_ordinary_reads(query):
    assert _duckdb_reads_external(query) is False
