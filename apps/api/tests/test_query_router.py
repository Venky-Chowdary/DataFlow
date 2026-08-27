"""Query playground endpoint tests."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from src.routers.query_router import _is_safe_sql, _validate_mongodb_aggregate

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from services import connector_store


def _sqlite_db(tmp_path: Path):
    db_path = tmp_path / "playground.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
    conn.commit()
    conn.close()
    return db_path


def _isolated_store(monkeypatch, tmp_path: Path):
    """Force connector_store to use a fresh file store under tmp_path."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    connector_store._backend_choice = None


@pytest.fixture
def test_client():
    from src.main import app
    return TestClient(app)


def test_query_sqlite_select(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users ORDER BY id",
        "limit": 100,
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["row_count"] == 2
    assert data["columns"] == ["id", "name"]
    assert data["rows"][0]["name"] == "alice"


def test_query_blocks_non_select(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite 2",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "DROP TABLE users",
    })
    assert response.status_code == 400


def test_query_export_csv(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = connector_store.create_connector({
        "name": "Test SQLite 3",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })

    response = test_client.post("/api/v1/query/export", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users",
        "format": "csv",
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True
    assert data["row_count"] == 2
    assert data["download_url"].startswith("/api/v1/transfer/download/")


def test_query_export_refuses_overwrite_to_connector(test_client, tmp_path, monkeypatch):
    """Playground export must not replace a table without Transfer Studio gates."""
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    src = connector_store.create_connector({
        "name": "Query src",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })
    dest = connector_store.create_connector({
        "name": "Query dest",
        "type": "sqlite",
        "role": "destination",
        "connection_string": f"sqlite:///{tmp_path / 'dest.db'}",
        "workspace_id": "",
    })
    response = test_client.post("/api/v1/query/export", json={
        "connector_id": src.id,
        "query": "SELECT * FROM users",
        "destination_connector_id": dest.id,
        "destination": "users_copy",
        "sync_mode": "overwrite",
    })
    assert response.status_code == 400, response.text
    assert "cannot overwrite" in response.json()["detail"].lower()


def test_safe_sql_guard():
    assert _is_safe_sql("SELECT * FROM users") is True
    assert _is_safe_sql("SELECT * FROM users;") is True
    assert _is_safe_sql("WITH d AS (SELECT * FROM users) SELECT * FROM d") is True
    assert _is_safe_sql("EXPLAIN SELECT * FROM users") is True
    assert _is_safe_sql("SHOW TABLES") is True
    assert _is_safe_sql("PRAGMA table_info(users)") is True
    assert _is_safe_sql("DROP TABLE users") is False
    assert _is_safe_sql("WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d") is False
    assert _is_safe_sql("SELECT * INTO OUTFILE '/tmp/x' FROM users") is False
    assert _is_safe_sql("SELECT * FROM users; DROP TABLE users") is False
    assert _is_safe_sql("INSERT INTO users VALUES (1)") is False


def _typed_sqlite_db(tmp_path: Path) -> Path:
    """A table whose columns span the type classes a console must distinguish."""
    db_path = tmp_path / "typed.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE readings ("
        " id INTEGER PRIMARY KEY,"
        " big_val BIGINT,"
        " ratio DOUBLE,"
        " label TEXT,"
        " seen_at TIMESTAMP,"
        " ok BOOLEAN)"
    )
    conn.execute(
        "INSERT INTO readings VALUES"
        " (1, 9223372036854775807, 1.25, 'alpha', '2024-03-01 10:00:00', 1),"
        " (2, 5000000000, 0.5, 'beta', '2024-03-02 11:30:00', 0)"
    )
    conn.commit()
    conn.close()
    return db_path


def _sqlite_connector(name: str, db_path: Path):
    return connector_store.create_connector({
        "name": name,
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })


_NUMERIC_FAMILY = ("INT", "NUM", "DECIMAL", "FLOAT", "DOUBLE", "REAL", "BIGINT")


def _is_numeric_type(logical: str) -> bool:
    """Numeric family check for width-safe assertions.

    A wider carrier than the source (BIGINT reported as DECIMAL(20,0)) is
    acceptable — over-wide costs storage, under-wide loses data. What must
    never happen is an integer column coming back as text.
    """
    return any(token in logical.upper() for token in _NUMERIC_FAMILY)


def test_execute_reports_inferred_column_types_not_string(
    test_client, tmp_path, monkeypatch
):
    """Regression: every column used to be reported as ``string``.

    A type-fidelity product cannot label a BIGINT and a TEXT column
    identically, so column_schema is now inferred through the canonical
    schema_inference choke point.
    """
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Typed SQLite", _typed_sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT id, big_val, ratio, label FROM readings ORDER BY id",
        "limit": 100,
    })
    assert response.status_code == 200, response.text
    data = response.json()
    schema = data["column_schema"]

    assert set(schema) == {"id", "big_val", "ratio", "label"}
    # The whole point: not every column is the same type any more.
    assert len(set(schema.values())) > 1, schema
    assert schema["label"] not in {"INTEGER", "BIGINT", "FLOAT", "DOUBLE"}, schema
    # int64-max must stay numeric. It regressed to VARCHAR because one sample
    # (5000000000) is 10 digits and classified as an epoch TIMESTAMP, mixing
    # {INTEGER, TIMESTAMP} and widening to text — an integer key rendered as a
    # string is exactly the fidelity failure this product sells against.
    assert _is_numeric_type(schema["big_val"]), schema
    assert "CHAR" not in schema["big_val"].upper(), schema


def test_execute_labels_type_provenance_as_inferred(test_client, tmp_path, monkeypatch):
    """Result-set types are inferred; the API must not imply source DDL."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Provenance SQLite", _typed_sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT id FROM readings",
    })
    assert response.status_code == 200, response.text
    assert response.json()["column_type_source"] == "inferred_from_values"


def test_execute_reports_duration(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Timing SQLite", _sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users",
    })
    assert response.status_code == 200, response.text
    duration = response.json()["duration_ms"]
    assert isinstance(duration, (int, float))
    assert duration >= 0.0


def test_execute_flags_truncation_at_limit(test_client, tmp_path, monkeypatch):
    """A full page must be reported as truncated, not as the whole result."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Truncate SQLite", _sqlite_db(tmp_path))

    capped = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users ORDER BY id",
        "limit": 1,
    })
    assert capped.status_code == 200, capped.text
    assert capped.json()["row_count"] == 1
    assert capped.json()["truncated"] is True

    full = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users ORDER BY id",
        "limit": 100,
    })
    assert full.status_code == 200, full.text
    assert full.json()["truncated"] is False


def test_execute_empty_result_reports_unknown_types(test_client, tmp_path, monkeypatch):
    """No rows means no evidence — report unknown rather than guessing."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Empty SQLite", _sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT id, name FROM users WHERE id < 0",
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["row_count"] == 0
    assert data["truncated"] is False
    assert set(data["column_schema"].values()) <= {"unknown"}


def test_execute_binds_named_parameters(test_client, tmp_path, monkeypatch):
    """`:name` placeholders must bind server-side, never be interpolated."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Params SQLite", _sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users WHERE name = :who",
        "params": {"who": "bob"},
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["row_count"] == 1
    assert data["rows"][0]["name"] == "bob"


def test_execute_bound_parameter_cannot_inject_sql(test_client, tmp_path, monkeypatch):
    """A parameter value containing SQL stays a value, and the table survives."""
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_db(tmp_path)
    conn = _sqlite_connector("Injection SQLite", db_path)

    response = test_client.post("/api/v1/query/execute", json={
        "connector_id": conn.id,
        "query": "SELECT * FROM users WHERE name = :who",
        "params": {"who": "bob'; DROP TABLE users; --"},
    })
    assert response.status_code == 200, response.text
    assert response.json()["row_count"] == 0

    check = sqlite3.connect(db_path)
    try:
        assert check.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 2
    finally:
        check.close()


def test_schema_lists_objects_without_columns(test_client, tmp_path, monkeypatch):
    """Phase one: objects only — expanding every table would not scale."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Schema SQLite", _typed_sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/schema", json={"connector_id": conn.id})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["connected"] is True
    names = [o["name"] for o in data["objects"]]
    assert any(n.split(".")[-1] == "readings" for n in names), names
    assert all(o["columns"] == [] for o in data["objects"]), data["objects"]


def test_schema_expands_requested_object_columns(test_client, tmp_path, monkeypatch):
    """Phase two: the named object comes back with its columns and types."""
    _isolated_store(monkeypatch, tmp_path)
    conn = _sqlite_connector("Schema Expand SQLite", _typed_sqlite_db(tmp_path))

    response = test_client.post("/api/v1/query/schema", json={
        "connector_id": conn.id,
        "object_name": "readings",
    })
    assert response.status_code == 200, response.text
    data = response.json()
    expanded = [o for o in data["objects"] if o["columns"]]
    assert len(expanded) == 1, data["objects"]
    cols = {c["name"]: c["type"] for c in expanded[0]["columns"]}
    assert {"id", "big_val", "ratio", "label"} <= set(cols), cols
    # Width must survive introspection — a narrowed or stringified key is the
    # P0 this product exists to prevent.
    assert _is_numeric_type(cols["big_val"]), cols
    assert "CHAR" not in cols["big_val"].upper(), cols
    assert data["type_source"] == "connector_introspection"


def test_schema_rejects_unknown_connector(test_client, tmp_path, monkeypatch):
    _isolated_store(monkeypatch, tmp_path)
    response = test_client.post("/api/v1/query/schema", json={"connector_id": "nope"})
    assert response.status_code == 404


def test_safe_sql_guard_blocks_additional_write_verbs():
    """The gate must not be a SELECT-prefix check with a small deny list."""
    assert _is_safe_sql("TRUNCATE TABLE users") is False
    assert _is_safe_sql("GRANT ALL ON users TO bob") is False
    assert _is_safe_sql("CREATE TABLE t (id INT)") is False
    assert _is_safe_sql("ALTER TABLE users ADD COLUMN x INT") is False
    assert _is_safe_sql("MERGE INTO users USING s ON 1=1") is False
    assert _is_safe_sql("") is False
    assert _is_safe_sql("   ") is False


def test_aggregate_stage_guard_blocks_writes():
    _validate_mongodb_aggregate([{"$match": {}}])  # ok
    with pytest.raises(Exception) as exc:
        _validate_mongodb_aggregate([{"$match": {}}, {"$out": "stolen"}])
    assert "$out" in str(exc.value)
