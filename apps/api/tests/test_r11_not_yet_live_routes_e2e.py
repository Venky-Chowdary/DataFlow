"""QA round 11 (build 9c75ada4), Addendum A — "not yet live" routes.

* ``PG → file (csv/json/excel/parquet/sqlite)`` was refused: "Combination
  database → file/* not yet live". The matrix only knows destination kind
  ``file_export``; ``file`` is now that kind for an export format, and a SQLite
  "export" is a SQLite database file written in the exports folder through the
  ``database/sqlite`` route. A ``file`` destination with no export format is
  refused by name.
* ``SQLite file upload → PG`` was refused although the catalog marks SQLite
  live: there was no upload reader. The upload is sniffed (magic bytes),
  capped, staged read-only, validated through an immutable read-only URI and
  read through ``database/sqlite``.

Studio ``/transfer/run`` (multipart upload) and REST ``/transfer/execute``, a
real PostgreSQL, NULL / '' / whitespace / unicode, run 2 into the created table.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from tests.test_r11_mysql_pg_fk_ddl_e2e import (  # noqa: F401 — fixture re-export
    _connector,
    _sql,
    _wait,
    client,
)

ROWS = [(1, "x"), (2, None), (3, ""), (4, " 日本 ✓ ")]


def _sqlite_bytes(tmp_path: Path, *, tables: tuple[str, ...] = ("items",)) -> bytes:
    path = tmp_path / f"up_{uuid.uuid4().hex[:6]}.sqlite"
    conn = sqlite3.connect(path)
    with conn:
        for t in tables:
            conn.execute(f"CREATE TABLE {t} (id INTEGER PRIMARY KEY, v TEXT)")
            conn.executemany(f"INSERT INTO {t} VALUES (?, ?)", ROWS)
    conn.close()
    return path.read_bytes()


def _upload(client, content: bytes, filename: str, dst_conn: str, dst_table: str, **extra):
    data = {
        "source_kind": "file",
        "dest_kind": "database",
        "dest_format": "postgresql",
        "dest_connector_id": dst_conn,
        "dest_schema": "public",
        "dest_table": dst_table,
        "mappings_json": json.dumps(
            [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")]
        ),
        "sync_mode": "full_refresh_overwrite",
        "validation_mode": "strict",
        "async_mode": "false",
        **extra,
    }
    return client.post(
        "/api/v1/transfer/run",
        data=data,
        files={"file": (filename, content, "application/octet-stream")},
    )


@pytest.fixture
def pg_table():
    name = f"r11sq_{uuid.uuid4().hex[:6]}"
    try:
        yield name
    finally:
        _sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("filename", ["inventory.sqlite", "inventory.db"])
def test_sqlite_upload_lands_in_postgres_run1_run2(client, tmp_path, pg_table, filename):
    dst = _connector("postgresql", "destination")
    content = _sqlite_bytes(tmp_path)
    for _run_no in (1, 2):
        resp = _upload(client, content, filename, dst, pg_table)
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {
            k: job.get(k) for k in ("status", "error", "message")
        }
        assert _sql(f"SELECT id, v FROM {pg_table} ORDER BY id") == ROWS


def test_sqlite_upload_untrusted_bytes_are_refused_by_name(client, tmp_path, pg_table):
    dst = _connector("postgresql", "destination")
    resp = _upload(client, b"id,v\n1,x\n", "fake.sqlite", dst, pg_table)
    body = resp.text
    assert "not a SQLite database" in body, body
    two = _sqlite_bytes(tmp_path, tables=("a", "b"))
    resp = _upload(client, two, "two.sqlite", dst, pg_table)
    assert "name the one to load" in resp.text, resp.text
    # Naming the table loads it.
    resp = _upload(client, two, "two.sqlite", dst, pg_table, source_table="b")
    assert resp.status_code == 200, resp.text
    assert _wait(str(resp.json().get("job_id") or "")).get("status") == "completed"
    assert _sql(f"SELECT id, v FROM {pg_table} ORDER BY id") == ROWS


@pytest.fixture
def pg_source():
    name = f"r11ps_{uuid.uuid4().hex[:6]}"
    _sql(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT)")
    for row in ROWS:
        _sql(f"INSERT INTO {name} VALUES (%s, %s)", row)
    try:
        yield name
    finally:
        _sql(f"DROP TABLE IF EXISTS {name}")


def _execute(client, src: str, table: str, destination: dict):
    resp = client.post(
        "/api/v1/transfer/execute",
        json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": src,
                       "table": table, "schema": "public"},
            "destination": destination,
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")],
            "sync_mode": "full_refresh_overwrite",
            "validation_mode": "strict",
            "async_mode": False,
        },
    )
    return resp


def test_pg_to_file_kind_csv_is_live(client, pg_source, tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    src = _connector("postgresql", "source")
    out = f"{pg_source}.csv"
    for _run_no in (1, 2):
        resp = _execute(client, src, pg_source, {"kind": "file", "format": "csv", "output_path": out})
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error")}
        with open(tmp_path / "exports" / out, newline="", encoding="utf-8") as fh:
            got = list(csv.DictReader(fh))
        assert [r["id"] for r in got] == ["1", "2", "3", "4"]
        assert got[3]["v"] == " 日本 ✓ "


def test_pg_to_sqlite_export_routes_to_database_sqlite(client, pg_source, tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    src = _connector("postgresql", "source")
    out = f"{pg_source}.sqlite"
    for _run_no in (1, 2):
        resp = _execute(
            client, src, pg_source,
            {"kind": "file", "format": "sqlite", "output_path": out, "table": "items"},
        )
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error")}
        conn = sqlite3.connect(tmp_path / "exports" / out)
        try:
            assert conn.execute("SELECT id, v FROM items ORDER BY id").fetchall() == ROWS
        finally:
            conn.close()


def test_file_destination_without_export_format_is_refused_by_name():
    from src.transfer.registry import validate_transfer

    ok, msg = validate_transfer("database", "postgresql", "file", "pdf")
    assert not ok
    assert "use kind 'file_export'" in msg
