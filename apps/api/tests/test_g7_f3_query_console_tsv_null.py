"""G7-F3: the Query console's TSV export offers the same NULL marker as a TSV destination.

``tsv_null: "\\N"`` writes NULL as ``\\N`` and ``''`` as an empty cell on both
export paths (the preview and the full-population stream); the default is
unchanged; ``\\N`` on any other format, or an unknown value, is a 422.
"""

from __future__ import annotations

import io
import sqlite3
from pathlib import Path

import pytest

ROWS = [(1, ""), (2, None), (3, "\\N"), (4, "=x")]


@pytest.fixture()
def console(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from services import connector_store
    from src.main import app

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    connector_store._backend_choice = None
    db = tmp_path / "q.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", ROWS)
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "G7F3 SQLite", "type": "sqlite", "role": "both",
        "connection_string": f"sqlite:///{db}", "workspace_id": "",
    })
    return TestClient(app), saved.id


def _export(console, **body):
    client, cid = console
    return client.post("/api/v1/query/export", json={
        "connector_id": cid, "query": "SELECT id, note FROM t ORDER BY id", **body,
    })


def _decoded(path: str) -> list[list[str | None]]:
    from services.csv_codec import iter_csv_records

    text = Path(path).read_bytes().decode("utf-8")
    return list(iter_csv_records(io.StringIO(text, newline=""), "\t", empty_cell="keep"))[1:]


@pytest.mark.parametrize("full_population", [False, True])
def test_query_console_tsv_text_null_round_trips(console, full_population):
    response = _export(console, format="tsv", tsv_null="\\N", full_population=full_population, formula_protection="off")
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True, data
    rows = _decoded(data["path"])
    assert [r[1] for r in rows] == ["", None, "\\N", "=x"]
    raw = Path(data["path"]).read_bytes()
    assert b"2\t\\N\r\n" in raw and b'3\t"\\N"\r\n' in raw


@pytest.mark.parametrize("full_population", [False, True])
def test_query_console_tsv_text_null_with_formula_guard(console, full_population):
    response = _export(console, format="tsv", tsv_null="\\N", full_population=full_population)
    rows = _decoded(response.json()["path"])
    assert [r[1] for r in rows] == ["", None, "\\N", "'=x"]


def test_query_console_tsv_default_unchanged(console):
    data = _export(console, format="tsv", full_population=True, formula_protection="off").json()
    assert Path(data["path"]).read_bytes() == b"id\tnote\r\n1\t\r\n2\t\r\n3\t\\N\r\n4\t=x\r\n"


@pytest.mark.parametrize("body", [{"format": "csv", "tsv_null": "\\N"}, {"format": "tsv", "tsv_null": "NULL"}])
def test_query_console_refuses_bad_tsv_null(console, body):
    response = _export(console, **body)
    assert response.status_code == 422, response.text
    assert "tsv_null" in response.text
