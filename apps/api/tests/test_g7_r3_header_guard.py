"""G7 review M1: CSV/TSV header names are guarded too when protection is on.

Column names come from data (Mongo field names, SQL aliases, upload
headers). Before: with ``formula_protection: "prefix"`` the data cells were
guarded but a header such as ``=cmd|'/c calc'!A1`` was written raw by
``convert_rows`` (``str(h)``), ``DictWriter.writeheader()`` and the Query
console full-population writer. Gate-8 must map guarded headers back so a
"prefix" transfer still reconciles.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

EVIL = "=cmd|'/c calc'!A1"
COLS = ["id", EVIL, "@who", "-x", "plain"]
RECORDS = [
    {"id": "1", EVIL: "=1+1", "@who": "ok", "-x": "-5", "plain": "a"},
    {"id": "2", EVIL: "fine", "@who": "@SUM(A1)", "-x": "7", "plain": "b"},
]


def _rows(content: bytes, fmt: str) -> list[list[str]]:
    delim = "\t" if fmt == "tsv" else ","
    return list(csv.reader(io.StringIO(content.decode("utf-8"), newline=""), delimiter=delim))


def _write(fmt: str, source_format: str, protection: str | None):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    extra = {"formula_protection": protection} if protection else {}
    return write_destination_file(
        EndpointConfig(kind="file_export", format=fmt, extra=extra),
        [dict(r) for r in RECORDS],
        list(COLS),
        source_format=source_format,
    )


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
@pytest.mark.parametrize("source_format", ["csv", "postgresql"])
def test_prefix_guards_header_cells(fmt, source_format):
    content, _name, _summary = _write(fmt, source_format, "prefix")
    rows = _rows(content, fmt)
    assert rows[0] == ["id", "'" + EVIL, "'@who", "'-x", "plain"]
    assert rows[1][1] == "'=1+1"
    assert rows[1][3] == "-5"


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
@pytest.mark.parametrize("source_format", ["csv", "postgresql"])
def test_default_keeps_header_exact(fmt, source_format):
    content, _name, _summary = _write(fmt, source_format, None)
    assert _rows(content, fmt)[0] == COLS


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_gate8_read_back_maps_guarded_headers_back(tmp_path, fmt):
    from src.transfer.reconcile_step import _file_export_cell_readback

    content, _name, summary = _write(fmt, "csv", "prefix")
    path = tmp_path / f"export.{fmt}"
    path.write_bytes(content)
    mappings = [{"source": c, "target": c, "target_type": "text"} for c in COLS]
    compare = _file_export_cell_readback(
        [dict(r) for r in RECORDS],
        list(COLS),
        mappings,
        {**summary, "path": str(path), "primary_key_columns": ["id"]},
        fmt=fmt,
    )
    assert compare is not None
    assert compare.get("passed") is True, compare
    assert compare.get("rows_compared") == len(RECORDS), compare


def _query_client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from services import connector_store
    from src.main import app

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    connector_store._backend_choice = None
    db_path = tmp_path / "hdr.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'x')")
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "Header SQLite", "type": "sqlite", "role": "both",
        "connection_string": f"sqlite:///{db_path}", "workspace_id": "",
    })
    return TestClient(app), saved.id


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
@pytest.mark.parametrize("full_population", [False, True])
def test_query_console_guards_alias_headers(monkeypatch, tmp_path, fmt, full_population):
    client, cid = _query_client(monkeypatch, tmp_path)
    query = 'SELECT id, note AS "=cmd|\'/c calc\'!A1" FROM t'
    for protection, expected in (("prefix", "'" + EVIL), ("off", EVIL)):
        response = client.post("/api/v1/query/export", json={
            "connector_id": cid, "query": query, "format": fmt,
            "full_population": full_population, "formula_protection": protection,
        })
        assert response.status_code == 200, response.text
        header = _rows(Path(response.json()["path"]).read_bytes(), fmt)[0]
        assert header == ["id", expected], (protection, header)
