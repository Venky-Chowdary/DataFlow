"""G7 review lows: numeral rule is ASCII/whole-string, one option vocabulary, XLSX scale."""

from __future__ import annotations

import io
import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'


# ── numeral exemption: ASCII digits, whole string ────────────────────────────


@pytest.mark.parametrize("raw", ["-١٢", "-5\n", "+3\n=1+1", "-５"])
def test_non_ascii_or_trailing_newline_numerals_are_guarded(raw):
    from services.csv_safety import neutralize_export_cell, restore_export_cell

    assert neutralize_export_cell(raw) == "'" + raw
    assert restore_export_cell("'" + raw) == raw


@pytest.mark.parametrize("raw", ["-5", "-1.5", "+3", "-1e-3", ".5", "-0"])
def test_ascii_numerals_stay_numbers(raw):
    from services.csv_safety import neutralize_export_cell

    assert neutralize_export_cell(raw) == raw


# ── one vocabulary for Query export and transfer destinations ────────────────


@pytest.mark.parametrize("value", [" PREFIX ", "Prefix", "off", " Off ", None, ""])
def test_endpoint_dto_accepts_the_vocabulary(value):
    from src.routers.transfer_router import EndpointDTO

    assert EndpointDTO(kind="file_export", format="csv", formula_protection=value)
    assert EndpointDTO(kind="file_export", format="csv", extra={"formula_protection": value})


@pytest.mark.parametrize("value", ["strip", "on", 1, "prefix,off"])
def test_endpoint_dto_rejects_others(value):
    from pydantic import ValidationError

    from src.routers.transfer_router import EndpointDTO

    with pytest.raises(ValidationError):
        EndpointDTO(kind="file_export", format="csv", formula_protection=value)


def _query_client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from services import connector_store
    from src.main import app

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    connector_store._backend_choice = None
    db_path = tmp_path / "vocab.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    conn.execute("INSERT INTO t VALUES (1, ?)", (HYPERLINK,))
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "Vocab SQLite", "type": "sqlite", "role": "both",
        "connection_string": f"sqlite:///{db_path}", "workspace_id": "",
    })
    return TestClient(app), saved.id


@pytest.mark.parametrize(
    "value, guarded",
    [(" PREFIX ", True), ("Prefix", True), (None, True), ("", True), ("off", False), (" Off ", False)],
)
def test_query_export_accepts_the_same_vocabulary(monkeypatch, tmp_path, value, guarded):
    client, cid = _query_client(monkeypatch, tmp_path)
    response = client.post("/api/v1/query/export", json={
        "connector_id": cid, "query": "SELECT id, note FROM t", "format": "csv",
        "formula_protection": value,
    })
    assert response.status_code == 200, response.text
    body = Path(response.json()["path"]).read_bytes().decode("utf-8")
    assert ("'=HYPERLINK" in body) is guarded, body


@pytest.mark.parametrize("value", ["strip", "on", 1])
def test_query_export_rejects_others(monkeypatch, tmp_path, value):
    client, cid = _query_client(monkeypatch, tmp_path)
    response = client.post("/api/v1/query/export", json={
        "connector_id": cid, "query": "SELECT id, note FROM t", "format": "csv",
        "formula_protection": value,
    })
    assert response.status_code == 422, response.text


# ── XLSX numeric scale is displayed ──────────────────────────────────────────


def _formats(content: bytes) -> list[tuple[object, str]]:
    import openpyxl

    ws = openpyxl.load_workbook(io.BytesIO(content)).active
    return [(c.value, c.number_format) for c in next(ws.iter_rows(min_row=2))]


def test_file_export_xlsx_shows_declared_scale():
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    types = {"a": "numeric(12,2)", "b": "DECIMAL(10, 0)", "c": "numeric", "d": "decimal(38,4)", "e": "integer"}
    content, _n, _s = write_destination_file(
        EndpointConfig(kind="file_export", format="excel"),
        [{"a": "12.50", "b": "7", "c": "1.5", "d": "-0.1000", "e": "3"}],
        list(types),
        source_format="csv",
        column_types=types,
    )
    assert _formats(content) == [(12.5, "0.00"), (7, "0"), (1.5, "General"), (-0.1, "0.0000"), (3, "General")]


def test_object_store_xlsx_shows_declared_scale():
    from connectors.object_store_materialize import ObjectStoreEncoder

    enc = ObjectStoreEncoder(
        key="out/r.xlsx", target_cols=["a", "b"], dest_types={"a": "NUMERIC(12,2)", "b": "TEXT"}
    )
    enc.append_rows([("10.50", "x")])
    assert _formats(enc.finish().read_all()) == [(10.5, "0.00"), ("x", "General")]
