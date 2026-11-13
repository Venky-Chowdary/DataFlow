"""G7 (CWE-1236): CSV/TSV exports and spreadsheet formula injection.

Two kinds of CSV/TSV export, two defaults:

* Human downloads produced on request (the Query console's file export, both
  the preview and the full-population path) neutralise formula leaders by
  default: a text cell starting with ``=``, ``+``, ``-``, ``@``, a tab or a CR
  is prefixed with ``'`` (OWASP). Numbers are never touched: ``-5`` and
  ``-1.5`` stay numbers.
* A transfer job's ``file_export`` destination is data for machine consumption
  (Gate-8 re-reads it, downstream loads ingest it). It stays byte-exact unless
  the destination opts in with ``formula_protection: "prefix"``; Gate-8 then
  reads the guarded file back to the exact values it sent.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'

# (id, note, amount REAL, qty INTEGER)
_ROWS = [
    (1, HYPERLINK, -1.5, -5),
    (2, "@SUM(A1)", 2.25, 3),
    (3, "+cmd", 0.0, 0),
    (4, "-2+3", -0.5, -12),
    (5, "\tlead-tab", 1.0, 1),
    (6, "-1.5", 3.5, 2),
    (7, "'=already-quoted", 4.0, 4),
    (8, "plain", 5.0, 5),
]


# ── the cell rule ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, guarded",
    [
        (HYPERLINK, "'" + HYPERLINK),
        ("=== VIP ===", "'=== VIP ==="),
        ("@SUM(A1)", "'@SUM(A1)"),
        ("+cmd", "'+cmd"),
        ("-2+3", "'-2+3"),
        ("\tx", "'\tx"),
        ("\rx", "'\rx"),
        ("'=x", "''=x"),
        ("plain", "plain"),
        ("", ""),
        (None, ""),
        # Numbers are data, not formulas — never mangled.
        ("-5", "-5"),
        ("-1.5", "-1.5"),
        ("+3", "+3"),
        ("-1e-3", "-1e-3"),
        (-5, "-5"),
        (-1.5, "-1.5"),
    ],
)
def test_neutralize_export_cell(raw, guarded):
    from services.csv_safety import neutralize_export_cell, restore_export_cell

    assert neutralize_export_cell(raw) == guarded
    # Reversible: Gate-8 reads a guarded file back to the exact value sent.
    expected = "" if raw is None else str(raw)
    assert restore_export_cell(guarded) == expected


def test_formula_protection_mode_vocabulary():
    from services.csv_safety import formula_protection_mode

    assert formula_protection_mode(None) == "off"
    assert formula_protection_mode("", default="prefix") == "prefix"
    assert formula_protection_mode("PREFIX") == "prefix"
    assert formula_protection_mode("off", default="prefix") == "off"
    with pytest.raises(ValueError, match="formula_protection"):
        formula_protection_mode("strip")


# ── Query console: a human download, guarded by default ──────────────────────


def _query_setup(monkeypatch, tmp_path: Path):
    from services import connector_store

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    connector_store._backend_choice = None
    db_path = tmp_path / "g7.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, amount REAL, qty INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", _ROWS)
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "G7 SQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path}",
        "workspace_id": "",
    })
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app), saved.id


def _export_rows(client, connector_id: str, **body) -> list[list[str]]:
    response = client.post("/api/v1/query/export", json={
        "connector_id": connector_id,
        "query": "SELECT id, note, amount, qty FROM t ORDER BY id",
        **body,
    })
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True, data
    text = Path(data["path"]).read_bytes().decode("utf-8")
    delim = "\t" if body.get("format") == "tsv" else ","
    return list(csv.reader(io.StringIO(text, newline=""), delimiter=delim))


def _assert_guarded(rows: list[list[str]]) -> None:
    assert rows[0] == ["id", "note", "amount", "qty"]
    by_id = {r[0]: r for r in rows[1:]}
    assert by_id["1"][1] == "'" + HYPERLINK
    assert by_id["2"][1] == "'@SUM(A1)"
    assert by_id["3"][1] == "'+cmd"
    assert by_id["4"][1] == "'-2+3"
    assert by_id["5"][1] == "'\tlead-tab"
    assert by_id["6"][1] == "-1.5"
    assert by_id["7"][1] == "''=already-quoted"
    assert by_id["8"][1] == "plain"
    # Negative numbers are numbers — never prefixed.
    assert (by_id["1"][2], by_id["1"][3]) == ("-1.5", "-5")
    assert (by_id["4"][2], by_id["4"][3]) == ("-0.5", "-12")


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_query_console_export_guards_formula_cells_by_default(monkeypatch, tmp_path, fmt):
    client, cid = _query_setup(monkeypatch, tmp_path)
    _assert_guarded(_export_rows(client, cid, format=fmt))


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_query_console_full_population_export_guards_formula_cells(monkeypatch, tmp_path, fmt):
    client, cid = _query_setup(monkeypatch, tmp_path)
    _assert_guarded(_export_rows(client, cid, format=fmt, full_population=True))


@pytest.mark.parametrize("full_population", [False, True])
def test_query_console_export_can_opt_out_for_exact_values(monkeypatch, tmp_path, full_population):
    client, cid = _query_setup(monkeypatch, tmp_path)
    rows = _export_rows(
        client, cid, format="csv", formula_protection="off", full_population=full_population
    )
    by_id = {r[0]: r for r in rows[1:]}
    assert by_id["1"][1] == HYPERLINK
    assert by_id["7"][1] == "'=already-quoted"


def test_query_console_export_rejects_unknown_protection(monkeypatch, tmp_path):
    client, cid = _query_setup(monkeypatch, tmp_path)
    response = client.post("/api/v1/query/export", json={
        "connector_id": cid, "query": "SELECT 1", "format": "csv", "formula_protection": "strip",
    })
    assert response.status_code == 422


# ── Transfer-job file destination: data, byte-exact by default ───────────────

_RECORDS = [
    {"id": "1", "note": HYPERLINK, "amount": "-1.5", "qty": "-5"},
    {"id": "2", "note": "=== VIP ===", "amount": "12.50", "qty": "3"},
    {"id": "3", "note": "@SUM(A1)", "amount": "0", "qty": "0"},
    {"id": "4", "note": "'=already-quoted", "amount": "1", "qty": "-1"},
]
_COLS = ["id", "note", "amount", "qty"]
_TYPES = {"id": "integer", "note": "text", "amount": "numeric(12,2)", "qty": "integer"}
_EXACT_CSV = (
    "id,note,amount,qty\n"
    '1,"=HYPERLINK(""http://evil.example/x"",""Click"")",-1.5,-5\n'
    "2,=== VIP ===,12.50,3\n"
    "3,@SUM(A1),0,0\n"
    "4,'=already-quoted,1,-1\n"
).encode("utf-8")


def _write(fmt: str, *, source_format: str, extra: dict | None = None):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    return write_destination_file(
        EndpointConfig(kind="file_export", format=fmt, extra=dict(extra or {})),
        [dict(r) for r in _RECORDS],
        list(_COLS),
        source_format=source_format,
        column_types=dict(_TYPES),
    )


@pytest.mark.parametrize("source_format", ["csv", "postgresql"])
def test_transfer_file_export_csv_is_byte_exact_by_default(source_format):
    content, _name, summary = _write("csv", source_format=source_format)
    assert content.replace(b"\r\n", b"\n") == _EXACT_CSV
    assert "formula_protection" not in summary


def test_transfer_file_export_tsv_is_exact_by_default():
    content, _name, _summary = _write("tsv", source_format="csv")
    rows = list(csv.reader(io.StringIO(content.decode("utf-8"), newline=""), delimiter="\t"))
    assert [r[1] for r in rows[1:]] == [r["note"] for r in _RECORDS]
    assert [(r[2], r[3]) for r in rows[1:]] == [(r["amount"], r["qty"]) for r in _RECORDS]


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
@pytest.mark.parametrize("source_format", ["csv", "postgresql"])
def test_transfer_file_export_opt_in_prefix(fmt, source_format):
    content, _name, summary = _write(fmt, source_format=source_format, extra={"formula_protection": "prefix"})
    delim = "\t" if fmt == "tsv" else ","
    rows = list(csv.reader(io.StringIO(content.decode("utf-8"), newline=""), delimiter=delim))
    by_id = {r[0]: r for r in rows[1:]}
    assert by_id["1"][1:] == ["'" + HYPERLINK, "-1.5", "-5"]
    assert by_id["2"][1] == "'=== VIP ==="
    assert by_id["3"][1] == "'@SUM(A1)"
    assert by_id["4"][1:] == ["''=already-quoted", "1", "-1"]
    assert summary.get("formula_protection") == "prefix"


def test_transfer_file_export_rejects_unknown_protection():
    with pytest.raises(ValueError, match="formula_protection"):
        _write("csv", source_format="csv", extra={"formula_protection": "strip"})


def test_endpoint_dto_rejects_unknown_protection():
    from pydantic import ValidationError

    from src.routers.transfer_router import EndpointDTO

    assert EndpointDTO(kind="file_export", format="csv", formula_protection="prefix")
    assert EndpointDTO(kind="file_export", format="csv", extra={"formula_protection": "off"})
    with pytest.raises(ValidationError):
        EndpointDTO(kind="file_export", format="csv", formula_protection="strip")
    with pytest.raises(ValidationError):
        EndpointDTO(kind="file_export", format="csv", extra={"formula_protection": "strip"})


@pytest.mark.parametrize("extra", [None, {"formula_protection": "prefix"}])
def test_gate8_cell_readback_green_for_default_and_guarded_csv(tmp_path, extra):
    from src.transfer.reconcile_step import _file_export_cell_readback

    content, _name, summary = _write("csv", source_format="csv", extra=extra)
    path = tmp_path / "export.csv"
    path.write_bytes(content)
    mappings = [{"source": c, "target": c, "target_type": _TYPES[c]} for c in _COLS]
    compare = _file_export_cell_readback(
        [dict(r) for r in _RECORDS],
        list(_COLS),
        mappings,
        {**summary, "path": str(path), "primary_key_columns": ["id"]},
        fmt="csv",
    )
    assert compare is not None
    assert compare.get("passed") is True, compare
    assert compare.get("rows_compared") == len(_RECORDS), compare


@pytest.mark.parametrize("extra", [None, {"formula_protection": "prefix"}])
def test_transfer_job_file_export_end_to_end(monkeypatch, tmp_path, extra):
    """A real job: default stays byte-exact, the opt-in guards; Gate-8 passes both."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    source = _EXACT_CSV
    destination = EndpointConfig.from_dict("file_export", {"format": "csv", **(extra or {})})
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=destination,
        source_filename="g7.csv",
        source_content=source,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    written = Path(result.destination_summary["path"]).read_bytes().replace(b"\r\n", b"\n")
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    if extra:
        assert b"'=HYPERLINK" in written
        assert b"'=== VIP ===" in written
        assert b",-1.5,-5\n" in written
    else:
        assert written == source
