"""G7 review H1: TSV cells that hold a tab, quote, CR or LF are quoted.

Before: ``csv_codec.format_csv_row`` joined TSV cells with a raw tab and never
quoted. A cell ``x<TAB>=HYPERLINK(...)`` became two cells, the second one a
formula that bypassed the ``formula_protection`` guard; ``y<LF>=1+1`` split
into a new row starting with a formula. The same bug silently corrupted any
TSV row with an embedded tab or newline.

Rule (every TSV writer, regardless of the formula setting): RFC-4180-style
quoting as Python's ``csv`` module does with ``delimiter='\\t'`` — a cell that
contains the delimiter, ``"``, CR or LF is wrapped in quotes with ``"``
doubled. Rows that need no quoting stay byte-identical.
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

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'
TRICKY = [
    {"id": "1", "note": "x\t" + HYPERLINK},
    {"id": "2", "note": "y\n=1+1"},
    {"id": "3", "note": "z\r@SUM(A1)"},
    {"id": "4", "note": 'say "hi"'},
    {"id": "5", "note": '"leading quote'},
    {"id": "6", "note": "plain"},
]


def _tsv_rows(content: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(content.decode("utf-8"), newline=""), delimiter="\t"))


def _no_cell_starts_with_a_formula(rows: list[list[str]]) -> None:
    for row in rows:
        for cell in row:
            assert cell[:1] not in {"=", "+", "@"}, row


def test_format_csv_row_quotes_tsv_cells_like_python_csv():
    from services.csv_codec import csv_cell_encode, format_csv_row

    cells = ["a\tb", "c\nd", "e\rf", 'g"h', "plain", "", None]
    line = format_csv_row([csv_cell_encode(c) for c in cells], "\t")
    buf = io.StringIO()
    csv.writer(buf, delimiter="\t", lineterminator="").writerow(cells[:5])
    # '' and NULL stay empty unquoted TSV cells, as before.
    assert line == buf.getvalue() + "\t\t"
    assert next(csv.reader(io.StringIO(line, newline=""), delimiter="\t"))[:5] == cells[:5]


def test_format_csv_row_plain_tsv_rows_stay_byte_identical():
    from services.csv_codec import csv_cell_encode, format_csv_row

    cells = ["1", "alice", "", None, "-5", "=1+1", "a,b", "\\N"]
    assert format_csv_row([csv_cell_encode(c) for c in cells], "\t") == "1\talice\t\t\t-5\t=1+1\ta,b\t\\N"


@pytest.mark.parametrize("source_format", ["csv", "postgresql"])
@pytest.mark.parametrize("protection", [None, "prefix"])
def test_transfer_file_export_tsv_round_trips_exactly(source_format, protection):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    extra = {"formula_protection": protection} if protection else {}
    content, _name, _summary = write_destination_file(
        EndpointConfig(kind="file_export", format="tsv", extra=extra),
        [dict(r) for r in TRICKY],
        ["id", "note"],
        source_format=source_format,
    )
    rows = _tsv_rows(content)
    assert rows[0] == ["id", "note"]
    assert [r[1] for r in rows[1:]] == [r["note"] for r in TRICKY]
    assert all(len(r) == 2 for r in rows)
    _no_cell_starts_with_a_formula(rows)


def test_gate8_tsv_read_back_parses_quoted_cells(tmp_path):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import _file_export_cell_readback

    content, _name, summary = write_destination_file(
        EndpointConfig(kind="file_export", format="tsv"),
        [dict(r) for r in TRICKY],
        ["id", "note"],
        source_format="csv",
    )
    path = tmp_path / "export.tsv"
    path.write_bytes(content)
    mappings = [{"source": c, "target": c, "target_type": "text"} for c in ("id", "note")]
    compare = _file_export_cell_readback(
        [dict(r) for r in TRICKY],
        ["id", "note"],
        mappings,
        {**summary, "path": str(path), "primary_key_columns": ["id"]},
        fmt="tsv",
    )
    assert compare is not None
    assert compare.get("passed") is True, compare
    assert compare.get("rows_compared") == len(TRICKY), compare


def test_object_store_tsv_round_trips_exactly():
    from connectors.object_store_common import serialize_object_store_export

    export = serialize_object_store_export(
        key="out/export.tsv",
        mapped_rows=[(r["id"], r["note"]) for r in TRICKY],
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    rows = _tsv_rows(export.read_all())
    assert [r[1] for r in rows[1:]] == [r["note"] for r in TRICKY]


def test_query_console_tsv_export_round_trips_and_guards(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from services import connector_store
    from src.main import app

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    connector_store._backend_choice = None
    db_path = tmp_path / "tsv.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(int(r["id"]), r["note"]) for r in TRICKY])
    conn.commit()
    conn.close()
    saved = connector_store.create_connector({
        "name": "TSV SQLite", "type": "sqlite", "role": "both",
        "connection_string": f"sqlite:///{db_path}", "workspace_id": "",
    })
    client = TestClient(app)
    for full in (False, True):
        response = client.post("/api/v1/query/export", json={
            "connector_id": saved.id,
            "query": "SELECT id, note FROM t ORDER BY id",
            "format": "tsv",
            "full_population": full,
        })
        assert response.status_code == 200, response.text
        rows = _tsv_rows(Path(response.json()["path"]).read_bytes())
        assert [r[1] for r in rows[1:]] == [r["note"] for r in TRICKY], full
        _no_cell_starts_with_a_formula(rows)
