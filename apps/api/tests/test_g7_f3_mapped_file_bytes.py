"""Re-review: the local COPY mapped file matches csv.writer byte-for-byte except
where NULL vs a text ``\\N`` must differ — and a text "\\x00" stays text.

Before: mapped_csv_line routed cells through csv_cell_encode, whose NULL
sentinel is "\\x00", so a text cell that is exactly "\\x00" was written as the
NULL marker ``\\N`` and landed NULL (the run then failed Gate-8's strict
checksum, where it had succeeded before this PR).
"""

from __future__ import annotations

import csv
import io
import sqlite3
import uuid

import pytest

MATRIX = [
    "plain", "", "\x00", "a\x00b", "\\N", "\\\\N", "a,b", "a\tb", 'say "hi"', '"lead', "x\ny", "x\ry",
    " lead space", "trail ", "café", "=1+1", "-5", "NULL", "N", "\\", "a\\N",
]


def _writer_line(cells: list, delim: str) -> str:
    buf = io.StringIO()
    csv.writer(buf, delimiter=delim, lineterminator="\n").writerow(cells)
    return buf.getvalue()[:-1]


@pytest.mark.parametrize("delim", [",", "\t"])
@pytest.mark.parametrize("cell", MATRIX)
def test_mapped_cell_matches_csv_writer_except_text_backslash_n(cell, delim):
    from services.copy_csv_local import mapped_csv_line

    got = mapped_csv_line(["1", cell], delim)
    if cell == "\\N":
        assert got == f'1{delim}"\\N"'  # the one deliberate difference: a text \N is quoted
    else:
        assert got == _writer_line(["1", cell], delim), repr(cell)


def test_null_is_the_bare_marker_and_a_lone_empty_is_quoted():
    from services.copy_csv_local import mapped_csv_line

    assert mapped_csv_line(["1", None], ",") == "1,\\N"
    assert mapped_csv_line([""], ",") == '""' == _writer_line([""], ",")
    assert mapped_csv_line(["\x00"], ",") == _writer_line(["\x00"], ",")


@pytest.mark.parametrize("fast", [True, False])
def test_sqlite_lands_a_text_nul_cell(monkeypatch, tmp_path, fast):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "1" if fast else "0")
    db = tmp_path / "d.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(db), table="t"),
        source_filename="n.csv",
        source_content=b"id,note\n1,\x00\n2,x\n3,\n",
        sync_mode="full_refresh_overwrite",
        mappings=[
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "note", "target": "note", "type": "TEXT", "transform": "none"},
        ],
        column_types={"id": "INTEGER", "note": "TEXT"},
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT id, note FROM t ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [(1, "\x00"), (2, "x"), (3, None)]
    assert (result.reconciliation or {}).get("passed") is True
    if fast:
        assert (result.destination_summary or {}).get("copy_fast_path") == "used"
