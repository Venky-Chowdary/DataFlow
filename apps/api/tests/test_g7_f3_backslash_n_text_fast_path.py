"""A text value that is literally ``\\N`` survives every file load path.

The codec reads a *quoted* ``"\\N"`` as the two-character text and an
unquoted ``\\N`` as NULL. Before: the local CSV/TSV COPY fast path re-encoded
the mapped rows with ``csv.writer`` (``\\N`` written bare) and then loaded them
with ``COPY ... NULL '\\N'`` (PostgreSQL) or re-read them (SQLite), so the text
became NULL while Gate-8 stayed green; ``combine_file_set`` re-encoded through
``csv_quote_cell``, which writes ``\\N`` bare, so a 2-file set lost it too. The
row path kept the text. After: wherever NULL is spelled ``\\N``, a text ``\\N``
is quoted, the fast path and the row path land the same values, and Gate-8
compares them truthfully.

DATA-21: an unquoted ``\\N`` is NULL only when the source declares it
(``read_options.null_marker``); these loads declare it, and the combined
file set (no read options) keeps every ``\\N`` as text.
"""

from __future__ import annotations

import socket
import sqlite3
import uuid

import pytest

CSV = b'id,note\n1,"\\N"\n2,\\N\n3,x\n4,""\n'
TSV = b'id\tnote\n1\t"\\N"\n2\t\\N\n3\tx\n4\t""\n'
EXPECTED = [(1, "\\N"), (2, None), (3, "x"), (4, "")]


def _pg_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


def _run(monkeypatch, content: bytes, filename: str, destination, *, fast: bool):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "1" if fast else "0")
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=filename.rsplit(".", 1)[-1]),
        destination=destination,
        source_filename=filename,
        source_content=content,
        sync_mode="full_refresh_overwrite",
        mappings=[
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "note", "target": "note", "type": "TEXT", "transform": "none"},
        ],
        column_types={"id": "INTEGER", "note": "TEXT"},
        read_options={"null_marker": "\\N"},
        skip_preflight=True,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@pytest.mark.parametrize("content, filename", [(CSV, "notes.csv"), (TSV, "notes.tsv")])
@pytest.mark.parametrize("fast", [True, False])
def test_sqlite_fast_and_row_path_keep_text_backslash_n(monkeypatch, tmp_path, content, filename, fast):
    from src.transfer.models import EndpointConfig

    db = tmp_path / "dst.db"
    dest = EndpointConfig(kind="database", format="sqlite", database=str(db), table="notes")
    result = _run(monkeypatch, content, filename, dest, fast=fast)
    assert result.success is True, result.error
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute("SELECT id, note FROM notes ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == EXPECTED, (fast, (result.destination_summary or {}).get("load_method"))
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
    if fast:
        assert (result.destination_summary or {}).get("copy_fast_path") == "used"


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
@pytest.mark.parametrize("fast", [True, False])
def test_postgres_copy_fast_and_row_path_keep_text_backslash_n(monkeypatch, fast):
    import psycopg2

    from src.transfer.models import EndpointConfig

    table = f"g7f3_bn_{uuid.uuid4().hex[:8]}"
    dest = EndpointConfig(
        kind="database", format="postgresql", host="127.0.0.1", port=5432, database="dataflow",
        username="dataflow", password="dataflow", schema="public", table=table,
    )
    try:
        result = _run(monkeypatch, CSV, "notes.csv", dest, fast=fast)
        assert result.success is True, result.error
        conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT id, note FROM public."{table}" ORDER BY id')  # nosec B608
                rows = [(int(i), n) for i, n in cur.fetchall()]
        finally:
            conn.close()
        assert rows == EXPECTED, (fast, (result.destination_summary or {}).get("load_method"))
        assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
    finally:
        conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()


def test_combine_file_set_keeps_text_backslash_n(tmp_path):
    import io

    from services.csv_codec import iter_csv_records
    from src.transfer.file_stream import combine_file_set

    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    a.write_bytes(CSV)
    b.write_bytes(b'id,note\n5,"\\N"\n6,\\N\n')

    def notes(paths: list) -> list:
        target = tmp_path / "combined.csv"
        combine_file_set([(p, p.name) for p in paths], target)
        text = target.read_text(encoding="utf-8")
        return [r[1] for r in list(iter_csv_records(io.StringIO(text, newline=""), ","))[1:]]

    assert notes([a]) == ["\\N", "\\N", "x", ""]
    assert notes([a, b]) == ["\\N", "\\N", "x", "", "\\N", "\\N"]


def test_writers_quote_a_text_backslash_n_wherever_null_is_spelled_backslash_n():
    from services.copy_csv_local import _csv_cell, mapped_csv_line
    from services.csv_codec import csv_cell_encode, csv_quote_cell, format_csv_row

    assert _csv_cell(None) == "\\N"
    assert mapped_csv_line(["\\N", None, "x", "", "a,b"], ",") == '"\\N",\\N,x,,"a,b"'
    assert mapped_csv_line([""], ",") == '""'
    assert mapped_csv_line(["\\N", None, "a\tb"], "\t") == '"\\N"\t\\N\t"a\tb"'
    # The operator CSV codec (NULL is an empty cell) also quotes a text \N, so
    # the reader's legacy "bare \N is NULL" rule cannot swallow it.
    assert csv_quote_cell("\\N") == '"\\N"'
    assert format_csv_row([csv_cell_encode("\\N"), csv_cell_encode(None)]) == '"\\N",'


def test_iceberg_csv_writers_decline_a_text_backslash_n():
    """csv.writer + Arrow null_values=["\\N"]: a bare text \\N would load as NULL."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_iceberg import iceberg_csv_cell

    assert iceberg_csv_cell(None) == "\\N"
    assert iceberg_csv_cell("x") == "x"
    with pytest.raises(FastPathUnavailable, match="row path"):
        iceberg_csv_cell("\\N")


def test_text_format_copy_writers_escape_a_text_backslash_n():
    """PostgreSQL / MySQL text-format COPY writers escape the backslash: no collision."""
    from services.copy_mysql_mysql import fast_load_data_text_value
    from services.copy_mysql_pg import fast_copy_text_value

    assert fast_copy_text_value(None) == "\\N"
    assert fast_copy_text_value("\\N") == "\\\\N"
    assert fast_load_data_text_value(None) == "\\N"
    assert fast_load_data_text_value("\\N") == "\\\\N"
