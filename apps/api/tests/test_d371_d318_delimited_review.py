"""PR 433 delimited review: quoted TSV on the PostgreSQL COPY paths, bare quotes, embedded CRLF.

* D371 edge case: S3 -> PostgreSQL ``COPY (FORMAT csv)`` takes the TSV this
  product writes (``''`` as ``""``, doubled quotes, quoted tab and CRLF) and
  lands every value exactly, NULL and ``''`` apart.
* A ``"`` that is not a well-formed quoted field declines COPY to the row path,
  for CSV as well as TSV: PostgreSQL's CSV parser loads ``a"b"c`` as ``abc``
  and lets ``5" pipe`` swallow the next rows. The decline comes from the one
  streaming scan (G7 F-a), including a quote pair split across chunks.
* D318: a CRLF inside a quoted CSV field reaches PostgreSQL byte for byte on
  the local COPY path and on a whole file -> PostgreSQL transfer. Python's
  default universal-newline text mode turned it into LF ("If newline='' is not
  specified, newlines embedded inside quoted fields will not be interpreted
  correctly", https://docs.python.org/3/library/csv.html#id4).
"""

from __future__ import annotations

import socket
import uuid
from pathlib import Path

import pytest

_PG = {
    "host": "127.0.0.1", "port": 5432, "database": "dataflow",
    "username": "dataflow", "password": "dataflow", "schema": "public",
}


def _pg_or_skip() -> None:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pg_rows(table: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, note FROM public."{table}" ORDER BY id::int')  # nosec B608
            return cur.fetchall()
    finally:
        conn.close()


def _pg_drop(table: str) -> None:
    import psycopg2

    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
    conn.close()


# ── the scan ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body, delimiter, bare",
    [
        (b'id\tnote\n1\t""\n2\t\n3\t"5"" pipe"\n4\t"a\tb"\n5\t"x\r\ny"\n', "\t", False),
        (b'id,note\n1,"a,b"\n2,"""lead"\n3,""\n', ",", False),
        (b'id\tnote\n1\t5" pipe\n', "\t", True),
        (b'id,note\n1,a"b"c\n', ",", True),
        (b'id,note\n1,"a"b\n', ",", True),
        (b'id,note\n1,"never closed\n', ",", True),
    ],
)
def test_quote_scanner_finds_bare_quotes(body, delimiter, bare):
    from services.delimited_scan import QuoteScanner

    whole = QuoteScanner(delimiter)
    whole.feed(body)
    assert whole.finish() is bare
    # Every split point gives the same answer: a "" or a closing quote can
    # straddle two chunks of the streaming scan.
    for cut in range(1, len(body)):
        split = QuoteScanner(delimiter)
        split.feed(body[:cut])
        split.feed(body[cut:])
        assert split.finish() is bare, cut


def test_record_breaks_skip_quoted_line_breaks():
    from services.delimited_scan import record_breaks, rewrite_record_breaks

    body = b'id\tnote\r\n1\t5" pipe\r\n2\t"a\nb"\r\n3\t"c\rd"\r\n'
    assert record_breaks(body, "\t") == (4, 0, 0)
    assert rewrite_record_breaks(body, b"\n", "\t") == b'id\tnote\n1\t5" pipe\n2\t"a\nb"\n3\t"c\rd"\n'


def test_scan_delimited_file_reports_bare_quotes_in_one_pass(tmp_path):
    from services.copy_s3_common import scan_delimited_file

    good = tmp_path / "good.tsv"
    good.write_bytes(b'id\tnote\n' + b'1\t"a""b"\n' * 5000)
    bad = tmp_path / "bad.tsv"
    bad.write_bytes(b'id\tnote\n' + b'1\t"a""b"\n' * 5000 + b'2\t5" pipe\n')
    assert scan_delimited_file(str(good), "\t", chunk_size=7).bare_quote is False
    assert scan_delimited_file(str(bad), "\t", chunk_size=7).bare_quote is True


# ── S3 -> PostgreSQL COPY ───────────────────────────────────────────────────


def _s3_copy(monkeypatch, body: bytes, key: str, rows: int):
    from services import copy_s3_pg

    class _FakeS3:
        def download_file(self, _bucket, _key, path):
            Path(path).write_bytes(body)

    monkeypatch.setattr(copy_s3_pg, "s3_list_keys", lambda *_a, **_k: [key])
    monkeypatch.setattr(copy_s3_pg, "s3_dest_count", lambda *_a, **_k: rows)
    monkeypatch.setattr(copy_s3_pg, "s3_client", lambda *_a, **_k: _FakeS3())
    monkeypatch.setattr(copy_s3_pg, "s3_bucket", lambda *_a, **_k: "bucket")
    table = f"d371_s3pg_{uuid.uuid4().hex[:8]}"
    result = copy_s3_pg.copy_s3_to_postgres(
        source_cfg={"type": "s3"}, source_table=key, dest_cfg=dict(_PG), dest_schema="public",
        dest_table=table, pairs=[("id", "id"), ("note", "note")], pg_ddls=["TEXT", "TEXT"],
        replace_destination=True,
    )
    return result, table


def test_product_tsv_takes_the_postgres_copy_path_exactly(monkeypatch):
    _pg_or_skip()
    from connectors.object_store_common import serialize_object_store_export

    values = [("1", ""), ("2", None), ("3", '5" pipe'), ("4", "a\tb"), ("5", "multi\r\nline"), ("6", "NULL")]
    export = serialize_object_store_export(
        key="out/data.tsv", mapped_rows=values, target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    )
    try:
        body = export.read_all()
    finally:
        export.close()
    table = ""
    try:
        result, table = _s3_copy(monkeypatch, body, "src.tsv", len(values))
        assert result.rows_copied == len(values)
        assert _pg_rows(table) == [tuple(v) for v in values]
    finally:
        if table:
            _pg_drop(table)


def test_csv_with_a_mid_field_quote_declines_instead_of_loading_abc(monkeypatch):
    _pg_or_skip()
    from services.copy_fast_path import FastPathUnavailable

    with pytest.raises(FastPathUnavailable, match="quote"):
        _s3_copy(monkeypatch, b'id,note\n1,a"b"c\n2,x\n', "src.csv", 2)


# ── D318: CRLF inside quotes into PostgreSQL ────────────────────────────────


def test_local_copy_keeps_a_quoted_crlf(tmp_path):
    _pg_or_skip()
    from services.copy_csv_local import copy_csv_to_postgres

    mapped = tmp_path / "mapped.csv"
    mapped.write_bytes(b'id,note\r\n1,"multi\r\nline"\r\n2,"cr\ronly"\r\n3,plain\r\n')
    table = f"d318_local_{uuid.uuid4().hex[:8]}"
    try:
        result = copy_csv_to_postgres(
            mapped_path=str(mapped), ext="csv", dest_cfg=dict(_PG), dest_schema="public",
            dest_table=table, pairs=[("id", "id"), ("note", "note")], pg_ddls=["TEXT", "TEXT"],
            source_count=3, replace_destination=True,
        )
        assert result.rows_copied == 3
        assert _pg_rows(table) == [("1", "multi\r\nline"), ("2", "cr\ronly"), ("3", "plain")]
    finally:
        _pg_drop(table)


def test_csv_file_to_postgres_transfer_keeps_a_quoted_crlf():
    _pg_or_skip()
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    table = f"d318_e2e_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="d318.csv",
        source_content=b'id,note\r\n1,"multi\r\nline"\r\n2,plain\r\n3,""\r\n4,\r\n',
        destination=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    try:
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert _pg_rows(table) == [(1, "multi\r\nline"), (2, "plain"), (3, ""), (4, None)]
    finally:
        _pg_drop(table)


# ── D297: a quoted "" on a JSON column ──────────────────────────────────────


def _json_into_nvarchar(empty_cells_as_null: bool):
    """Plan (coercion probe) and execute (the writer's row mapper) on one column.

    SQL Server keeps a JSON column in NVARCHAR(MAX). The ``json`` transform
    does not accept '' (it is not JSON text), so the writer lands a quoted ''
    as NULL when blank cells are absence (``empty_cells_as_null``) and refuses
    the row otherwise. The plan must say the same thing (R8).
    """
    from connectors.writer_common import build_mapped_rows_with_details
    from services.coercion_probe import analyze_coercion

    mapping = {"source": "payload", "target": "payload", "source_type": "TEXT",
               "target_type": "NVARCHAR(MAX)", "transform": "json", "create_new": True}
    report = analyze_coercion(
        sample_rows=[{"payload": None}, {"payload": ""}, {"payload": "{}"}],
        mappings=[mapping],
        source_types={"payload": "TEXT"}, dest_types={"payload": "NVARCHAR(MAX)"},
        dest_db_type="sqlserver", table_exists=False, validation_mode="balanced",
        empty_cells_as_null=empty_cells_as_null,
    )
    rows, errors = build_mapped_rows_with_details(
        headers=["id", "payload"], data_rows=[["1", ""], ["2", None], ["3", "{}"]],
        mappings=[{"source": "id", "target": "id"}, mapping],
        target_cols=["id", "payload"], column_types={"id": "INTEGER", "payload": "TEXT"},
        dest_types={"payload": "NVARCHAR(MAX)"}, dest_kind="sqlserver",
        error_policy="fail", empty_cells_as_null=empty_cells_as_null,
    )[:2]
    return report, rows, errors


def test_sqlserver_nvarchar_json_lands_a_quoted_empty_as_null_when_blanks_are_absence():
    """D297/R8: a blank cell is absence under empty_cells_as_null, so the
    writer stores NULL; the plan probes the column and predicts that NULL."""
    from src.transfer.file_stream import file_empty_cell_bind

    # Without a transform, the shared empty-cell bind keeps '' on NVARCHAR (R2).
    assert file_empty_cell_bind("", "NVARCHAR(MAX)") == ""
    assert file_empty_cell_bind(None, "NVARCHAR(MAX)") is None

    report, rows, errors = _json_into_nvarchar(True)
    assert report["checked"] == 1, report
    column = report["by_source"]["payload"]
    assert column["failed"] == 0
    assert column["nulls"] == 2  # the SQL NULL and the '' the writer lands as NULL
    assert column["ok"] == 1
    assert report["has_blocking_failures"] is False
    assert errors == []
    assert sorted(rows) == [(1, None), (2, None), (3, "{}")]


def test_sqlserver_nvarchar_json_refuses_a_quoted_empty_at_plan_as_at_execute():
    """Without empty_cells_as_null the writer refuses the '' row; the plan
    must name that refusal rather than report nothing (R8)."""
    report, rows, errors = _json_into_nvarchar(False)
    assert report["checked"] == 1, report
    column = report["by_source"]["payload"]
    assert column["failed"] == 1
    assert "Empty value cannot coerce to json" in str(column["sample_failures"])
    assert column["severity"] == "block"
    assert report["has_blocking_failures"] is True
    assert errors and "Empty value cannot coerce to json" in errors[0]
    assert (1, "") not in rows


@pytest.mark.parametrize("pg_type", ["json", "jsonb"])
def test_quoted_empty_into_a_postgres_json_column_still_loads(pg_type):
    """PR 433 made the json transform return '' for a quoted "", which the JSON
    bind then refused: an append the plan passed (a blank is absence on a
    nullable JSON column) failed at execute (R8). jsonb cannot hold ''; the
    C04 empty-cell rule lands it NULL as before, the unquoted empty too."""
    _pg_or_skip()
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    table = f"d297_{pg_type}_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}" (id int, note {pg_type})')  # nosec B608
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="d297.csv",
        source_content=b'id,note\n1,"{""a"":1}"\n2,""\n3,\n',
        destination=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    try:
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        rows = _pg_rows(table)
        assert [r[0] for r in rows] == [1, 2, 3]
        assert rows[0][1] == {"a": 1} and rows[1][1] is None and rows[2][1] is None
    finally:
        _pg_drop(table)
