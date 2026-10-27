"""D121 / CR-D124 — file and object sources keep wire-spelling text as text.

A CSV/JSON cell that really says ``__DF_SQL_NULL__`` landed as NULL and a
``__DF_ESC__x`` cell landed as ``x``, while Gate-8 reported matching
checksums. File parsers return what the payload spells, but every engine
consumer (matrix, spool, writer leaf, Gate-8 fingerprint) reads cells as the
transfer wire — the encoding SQL readers emit with
``cell_to_string(..., preserve_sql_null=True)``. The fix escapes parsed text
once where file/object payloads become records, keeps a decoded literal
marked (``WireLiteral``) through the matrix and the JSONL spool, and puts
destination read-backs on the same wire so verification compares like with
like. The legacy merge branch that read ``__DF_SQL_NULL__`` text in an
existing object as NULL is gone: the object encoder writes NULL as an empty
unquoted cell and a literal verbatim.

Round trip per destination: literal, literal, ``''``, NULL — and verified.
"""

from __future__ import annotations

import uuid

import pytest

import src.transfer.engine as engine_mod
from tests.test_cr_d125_file_quoted_empty_fidelity import (
    assert_verified,
    file_source,
    mysql_dest,
    mysql_rows_and_drop,
    pg_dest,
    pg_rows_and_drop,
    put_s3_object,
    run_transfer,
    s3_source,
    sqlite_dest,
    sqlite_rows,
)

CSV = 'id,name\n1,__DF_SQL_NULL__\n2,__DF_ESC__x\n3,""\n4,\n5,plain\n'
JSONL = (
    '{"id":1,"name":"__DF_SQL_NULL__"}\n{"id":2,"name":"__DF_ESC__x"}\n'
    '{"id":3,"name":""}\n{"id":4,"name":null}\n{"id":5,"name":"plain"}\n'
)
PAYLOADS = {"t.csv": CSV, "t.jsonl": JSONL}
EXPECTED = [(1, "__DF_SQL_NULL__"), (2, "__DF_ESC__x"), (3, ""), (4, None), (5, "plain")]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


@pytest.mark.parametrize("fast_path", ["1", "0"])
@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_local_file_literals_land_as_text_in_sqlite(tmp_path, monkeypatch, name, fast_path):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", fast_path)
    result = run_transfer(destination=sqlite_dest(tmp_path), **file_source(name, PAYLOADS[name]))
    assert_verified(result)
    assert sqlite_rows(tmp_path) == EXPECTED


@pytest.mark.parametrize("fast_path", ["1", "0"])
@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_local_file_literals_land_as_text_in_postgres(monkeypatch, name, fast_path):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", fast_path)
    table = f"d121_{uuid.uuid4().hex[:8]}"
    result = run_transfer(destination=pg_dest(table), **file_source(name, PAYLOADS[name]))
    rows = pg_rows_and_drop(table)
    assert_verified(result)
    assert rows == EXPECTED


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_local_file_literals_land_as_text_in_mysql(name):
    """MySQL row path: the matrix → JSONL spool → bind leaf."""
    table = f"d121_{uuid.uuid4().hex[:8]}"
    result = run_transfer(destination=mysql_dest(table), **file_source(name, PAYLOADS[name]))
    rows = mysql_rows_and_drop(table)
    assert_verified(result)
    assert rows == EXPECTED


@pytest.mark.parametrize("dest", ["sqlite", "postgresql"])
@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_s3_object_literals_land_as_text(tmp_path, name, dest):
    import moto

    key = f"in/{name}"
    bucket = f"d121-{uuid.uuid4().hex[:8]}"
    table = f"d121_{uuid.uuid4().hex[:8]}"
    destination = sqlite_dest(tmp_path) if dest == "sqlite" else pg_dest(table)
    with moto.mock_aws():
        put_s3_object(bucket, key, PAYLOADS[name])
        result = run_transfer(source=s3_source(bucket, key), destination=destination)
    rows = sqlite_rows(tmp_path) if dest == "sqlite" else pg_rows_and_drop(table)
    assert_verified(result)
    assert rows == EXPECTED


@pytest.mark.parametrize(
    ("key", "existing"),
    [
        ("exports/t.csv", 'id,name\n1,__DF_SQL_NULL__\n2,__DF_ESC__x\n3,""\n4,\n'),
        (
            "exports/t.jsonl",
            '{"id":1,"name":"__DF_SQL_NULL__"}\n{"id":2,"name":"__DF_ESC__x"}\n'
            '{"id":3,"name":""}\n{"id":4}\n',
        ),
    ],
)
def test_incremental_merge_keeps_stored_literals(key, existing):
    """The stored object is text: merging a batch must not reinterpret it."""
    import io
    import json

    from connectors.object_store_common import serialize_object_store_export
    from connectors.object_store_incremental import merge_object_bytes
    from services.value_serializer import SQL_NULL_SENTINEL

    cols = ["id", "name"]
    types = {"id": "INTEGER", "name": "VARCHAR"}
    new = serialize_object_store_export(
        key=key,
        mapped_rows=[("5", "plain"), ("6", SQL_NULL_SENTINEL)],
        target_cols=cols,
        dest_types=types,
        spill_max_size=1 << 20,
    )
    merged = merge_object_bytes(
        key=key,
        existing=existing.encode("utf-8"),
        new_export=new,
        target_cols=cols,
        dest_types=types,
        conflict_columns=["id"],
        strategy="key",
        spill_max_size=1 << 20,
    )
    body = merged.read_all().decode("utf-8")
    if key.endswith(".csv"):
        from services.csv_codec import iter_csv_records

        records = list(iter_csv_records(io.StringIO(body, newline=""), ","))
        got = {int(r[0]): r[1] for r in records[1:]}
        assert "__DF_ESC____DF" not in body
    else:
        got = {}
        for line in body.splitlines():
            rec = json.loads(line)
            got[int(rec["id"])] = rec.get("name")
    assert got == {
        1: "__DF_SQL_NULL__",
        2: "__DF_ESC__x",
        3: "",
        4: None,
        5: "plain",
        6: None,
    }


def test_decoded_escape_prefixed_literal_survives_a_second_decode_and_the_spool():
    from connectors.source_row_spool import SourceRowSpool, matrix_present_cell
    from services.value_serializer import (
        WireLiteral,
        escape_wire_text,
        payload_cell_wire,
        unescape_wire_text,
    )

    for literal in ("__DF_ESC__x", "__DF_SQL_NULL__"):
        cell = matrix_present_cell(payload_cell_wire(literal))
        assert isinstance(cell, WireLiteral) and cell == literal
        assert unescape_wire_text(cell) == literal
        spool = SourceRowSpool()
        try:
            spool.append_row([cell])
            [[loaded]] = list(spool.iter_rows())
        finally:
            spool.close()
        assert unescape_wire_text(loaded) == literal
        assert loaded == escape_wire_text(literal)
