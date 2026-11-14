"""DATA-21: CSV NULL spellings behave the same on every engine.

Before the fix an unquoted ``\\N`` cell silently landed NULL (the C04 codec
read it as the legacy NULL spelling on every file), and a quoted ``""`` in a
DECIMAL/DATE column landed NULL on PostgreSQL, MySQL and MongoDB but was
refused on SQLite (its TEXT carrier read the blank as a stored ''). Now
``\\N`` is NULL only under the source's ``null_marker`` read option, and a
quoted ``""`` in a typed column is NULL everywhere, with the Validate note
"blank stored as SQL NULL".
"""

from __future__ import annotations

import json

import pytest

from services.csv_codec import decode_cell
from tests import w2dat_live as live

BSN = b"id,note,n\r\n1,\\N,5\r\n2,abc,\\N\r\n3,,7\r\n"
QUOTED = b'id,amount,d\r\n1,1.5,2024-01-02\r\n2,"",""\r\n3,2.5,2024-01-03\r\n'


def test_decode_cell_needs_a_declared_marker():
    assert decode_cell("\\N", False) == "\\N"
    assert decode_cell("\\N", False, null_marker="\\N") is None
    assert decode_cell("\\N", True, null_marker="\\N") == "\\N"
    assert decode_cell("", False) is None and decode_cell("", True) == ""


def test_read_options_carry_the_marker():
    from services.read_options import ReadOptions, ReadOptionsError

    assert ReadOptions.from_dict({"null_marker": "\\N"}).null_marker == "\\N"
    assert ReadOptions.from_dict({}).null_marker == ""
    with pytest.raises(ReadOptionsError):
        ReadOptions(null_marker="x" * 40)


def _dest(kind: str, name: str, tmp_path):
    if kind == "pg":
        return live.ep_pg(name)
    if kind == "mysql":
        return live.ep_my(name)
    if kind == "mongo":
        return live.ep_mongo(name)
    return live.ep_sqlite(tmp_path / "d21.db", name)


def _read(kind: str, name: str, tmp_path, cols: list[str]) -> list[tuple]:
    sel = ", ".join(cols)
    if kind == "pg":
        return [tuple(r) for r in live.pg(f'SELECT {sel} FROM "{name}" ORDER BY id')]
    if kind == "mysql":
        return [tuple(r) for r in live.my(f"SELECT {sel} FROM `{name}` ORDER BY id")]
    if kind == "mongo":
        import pymongo

        c = pymongo.MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=3000)
        try:
            return [tuple(d.get(k) for k in cols) for d in c["dataflow"][name].find().sort("id", 1)]
        finally:
            c.close()
    import sqlite3

    with sqlite3.connect(tmp_path / "d21.db") as conn:
        return conn.execute(f'SELECT {sel} FROM "{name}" ORDER BY id').fetchall()


def _drop(kind: str, name: str) -> None:
    if kind == "pg":
        live.drop_pg(name)
    elif kind == "mysql":
        live.drop_my(name)
    elif kind == "mongo":
        import pymongo

        c = pymongo.MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=3000)
        c["dataflow"].drop_collection(name)
        c.close()


def _need(kind: str) -> None:
    if kind == "pg":
        live.need_pg()
    elif kind == "mysql":
        live.need_mysql()
    elif kind == "mongo" and not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")


def _csv_run(kind, tmp_path, body: bytes, cols: list[str], **read_options):
    from src.transfer.models import EndpointConfig

    _need(kind)
    name = f"w2dat_d21_{live.uid()}"
    extra = {"source_content": body, "source_filename": "c.csv"}
    if read_options:
        extra["read_options"] = read_options
    try:
        res = live.run(EndpointConfig(kind="file", format="csv"), _dest(kind, name, tmp_path),
                       key="id", cols=cols, extra_req=extra)
        rows = _read(kind, name, tmp_path, cols) if res.success else []
        return res, rows
    finally:
        _drop(kind, name)


@pytest.mark.parametrize("kind", ["pg", "sqlite", "mysql", "mongo"])
def test_backslash_n_is_text_without_a_marker(kind, tmp_path):
    res, rows = _csv_run(kind, tmp_path, BSN, ["id", "note", "n"])
    assert res.success, res.error
    assert [(int(r[0]), r[1], str(r[2]) if r[2] is not None else None) for r in rows] == [
        (1, "\\N", "5"), (2, "abc", "\\N"), (3, None, "7")]


@pytest.mark.parametrize("kind", ["pg", "sqlite"])
def test_backslash_n_is_null_under_the_declared_marker(kind, tmp_path):
    res, rows = _csv_run(kind, tmp_path, BSN, ["id", "note", "n"], null_marker="\\N")
    assert res.success, res.error
    assert [(int(r[0]), r[1], None if r[2] is None else int(r[2])) for r in rows] == [
        (1, None, 5), (2, "abc", None), (3, None, 7)]


@pytest.mark.parametrize("kind", ["pg", "sqlite", "mysql", "mongo"])
def test_quoted_empty_in_a_typed_column_is_null_everywhere(kind, tmp_path):
    res, rows = _csv_run(kind, tmp_path, QUOTED, ["id", "amount", "d"])
    assert res.success, res.error
    assert (res.reconciliation or {}).get("passed") is True
    assert rows[1][1] is None and rows[1][2] is None
    assert "stored as SQL NULL" in json.dumps(res.validation_plan or {}, default=str)
