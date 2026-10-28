"""D178 — a MySQL binlog JSON document must reach the destination as the same document.

python-mysql-replication decodes a binary JSON column into containers whose keys and
string values are ``bytes``. Serialised as is, the keys became ``"b'k'"`` and the
strings base64, and Gate-8 only caught it after the corrupted rows were committed.
"""

from __future__ import annotations

import json
import uuid

import pytest

from connectors.mysql_change_stream import MySqlChangeStreamCdc, _serialize

DOC = {"k": 1, "s": 'q"uote', "nested": {"a": [1, 2, 1]}, "ü": "ünï"}


def _binlog_form(value):
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, dict):
        return {_binlog_form(k): _binlog_form(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_binlog_form(v) for v in value]
    return value


def test_binlog_json_document_serialises_as_the_source_document():
    wire = _serialize(_binlog_form(DOC))
    assert json.loads(wire) == DOC
    assert "b'" not in wire


def test_binlog_json_array_serialises_as_the_source_array():
    assert json.loads(_serialize(_binlog_form([1, "two", {"x": [3]}]))) == [1, "two", {"x": [3]}]


def test_blob_bytes_stay_base64():
    assert _serialize(b"\x00\xffbin") == "AP9iaW4="


def test_row_to_record_decodes_json_and_keeps_blob():
    cdc = MySqlChangeStreamCdc.__new__(MySqlChangeStreamCdc)
    cdc._remap_positional = lambda row, table=None: row
    cdc._instant_columns = lambda table=None: frozenset()
    record = MySqlChangeStreamCdc._row_to_record(
        cdc, {"id": 1, "payload": _binlog_form(DOC), "raw": b"\x00\xff"}
    )
    assert json.loads(record["payload"]) == DOC
    assert record["raw"] == "AP8="


def _mysql_ready() -> bool:
    try:
        from tests.test_cdc_mysql_binlog_integration import _mysql_binlog_ready

        return _mysql_binlog_ready()
    except Exception:
        return False


def _insert(table: str, pk: int, name: str, doc_text: str) -> None:
    from tests.test_cdc_mysql_binlog_integration import _connect

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {table} VALUES (%s, %s, CAST(%s AS JSON))", (pk, name, doc_text))
        conn.commit()
    finally:
        conn.close()


@pytest.mark.skipif(not _mysql_ready(), reason="MySQL with ROW binlog not reachable")
def test_live_binlog_insert_and_update_keep_the_json_document():
    from tests.test_cdc_mysql_binlog_integration import CFG, _exec

    table = "d178_" + uuid.uuid4().hex[:8]
    doc_text = json.dumps(DOC, ensure_ascii=False)
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(50), payload JSON)")
    _insert(table, 1, "a", doc_text)
    holder = f"d178-{table}"
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    cdc = resume = None
    try:
        cdc = MySqlChangeStreamCdc(cfg, table=table, primary_key="id", max_wait_seconds=8.0, cursor_key=f"it:{holder}")
        assert cdc.is_available() is True
        token = list(cdc.snapshot())[-1].resume_token
        _exec(f"UPDATE {table} SET name='x' WHERE id=1")
        _insert(table, 2, "b", doc_text)
        cdc.close()
        resume = MySqlChangeStreamCdc(
            cfg, table=table, primary_key="id", resume_token=token, max_wait_seconds=8.0, cursor_key=f"it:{holder}"
        )
        changes = list(resume.poll())
        rows = [r for b in changes for r in (*b.inserts, *b.updates)]
        assert {str(r["id"]) for r in rows} == {"1", "2"}
        for r in rows:
            assert json.loads(r["payload"]) == DOC, r["payload"]
    finally:
        for o in (resume, cdc):
            if o is not None:
                try:
                    o.close()
                except Exception:
                    pass
        _exec(f"DROP TABLE IF EXISTS {table}")
