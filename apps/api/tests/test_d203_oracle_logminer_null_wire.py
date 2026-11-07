"""Provisional D203 family — Oracle LogMiner rendered SQL NULL as ''.

Same defect as the SQL Server native CDC reader: the LogMiner snapshot built
records with ``"" if v is None else str(v)`` and the SQL_REDO parser turned a
``NULL`` literal into ``""``. The shared CDC matrix builder then wrote a present
empty string where the source held NULL.

The snapshot test reads a live Oracle table through ``OracleLogMinerCdc.snapshot``
(resuming a snapshot-phase token, so the read does not need ``v$database``,
which the local test user cannot see) and runs the records through the CDC
matrix builder the transfer uses. LogMiner streaming itself needs ARCHIVELOG
and supplemental logging, which the local Oracle Free container does not have;
the redo test drives the real SQL_REDO parser with LogMiner's spelling.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    classify_sql_redo,
    encode_logminer_token,
)
from services.value_serializer import SQL_NULL_SENTINEL
from src.transfer.cdc_transfer import _records_to_matrix

_USER = os.getenv("DATAFLOW_TEST_ORACLE_USER", "")
_PW = os.getenv("DATAFLOW_TEST_ORACLE_PASSWORD", "")
_HOST = os.getenv("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1")
_SVC = os.getenv("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")


def _up() -> bool:
    if not (_USER and _PW):
        return False
    try:
        socket.create_connection((_HOST, 1521), timeout=3).close()
        return True
    except OSError:
        return False


def test_logminer_redo_null_literal_is_sql_null_not_empty():
    status, row = classify_sql_redo(
        'update "APP"."T" set "NAME" = NULL, "NOTE" = \' \', "AMT" = NULL '
        'where "ID" = \'7\' and "NAME" = \'x\'',
        op="update",
    )
    assert status == "ok", row
    matrix = _records_to_matrix([row], ["ID", "NAME", "NOTE", "AMT"])
    assert matrix == [["7", SQL_NULL_SENTINEL, " ", SQL_NULL_SENTINEL]]

    status, row = classify_sql_redo(
        'insert into "APP"."T"("ID","NAME","NOTE") values (\'8\',NULL,\'中文 😀\')',
        op="insert",
    )
    assert status == "ok", row
    assert _records_to_matrix([row], ["ID", "NAME", "NOTE"]) == [
        ["8", SQL_NULL_SENTINEL, "中文 😀"]
    ]


@pytest.mark.skipif(not _up(), reason="live Oracle not configured")
def test_logminer_snapshot_keeps_sql_null_distinct_from_text():
    import oracledb

    table = "D203O_" + uuid.uuid4().hex[:8].upper()
    conn = oracledb.connect(user=_USER, password=_PW, dsn=f"{_HOST}:1521/{_SVC}")
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(40), "
        "AMT NUMBER(10,2))"
    )
    try:
        cur.executemany(
            f"INSERT INTO {table} VALUES (:1, :2, :3)",  # nosec B608
            [(1, "a", 1.5), (2, None, None), (3, " ", 0), (4, "中文 😀", 2)],
        )
        conn.commit()
        reader = OracleLogMinerCdc(
            {"host": _HOST, "port": 1521, "database": _SVC, "service_name": _SVC,
             "username": _USER, "password": _PW},
            table=table,
            primary_key="ID",
            schema=_USER,
            resume_token=encode_logminer_token(1, table=table, phase="snapshot"),
        )
        records = []
        for batch in reader.snapshot():
            records.extend(batch.inserts or [])
        by_id = {
            row[0]: row
            for row in _records_to_matrix(records, ["ID", "NAME", "AMT"])
        }
        assert by_id["2"] == ["2", SQL_NULL_SENTINEL, SQL_NULL_SENTINEL]
        assert by_id["3"][1] == " "
        assert by_id["4"][1] == "中文 😀"
    finally:
        cur.execute(f"DROP TABLE {table} PURGE")
        conn.close()
