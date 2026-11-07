"""D250 (Oracle leg): once mirror to Oracle reached the buffered merge, the key
census failed with ORA-00904 "id": invalid identifier on
CREATE TABLE ... AS SELECT "id" FROM ... — the hand-built SQL quoted the
logical lower-case key while the table stores ID. The census and active-row
digest now address the catalog's own spelling.
"""
from __future__ import annotations

import os
import socket
import uuid

import pytest

from src.transfer.models import EndpointConfig

ORA = dict(
    host="127.0.0.1",
    port=1521,
    database=os.getenv("DATAFLOW_TEST_ORACLE_SERVICE") or os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_TEST_ORACLE_USER") or os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_TEST_ORACLE_PASSWORD") or os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(1521), reason="Oracle required")


def _ora(sql: str) -> list[tuple]:
    import oracledb

    conn = oracledb.connect(
        user=ORA["username"], password=ORA["password"],
        dsn=f"{ORA['host']}:{ORA['port']}/{ORA['database']}",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def test_mirror_census_on_folded_oracle_table_soft_deletes_the_missing_key():
    from services.mirror_engine import apply_inferred_soft_deletes

    table = f"D250_MIR_{uuid.uuid4().hex[:6].upper()}"
    _ora(f"CREATE TABLE {table} (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(40))")
    try:
        for i in (1, 2, 3):
            _ora(f"INSERT INTO {table} VALUES ({i}, 'n{i}')")
        records = [{"id": 1, "name": "n1"}, {"id": 2, "name": "n2"}]
        out = apply_inferred_soft_deletes(
            EndpointConfig(kind="database", format="oracle", table=table, **ORA),
            records,
            ["id", "name"],
            {"id": "INTEGER", "name": "TEXT"},
            [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            ["id"],
        )
        assert out["soft_deleted"] == 1
        assert out["active_rows"] == 2
        rows = _ora(f'SELECT ID, "_deleted" FROM {table} ORDER BY ID')
        assert [(int(r[0]), int(r[1] or 0)) for r in rows] == [(1, 0), (2, 0), (3, 1)]
    finally:
        _ora(f"DROP TABLE {table} PURGE")
