"""Oracle LogMiner transaction-boundary regressions."""

from __future__ import annotations

import time
import uuid
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token
from test_cdc_oracle_logminer_transfer_e2e import (
    _oracle_cfg,
    _oracle_logminer_ready,
)


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M1 red: Oracle long transaction is skipped after another poll advances its SCN",
)
def test_oracle_logminer_delivers_long_transaction_after_later_commit() -> None:
    from connectors.generic_sql import get_connection

    cfg = _oracle_cfg()
    schema = str(cfg["schema"])
    table = "CDC_TXN_" + uuid.uuid4().hex[:8].upper()
    table_ref = f'"{schema}"."{table}"'
    reader = OracleLogMinerCdc(
        cfg,
        table=table,
        primary_key="ID",
        schema=schema,
        batch_size=100,
        cursor_key=f"gcdc-r1:{table}",
    )
    table_created = False

    with ExitStack() as stack:
        session_a = stack.enter_context(
            get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            )
        )
        session_b = stack.enter_context(
            get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            )
        )
        try:
            with session_b.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table_ref} (ID NUMBER PRIMARY KEY, V VARCHAR2(32))"
                )
                table_created = True
                cur.execute(
                    f"ALTER TABLE {table_ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
            session_b.commit()
            list(reader.snapshot())

            with session_a.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (1, 'long')")

            with session_b.cursor() as cur:
                cur.execute("SELECT current_scn FROM v$database")
                long_dml_scn = int(cur.fetchone()[0])
            with session_b.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (2, 'short')")
            session_b.commit()

            delivered: set[str] = set()
            for _ in range(10):
                for batch in reader.poll():
                    delivered.update(str(row.get("ID")) for row in batch.inserts)
                if "2" in delivered:
                    break
                time.sleep(0.25)

            assert "2" in delivered, f"later committed row was not delivered: {delivered}"
            assert reader.scn >= long_dml_scn, (
                f"cursor did not pass the long transaction SCN: "
                f"cursor={reader.scn}, long_dml_scn={long_dml_scn}"
            )

            session_a.commit()
            for _ in range(8):
                for batch in reader.poll():
                    delivered.update(str(row.get("ID")) for row in batch.inserts)
                if "1" in delivered:
                    break
                time.sleep(0.25)

            assert "1" in delivered, f"long-transaction row was not delivered: {delivered}"
        finally:
            session_a.rollback()
            reader.close()
            if table_created:
                with session_b.cursor() as cur:
                    cur.execute(f"DROP TABLE {table_ref} PURGE")
                session_b.commit()


@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M1 red: Oracle single-table polling splits committed transactions across batches",
)
def test_oracle_single_table_poll_keeps_transactions_whole() -> None:
    rows = [
        (
            150,
            "0x000001.0001.0001",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'1\',\'t1-a\')',
            "T",
            "APP",
            1,
            0,
            1,
        ),
        (
            151,
            "0x000001.0001.0002",
            2,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'2\',\'t1-b\')',
            "T",
            "APP",
            1,
            0,
            1,
        ),
        (
            200,
            "0x000001.0001.0003",
            3,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'3\',\'t2-a\')',
            "T",
            "APP",
            1,
            0,
            2,
        ),
        (
            201,
            "0x000001.0001.0004",
            4,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'4\',\'t2-b\')',
            "T",
            "APP",
            1,
            0,
            2,
        ),
        (
            202,
            "0x000001.0001.0005",
            5,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'5\',\'t2-c\')',
            "T",
            "APP",
            1,
            0,
            2,
        ),
    ]
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
        batch_size=3,
    )
    cdc.phase = "streaming"
    cdc.scn = 100

    conn = MagicMock()
    cur = MagicMock()
    result_rows = []

    def execute(sql, params=None):
        if "V$LOGMNR_CONTENTS" in sql.upper():
            columns = [
                "SCN",
                "RS_ID",
                "SSN",
                "OPERATION",
                "SQL_REDO",
                "TABLE_NAME",
                "SEG_OWNER",
            ]
            if "XIDUSN" in sql.upper():
                columns.extend(["XIDUSN", "XIDSLT", "XIDSEQ"])
            cur.description = [(name,) for name in columns]
            bind = params or {}
            start = (
                int(bind.get("start_scn", 0)),
                str(bind.get("rs_id") or ""),
                int(bind.get("ssn", 0)),
            )
            visible = [
                row
                for row in rows
                if (int(row[0]), str(row[1]), int(row[2])) > start
            ]
            limit = int(bind.get("lim", 3))
            result_rows[:] = visible[:limit]

    def fetchall():
        return list(result_rows)

    cur.execute.side_effect = execute
    cur.fetchone.side_effect = [(300,), (300,)]
    cur.fetchall.side_effect = fetchall
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_mining_conn", return_value=conn), patch.object(
        cdc, "_acquire_cdc_lease"
    ), patch(
        "connectors.oracle_logminer.fetch_oldest_available_scn",
        return_value=None,
    ), patch(
        "connectors.oracle_logminer.assert_resume_scn_in_redo",
    ), patch(
        "connectors.oracle_logminer.start_logminer_session",
    ), patch(
        "services.cdc_incremental_runner.interleave_incremental_snapshot",
        return_value=iter(()),
    ):
        first = list(cdc.poll())
        second = list(cdc.poll())

    first_ids = [row["ID"] for batch in first for row in batch.inserts]
    second_ids = [row["ID"] for batch in second for row in batch.inserts]
    assert first_ids == ["1", "2"]
    assert second_ids == ["3", "4", "5"]
