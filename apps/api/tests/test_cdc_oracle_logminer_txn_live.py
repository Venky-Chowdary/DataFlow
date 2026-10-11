"""Oracle LogMiner transaction-boundary regressions."""

from __future__ import annotations

import time
import uuid
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token
from services.brand_env import getenv_brand
from test_cdc_oracle_logminer_transfer_e2e import (
    _oracle_cfg,
    _oracle_logminer_ready,
)


def _pg_cfg() -> dict[str, object]:
    return {
        "host": getenv_brand("PG_HOST", "localhost") or "localhost",
        "port": int(getenv_brand("PG_PORT", "5433") or 5433),
        "database": getenv_brand("PG_DB", "dataflow") or "dataflow",
        "username": getenv_brand("PG_USER", "postgres") or "postgres",
        "password": getenv_brand("PG_PASSWORD", "postgres") or "postgres",
        "connection_string": "",
        "ssl": False,
    }


def _pg_ready() -> bool:
    cfg = _pg_cfg()
    try:
        import socket

        with socket.create_connection((str(cfg["host"]), int(cfg["port"])), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
@pytest.mark.parametrize(
    "txn_buffer",
    [
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                strict=True,
                reason="G-CDC M1 legacy: long transaction is skipped after another poll advances its SCN",
            ),
            id="legacy-buffer-off",
        ),
        pytest.param(True, id="transaction-buffer-on"),
    ],
)
def test_oracle_logminer_delivers_long_transaction_after_later_commit(
    txn_buffer: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors.generic_sql import get_connection

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
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


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
def test_oracle_shared_logminer_poll_selects_xidsqn_live() -> None:
    from connectors.generic_sql import get_connection

    cfg = _oracle_cfg()
    schema = str(cfg["schema"])
    suffix = uuid.uuid4().hex[:8].upper()
    tables = [f"GCDC_XID_A_{suffix}", f"GCDC_XID_B_{suffix}"]
    refs = [f'"{schema}"."{table}"' for table in tables]
    created: list[str] = []
    reader = OracleLogMinerCdc(
        cfg,
        table=tables,
        primary_key="ID",
        primary_keys={table: "ID" for table in tables},
        schema=schema,
        batch_size=50,
        cursor_key=f"gcdc-xid-column:{suffix}",
    )

    try:
        with get_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                for ref in refs:
                    cur.execute(
                        f"CREATE TABLE {ref} (ID NUMBER PRIMARY KEY, V VARCHAR2(32))"
                    )
                    created.append(ref)
                    cur.execute(
                        f"ALTER TABLE {ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                    )
            conn.commit()

        list(reader.snapshot())

        with get_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {refs[0]} (ID, V) VALUES (1, 'a')")
                cur.execute(f"INSERT INTO {refs[1]} (ID, V) VALUES (2, 'b')")
            conn.commit()

        seen: dict[str, set[str]] = {table: set() for table in tables}
        expected = {tables[0]: {"1"}, tables[1]: {"2"}}
        for _ in range(8):
            for batch in reader.poll():
                if batch.table in seen:
                    seen[batch.table].update(
                        str(row["ID"]) for row in batch.inserts
                    )
            if all(seen[table] == rows for table, rows in expected.items()):
                break
            time.sleep(0.25)

        assert seen[tables[0]] == {"1"}
        assert seen[tables[1]] == {"2"}
    finally:
        reader.close()
        if created:
            with get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    for ref in reversed(created):
                        try:
                            cur.execute(f"DROP TABLE {ref} PURGE")
                        except Exception:
                            pass
                conn.commit()


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
def test_oracle_transaction_buffer_savepoint_and_full_rollback_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.generic_sql import get_connection

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    cfg = _oracle_cfg()
    schema = str(cfg["schema"])
    table = "CDC_RB_" + uuid.uuid4().hex[:8].upper()
    table_ref = f'"{schema}"."{table}"'
    reader = OracleLogMinerCdc(
        cfg,
        table=table,
        primary_key="ID",
        schema=schema,
        batch_size=100,
        cursor_key=f"gcdc-rollback:{table}",
    )
    created = False
    try:
        with get_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table_ref} (ID NUMBER PRIMARY KEY, V VARCHAR2(32))"
                )
                created = True
                cur.execute(
                    f"ALTER TABLE {table_ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
            conn.commit()
        list(reader.snapshot())

        with get_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (1, 'before')")
                cur.execute("SAVEPOINT GCDC_TXN_BUFFER_SP")
                cur.execute(
                    f"UPDATE {table_ref} SET V = 'temporary' WHERE ID = 1"
                )
                cur.execute("ROLLBACK TO GCDC_TXN_BUFFER_SP")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (2, 'discard')")
            conn.rollback()

        seen: dict[str, str] = {}
        for _ in range(10):
            for batch in reader.poll():
                for row in batch.inserts + batch.updates:
                    seen[str(row.get("ID"))] = str(row.get("V"))
                for key in batch.deletes:
                    seen.pop(str(key), None)
            if "1" in seen:
                break
            time.sleep(0.25)

        assert seen == {"1": "before"}
    finally:
        reader.close()
        if created:
            with get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE {table_ref} PURGE")
                conn.commit()


@pytest.mark.skipif(
    not _oracle_logminer_ready() or not _pg_ready(),
    reason="Oracle LogMiner or df-pg is not reachable",
)
def test_oracle_transaction_buffer_eos_restart_to_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restart the EOS runner while a source XID is open, then prove row-set equality."""
    from connectors.generic_sql import get_connection as oracle_connection
    from connectors.postgresql_conn import get_connection as pg_connection
    from services.cdc_exactly_once import WATERMARK_TABLE
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    cfg = _oracle_cfg()
    pg = _pg_cfg()
    schema = str(cfg["schema"])
    table = "CDC_EOS_TXN_" + uuid.uuid4().hex[:8].upper()
    dest_table = table.lower() + "_dest"
    table_ref = f'"{schema}"."{table}"'
    job_id = "oracle-txn-eos-" + uuid.uuid4().hex[:8]
    created = False

    def run_transfer():
        return run_cdc_database_transfer(
            src,
            dst,
            mappings,
            column_types,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=100,
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )

    src = EndpointConfig(
        kind="database",
        format="oracle",
        table=table,
        schema=schema,
        extra={
            "cdb_service": getenv_brand("ORACLE_CDB_SERVICE", "XE") or "XE",
            "logminer_username": getenv_brand("ORACLE_LOGMINER_USER", "C##DATAFLOW")
            or "C##DATAFLOW",
        },
        **{
            key: cfg[key]
            for key in (
                "host",
                "port",
                "database",
                "username",
                "password",
                "connection_string",
                "ssl",
            )
        },
    )
    dst = EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=dest_table,
        **{
            key: pg[key]
            for key in (
                "host",
                "port",
                "database",
                "username",
                "password",
                "connection_string",
                "ssl",
            )
        },
    )
    mappings = [
        {"source": "ID", "target": "id"},
        {"source": "V", "target": "v"},
    ]
    column_types = {"ID": "NUMERIC", "V": "VARCHAR(32)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "ID",
            "sync_mode": "cdc",
        }
    ]

    def source_rows() -> list[tuple[str, str]]:
        with oracle_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT ID, V FROM {table_ref} ORDER BY ID")
                return [(str(row[0]), str(row[1])) for row in cur.fetchall()]

    def dest_rows() -> list[tuple[str, str]]:
        with pg_connection(**pg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v FROM "public"."{dest_table}" ORDER BY id'
                )
                return [(str(row[0]), str(row[1])) for row in cur.fetchall()]

    try:
        with oracle_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table_ref} (ID NUMBER PRIMARY KEY, V VARCHAR2(32))"
                )
                created = True
                cur.execute(
                    f"ALTER TABLE {table_ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
            conn.commit()

        rows, _ddl, _summary, _extra = run_transfer()
        assert rows == 0

        with ExitStack() as stack:
            long_conn = stack.enter_context(
                oracle_connection(
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
            short_conn = stack.enter_context(
                oracle_connection(
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
            with long_conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (1, 'long')")
            with short_conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (2, 'short')")
            short_conn.commit()
            run_transfer()  # The runner exits with XID 1 open and a persisted low_scn.
            assert dest_rows() == [("2", "short")]
            run_transfer()  # Restart while XID 1 is still open.
            assert dest_rows() == [("2", "short")]
            long_conn.commit()

        with oracle_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (3, 'before')")
                cur.execute("SAVEPOINT GCDC_EOS_TXN_SP")
                cur.execute(f"UPDATE {table_ref} SET V = 'temporary' WHERE ID = 3")
                cur.execute("ROLLBACK TO GCDC_EOS_TXN_SP")
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (4, 'rolled-back')")
            conn.rollback()

        run_transfer()  # A new source reader is rebuilt from the destination watermark.
        actual = dest_rows()
        expected = source_rows()
        assert actual == expected == [("1", "long"), ("2", "short"), ("3", "before")]
        assert len(actual) == len({row[0] for row in actual})
    finally:
        try:
            with pg_connection(**pg) as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS "public"."{dest_table}"')
                    cur.execute(
                        f"DELETE FROM public.{WATERMARK_TABLE} WHERE dest_object = %s",
                        (dest_table,),
                    )
                conn.commit()
        except Exception:
            pass
        if created:
            with oracle_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE {table_ref} PURGE")
                conn.commit()


@pytest.mark.parametrize(
    "txn_buffer",
    [
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                strict=True,
                reason="G-CDC M1 legacy: single-table polling splits committed transactions across batches",
            ),
            id="legacy-buffer-off",
        ),
        pytest.param(True, id="transaction-buffer-on"),
    ],
)
def test_oracle_single_table_poll_keeps_transactions_whole(
    txn_buffer: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
    dml_rows = [
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
    txn_dml_rows = [(*row, 0) for row in dml_rows]
    lifecycle_rows = [
        (149, "0x000001.0001.0000", 0, "START", None, None, None, 1, 0, 1, 0),
        *txn_dml_rows[:2],
        (152, "0x000001.0001.0006", 6, "COMMIT", "commit", None, None, 1, 0, 1, 0),
        (199, "0x000001.0001.0007", 7, "START", None, None, None, 1, 0, 2, 0),
        *txn_dml_rows[2:],
        (203, "0x000001.0001.0009", 9, "COMMIT", "commit", None, None, 1, 0, 2, 0),
    ]
    rows = lifecycle_rows if txn_buffer else dml_rows
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
                columns.extend(["XIDUSN", "XIDSLT", "XIDSQN"])
            if '"ROLLBACK"' in sql.upper():
                columns.append("ROLLBACK")
            if "ROW_ID" in sql.upper():
                columns.append("ROW_ID")
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

    emitted = [
        [row["ID"] for row in batch.inserts]
        for batch in first + second
        if batch.inserts
    ]
    assert emitted[0] == ["1", "2"]
    assert all(ids == ["3", "4", "5"] for ids in emitted[1:])
