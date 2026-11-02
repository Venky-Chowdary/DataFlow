"""Live Oracle LogMiner DDL-to-schema-history regression coverage."""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services.cdc_schema_history import list_history
from services.cdc_mapping_review import list_reviews
from test_cdc_oracle_logminer_transfer_e2e import (
    _oracle_cfg,
    _oracle_logminer_ready,
)
from test_cdc_oracle_logminer_txn_live import _pg_cfg, _pg_ready


def _oracle_test_rs_id(position: int) -> str:
    return f" 0x000001.{position:08x}.0000 "


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
def test_oracle_numeric_rs_id_order_and_resume_live() -> None:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import (
        OracleLogMinerCdc,
        _RS_ID_ORDER_BY,
        _RS_ID_RESUME_PREDICATE,
        encode_logminer_token,
    )
    from connectors.oracle_logminer_txn import mining_position

    cfg = _oracle_cfg()
    rows_sql = """
        SELECT 100 AS SCN, ' 0xffffff.00000010.0010 ' AS RS_ID, 16 AS SSN
        FROM DUAL
        UNION ALL
        SELECT 100, ' 0x1000000.00000002.0010 ', 16 FROM DUAL
        UNION ALL
        SELECT 100, ' 0x1000000.00000003.0001 ', 16 FROM DUAL
        UNION ALL
        SELECT 100, ' 0x1000000.00000003.0002 ', 16 FROM DUAL
    """
    sql = f"""
        SELECT SCN, RS_ID, SSN
        FROM (
          SELECT SCN, RS_ID, SSN, ROWNUM AS DF_RN
          FROM ({rows_sql}) DF_CONTENTS
          WHERE {_RS_ID_RESUME_PREDICATE}
          {_RS_ID_ORDER_BY}
        )
    """

    def fetch(cursor, binds: dict) -> list[tuple]:
        cursor.execute(sql, binds)
        return list(cursor.fetchall() or [])

    reader = OracleLogMinerCdc(
        cfg,
        table="M6RSID_DUAL",
        primary_key="ID",
        schema=str(cfg["schema"]),
        resume_token=encode_logminer_token(
            99, table="M6RSID_DUAL", phase="streaming"
        ),
    )
    reader.scn = 99
    reader.rs_id = ""
    reader.ssn = 16
    reader.scn_complete = 0
    ordered_binds = reader._resume_binds()
    reader.scn = 100
    reader.rs_id = "0xffffff.00000010.0010"
    resume_binds = reader._resume_binds()

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
        with conn.cursor() as cursor:
            ordered = fetch(cursor, ordered_binds)
            resumed = fetch(cursor, resume_binds)

    assert resume_binds["rs_id"] == " 0xffffff.00000010.0010 "
    assert resume_binds["rs_len"] == len(resume_binds["rs_id"])
    assert [row[1] for row in ordered] == [
        " 0xffffff.00000010.0010 ",
        " 0x1000000.00000002.0010 ",
        " 0x1000000.00000003.0001 ",
        " 0x1000000.00000003.0002 ",
    ]
    assert [mining_position(row[0], row[1], row[2]) for row in ordered] == sorted(
        mining_position(row[0], row[1], row[2]) for row in ordered
    )
    assert [row[1] for row in resumed] == [
        " 0x1000000.00000002.0010 ",
        " 0x1000000.00000003.0001 ",
        " 0x1000000.00000003.0002 ",
    ]
    assert all(row[1] != " 0xffffff.00000010.0010 " for row in resumed)


def _use_oracle_mining_timezone_0930(
    monkeypatch: pytest.MonkeyPatch, cfg: dict
) -> dict[str, str]:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import OracleLogMinerCdc

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
            cur.execute("SELECT DBTIMEZONE FROM DUAL")
            db_timezone = str(cur.fetchone()[0])
    host_offset = time.strftime("%z")
    assert db_timezone != "+09:30"
    assert host_offset != "+0930"

    original_mining_conn = OracleLogMinerCdc._mining_conn

    def mining_conn_with_timezone(reader):
        conn = original_mining_conn(reader)
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET TIME_ZONE = '+09:30'")
            cur.execute("SELECT SESSIONTIMEZONE FROM DUAL")
            assert str(cur.fetchone()[0]) == "+09:30"
        return conn

    monkeypatch.setattr(
        OracleLogMinerCdc, "_mining_conn", mining_conn_with_timezone
    )
    return {"dbtimezone": db_timezone, "host_offset": host_offset}


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
def test_oracle_logminer_records_live_ddl_without_emitting_it_as_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import OracleLogMinerCdc
    from test_cdc_oracle_logminer_txn_live import (
        _ensure_oracle_gv_instance_readable,
        _oracle_v_transaction_grant_count,
        _oracle_v_transaction_grant_user,
        _set_oracle_gv_instance_grant,
        _set_oracle_v_transaction_grant,
    )

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    cfg = _oracle_cfg()
    timezone_probe = _use_oracle_mining_timezone_0930(monkeypatch, cfg)
    assert timezone_probe["dbtimezone"] != "+09:30"
    assert timezone_probe["host_offset"] != "+0930"
    schema = str(cfg["schema"])
    table = "CDC_DDL_" + uuid.uuid4().hex[:8].upper()
    table_ref = f'"{schema}"."{table}"'
    qualified = f"{schema}.{table}"
    reader = OracleLogMinerCdc(
        cfg,
        table=table,
        primary_key="ID",
        schema=schema,
        cursor_key=f"o6-ddl:{table}",
    )
    cdc_user = _oracle_v_transaction_grant_user(cfg)
    created = False
    transaction_probe_granted = False
    instance_probe_granted = False
    try:
        transaction_probe_granted = _oracle_v_transaction_grant_count(cdc_user) == 0
        if transaction_probe_granted:
            _set_oracle_v_transaction_grant(cdc_user, enabled=True)
        instance_probe_granted = _ensure_oracle_gv_instance_readable(cfg)
        delivered: dict[str, dict] = {}

        def collect_poll() -> None:
            for batch in reader.poll():
                for row in batch.inserts + batch.updates:
                    delivered[str(row.get("ID"))] = row

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
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (1, 'r1')")
            conn.commit()

        for _ in range(12):
            collect_poll()
            if "1" in delivered:
                break
            time.sleep(0.25)
        assert "1" in delivered, delivered

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
                cur.execute(f"ALTER TABLE {table_ref} ADD (NOTE VARCHAR2(50))")
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, V, NOTE) "
                    "VALUES (2, 'r2', 'after-ddl')"
                )
            conn.commit()

        for _ in range(12):
            collect_poll()
            if {"1", "2"} <= delivered.keys():
                break
            time.sleep(0.25)

        assert {"1", "2"} <= delivered.keys(), delivered
        assert delivered["2"].get("NOTE") == "after-ddl", delivered
        assert set(delivered) == {"1", "2"}, delivered

        history = list_history(reader.source_key, qualified)
        assert len(history) == 1, history
        assert "ADD" in history[0]["ddl"].upper(), history
        assert any(
            str(column.get("name") or "").upper() == "NOTE"
            for column in (
                (history[0].get("schema_snapshot") or {}).get("columns") or []
            )
        ), history
        reviews = list_reviews(source_key=reader.source_key, status="all")
        assert any(
            item.get("table") == qualified
            and item.get("reason") == "cdc_schema_drift"
            for item in reviews
        ), reviews
        assert reader.last_ddl_at
    finally:
        reader.close()
        try:
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
        finally:
            try:
                if transaction_probe_granted:
                    _set_oracle_v_transaction_grant(cdc_user, enabled=False)
            finally:
                if instance_probe_granted:
                    _set_oracle_gv_instance_grant(cdc_user, enabled=False)


@pytest.mark.skipif(
    not _oracle_logminer_ready() or not _pg_ready(),
    reason="Oracle LogMiner or df-pg is not reachable",
)
@pytest.mark.parametrize("txn_buffer", [False, True], ids=["buffer-off", "buffer-on"])
def test_oracle_logminer_ddl_eos_restart_has_no_loss_or_duplicate(
    txn_buffer: bool, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from connectors.generic_sql import get_connection as oracle_connection
    from connectors.postgresql_conn import get_connection as pg_connection
    from services.cdc_exactly_once import WATERMARK_TABLE
    from services.brand_env import getenv_brand
    from services.cdc_mapping_review import acknowledge_review
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig
    from test_cdc_oracle_logminer_txn_live import (
        _ensure_oracle_gv_instance_readable,
        _oracle_v_transaction_grant_count,
        _oracle_v_transaction_grant_user,
        _set_oracle_gv_instance_grant,
        _set_oracle_v_transaction_grant,
    )

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
    monkeypatch.setenv(
        "CDC_MAPPING_REVIEW_STORE", str(tmp_path / "cdc_mapping_reviews.jsonl")
    )
    cfg = _oracle_cfg()
    timezone_probe = _use_oracle_mining_timezone_0930(monkeypatch, cfg)
    assert timezone_probe["dbtimezone"] != "+09:30"
    assert timezone_probe["host_offset"] != "+0930"
    pg = _pg_cfg()
    schema = str(cfg["schema"])
    table = "CDC_DDL_EOS_" + uuid.uuid4().hex[:8].upper()
    dest_table = table.lower() + "_dest"
    table_ref = f'"{schema}"."{table}"'
    qualified = f"{schema}.{table}"
    job_id = f"oracle-ddl-eos-{uuid.uuid4().hex[:8]}"
    created = False
    cdc_user = _oracle_v_transaction_grant_user(cfg)
    transaction_probe_granted = False
    instance_probe_granted = False

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
    reader = None

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

    def source_rows() -> list[tuple[str, str, str | None]]:
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
                cur.execute(f"SELECT ID, V, NOTE FROM {table_ref} ORDER BY ID")
                return [
                    (str(row[0]), str(row[1]), None if row[2] is None else str(row[2]))
                    for row in cur.fetchall()
                ]

    def dest_rows() -> list[tuple[str, str, str | None]]:
        with pg_connection(**pg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, v, note FROM "public"."{dest_table}" ORDER BY id'
                )
                return [
                    (str(row[0]), str(row[1]), None if row[2] is None else str(row[2]))
                    for row in cur.fetchall()
                ]

    try:
        if not txn_buffer:
            transaction_probe_granted = (
                _oracle_v_transaction_grant_count(cdc_user) == 0
            )
            if transaction_probe_granted:
                _set_oracle_v_transaction_grant(cdc_user, enabled=True)
            instance_probe_granted = _ensure_oracle_gv_instance_readable(cfg)
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

        run_transfer()
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
                cur.execute(f"INSERT INTO {table_ref} (ID, V) VALUES (1, 'r1')")
            conn.commit()
        run_transfer()

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
                cur.execute("SELECT current_scn FROM V$DATABASE")
                first_ddl_start_scn = int(cur.fetchone()[0])
                cur.execute(f"ALTER TABLE {table_ref} ADD (NOTE VARCHAR2(50))")
            conn.commit()
        time.sleep(2.1)
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
                cur.execute(f"GRANT SELECT ON {table_ref} TO PUBLIC")
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, V, NOTE) "
                    "VALUES (2, 'r2', 'after-ddl')"
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM V$DATABASE")
                first_ddl_end_scn = int(cur.fetchone()[0])

        with pg_connection(**pg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "public"."{dest_table}" ADD COLUMN note VARCHAR(50)'
                )
            conn.commit()
        mappings.append({"source": "NOTE", "target": "note"})
        column_types["NOTE"] = "VARCHAR(50)"
        from connectors.oracle_logminer import OracleLogMinerCdc, OracleLogMinerReadError

        reader = OracleLogMinerCdc(
            cfg,
            table=table,
            primary_key="ID",
            schema=schema,
            cursor_key=f"o6-ddl-eos:{table}",
        )

        with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
            run_transfer()
        first_reviews = [
            item
            for item in list_reviews(
                source_key=reader.source_key, status="all", limit=1000
            )
            if item.get("table") == qualified
            and item.get("reason") == "cdc_schema_drift"
        ]
        assert len(first_reviews) == 1
        assert first_reviews[0]["status"] == "open"
        assert first_reviews[0]["ddl"].startswith("[oracle-ddl@scn=")
        first_review_scn = int(
            first_reviews[0]["ddl"].split("@scn=", 1)[1].split(",", 1)[0]
        )
        from connectors.oracle_logminer import _logminer_session

        with reader._mining_conn() as conn:
            with conn.cursor() as cur:
                with _logminer_session(
                    cur,
                    start_scn=first_ddl_start_scn,
                    end_scn=first_ddl_end_scn,
                    reader_identity=f"test table={qualified}",
                    step_context="test_ddl_barrier_scn",
                ):
                    cur.execute(
                        """
                        SELECT SCN, SQL_REDO
                        FROM V$LOGMNR_CONTENTS
                        WHERE OPERATION = 'DDL'
                          AND SCN BETWEEN :start_scn AND :end_scn
                        ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN
                        """,
                        {
                            "start_scn": first_ddl_start_scn,
                            "end_scn": first_ddl_end_scn,
                        },
                    )
                    logminer_ddl_rows = cur.fetchall() or []
        matching_ddl_scns = []
        for scn, sql_redo in logminer_ddl_rows:
            if hasattr(sql_redo, "read") and callable(sql_redo.read):
                sql_redo = sql_redo.read()
            sql_redo_text = str(sql_redo or "").upper()
            if (
                "ALTER TABLE" in sql_redo_text
                and table.upper() in sql_redo_text
                and "NOTE" in sql_redo_text
            ):
                matching_ddl_scns.append(int(scn))
        assert first_review_scn in matching_ddl_scns, {
            "review_scn": first_review_scn,
            "matching_v_logmnr_ddl_scns": matching_ddl_scns,
            "logminer_ddl_rows": logminer_ddl_rows,
        }
        assert {row[0] for row in source_rows()} == {"1", "2"}
        assert dest_rows() == [("1", "r1", None)]

        with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
            run_transfer()
        assert len(
            [
                item
                for item in list_reviews(
                    source_key=reader.source_key, status="all", limit=1000
                )
                if item.get("table") == qualified
                and item.get("reason") == "cdc_schema_drift"
            ]
        ) == 1
        assert acknowledge_review(first_reviews[0]["id"])["status"] == "acknowledged"

        run_transfer()
        run_transfer()
        actual = dest_rows()
        expected = source_rows()
        assert actual == expected == [
            ("1", "r1", None),
            ("2", "r2", "after-ddl"),
        ]
        assert len(actual) == len({row[0] for row in actual})
        assert len(list_history(reader.source_key, qualified)) == 1

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
                cur.execute(f"ALTER TABLE {table_ref} ADD (NOTE2 VARCHAR2(50))")
            conn.commit()
        time.sleep(2.1)
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
                cur.execute(f"GRANT SELECT ON {table_ref} TO PUBLIC")
                cur.execute(
                    f"INSERT INTO {table_ref} (ID, V, NOTE, NOTE2) "
                    "VALUES (3, 'r3', 'after-ddl-2', 'new')"
                )
            conn.commit()
        with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
            run_transfer()

        all_reviews = [
            item
            for item in list_reviews(
                source_key=reader.source_key, status="all", limit=1000
            )
            if item.get("table") == qualified
            and item.get("reason") == "cdc_schema_drift"
        ]
        assert len(all_reviews) == 2, all_reviews
        assert sum(item["status"] == "open" for item in all_reviews) == 1
        assert sum(item["status"] == "acknowledged" for item in all_reviews) == 1
        assert all_reviews[0]["ddl"].split("] [", 1)[0] != all_reviews[1]["ddl"].split(
            "] [", 1
        )[0]
        assert dest_rows() == [
            row for row in source_rows() if row[0] in {"1", "2"}
        ]
        assert "3" not in {row[0] for row in dest_rows()}

        history = list_history(reader.source_key, qualified)
        assert len(history) == 1, history
        assert "ADD" in history[0]["ddl"].upper(), history
    finally:
        if reader is not None:
            reader.close()
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
        if transaction_probe_granted:
            _set_oracle_v_transaction_grant(cdc_user, enabled=False)
        if instance_probe_granted:
            _set_oracle_gv_instance_grant(cdc_user, enabled=False)


def _unit_reader(*, table: str | list[str] = "T1", resume_token=None):
    from connectors.oracle_logminer import OracleLogMinerCdc

    tables = [table] if isinstance(table, str) else table
    reader = OracleLogMinerCdc(
        {
            "host": "localhost",
            "port": 1521,
            "database": "FREEPDB1",
            "username": "DATAFLOW",
            "password": "unit-test",
            "connector_id": "o6-unit",
        },
        table=table,
        primary_key="ID" if isinstance(table, str) else "",
        primary_keys=None if isinstance(table, str) else {name: "ID" for name in tables},
        schema="DATAFLOW",
        resume_token=resume_token,
        cursor_key="o6-unit:" + ",".join(tables),
    )
    reader._probe_oldest_open_txn = lambda _cur=None: None
    return reader


def test_oracle_ddl_ambiguity_uses_numeric_rs_id_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import connectors.oracle_logminer as logminer

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 100
    reader._open_txn_detection_checked = True
    monkeypatch.setattr(reader, "_buffer_off_window_end", lambda: (101, False))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(
        "services.cdc_incremental_runner.interleave_incremental_snapshot",
        lambda *_args, **_kwargs: iter(()),
    )
    monkeypatch.setattr(
        logminer, "fetch_oldest_available_scn", lambda _cur: 1
    )
    monkeypatch.setattr(logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(
        logminer, "assert_resume_scn_in_redo", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        logminer, "assert_redo_continuity", lambda *_args, **_kwargs: None
    )

    earlier_rs_id = " 0xffffff.00000010.0010 "
    later_rs_id = " 0x1000000.00000002.0010 "
    rows = [
        (
            100,
            earlier_rs_id,
            16,
            "DDL",
            "ALTER TABLE T1 MODIFY A NUMBER",
            "T1",
            "DATAFLOW",
            0,
            0,
            0,
            "T1",
        ),
        (
            100,
            later_rs_id,
            16,
            "DDL",
            "ALTER TABLE T1 MODIFY B NUMBER",
            "T1",
            "DATAFLOW",
            0,
            0,
            0,
            "T1",
        ),
    ]
    monkeypatch.setattr(
        reader,
        "_fetch_logminer_rows_visible",
        lambda *_args, **_kwargs: (rows, 101),
    )
    recorded: list[dict] = []
    monkeypatch.setattr(
        reader, "_record_oracle_ddl", lambda **kwargs: recorded.append(kwargs)
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (101,)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(reader, "_mining_conn", lambda: Connection())
    list(reader._poll_once())

    assert len(recorded) == 2
    assert recorded[0]["offset"]["rs_id"] == earlier_rs_id
    assert recorded[0]["ambiguous_later_ddl"] is True


def test_oracle_ddl_queries_include_only_table_scoped_ddl() -> None:
    from connectors.oracle_logminer import (
        logminer_contents_sql,
        logminer_txn_contents_sql,
    )

    legacy = logminer_contents_sql(
        table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl"
    )
    buffered = logminer_txn_contents_sql(
        table_predicate="TABLE_NAME IN ('T1') OR SEG_NAME IN ('T1')"
    )
    assert "SEG_OWNER = :owner" in legacy
    assert "OPERATION = 'DDL'" in legacy
    assert "SEG_NAME" in legacy
    assert "SEG_OWNER = :owner" in buffered
    assert "OPERATION = 'DDL'" in buffered
    assert "OPERATION IN ('START','COMMIT','ROLLBACK')" in buffered


def test_oracle_ddl_ignores_untracked_tables_and_dedupes_after_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import cdc_mapping_review, cdc_schema_history

    stored: list[dict] = []
    reviews: list[dict] = []
    snapshots: list[str] = []

    def list_entries(source_key: str, table: str) -> list[dict]:
        return [
            entry
            for entry in stored
            if entry["source_key"] == source_key and entry["table"] == table
        ]

    def record(
        source_key: str, table: str, ddl: str, offset, schema_snapshot: dict
    ) -> dict:
        entry = {
            "source_key": source_key,
            "table": table,
            "ddl": ddl,
            "offset": offset,
            "schema_snapshot": schema_snapshot,
            "version": 1,
            "recorded_at": "2026-10-12T00:00:00Z",
        }
        stored.append(entry)
        return entry

    monkeypatch.setattr(cdc_schema_history, "list_history", list_entries)
    monkeypatch.setattr(cdc_schema_history, "record_ddl", record)
    monkeypatch.setattr(
        cdc_schema_history, "last_ddl_at", lambda _source, _table: stored[-1]["recorded_at"]
    )
    monkeypatch.setattr(
        cdc_mapping_review,
        "flag_mapping_review",
        lambda **signal: reviews.append(signal) or signal,
    )

    reader = _unit_reader(table=["T1", "T2"])
    snapshot = {
        "columns": [
            {"name": "ID", "type": "NUMBER", "nullable": False, "ordinal": 1},
            {"name": "NOTE", "type": "VARCHAR2(50 BYTE)", "nullable": True, "ordinal": 2},
        ]
    }
    monkeypatch.setattr(
        reader,
        "_oracle_ddl_schema_snapshot",
        lambda table: snapshots.append(table) or snapshot,
    )
    reader._record_oracle_ddl(
        owner="DATAFLOW",
        table="UNTRACKED",
        ddl="ALTER TABLE UNTRACKED ADD NOTE VARCHAR2(50)",
        offset={"scn": 10, "rs_id": "r1", "ssn": 0},
    )
    assert not snapshots
    assert not stored

    ddl = "ALTER TABLE DATAFLOW.T2 ADD (NOTE VARCHAR2(50))"
    offset = {"scn": 20, "rs_id": "r2", "ssn": 1}
    reader._record_oracle_ddl(
        owner="DATAFLOW", table="T2", ddl=ddl, offset=offset
    )
    assert len(stored) == 1
    assert stored[0]["table"] == "DATAFLOW.T2"
    assert stored[0]["offset"] == offset
    assert reviews[0]["reason"] == "cdc_schema_drift"

    restarted = _unit_reader(table=["T1", "T2"])
    monkeypatch.setattr(restarted, "_oracle_ddl_schema_snapshot", lambda _table: snapshot)
    restarted._record_oracle_ddl(
        owner="DATAFLOW", table="T2", ddl=ddl, offset=offset
    )
    assert len(stored) == 1
    assert len(reviews) == 1
    assert restarted.last_ddl_at == stored[0]["recorded_at"]


def test_oracle_shared_reader_records_ddl_for_each_tracked_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import connectors.oracle_logminer as logminer
    from services import cdc_mapping_review, cdc_schema_history

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    reader = _unit_reader(table=["T1", "T2"])
    reader.phase = "streaming"
    reader.scn = 1
    reader._open_txn_detection_checked = True
    monkeypatch.setattr(reader, "_buffer_off_window_end", lambda: (100, False))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(logminer, "assert_resume_scn_in_redo", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(logminer, "assert_redo_continuity", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(logminer, "start_logminer_session", lambda *_a, **_k: None)
    requested: list[str] = []
    monkeypatch.setattr(
        reader,
        "_oracle_ddl_schema_snapshot",
        lambda table: requested.append(table)
        or {"columns": [{"name": "ID"}, {"name": "NOTE"}]},
    )
    recorded: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_a: [])
    monkeypatch.setattr(
        cdc_schema_history,
        "record_ddl",
        lambda source, table, ddl, offset, schema_snapshot: recorded.append(
            (table, ddl, offset)
        )
        or {
            "version": 1,
            "recorded_at": "2026-10-12T00:00:00Z",
            "ddl": ddl,
        },
    )
    monkeypatch.setattr(
        cdc_mapping_review, "flag_mapping_review", lambda **_signal: None
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (100,)

        def fetchall(self):
            return [
                (
                    5,
                    _oracle_test_rs_id(5),
                    0,
                    "DDL",
                    "ALTER TABLE T2 ADD NOTE VARCHAR2(50)",
                    "T2",
                    "DATAFLOW",
                    0,
                    0,
                    0,
                    "T2",
                )
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(reader, "_mining_conn", lambda: Connection())
    batches = list(reader._poll_shared_multi())
    assert requested == ["T2"]
    assert recorded[0][0] == "DATAFLOW.T2"
    assert recorded[0][1] == "ALTER TABLE T2 ADD NOTE VARCHAR2(50)"
    assert recorded[0][2] == {
        "scn": 5,
        "rs_id": _oracle_test_rs_id(5),
        "ssn": 0,
    }
    assert len(batches) == 1
    assert not batches[0].inserts
    assert batches[0].ack_barrier


def test_oracle_buffered_ddl_is_an_ordering_barrier() -> None:
    from connectors.oracle_logminer import _ordered_txn_ddl_actions
    from connectors.oracle_logminer_txn import OracleTxnBuffer, mining_position

    buffer = OracleTxnBuffer(max_txns=4, max_bytes=10000)
    committed = []
    for xid, first_scn, commit_position in (
        ("before", 5, 10),
        ("after", 26, 30),
    ):
        buffer.feed(
            xid=xid,
            scn=first_scn,
            operation="INSERT",
            table="T1",
            row={"ID": xid},
        )
        committed.extend(
            buffer.feed(
                xid=xid,
                scn=20,
                rs_id=_oracle_test_rs_id(commit_position),
                ssn=1,
                operation="COMMIT",
            )
        )
    ddl_event = (
        20,
        _oracle_test_rs_id(20),
        1,
        "DATAFLOW",
        "T1",
        "ALTER TABLE T1 ADD NOTE",
    )

    actions = _ordered_txn_ddl_actions(committed, [ddl_event])
    assert [action[2] for action in actions] == ["txn", "ddl", "txn"]
    assert [action[0] for action in actions] == [
        mining_position(20, _oracle_test_rs_id(10), 1),
        mining_position(20, _oracle_test_rs_id(20), 1),
        mining_position(20, _oracle_test_rs_id(30), 1),
    ]


def test_oracle_buffered_poll_emits_commits_around_ddl_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import connectors.oracle_logminer as logminer
    from connectors.oracle_logminer import decode_logminer_token
    from services import cdc_mapping_review, cdc_schema_history

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    reader._lob_column_cache["T1"] = ()
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(logminer, "assert_resume_scn_in_redo", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(logminer, "assert_redo_continuity", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(logminer, "start_logminer_session", lambda *_a, **_k: None)
    monkeypatch.setattr(
        logminer,
        "classify_sql_redo",
        lambda sql, **_kwargs: ("ok", {"ID": sql[-1], "V": sql}),
    )
    monkeypatch.setattr(
        reader,
        "_oracle_ddl_schema_snapshot",
        lambda _table: {"columns": [{"name": "ID"}, {"name": "NOTE"}]},
    )

    history: list[dict] = []
    monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_a: history)

    def record(source_key, table, ddl, offset, schema_snapshot):
        entry = {
            "source_key": source_key,
            "table": table,
            "ddl": ddl,
            "offset": offset,
            "schema_snapshot": schema_snapshot,
            "version": 1,
            "recorded_at": "2026-10-12T00:00:00Z",
        }
        history.append(entry)
        return entry

    monkeypatch.setattr(cdc_schema_history, "record_ddl", record)
    monkeypatch.setattr(
        cdc_schema_history, "last_ddl_at", lambda *_args: history[-1]["recorded_at"]
    )
    monkeypatch.setattr(
        cdc_mapping_review,
        "flag_mapping_review",
        lambda **signal: signal,
    )

    def row(scn, rs_id, operation, sql_redo, table, xid, row_id=""):
        return (
            scn,
            rs_id,
            0,
            operation,
            sql_redo,
            table,
            "DATAFLOW" if table else None,
            *xid,
            0,
            row_id,
            table,
        )

    raw_rows = [
        row(5, _oracle_test_rs_id(5), "START", None, None, (1, 1, 1)),
        row(
            10,
            _oracle_test_rs_id(10),
            "INSERT",
            "row1",
            "T1",
            (1, 1, 1),
            "ROW1",
        ),
        row(20, _oracle_test_rs_id(10), "COMMIT", None, None, (1, 1, 1)),
        row(
            20,
            _oracle_test_rs_id(20),
            "DDL",
            "ALTER TABLE T1 ADD NOTE VARCHAR2(50)",
            "T1",
            (0, 0, 0),
            "T1",
        ),
        row(20, _oracle_test_rs_id(21), "START", None, None, (2, 2, 2)),
        row(
            20,
            _oracle_test_rs_id(25),
            "INSERT",
            "row2",
            "T1",
            (2, 2, 2),
            "ROW2",
        ),
        row(20, _oracle_test_rs_id(30), "COMMIT", None, None, (2, 2, 2)),
    ]

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (100,)

        def fetchall(self):
            return raw_rows

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(reader, "_mining_conn", lambda: Connection())
    batches = list(reader._poll_txn_buffered())
    tokens = [decode_logminer_token(batch.resume_token) for batch in batches]
    positions = [(token["scn"], token["rs_id"], token["ssn"]) for token in tokens]
    assert positions == [
        (20, _oracle_test_rs_id(10), 0),
        (20, _oracle_test_rs_id(20), 0),
        (20, _oracle_test_rs_id(30), 0),
    ]
    assert [batch.inserts[0]["ID"] for batch in batches if batch.inserts] == [
        "1",
        "2",
    ]
    assert len(history) == 1
    assert "DDL" not in repr([batch.inserts for batch in batches])


def _install_buffered_ddl_window(
    monkeypatch: pytest.MonkeyPatch,
    reader,
    *,
    additional_ddl: bool = False,
    ddl_timestamp: datetime | None = None,
):
    import connectors.oracle_logminer as logminer
    from services import cdc_mapping_review, cdc_schema_history

    monkeypatch.setattr(logminer, "assert_resume_scn_in_redo", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(logminer, "assert_redo_continuity", lambda *_a, **_k: None)
    monkeypatch.setattr(logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(logminer, "start_logminer_session", lambda *_a, **_k: None)
    monkeypatch.setattr(
        logminer,
        "classify_sql_redo",
        lambda sql, **_kwargs: ("ok", {"ID": sql[-1], "V": sql}),
    )
    monkeypatch.setattr(
        reader,
        "_oracle_ddl_schema_snapshot",
        lambda _table, **_kwargs: {"columns": [{"name": "ID"}, {"name": "NOTE"}]},
    )

    history: list[dict] = []
    reviews: list[dict] = []
    calls: list[dict] = []
    monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_a: history)
    monkeypatch.setattr(
        cdc_schema_history,
        "last_ddl_at",
        lambda *_a: history[-1]["recorded_at"] if history else None,
    )

    def record(source_key, table, ddl, offset, schema_snapshot):
        entry = {
            "source_key": source_key,
            "table": table,
            "ddl": ddl,
            "offset": offset,
            "schema_snapshot": schema_snapshot,
            "version": len(history) + 1,
            "recorded_at": "2026-10-12T00:00:00Z",
        }
        calls.append(entry)
        history.append(entry)
        return entry

    monkeypatch.setattr(cdc_schema_history, "record_ddl", record)
    monkeypatch.setattr(
        cdc_mapping_review,
        "flag_mapping_review",
        lambda **signal: reviews.append(signal),
    )
    monkeypatch.setattr(
        cdc_mapping_review,
        "list_reviews",
        lambda **_params: [],
    )

    def row(scn, position, operation, sql_redo, table, xid, timestamp=None):
        return (
            scn,
            _oracle_test_rs_id(position),
            0,
            operation,
            sql_redo,
            table,
            "DATAFLOW" if table else None,
            *xid,
            0,
            f"ROW{position}",
            table,
            timestamp,
        )

    rows = [
        row(5, 5, "START", None, None, (1, 1, 1)),
        row(10, 10, "INSERT", "row1", "T1", (1, 1, 1)),
        row(20, 10, "COMMIT", None, None, (1, 1, 1)),
        row(
            20,
            20,
            "DDL",
            "ALTER TABLE T1 ADD NOTE",
            "T1",
            (0, 0, 0),
            ddl_timestamp,
        ),
        row(20, 21, "START", None, None, (2, 2, 2)),
        row(20, 25, "INSERT", "row2", "T1", (2, 2, 2)),
        row(30, 30, "COMMIT", None, None, (2, 2, 2)),
    ]
    if additional_ddl:
        rows.append(
            row(20, 22, "DDL", "ALTER TABLE T1 ADD NOTE2", "T1", (0, 0, 0))
        )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return (100,)

        def fetchall(self):
            return rows

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    connection_factory = lambda: Connection()
    monkeypatch.setattr(reader, "_mining_conn", connection_factory)
    return history, reviews, calls, connection_factory


@pytest.mark.parametrize("ambiguity", ["later-ddl", "catalog-time"])
def test_oracle_ambiguous_ddl_fails_closed_without_history(
    monkeypatch: pytest.MonkeyPatch,
    ambiguity: str,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from connectors.oracle_logminer_txn import mining_position

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    reader._lob_column_cache["T1"] = ()
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch,
        reader,
        additional_ddl=ambiguity == "later-ddl",
        ddl_timestamp=(
            datetime(2026, 10, 12, 0, 0, 0)
            if ambiguity == "catalog-time"
            else None
        ),
    )
    if ambiguity == "catalog-time":
        monkeypatch.setattr(
            reader,
            "_oracle_ddl_timing_delta_seconds",
            lambda *_a: 2.0,
        )

    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
        next(poll)

    assert history == []
    assert len(reviews) == 1
    assert reviews[0]["reason"] == "cdc_schema_drift"
    assert "ambiguous schema as of DDL SCN" in reviews[0]["ddl"]
    assert mining_position(reader.scn, reader.rs_id, reader.ssn) < mining_position(
        20, _oracle_test_rs_id(20), 0
    )


@pytest.mark.parametrize(
    ("delta_seconds", "should_pause"),
    [(0.0, False), (1.0, False), (1.5, True), (None, True)],
)
def test_oracle_ddl_database_clock_delta_boundary(
    monkeypatch: pytest.MonkeyPatch,
    delta_seconds: float | None,
    should_pause: bool,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch,
        reader,
        ddl_timestamp=datetime(2026, 10, 12, 0, 0, 0),
    )
    monkeypatch.setattr(
        reader,
        "_oracle_ddl_timing_delta_seconds",
        lambda *_args: delta_seconds,
    )
    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)

    if should_pause:
        expected = (
            "DDL timing could not be verified"
            if delta_seconds is None
            else "ambiguous schema as of DDL SCN"
        )
        with pytest.raises(OracleLogMinerReadError, match=expected):
            list(poll)
        assert history == []
        assert len(reviews) == 1
    else:
        batches = list(poll)
        assert [row["ID"] for batch in batches for row in batch.inserts] == ["2"]
        assert len(history) == 1


@pytest.mark.parametrize(
    ("delta_seconds", "should_pause"),
    [(0.0, False), (2.0, True)],
)
def test_oracle_ddl_database_clock_delta_ignores_host_timezone(
    monkeypatch: pytest.MonkeyPatch,
    delta_seconds: float,
    should_pause: bool,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_mapping_review, cdc_schema_history

    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is unavailable")
    original_timezone = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    try:
        assert time.strftime("%z") != "+0530"
        reader = _unit_reader()
        server_utc_timestamp = datetime(
            2026, 10, 11, 7, 28, 11, tzinfo=timezone.utc
        )
        server_redo_date = server_utc_timestamp + timedelta(hours=5, minutes=30)
        redo_timestamp = server_redo_date.replace(tzinfo=None)
        catalog_timestamp = redo_timestamp + timedelta(seconds=delta_seconds)
        assert redo_timestamp.tzinfo is None
        assert catalog_timestamp.tzinfo is None
        executed: list[tuple[str, dict]] = []
        history: list[dict] = []
        reviews: list[dict] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def execute(self, statement, binds):
                sql = " ".join(statement.split())
                assert (
                    "SELECT (o.LAST_DDL_TIME - CAST(:redo_ts AS DATE)) * 86400"
                    in sql
                )
                assert binds["redo_ts"] is redo_timestamp
                executed.append((sql, binds))
                self.delta = (catalog_timestamp - redo_timestamp).total_seconds()

            def fetchone(self):
                return (self.delta,)

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def cursor(self):
                return Cursor()

        monkeypatch.setattr(reader, "_conn", lambda: Connection())
        monkeypatch.setattr(
            reader,
            "_oracle_ddl_schema_snapshot",
            lambda _table: {"columns": [{"name": "ID"}]},
        )
        monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_args: [])
        monkeypatch.setattr(
            cdc_schema_history,
            "record_ddl",
            lambda source, table, ddl, offset, schema_snapshot: history.append(
                {"table": table, "ddl": ddl, "offset": offset}
            )
            or {"version": 1, "recorded_at": "2026-10-11T13:00:00Z", "ddl": ddl},
        )
        monkeypatch.setattr(cdc_mapping_review, "list_reviews", lambda **_args: [])
        monkeypatch.setattr(
            cdc_mapping_review,
            "flag_mapping_review",
            lambda **review: reviews.append(review),
        )

        if should_pause:
            with pytest.raises(
                OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"
            ):
                reader._record_oracle_ddl(
                    owner="DATAFLOW",
                    table="T1",
                    ddl="ALTER TABLE DATAFLOW.T1 ADD NOTE",
                    offset={"scn": 20, "rs_id": "r2", "ssn": 1},
                    ddl_timestamp=redo_timestamp,
                )
            assert history == []
            assert len(reviews) == 1
        else:
            reader._record_oracle_ddl(
                owner="DATAFLOW",
                table="T1",
                ddl="ALTER TABLE DATAFLOW.T1 ADD NOTE",
                offset={"scn": 20, "rs_id": "r2", "ssn": 1},
                ddl_timestamp=redo_timestamp,
            )
            assert len(history) == 1
            assert len(reviews) == 1
        assert len(executed) == 1
        assert executed[0][1]["redo_ts"] is redo_timestamp
    finally:
        if original_timezone is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", original_timezone)
        time.tzset()


def test_oracle_open_ambiguous_ddl_review_pauses_without_reflagging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_mapping_review

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch, reader, additional_ddl=True
    )
    marker = f"oracle-ddl@scn=20,rs_id={_oracle_test_rs_id(20)},ssn=0"
    matches = [
        {
            "id": "review-open",
            "table": "DATAFLOW.T1",
            "reason": "cdc_schema_drift",
            "ddl": f"[{marker}] [ambiguous schema as of DDL SCN: earlier probe] ALTER TABLE T1",
            "status": "open",
        }
    ]
    calls: list[dict] = []
    monkeypatch.setattr(
        cdc_mapping_review,
        "list_reviews",
        lambda **kwargs: calls.append(kwargs) or matches,
    )

    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
        next(poll)

    assert calls == [
        {"source_key": reader.source_key, "status": "all", "limit": 1000}
    ]
    assert history == []
    assert reviews == []


def test_oracle_acknowledged_ambiguous_review_records_then_passes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import cdc_mapping_review

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, calls, connection_factory = _install_buffered_ddl_window(
        monkeypatch, reader, additional_ddl=True
    )
    marker = f"oracle-ddl@scn=20,rs_id={_oracle_test_rs_id(20)},ssn=0"
    acknowledged = {
        "id": "review-ack",
        "table": "DATAFLOW.T1",
        "reason": "cdc_schema_drift",
        "ddl": f"[{marker}] [ambiguous schema as of DDL SCN: operator-approved] ALTER TABLE T1",
        "status": "acknowledged",
    }
    monkeypatch.setattr(
        cdc_mapping_review, "list_reviews", lambda **_kwargs: [acknowledged]
    )

    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    remaining = list(poll)
    assert [row["ID"] for batch in remaining for row in batch.inserts] == ["2"]
    assert len(history) == 1
    assert len(calls) == 1
    assert reviews == []

    restarted = _unit_reader(resume_token=before.resume_token)
    monkeypatch.setattr(
        restarted,
        "_oracle_ddl_schema_snapshot",
        lambda _table: {"columns": [{"name": "ID"}, {"name": "NOTE"}]},
    )
    monkeypatch.setattr(restarted, "_mining_conn", connection_factory)
    restarted._record_oracle_ddl(
        owner="DATAFLOW",
        table="T1",
        ddl="ALTER TABLE T1 ADD NOTE",
        offset={"scn": 20, "rs_id": _oracle_test_rs_id(20), "ssn": 0},
        ambiguous_later_ddl=True,
    )
    assert len(history) == 1
    assert len(calls) == 1
    assert reviews == []


def test_oracle_acknowledged_review_for_another_ddl_marker_still_pauses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_mapping_review

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch, reader, additional_ddl=True
    )
    different_marker = f"oracle-ddl@scn=19,rs_id={_oracle_test_rs_id(19)},ssn=0"
    monkeypatch.setattr(
        cdc_mapping_review,
        "list_reviews",
        lambda **_kwargs: [
            {
                "id": "different",
                "table": "DATAFLOW.T1",
                "reason": "cdc_schema_drift",
                "ddl": f"[{different_marker}] prior review",
                "status": "acknowledged",
            }
        ],
    )

    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    with pytest.raises(OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"):
        next(poll)

    expected_marker = f"oracle-ddl@scn=20,rs_id={_oracle_test_rs_id(20)},ssn=0"
    assert history == []
    assert len(reviews) == 1
    assert reviews[0]["ddl"].startswith(
        f"[{expected_marker}] [ambiguous schema as of DDL SCN: "
    )


def test_oracle_mapping_review_read_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_mapping_review

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch, reader, additional_ddl=True
    )

    def fail_read(**_kwargs):
        raise OSError("mapping review store unavailable")

    monkeypatch.setattr(cdc_mapping_review, "list_reviews", fail_read)
    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    with pytest.raises(OracleLogMinerReadError, match="mapping-review read"):
        next(poll)

    assert history == []
    assert reviews == []


def test_oracle_corrupt_mapping_review_store_fails_closed_without_later_dml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_mapping_review

    store_path = tmp_path / "cdc_mapping_reviews.jsonl"
    store_path.write_text("{corrupt json\n", encoding="utf-8")
    monkeypatch.setenv("CDC_MAPPING_REVIEW_STORE", str(store_path))
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    real_list_reviews = cdc_mapping_review.list_reviews
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, reviews, _calls, _connection = _install_buffered_ddl_window(
        monkeypatch, reader, additional_ddl=True
    )
    monkeypatch.setattr(cdc_mapping_review, "list_reviews", real_list_reviews)
    assert (
        cdc_mapping_review.list_reviews(
            source_key=reader.source_key, status="all", limit=1000
        )
        == []
    )

    poll = reader._poll_txn_buffered()
    before = next(poll)
    assert [row["ID"] for row in before.inserts] == ["1"]
    reader.ack(before.resume_token)
    with pytest.raises(
        OracleLogMinerReadError, match="ambiguous schema as of DDL SCN"
    ):
        next(poll)

    assert history == []
    assert len(reviews) == 1
    assert reviews[0]["reason"] == "cdc_schema_drift"


@pytest.mark.parametrize(
    ("failure_stage", "expected_history_count"),
    [
        ("introspection", 0),
        ("history write", 0),
        ("mapping-review write", 1),
    ],
)
def test_oracle_ddl_failure_stops_at_barrier_and_keeps_retry_position(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    expected_history_count: int,
) -> None:
    from connectors.oracle_logminer import (
        OracleLogMinerReadError,
        decode_logminer_token,
    )
    from connectors.oracle_logminer_txn import mining_position
    from services import cdc_mapping_review, cdc_schema_history

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    reader._lob_column_cache["T1"] = ()
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, _reviews, _calls, _connection_factory = _install_buffered_ddl_window(
        monkeypatch, reader
    )

    if failure_stage == "introspection":
        def fail_introspection(_table, **_kwargs):
            raise OSError("catalog unavailable")

        monkeypatch.setattr(reader, "_oracle_ddl_schema_snapshot", fail_introspection)
    elif failure_stage == "history write":
        def fail_history(*_args, **_kwargs):
            raise OSError("history unavailable")

        monkeypatch.setattr(cdc_schema_history, "record_ddl", fail_history)
    else:
        def fail_review(**_signal):
            raise OSError("mapping review unavailable")

        monkeypatch.setattr(cdc_mapping_review, "flag_mapping_review", fail_review)

    poll = reader._poll_txn_buffered()
    before = next(poll)
    assert before.inserts == [{"ID": "1", "V": "row1"}]
    reader.ack(before.resume_token)
    token = decode_logminer_token(before.resume_token)

    with pytest.raises(OracleLogMinerReadError) as raised:
        next(poll)

    message = str(raised.value).lower()
    assert "dataflow.t1" in message
    assert "scn 20" in message
    assert failure_stage in message
    assert len(history) == expected_history_count
    assert mining_position(
        token["poll_scn"], token["poll_rs_id"], token["poll_ssn"]
    ) < mining_position(20, _oracle_test_rs_id(20), 0)
    assert mining_position(reader.scn, reader.rs_id, reader.ssn) < mining_position(
        20, _oracle_test_rs_id(20), 0
    )


def test_oracle_ddl_retry_remines_and_records_history_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.oracle_logminer import OracleLogMinerReadError
    from services import cdc_schema_history

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    history, _reviews, _calls, connection_factory = _install_buffered_ddl_window(
        monkeypatch, reader
    )
    record = cdc_schema_history.record_ddl
    attempts = 0

    def fail_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("history unavailable")
        return record(*args, **kwargs)

    monkeypatch.setattr(cdc_schema_history, "record_ddl", fail_once)
    poll = reader._poll_txn_buffered()
    before = next(poll)
    reader.ack(before.resume_token)
    with pytest.raises(OracleLogMinerReadError):
        next(poll)

    resumed = _unit_reader(resume_token=before.resume_token)
    resumed.phase = "streaming"
    monkeypatch.setattr(resumed, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(resumed, "_oracle_ddl_schema_snapshot", lambda *_a, **_k: {"columns": [{"name": "ID"}, {"name": "NOTE"}]})
    monkeypatch.setattr(resumed, "_mining_conn", connection_factory)
    batches = list(resumed._poll_txn_buffered())

    assert attempts == 2
    assert len(history) == 1
    assert [row["ID"] for batch in batches for row in batch.inserts] == ["2"]
