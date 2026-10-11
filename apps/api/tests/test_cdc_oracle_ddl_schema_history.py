"""Live Oracle LogMiner DDL-to-schema-history regression coverage."""

from __future__ import annotations

import logging
import time
import uuid

import pytest

from services.cdc_schema_history import list_history
from services.cdc_mapping_review import list_reviews
from test_cdc_oracle_logminer_transfer_e2e import (
    _oracle_cfg,
    _oracle_logminer_ready,
)
from test_cdc_oracle_logminer_txn_live import _pg_cfg, _pg_ready


def _oracle_test_rs_id(position: int) -> str:
    return f"0x000001.{position:08x}.0000"


@pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)
def test_oracle_logminer_records_live_ddl_without_emitting_it_as_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import OracleLogMinerCdc

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    cfg = _oracle_cfg()
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
    created = False
    try:
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
@pytest.mark.parametrize("txn_buffer", [False, True], ids=["buffer-off", "buffer-on"])
def test_oracle_logminer_ddl_eos_restart_has_no_loss_or_duplicate(
    txn_buffer: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from connectors.generic_sql import get_connection as oracle_connection
    from connectors.postgresql_conn import get_connection as pg_connection
    from services.cdc_exactly_once import WATERMARK_TABLE
    from services.brand_env import getenv_brand
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
    cfg = _oracle_cfg()
    pg = _pg_cfg()
    schema = str(cfg["schema"])
    table = "CDC_DDL_EOS_" + uuid.uuid4().hex[:8].upper()
    dest_table = table.lower() + "_dest"
    table_ref = f'"{schema}"."{table}"'
    qualified = f"{schema}.{table}"
    job_id = f"oracle-ddl-eos-{uuid.uuid4().hex[:8]}"
    created = False

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
                cur.execute(f"ALTER TABLE {table_ref} ADD (NOTE VARCHAR2(50))")
            conn.commit()
        with pg_connection(**pg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "public"."{dest_table}" ADD COLUMN note VARCHAR(50)'
                )
            conn.commit()
        mappings.append({"source": "NOTE", "target": "note"})
        column_types["NOTE"] = "VARCHAR(50)"
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
                    f"INSERT INTO {table_ref} (ID, V, NOTE) "
                    "VALUES (2, 'r2', 'after-ddl')"
                )
            conn.commit()

        run_transfer()
        run_transfer()
        actual = dest_rows()
        expected = source_rows()
        assert actual == expected == [
            ("1", "r1", None),
            ("2", "r2", "after-ddl"),
        ]
        assert len(actual) == len({row[0] for row in actual})

        from connectors.oracle_logminer import OracleLogMinerCdc

        reader = OracleLogMinerCdc(
            cfg,
            table=table,
            primary_key="ID",
            schema=schema,
            cursor_key=f"o6-ddl-eos:{table}",
        )
        history = list_history(reader.source_key, qualified)
        assert len(history) == 1, history
        assert "ADD" in history[0]["ddl"].upper(), history
        reader.close()
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


def _unit_reader(*, table: str | list[str] = "T1"):
    from connectors.oracle_logminer import OracleLogMinerCdc

    tables = [table] if isinstance(table, str) else table
    return OracleLogMinerCdc(
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
        cursor_key="o6-unit:" + ",".join(tables),
    )


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


def test_oracle_ddl_history_failure_warns_and_keeps_streaming_dml(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import connectors.oracle_logminer as logminer
    from services import cdc_schema_history

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    reader = _unit_reader()
    reader.phase = "streaming"
    reader.scn = 1
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(reader, "_oracle_ddl_schema_snapshot", lambda _table: {"columns": [{"name": "ID"}]})
    monkeypatch.setattr(cdc_schema_history, "list_history", lambda *_args: [])
    monkeypatch.setattr(
        cdc_schema_history,
        "record_ddl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("history unavailable")),
    )
    monkeypatch.setattr(
        logminer,
        "assert_resume_scn_in_redo",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(
        logminer, "assert_redo_continuity", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(
        logminer,
        "classify_sql_redo",
        lambda *_args, **_kwargs: ("ok", {"ID": "1", "V": "kept"}),
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

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(reader, "_mining_conn", lambda: Connection())
    monkeypatch.setattr(
        reader,
        "_fetch_logminer_rows_visible",
        lambda *_args, **_kwargs: (
            [
                (
                    5,
                    _oracle_test_rs_id(5),
                    0,
                    "DDL",
                    "ALTER TABLE T1 ADD NOTE VARCHAR2(50)",
                    "T1",
                    "DATAFLOW",
                    "T1",
                ),
                (
                    6,
                    _oracle_test_rs_id(6),
                    0,
                    "INSERT",
                    "INSERT INTO T1 VALUES (1)",
                    "T1",
                    "DATAFLOW",
                    "T1",
                ),
            ],
            100,
        ),
    )
    monkeypatch.setattr(
        "services.cdc_incremental_runner.interleave_incremental_snapshot",
        lambda *_args, **_kwargs: iter(()),
    )

    with caplog.at_level(logging.WARNING):
        batches = list(reader._poll_once())

    assert any(batch.inserts == [{"ID": "1", "V": "kept"}] for batch in batches)
    assert "schema history could not be recorded" in caplog.text
    assert "history unavailable" in caplog.text
