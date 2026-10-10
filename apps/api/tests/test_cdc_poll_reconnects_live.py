"""Opt-in source-container restart checks through the PostgreSQL EOS runner."""

from __future__ import annotations

import logging
import socket
import subprocess
import time
import uuid
from contextlib import contextmanager

import pytest
from connectors.write_resilience import is_connection_lost
from services.brand_env import getenv_brand


pytestmark = pytest.mark.skipif(
    getenv_brand("CDC_LIVE_RESTART_TESTS", "").strip().lower() not in {"1", "true", "yes"},
    reason="set DATAFLOW_CDC_LIVE_RESTART_TESTS=1 to restart CDC source containers",
)


def _wait_for_port(host: str, port: int, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.25)
    raise AssertionError(f"service did not become reachable at {host}:{port}")


def _ensure_container_up(name: str, host: str, port: int) -> None:
    try:
        _wait_for_port(host, port)
    except AssertionError:
        subprocess.run(
            ["docker", "start", name],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        _wait_for_port(host, port)


def _restart_container(name: str) -> None:
    subprocess.run(
        ["docker", "restart", name],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
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


def _postgres_rows(pg: dict[str, object], table: str) -> list[tuple[int, str]]:
    from connectors.postgresql_conn import get_connection

    with get_connection(**pg) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, amount FROM "public"."{table}" ORDER BY id')
            return [(int(row[0]), f"{float(row[1]):.2f}") for row in cur.fetchall()]


def _delete_postgres_target(pg: dict[str, object], table: str) -> None:
    from connectors.postgresql_conn import get_connection
    from services.cdc_exactly_once import WATERMARK_TABLE

    try:
        with get_connection(**pg) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "public"."{table}"')
                cur.execute(
                    f"DELETE FROM public.{WATERMARK_TABLE} WHERE dest_object = %s",
                    (table,),
                )
            conn.commit()
    except Exception:
        pass


def _run_postgres_eos_transfer(src, dst, *, mappings, types, stream, job_id):
    from src.transfer.cdc_transfer import run_cdc_database_transfer

    return run_cdc_database_transfer(
        src,
        dst,
        mappings,
        types,
        sync_mode="cdc",
        stream_contracts=stream,
        job_id=job_id,
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
        limit=100,
    )


def test_sqlserver_eos_runner_recovers_from_source_restart(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc
    from src.transfer.models import EndpointConfig
    from test_cdc_sqlserver_native_integration import (
        CFG,
        _enable_cdc_on_table,
        _sqlserver_native_ready,
    )
    from test_cdc_oracle_logminer_txn_live import _pg_ready

    if not _sqlserver_native_ready() or not _pg_ready():
        pytest.skip("df-mssql or df-pg is not reachable")
    table = "GCDC_RECONNECT_" + uuid.uuid4().hex[:8].upper()
    target = table.lower() + "_dest"
    job_id = "mssql-reconnect-" + uuid.uuid4().hex[:8]
    holder = "mssql-reconnect-" + uuid.uuid4().hex[:8]
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    pg = _pg_cfg()
    bootstrap = SqlServerNativeCdc(cfg, table="cdc_native_orders", primary_key="id")
    created = False
    src = EndpointConfig(
        kind="database", format="sqlserver", table=table, schema="dbo", **CFG
    )
    dst = EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=target,
        **pg,
    )
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "amount", "target": "amount", "confidence": 1.0},
    ]
    types = {"id": "INTEGER", "amount": "NUMERIC(12,2)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]
    restarted = False
    original_conn = SqlServerNativeCdc._conn

    @contextmanager
    def restart_during_stream(self):
        nonlocal restarted
        with original_conn(self) as conn:
            if not restarted and self.phase == "streaming":
                restarted = True
                _restart_container("df-mssql")
            yield conn

    def source_rows() -> list[tuple[int, str]]:
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, amount FROM dbo.[{table}] ORDER BY id")
                return [(int(row[0]), f"{float(row[1]):.2f}") for row in cur.fetchall()]

    try:
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                _enable_cdc_on_table(cur, table)
            conn.commit()
        created = True
        bootstrap.force_cdc_scan()
        rows, _ddl, _summary, _extra = _run_postgres_eos_transfer(
            src, dst, mappings=mappings, types=types, stream=stream, job_id=job_id
        )
        assert rows == 2

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE dbo.[{table}] SET amount = 99.00 WHERE id = 1")
            conn.commit()
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(SqlServerNativeCdc, "_conn", restart_during_stream)
            with caplog.at_level(logging.WARNING):
                _run_postgres_eos_transfer(
                    src,
                    dst,
                    mappings=mappings,
                    types=types,
                    stream=stream,
                    job_id=job_id,
                )

        assert restarted
        assert any(
            "SQL Server CDC poll connection lost; reconnecting (attempt=1)" in record.message
            for record in caplog.records
        ), caplog.text
        deadline = time.monotonic() + 60
        expected = source_rows()
        actual: list[tuple[int, str]] = []
        while time.monotonic() < deadline:
            actual = _postgres_rows(pg, target)
            if actual == expected == [(1, "99.00"), (2, "20.00"), (3, "30.00")]:
                break
            _run_postgres_eos_transfer(
                src, dst, mappings=mappings, types=types, stream=stream, job_id=job_id
            )
            time.sleep(0.25)
        assert actual == expected == [(1, "99.00"), (2, "20.00"), (3, "30.00")]
        assert len(actual) == len({row[0] for row in actual})
    finally:
        _ensure_container_up(
            "df-mssql", str(CFG["host"]), int(CFG.get("port", 1433))
        )
        _delete_postgres_target(pg, target)
        if created:
            with bootstrap._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', "
                        f"@source_name=N'{table}', @capture_instance=N'all'"
                    )
                    cur.execute(f"DROP TABLE dbo.[{table}]")
                conn.commit()
        bootstrap.close()


@pytest.mark.parametrize("txn_buffer", [False, True], ids=["buffer-off", "buffer-on"])
@pytest.mark.xfail(
    strict=True,
    reason="G-CDC M2 residual: non-empty redelivery at committed composite position after Oracle restart; under diagnosis",
)
def test_oracle_eos_runner_recovers_from_source_restart(
    txn_buffer: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from connectors.generic_sql import get_connection as oracle_connection
    from connectors import oracle_logminer
    from connectors import cdc_eos_sql
    from connectors.oracle_logminer import (
        OracleLogMinerCdc,
        decode_logminer_token,
    )
    from services import cdc_exactly_once
    from services.cdc_exactly_once import batch_lsn
    from src.transfer.models import EndpointConfig
    from test_cdc_oracle_logminer_transfer_e2e import (
        _oracle_cfg,
        _oracle_logminer_ready,
    )
    from test_cdc_oracle_logminer_txn_live import _pg_ready

    if not _oracle_logminer_ready() or not _pg_ready():
        pytest.skip("df-oracle or df-pg is not reachable")
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
    cfg = _oracle_cfg()
    pg = _pg_cfg()
    schema = str(cfg["schema"])
    table = "GCDC_RECONNECT_" + uuid.uuid4().hex[:8].upper()
    target = table.lower() + "_dest"
    table_ref = f'"{schema}"."{table}"'
    job_id = "oracle-reconnect-" + uuid.uuid4().hex[:8]
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
        table=target,
        **pg,
    )
    mappings = [
        {"source": "ID", "target": "id", "confidence": 1.0},
        {"source": "AMOUNT", "target": "amount", "confidence": 1.0},
    ]
    types = {"ID": "NUMERIC", "AMOUNT": "NUMERIC(12,2)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "ID",
            "sync_mode": "cdc",
        }
    ]
    restarted = False
    original_mining_conn = OracleLogMinerCdc._mining_conn
    original_poll_once = OracleLogMinerCdc._poll_once
    original_init = OracleLogMinerCdc.__init__
    original_open_eos = cdc_eos_sql.open_eos_session
    original_start_logminer = oracle_logminer.start_logminer_session
    original_decide_eos_apply = cdc_exactly_once.decide_eos_apply
    poll_attempt = 0
    resume_expected = False
    resume_from_dest = None
    trace_logger = logging.getLogger(__name__)

    def trace_open_eos(**kwargs):
        nonlocal resume_from_dest
        result = original_open_eos(**kwargs)
        if resume_expected:
            resume_from_dest = result.resume
        return result

    def assert_resume_state(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        if resume_expected:
            assert resume_from_dest is not None
            state = decode_logminer_token(resume_from_dest)
            trace_logger.warning(
                "M23_DIAG reopened-reader scn=%s rs_id=%r ssn=%s "
                "low_scn=%s phase=%s resume_token=%r dest_resume=%r",
                self.scn,
                self.rs_id,
                self.ssn,
                self.low_scn,
                self.phase,
                self.resume_token,
                resume_from_dest,
            )
            assert self.resume_token == resume_from_dest
            assert self.scn == state["scn"]
            assert self.rs_id == state["rs_id"]
            assert self.ssn == state["ssn"]
            assert self.low_scn == state["low_scn"]
            assert self.phase == state["phase"]
            assert self.phase != "initial"
            if self.phase == "snapshot":
                assert self.snapshot_offset or self.snapshot_last_pk
            assert self.scn > 0

    @contextmanager
    def restart_during_stream(self):
        nonlocal restarted
        with original_mining_conn(self) as conn:
            if not restarted and self.phase == "streaming":
                restarted = True
                with oracle_connection(
                    host=cfg["host"],
                    port=cfg["port"],
                    database=cfg["database"],
                    username=cfg["username"],
                    password=cfg["password"],
                    connection_string="",
                    ssl=False,
                    db_type="oracle",
                ) as source_conn:
                    with source_conn.cursor() as cur:
                        cur.execute(
                            f"INSERT INTO {table_ref} (ID, AMOUNT) VALUES (3, 30)"
                        )
                        cur.execute(f"UPDATE {table_ref} SET AMOUNT = 99 WHERE ID = 1")
                    source_conn.commit()
                _restart_container("df-oracle")
            yield conn

    def trace_poll_once(self):
        nonlocal poll_attempt
        poll_attempt += 1
        attempt = poll_attempt
        if attempt == 1:
            assert self.phase == "streaming"
            assert self.scn > 0
        trace_logger.warning(
            "M23_DIAG poll-start attempt=%d token=%s",
            attempt,
            self._token(),
        )
        try:
            for batch in original_poll_once(self):
                raw_token = batch.resume_token
                token = (
                    raw_token
                    if isinstance(raw_token, dict)
                    else decode_logminer_token(raw_token)
                )
                rows = [
                    ("insert", row)
                    for row in (batch.inserts or [])
                ] + [
                    ("update", row)
                    for row in (batch.updates or [])
                ]
                row_ids = [
                    row.get("ID", row.get("id"))
                    for _operation, row in rows
                ] + list(batch.deletes or [])
                field_counts = {
                    field: (
                        len(getattr(batch, field) or [])
                        if getattr(batch, field, None) is not None
                        else None
                    )
                    for field in ("inserts", "updates", "deletes", "rejected")
                }
                trace_logger.warning(
                    "M23_DIAG batch attempt=%d scn=%s rs_id=%r ssn=%s "
                    "batch_lsn=%s row_ids=%s rows=%s fields=%s raw_token=%r",
                    attempt,
                    token.get("scn"),
                    token.get("rs_id"),
                    token.get("ssn"),
                    batch_lsn(batch.resume_token),
                    row_ids,
                    rows,
                    field_counts,
                    raw_token,
                )
                yield batch
        except Exception as exc:
            trace_logger.warning(
                "M23_DIAG poll-error attempt=%d error_type=%s "
                "error_number=%s transient=%s",
                attempt,
                type(exc).__name__,
                getattr(exc, "error_number", None),
                getattr(exc, "transient", None),
            )
            raise
        finally:
            trace_logger.warning(
                "M23_DIAG poll-state attempt=%d scn=%s rs_id=%r ssn=%s "
                "low_scn=%s poll_position=(%s,%r,%s) emitted_commit=%r",
                attempt,
                self.scn,
                self.rs_id,
                self.ssn,
                self.low_scn,
                self._poll_scn,
                self._poll_rs_id,
                self._poll_ssn,
                self._emitted_commit,
            )

    def trace_decide_eos_apply(**kwargs):
        change = kwargs.get("change")
        field_counts = {
            field: (
                len(getattr(change, field) or [])
                if getattr(change, field, None) is not None
                else None
            )
            for field in ("inserts", "updates", "deletes", "rejected")
        }
        details = {
            "incoming_lsn": kwargs.get("incoming_lsn"),
            "dest_lsn": kwargs.get("dest_lsn"),
            "change_type": type(change).__name__ if change is not None else None,
            "change_is_none": change is None,
            "change_fields": field_counts,
            "change_token": getattr(change, "resume_token", None),
        }
        try:
            result = original_decide_eos_apply(**kwargs)
        except Exception as exc:
            trace_logger.warning(
                "M23_DIAG eos-decision error details=%r error_type=%s error=%s",
                details,
                type(exc).__name__,
                exc,
            )
            raise
        trace_logger.warning(
            "M23_DIAG eos-decision result=%s details=%r",
            result[0],
            details,
        )
        return result

    def trace_start_logminer(cur, *args, **kwargs):
        trace_logger.warning(
            "M23_DIAG logminer-window start_scn=%s end_scn=%s committed_only=%s",
            kwargs.get("start_scn"),
            kwargs.get("end_scn"),
            kwargs.get("committed_only", True),
        )
        return original_start_logminer(cur, *args, **kwargs)

    def source_rows() -> list[tuple[int, str]]:
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
                cur.execute(f"SELECT ID, AMOUNT FROM {table_ref} ORDER BY ID")
                return [(int(row[0]), f"{float(row[1]):.2f}") for row in cur.fetchall()]

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
                    f"CREATE TABLE {table_ref} "
                    "(ID NUMBER PRIMARY KEY, AMOUNT NUMBER(12,2))"
                )
                created = True
                cur.execute(
                    f"ALTER TABLE {table_ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
                cur.execute(f"INSERT INTO {table_ref} (ID, AMOUNT) VALUES (1, 10)")
                cur.execute(f"INSERT INTO {table_ref} (ID, AMOUNT) VALUES (2, 20)")
            conn.commit()
        _run_postgres_eos_transfer(
            src, dst, mappings=mappings, types=types, stream=stream, job_id=job_id
        )
        with pytest.MonkeyPatch.context() as patcher:
            patcher.setattr(OracleLogMinerCdc, "__init__", assert_resume_state)
            patcher.setattr(cdc_eos_sql, "open_eos_session", trace_open_eos)
            patcher.setattr(OracleLogMinerCdc, "_mining_conn", restart_during_stream)
            patcher.setattr(OracleLogMinerCdc, "_poll_once", trace_poll_once)
            patcher.setattr(
                oracle_logminer, "start_logminer_session", trace_start_logminer
            )
            patcher.setattr(
                cdc_exactly_once, "decide_eos_apply", trace_decide_eos_apply
            )
            resume_expected = True
            with caplog.at_level(logging.WARNING):
                _run_postgres_eos_transfer(
                    src,
                    dst,
                    mappings=mappings,
                    types=types,
                    stream=stream,
                    job_id=job_id,
                )

        assert restarted
        assert any(
            "Oracle LogMiner poll connection lost; reconnecting (attempt=1)" in record.message
            for record in caplog.records
        ), caplog.text
        source_deadline = time.monotonic() + 60
        expected: list[tuple[int, str]] | None = None
        while time.monotonic() < source_deadline:
            try:
                expected = source_rows()
                break
            except Exception as exc:
                if not is_connection_lost(exc):
                    raise
                time.sleep(0.25)
        assert expected is not None, "Oracle did not become queryable after restart"
        deadline = time.monotonic() + 60
        actual: list[tuple[int, str]] = []
        while time.monotonic() < deadline:
            actual = _postgres_rows(pg, target)
            if actual == expected == [(1, "99.00"), (2, "20.00"), (3, "30.00")]:
                break
            _run_postgres_eos_transfer(
                src, dst, mappings=mappings, types=types, stream=stream, job_id=job_id
            )
            time.sleep(0.25)
        assert actual == expected == [(1, "99.00"), (2, "20.00"), (3, "30.00")]
        assert len(actual) == len({row[0] for row in actual})
    finally:
        _ensure_container_up("df-oracle", str(cfg["host"]), int(cfg["port"]))
        _delete_postgres_target(pg, target)
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
