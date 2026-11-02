"""Live SQL Server *native* CDC integration (capture instance + LSN).

Requires:
  - SQL Server on localhost:1433 (compose / CI ``cdc-sqlserver``)
  - ``tests/sqlserver_ct_init.sql`` then ``tests/sqlserver_cdc_init.sql``
  - ODBC / pymssql via generic_sql

When SQL Agent is unavailable, the test calls ``force_cdc_scan()`` after DML.
"""

from __future__ import annotations
import os

import logging
import socket
import sys
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sqlserver_cdc_native import (  # noqa: E402
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
)
from connectors.writer_common import extract_cdc_lsn  # noqa: E402
from cdc_sqlserver_live_helpers import (  # noqa: E402
    sqlserver_server_now,
    wait_for_sqlserver_capture_job_healthy,
    wait_for_sqlserver_capture_job_stopped,
)

CFG = {
    "host": "localhost",
    "port": 1433,
    "database": "dataflow",
    "username": "sa",
    "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
    "connection_string": "",
    "ssl": False,
}


def _sqlserver_native_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 1433), timeout=1):
            pass
    except OSError:
        return False
    try:
        cdc = SqlServerNativeCdc(
            CFG, table="cdc_native_orders", primary_key="id", schema="dbo"
        )
        return cdc.is_available()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _sqlserver_native_ready(),
    reason="SQL Server native CDC (cdc_native_orders) not reachable on localhost:1433",
)


def _enable_cdc_on_table(cur, table: str) -> None:
    cur.execute(
        f"""
        IF OBJECT_ID('dbo.[{table}]') IS NOT NULL DROP TABLE dbo.[{table}];
        CREATE TABLE dbo.[{table}] (
            id INT NOT NULL PRIMARY KEY,
            amount DECIMAL(12,2) NOT NULL
        );
        """
    )
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1
        )
            EXEC sys.sp_cdc_enable_db;
        """
    )
    cur.execute(
        f"""
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'{table}',
            @role_name = NULL,
            @supports_net_changes = 0;
        """
    )
    cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10.00), (2, 20.00)")


def _wait_capture(cdc: SqlServerNativeCdc, table: str, timeout: float = 20.0) -> None:
    """Wait for the capture instance. Agent-less hosts need ``sp_cdc_scan``."""
    deadline = time.time() + timeout
    last_min = ""
    while time.time() < deadline:
        if cdc.is_available():
            try:
                cdc.force_cdc_scan()
            except Exception:
                pass
            with cdc._conn() as conn:
                with conn.cursor() as cur:
                    last_min = cdc._min_lsn(cur)
            if last_min:
                return
        time.sleep(0.5)
    raise AssertionError(
        f"capture instance for {table} not ready (min_lsn={last_min!r})"
    )


def test_sqlserver_native_snapshot_poll_ack_and_df_lsn():
    table = "cdc_n_" + uuid.uuid4().hex[:8]
    holder = f"it-native-{table}"
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            _enable_cdc_on_table(cur, table)
        conn.commit()

    cdc = SqlServerNativeCdc(
        cfg, table=table, primary_key="id", schema="dbo", batch_size=100, cursor_key=f"it:{table}"
    )
    try:
        _wait_capture(cdc, table)
        assert cdc.capture_instance  # discovered
        meta = cdc.cdc_metadata()
        assert meta["delivery"] == "at-least-once"
        assert meta["plugin"] == "sqlserver_native_cdc"

        batches = list(cdc.snapshot())
        inserts = [r for b in batches for r in b.inserts]
        assert len(inserts) == 2
        handoff = decode_mssql_cdc_token(batches[-1].resume_token)
        assert handoff["phase"] == "streaming"
        assert handoff["lsn"]
        assert extract_cdc_lsn(batches[-1].resume_token) == handoff["lsn"]

        with cdc._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE dbo.[{table}] SET amount = 99.00 WHERE id = 1")
                cur.execute(f"DELETE FROM dbo.[{table}] WHERE id = 2")
            conn.commit()
        cdc.force_cdc_scan()

        seen_ins: set[str] = set()
        seen_upd: set[str] = set()
        seen_del: set[str] = set()
        deadline = time.time() + 25.0
        last_token = batches[-1].resume_token
        while time.time() < deadline:
            changes = list(cdc.poll())
            for b in changes:
                if b.resume_token:
                    last_token = b.resume_token
                for r in b.inserts:
                    seen_ins.add(str(r.get("id")))
                for r in b.updates:
                    seen_upd.add(str(r.get("id")))
                for d in b.deletes:
                    seen_del.add(str(d))
                if b.total_changes:
                    cdc.ack(b.resume_token)
            if "3" in seen_ins and "1" in seen_upd and "2" in seen_del:
                break
            cdc.force_cdc_scan()
            time.sleep(0.4)

        assert "3" in seen_ins, seen_ins
        assert "1" in seen_upd, seen_upd
        assert "2" in seen_del, seen_del
        assert extract_cdc_lsn(last_token)
        # Peek: after ack, empty or no redelivery of same ops required — at-least-once OK.
        again = list(cdc.poll())
        assert all(isinstance(b.resume_token, str) for b in again)
    finally:
        try:
            cdc.close()
        except Exception:
            pass
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                # Disable CDC on table before drop when possible.
                try:
                    cur.execute(
                        f"""
                        EXEC sys.sp_cdc_disable_table
                            @source_schema = N'dbo',
                            @source_name = N'{table}',
                            @capture_instance = N'all';
                        """
                    )
                except Exception:
                    pass
                cur.execute(
                    f"IF OBJECT_ID('dbo.[{table}]') IS NOT NULL DROP TABLE dbo.[{table}]"
                )
            conn.commit()


def test_force_cdc_scan_releases_capture_job_for_new_changes(caplog):
    table = "cdc_owner_" + uuid.uuid4().hex[:8]
    cfg = {
        **CFG,
        "lease_holder_id": f"it-owner-{table}",
        "job_id": f"it-owner-{table}",
    }
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    reader = SqlServerNativeCdc(
        cfg,
        table=table,
        primary_key="id",
        schema="dbo",
        cursor_key=f"it:owner:{table}",
    )
    table_created = False
    capture_since = None

    try:
        table_created = True
        with reader._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("EXEC sys.sp_cdc_stop_job 'capture'")
                except Exception:
                    pass
            conn.commit()
        wait_for_sqlserver_capture_job_stopped(reader)

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                _enable_cdc_on_table(cur, table)
            conn.commit()

        since = sqlserver_server_now(reader)
        with reader._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("EXEC sys.sp_cdc_start_job 'capture'")
                except Exception:
                    pass
            conn.commit()
        wait_for_sqlserver_capture_job_healthy(reader, since=since)
        capture_instance = reader.capture_instance
        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT start_lsn
                    FROM cdc.change_tables
                    WHERE capture_instance = %s
                    """,
                    (capture_instance,),
                )
                capture_start = cur.fetchone()
        assert capture_start and capture_start[0], (
            f"capture instance {capture_instance!r} did not receive a start LSN"
        )

        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("EXEC sys.sp_cdc_stop_job 'capture'")
            conn.commit()
        wait_for_sqlserver_capture_job_stopped(reader)

        capture_since = sqlserver_server_now(reader)
        caplog.set_level(logging.DEBUG, logger="connectors.sqlserver_cdc_native")
        reader.force_cdc_scan()
        assert "sp_cdc_scan skipped" not in caplog.text

        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("EXEC sys.sp_cdc_start_job 'capture'")
            conn.commit()
        wait_for_sqlserver_capture_job_healthy(reader, since=capture_since)

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
            conn.commit()

        deadline = time.monotonic() + 15.0
        captured_rows = 0
        while time.monotonic() < deadline:
            with reader._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT(*) FROM cdc.[{capture_instance}_CT] WHERE id = %s",
                        (3,),
                    )
                    captured_rows = int(cur.fetchone()[0] or 0)
            if captured_rows:
                break
            time.sleep(0.25)
        assert captured_rows == 1, (
            f"row 3 did not reach {capture_instance}_CT within 15s "
            "without another force_cdc_scan()"
        )
    finally:
        try:
            if capture_since is None:
                capture_since = sqlserver_server_now(reader)
            with reader._conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute("EXEC sys.sp_cdc_start_job 'capture'")
                    except Exception:
                        pass
                conn.commit()
            wait_for_sqlserver_capture_job_healthy(
                reader, since=capture_since
            )
        finally:
            try:
                reader.close()
            except Exception:
                pass
            if table_created:
                try:
                    with bootstrap._conn() as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(
                                    f"""
                                    EXEC sys.sp_cdc_disable_table
                                        @source_schema = N'dbo',
                                        @source_name = N'{table}',
                                        @capture_instance = N'all';
                                    """
                                )
                            except Exception:
                                pass
                            cur.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")
                        conn.commit()
                except Exception:
                    pass
            bootstrap.close()


def test_sqlserver_retention_probe_gap_and_clear():
    """Live: fabricate resume < min_lsn → probe gap → clear → no_watermark.

    Honesty: this is the cleanup/AG gap *class* without requiring dual-node AG.
    """
    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token
    from services.cdc_cursor_gap import CdcLsnGapError
    from services.cdc_retention_probe import probe_sqlserver_retention
    from services.sync_cursor import clear_watermark, set_watermark

    table = "cdc_ret_" + uuid.uuid4().hex[:8]
    holder = f"it-ret-{table}"
    ck = f"it:retention:{table}"
    cfg = {
        **CFG,
        "lease_holder_id": holder,
        "job_id": holder,
        "type": "sqlserver",
        "primary_key": "id",
    }
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            _enable_cdc_on_table(cur, table)
        conn.commit()

    cdc = SqlServerNativeCdc(
        cfg, table=table, primary_key="id", schema="dbo", batch_size=50, cursor_key=ck
    )
    try:
        _wait_capture(cdc, table)
        with cdc._conn() as conn:
            with conn.cursor() as cur:
                min_lsn = cdc._min_lsn(cur)
        assert min_lsn, "expected capture min_lsn"

        # Fabricate a resume LSN strictly before retention (gap class).
        stale = encode_mssql_cdc_token("00", table=table, phase="streaming")
        set_watermark(ck, stale)

        probe = probe_sqlserver_retention(
            cfg, table=table, schema="dbo", cursor_key=ck
        )
        assert probe.status == "gap", probe.to_dict()
        assert probe.retained

        # Poll must fail closed on the same gap class.
        cdc2 = SqlServerNativeCdc(
            cfg,
            table=table,
            primary_key="id",
            schema="dbo",
            batch_size=50,
            cursor_key=ck,
            resume_token=stale,
        )
        try:
            with pytest.raises(CdcLsnGapError):
                list(cdc2.poll())
        finally:
            try:
                cdc2.close()
            except Exception:
                pass

        clear_watermark(ck)
        after = probe_sqlserver_retention(
            cfg, table=table, schema="dbo", cursor_key=ck
        )
        assert after.status in {"no_watermark", "ok"}, after.to_dict()
    finally:
        try:
            cdc.close()
        except Exception:
            pass
        clear_watermark(ck)
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        EXEC sys.sp_cdc_disable_table
                            @source_schema = N'dbo',
                            @source_name = N'{table}',
                            @capture_instance = N'all';
                        """
                    )
                except Exception:
                    pass
                cur.execute(
                    f"IF OBJECT_ID('dbo.[{table}]') IS NOT NULL DROP TABLE dbo.[{table}]"
                )
            conn.commit()
