"""Live SQL Server native CDC → sqlite transfer when :1433 is up.

Skips cleanly when the port is closed or native CDC is not enabled.
Delivery remains at-least-once upsert. Not leftover MERGE.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.sqlserver_cdc_native import (  # noqa: E402
    SqlServerNativeCdc,
    _hex_to_lsn,
    _lsn_to_hex,
    compare_mssql_hex_lsn,
    decode_mssql_cdc_token,
)
from connectors.postgresql_conn import get_connection as pg_connection  # noqa: E402
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.test_cdc_exactly_once_live_engines import (  # noqa: E402
    _require as _require_live_engine,
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


def _sqlserver_port_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 1433), timeout=1):
            return True
    except OSError:
        return False


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


def _dest_rows(path: Path, table: str) -> list[tuple]:
    con = sqlite3.connect(str(path))
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        if "_df_lsn" in cols:
            return list(
                con.execute(f'SELECT id, amount, "_df_lsn" FROM "{table}" ORDER BY id')
            )
        return list(con.execute(f'SELECT id, amount FROM "{table}" ORDER BY id'))
    finally:
        con.close()


def _wait_for_cdc_events(
    reader: SqlServerNativeCdc,
    resume_lsn: str,
    expected: set[tuple[int, int]],
    *,
    after_lsn: str | None = None,
    timeout_sec: float = 20.0,
) -> None:
    deadline = time.monotonic() + timeout_sec
    last_state: dict[str, object] = {}
    while time.monotonic() < deadline:
        with reader._conn() as conn:
            with conn.cursor() as cur:
                reader._resolve_capture_instance(cur)
                min_lsn = reader._min_lsn(cur)
                max_lsn = reader._max_lsn(cur)
                last_state = {"min_lsn": min_lsn, "max_lsn": max_lsn, "events": set()}
                if min_lsn and compare_mssql_hex_lsn(resume_lsn, min_lsn) < 0:
                    raise AssertionError(
                        f"CDC retention passed the handoff before test changes were "
                        f"visible (resume={resume_lsn}, min_lsn={min_lsn})"
                    )
                if (
                    min_lsn
                    and max_lsn
                    and compare_mssql_hex_lsn(max_lsn, resume_lsn) >= 0
                ):
                    cur.execute(
                        f"""
                        SELECT *
                        FROM {reader._changes_tvf()}(%s, %s, %s)
                        ORDER BY __$start_lsn, __$seqval
                        """,
                        (_hex_to_lsn(resume_lsn), _hex_to_lsn(max_lsn), "all"),
                    )
                    cols = [desc[0] for desc in (cur.description or [])]
                    operation_idx = cols.index("__$operation")
                    id_idx = cols.index("id")
                    rows = cur.fetchall() or []
                    events = {
                        (int(row[operation_idx]), int(row[id_idx]))
                        for row in rows
                    }
                    last_state["events"] = events
                    if expected.issubset(events):
                        if after_lsn is None:
                            return
                        start_lsn_idx = cols.index("__$start_lsn")
                        events_after_lsn = {
                            (int(row[operation_idx]), int(row[id_idx]))
                            for row in rows
                            if compare_mssql_hex_lsn(
                                _lsn_to_hex(row[start_lsn_idx]),
                                after_lsn,
                            )
                            > 0
                        }
                        if expected.issubset(events_after_lsn):
                            return
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    raise AssertionError(
        f"CDC capture did not expose expected events before transfer: "
        f"expected={expected}, observed={last_state}"
    )


def _m4_lob_signature(value):
    if value is None:
        return {"length": None, "bytes": None, "md5": None}
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, bytes):
        return {
            "length": len(value),
            "bytes": len(value),
            "md5": hashlib.md5(value).hexdigest(),
        }
    text = str(value)
    encoded = text.encode("utf-8")
    return {
        "length": len(text),
        "bytes": len(encoded),
        "md5": hashlib.md5(encoded).hexdigest(),
    }


def _m4_lob_rows(reader, pg_cfg, dest_table):
    columns = ("id", "c", "b", "x", "v", "t")
    with reader._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, CAST(c AS NVARCHAR(MAX)), b,
                       CAST(x AS NVARCHAR(MAX)), v, CAST(t AS NVARCHAR(MAX))
                FROM dbo.[{reader.table}]
                ORDER BY id
                """
            )
            source_rows = cur.fetchall() or []
    with pg_connection(**pg_cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, c, b, x::text, v, t
                FROM "public"."{dest_table}"
                ORDER BY id
                """
            )
            destination_rows = cur.fetchall() or []

    def signatures(rows):
        return {
            str(row[0]): {
                column: _m4_lob_signature(value)
                for column, value in zip(columns, row)
            }
            for row in rows
        }

    return signatures(source_rows), signatures(destination_rows)


def _m4_wait_for_lob_changes(reader, row_id, predicate, *, timeout_sec=30.0):
    deadline = time.monotonic() + timeout_sec
    last_rows = []
    while time.monotonic() < deadline:
        try:
            reader.force_cdc_scan()
        except Exception:
            pass
        with reader._conn() as conn:
            with conn.cursor() as cur:
                reader._resolve_capture_instance(cur)
                from_lsn = reader._min_lsn(cur)
                to_lsn = reader._max_lsn(cur)
                if from_lsn and to_lsn:
                    cur.execute(
                        f"""
                        SELECT __$start_lsn, __$seqval, __$operation, id,
                               DATALENGTH(c) AS c_bytes,
                               DATALENGTH(b) AS b_bytes,
                               DATALENGTH(x) AS x_bytes,
                               DATALENGTH(t) AS t_bytes,
                               v
                        FROM {reader._changes_tvf()}(%s, %s, %s)
                        WHERE id = %s
                        ORDER BY __$start_lsn, __$seqval, __$operation
                        """,
                        (
                            _hex_to_lsn(from_lsn),
                            _hex_to_lsn(to_lsn),
                            reader._row_filter_sql_arg(),
                            row_id,
                        ),
                    )
                    names = [desc[0] for desc in (cur.description or [])]
                    last_rows = [
                        dict(zip(names, row)) for row in (cur.fetchall() or [])
                    ]
        if predicate(last_rows):
            return last_rows
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    raise AssertionError(
        f"SQL Server CDC rows did not match the expected LOB step; rows={last_rows!r}"
    )


def _m4_enable_lob_table(reader, table):
    with reader._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT is_cdc_enabled FROM sys.databases "
                "WHERE database_id = DB_ID()"
            )
            row = cur.fetchone()
            if not row or not row[0]:
                cur.execute("EXEC sys.sp_cdc_enable_db")
            cur.execute(
                f"""
                CREATE TABLE dbo.[{table}] (
                    id INT NOT NULL PRIMARY KEY,
                    c NVARCHAR(MAX) NULL,
                    b VARBINARY(MAX) NULL,
                    x XML NULL,
                    v NVARCHAR(50) NULL,
                    t NTEXT NULL
                )
                """
            )
            cur.execute(
                """
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name = %s,
                    @role_name = NULL,
                    @supports_net_changes = 1
                """,
                (table,),
            )
        conn.commit()


@pytest.mark.skipif(
    not _sqlserver_native_ready(),
    reason="SQL Server native CDC (cdc_native_orders) not reachable on localhost:1433",
)
def test_sqlserver_native_transfer_snapshot_resume_delete(tmp_path: Path) -> None:
    table = "cdc_sx_" + uuid.uuid4().hex[:8]
    dest_path = tmp_path / "sqlserver_native_dest.db"
    job_id = "mssql-native-e2e-" + uuid.uuid4().hex[:8]
    holder = f"e2e-{table}"
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            _enable_cdc_on_table(cur, table)
        conn.commit()
    bootstrap.force_cdc_scan()

    src = EndpointConfig(
        kind="database", format="sqlserver", table=table, schema="dbo", **CFG
    )
    dst = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table=table,
    )
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "amount", "target": "amount"},
    ]
    schema = {"id": "INTEGER", "amount": "NUMERIC(12,2)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]

    try:
        rows1, ddl1, summary1, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=2,
        )
        assert any("CDC(sqlserver_native)" in line for line in ddl1), ddl1
        assert rows1 == 2, (rows1, summary1)

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE dbo.[{table}] SET amount = 99.00 WHERE id = 1")
                cur.execute(f"DELETE FROM dbo.[{table}] WHERE id = 2")
            conn.commit()
            runner = SqlServerNativeCdc(
                cfg, table=table, primary_key="id", schema="dbo"
            )
            runner.force_cdc_scan()
            handoff_lsn = decode_mssql_cdc_token(
                (summary1.get("cdc") or {}).get("watermark")
            )["lsn"]
            assert handoff_lsn, summary1
            _wait_for_cdc_events(
                runner,
                handoff_lsn,
                {(1, 2), (2, 3), (4, 1)},
            )

            rows2, ddl2, summary2, _ = run_cdc_database_transfer(
                src,
                dst,
                mappings,
                schema,
                sync_mode="cdc",
                stream_contracts=stream,
                job_id=job_id,
            )
        assert any("CDC(sqlserver_native)" in line for line in ddl2), ddl2
        dest2 = _dest_rows(dest_path, table)
        ids = [int(r[0]) for r in dest2]
        amounts = {int(r[0]): float(r[1]) for r in dest2}
        assert 2 not in ids, dest2
        assert 1 in ids and 3 in ids, dest2
        assert amounts[1] == 99.0, dest2
        assert amounts[3] == 30.0, dest2
        artifact = {
            "test": "test_sqlserver_native_transfer_snapshot_resume_delete",
            "delivery": "at-least-once upsert",
            "capture": "CDC(sqlserver_native)",
            "snapshot_rows": rows1,
            "resume_rows": rows2,
            "dest_ids": ids,
            "watermark": (summary2.get("cdc") or {}).get("watermark"),
            "note": "Not leftover MERGE. Not dest-owned exactly-once.",
        }
        out_dir = Path(os.environ.get("DATAFLOW_PROOF_DIR", "/opt/cursor/artifacts/cdc-merge-live"))
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "sqlserver-native-transfer.json").write_text(
                json.dumps(artifact, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError:
            pass
    finally:
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


def test_sqlserver_native_lob_transfer_to_postgres_exactly_once():
    if not _sqlserver_port_ready():
        pytest.skip("SQL Server native CDC container is not available")
    pg_cfg = _require_live_engine("postgresql")
    pg_cfg.pop("type", None)
    pg_cfg.update(connection_string="", ssl=False)

    suffix = uuid.uuid4().hex[:8]
    source_table = f"m4_lob_{suffix}"
    destination_table = f"m4_lob_dest_{suffix}"
    job_id = f"m4-lob-eos-{suffix}"
    reader_cfg = {
        **CFG,
        "job_id": job_id,
        "cdc_row_filter": "all update old",
        # Driver 18 verifies the chain. This lab container is self-signed.
        # Same declaration the other live SQL Server proofs already use.
        # The product default stays verify-or-fail.
        "trust_server_certificate": True,
    }
    reader = SqlServerNativeCdc(
        reader_cfg,
        table=source_table,
        primary_key="id",
        schema="dbo",
        cursor_key=f"m4-lob:{suffix}",
    )
    source = EndpointConfig(
        kind="database",
        format="sqlserver",
        table=source_table,
        schema="dbo",
        extra={
            "cdc_row_filter": "all update old",
            "trust_server_certificate": True,
        },
        **CFG,
    )
    destination = EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=destination_table,
        **pg_cfg,
    )
    columns = ("id", "c", "b", "x", "v", "t")
    mappings = [{"source": column, "target": column} for column in columns]
    column_types = {
        "id": "INT",
        "c": "TEXT",
        "b": "BYTEA",
        "x": "XML",
        "v": "VARCHAR(50)",
        "t": "TEXT",
    }
    stream = [
        {
            "name": source_table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]

    def transfer_and_compare(step):
        _rows, _ddl, summary, _extra = run_cdc_database_transfer(
            source,
            destination,
            mappings,
            column_types,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=100,
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )
        assert summary.get("cdc_delivery") == "exactly_once", summary
        expected, actual = _m4_lob_rows(reader, pg_cfg, destination_table)
        assert actual == expected, {
            "step": step,
            "source_signatures": expected,
            "destination_signatures": actual,
        }

    try:
        _m4_enable_lob_table(reader, source_table)
        assert reader.is_available()
        transfer_and_compare("initial-empty-snapshot")

        blob = bytes(range(256)) * 39 + bytes(range(16))
        assert len(blob) == 10_000
        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO dbo.[{source_table}] (id, c, b, x, v, t)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        1,
                        "c" * 20_000,
                        blob,
                        "<root><value>large-row-probe</value></root>",
                        "step-1",
                        "t" * 10_000,
                    ),
                )
            conn.commit()
        insert_rows = _m4_wait_for_lob_changes(
            reader,
            1,
            lambda rows: any(
                row["__$operation"] == 2
                and row["v"] == "step-1"
                and row["c_bytes"] == 40_000
                and row["b_bytes"] == 10_000
                and row["x_bytes"] is not None
                and row["t_bytes"] == 20_000
                for row in rows
            ),
        )
        assert insert_rows
        transfer_and_compare("insert")

        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE dbo.[{source_table}] SET v = %s WHERE id = %s",
                    ("step-2", 1),
                )
            conn.commit()
        update_v_rows = _m4_wait_for_lob_changes(
            reader,
            1,
            lambda rows: any(
                row["__$operation"] == 4 and row["v"] == "step-2"
                for row in rows
            ),
        )
        update_v_after = next(
            row
            for row in update_v_rows
            if row["__$operation"] == 4 and row["v"] == "step-2"
        )
        update_v_before = next(
            row
            for row in update_v_rows
            if row["__$operation"] == 3
            and row["__$start_lsn"] == update_v_after["__$start_lsn"]
            and row["__$seqval"] == update_v_after["__$seqval"]
        )
        assert all(
            update_v_before[f"{column}_bytes"] is None
            for column in ("c", "b", "x", "t")
        )
        assert all(
            update_v_after[f"{column}_bytes"] is not None
            for column in ("c", "b", "x", "t")
        )
        transfer_and_compare("update-v-only")

        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE dbo.[{source_table}] SET c = %s WHERE id = %s",
                    ("z" * 21_000, 1),
                )
            conn.commit()
        update_c_rows = _m4_wait_for_lob_changes(
            reader,
            1,
            lambda rows: any(
                row["__$operation"] == 4
                and row["v"] == "step-2"
                and row["c_bytes"] == 42_000
                for row in rows
            ),
        )
        update_c_after = next(
            row
            for row in update_c_rows
            if row["__$operation"] == 4
            and row["v"] == "step-2"
            and row["c_bytes"] == 42_000
        )
        update_c_before = next(
            row
            for row in update_c_rows
            if row["__$operation"] == 3
            and row["__$start_lsn"] == update_c_after["__$start_lsn"]
            and row["__$seqval"] == update_c_after["__$seqval"]
        )
        assert update_c_before["c_bytes"] == 40_000
        assert all(
            update_c_before[f"{column}_bytes"] is None
            for column in ("b", "x", "t")
        )
        transfer_and_compare("update-c-only")

        with reader._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM dbo.[{source_table}] WHERE id = %s",
                    (1,),
                )
            conn.commit()
        delete_rows = _m4_wait_for_lob_changes(
            reader,
            1,
            lambda rows: any(row["__$operation"] == 1 for row in rows),
        )
        assert delete_rows
        transfer_and_compare("delete")
    finally:
        try:
            with reader._conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(
                            """
                            EXEC sys.sp_cdc_disable_table
                                @source_schema = N'dbo',
                                @source_name = %s,
                                @capture_instance = N'all'
                            """,
                            (source_table,),
                        )
                    except Exception:
                        pass
                    cur.execute(
                        f"IF OBJECT_ID('dbo.[{source_table}]') IS NOT NULL "
                        f"DROP TABLE dbo.[{source_table}]"
                    )
                conn.commit()
        finally:
            with pg_connection(**pg_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'DROP TABLE IF EXISTS "public"."{destination_table}"'
                    )
                conn.commit()
