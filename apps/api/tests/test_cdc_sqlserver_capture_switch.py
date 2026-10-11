"""Live SQL Server CDC capture-instance handoff coverage."""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import MagicMock

import pytest

from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    compare_mssql_hex_lsn,
    decode_mssql_cdc_token,
)
from services.cdc_cursor_gap import CdcLsnGapError
from services.brand_env import getenv_brand
from test_cdc_sqlserver_native_integration import CFG


def test_native_capture_switch_drains_old_instance_before_new() -> None:
    cfg = {
        **CFG,
        "host": getenv_brand("DATAFLOW_SQLSERVER_HOST", str(CFG["host"]))
        or str(CFG["host"]),
        "port": int(
            getenv_brand("DATAFLOW_SQLSERVER_PORT", str(CFG.get("port", 1433)))
            or CFG.get("port", 1433)
        ),
    }
    table = "m3_switch_" + uuid.uuid4().hex[:8]
    capture_a = f"{table}_v1"
    capture_b = f"{table}_v2"
    holder = f"m3-switch-{uuid.uuid4().hex[:8]}"
    cfg.update(lease_holder_id=holder, job_id=holder)
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    reader = SqlServerNativeCdc(
        cfg,
        table=table,
        primary_key="id",
        schema="dbo",
        batch_size=50,
        cursor_key=f"m3:capture-switch:{table}",
    )
    destination = "m3_switch_pg_" + uuid.uuid4().hex[:10]
    destination_key = f"m3-capture-switch-{uuid.uuid4().hex}"
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once
    from test_cdc_exactly_once_live_engines import _drop, _require

    pg_cfg = _require("postgresql")
    _drop("postgresql", [destination])

    def apply_to_postgres(batch) -> None:
        if not batch.total_changes:
            return
        apply_change_batch_exactly_once(
            dest_type="postgresql",
            dest_cfg=pg_cfg,
            dest_table=destination,
            change=batch,
            mappings=[
                {"source": name, "target": name, "confidence": 1.0}
                for name in ("id", "amount", "extra")
            ],
            column_types={"id": "int", "amount": "int", "extra": "string"},
            headers=["id", "amount", "extra"],
            pk_target_cols=["id"],
            cursor_key=destination_key,
        )

    try:
        if not bootstrap.is_available():
            pytest.skip("SQL Server native CDC is not reachable")
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL "
                    f"DROP TABLE dbo.[{table}]"
                )
                cur.execute(
                    f"CREATE TABLE dbo.[{table}] ("
                    "id INT NOT NULL PRIMARY KEY, amount INT NOT NULL)"
                )
                cur.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.databases
                        WHERE name = DB_NAME() AND is_cdc_enabled = 1
                    )
                        EXEC sys.sp_cdc_enable_db;
                    """
                )
                cur.execute(
                    """
                    EXEC sys.sp_cdc_enable_table
                        @source_schema = N'dbo',
                        @source_name = %s,
                        @role_name = NULL,
                        @capture_instance = %s,
                        @supports_net_changes = 0;
                    """,
                    (table, capture_a),
                )
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10)")
            conn.commit()

        _wait_for_capture(reader, capture_a)
        snapshot = list(reader.snapshot())
        assert sum(len(batch.inserts) for batch in snapshot) == 1
        for batch in snapshot:
            apply_to_postgres(batch)
        resume_before_r1 = snapshot[-1].resume_token
        reader.ack(snapshot[-1].resume_token)
        cursor_before_r1 = reader.start_lsn
        assert cursor_before_r1

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (2, 20)")
            conn.commit()
        reader.force_cdc_scan()

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE dbo.[{table}] ADD extra NVARCHAR(40) NULL")
                cur.execute(
                    """
                    EXEC sys.sp_cdc_enable_table
                        @source_schema = N'dbo',
                        @source_name = %s,
                        @role_name = NULL,
                        @capture_instance = %s,
                        @supports_net_changes = 0;
                    """,
                    (table, capture_b),
                )
            conn.commit()

        new_start_lsn = _wait_for_capture(reader, capture_b)
        assert compare_mssql_hex_lsn(cursor_before_r1, new_start_lsn) < 0
        reader.close()
        reader = SqlServerNativeCdc(
            cfg,
            table=table,
            primary_key="id",
            schema="dbo",
            batch_size=50,
            cursor_key=f"m3:capture-switch:{table}",
            resume_token=resume_before_r1,
        )
        assert reader.capture_instance == capture_a
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO dbo.[{table}] (id, amount, extra) "
                    "VALUES (3, 30, N'new schema')"
                )
            conn.commit()

        seen: dict[int, dict] = {}
        seen_counts: dict[int, int] = {}
        poll_trace: list[dict] = []
        empty_polls = 0
        deadline = time.monotonic() + 30.0
        try:
            while time.monotonic() < deadline and empty_polls < 3:
                reader.force_cdc_scan()
                batches = list(reader.poll())
                for batch in batches:
                    for row in (*batch.inserts, *batch.updates):
                        row_id = int(row["id"])
                        if row_id != 1:
                            seen[row_id] = row
                            seen_counts[row_id] = seen_counts.get(row_id, 0) + 1
                    apply_to_postgres(batch)
                    if batch.resume_token:
                        reader.ack(batch.resume_token)
                poll_trace.append(
                    {
                        "capture": reader.capture_instance,
                        "captures": dict(reader._captures),
                        "pending": dict(reader._pending_capture_switches),
                        "start_lsn": reader.start_lsn,
                        "delivered": sorted(seen),
                        "batch_tokens": [
                            decode_mssql_cdc_token(batch.resume_token).get("lsn")
                            for batch in batches
                            if batch.resume_token
                        ],
                    }
                )
                if set(seen) == {2, 3} and any(
                    batch.total_changes for batch in batches
                ):
                    empty_polls = 0
                elif set(seen) != {2, 3}:
                    empty_polls = 0
                else:
                    empty_polls += 1
                time.sleep(0.25)
        except Exception as exc:
            pytest.fail(
                f"capture switch poll raised {type(exc).__name__}: {exc}; "
                f"delivered_ids={sorted(seen)}; "
                f"old={capture_a}; new={capture_b}; "
                f"cursor_before_r1={cursor_before_r1}; "
                f"new_start_lsn={new_start_lsn}; trace={poll_trace}"
            )

        assert set(seen) == {2, 3}, (
            f"capture switch did not deliver both old/new changes: "
            f"delivered_ids={sorted(seen)}; old={capture_a}; new={capture_b}; "
            f"cursor_before_r1={cursor_before_r1}; new_start_lsn={new_start_lsn}; "
            f"trace={poll_trace}"
        )
        assert seen[3]["extra"] == "new schema"
        assert seen_counts == {2: 1, 3: 1}

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, amount, extra FROM dbo.[{table}] ORDER BY id"
                )
                source_rows = [
                    (int(row[0]), int(row[1]), row[2]) for row in cur.fetchall()
                ]
        from connectors.generic_sql import _engine
        from services.engine_pool import release_engine
        from sqlalchemy import text

        engine = _engine(pg_cfg)
        try:
            with engine.connect() as conn:
                destination_rows = [
                    (int(row[0]), int(row[1]), row[2])
                    for row in conn.execute(
                        text(
                            f'SELECT "id", "amount", "extra" FROM "{destination}" '
                            'ORDER BY "id"'
                        )
                    )
                ]
        finally:
            release_engine(engine)
        assert destination_rows == source_rows

        from services.cdc_mapping_review import list_reviews
        from services.cdc_schema_history import list_history

        switch_ddl = f"cdc.capture_instance switch {capture_a}->{capture_b}"
        history = list_history(reader.source_key, f"dbo.{table}")
        switch_entries = [
            entry for entry in history if entry.get("ddl") == switch_ddl
        ]
        assert len(switch_entries) == 1
        reviews = list_reviews(source_key=reader.source_key, status="all")
        switch_reviews = [
            review
            for review in reviews
            if review.get("table") == f"dbo.{table}"
            and review.get("ddl") == switch_ddl
            and review.get("reason") == "cdc_schema_drift"
        ]
        assert len(switch_reviews) == 1
    finally:
        try:
            reader.close()
        except Exception:
            pass
        try:
            with bootstrap._conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.execute(
                            """
                            EXEC sys.sp_cdc_disable_table
                                @source_schema = N'dbo',
                                @source_name = %s,
                                @capture_instance = N'all';
                            """,
                            (table,),
                        )
                    except Exception:
                        pass
                    cur.execute(
                        f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL "
                        f"DROP TABLE dbo.[{table}]"
                    )
                conn.commit()
        finally:
            bootstrap.close()
            _drop("postgresql", [destination])


def _wait_for_capture(
    reader: SqlServerNativeCdc,
    capture_instance: str,
    timeout: float = 30.0,
) -> str:
    deadline = time.monotonic() + timeout
    last_start_lsn = ""
    while time.monotonic() < deadline:
        reader.force_cdc_scan()
        with reader._conn() as conn:
            with conn.cursor() as cur:
                last_start_lsn = reader._capture_start_lsn_for(cur, capture_instance)
                min_lsn = reader._min_lsn_for(cur, capture_instance)
        if last_start_lsn and min_lsn:
            return last_start_lsn
        time.sleep(0.25)
    raise AssertionError(
        f"capture instance {capture_instance!r} did not become ready "
        f"(start_lsn={last_start_lsn!r})"
    )


RESUME_LSN = "00000000000000000010"
ORDERS_START_LSN = "00000000000000000020"
USERS_START_LSN = "00000000000000000030"
SWITCH_CEILING_LSN = "0000000000000000001f"
MAX_LSN = "00000000000000000040"
CHANGE_LSN = "00000000000000000015"
CHANGE_COLUMNS = [
    ("__$start_lsn",),
    ("__$seqval",),
    ("__$operation",),
    ("id",),
    ("amount",),
]


def _unit_reader(*, table="orders", capture_instance="dbo_orders_v1", resume=RESUME_LSN):
    reader = SqlServerNativeCdc(
        CFG,
        table=table,
        primary_key="id",
        primary_keys={"orders": "id", "users": "id"} if isinstance(table, list) else None,
        schema="dbo",
        capture_instance=capture_instance,
    )
    reader.phase = "streaming"
    reader.start_lsn = resume
    return reader


def _connection_with_cursor(cursor: MagicMock):
    @contextmanager
    def connection():
        conn = MagicMock()

        @contextmanager
        def cursor_context():
            yield cursor

        conn.cursor = cursor_context
        yield conn

    return connection


def _change_row(row_id: int) -> tuple:
    return (
        bytes.fromhex(CHANGE_LSN),
        b"\x01",
        2,
        str(row_id),
        str(row_id * 10),
    )


def test_old_capture_missing_before_drain_fails_with_both_instance_names(
    monkeypatch,
) -> None:
    reader = _unit_reader()
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, _table: [("dbo_orders_v2", ORDERS_START_LSN)],
    )

    with pytest.raises(CdcLsnGapError) as exc:
        reader._resolve_capture_for_table(MagicMock(), "orders")

    message = str(exc.value)
    assert "dbo_orders_v1" in message
    assert "dbo_orders_v2" in message
    assert "Re-snapshot" in message


def test_null_new_capture_start_drains_old_then_switches_when_ready(
    monkeypatch, caplog
) -> None:
    reader = _unit_reader()
    cursor = MagicMock()
    cursor.description = CHANGE_COLUMNS
    cursor.fetchall.return_value = [_change_row(2)]
    capture_catalog = [
        ("dbo_orders_v2", ""),
        ("dbo_orders_v1", RESUME_LSN),
    ]
    history = MagicMock()
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, _table: capture_catalog,
    )
    monkeypatch.setattr(reader, "_conn", _connection_with_cursor(cursor))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(reader, "_capture_instance_exists", lambda *_a: True)
    monkeypatch.setattr(reader, "_min_lsn", lambda _cur: RESUME_LSN)
    monkeypatch.setattr(reader, "_max_lsn", lambda _cur: MAX_LSN)
    monkeypatch.setattr(
        reader, "_changes_tvf", lambda: f"cdc.fn_{reader.capture_instance}"
    )
    monkeypatch.setattr(
        reader,
        "_decrement_lsn_for",
        lambda _cur, _lsn: SWITCH_CEILING_LSN,
    )
    monkeypatch.setattr(reader, "_maybe_record_capture_schema", history)
    caplog.set_level(logging.INFO)

    assert reader._resolve_capture_for_table(cursor, "orders") == "dbo_orders_v1"
    assert reader._resolve_capture_for_table(cursor, "orders") == "dbo_orders_v1"
    assert not reader._pending_capture_switches

    batches = list(reader._poll_once())
    assert batches[0].inserts[0]["id"] == "2"
    assert any(
        "cdc.fn_dbo_orders_v1" in call.args[0]
        for call in cursor.execute.call_args_list
    )
    assert not any(call.kwargs.get("force") for call in history.call_args_list)
    assert sum("switch deferred" in record.getMessage() for record in caplog.records) == 1

    reader.ack(batches[0].resume_token)
    capture_catalog[:] = [
        ("dbo_orders_v2", ORDERS_START_LSN),
        ("dbo_orders_v1", RESUME_LSN),
    ]
    cursor.fetchall.return_value = []
    switched_batches = list(reader._poll_once())

    assert reader.capture_instance == "dbo_orders_v2"
    assert not reader._pending_capture_switches
    assert any(call.kwargs.get("force") for call in history.call_args_list)
    assert (
        decode_mssql_cdc_token(switched_batches[0].resume_token)["capture_instance"]
        == "dbo_orders_v2"
    )


def test_shared_null_new_capture_start_drains_old_then_switches_when_ready(
    monkeypatch, caplog
) -> None:
    reader = _unit_reader(
        table=["orders", "users"],
        capture_instance="dbo_orders_v1",
    )
    reader._captures = {"orders": "dbo_orders_v1", "users": "dbo_users_v1"}
    cursor = MagicMock()
    cursor.description = CHANGE_COLUMNS
    cursor.fetchall.side_effect = [[_change_row(2)], []]
    capture_catalog = {
        "orders": [
            ("dbo_orders_v2", ""),
            ("dbo_orders_v1", RESUME_LSN),
        ],
        "users": [("dbo_users_v1", RESUME_LSN)],
    }
    history = MagicMock()
    monkeypatch.setattr(reader, "_conn", _connection_with_cursor(cursor))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, table: capture_catalog[table],
    )
    monkeypatch.setattr(reader, "_capture_instance_exists", lambda *_a: True)
    monkeypatch.setattr(reader, "_min_lsn_for", lambda *_a: RESUME_LSN)
    monkeypatch.setattr(reader, "_max_lsn", lambda _cur: MAX_LSN)
    monkeypatch.setattr(
        reader,
        "_decrement_lsn_for",
        lambda _cur, _lsn: SWITCH_CEILING_LSN,
    )
    monkeypatch.setattr(
        reader, "_changes_tvf_for", lambda capture: f"cdc.fn_{capture}"
    )
    monkeypatch.setattr(reader, "_maybe_record_capture_schema", history)
    caplog.set_level(logging.INFO)

    assert (
        reader._resolve_capture_for_table(cursor, "orders") == "dbo_orders_v1"
    )
    assert (
        reader._resolve_capture_for_table(cursor, "orders") == "dbo_orders_v1"
    )
    assert not reader._pending_capture_switches

    batches = list(reader._poll_shared_multi())
    assert batches[0].inserts[0]["id"] == "2"
    assert any(
        "cdc.fn_dbo_orders_v1" in call.args[0]
        for call in cursor.execute.call_args_list
    )
    assert reader._captures["orders"] == "dbo_orders_v1"
    assert not reader._pending_capture_switches
    assert sum("switch deferred" in record.getMessage() for record in caplog.records) == 1

    reader.ack(batches[0].resume_token)
    capture_catalog["orders"] = [
        ("dbo_orders_v2", ORDERS_START_LSN),
        ("dbo_orders_v1", RESUME_LSN),
    ]
    cursor.fetchall.side_effect = [[], []]
    switched_batches = list(reader._poll_shared_multi())

    assert reader._captures["orders"] == "dbo_orders_v2"
    assert not reader._pending_capture_switches
    assert any(call.kwargs.get("force") for call in history.call_args_list)
    assert (
        decode_mssql_cdc_token(switched_batches[0].resume_token)["capture_instances"][
            "orders"
        ]
        == "dbo_orders_v2"
    )


def test_missing_old_capture_and_null_new_start_returns_heartbeat(
    monkeypatch,
) -> None:
    reader = _unit_reader()
    cursor = MagicMock()
    cursor.description = CHANGE_COLUMNS
    monkeypatch.setattr(reader, "_conn", _connection_with_cursor(cursor))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, _table: [("dbo_orders_v2", "")],
    )
    monkeypatch.setattr(reader, "_capture_instance_exists", lambda *_a: True)
    monkeypatch.setattr(reader, "_min_lsn", lambda _cur: ORDERS_START_LSN)
    monkeypatch.setattr(reader, "_max_lsn", lambda _cur: MAX_LSN)
    monkeypatch.setattr(reader, "_maybe_record_capture_schema", lambda *_a, **_k: None)

    batches = list(reader._poll_once())

    assert len(batches) == 1
    assert batches[0].total_changes == 0
    assert reader.start_lsn == RESUME_LSN
    assert (
        compare_mssql_hex_lsn(
            decode_mssql_cdc_token(batches[0].resume_token)["lsn"], RESUME_LSN
        )
        == 0
    )


def test_shared_missing_old_capture_and_null_start_returns_heartbeat(
    monkeypatch,
) -> None:
    reader = _unit_reader(
        table=["orders", "users"],
        capture_instance="dbo_orders_v1",
    )
    reader._captures = {"orders": "dbo_orders_v1", "users": "dbo_users_v1"}
    cursor = MagicMock()
    monkeypatch.setattr(reader, "_conn", _connection_with_cursor(cursor))
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, table: (
            [("dbo_orders_v2", "")]
            if table == "orders"
            else [("dbo_users_v1", RESUME_LSN)]
        ),
    )
    monkeypatch.setattr(
        reader,
        "_capture_instance_exists",
        lambda *_a: pytest.fail("waiting for start_lsn should skip capture probes"),
    )
    monkeypatch.setattr(
        reader,
        "_min_lsn_for",
        lambda *_a: pytest.fail("waiting for start_lsn should skip retention probes"),
    )

    batches = list(reader._poll_shared_multi())

    assert len(batches) == 1
    assert batches[0].total_changes == 0
    assert batches[0].ack_barrier
    assert reader.start_lsn == RESUME_LSN
    assert (
        compare_mssql_hex_lsn(
            decode_mssql_cdc_token(batches[0].resume_token)["lsn"], RESUME_LSN
        )
        == 0
    )


def test_capture_instance_survives_switch_token_round_trip(monkeypatch) -> None:
    reader = _unit_reader()
    monkeypatch.setattr(reader, "_maybe_record_capture_schema", lambda *_a, **_k: None)

    reader._switch_capture_instance(
        MagicMock(),
        table="orders",
        old_capture="dbo_orders_v1",
        new_capture="dbo_orders_v2",
        switch_lsn=ORDERS_START_LSN,
    )

    state = decode_mssql_cdc_token(reader.resume_token)
    assert state["capture_instance"] == "dbo_orders_v2"
    restored = SqlServerNativeCdc(
        CFG,
        table="orders",
        primary_key="id",
        schema="dbo",
        resume_token=reader.resume_token,
    )
    assert restored.capture_instance == "dbo_orders_v2"
    assert reader._lease.meta["capture_instance"] == "dbo_orders_v2"


def test_one_capture_instance_keeps_existing_selection(monkeypatch) -> None:
    reader = _unit_reader(capture_instance="dbo_orders")
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, _table: [("dbo_orders", RESUME_LSN)],
    )
    monkeypatch.setattr(
        reader,
        "_maybe_record_capture_schema",
        lambda *_a, **_k: pytest.fail("single capture must not record a switch"),
    )

    assert reader._resolve_capture_for_table(MagicMock(), "orders") == "dbo_orders"
    assert reader.capture_instance == "dbo_orders"
    assert not reader._pending_capture_switches


def test_newest_capture_does_not_repeat_completed_switch(monkeypatch) -> None:
    reader = _unit_reader(
        capture_instance="dbo_orders_v2",
        resume=ORDERS_START_LSN,
    )
    reader._captures["orders"] = "dbo_orders_v2"
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, _table: [
            ("dbo_orders_v2", ORDERS_START_LSN),
            ("dbo_orders_v1", RESUME_LSN),
        ],
    )
    monkeypatch.setattr(
        reader,
        "_maybe_record_capture_schema",
        lambda *_a, **_k: pytest.fail("completed switch must not be recorded twice"),
    )

    assert reader._resolve_capture_for_table(MagicMock(), "orders") == "dbo_orders_v2"
    assert reader.capture_instance == "dbo_orders_v2"


def test_shared_poll_caps_old_instance_and_switches_only_reached_table(
    monkeypatch,
) -> None:
    reader = _unit_reader(
        table=["orders", "users"],
        capture_instance="dbo_orders_v1",
    )
    reader._captures = {"orders": "dbo_orders_v1", "users": "dbo_users_v1"}
    cursor = MagicMock()
    cursor.description = []
    cursor.fetchall.return_value = []

    @contextmanager
    def connection() -> Iterator[MagicMock]:
        conn = MagicMock()

        @contextmanager
        def cursor_context():
            yield cursor

        conn.cursor = cursor_context
        yield conn

    captures = {
        "orders": [
            ("dbo_orders_v2", ORDERS_START_LSN),
            ("dbo_orders_v1", RESUME_LSN),
        ],
        "users": [
            ("dbo_users_v2", USERS_START_LSN),
            ("dbo_users_v1", RESUME_LSN),
        ],
    }
    monkeypatch.setattr(reader, "_conn", connection)
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(
        reader,
        "_capture_instances_for_table",
        lambda _cur, table: captures[table],
    )
    monkeypatch.setattr(reader, "_capture_instance_exists", lambda *_a: True)
    monkeypatch.setattr(reader, "_min_lsn_for", lambda *_a: RESUME_LSN)
    monkeypatch.setattr(reader, "_max_lsn", lambda *_a: MAX_LSN)
    monkeypatch.setattr(
        reader, "_decrement_lsn_for", lambda _cur, _lsn: SWITCH_CEILING_LSN
    )
    monkeypatch.setattr(reader, "_changes_tvf_for", lambda capture: f"cdc.fn_{capture}")
    monkeypatch.setattr(reader, "_maybe_record_capture_schema", lambda *_a, **_k: None)

    batches = list(reader._poll_shared_multi())

    assert reader._captures == {
        "orders": "dbo_orders_v2",
        "users": "dbo_users_v1",
    }
    assert reader.start_lsn == ORDERS_START_LSN
    assert reader._pending_capture_switches == {
        "users": ("dbo_users_v2", USERS_START_LSN)
    }
    assert decode_mssql_cdc_token(batches[-1].resume_token)["capture_instances"] == {
        "orders": "dbo_orders_v2",
        "users": "dbo_users_v1",
    }
    tvf_params = [
        call.args[1]
        for call in cursor.execute.call_args_list
        if len(call.args) > 1 and isinstance(call.args[1], tuple) and len(call.args[1]) == 3
    ]
    assert len(tvf_params) == 2
    assert all(params[1] == bytes.fromhex(SWITCH_CEILING_LSN) for params in tvf_params)
