"""Unit proofs for SQL Server native CDC (capture discovery, LSN boundary, tokens)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
    encode_mssql_cdc_token,
)
from connectors.sqlserver_change_stream import encode_sqlserver_resume_token
from connectors.writer_common import extract_cdc_lsn
from services.value_serializer import SQL_NULL_SENTINEL


def test_mssql_cdc_token_roundtrip_with_offset_and_capture() -> None:
    token = encode_mssql_cdc_token(
        "aabb",
        table="orders",
        phase="snapshot",
        offset=50,
        seqval="01",
        capture_instance="dbo_orders",
    )
    state = decode_mssql_cdc_token(token)
    assert state["lsn"] == "aabb"
    assert state["phase"] == "snapshot"
    assert state["offset"] == 50
    assert state["seqval"] == "01"
    assert state["capture_instance"] == "dbo_orders"


def test_extract_cdc_lsn_mssql_native_and_ct_json() -> None:
    native = encode_mssql_cdc_token("deadbeef", table="orders", phase="streaming")
    assert extract_cdc_lsn(native) == "deadbeef"
    ct = encode_sqlserver_resume_token(42, table="orders", phase="streaming")
    assert extract_cdc_lsn(ct) == f"{42:020d}"
    assert extract_cdc_lsn({"kind": "mssql-cdc", "lsn": "abc"}) == "abc"
    assert extract_cdc_lsn({"version": 7}) == f"{7:020d}"


def test_truncate_at_lsn_boundary_does_not_split_txn() -> None:
    cols = ["__$start_lsn", "__$seqval", "__$operation", "id"]
    lsn_a = bytes.fromhex("0a")
    lsn_b = bytes.fromhex("0b")
    # batch_size=2 but rows 2 and 3 share LSN B with look-ahead → drop incomplete B
    rows = [
        (lsn_a, bytes.fromhex("01"), 2, 1),
        (lsn_b, bytes.fromhex("01"), 2, 2),
        (lsn_b, bytes.fromhex("02"), 2, 3),
    ]
    keep, next_lsn, next_seq = SqlServerNativeCdc._truncate_at_lsn_boundary(rows, cols, 2)
    assert len(keep) == 1
    assert keep[0][3] == 1
    assert next_lsn == "0a"


def test_native_snapshot_handoff_and_capture_resolve() -> None:
    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=2,
        capture_instance="wrong_name",
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    # resolve capture, max_lsn, schema columns, page1, page2, empty
    cur.fetchone.side_effect = [
        (bytes.fromhex("0abc"),),  # max lsn
        (bytes.fromhex("0abc"),),  # min lsn (floor)
        (bytes.fromhex("0abc"),),  # capture start lsn
    ]
    cur.fetchall.side_effect = [
        [("dbo_orders", bytes.fromhex("0abc"))],  # resolve capture instances
        [],  # captured_columns
        [("1", "10"), ("2", "20")],
        [("3", "30")],
        [],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(
        cdc, "_acquire_cdc_lease"
    ):
        batches = list(cdc.snapshot())

    assert cdc.capture_instance == "dbo_orders"
    assert sum(len(b.inserts) for b in batches) == 3
    assert decode_mssql_cdc_token(batches[0].resume_token)["phase"] == "snapshot"
    assert decode_mssql_cdc_token(batches[0].resume_token)["offset"] == 2
    handoff = decode_mssql_cdc_token(batches[-1].resume_token)
    assert handoff["phase"] == "streaming"
    assert handoff["lsn"] == "0abc"
    assert handoff["capture_instance"] == "dbo_orders"


def test_native_poll_ops_1_2_4() -> None:
    token = encode_mssql_cdc_token(
        "0a", table="orders", phase="streaming", capture_instance="dbo_orders"
    )
    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app", "job_id": "j1"},
        table="orders",
        primary_key="id",
        resume_token=token,
        capture_instance="dbo_orders",
    )
    conn = MagicMock()
    cur = MagicMock()
    lsn = bytes.fromhex("0b")
    cur.description = [
        ("__$start_lsn",),
        ("__$seqval",),
        ("__$operation",),
        ("id",),
        ("amount",),
    ]
    # resolve capture, min_lsn (arg eval), max_lsn, then change rows
    cur.fetchone.side_effect = [
        (1,),  # capture instance exists
        (bytes.fromhex("0a"),),  # min_lsn ≤ resume
        (bytes.fromhex("0c"),),  # max_lsn
    ]
    cur.fetchall.side_effect = [
        [("dbo_orders", bytes.fromhex("0a"))],  # resolve capture instances
        [],  # captured_columns
        [
            (lsn, bytes.fromhex("01"), 2, "3", "30"),
            (lsn, bytes.fromhex("02"), 4, "1", "99"),
            (lsn, bytes.fromhex("03"), 1, "2", "20"),
        ],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(
        cdc, "_acquire_cdc_lease"
    ), patch(
        "services.cdc_incremental_runner.interleave_incremental_snapshot",
        return_value=iter(()),
    ):
        batches = list(cdc.poll())

    assert len(batches) == 1
    batch = batches[0]
    assert any(r.get("id") == "3" for r in batch.inserts)
    assert any(r.get("id") == "1" for r in batch.updates)
    assert "2" in batch.deletes
    state = decode_mssql_cdc_token(batch.resume_token)
    assert state["lsn"] == "0b"
    assert state["capture_instance"] == "dbo_orders"


def test_normalize_and_classify_before_image_and_net() -> None:
    from connectors.sqlserver_cdc_native import (
        ROW_FILTER_ALL_UPDATE_OLD,
        ROW_FILTER_NET,
        classify_mssql_cdc_rows,
        normalize_mssql_row_filter,
    )

    assert normalize_mssql_row_filter("all_update_old") == ROW_FILTER_ALL_UPDATE_OLD
    assert normalize_mssql_row_filter("net-changes") == ROW_FILTER_NET

    rows = [
        {"__$operation": 3, "id": "1", "amount": "10"},
        {"__$operation": 4, "id": "1", "amount": "99"},
        {"__$operation": 2, "id": "2", "amount": "5"},
        {"__$operation": 1, "id": "3", "amount": "0"},
    ]
    inserts, updates, deletes = classify_mssql_cdc_rows(
        rows, primary_key="id", row_filter=ROW_FILTER_ALL_UPDATE_OLD
    )
    assert len(inserts) == 1 and inserts[0]["id"] == "2"
    assert deletes == ["3"]
    assert len(updates) == 1
    assert updates[0]["amount"] == "99"
    assert updates[0]["_df_before"]["amount"] == "10"

    # Without before-image mode, op 3 is ignored.
    _, updates2, _ = classify_mssql_cdc_rows(
        rows, primary_key="id", row_filter="all"
    )
    assert "_df_before" not in updates2[0]

    cdc = SqlServerNativeCdc(
        {"database": "app", "cdc_row_filter": "net"},
        table="orders",
        primary_key="id",
    )
    assert cdc.row_filter == ROW_FILTER_NET
    assert "fn_cdc_get_net_changes_" in cdc._changes_tvf()


def test_native_cdc_binary_wire_roundtrips_bytes_and_empty_values() -> None:
    from connectors.sql_bind import coerce_binary_wire
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    payload = b"\x00\xffsqlserver-binary\x80"
    inserts, updates, deletes = classify_mssql_cdc_rows(
        [
            {"__$operation": 2, "id": 1, "payload": payload},
            {"__$operation": 2, "id": 2, "payload": b""},
            {"__$operation": 2, "id": 3, "payload": None},
        ],
        primary_key="id",
        row_filter="all",
    )

    assert not updates
    assert not deletes
    by_id = {record["id"]: record["payload"] for record in inserts}
    assert coerce_binary_wire(by_id["1"]) == payload
    assert coerce_binary_wire(by_id["2"]) == b""
    # SQL NULL stays NULL on the wire, distinct from b"" (D203).
    assert by_id["3"] == SQL_NULL_SENTINEL


def test_native_cdc_non_binary_values_preserve_legacy_wire_format() -> None:
    from datetime import date, datetime
    from decimal import Decimal
    from uuid import UUID

    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    values = {
        "null_value": None,
        "timestamp": datetime(2024, 3, 4, 5, 6, 7, 890123),
        "event_date": date(2024, 3, 4),
        "enabled": True,
        "amount": Decimal("123.4500"),
        "guid": UUID("3a95e6d0-0200-4b2c-a7f9-15b3e8de4c11"),
        "text": "unchanged",
    }
    inserts, updates, deletes = classify_mssql_cdc_rows(
        [{"__$operation": 2, "id": 1, **values}],
        primary_key="id",
        row_filter="all",
    )

    assert not updates
    assert not deletes
    expected = {
        name: SQL_NULL_SENTINEL if value is None else str(value)
        for name, value in values.items()
    }
    assert {name: inserts[0][name] for name in values} == expected


def test_native_snapshot_binary_wire_roundtrips_bytes_and_empty_values() -> None:
    from connectors.sql_bind import coerce_binary_wire
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        batch_size=10,
    )
    cur = MagicMock()
    cur.description = [
        ("id",),
        ("payload",),
        ("empty_payload",),
        ("null_payload",),
    ]

    with patch(
        "connectors.sql_snapshot_scan.fetch_scan_page",
        return_value=[(1, b"\x00\xffsnapshot", b"", None)],
    ):
        batches = list(
            cdc._iter_snapshot_table(
                cur,
                "orders",
                handoff="0a",
                offset=0,
                last_pk="",
                ack_barrier=False,
            )
        )

    record = batches[0].inserts[0]
    assert coerce_binary_wire(record["payload"]) == b"\x00\xffsnapshot"
    assert coerce_binary_wire(record["empty_payload"]) == b""
    assert record["null_payload"] == SQL_NULL_SENTINEL


def test_native_incremental_snapshot_preserves_legacy_values_and_binary_wire() -> None:
    from datetime import datetime
    from decimal import Decimal

    from connectors.sql_bind import coerce_binary_wire

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
    )

    class _Sig:
        primary_key = "id"
        chunk_size = 2
        last_pk = ""
        table = "orders"

    conn = MagicMock()
    cur = MagicMock()
    cur.description = [
        ("id",),
        ("created_at",),
        ("enabled",),
        ("amount",),
        ("payload",),
        ("null_payload",),
    ]
    payload = b"\x00\xffincremental"
    cur.fetchall.return_value = [
        (
            1,
            datetime(2024, 3, 4, 5, 6, 7, 890123),
            True,
            Decimal("123.4500"),
            payload,
            None,
        )
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        records, new_last, done = cdc._fetch_incremental_chunk(_Sig())

    record = records[0]
    assert record["created_at"] == "2024-03-04 05:06:07.890123"
    assert record["enabled"] == "True"
    assert record["amount"] == "123.4500"
    assert coerce_binary_wire(record["payload"]) == payload
    assert record["null_payload"] == SQL_NULL_SENTINEL
    assert new_last == "1"
    assert done is True


def test_native_cdc_update_old_before_image_nulls_are_not_written() -> None:
    from connectors.sql_bind import coerce_binary_wire
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    payload = b"\x00\xfeafter-image"
    inserts, updates, deletes = classify_mssql_cdc_rows(
        [
            {"__$operation": 3, "id": 1, "payload": None, "large_value": None},
            {"__$operation": 4, "id": 1, "payload": payload, "large_value": "new"},
        ],
        primary_key="id",
        row_filter="all update old",
    )

    assert not inserts
    assert not deletes
    assert len(updates) == 1
    assert coerce_binary_wire(updates[0]["payload"]) == payload
    assert updates[0]["large_value"] == "new"
    assert updates[0]["_df_before"]["payload"] == SQL_NULL_SENTINEL
    assert updates[0]["_df_before"]["large_value"] == SQL_NULL_SENTINEL


def test_native_cdc_legacy_value_redelivery_is_already_committed(tmp_path) -> None:
    from datetime import datetime
    from decimal import Decimal

    from connectors.cdc_eos_sql import (
        apply_change_batch_exactly_once,
        dest_engine_count,
    )
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows
    from services.cdc_engine import ChangeBatch

    source_values = {
        "created_at": datetime(2024, 3, 4, 5, 6, 7, 890123),
        "enabled": True,
        "amount": Decimal("123.4500"),
    }
    legacy_row = {
        "id": "1",
        **{
            name: "" if value is None else str(value)
            for name, value in source_values.items()
        },
    }
    token = {"lsn": "0/3A", "phase": "streaming"}
    dest_cfg = {"database": str(tmp_path / "serializer-upgrade.db")}
    columns = ["id", *source_values]
    mappings = [
        {"source": name, "target": name, "confidence": 1.0}
        for name in columns
    ]
    column_types = dict.fromkeys(columns, "string")

    rows, _checksum, committed, _deleted = apply_change_batch_exactly_once(
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="orders",
        change=ChangeBatch(inserts=[legacy_row], resume_token=token),
        mappings=mappings,
        column_types=column_types,
        headers=columns,
        pk_target_cols=["id"],
        cursor_key="serializer-upgrade|orders",
    )
    assert rows == 1
    assert committed["exactly_once_active"] is True

    inserts, updates, deletes = classify_mssql_cdc_rows(
        [{"__$operation": 2, "id": 1, **source_values}],
        primary_key="id",
        row_filter="all",
    )
    assert not updates
    assert not deletes
    rows2, _checksum2, replayed, _deleted2 = apply_change_batch_exactly_once(
        dest_type="sqlite",
        dest_cfg=dest_cfg,
        dest_table="orders",
        change=ChangeBatch(inserts=inserts, resume_token=token),
        mappings=mappings,
        column_types=column_types,
        headers=columns,
        pk_target_cols=["id"],
        cursor_key="serializer-upgrade|orders",
    )

    assert replayed["eos_already_committed"] is True
    assert rows2 == 0
    assert dest_engine_count(dest_cfg, "orders") == 1


def test_mssql_shared_reader_init_and_lease() -> None:
    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app", "job_id": "j1"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        cursor_key="cdc-shared:sqlserver:app:x:j1",
    )
    assert cdc.tables == ["orders", "users"]
    assert cdc._shared is True
    assert cdc.primary_keys["users"] == "user_id"
    assert cdc._lease.meta.get("shared_reader") is True
    assert "mssql_cdc_shared:" in cdc._lease.resource


def test_truncate_tagged_at_lsn_boundary() -> None:
    tagged = [
        ("0a", "01", "orders", {"__$operation": 2, "id": "1"}),
        ("0b", "01", "orders", {"__$operation": 2, "id": "2"}),
        ("0b", "02", "users", {"__$operation": 2, "id": "u1"}),
    ]
    keep = SqlServerNativeCdc._truncate_tagged_at_lsn_boundary(tagged, 2)
    assert len(keep) == 1
    assert keep[0][0] == "0a"


def test_mssql_shared_poll_demuxes_two_tables() -> None:
    token = encode_mssql_cdc_token(
        "0a", table="orders,users", phase="streaming", capture_instance=""
    )
    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        resume_token=token,
        batch_size=50,
    )
    cdc.phase = "streaming"
    cdc.start_lsn = "0a"

    lsn_b = bytes.fromhex("0b")
    orders_rows = [
        (lsn_b, bytes.fromhex("01"), 2, 1, "10"),
    ]
    users_rows = [
        (lsn_b, bytes.fromhex("02"), 2, 9, "alice"),
    ]

    conn = MagicMock()
    cur = MagicMock()
    tvf_calls = {"n": 0}

    def _execute(sql, params=None):
        sql_s = str(sql)
        if "fn_cdc_get_all_changes_dbo_orders" in sql_s:
            tvf_calls["n"] = 1
            cur.description = [
                ("__$start_lsn",),
                ("__$seqval",),
                ("__$operation",),
                ("id",),
                ("amount",),
            ]
        elif "fn_cdc_get_all_changes_dbo_users" in sql_s:
            tvf_calls["n"] = 2
            cur.description = [
                ("__$start_lsn",),
                ("__$seqval",),
                ("__$operation",),
                ("user_id",),
                ("name",),
            ]

    def _fetchall():
        if tvf_calls["n"] == 1:
            return orders_rows
        if tvf_calls["n"] == 2:
            return users_rows
        return []

    cur.execute.side_effect = _execute
    cur.fetchone.side_effect = [
        ("dbo_orders",),
        ("dbo_users",),
        (1,),  # orders capture exists
        (1,),  # users capture exists
        (bytes.fromhex("0a"),),  # min orders
        (bytes.fromhex("0a"),),  # min users
        (bytes.fromhex("0b"),),  # max_lsn
    ]
    cur.fetchall.side_effect = _fetchall
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            batches = list(cdc.poll())

    assert len(batches) == 2
    by_table = {b.table: b for b in batches}
    assert "orders" in by_table and "users" in by_table
    assert by_table["orders"].ack_barrier is False
    assert by_table["users"].ack_barrier is True
    assert by_table["orders"].inserts[0]["id"] == "1"
    assert by_table["users"].inserts[0]["user_id"] == "9"
    assert by_table["orders"].resume_token == by_table["users"].resume_token


def test_compare_mssql_hex_lsn_orders_binary() -> None:
    from connectors.sqlserver_cdc_native import compare_mssql_hex_lsn

    assert compare_mssql_hex_lsn("0a", "0b") == -1
    assert compare_mssql_hex_lsn("0b", "0a") == 1
    assert compare_mssql_hex_lsn("0a", "0a") == 0
    # Unequal length: pad high bytes
    assert compare_mssql_hex_lsn("0a", "000b") == -1


def test_assert_resume_lsn_in_retention_raises_on_gap() -> None:
    from connectors.sqlserver_cdc_native import (
        CdcLsnGapError,
        assert_resume_lsn_in_retention,
    )

    assert_resume_lsn_in_retention("0b", "0a")  # resume ahead of min — ok
    assert_resume_lsn_in_retention("0a", "0a")  # equal — ok
    try:
        assert_resume_lsn_in_retention("0a", "0b")
        raise AssertionError("expected CdcLsnGapError")
    except CdcLsnGapError as exc:
        assert "min_lsn" in str(exc)
        assert "failover" in str(exc).lower() or "cleanup" in str(exc).lower()


def test_snapshot_handoff_clamps_to_capture_min_lsn() -> None:
    """New capture min_lsn can sit above stale database max_lsn until scan."""
    from connectors.sqlserver_cdc_native import SqlServerNativeCdc

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        capture_instance="dbo_orders",
    )
    cur = MagicMock()
    cur.fetchone.side_effect = [
        (bytes.fromhex("0a"),),  # max_lsn below floor
        (bytes.fromhex("0b"),),  # capture min_lsn
        (bytes.fromhex("0b"),),  # capture start_lsn
    ]
    assert cdc._snapshot_handoff_lsn(cur, "dbo_orders") == "0b"


def test_poll_fails_closed_when_resume_before_min_lsn() -> None:
    """AG failover / cleanup: cursor behind retention must not silently skip."""
    from connectors.sqlserver_cdc_native import CdcLsnGapError, SqlServerNativeCdc

    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=10,
        capture_instance="dbo_orders",
    )
    cdc.phase = "streaming"
    cdc.start_lsn = "0a"  # older than min
    cdc._capture_resolved = True

    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.side_effect = [
        (1,),  # capture instance exists
        (bytes.fromhex("0b"),),  # min_lsn
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            with patch.object(cdc, "_resolve_capture_instance"):
                with patch.object(cdc, "_maybe_record_capture_schema"):
                    try:
                        list(cdc.poll())
                        raise AssertionError("expected CdcLsnGapError")
                    except CdcLsnGapError:
                        pass


def _force_scan_connection(
    *, scan_error: Exception | None = None, flush_error: Exception | None = None
):
    events = []
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False

    def execute(statement):
        events.append(("execute", statement))
        if statement == "EXEC sys.sp_cdc_scan" and scan_error is not None:
            raise scan_error
        if statement == "EXEC sys.sp_replflush" and flush_error is not None:
            raise flush_error

    cursor.execute.side_effect = execute
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value = cursor
    conn.commit.side_effect = lambda: events.append(("commit",))
    conn.invalidate.side_effect = lambda: events.append(("invalidate",))
    return conn, events


def test_force_cdc_scan_releases_log_reader_ownership() -> None:
    cdc = SqlServerNativeCdc(
        {"database": "app"},
        table="orders",
        primary_key="id",
        capture_instance="dbo_orders",
        cursor_key="orders-to-warehouse",
    )
    conn, events = _force_scan_connection()

    with patch.object(cdc, "_conn", return_value=conn):
        cdc.force_cdc_scan()

    assert events == [
        ("execute", "EXEC sys.sp_cdc_scan"),
        ("commit",),
        ("execute", "EXEC sys.sp_replflush"),
        ("commit",),
    ]


def test_force_cdc_scan_does_not_flush_after_scan_error(caplog) -> None:
    cdc = SqlServerNativeCdc(
        {"database": "app"},
        table="orders",
        primary_key="id",
        capture_instance="dbo_orders",
    )
    conn, events = _force_scan_connection(
        scan_error=RuntimeError(22903, "capture agent owns sp_replcmds")
    )
    caplog.set_level(logging.DEBUG, logger="connectors.sqlserver_cdc_native")

    with patch.object(cdc, "_conn", return_value=conn):
        cdc.force_cdc_scan()

    assert events == [("execute", "EXEC sys.sp_cdc_scan")]
    assert any(
        record.levelno == logging.DEBUG and "sp_cdc_scan skipped" in record.message
        for record in caplog.records
    )


def test_force_cdc_scan_invalidates_connection_when_flush_fails(caplog) -> None:
    cdc = SqlServerNativeCdc(
        {"database": "app"},
        table="orders",
        primary_key="id",
        capture_instance="dbo_orders",
        cursor_key="orders-to-warehouse",
    )
    conn, events = _force_scan_connection(flush_error=ValueError("flush failed"))
    caplog.set_level(logging.WARNING, logger="connectors.sqlserver_cdc_native")

    with patch.object(cdc, "_conn", return_value=conn):
        cdc.force_cdc_scan()

    assert events == [
        ("execute", "EXEC sys.sp_cdc_scan"),
        ("commit",),
        ("execute", "EXEC sys.sp_replflush"),
        ("invalidate",),
    ]
    warning = next(
        record for record in caplog.records if record.levelno == logging.WARNING
    )
    assert "orders-to-warehouse" in warning.message
    assert "dbo_orders" in warning.message
    assert "ValueError" in warning.message
    assert "flush failed" in warning.message


def test_mssql_connection_adapter_delegates_invalidate() -> None:
    from connectors.generic_sql import _MssqlQmarkConnection

    proxy = MagicMock()
    _MssqlQmarkConnection(proxy).invalidate()

    proxy.invalidate.assert_called_once_with()
