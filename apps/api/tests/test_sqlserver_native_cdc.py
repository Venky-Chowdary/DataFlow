"""Unit proofs for SQL Server native CDC (capture discovery, LSN boundary, tokens)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
    encode_mssql_cdc_token,
)
from connectors.sqlserver_change_stream import encode_sqlserver_resume_token
from connectors.writer_common import extract_cdc_lsn


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
        ("dbo_orders",),  # resolve
        (bytes.fromhex("0abc"),),  # max lsn
    ]
    cur.fetchall.side_effect = [
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
    # resolve capture, schema history query (empty), max_lsn, then change rows
    cur.fetchone.side_effect = [
        ("dbo_orders",),
        (bytes.fromhex("0c"),),
    ]
    cur.fetchall.side_effect = [
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
