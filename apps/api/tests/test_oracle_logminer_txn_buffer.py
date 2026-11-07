"""Unit proofs for Oracle LogMiner commit buffering and restart positions."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

import connectors.oracle_logminer as oracle_logminer
from connectors.oracle_logminer import (
    OracleLogMinerReadError,
    OracleLogMinerCdc,
    _canonical_rs_id,
    _row_rs_id,
    decode_logminer_token,
    encode_logminer_token,
)
from connectors.oracle_logminer_txn import (
    OracleTxnBuffer,
    mining_position,
    oracle_txn_buffer_enabled,
    rs_id_sort_key,
)
from connectors.oracle_logminer import (
    logminer_contents_sql,
    logminer_txn_contents_sql,
)
from connectors.lsn_guards import (
    compare_lsn,
    extract_cdc_commit_position,
    lsn_sort_key,
    parse_oracle_rs_id,
)
from services.cdc_exactly_once import (
    batch_lsn,
    clamp_job_resume_to_dest,
    decide_eos_apply,
    encode_resume_blob,
)
from services.cdc_transaction_buffer import CdcTxnBufferOverflow


@pytest.fixture(autouse=True)
def _no_lob_schema_in_unit_readers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        OracleLogMinerCdc,
        "_lob_columns_for_table",
        lambda _self, _table: [],
    )


def _dml(
    buffer: OracleTxnBuffer,
    xid: str,
    scn: int,
    rs_id: str,
    ssn: int,
    row_id: str,
) -> None:
    buffer.feed(
        xid=xid,
        scn=scn,
        rs_id=rs_id,
        ssn=ssn,
        operation="INSERT",
        table="T",
        row={"ID": row_id},
    )


def test_oracle_txn_buffer_is_on_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    monkeypatch.delenv("DATAWRAP_CDC_ORACLE_TXN_BUFFER", raising=False)
    assert oracle_txn_buffer_enabled() is True
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "true")
    assert oracle_txn_buffer_enabled() is True


@pytest.mark.parametrize(
    ("raw", "enabled", "warns"),
    [
        (None, True, False),
        ("", True, False),
        ("0", False, False),
        ("false", False, False),
        ("OFF", False, False),
        ("no", False, False),
        ("1", True, False),
        ("true", True, False),
        ("garbage", True, True),
    ],
)
def test_oracle_txn_buffer_env_defaults_safely(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    raw: str | None,
    enabled: bool,
    warns: bool,
) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    monkeypatch.delenv("DATAWRAP_CDC_ORACLE_TXN_BUFFER", raising=False)
    if raw is not None:
        monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raw)

    with caplog.at_level(logging.WARNING):
        assert oracle_txn_buffer_enabled() is enabled

    if warns:
        assert raw in caplog.text
    else:
        assert "Unknown DATAFLOW_CDC_ORACLE_TXN_BUFFER" not in caplog.text


def _make_open_txn_reader(
    monkeypatch: pytest.MonkeyPatch, *, table: str = "T", shared: bool = False
) -> OracleLogMinerCdc:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    tables = ["T", "U"] if shared else table
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table=tables,
        primary_key="ID",
        primary_keys={"T": "ID", "U": "ID"} if shared else None,
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
        cursor_key="open-txn-hold-test",
    )
    reader.phase = "streaming"
    reader.scn = 100
    reader._open_txn_detection_checked = True
    return reader


def _open_txn_connection(*, head_scn: int = 200):
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (head_scn,)
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cur


def test_open_txn_detection_refuses_before_logminer_on_missing_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    reader._open_txn_detection_checked = False
    conn, cur = _open_txn_connection()

    def execute(sql: str) -> None:
        if "GV$INSTANCE" in sql.upper():
            cur.fetchone.return_value = (1,)
            return
        raise RuntimeError(
            'ORA-00942: table or view "SYS"."V_$TRANSACTION" does not exist'
        )

    cur.execute.side_effect = execute
    source_conn = MagicMock(return_value=conn)
    mining_conn = MagicMock(
        side_effect=AssertionError("LogMiner query must not run")
    )
    monkeypatch.setattr(reader, "_conn", source_conn)
    monkeypatch.setattr(reader, "_mining_conn", mining_conn)
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)

    with pytest.raises(OracleLogMinerReadError) as raised:
        list(reader._poll_once())

    assert getattr(raised.value, "code", None) == (
        "oracle_open_txn_detection_unavailable"
    )
    assert "SELECT ON V_$TRANSACTION" in str(raised.value)
    assert "unset DATAFLOW_CDC_ORACLE_TXN_BUFFER" in str(raised.value)
    assert source_conn.call_count == 2
    assert mining_conn.call_count == 0
    assert len(cur.execute.call_args_list) == 2
    assert "GV$INSTANCE" in cur.execute.call_args_list[0].args[0]
    assert "V$TRANSACTION" in cur.execute.call_args_list[1].args[0]


def test_oldest_open_transaction_holds_window_and_logs_no_row_values(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = _make_open_txn_reader(monkeypatch, table="ROW_SENTINEL")
    conn, cur = _open_txn_connection()
    cur.fetchone.return_value = (200, 150, 1, 2, 3, 12.5)
    monkeypatch.setattr(reader, "_conn", lambda: conn)

    with caplog.at_level(logging.WARNING, logger="connectors.oracle_logminer"):
        end_scn, held = reader._buffer_off_window_end()

    assert end_scn == 149
    assert held is True
    assert cur.execute.call_count == 1
    sql = cur.execute.call_args.args[0].upper()
    assert "SELECT D.CURRENT_SCN" in sql
    assert "FROM V$DATABASE D" in sql
    assert "V$TRANSACTION" in sql
    assert "cursor_key=open-txn-hold-test" in caplog.text
    assert "xid=1.2.3" in caplog.text
    assert "start_scn=150" in caplog.text
    assert "age_seconds=12.5" in caplog.text
    assert "held_end_scn=149" in caplog.text
    assert "ROW_SENTINEL" not in caplog.text
    reader._advance_offset(
        last_scn=0,
        last_rs_id="",
        last_ssn=0,
        end_scn=end_scn,
        fetched=0,
        limit=500,
    )
    assert reader.scn == 100
    assert reader.scn <= 149


def test_no_open_transaction_does_not_cap_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    conn, cur = _open_txn_connection()
    cur.fetchone.return_value = (200, None, None, None, None, None)
    monkeypatch.setattr(reader, "_conn", lambda: conn)

    assert reader._buffer_off_window_end() == (200, False)


def test_atomic_window_probe_failure_uses_typed_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    conn, cur = _open_txn_connection()
    cur.execute.side_effect = RuntimeError("ORA-01031: insufficient privileges")
    monkeypatch.setattr(reader, "_conn", lambda: conn)

    with pytest.raises(OracleLogMinerReadError) as raised:
        reader._buffer_off_window_end()

    assert getattr(raised.value, "code", None) == (
        "oracle_open_txn_detection_unavailable"
    )
    assert "SELECT ON V_$TRANSACTION" in str(raised.value)
    assert "unset DATAFLOW_CDC_ORACLE_TXN_BUFFER" in str(raised.value)


def test_buffer_off_window_uses_atomic_head_and_oldest_txn_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    conn, cur = _open_txn_connection()
    # The transaction is open at CURRENT_SCN=200 and commits before a later
    # V$TRANSACTION-only probe could observe it.
    cur.fetchone.return_value = (200, 150, 4, 5, 6, 20.0)
    monkeypatch.setattr(reader, "_conn", lambda: conn)
    monkeypatch.setattr(
        reader,
        "_probe_oldest_open_txn",
        lambda: pytest.fail("window sampling must not use a later probe"),
    )

    window_end = getattr(reader, "_buffer_off_window_end", None)
    assert callable(window_end)
    assert window_end() == (149, True)

    assert cur.execute.call_count == 1
    sql = cur.execute.call_args.args[0].upper()
    assert "SELECT D.CURRENT_SCN" in sql
    assert "LEFT JOIN" in sql
    assert "V$TRANSACTION" in sql


def test_buffer_off_startup_refuses_on_multiple_rac_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    reader._open_txn_detection_checked = False
    conn, cur = _open_txn_connection()
    cur.fetchone.return_value = (2,)
    monkeypatch.setattr(reader, "_conn", lambda: conn)
    monkeypatch.setattr(
        reader,
        "_probe_oldest_open_txn",
        lambda: pytest.fail("RAC must be refused before probing V$TRANSACTION"),
    )

    with pytest.raises(OracleLogMinerReadError) as raised:
        reader._ensure_open_txn_detection()

    assert getattr(raised.value, "code", None) == "oracle_buffer_off_rac_unsupported"
    assert "Oracle RAC" in str(raised.value)
    assert "V$TRANSACTION is instance-local" in str(raised.value)
    assert "unset DATAFLOW_CDC_ORACLE_TXN_BUFFER" in str(raised.value)
    assert cur.execute.call_count == 1
    assert "SELECT COUNT(*) FROM GV$INSTANCE" in cur.execute.call_args.args[0].upper()


def test_buffer_off_startup_accepts_single_rac_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    reader._open_txn_detection_checked = False
    conn, cur = _open_txn_connection()
    cur.fetchone.return_value = (1,)
    probe = MagicMock(return_value=None)
    monkeypatch.setattr(reader, "_conn", lambda: conn)
    monkeypatch.setattr(reader, "_probe_oldest_open_txn", probe)

    reader._ensure_open_txn_detection()

    assert reader._open_txn_detection_checked is True
    probe.assert_called_once_with()
    assert "GV$INSTANCE" in cur.execute.call_args.args[0].upper()


def test_buffer_off_startup_refuses_when_gv_instance_is_not_granted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    reader._open_txn_detection_checked = False
    conn, cur = _open_txn_connection()
    cur.execute.side_effect = RuntimeError(
        'ORA-00942: table or view "SYS"."GV_$INSTANCE" does not exist'
    )
    monkeypatch.setattr(reader, "_conn", lambda: conn)

    with pytest.raises(OracleLogMinerReadError) as raised:
        reader._ensure_open_txn_detection()

    assert getattr(raised.value, "code", None) == (
        "oracle_instance_detection_unavailable"
    )
    assert "SELECT ON GV_$INSTANCE" in str(raised.value)
    assert cur.execute.call_count == 1


@pytest.mark.parametrize(
    ("cursor_scn", "expected_scn", "expected_fetches"),
    [(100, 100, 1), (150, 150, 0)],
)
def test_single_table_poll_applies_oldest_transaction_hold(
    monkeypatch: pytest.MonkeyPatch,
    cursor_scn: int,
    expected_scn: int,
    expected_fetches: int,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    reader.scn = cursor_scn
    conn, cur = _open_txn_connection()
    calls: list[bool] = []
    fetches: list[int] = []

    def window_end():
        calls.append(True)
        return 149, True

    monkeypatch.setattr(reader, "_buffer_off_window_end", window_end, raising=False)
    monkeypatch.setattr(reader, "_mining_conn", lambda: conn)
    monkeypatch.setattr(oracle_logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(oracle_logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(oracle_logminer, "assert_resume_scn_in_redo", lambda *_a, **_k: None)
    monkeypatch.setattr(oracle_logminer, "assert_redo_continuity", lambda *_a, **_k: None)
    monkeypatch.setattr(
        oracle_logminer,
        "_logminer_session",
        lambda *_a, **_k: nullcontext(),
    )
    monkeypatch.setattr(
        reader,
        "_fetch_logminer_rows_visible",
        lambda _cur, **kwargs: fetches.append(kwargs["end_scn"])
        or ([], kwargs["end_scn"]),
    )
    monkeypatch.setattr(
        "services.cdc_incremental_runner.interleave_incremental_snapshot",
        lambda *_a, **_k: iter(()),
    )
    monkeypatch.setattr(reader, "_acquire_cdc_lease", lambda: None)

    list(reader._poll_once())

    assert calls == [True]
    assert reader.scn == expected_scn
    assert len(fetches) == expected_fetches


def test_shared_poll_applies_oldest_transaction_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch, shared=True)
    conn, cur = _open_txn_connection()
    calls: list[bool] = []

    def window_end():
        calls.append(True)
        return 149, True

    monkeypatch.setattr(reader, "_buffer_off_window_end", window_end, raising=False)
    monkeypatch.setattr(reader, "_mining_conn", lambda: conn)
    monkeypatch.setattr(oracle_logminer, "fetch_oldest_available_scn", lambda _cur: 1)
    monkeypatch.setattr(oracle_logminer, "fetch_redo_inventory", lambda _cur: [])
    monkeypatch.setattr(oracle_logminer, "assert_resume_scn_in_redo", lambda *_a, **_k: None)
    monkeypatch.setattr(oracle_logminer, "assert_redo_continuity", lambda *_a, **_k: None)
    monkeypatch.setattr(
        oracle_logminer,
        "_logminer_session",
        lambda *_a, **_k: nullcontext(),
    )
    monkeypatch.setattr(
        oracle_logminer,
        "_fetch_logminer_csf_window",
        lambda *_a, **_k: ([], False),
    )

    list(reader._poll_shared_multi())

    assert calls == [True]
    assert reader.scn <= 149


def test_peek_applies_oldest_transaction_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    conn, cur = _open_txn_connection()
    calls: list[bool] = []

    def window_end():
        calls.append(True)
        return 149, True

    monkeypatch.setattr(reader, "_buffer_off_window_end", window_end, raising=False)
    monkeypatch.setattr(reader, "_mining_conn", lambda: conn)
    monkeypatch.setattr(
        oracle_logminer,
        "_logminer_session",
        lambda *_a, **_k: nullcontext(),
    )
    monkeypatch.setattr(reader, "_query_logminer_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(reader, "_enrich_lob_lookup_events", lambda _events: None)
    monkeypatch.setattr(reader, "_coalesce_lob_locator_events", lambda events: events)

    assert reader._peek_stream_events_during_chunk(
        MagicMock(chunk_size=10)
    ) == []
    assert calls == [True]


def test_empty_window_retry_reapplies_oldest_transaction_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = _make_open_txn_reader(monkeypatch)
    _conn, cur = _open_txn_connection()
    calls: list[bool] = []

    def window_end():
        calls.append(True)
        return 149, True

    monkeypatch.setattr(reader, "_buffer_off_window_end", window_end, raising=False)
    monkeypatch.setattr(reader, "_empty_window_retry_sec", lambda: 1)
    monkeypatch.setattr(oracle_logminer.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(
        oracle_logminer,
        "_logminer_session",
        lambda *_a, **_k: nullcontext(),
    )
    monkeypatch.setattr(reader, "_query_logminer_rows", lambda *_a, **_k: [])

    rows, end_scn = reader._fetch_logminer_rows_visible(
        cur,
        start_scn=100,
        end_scn=150,
        table_predicate="TABLE_NAME = :tbl",
        limit=10,
    )

    assert rows == []
    assert calls == [True]
    assert end_scn == 149


def test_legacy_token_shape_and_low_scn_fallback_remain_compatible() -> None:
    token = encode_logminer_token(123, table="T")
    assert token == (
        '{"kind":"oracle-logminer","table":"T","scn":123,"phase":"streaming",'
        '"scn_complete":false}'
    )
    assert decode_logminer_token(token)["low_scn"] == 123


def test_interleaved_transactions_emit_in_commit_scn_order() -> None:
    buf = OracleTxnBuffer(max_txns=4, max_bytes=10_000)
    buf.feed(xid="a", scn=10, operation="START")
    buf.feed(xid="b", scn=11, operation="START")
    _dml(buf, "a", 12, " 0x000001.00000001.0001 ", 1, "a")
    _dml(buf, "b", 13, " 0x000001.00000001.0002 ", 2, "b")

    committed_b = buf.feed(
        xid="b",
        scn=20,
        rs_id=" 0x000001.00000001.0003 ",
        ssn=3,
        operation="COMMIT",
    )
    committed_a = buf.feed(
        xid="a",
        scn=30,
        rs_id=" 0x000001.00000001.0004 ",
        ssn=4,
        operation="COMMIT",
    )

    assert [txn.xid for txn in committed_b + committed_a] == ["b", "a"]
    assert [txn.commit_scn for txn in committed_b + committed_a] == [20, 30]


def test_invalid_rs_id_cannot_enter_commit_position_order() -> None:
    assert mining_position(100, "", 0) < mining_position(
        100, " 0x000001.00000001.0001 ", 1
    )
    with pytest.raises(ValueError, match="refusing opaque ordering"):
        mining_position(100, "scn:100.pXYZ", 0)


def test_rs_id_sort_key_reuses_shared_parser_contract() -> None:
    rs_id = " 0x000017.0000b4a2.0010 "
    assert parse_oracle_rs_id(rs_id) == (0x17, 0xB4A2, 0x10)
    assert parse_oracle_rs_id("not-an-rs-id") is None
    assert rs_id_sort_key("") == (0, 0, 0, 0, "")
    assert rs_id_sort_key(rs_id) == (1, 0x17, 0xB4A2, 0x10, "")
    with pytest.raises(ValueError) as exc:
        rs_id_sort_key("bad")
    assert str(exc.value) == (
        "Invalid Oracle LogMiner RS_ID 'bad'; refusing opaque ordering"
    )


def test_oracle_rs_id_ordering_agrees_at_sequence_width_boundary() -> None:
    earlier = " 0xffffff.00000010.0010 "
    later = " 0x1000000.00000002.0010 "

    assert mining_position(100, earlier, 16) < mining_position(100, later, 16)
    assert lsn_sort_key(
        batch_lsn(encode_logminer_token(100, table="T", rs_id=earlier, ssn=16))
    ) < lsn_sort_key(
        batch_lsn(encode_logminer_token(100, table="T", rs_id=later, ssn=16))
    )


def test_resume_binds_use_canonical_rs_id_and_length() -> None:
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
    )
    empty_binds = reader._resume_binds()
    assert empty_binds["rs_id"] is None
    assert empty_binds["rs_len"] is None

    reader.rs_id = "0x1000000.00000002.0010"
    binds = reader._resume_binds()
    assert binds["rs_id"] == " 0x1000000.00000002.0010 "
    assert binds["rs_len"] == len(binds["rs_id"])


@pytest.mark.parametrize(
    "rs_id",
    [
        " 0x00009A.00000002.0010 ",
        " 0x000094.0000002.0010 ",
        " 0x0000094.00000002.0010 ",
        " 000094.00000002.0010 ",
    ],
)
def test_noncanonical_row_rs_id_fails_closed(rs_id: str) -> None:
    with pytest.raises(
        OracleLogMinerReadError,
        match="RS_ID.*violates the canonical ordering assumption",
    ):
        _row_rs_id(rs_id)


def test_canonical_rs_id_accepts_sequence_width_growth() -> None:
    rs_id = " 0x1000000.00000002.0010 "
    assert _canonical_rs_id(rs_id) == rs_id
    assert _row_rs_id(rs_id) == rs_id


def test_unparseable_cursor_rs_id_fails_closed() -> None:
    with pytest.raises(
        OracleLogMinerReadError,
        match="RS_ID.*violates the canonical ordering assumption",
    ):
        _canonical_rs_id("not-an-rs-id")


def test_logminer_contents_sql_resumes_and_orders_by_canonical_rs_id() -> None:
    queries = (
        logminer_contents_sql(
            table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl"
        ),
        logminer_txn_contents_sql(
            table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl"
        ),
    )

    for query in queries:
        normalized = " ".join(query.split()).upper()
        assert "LENGTH(RS_ID) > :RS_LEN" in normalized
        assert "LENGTH(RS_ID) = :RS_LEN AND RS_ID > :RS_ID" in normalized
        assert "RS_ID = :RS_ID AND SSN > :SSN" in normalized
        assert "ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN, DF_RN" in normalized
        assert ":RS_ID IS NULL AND :SCN_COMPLETE = 0" in normalized
        assert "REGEXP_SUBSTR" not in normalized
        assert "DF_RS_SEQ" not in normalized
        assert "ORDER BY SCN, RS_ID, SSN, DF_RN" not in normalized


def test_full_rollback_discards_transaction() -> None:
    buf = OracleTxnBuffer(max_txns=2, max_bytes=10_000)
    buf.feed(xid="rolled", scn=10, operation="START")
    _dml(buf, "rolled", 11, " 0x000001.00000001.0001 ", 1, "x")
    assert buf.feed(
        xid="rolled",
        scn=12,
        rs_id=" 0x000001.00000001.0002 ",
        ssn=2,
        operation="ROLLBACK",
    ) == []
    assert buf.open_txn_count == 0


def test_savepoint_compensating_redo_is_kept_in_transaction_order() -> None:
    buf = OracleTxnBuffer(max_txns=2, max_bytes=10_000)
    buf.feed(xid="partial", scn=10, operation="START")
    buf.feed(
        xid="partial",
        scn=11,
        rs_id=" 0x000001.00000001.0001 ",
        ssn=1,
        operation="INSERT",
        table="T",
        row={"ID": "1", "V": "before"},
        row_id="RID-1",
        primary_key="ID",
    )
    buf.feed(
        xid="partial",
        scn=12,
        rs_id=" 0x000001.00000001.0002 ",
        ssn=2,
        operation="UPDATE",
        table="T",
        row={"ID": "1", "ROWID": "RID-1", "V": "temporary"},
        row_id="RID-1",
        primary_key="ID",
    )
    buf.feed(
        xid="partial",
        scn=13,
        rs_id=" 0x000001.00000001.0003 ",
        ssn=3,
        operation="UPDATE",
        table="T",
        row={"ROWID": "RID-1", "V": "before"},
        rollback=True,
        row_id="RID-1",
        primary_key="ID",
    )
    committed = buf.feed(
        xid="partial",
        scn=14,
        rs_id=" 0x000001.00000001.0004 ",
        ssn=4,
        operation="COMMIT",
    )
    assert len(committed) == 1
    assert committed[0].rows[-1].row["ID"] == "1"
    assert [row.row["V"] for row in committed[0].rows] == [
        "before",
        "temporary",
        "before",
    ]
    assert [row.rollback for row in committed[0].rows] == [False, False, True]


def test_buffered_query_selects_lifecycle_and_dml_rows() -> None:
    sql = logminer_txn_contents_sql(table_predicate="TABLE_NAME IN ('T')")
    assert "XIDSQN" in sql
    assert '"ROLLBACK"' in sql
    assert "ROW_ID" in sql
    assert "SCN = :start_scn AND :rs_id IS NULL" in sql
    assert "OPERATION IN ('START','COMMIT','ROLLBACK')" in sql
    assert "ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN, DF_RN" in sql


def test_open_transaction_and_byte_bounds_raise_typed_remedy() -> None:
    by_count = OracleTxnBuffer(max_txns=1, max_bytes=10_000)
    by_count.feed(xid="one", scn=1, operation="START")
    with pytest.raises(CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_OPEN"):
        by_count.feed(xid="two", scn=2, operation="START")

    by_bytes = OracleTxnBuffer(max_txns=2, max_bytes=4)
    with pytest.raises(CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_BYTES"):
        _dml(by_bytes, "large", 1, " 0x000001.00000001.0001 ", 1, "payload")


def test_configured_transaction_age_bounds_raise_typed_remedy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 10.0}
    monkeypatch.setattr(
        "connectors.oracle_logminer_txn.time.monotonic", lambda: clock["now"]
    )
    by_seconds = OracleTxnBuffer(max_txns=2, max_bytes=10_000, max_age_seconds=1)
    by_seconds.feed(xid="old", scn=100, operation="START")
    clock["now"] = 12.0
    with pytest.raises(
        CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_AGE_SECONDS"
    ):
        by_seconds.check_abandoned(head_scn=100)

    by_scn = OracleTxnBuffer(max_txns=2, max_bytes=10_000, max_age_scn=5)
    by_scn.feed(xid="old-scn", scn=100, operation="START")
    with pytest.raises(
        CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_AGE_SCN"
    ):
        by_scn.check_abandoned(head_scn=106)


def test_restart_remines_open_transaction_but_dedupes_emitted_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    token = encode_logminer_token(
        150,
        table="T",
        rs_id=" 0x000001.00000001.0004 ",
        ssn=4,
        low_scn=100,
        poll_scn=180,
        poll_rs_id=" 0x000001.00000001.0008 ",
        poll_ssn=8,
        commit_scn=150,
        commit_rs_id=" 0x000001.00000001.0004 ",
        commit_ssn=4,
        commit_xid="already-emitted",
        txn_buffer=True,
    )
    from connectors.oracle_logminer import decode_logminer_token

    state = decode_logminer_token(token)
    assert state["low_scn"] == 100
    assert state["poll_scn"] == 180
    assert state["txn_buffer"] is True
    restart = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=token,
    )
    assert restart.scn == 100
    assert restart.rs_id == ""
    assert restart._poll_scn == 180
    restart.close()
    buf = OracleTxnBuffer(
        max_txns=2,
        max_bytes=10_000,
        emitted_commit=(state["commit_scn"], state["commit_xid"]),
    )
    _dml(buf, "already-emitted", 120, " 0x000001.00000001.0001 ", 1, "old")
    replayed = buf.feed(
        xid="already-emitted",
        scn=150,
        rs_id=" 0x000001.00000001.0004 ",
        ssn=4,
        operation="COMMIT",
    )
    assert replayed == []

    _dml(buf, "still-open", 130, " 0x000001.00000001.0005 ", 5, "new")
    assert buf.low_scn == 130
    resumed = buf.feed(
        xid="still-open",
        scn=190,
        rs_id=" 0x000001.00000001.0009 ",
        ssn=9,
        operation="COMMIT",
    )
    assert [txn.xid for txn in resumed] == ["still-open"]


def test_late_distinct_commit_before_emitted_cursor_is_not_deduped() -> None:
    buf = OracleTxnBuffer(
        max_txns=2,
        max_bytes=10_000,
        emitted_commit=(150, "already-emitted"),
    )
    _dml(buf, "late-xid", 110, " 0x000001.00000001.0001 ", 1, "late")

    late = buf.feed(
        xid="late-xid",
        scn=140,
        rs_id=" 0x000001.00000001.0007 ",
        ssn=7,
        operation="COMMIT",
    )

    assert [txn.xid for txn in late] == ["late-xid"]


def _make_mining_conn(rows: list[tuple], head_scn: int = 200):
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (head_scn,)
    cur.fetchall.return_value = list(rows)
    cur.execute.return_value = None
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn


def test_buffered_short_window_keeps_last_mined_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    txn_id = (1, 0, 21)
    rows = [
        (101, " 0x000001.00000001.0001 ", 0, "START", None, None, None, *txn_id, 0),
        (
            102,
            " 0x000001.00000001.0002 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'1\',\'v1\')',
            "T",
            "APP",
            *txn_id,
            0,
        ),
        (110, " 0x000001.00000001.0003 ", 0, "COMMIT", "commit", None, None, *txn_id, 0),
    ]
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
    )
    reader.phase = "streaming"
    conn = _make_mining_conn(rows, head_scn=200)

    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[]),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch("connectors.oracle_logminer.start_logminer_session"),
    ):
        batches = list(reader.poll())

    assert reader._poll_scn == 110
    assert reader.scn == 110
    assert [row["ID"] for batch in batches for row in batch.inserts] == ["1"]


def test_buffered_restart_remines_open_xid_at_low_scn_from_destination_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    long_xid = (5, 9, 745)
    emitted_xid = (3, 0, 734)
    low_scn = 100
    emitted_commit_scn = 111
    token = {
        "kind": "oracle-logminer",
        "table": "T",
        "scn": emitted_commit_scn,
        "phase": "streaming",
        "rs_id": "",
        "ssn": 0,
        "low_scn": low_scn,
        "poll_scn": emitted_commit_scn,
        "poll_rs_id": " 0x000001.00000001.0005 ",
        "poll_ssn": 0,
        "commit_scn": emitted_commit_scn,
        "commit_rs_id": " 0x000001.00000001.0005 ",
        "commit_ssn": 0,
        "commit_xid": ".".join(str(value) for value in emitted_xid),
        "txn_buffer": True,
    }
    rows = [
        (100, " 0x000001.00000001.0001 ", 0, "START", None, None, None, *long_xid, 0),
        (
            100,
            " 0x000001.00000001.0002 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'1\',\'long\')',
            "T",
            "APP",
            *long_xid,
            0,
        ),
        (110, " 0x000001.00000001.0003 ", 0, "START", None, None, None, *emitted_xid, 0),
        (
            110,
            " 0x000001.00000001.0004 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'2\',\'short\')',
            "T",
            "APP",
            *emitted_xid,
            0,
        ),
        (111, " 0x000001.00000001.0005 ", 0, "COMMIT", "commit", None, None, *emitted_xid, 0),
        (130, " 0x000001.00000001.0006 ", 0, "COMMIT", "commit", None, None, *long_xid, 0),
    ]
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=token,
    )
    reader.phase = "streaming"
    assert decode_logminer_token(token)["scn_complete"] is True
    destination_lsn = extract_cdc_commit_position(token)
    assert destination_lsn == "scn:111.c"
    assert lsn_sort_key(destination_lsn) == lsn_sort_key("scn:111.c")
    clamped, proof = clamp_job_resume_to_dest(
        token,
        destination_lsn,
        encode_resume_blob(token),
    )
    assert proof["reason"] == "dest_resume_blob_authoritative"
    assert clamped == token
    assert reader.low_scn == low_scn
    assert reader.scn == low_scn
    assert reader._txn_buffer.emitted_commit == (
        emitted_commit_scn,
        ".".join(str(value) for value in emitted_xid),
    )
    conn = _make_mining_conn(rows, head_scn=140)
    cur = conn.cursor.return_value.__enter__.return_value
    mining_starts: list[int] = []
    query: dict[str, object] = {}

    def execute(sql: str, binds: dict[str, object] | None = None) -> None:
        query["sql"] = sql
        query["binds"] = binds or {}

    def fetchall() -> list[tuple]:
        sql = str(query.get("sql") or "")
        binds = query.get("binds") or {}
        start_scn = int(binds.get("start_scn") or 0)
        rs_id = str(binds.get("rs_id") or "")
        ssn = int(binds.get("ssn") or 0)
        includes_null_cursor = (
            ":rs_id IS NULL" in sql and not int(binds.get("scn_complete") or 0)
        )
        cursor_position = mining_position(start_scn, rs_id, ssn)
        visible = []
        for row in rows:
            scn, row_rs_id, row_ssn = (
                int(row[0] or 0),
                str(row[1] or ""),
                int(row[2] or 0),
            )
            if scn < mining_starts[-1]:
                continue
            after_cursor = scn > start_scn or (
                scn == start_scn
                and (
                    (not rs_id and includes_null_cursor)
                    or (
                        bool(rs_id)
                        and mining_position(scn, row_rs_id, row_ssn)
                        > cursor_position
                    )
                )
            )
            if after_cursor:
                visible.append(row)
        return visible

    cur.execute.side_effect = execute
    cur.fetchall.side_effect = fetchall

    def start_logminer(
        _cur,
        *,
        start_scn: int,
        end_scn: int,
        committed_only: bool,
        **_kwargs,
    ):
        mining_starts.append(start_scn)

    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[]),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch(
            "connectors.oracle_logminer.start_logminer_session",
            side_effect=start_logminer,
        ),
    ):
        batches = list(reader._poll_txn_buffered())

    data_batches = [batch for batch in batches if batch.inserts or batch.updates]
    assert [row for batch in data_batches for row in batch.inserts] == [
        {"ID": "1", "V": "long"}
    ]
    assert len(data_batches) == 1
    assert decide_eos_apply(
        incoming_lsn=batch_lsn(data_batches[0].resume_token),
        dest_lsn=destination_lsn,
        change=data_batches[0],
    )[0] == "apply"
    assert mining_starts == [low_scn]
    assert reader.capture_reader_position() == "scn:140.c"


@pytest.mark.parametrize("buffer_mode", [False, True], ids=["buffer-off", "buffer-on"])
def test_committed_checkpoint_restarts_within_scn_without_duplicate_or_skip(
    monkeypatch: pytest.MonkeyPatch, buffer_mode: bool
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if buffer_mode else "0")
    first_rs = " 0x000001.00000001.0001 "
    remaining_rs = " 0x000001.00000001.0002 "
    if buffer_mode:
        prior_xid = (1, 0, 1)
        remaining_xid = (2, 0, 2)
        rows = [
            (100, first_rs, 0, "START", None, None, None, *prior_xid, 0),
            (100, " 0x000001.00000001.0002 ", 1, "INSERT",
             'INSERT INTO "T"("ID","V") VALUES(\'1\',\'applied\')', "T", "APP",
             *prior_xid, 0),
            (100, " 0x000001.00000001.0003 ", 2, "COMMIT", None, None, None, *prior_xid, 0),
            (100, " 0x000001.00000001.0004 ", 0, "START", None, None, None, *remaining_xid, 0),
            (100, remaining_rs, 3, "INSERT",
             'INSERT INTO "T"("ID","V") VALUES(\'2\',\'remaining\')', "T", "APP",
             *remaining_xid, 0),
            (100, " 0x000001.00000001.0006 ", 4, "COMMIT", None, None, None, *remaining_xid, 0),
        ]
        token = encode_logminer_token(
            100, table="T", phase="streaming", low_scn=99,
            commit_scn=100, commit_xid="1.0.1", txn_buffer=True,
        )
    else:
        rows = [
            (100, first_rs, 1, "INSERT",
             'INSERT INTO "T"("ID","V") VALUES(\'1\',\'applied\')', "T", "APP"),
            (100, remaining_rs, 2, "INSERT",
             'INSERT INTO "T"("ID","V") VALUES(\'2\',\'remaining\')', "T", "APP"),
        ]
        token = encode_logminer_token(
            100, table="T", phase="streaming", rs_id=first_rs, ssn=1,
        )

    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T", primary_key="ID", schema="APP", resume_token=token,
    )
    reader.phase = "streaming"
    if not buffer_mode:
        reader._open_txn_detection_checked = True
        reader._buffer_off_window_end = lambda: (120, False)
    conn = _make_mining_conn(rows, head_scn=120)
    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[]),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch("connectors.oracle_logminer.start_logminer_session"),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter(()),
        ),
    ):
        if buffer_mode:
            cur = conn.cursor.return_value.__enter__.return_value
            query: dict[str, object] = {}

            def execute(sql: str, binds: dict[str, object] | None = None) -> None:
                query["sql"], query["binds"] = sql, binds or {}

            def fetchall() -> list[tuple]:
                binds = query.get("binds") or {}
                start_scn = int(binds.get("start_scn") or 0)
                rs_id = str(binds.get("rs_id") or "")
                ssn = int(binds.get("ssn") or 0)
                complete = int(binds.get("scn_complete") or 0)
                cursor_position = mining_position(start_scn, rs_id, ssn)
                return [
                    row for row in rows
                    if int(row[0]) > start_scn
                    or (int(row[0]) == start_scn and (
                        (not rs_id and not complete)
                        or (
                            rs_id
                            and mining_position(
                                int(row[0]), str(row[1]), int(row[2])
                            ) > cursor_position
                        )
                    ))
                ]

            cur.execute.side_effect = execute
            cur.fetchall.side_effect = fetchall
            batches = list(reader._poll_txn_buffered())
        else:
            monkeypatch.setattr(
                reader, "_probe_oldest_open_txn", lambda _cur=None: None
            )

            def fetch_window(_cur: object, **kwargs: object) -> tuple[list[tuple], int]:
                assert kwargs["start_scn"] == 100
                assert reader.rs_id == first_rs
                return [rows[1]], 120

            with patch.object(reader, "_fetch_logminer_rows_visible", side_effect=fetch_window):
                batches = list(reader._poll_once())

    applied = [row for batch in batches for row in batch.inserts]
    assert [row["ID"] for row in applied] == ["2"]


def test_oversized_transaction_chunks_have_increasing_same_commit_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    txn_id = (1, 0, 7)
    rows = [
        (101, " 0x000001.00000001.0000 ", 0, "START", None, None, None, *txn_id, 0),
    ]
    for index in range(5):
        rs_id = f" 0x000001.00000001.{index + 1:04x} "
        rows.append(
            (
                102 + index,
                rs_id,
                index + 1,
                "INSERT",
                f'INSERT INTO "T"("ID","V") VALUES(\'{index + 1}\',\'v{index + 1}\')',
                "T",
                "APP",
                *txn_id,
                0,
            )
        )
    rows.append((150, " 0x000001.00000001.0008 ", 8, "COMMIT", "commit", None, None, *txn_id, 0))
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(
            100, table="T", phase="streaming"
        ),
        batch_size=2,
    )
    cdc.phase = "streaming"
    conn = _make_mining_conn(rows)
    with (
        patch.object(cdc, "_mining_conn", return_value=conn),
        patch.object(cdc, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.start_logminer_session"),
    ):
        batches = list(cdc.poll())

    data_batches = [batch for batch in batches if batch.inserts]
    assert [len(batch.inserts) for batch in data_batches] == [2, 2, 1]
    positions = [batch_lsn(batch.resume_token) for batch in data_batches]
    assert all(position for position in positions)
    assert len({position.split(".")[0] for position in positions}) == 1
    assert all(
        compare_lsn(batch_lsn(right.resume_token), batch_lsn(left.resume_token)) > 0
        for left, right in zip(data_batches, data_batches[1:])
    )


def test_multiple_commits_in_one_poll_advance_each_batch_commit_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    rows = [
        (101, " 0x000001.00000001.0001 ", 0, "START", None, None, None, 1, 0, 11, 0),
        (
            102,
            " 0x000001.00000001.0002 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'1\',\'v1\')',
            "T",
            "APP",
            1,
            0,
            11,
            0,
        ),
        (103, " 0x000001.00000001.0003 ", 0, "START", None, None, None, 1, 0, 12, 0),
        (
            104,
            " 0x000001.00000001.0004 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'2\',\'v2\')',
            "T",
            "APP",
            1,
            0,
            12,
            0,
        ),
        (110, " 0x000001.00000001.0005 ", 2, "COMMIT", None, None, None, 1, 0, 11, 0),
        (120, " 0x000001.00000001.0006 ", 2, "COMMIT", None, None, None, 1, 0, 12, 0),
    ]
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
        batch_size=10,
    )
    cdc.phase = "streaming"
    conn = _make_mining_conn(rows)
    with (
        patch.object(cdc, "_mining_conn", return_value=conn),
        patch.object(cdc, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.start_logminer_session"),
    ):
        batches = list(cdc.poll())

    data_batches = [batch for batch in batches if batch.inserts]
    assert [[row["ID"] for row in batch.inserts] for batch in data_batches] == [
        ["1"],
        ["2"],
    ]
    from connectors.oracle_logminer import decode_logminer_token

    states = [decode_logminer_token(batch.resume_token) for batch in data_batches]
    assert [state["commit_scn"] for state in states] == [110, 120]
    assert [state["low_scn"] for state in states] == [101, 101]
    assert compare_lsn(batch_lsn(data_batches[1].resume_token), batch_lsn(data_batches[0].resume_token)) > 0
