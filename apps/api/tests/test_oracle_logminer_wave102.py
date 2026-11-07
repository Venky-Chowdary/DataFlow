"""Wave 102: Oracle LogMiner silent-loss and false-healthy CDC fixes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    _logminer_options_sql,
    decode_logminer_token,
    encode_logminer_token,
    logminer_contents_sql,
    start_logminer_session,
)
from connectors.oracle_logminer_txn import mining_position


@pytest.fixture(autouse=True)
def _no_lob_schema_in_unit_readers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        OracleLogMinerCdc,
        "_lob_columns_for_table",
        lambda _self, _table: [],
    )


def test_logminer_options_are_committed_only_without_continuous_mine():
    opts = _logminer_options_sql()
    assert "COMMITTED_DATA_ONLY" in opts
    assert "CONTINUOUS_MINE" not in opts


def test_contents_sql_orders_inside_rownum_outside():
    sql = logminer_contents_sql(table_predicate="TABLE_NAME = :tbl")
    # Flat ROWNUM-then-ORDER-BY is the silent-loss bug; order must be nested.
    order_clause = "ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN, DF_RN"
    assert order_clause in sql
    assert "SCN = :start_scn AND :rs_id IS NULL" in sql
    outer_cap = sql.rfind("ROWNUM")
    inner_order = sql.find(order_clause)
    assert 0 <= inner_order < outer_cap


def test_token_roundtrips_rs_id_ssn():
    token = encode_logminer_token(
        42, table="ORDERS", phase="streaming", rs_id="0xABC", ssn=7
    )
    state = decode_logminer_token(token)
    assert state["scn"] == 42
    assert state["rs_id"] == "0xABC"
    assert state["ssn"] == 7


def test_advance_offset_stops_at_last_consumed_when_truncated():
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(10, table="ORDERS", phase="streaming"),
    )
    cdc._advance_offset(
        last_scn=50,
        last_rs_id="0x1",
        last_ssn=3,
        end_scn=999,
        fetched=500,
        limit=500,
    )
    # Truncated window must NOT jump to end_scn — that skipped unread rows.
    assert cdc.scn == 50
    assert cdc.rs_id == "0x1"
    assert cdc.ssn == 3


def test_advance_offset_stops_at_last_consumed_when_window_exhausted():
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(10, table="ORDERS", phase="streaming"),
    )
    cdc._advance_offset(
        last_scn=50,
        last_rs_id="0x1",
        last_ssn=3,
        end_scn=999,
        fetched=12,
        limit=500,
    )
    assert cdc.scn == 50
    assert cdc.rs_id == "0x1"
    assert cdc.ssn == 3


def test_advance_offset_empty_fetch_does_not_skip_unseen_scn() -> None:
    """Empty LogMiner contents must not jump to end_scn — LGWR may still flush."""
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(100, table="ORDERS", phase="streaming"),
    )
    cdc._advance_offset(
        last_scn=0,
        last_rs_id="",
        last_ssn=0,
        end_scn=999,
        fetched=0,
        limit=500,
    )
    assert cdc.scn == 100
    assert cdc.rs_id == ""
    assert cdc.ssn == 0


def test_advance_offset_keeps_mid_scn_cursor_on_idle_empty_poll():
    """An empty poll at the current SCN must not wipe an active (rs_id, ssn)."""
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(
            100, table="ORDERS", phase="streaming",
            rs_id=" 0x000001.00000001.0001 ", ssn=5,
        ),
    )
    cdc.scn = 100
    cdc.rs_id = " 0x000001.00000001.0001 "
    cdc.ssn = 5
    cdc._advance_offset(
        last_scn=0,
        last_rs_id="",
        last_ssn=0,
        end_scn=100,
        fetched=0,
        limit=500,
    )
    assert cdc.scn == 100
    assert cdc.rs_id == " 0x000001.00000001.0001 "
    assert cdc.ssn == 5


def test_late_redo_at_window_end_is_remined_exactly_once(
    monkeypatch,
) -> None:
    from services.cdc_exactly_once import batch_lsn, compare_lsn

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="ORDERS",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(
            9, table="ORDERS", phase="streaming"
        ),
    )
    reader.phase = "streaming"
    reader._open_txn_detection_checked = True
    reader._probe_oldest_open_txn = lambda _cur=None: None
    reader._buffer_off_window_end = lambda: (12, False)
    cursor = MagicMock()
    cursor.fetchone.return_value = (12,)
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    rows = [
        (
            10,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            'INSERT INTO "ORDERS"("ID","AMOUNT") VALUES(\'10\',\'10\')',
            "ORDERS",
            "APP",
        ),
        (
            12,
            " 0x000001.00000002.0001 ",
            1,
            "INSERT",
            'INSERT INTO "ORDERS"("ID","AMOUNT") VALUES(\'12\',\'12\')',
            "ORDERS",
            "APP",
        ),
    ]
    windows = 0

    def fetch_window(_cur, *, start_scn, end_scn, table_predicate, limit):
        nonlocal windows
        windows += 1
        if windows == 1:
            return [rows[0]], 12
        late = rows[1]
        visible = late[0] > start_scn or (
            late[0] == start_scn
            and bool(reader.rs_id)
            and (
                mining_position(late[0], late[1], late[2])
                > mining_position(start_scn, reader.rs_id, reader.ssn)
            )
        )
        return ([late] if visible else []), 12

    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch.object(reader, "_fetch_logminer_rows_visible", side_effect=fetch_window),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter(()),
        ),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[]),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch("connectors.oracle_logminer.start_logminer_session"),
    ):
        first = [batch for batch in reader.poll() if batch.inserts]
        second = [batch for batch in reader.poll() if batch.inserts]

    emitted = first + second
    assert [row["ID"] for batch in emitted for row in batch.inserts] == ["10", "12"]
    assert len({row["ID"] for batch in emitted for row in batch.inserts}) == 2
    assert compare_lsn(batch_lsn(emitted[1].resume_token), batch_lsn(emitted[0].resume_token)) > 0


def test_start_logminer_registers_files_and_uses_committed_options():
    cur = MagicMock()
    # V$LOGFILE then V$ARCHIVED_LOG listings.
    cur.fetchall.side_effect = [
        [(1, 1, "/redo/online1.log")],
        [(1, 2, 1, "A", "/redo/arch1.log")],
    ]
    start_logminer_session(cur, start_scn=10, end_scn=20)
    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "ADD_LOGFILE" in executed
    assert "COMMITTED_DATA_ONLY" in executed
    assert "CONTINUOUS_MINE" not in executed


def test_start_logminer_ends_a_stale_session_before_registering_files() -> None:
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [(1, 1, "/redo/online1.log")],
        [(1, 2, 1, "A", "/redo/arch1.log")],
    ]

    start_logminer_session(cur, start_scn=10, end_scn=20)

    statements = [str(call.args[0]) for call in cur.execute.call_args_list]
    end_session = next(i for i, statement in enumerate(statements) if "END_LOGMNR" in statement)
    add_file = next(i for i, statement in enumerate(statements) if "ADD_LOGFILE" in statement)
    assert end_session < add_file


def test_start_logminer_resets_registered_files_after_a_failed_start() -> None:
    class FakeOracleError(Exception):
        def __init__(self, code: int) -> None:
            self.code = code
            super().__init__(f"ORA-{code}: simulated LogMiner error")

    class FakeCursor:
        def __init__(self) -> None:
            self.statement = ""
            self.registered: set[str] = set()
            self.start_calls = 0

        def execute(self, statement: str, params=None) -> None:
            self.statement = statement
            if "END_LOGMNR" in statement:
                raise FakeOracleError(1307)
            if "ADD_LOGFILE" in statement:
                filename = str(params["fname"])
                if "DBMS_LOGMNR.NEW" in statement:
                    self.registered.clear()
                elif filename in self.registered:
                    raise FakeOracleError(1289)
                self.registered.add(filename)
            elif "START_LOGMNR" in statement:
                self.start_calls += 1
                if self.start_calls == 1:
                    raise FakeOracleError(16331)

        def fetchall(self):
            if "V$LOGFILE" in self.statement:
                return [(1, 1, "/redo/online1.log")]
            if "V$ARCHIVED_LOG" in self.statement:
                return [(1, 2, 1, "A", "/redo/arch1.log")]
            return []

    cur = FakeCursor()

    try:
        start_logminer_session(cur, start_scn=10, end_scn=20)
        raise AssertionError("expected first START_LOGMNR to fail")
    except FakeOracleError as exc:
        assert exc.code == 16331

    start_logminer_session(cur, start_scn=10, end_scn=20)

    assert cur.start_calls == 2
    assert cur.registered == {"/redo/online1.log", "/redo/arch1.log"}


def test_start_logminer_fails_closed_with_no_redo_files():
    cur = MagicMock()
    cur.fetchall.side_effect = [[], []]
    try:
        start_logminer_session(cur, start_scn=10, end_scn=20)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "no redo" in str(exc).lower()


def test_poll_truncated_window_does_not_skip_past_last_row(monkeypatch):
    """A full window defers its trailing key group for the next poll."""
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="ID",
        schema="APP",
        batch_size=2,
        resume_token=encode_logminer_token(10, table="ORDERS", phase="streaming"),
    )
    cdc.phase = "streaming"
    cdc._open_txn_detection_checked = True
    monkeypatch.setattr(cdc, "_probe_oldest_open_txn", lambda _cur=None: None)
    monkeypatch.setattr(cdc, "_buffer_off_window_end", lambda: (99, False))

    mined = [
        (
            11,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            'INSERT INTO "ORDERS"("ID") VALUES(\'1\')',
            "ORDERS",
            "APP",
        ),
        (
            12,
            " 0x000001.00000001.0002 ",
            1,
            "INSERT",
            'INSERT INTO "ORDERS"("ID") VALUES(\'2\')',
            "ORDERS",
            "APP",
        ),
        (
            13,
            " 0x000001.00000001.0003 ",
            1,
            "INSERT",
            'INSERT INTO "ORDERS"("ID") VALUES(\'3\')',
            "ORDERS",
            "APP",
        ),
    ]

    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.side_effect = [(10,), (10,), (99,)] * 3
    cur.fetchall.side_effect = [
        [], [], [(1, 1, "/redo/online1.log")], [],
        mined[:2],
        [], [], [(1, 1, "/redo/online1.log")], [],
        mined[1:],
        [], [], [(1, 1, "/redo/online1.log")], [],
        mined[2:],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_mining_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            with patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                return_value=iter(()),
            ):
                polls = [list(cdc.poll()) for _ in range(3)]

    assert all(len(batches) == 1 for batches in polls)
    assert [
        row["ID"]
        for batches in polls
        for batch in batches
        for row in batch.inserts
    ] == ["1", "2", "3"]
    first_state = decode_logminer_token(polls[0][0].resume_token)
    assert first_state["scn"] == 11
    assert first_state["rs_id"] == " 0x000001.00000001.0001 "
    assert cdc.scn == 13
    assert cdc.rs_id == " 0x000001.00000001.0003 "
    assert cdc.ssn == 1
    last_state = decode_logminer_token(polls[-1][0].resume_token)
    assert last_state["scn"] == 13
    assert last_state["rs_id"] == " 0x000001.00000001.0003 "


def test_peek_continues_on_intra_scn_cursor() -> None:
    """A mid-SCN resume must still peek — DDD-3 stream-wins depends on it."""
    from unittest.mock import MagicMock, patch

    from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token

    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(
            100, table="ORDERS", phase="streaming",
            rs_id=" 0x000001.00000001.0001 ", ssn=1,
        ),
    )
    cdc.phase = "streaming"
    cdc.scn = 100
    cdc.rs_id = " 0x000001.00000001.0001 "
    cdc.ssn = 1

    conn = MagicMock()
    cur = MagicMock()
    # current_scn has not advanced past the resume SCN, but unread rows remain
    # at SCN 100 past (rs_id, ssn).
    cur.fetchone.side_effect = [(100,)]
    cur.fetchall.side_effect = [
        [(1, 1, "/redo/redo01.log")],
        [],
        [
            (
                100,
                " 0x000001.00000001.0002 ",
                2,
                "UPDATE",
                'UPDATE "ORDERS" SET "AMOUNT" = \'99\' WHERE "ID" = \'1\'',
                "ORDERS",
                "APP",
            ),
        ],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    class _Sig:
        chunk_size = 50

    with patch.object(cdc, "_mining_conn", return_value=conn):
        events = cdc._peek_stream_events_during_chunk(_Sig())

    assert events, "intra-SCN peek must surface the mid-chunk UPDATE"
    # Session must start at the resume SCN (not scn+1) so the (rs_id, ssn)
    # binds can see the remainder of the current SCN.
    start_calls = [
        c.args[0] for c in cur.execute.call_args_list
        if "START_LOGMNR" in str(c.args[0])
    ]
    assert start_calls, "peek must start a LogMiner session"
