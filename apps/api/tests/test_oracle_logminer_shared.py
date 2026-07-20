"""Oracle LogMiner shared multi-table CDC unit proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token
from services.cdc_multi_table import can_share_log_reader


def test_oracle_can_share_log_reader() -> None:
    assert can_share_log_reader("oracle", 2) is True
    assert can_share_log_reader("oracle", 1) is False


def test_oracle_shared_reader_init_and_lease() -> None:
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP", "job_id": "j1"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        schema="APP",
        cursor_key="cdc-shared:oracle:ORCL:x:j1",
    )
    assert cdc.tables == ["ORDERS", "USERS"]
    assert cdc._shared is True
    assert cdc.primary_keys["USERS"] == "USER_ID"
    assert cdc._lease.meta.get("shared_reader") is True
    assert "oracle_logminer_shared:" in cdc._lease.resource


def test_truncate_tagged_at_xid_boundary() -> None:
    tagged = [
        ("1.0.1", 10, "ORDERS", "insert", {"ID": "1"}),
        ("1.0.2", 11, "ORDERS", "insert", {"ID": "2"}),
        ("1.0.2", 11, "USERS", "insert", {"USER_ID": "9"}),
    ]
    keep = OracleLogMinerCdc._truncate_tagged_at_xid_boundary(tagged, 2)
    assert len(keep) == 1
    assert keep[0][0] == "1.0.1"


def test_oracle_shared_poll_demuxes_two_tables() -> None:
    token = encode_logminer_token(100, table="ORDERS,USERS", phase="streaming")
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        schema="APP",
        resume_token=token,
        batch_size=50,
    )
    cdc.phase = "streaming"
    cdc.scn = 100

    conn = MagicMock()
    cur = MagicMock()
    # current_scn, then logminer rows
    cur.fetchone.side_effect = [(200,)]
    cur.fetchall.side_effect = [
        [
            # scn, op, sql_redo, table, owner, xidusn, xidslt, xidseq
            (
                150,
                "INSERT",
                'INSERT INTO "ORDERS"("ID","AMOUNT") VALUES(\'1\',\'10\')',
                "ORDERS",
                "APP",
                1,
                0,
                9,
            ),
            (
                150,
                "INSERT",
                'INSERT INTO "USERS"("USER_ID","NAME") VALUES(\'9\',\'alice\')',
                "USERS",
                "APP",
                1,
                0,
                9,
            ),
        ]
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            with patch(
                "connectors.oracle_logminer.assert_resume_scn_in_redo",
            ), patch(
                "connectors.oracle_logminer.fetch_oldest_available_scn",
                return_value=None,
            ):
                batches = list(cdc.poll())

    assert len(batches) == 2
    by_table = {b.table: b for b in batches}
    assert "ORDERS" in by_table and "USERS" in by_table
    assert by_table["ORDERS"].ack_barrier is False
    assert by_table["USERS"].ack_barrier is True
    assert by_table["ORDERS"].inserts[0]["ID"] == "1"
    assert by_table["USERS"].inserts[0]["USER_ID"] == "9"
    assert by_table["ORDERS"].resume_token == by_table["USERS"].resume_token


def test_assert_resume_scn_in_redo_raises_on_gap() -> None:
    from connectors.oracle_logminer import CdcScnGapError, assert_resume_scn_in_redo

    assert_resume_scn_in_redo(200, 100)  # ahead — ok
    assert_resume_scn_in_redo(100, 100)  # equal — ok
    assert_resume_scn_in_redo(50, None)  # undetermined — fail-open
    try:
        assert_resume_scn_in_redo(50, 100)
        raise AssertionError("expected CdcScnGapError")
    except CdcScnGapError as exc:
        assert "oldest_available" in str(exc)


def test_is_oracle_redo_gap_error() -> None:
    from connectors.oracle_logminer import is_oracle_redo_gap_error

    assert is_oracle_redo_gap_error(RuntimeError("ORA-01291: missing logfile"))
    assert is_oracle_redo_gap_error(RuntimeError("ORA-01292: no log file"))
    assert not is_oracle_redo_gap_error(RuntimeError("ORA-00942: table or view does not exist"))


def test_poll_fails_closed_when_resume_before_oldest_redo() -> None:
    from connectors.oracle_logminer import CdcScnGapError, OracleLogMinerCdc

    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(50, table="ORDERS", phase="streaming"),
    )
    cdc.phase = "streaming"
    cdc.scn = 50

    conn = MagicMock()
    cur = MagicMock()
    # V$LOG min, V$ARCHIVED_LOG min → oldest 100 > resume 50
    cur.fetchone.side_effect = [(100,), (None,)]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            with patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                return_value=iter(()),
            ):
                try:
                    list(cdc.poll())
                    raise AssertionError("expected CdcScnGapError")
                except CdcScnGapError:
                    pass


def test_poll_maps_ora_01291_to_scn_gap() -> None:
    from connectors.oracle_logminer import CdcScnGapError, OracleLogMinerCdc

    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        resume_token=encode_logminer_token(100, table="ORDERS", phase="streaming"),
    )
    cdc.phase = "streaming"
    cdc.scn = 100

    conn = MagicMock()
    cur = MagicMock()

    def _execute(sql, params=None):
        if "START_LOGMNR" in str(sql):
            raise RuntimeError("ORA-01291: missing logfile")

    cur.execute.side_effect = _execute
    cur.fetchone.side_effect = [
        (None,),  # V$LOG
        (None,),  # V$ARCHIVED_LOG
        (200,),  # current_scn
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        with patch.object(cdc, "_acquire_cdc_lease"):
            with patch(
                "services.cdc_incremental_runner.interleave_incremental_snapshot",
                return_value=iter(()),
            ):
                try:
                    list(cdc.poll())
                    raise AssertionError("expected CdcScnGapError")
                except CdcScnGapError as exc:
                    assert "01291" in str(exc) or "redo" in str(exc).lower()
