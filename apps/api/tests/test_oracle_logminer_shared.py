"""Oracle LogMiner shared multi-table CDC unit proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    CdcScnGapError,
    assert_redo_continuity,
    encode_logminer_token,
    fetch_redo_inventory,
    logminer_contents_sql,
)
from services.cdc_multi_table import can_share_log_reader


def test_oracle_can_share_log_reader() -> None:
    assert can_share_log_reader("oracle", 2) is True
    assert can_share_log_reader("oracle", 1) is False


def test_logminer_contents_selects_documented_xidsqn_column() -> None:
    sql = logminer_contents_sql(
        table_predicate="TABLE_NAME IN ('ORDERS','USERS')",
        include_xid=True,
    )

    assert ", XIDUSN, XIDSLT, XIDSQN" in sql
    assert "XIDSEQ" not in sql


def test_redo_continuity_skips_invalid_rows_and_still_detects_holes(caplog) -> None:
    inventory = [
        (1, 1, 100, 200, "archived"),
        (1, 0, 0, 0, "online"),
        (1, 3, 300, 400, "archived"),
    ]

    with pytest.raises(CdcScnGapError, match="thread 1: missing sequence range 2-2"):
        assert_redo_continuity(150, inventory, cursor_key="invalid-row-hole")
    assert "invalid" in caplog.text.lower()


def test_redo_continuity_warning_is_deduplicated_per_cursor_key(caplog) -> None:
    cursor_key = f"warning-dedupe-{id(caplog)}"

    assert_redo_continuity(150, None, cursor_key=cursor_key) is None
    assert_redo_continuity(150, None, cursor_key=cursor_key) is None

    assert caplog.text.count("continuity is unverified") == 1


class _RedoInventoryCursor:
    def __init__(self, archived_rows, online_rows) -> None:
        self.archived_rows = archived_rows
        self.online_rows = online_rows
        self.statement = ""
        self.executed = []

    def execute(self, statement: str) -> None:
        self.statement = statement
        self.executed.append(statement)

    def fetchall(self):
        if "V$ARCHIVED_LOG" in self.statement:
            if "RESETLOGS_CHANGE#" in self.statement:
                return self.archived_rows
            return self.archived_rows + self.prior_incarnation_rows
        return self.online_rows


def test_prior_incarnation_logs_do_not_create_a_false_sequence_gap() -> None:
    cur = _RedoInventoryCursor(
        archived_rows=[(1, 1, 100, 200)],
        online_rows=[(1, 2, 200, 300), (1, 3, 300, 400)],
    )
    cur.prior_incarnation_rows = [(1, 5, 10, 20)]

    inventory = fetch_redo_inventory(cur)

    assert inventory is not None
    assert [entry[1] for entry in inventory] == [1, 2, 3]
    assert_redo_continuity(150, inventory, cursor_key="current-chain")


def test_prior_incarnation_logs_do_not_mask_a_current_sequence_hole() -> None:
    cur = _RedoInventoryCursor(
        archived_rows=[(1, 1, 100, 200), (1, 3, 300, 400)],
        online_rows=[(1, 4, 400, 500)],
    )
    cur.prior_incarnation_rows = [(1, 2, 10, 20)]

    inventory = fetch_redo_inventory(cur)

    assert inventory is not None
    with pytest.raises(CdcScnGapError, match="thread 1: missing sequence range 2-2"):
        assert_redo_continuity(150, inventory, cursor_key="current-hole")


def test_redo_inventory_sql_filters_unused_online_and_old_incarnation_rows() -> None:
    cur = _RedoInventoryCursor(archived_rows=[], online_rows=[])
    cur.prior_incarnation_rows = []

    fetch_redo_inventory(cur)
    archive_query, online_query = cur.executed
    archive_query = " ".join(archive_query.split())
    online_query = " ".join(online_query.split())

    assert "RESETLOGS_CHANGE# = (" in archive_query
    assert "SELECT RESETLOGS_CHANGE# FROM V$DATABASE" in archive_query
    assert "STATUS <> 'UNUSED'" in online_query
    assert "FIRST_CHANGE# > 0" in online_query


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
        [],  # archived redo inventory
        [],  # online redo inventory
        # V$LOGFILE members, then V$ARCHIVED_LOG names: the session now
        # ADD_LOGFILEs an explicit redo set instead of using CONTINUOUS_MINE.
        [("/redo/redo01.log",)],
        [],
        [
            # scn, rs_id, ssn, op, sql_redo, table, owner, xidusn, xidslt, xidseq
            # RS_ID/SSN are part of LogMiner's true total order and the resume token.
            (
                150,
                "0x000001.0001.0001",
                1,
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
                "0x000001.0001.0002",
                2,
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

    with patch.object(cdc, "_conn", return_value=conn), patch.object(
        cdc, "_mining_conn", return_value=conn
    ):
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

    with patch.object(cdc, "_conn", return_value=conn), patch.object(
        cdc, "_mining_conn", return_value=conn
    ):
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

    with patch.object(cdc, "_conn", return_value=conn), patch.object(
        cdc, "_mining_conn", return_value=conn
    ):
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
