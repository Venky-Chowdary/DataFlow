"""Oracle LogMiner continuation and key-group window proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import (
    OracleLogMinerCdc,
    OracleLogMinerReadError,
    encode_logminer_token,
    logminer_contents_sql,
    logminer_txn_contents_sql,
)
from connectors.oracle_logminer_txn import mining_position


def _contents_row(
    scn: int,
    rs_id: str,
    ssn: int,
    operation: str,
    sql_redo: str,
    csf: int = 0,
) -> tuple:
    return (
        scn,
        rs_id,
        ssn,
        operation,
        sql_redo,
        "T",
        "APP",
        "T",
        None,
        csf,
    )


class _Cursor:
    def __init__(self, rows: list[tuple], *, honor_limit: bool = False) -> None:
        self.rows = rows
        self.honor_limit = honor_limit
        self.statement = ""
        self.binds: dict = {}
        self.limits: list[int] = []

    def execute(self, statement: str, binds: dict | None = None) -> None:
        self.statement = statement
        self.binds = binds or {}
        if "V$LOGMNR_CONTENTS" in statement.upper():
            self.limits.append(int(self.binds.get("lim") or 0))

    def fetchall(self) -> list[tuple]:
        if "V$LOGMNR_CONTENTS" not in self.statement.upper():
            return []
        if self.honor_limit:
            return self.rows[: int(self.binds["lim"])]
        return self.rows


class _WindowCursor:
    def __init__(self, rows: list[tuple], *, head_scn: int = 200) -> None:
        self.rows = rows
        self.head_scn = head_scn
        self.statement = ""
        self.binds: dict = {}
        self.limits: list[int] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def execute(self, statement: str, binds: dict | None = None) -> None:
        self.statement = statement
        self.binds = binds or {}
        if "V$LOGMNR_CONTENTS" in statement.upper():
            self.limits.append(int(self.binds["lim"]))

    def fetchone(self) -> tuple[int]:
        return (self.head_scn,)

    def fetchall(self) -> list[tuple]:
        if "V$LOGMNR_CONTENTS" not in self.statement.upper():
            return []
        start_scn = int(self.binds.get("start_scn") or 0)
        resume_rs = self.binds.get("rs_id")
        resume_ssn = int(self.binds.get("ssn") or 0)
        scn_complete = int(self.binds.get("scn_complete") or 0)
        unread = []
        for row in self.rows:
            key = (int(row[0] or 0), str(row[1] or "").strip(), int(row[2] or 0))
            if key[0] > start_scn:
                unread.append(row)
            elif key[0] == start_scn:
                if not resume_rs and not scn_complete:
                    unread.append(row)
                elif resume_rs and mining_position(*key) > mining_position(
                    start_scn, str(resume_rs), resume_ssn
                ):
                    unread.append(row)
        return unread[: int(self.binds["lim"])]


class _MiningConnection:
    def __init__(self, cursor: _WindowCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def cursor(self) -> _WindowCursor:
        return self._cursor


def _reader(*, batch_size: int = 2) -> OracleLogMinerCdc:
    reader = OracleLogMinerCdc(
        {"host": "localhost", "database": "ORCL", "username": "APP"},
        table="T",
        primary_key="ID",
        schema="APP",
        resume_token=encode_logminer_token(100, table="T", phase="streaming"),
        batch_size=batch_size,
    )
    reader.phase = "streaming"
    reader._lob_column_cache["T"] = ()
    return reader


def test_logminer_contents_queries_select_csf_and_order_by_natural_redo_row() -> None:
    for sql in (
        logminer_contents_sql(table_predicate="TABLE_NAME = 'T'"),
        logminer_txn_contents_sql(table_predicate="TABLE_NAME = 'T'"),
    ):
        normalized = " ".join(sql.upper().split())
        assert "CSF" in normalized
        assert "ROWNUM AS DF_RN" in normalized
        assert (
            "ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN, DF_RN"
            in normalized
        )
        assert "WHERE ROWNUM <= :LIM" in normalized


def test_two_fragment_insert_and_update_are_reassembled_and_parsed_once() -> None:
    from connectors.oracle_logminer import classify_sql_redo

    rows = [
        _contents_row(
            101,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VAL',
            1,
        ),
        _contents_row(
            101,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            "UES('1','inserted')",
        ),
        _contents_row(
            102,
            " 0x000001.00000002.0002 ",
            2,
            "UPDATE",
            'UPDATE "T" SET "V"=\'updated',
            1,
        ),
        _contents_row(
            102,
            " 0x000001.00000002.0002 ",
            2,
            "UPDATE",
            '\' WHERE "ID"=\'1\'',
        ),
    ]
    reader = _reader()
    cur = _Cursor(rows)

    with patch(
        "connectors.oracle_logminer.classify_sql_redo",
        wraps=classify_sql_redo,
    ) as classify:
        fetched = reader._query_logminer_rows(
            cur,
            table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl",
            limit=20,
        )
        parsed = [
            classify(row[4], op="insert" if row[3] == "INSERT" else "update", table="T")
            for row in fetched
        ]

    assert len(fetched) == 2
    assert [item[1] for item in parsed] == [
        {"ID": "1", "V": "inserted"},
        {"ID": "1", "V": "updated"},
    ]
    assert classify.call_count == 2


def test_full_window_rewinds_trailing_group_without_loss_or_duplicate(monkeypatch) -> None:
    from connectors.oracle_logminer import classify_sql_redo

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "0")
    rows = [
        _contents_row(
            101,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VALUES(\'1\',\'first\')',
        ),
        _contents_row(
            102,
            " 0x000001.00000002.0002 ",
            2,
            "UPDATE",
            'UPDATE "T" SET "V"=\'sec',
            1,
        ),
        _contents_row(
            102,
            " 0x000001.00000002.0002 ",
            2,
            "UPDATE",
            'ond\' WHERE "ID"=\'1\'',
        ),
    ]
    reader = _reader(batch_size=2)
    reader._open_txn_detection_checked = True
    monkeypatch.setattr(reader, "_probe_oldest_open_txn", lambda _cur=None: None)
    monkeypatch.setattr(reader, "_buffer_off_window_end", lambda: (102, False))
    cur = _WindowCursor(rows)
    conn = _MiningConnection(cur)

    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch("connectors.oracle_logminer.start_logminer_session"),
        patch(
            "services.cdc_incremental_runner.interleave_incremental_snapshot",
            return_value=iter(()),
        ),
        patch(
            "connectors.oracle_logminer.classify_sql_redo",
            wraps=classify_sql_redo,
        ) as classify,
    ):
        first = list(reader.poll())
        assert [row["ID"] for batch in first for row in batch.inserts] == ["1"]
        assert (reader.scn, reader.rs_id, reader.ssn) == (
            101,
            " 0x000001.00000001.0001 ",
            1,
        )

        second = list(reader.poll())

    assert [row["ID"] for batch in second for row in batch.updates] == ["1"]
    assert (reader.scn, reader.rs_id, reader.ssn) == (
        102,
        " 0x000001.00000002.0002 ",
        2,
    )
    assert cur.limits == [2, 2, 4]
    assert classify.call_count == 2


def test_all_rows_in_one_group_refetches_with_doubled_limit() -> None:
    rows = [
        _contents_row(
            101, " 0x000001.00000001.0001 ", 1, "INSERT", "first", 1
        ),
        _contents_row(101, " 0x000001.00000001.0001 ", 1, "INSERT", "second"),
    ]
    reader = _reader(batch_size=2)
    cur = _Cursor(rows, honor_limit=True)

    fetched = reader._query_logminer_rows(
        cur, table_predicate="TABLE_NAME = :tbl", limit=2
    )

    assert cur.limits == [2, 4]
    assert len(fetched) == 1
    assert fetched[0][4] == "firstsecond"


def test_key_group_larger_than_64x_limit_raises_typed_error() -> None:
    reader = _reader(batch_size=2)
    rs_id = " 0x000001.00000001.0007 "
    cur = _Cursor(
        [
            _contents_row(12345, rs_id, 7, "INSERT", "fragment")
            for _ in range(128)
        ],
        honor_limit=True,
    )

    with pytest.raises(
        OracleLogMinerReadError, match=f"12345.*{rs_id}|{rs_id}.*12345"
    ):
        reader._query_logminer_rows(
            cur, table_predicate="TABLE_NAME = :tbl", limit=2
        )

    assert cur.limits == [2, 4, 8, 16, 32, 64, 128]


def test_short_window_ending_with_csf_continuation_raises_typed_error() -> None:
    reader = _reader(batch_size=10)
    cur = _Cursor(
        [
            _contents_row(
                23456,
                " 0x000001.00000001.0008 ",
                8,
                "UPDATE",
                "partial",
                1,
            )
        ]
    )

    with pytest.raises(
        OracleLogMinerReadError,
        match="23456.*0x000001.00000001.0008|0x000001.00000001.0008.*23456",
    ):
        reader._query_logminer_rows(
            cur, table_predicate="TABLE_NAME = :tbl", limit=10
        )


def test_buffered_poll_reassembles_csf_before_classifying(monkeypatch) -> None:
    from connectors.oracle_logminer import classify_sql_redo

    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    txn = (1, 0, 31)
    rows = [
        (
            101,
            " 0x000001.00000001.0000 ",
            0,
            "START",
            None,
            None,
            None,
            *txn,
            0,
            None,
            None,
            None,
            0,
        ),
        (
            102,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            'INSERT INTO "T"("ID","V") VAL',
            "T",
            "APP",
            *txn,
            0,
            "RID-1",
            None,
            None,
            1,
        ),
        (
            102,
            " 0x000001.00000001.0001 ",
            1,
            "INSERT",
            "UES('1','buffered')",
            "T",
            "APP",
            *txn,
            0,
            "RID-1",
            None,
            None,
            0,
        ),
        (
            103,
            " 0x000001.00000001.0002 ",
            2,
            "COMMIT",
            "commit",
            None,
            None,
            *txn,
            0,
            None,
            None,
            None,
            0,
        ),
    ]
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (200,)
    cur.fetchall.return_value = rows
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    reader = _reader(batch_size=2)

    with (
        patch.object(reader, "_mining_conn", return_value=conn),
        patch.object(reader, "_acquire_cdc_lease"),
        patch("connectors.oracle_logminer.fetch_oldest_available_scn", return_value=None),
        patch("connectors.oracle_logminer.fetch_redo_inventory", return_value=[]),
        patch("connectors.oracle_logminer.assert_resume_scn_in_redo"),
        patch("connectors.oracle_logminer.assert_redo_continuity"),
        patch("connectors.oracle_logminer.start_logminer_session"),
        patch(
            "connectors.oracle_logminer.classify_sql_redo",
            wraps=classify_sql_redo,
        ) as classify,
    ):
        batches = list(reader.poll())

    inserts = [row for batch in batches for row in batch.inserts]
    assert inserts == [{"ID": "1", "V": "buffered"}]
    assert classify.call_count == 1
