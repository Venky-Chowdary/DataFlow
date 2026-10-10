"""Unit proofs for Oracle LogMiner commit buffering and restart positions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token
from connectors.oracle_logminer_txn import (
    OracleTxnBuffer,
    mining_position,
    oracle_txn_buffer_enabled,
)
from connectors.oracle_logminer import logminer_txn_contents_sql
from connectors.lsn_guards import compare_lsn
from services.cdc_exactly_once import batch_lsn
from services.cdc_transaction_buffer import CdcTxnBufferOverflow


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


def test_oracle_txn_buffer_is_opt_in_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", raising=False)
    monkeypatch.delenv("DATAWRAP_CDC_ORACLE_TXN_BUFFER", raising=False)
    assert oracle_txn_buffer_enabled() is False
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "true")
    assert oracle_txn_buffer_enabled() is True


def test_legacy_token_shape_and_low_scn_fallback_remain_compatible() -> None:
    token = encode_logminer_token(123, table="T")
    assert token == (
        '{"kind":"oracle-logminer","table":"T","scn":123,"phase":"streaming"}'
    )
    from connectors.oracle_logminer import decode_logminer_token

    assert decode_logminer_token(token)["low_scn"] == 123


def test_interleaved_transactions_emit_in_commit_scn_order() -> None:
    buf = OracleTxnBuffer(max_txns=4, max_bytes=10_000)
    buf.feed(xid="a", scn=10, operation="START")
    buf.feed(xid="b", scn=11, operation="START")
    _dml(buf, "a", 12, "0x000001.0001.0001", 1, "a")
    _dml(buf, "b", 13, "0x000001.0001.0002", 2, "b")

    committed_b = buf.feed(
        xid="b",
        scn=20,
        rs_id="0x000001.0001.0003",
        ssn=3,
        operation="COMMIT",
    )
    committed_a = buf.feed(
        xid="a",
        scn=30,
        rs_id="0x000001.0001.0004",
        ssn=4,
        operation="COMMIT",
    )

    assert [txn.xid for txn in committed_b + committed_a] == ["b", "a"]
    assert [txn.commit_scn for txn in committed_b + committed_a] == [20, 30]


def test_invalid_rs_id_cannot_enter_commit_position_order() -> None:
    assert mining_position(100, "", 0) < mining_position(
        100, "0x000001.0001.0001", 1
    )
    with pytest.raises(ValueError, match="refusing opaque ordering"):
        mining_position(100, "scn:100.pXYZ", 0)


def test_full_rollback_discards_transaction() -> None:
    buf = OracleTxnBuffer(max_txns=2, max_bytes=10_000)
    buf.feed(xid="rolled", scn=10, operation="START")
    _dml(buf, "rolled", 11, "0x000001.0001.0001", 1, "x")
    assert buf.feed(
        xid="rolled",
        scn=12,
        rs_id="0x000001.0001.0002",
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
        rs_id="0x000001.0001.0001",
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
        rs_id="0x000001.0001.0002",
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
        rs_id="0x000001.0001.0003",
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
        rs_id="0x000001.0001.0004",
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
    assert "OPERATION IN ('START','COMMIT','ROLLBACK')" in sql
    assert "CASE OPERATION" in sql


def test_open_transaction_and_byte_bounds_raise_typed_remedy() -> None:
    by_count = OracleTxnBuffer(max_txns=1, max_bytes=10_000)
    by_count.feed(xid="one", scn=1, operation="START")
    with pytest.raises(CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_OPEN"):
        by_count.feed(xid="two", scn=2, operation="START")

    by_bytes = OracleTxnBuffer(max_txns=2, max_bytes=4)
    with pytest.raises(CdcTxnBufferOverflow, match="DATAFLOW_CDC_ORACLE_TXN_MAX_BYTES"):
        _dml(by_bytes, "large", 1, "0x000001.0001.0001", 1, "payload")


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
        rs_id="0x000001.0001.0004",
        ssn=4,
        low_scn=100,
        poll_scn=180,
        poll_rs_id="0x000001.0001.0008",
        poll_ssn=8,
        commit_scn=150,
        commit_rs_id="0x000001.0001.0004",
        commit_ssn=4,
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
        emitted_position=(
            mining_position(
                state["commit_scn"], state["commit_rs_id"], state["commit_ssn"]
            )
        ),
    )
    _dml(buf, "already-emitted", 120, "0x000001.0001.0001", 1, "old")
    replayed = buf.feed(
        xid="already-emitted",
        scn=150,
        rs_id="0x000001.0001.0004",
        ssn=4,
        operation="COMMIT",
    )
    assert replayed == []

    _dml(buf, "still-open", 130, "0x000001.0001.0005", 5, "new")
    assert buf.low_scn == 130
    resumed = buf.feed(
        xid="still-open",
        scn=190,
        rs_id="0x000001.0001.0009",
        ssn=9,
        operation="COMMIT",
    )
    assert [txn.xid for txn in resumed] == ["still-open"]


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


def test_oversized_transaction_chunks_have_increasing_same_commit_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1")
    txn_id = (1, 0, 7)
    rows = [
        (101, "0x000001.0001.0000", 0, "START", None, None, None, *txn_id, 0),
    ]
    for index in range(5):
        rs_id = f"0x000001.0001.{index + 1:04x}"
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
    rows.append((150, "0x000001.0001.0008", 8, "COMMIT", "commit", None, None, *txn_id, 0))
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
        (101, "0x000001.0001.0001", 0, "START", None, None, None, 1, 0, 11, 0),
        (
            102,
            "0x000001.0001.0002",
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
        (103, "0x000001.0001.0003", 0, "START", None, None, None, 1, 0, 12, 0),
        (
            104,
            "0x000001.0001.0004",
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
        (110, "0x000001.0001.0005", 2, "COMMIT", None, None, None, 1, 0, 11, 0),
        (120, "0x000001.0001.0006", 2, "COMMIT", None, None, None, 1, 0, 12, 0),
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
