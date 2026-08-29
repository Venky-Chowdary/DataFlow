"""Unparsed Oracle SQL_REDO must quarantine — never dest upsert."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from services.cdc_engine import ChangeBatch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.oracle_logminer import (  # noqa: E402
    UNPARSED_SQL_REDO_FLAG,
    _parse_sql_redo,
    classify_sql_redo,
    is_unparsed_sql_redo,
)
from src.transfer.cdc_transfer import _apply_change_batch  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


def test_mismatch_and_garbage_sql_redo_are_unparsed() -> None:
    mismatch = _parse_sql_redo('INSERT INTO T("A","B") VALUES(\'only-one\')', op="insert")
    assert mismatch.get(UNPARSED_SQL_REDO_FLAG) == "1"
    kind, detail = classify_sql_redo(
        'INSERT INTO T("A","B") VALUES(\'only-one\')', op="insert", table="T"
    )
    assert kind == "unparsed"
    assert "mismatch" in detail["failure_reason"]
    assert detail["original_value"]["sql_redo"]

    garbage = _parse_sql_redo("BEGIN DBMS_OUTPUT.PUT_LINE('x'); END;", op="insert")
    assert is_unparsed_sql_redo(garbage)
    kind2, detail2 = classify_sql_redo(
        "BEGIN DBMS_OUTPUT.PUT_LINE('x'); END;", op="update", table="T"
    )
    assert kind2 == "unparsed"
    assert "unparsed" in detail2["failure_reason"]


def test_apply_change_batch_refuses_unparsed_sql_redo(tmp_path: Path) -> None:
    dest_path = tmp_path / "oracle_unparsed.db"
    dest = EndpointConfig(
        kind="database", format="sqlite", database=str(dest_path), table="orders"
    )
    dest_cfg = {
        "database": str(dest_path),
        "table": "orders",
        "type": "sqlite",
        "schema": "",
    }
    change = ChangeBatch(
        inserts=[
            {"ID": "1", "AMOUNT": "10"},
            {
                UNPARSED_SQL_REDO_FLAG: "1",
                "_df_parse_error": "insert col/val mismatch (2 vs 1)",
                "_df_sql_redo": 'INSERT INTO T("A","B") VALUES(\'only-one\')',
            },
        ],
        rejected=[],
        resume_token="oracle-logminer:T:1",
    )
    rows, _, summary, deleted = _apply_change_batch(
        "sqlite",
        dest,
        dest_cfg,
        "orders",
        change,
        [
            {"source": "ID", "target": "id"},
            {"source": "AMOUNT", "target": "amount"},
        ],
        {"ID": "integer", "AMOUNT": "decimal"},
        ["ID", "AMOUNT"],
        "id",
        0,
        1,
        job_id="oracle-unparsed-unit",
    )
    assert rows == 1
    assert deleted == 0
    assert int(summary.get("rejected_rows") or 0) == 1
    assert int(summary.get("cdc_unparsed_sql_redo") or 0) == 1
    details = summary.get("rejected_details") or []
    assert details and "mismatch" in str(details[0].get("failure_reason") or "")
    con = sqlite3.connect(str(dest_path))
    try:
        got = list(con.execute('SELECT id, amount FROM "orders" ORDER BY id'))
    finally:
        con.close()
    assert [str(r[0]) for r in got] == ["1"]
    assert "_df_unparsed" not in str(got).lower()
