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


def test_logminer_where_and_predicates_parse_pk() -> None:
    from connectors.oracle_logminer import _parse_sql_redo

    sql = (
        'update "DATAFLOW"."T" set "AMOUNT" = \'99\' '
        "where \"ID\" = '1' and \"AMOUNT\" = '10' and ROWID = 'AAASjEAAMAAAADTAAA';"
    )
    row = _parse_sql_redo(sql, op="update")
    assert row.get("ID") == "1"
    assert row.get("AMOUNT") == "99"
    delete_sql = (
        'delete from "DATAFLOW"."T" where "ID" = \'2\' and ROWID = \'AAASjEAAMAAAADTAAA\';'
    )
    deleted = _parse_sql_redo(delete_sql, op="delete")
    assert deleted.get("ID") == "2"


def test_infer_cdb_service_for_xe_and_free() -> None:
    from connectors.oracle_logminer import OracleLogMinerCdc

    assert OracleLogMinerCdc.infer_cdb_service("XEPDB1") == "XE"
    assert OracleLogMinerCdc.infer_cdb_service("FREEPDB1") == "FREE"
    assert OracleLogMinerCdc.infer_cdb_service("ORCLPDB1") == "ORCLCDB"
    assert OracleLogMinerCdc.infer_cdb_service("CUSTOM") == ""


def test_decode_logminer_token_unwraps_double_encoded_json() -> None:
    """Persist used to json.dumps(encode_logminer_token(...)) — decode must restore."""
    import json

    from connectors.oracle_logminer import decode_logminer_token, encode_logminer_token

    token = encode_logminer_token(2981200, table="T", phase="streaming")
    dumped = json.dumps(token)
    assert dumped.startswith('"')
    state = decode_logminer_token(dumped)
    assert state["scn"] == 2981200
    assert state["phase"] == "streaming"
    assert state["table"] == "T"


def test_serialize_resume_token_does_not_double_encode_json_string() -> None:
    import json

    from connectors.oracle_logminer import encode_logminer_token
    from services.cdc_resume_tokens import serialize_resume_token, unwrap_resume_token

    token = encode_logminer_token(2981200, table="T", phase="streaming")
    once = serialize_resume_token(token)
    twice = serialize_resume_token(once)
    dumped = json.dumps(token)
    from_dumped = serialize_resume_token(dumped)
    assert once == token
    assert twice == token
    assert from_dumped == token
    parsed = json.loads(once)
    assert isinstance(parsed, dict)
    assert parsed["kind"] == "oracle-logminer"
    assert parsed["scn"] == 2981200
    assert not once.startswith('"')
    unwrapped = unwrap_resume_token(dumped)
    assert isinstance(unwrapped, dict)
    assert unwrapped["scn"] == 2981200


def test_oracle_logminer_cdc_keeps_streaming_scn_from_dumped_watermark() -> None:
    """A double-encoded watermark must not reset to phase=initial / scn=0."""
    import json

    from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token

    token = encode_logminer_token(2981200, table="CDC_T", phase="streaming")
    dumped = json.dumps(token)
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "database": "XEPDB1", "username": "DATAFLOW"},
        table="CDC_T",
        primary_key="ID",
        schema="DATAFLOW",
        resume_token=dumped,
    )
    assert cdc.scn == 2981200
    assert cdc.phase == "streaming"


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
        "host": "",
        "port": 0,
        "database": str(dest_path),
        "table": "orders",
        "username": "",
        "password": "",
        "connection_string": "",
        "ssl": False,
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
