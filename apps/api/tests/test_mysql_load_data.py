"""MySQL LOAD DATA LOCAL INFILE — STRICT, warning rollback, dest COUNT(*)."""

from __future__ import annotations

import socket
import sys
import uuid
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_load_data import (  # noqa: E402
    blocking_load_data_warnings,
    build_load_data_sql,
    load_data_text_value,
    mysql_load_data_eligible,
    mysql_load_data_enabled,
    quote_load_data_path,
    render_load_data_tsv,
)
from connectors.mysql_writer import write_mapped_rows  # noqa: E402
from services.copy_pg_mysql import (  # noqa: E402
    _mysql_create_sql,
    mapping_is_plain_carry,
    pg_type_is_load_safe,
)


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_tsv_null_is_slash_n_not_empty_string():
    assert load_data_text_value(None) == "\\N"
    assert load_data_text_value("") == ""
    assert load_data_text_value(0) == "0"
    assert load_data_text_value(False) == "0"
    assert load_data_text_value(True) == "1"
    assert load_data_text_value(date(2020, 1, 2)) == "2020-01-02"


def test_tsv_escapes_tab_newline_backslash():
    raw = "a\tb\\c\nd"
    escaped = load_data_text_value(raw)
    assert "\t" not in escaped
    assert "\n" not in escaped
    assert escaped == "a\\tb\\\\c\\nd"
    tsv = render_load_data_tsv([("1", raw), ("2", None)])
    assert tsv.split("\n")[0] == "1\ta\\tb\\\\c\\nd"
    assert tsv.split("\n")[1] == "2\t\\N"


def test_load_data_sql_is_tab_delimited_local_infile():
    sql = build_load_data_sql(
        table_q="`orders`",
        columns=["id", "amount"],
        infile_sql="'/tmp/df_mysql_ld_x.tsv'",
    )
    assert sql.startswith("LOAD DATA LOCAL INFILE '/tmp/df_mysql_ld_x.tsv' INTO TABLE `orders`")
    assert "CHARACTER SET utf8mb4" in sql
    assert r"FIELDS TERMINATED BY '\t'" in sql
    assert r"ESCAPED BY '\\'" in sql
    assert "(`id`, `amount`)" in sql
    assert "IGNORE" not in sql


def test_quote_load_data_path_refuses_quote():
    with pytest.raises(ValueError, match="quote"):
        quote_load_data_path("/tmp/o'brian.tsv")


def test_eligible_refuses_upsert_binary_and_disabled(monkeypatch):
    ok, reason = mysql_load_data_eligible(
        write_mode="upsert",
        conflict_columns=["id"],
        target_cols=["id"],
        target_types=["INT"],
        proxy=False,
    )
    assert ok is False
    assert "upsert" in reason or "conflict" in reason

    ok, reason = mysql_load_data_eligible(
        write_mode="insert",
        conflict_columns=None,
        target_cols=["payload"],
        target_types=["BLOB"],
        proxy=False,
    )
    assert ok is False
    assert "binary" in reason

    ok, reason = mysql_load_data_eligible(
        write_mode="insert",
        conflict_columns=None,
        target_cols=["id"],
        target_types=["INT"],
        proxy=True,
    )
    assert ok is False
    assert "proxy" in reason

    monkeypatch.setenv("DATAFLOW_MYSQL_LOAD_DATA", "0")
    assert mysql_load_data_enabled() is False
    ok, reason = mysql_load_data_eligible(
        write_mode="insert",
        conflict_columns=None,
        target_cols=["id"],
        target_types=["INT"],
        proxy=False,
    )
    assert ok is False
    assert "MYSQL_LOAD_DATA=0" in reason


def test_copy_pg_mysql_declines_lossy_transform_and_jsonb():
    ok, reason = mapping_is_plain_carry(
        [{"source": "id", "target": "id", "transform": "hash_pii"}]
    )
    assert ok is False
    assert "hash_pii" in reason
    ok, _ = mapping_is_plain_carry(
        [{"source": "id", "target": "id", "transform": "none"}]
    )
    assert ok is True
    assert pg_type_is_load_safe("VARCHAR(32)") is True
    assert pg_type_is_load_safe("BIGINT") is True
    assert pg_type_is_load_safe("DATE") is True
    assert pg_type_is_load_safe("jsonb") is False
    assert pg_type_is_load_safe("bytea") is False
    assert pg_type_is_load_safe("timestamp with time zone") is False
    sql = _mysql_create_sql(
        "orders",
        [("id", "id"), ("amt", "amt")],
        ["BIGINT", "DECIMAL(10,2)"],
        ["id"],
    )
    assert sql.startswith("CREATE TABLE `orders`")
    assert "PRIMARY KEY (`id`)" in sql


def test_warning_rows_block_commit_notes_do_not():
    blocked = blocking_load_data_warnings(
        [("Warning", 1265, "Data truncated for column 'age' at row 2")]
    )
    assert blocked
    notes = blocking_load_data_warnings(
        [("Note", 1592, "Unsafe statement written to the binary log")]
    )
    assert notes == []


def _mysql_live_or_skip() -> dict:
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL not reachable on localhost:3306")
    return {
        "host": "localhost",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "",
        "connection_string": "",
        "ssl": False,
    }


def _ensure_local_infile() -> None:
    """Desktop lab only — writer itself never SET GLOBAL."""
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"cannot SET GLOBAL local_infile as root: {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("SET GLOBAL local_infile = 1")
            cur.execute("SELECT @@GLOBAL.local_infile")
            raw = cur.fetchone()[0]
        if str(raw).strip().lower() not in {"1", "on", "true"}:
            pytest.skip("server local_infile still OFF after SET GLOBAL")
    finally:
        conn.close()


def test_live_load_data_lands_dest_count_and_round_trips_tab():
    common = _mysql_live_or_skip()
    _ensure_local_infile()
    table = "mysql_ld_" + uuid.uuid4().hex[:8]
    tabbed = "hello\tworld"
    result = write_mapped_rows(
        **common,
        table_name=table,
        headers=["id", "label"],
        mappings=[_mapping("id", "id"), _mapping("label", "label")],
        column_types={"id": "INTEGER", "label": "TEXT"},
        data_rows=[["1", "alpha"], ["2", tabbed], ["3", "gamma"]],
        create_table=True,
        write_mode="insert",
        error_policy="quarantine",
    )
    assert result.ok, result.error
    assert result.rows_written == 3
    assert result.rejected_rows == 0
    assert result.load_method == "load_data"

    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            assert int(cur.fetchone()[0]) == 3
            cur.execute(f"SELECT id, label FROM `{table}` WHERE id = 2")
            row = cur.fetchone()
            assert row[1] == tabbed
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
    finally:
        conn.close()


def test_live_strict_unfit_int_quarantines_not_zero():
    """Invalid INT must not land as 0. Dest COUNT omits the quarantined row."""
    common = _mysql_live_or_skip()
    _ensure_local_infile()
    table = "mysql_ld_q_" + uuid.uuid4().hex[:8]
    result = write_mapped_rows(
        **common,
        table_name=table,
        headers=["id", "age"],
        mappings=[_mapping("id", "id"), _mapping("age", "age")],
        column_types={"id": "INTEGER", "age": "INTEGER"},
        data_rows=[["1", "30"], ["2", "not-an-int"]],
        create_table=True,
        write_mode="insert",
        error_policy="quarantine",
    )
    assert result.ok, result.error
    assert result.rejected_rows >= 1
    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            assert int(cur.fetchone()[0]) == 1
            cur.execute(f"SELECT id, age FROM `{table}`")
            rows = cur.fetchall()
            assert rows == ((1, 30),)
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
    finally:
        conn.close()


def test_live_env_off_uses_insert_and_still_counts(monkeypatch):
    common = _mysql_live_or_skip()
    monkeypatch.setenv("DATAFLOW_MYSQL_LOAD_DATA", "0")
    table = "mysql_ld_off_" + uuid.uuid4().hex[:8]
    result = write_mapped_rows(
        **common,
        table_name=table,
        headers=["id", "label"],
        mappings=[_mapping("id", "id"), _mapping("label", "label")],
        column_types={"id": "INTEGER", "label": "TEXT"},
        data_rows=[["1", "a"], ["2", "b"]],
        create_table=True,
        write_mode="insert",
        error_policy="quarantine",
    )
    assert result.ok, result.error
    assert result.rows_written == 2
    assert result.load_method == "insert"
    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            assert int(cur.fetchone()[0]) == 2
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
    finally:
        conn.close()
