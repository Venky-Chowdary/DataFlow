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
    ctid_predicate,
    heap_page_ranges,
    key_ranges_from_cuts,
    mapped_single_pk,
    mapping_is_plain_carry,
    mysql_pk_range_clause,
    integer_pk_cuts,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
    pg_type_is_load_safe,
    pk_range_predicate,
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


def test_heap_page_ranges_are_disjoint_and_cover():
    ranges = heap_page_ranges(100, 4)
    assert ranges[0][0] == 0
    assert ranges[-1][1] is None
    for i in range(len(ranges) - 1):
        lo, hi = ranges[i]
        assert hi is not None
        assert lo < hi
        assert hi == ranges[i + 1][0]
    assert heap_page_ranges(0, 8) == [(0, None)]
    assert ctid_predicate(0, None) == ""
    assert "ctid >=" in ctid_predicate(25, None)
    assert "AND" in ctid_predicate(10, 20)
    assert pg_mysql_copy_workers(10) == 1


def test_pk_key_ranges_from_cuts_are_half_open_and_cover():
    ranges = key_ranges_from_cuts(["b", "b", "m"])
    assert ranges[0] == (None, "b")
    assert ranges[1] == ("b", "m")
    assert ranges[-1] == ("m", None)
    assert key_ranges_from_cuts([]) == [(None, None)]
    assert mapped_single_pk(["id"], [("id", "id")]) == ("id", "id")
    assert mapped_single_pk(["id", "sk"], [("id", "id")]) is None
    assert pk_range_predicate("`id`", "'a'", "'m'") == "`id` >= 'a' AND `id` < 'm'"
    clause, params = mysql_pk_range_clause("`id`", "a", "m")
    assert clause == "`id` >= %s AND `id` < %s"
    assert params == ["a", "m"]
    unbounded, unbound_params = mysql_pk_range_clause("`id`", None, None)
    assert unbounded == "1=1"
    assert unbound_params == []
    null_clause, null_params = mysql_pk_range_clause("`id`", None, None, null_shard=True)
    assert null_clause == "`id` IS NULL"
    assert null_params == []
    lo_only, lo_params = mysql_pk_range_clause("`id`", "m", None)
    assert lo_only == "`id` >= %s"
    assert lo_params == ["m"]


def test_auto_copy_workers_scale_with_volume(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "auto")
    assert pg_mysql_copy_workers(10) == 1
    mid = pg_mysql_copy_workers(50_000)
    big = pg_mysql_copy_workers(5_000_000)
    assert 1 <= mid <= 4
    assert mid <= big <= 8
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "3")
    assert pg_mysql_copy_workers(10) == 3


def test_integer_pk_cuts_and_partition_count():
    cuts = integer_pk_cuts(1, 8000, 4)
    assert cuts == [2001, 4001, 6001]
    ranges = key_ranges_from_cuts(cuts)
    assert ranges[0] == (None, 2001)
    assert ranges[1] == (2001, 4001)
    assert ranges[2] == (4001, 6001)
    assert ranges[3] == (6001, None)
    assert integer_pk_cuts(1, 1, 8) == []
    assert pg_mysql_copy_partitions(10, 1) == 1
    assert pg_mysql_copy_partitions(8_000, 4) == 4
    assert pg_mysql_copy_partitions(200_000_000, 4) == 32
    assert pg_mysql_copy_partitions(10_000_000, 4) == 4


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


def test_live_pk_partition_resume_reloads_partial_range(monkeypatch):
    """Completed PK ranges are skipped; a partial range is deleted and reloaded."""
    _mysql_live_or_skip()
    _ensure_local_infile()
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL not reachable on localhost:5432")
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    from services.copy_pg_mysql import copy_postgres_to_mysql

    tag = uuid.uuid4().hex[:8]
    src_table = f"pk_resume_src_{tag}"
    dest_table = f"pk_resume_dst_{tag}"
    source_cfg = {
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    dest_cfg = {
        "host": "localhost",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    pg = psycopg2.connect(
        host="localhost", port=5432, user="dataflow", password="dataflow", dbname="dataflow"
    )
    pg.autocommit = True
    my = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src_table}"')
            cur.execute(
                f'CREATE TABLE "{src_table}" (id bigint PRIMARY KEY, label varchar(32))'
            )
            cur.execute(
                f"""
                INSERT INTO "{src_table}"
                SELECT i, 'r' || i FROM generate_series(1, 8000) AS s(i)
                """
            )
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest_table}`")
        first = copy_postgres_to_mysql(
            source_cfg=source_cfg,
            source_schema="public",
            source_table=src_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        assert first.target_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        assert first.source_snapshot.get("shard_mode") == "pk"
        assert first.source_snapshot.get("copy_split") == "ctid"
        victim = parts[2]
        with my.cursor() as cur:
            # Leave a partial range (not empty, not complete) so resume must DELETE+reload.
            lo = victim["lo"]
            assert lo is not None
            cur.execute(f"DELETE FROM `{dest_table}` WHERE `id` = %s", (lo,))
            cur.execute(f"SELECT COUNT(*) FROM `{dest_table}`")
            assert int(cur.fetchone()[0]) == 7999
        second = copy_postgres_to_mysql(
            source_cfg=source_cfg,
            source_schema="public",
            source_table=src_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_rows == 8000
        assert second.target_rows == 8000
        actions = [p["action"] for p in second.source_snapshot["partition_proof"]]
        assert actions.count("skip") == 3
        assert actions.count("reload") == 1
        assert second.source_snapshot.get("partitions_skipped") == 3
        assert second.source_snapshot.get("copy_split") == "pk"
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest_table}`")
            assert int(cur.fetchone()[0]) == 8000
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src_table}"')
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest_table}`")
        pg.close()
        my.close()
