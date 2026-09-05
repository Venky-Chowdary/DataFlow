"""Identity incremental COPY: cursor predicate, fail-closed append, live PG/MySQL."""

from __future__ import annotations

import socket
import uuid
from datetime import datetime

import pytest

from services.copy_fast_path import (
    begin_copy_decline_capture,
    reset_copy_decline_capture,
)
from services.copy_incremental import (
    COPY_INCREMENTAL_MODES,
    mysql_insert_from_staging_sql,
    pg_cursor_predicate_sql,
    pg_insert_from_staging_sql,
)
from services.sync_cursor import get_watermark


def test_incremental_insert_sql_fails_closed_not_ignore():
    mysql = mysql_insert_from_staging_sql(
        "`dest`", "`_df_stg_dest`", ["id", "name"], lambda c: f"`{c}`"
    )
    assert "INSERT INTO `dest`" in mysql
    assert "IGNORE" not in mysql
    pg = pg_insert_from_staging_sql(
        "public.dest", "public._df_stg_dest", ["id", "name"], lambda c: f'"{c}"'
    )
    assert "INSERT INTO public.dest" in pg
    assert "ON CONFLICT" not in pg
    assert "DO NOTHING" not in pg


def test_pg_cursor_predicate_matches_reader_lexicographic():
    class _Cur:
        def mogrify(self, _fmt, args):
            value = args[0]
            if isinstance(value, str):
                return "'" + value.replace("'", "''") + "'"
            return str(value)

    sql = pg_cursor_predicate_sql(
        _Cur(),
        cursor_column="updated_at",
        watermark="2024-06-01 00:00:00\x1f42",
        pk_column="id",
    )
    assert '("updated_at", "id") >' in sql
    assert "'2024-06-01 00:00:00'" in sql
    assert "'42'" in sql
    empty = pg_cursor_predicate_sql(
        _Cur(), cursor_column="updated_at", watermark=None, pk_column="id"
    )
    assert empty == ""
    single = pg_cursor_predicate_sql(
        _Cur(), cursor_column="id", watermark="100", pk_column="id"
    )
    assert single.startswith('"id" >')
    assert "," not in single.split(">")[0]


def test_copy_route_declines_cdc_incremental():
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "postgresql", "table": "orders", "schema": "public"},
    )
    dest = EndpointConfig.from_dict(
        "database",
        {"format": "mysql", "table": "orders", "schema": ""},
    )
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        result = _try_copy_fast_path(
            source=source,
            destination=dest,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "INTEGER"},
            src_type="postgresql",
            dest_type="mysql",
            src_cfg={"type": "postgresql", "table": "orders"},
            dest_cfg={"type": "mysql", "table": "orders"},
            effective_sync="cdc",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("CDC" in r or "cdc" in r for r in sink)
    assert "incremental_deduped" in COPY_INCREMENTAL_MODES


def _pg_mysql_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
        return True
    except OSError:
        return False


def _pg_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


def _cfg(table: str, *, mysql: bool = False) -> dict:
    if mysql:
        return {
            "format": "mysql",
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "table": table,
        }
    return {
        "format": "postgresql",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "schema": "public",
        "username": "dataflow",
        "password": "dataflow",
        "table": table,
    }


def _run_inc(
    *,
    src: str,
    dst: str,
    dest_mysql: bool,
    sync_mode: str,
    job_id: str,
):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    source = EndpointConfig.from_dict("database", _cfg(src))
    destination = EndpointConfig.from_dict("database", _cfg(dst, mysql=dest_mysql))
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "VARCHAR", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": "TIMESTAMP",
            "transform": "none",
        },
    ]
    schema = {"id": "INTEGER", "name": "VARCHAR", "updated_at": "TIMESTAMP"}
    contracts = [
        {
            "name": "stream",
            "selected": True,
            "sync_mode": sync_mode,
            "cursor_field": "updated_at",
            "primary_key": "id",
        }
    ]
    return stream_database_transfer(
        source,
        destination,
        mappings,
        schema,
        sync_mode=sync_mode,
        stream_contracts=contracts,
        job_id=job_id,
    )


@pytest.mark.skipif(not _pg_mysql_up(), reason="PostgreSQL/MySQL not on 5432/3306")
def test_pg_mysql_incremental_deduped_copy_delta_and_watermark():
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_src_{suffix}"
    dst = f"inc_dst_{suffix}"
    pg = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    pg.autocommit = True
    my = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    t1 = datetime(2024, 1, 1, 12, 0, 0)
    t2 = datetime(2024, 1, 2, 12, 0, 0)
    t3 = datetime(2024, 1, 3, 12, 0, 0)
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(
                f'CREATE TABLE "{src}" ('
                "id integer PRIMARY KEY, "
                "name varchar(64) NOT NULL, "
                "updated_at timestamp NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
                f"(1, 'one', %s), (2, 'two', %s), (3, 'three', %s)",
                (t1, t1, t2),
            )
        first, ddl1, summary1, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-a-{suffix}",
        )
        assert first == 3
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        assert summary1.get("sync_mode") == "incremental_deduped"
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        assert wm1
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 3
            cur.execute(f"SELECT name FROM `{dst}` WHERE id = 1")
            assert cur.fetchone()[0] == "one"
        with pg.cursor() as cur:
            cur.execute(
                f"UPDATE \"{src}\" SET name = 'ONE', updated_at = %s WHERE id = 1",
                (t3,),
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES (4, %s, %s)',
                ("four", t3),
            )
        second, ddl2, summary2, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        assert summary2.get("copy_fast_path") == "used"
        assert int((summary2.get("source_snapshot") or {}).get("staging_count") or 0) == 2
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{dst}` WHERE id = 1")
            name = cur.fetchone()[0]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"_df_stg_{dst}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        cursor_key = str(summary2.get("cursor_key") or "")
        assert cursor_key
        stored = get_watermark(cursor_key)
        assert stored
        assert stored != wm1
        third, _ddl3, summary3, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 4
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        pg.close()
        my.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_pg_pg_incremental_append_copy_delta_and_watermark():
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_pgs_{suffix}"
    dst = f"inc_pgd_{suffix}"
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    t1 = datetime(2024, 2, 1, 8, 0, 0)
    t2 = datetime(2024, 2, 2, 8, 0, 0)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
            cur.execute(
                f'CREATE TABLE "{src}" ('
                "id integer PRIMARY KEY, "
                "name varchar(64) NOT NULL, "
                "updated_at timestamp NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
                f"(1, 'a', %s), (2, 'b', %s)",
                (t1, t1),
            )
        first, _ddl1, summary1, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=False,
            sync_mode="incremental_append",
            job_id=f"inc-pg-a-{suffix}",
        )
        assert first == 2
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 2
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES (3, %s, %s)',
                ("c", t2),
            )
        second, _ddl2, summary2, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=False,
            sync_mode="incremental_append",
            job_id=f"inc-pg-b-{suffix}",
        )
        assert second == 1, summary2
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 3
            cur.execute(f'SELECT name FROM "{dst}" ORDER BY id')
            names = [r[0] for r in cur.fetchall()]
        assert names == ["a", "b", "c"]
        assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
        assert get_watermark(str(summary2.get("cursor_key") or ""))
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
        conn.close()
