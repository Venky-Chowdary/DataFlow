"""Identity incremental COPY: cursor predicate, fail-closed append, live PG/MySQL/SQLite."""

from __future__ import annotations

import sqlite3
import socket
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

from services.copy_fast_path import (
    begin_copy_decline_capture,
    reset_copy_decline_capture,
)
from services.copy_incremental import (
    COPY_INCREMENTAL_MODES,
    identity_incremental_route,
    mysql_cursor_predicate_sql,
    mysql_insert_from_staging_sql,
    pg_cursor_predicate_sql,
    pg_insert_from_staging_sql,
    sqlite_cursor_predicate_sql,
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


def test_mysql_cursor_predicate_matches_reader_lexicographic():
    class _Cur:
        def mogrify(self, fmt, args):
            out = fmt
            for value in args:
                if isinstance(value, str):
                    lit = "'" + value.replace("'", "''") + "'"
                else:
                    lit = str(value)
                out = out.replace("%s", lit, 1)
            return out

    sql = mysql_cursor_predicate_sql(
        _Cur(),
        cursor_column="updated_at",
        watermark="2024-06-01 00:00:00\x1f42",
        pk_column="id",
    )
    assert "(`updated_at`, `id`) >" in sql
    assert "'2024-06-01 00:00:00'" in sql
    assert "'42'" in sql
    empty = mysql_cursor_predicate_sql(
        _Cur(), cursor_column="updated_at", watermark=None, pk_column="id"
    )
    assert empty == ""
    single = mysql_cursor_predicate_sql(
        _Cur(), cursor_column="id", watermark="100", pk_column="id"
    )
    assert single.startswith("`id` >")
    assert "," not in single.split(">")[0]


def test_sqlite_cursor_predicate_matches_lexicographic():
    sql = sqlite_cursor_predicate_sql(
        cursor_column="updated_at",
        watermark="2024-06-01 00:00:00\x1f42",
        pk_column="id",
    )
    assert '("updated_at", "id") >' in sql
    assert "'2024-06-01 00:00:00'" in sql
    assert "'42'" in sql
    empty = sqlite_cursor_predicate_sql(
        cursor_column="updated_at", watermark=None, pk_column="id"
    )
    assert empty == ""
    single = sqlite_cursor_predicate_sql(
        cursor_column="id", watermark="100", pk_column="id"
    )
    assert single.startswith('"id" >')
    assert "," not in single.split(">")[0]


def test_identity_incremental_route_sql_core_only():
    assert identity_incremental_route("mysql", "postgresql")
    assert identity_incremental_route("mariadb", "postgres")
    assert identity_incremental_route("mysql", "mysql")
    assert identity_incremental_route("postgresql", "mysql")
    assert identity_incremental_route("sqlite", "sqlite")
    assert identity_incremental_route("mysql", "sqlite")
    assert identity_incremental_route("postgresql", "sqlite")
    assert identity_incremental_route("sqlite", "postgresql")
    assert identity_incremental_route("sqlite", "mysql")
    assert not identity_incremental_route("mysql", "snowflake")
    assert not identity_incremental_route("csv", "sqlite")


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


def test_copy_route_declines_sqlite_datetime_to_pg_incremental() -> None:
    """SQLite DATETIME is COPY-unsafe into PostgreSQL; TEXT is the proven cursor."""
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "table": "orders", "database": "/tmp/x.db"},
    )
    dest = EndpointConfig.from_dict(
        "database",
        {"format": "postgresql", "table": "orders", "schema": "public"},
    )
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        result = _try_copy_fast_path(
            source=source,
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "type": "INTEGER"},
                {"source": "updated_at", "target": "updated_at", "type": "DATETIME"},
            ],
            schema={"id": "INTEGER", "updated_at": "DATETIME"},
            src_type="sqlite",
            dest_type="postgresql",
            src_cfg={"type": "sqlite", "table": "orders", "database": "/tmp/x.db"},
            dest_cfg={"type": "postgresql", "table": "orders"},
            effective_sync="incremental_deduped",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
            incremental_watermark="2024-01-01 00:00:00\x1f1",
            incremental_pk="id",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("DATETIME" in r or "COPY-safe" in r for r in sink)


def test_copy_route_declines_sqlite_datetime_to_mysql_incremental() -> None:
    """SQLite DATETIME is COPY-unsafe into MySQL; TEXT is the proven cursor."""
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "table": "orders", "database": "/tmp/x.db"},
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
            mappings=[
                {"source": "id", "target": "id", "type": "INTEGER"},
                {"source": "updated_at", "target": "updated_at", "type": "DATETIME"},
            ],
            schema={"id": "INTEGER", "updated_at": "DATETIME"},
            src_type="sqlite",
            dest_type="mysql",
            src_cfg={"type": "sqlite", "table": "orders", "database": "/tmp/x.db"},
            dest_cfg={"type": "mysql", "table": "orders"},
            effective_sync="incremental_append",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
            incremental_watermark="2024-01-01 00:00:00\x1f1",
            incremental_pk="id",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("DATETIME" in r or "COPY-safe" in r or "MySQL COPY-safe" in r for r in sink)


def test_copy_route_declines_pg_timestamp_sqlite_incremental() -> None:
    """PG TIMESTAMP is COPY-unsafe into SQLite; DATE is the proven identity cursor."""
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "postgresql", "table": "orders", "schema": "public"},
    )
    dest = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "table": "orders", "database": "/tmp/x.db"},
    )
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        result = _try_copy_fast_path(
            source=source,
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "type": "INTEGER"},
                {"source": "updated_at", "target": "updated_at", "type": "TIMESTAMP"},
            ],
            schema={"id": "INTEGER", "updated_at": "TIMESTAMP"},
            src_type="postgresql",
            dest_type="sqlite",
            src_cfg={"type": "postgresql", "table": "orders"},
            dest_cfg={"type": "sqlite", "table": "orders", "database": "/tmp/x.db"},
            effective_sync="incremental_append",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
            incremental_watermark="2024-01-01\x1f1",
            incremental_pk="id",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("TIMESTAMP" in r or "COPY-safe" in r for r in sink)


def test_copy_route_declines_mysql_timestamp_sqlite_incremental() -> None:
    """MySQL TIMESTAMP is COPY-unsafe into SQLite; DATETIME is the proven cursor."""
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "mysql", "table": "orders", "schema": ""},
    )
    dest = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "table": "orders", "database": "/tmp/x.db"},
    )
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        result = _try_copy_fast_path(
            source=source,
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "type": "INTEGER"},
                {"source": "updated_at", "target": "updated_at", "type": "TIMESTAMP"},
            ],
            schema={"id": "INTEGER", "updated_at": "TIMESTAMP"},
            src_type="mysql",
            dest_type="sqlite",
            src_cfg={"type": "mysql", "table": "orders"},
            dest_cfg={"type": "sqlite", "table": "orders", "database": "/tmp/x.db"},
            effective_sync="incremental_deduped",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
            incremental_watermark="2024-01-01 00:00:00\x1f1",
            incremental_pk="id",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("TIMESTAMP" in r or "COPY-safe" in r for r in sink)


def test_copy_route_declines_mysql_timestamp_incremental():
    from src.transfer.copy_route import _try_copy_fast_path
    from src.transfer.models import EndpointConfig

    source = EndpointConfig.from_dict(
        "database",
        {"format": "mysql", "table": "orders", "schema": ""},
    )
    dest = EndpointConfig.from_dict(
        "database",
        {"format": "postgresql", "table": "orders", "schema": "public"},
    )
    sink: list[str] = []
    token, _ = begin_copy_decline_capture(sink)
    try:
        result = _try_copy_fast_path(
            source=source,
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "type": "INTEGER"},
                {"source": "updated_at", "target": "updated_at", "type": "TIMESTAMP"},
            ],
            schema={"id": "INTEGER", "updated_at": "TIMESTAMP"},
            src_type="mysql",
            dest_type="postgresql",
            src_cfg={"type": "mysql", "table": "orders"},
            dest_cfg={"type": "postgresql", "table": "orders"},
            effective_sync="incremental_deduped",
            incremental=True,
            source_filter=None,
            limit=0,
            checkpoint=None,
            incremental_cursor="updated_at",
            incremental_watermark="2024-01-01 00:00:00\x1f1",
            incremental_pk="id",
        )
    finally:
        reset_copy_decline_capture(token)
    assert result is None
    assert any("TIMESTAMP" in r or "COPY-safe" in r for r in sink)


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


def _mysql_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 3306), timeout=1).close()
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
    src_mysql: bool = False,
    cursor_type: str = "TIMESTAMP",
):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    source = EndpointConfig.from_dict("database", _cfg(src, mysql=src_mysql))
    destination = EndpointConfig.from_dict("database", _cfg(dst, mysql=dest_mysql))
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "VARCHAR", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": cursor_type,
            "transform": "none",
        },
    ]
    schema = {"id": "INTEGER", "name": "VARCHAR", "updated_at": cursor_type}
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


def _run_inc_into_sqlite(
    *,
    src: str,
    dst_path: Path,
    dst: str,
    sync_mode: str,
    job_id: str,
    src_mysql: bool = False,
    cursor_type: str,
):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    source = EndpointConfig.from_dict("database", _cfg(src, mysql=src_mysql))
    destination = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "database": str(dst_path), "table": dst},
    )
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "VARCHAR", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": cursor_type,
            "transform": "none",
        },
    ]
    schema = {"id": "INTEGER", "name": "VARCHAR", "updated_at": cursor_type}
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


@pytest.mark.skipif(not _pg_mysql_up(), reason="PostgreSQL/MySQL not on 5432/3306")
def test_mysql_pg_incremental_deduped_copy_delta_and_watermark():
    """MySQL DATETIME is COPY-safe; TIMESTAMP is not. Delta then no-op."""
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_msrc_{suffix}"
    dst = f"inc_mpgd_{suffix}"
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
    t1 = datetime(2024, 3, 1, 12, 0, 0)
    t2 = datetime(2024, 3, 2, 12, 0, 0)
    t3 = datetime(2024, 3, 3, 12, 0, 0)
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id INT PRIMARY KEY, "
                "name VARCHAR(64) NOT NULL, "
                "updated_at DATETIME NOT NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES "
                "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
                (1, "one", t1, 2, "two", t1, 3, "three", t2),
            )
        first, ddl1, summary1, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=False,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-pg-a-{suffix}",
        )
        assert first == 3, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        assert summary1.get("sync_mode") == "incremental_deduped"
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        assert wm1
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 3
            cur.execute(f'SELECT name FROM "{dst}" WHERE id = 1')
            assert cur.fetchone()[0] == "one"
        with my.cursor() as cur:
            cur.execute(
                f"UPDATE `{src}` SET name = 'ONE', updated_at = %s WHERE id = 1",
                (t3,),
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES (4, %s, %s)",
                ("four", t3),
            )
        second, ddl2, summary2, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=False,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-pg-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        assert summary2.get("copy_fast_path") == "used"
        assert int((summary2.get("source_snapshot") or {}).get("staging_count") or 0) == 2
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            dest_count = int(cur.fetchone()[0])
            cur.execute(f'SELECT name FROM "{dst}" WHERE id = 1')
            name = cur.fetchone()[0]
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"public._df_stg_{dst}",),
            )
            staging_left = cur.fetchone()[0]
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
            dest_mysql=False,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-pg-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 4
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        pg.close()
        my.close()


@pytest.mark.skipif(not _pg_mysql_up(), reason="PostgreSQL/MySQL not on 5432/3306")
def test_mysql_mysql_incremental_append_copy_delta_and_watermark():
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_mms_{suffix}"
    dst = f"inc_mmd_{suffix}"
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    t1 = datetime(2024, 4, 1, 8, 0, 0)
    t2 = datetime(2024, 4, 2, 8, 0, 0)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`, `{dst}`")
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id INT PRIMARY KEY, "
                "name VARCHAR(64) NOT NULL, "
                "updated_at DATETIME NOT NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES "
                "(%s, %s, %s), (%s, %s, %s)",
                (1, "a", t1, 2, "b", t1),
            )
        first, _ddl1, summary1, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_append",
            job_id=f"inc-mysql-mysql-a-{suffix}",
        )
        assert first == 2
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 2
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES (%s, %s, %s)",
                (3, "c", t2),
            )
        second, _ddl2, summary2, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_append",
            job_id=f"inc-mysql-mysql-b-{suffix}",
        )
        assert second == 1, summary2
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{dst}` ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"_df_stg_{dst}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        assert staging_left is None
        assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
        stored = get_watermark(str(summary2.get("cursor_key") or ""))
        assert stored
        third, _ddl3, summary3, _ = _run_inc(
            src=src,
            dst=dst,
            dest_mysql=True,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_append",
            job_id=f"inc-mysql-mysql-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(str(summary2.get("cursor_key") or "")) == stored
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`, `{dst}`")
        conn.close()


def _run_inc_sqlite(
    *,
    src_path: Path,
    dst_path: Path,
    src: str,
    dst: str,
    sync_mode: str,
    job_id: str,
):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    source = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "database": str(src_path), "table": src},
    )
    destination = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "database": str(dst_path), "table": dst},
    )
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "TEXT", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": "TEXT",
            "transform": "none",
        },
    ]
    schema = {"id": "INTEGER", "name": "TEXT", "updated_at": "TEXT"}
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


def _run_inc_from_sqlite(
    *,
    src_path: Path,
    src: str,
    dst: str,
    sync_mode: str,
    job_id: str,
    dest_mysql: bool = False,
):
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    source = EndpointConfig.from_dict(
        "database",
        {"format": "sqlite", "database": str(src_path), "table": src},
    )
    destination = EndpointConfig.from_dict("database", _cfg(dst, mysql=dest_mysql))
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "name", "target": "name", "type": "TEXT", "transform": "none"},
        {
            "source": "updated_at",
            "target": "updated_at",
            "type": "TEXT",
            "transform": "none",
        },
    ]
    schema = {"id": "INTEGER", "name": "TEXT", "updated_at": "TEXT"}
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


def test_sqlite_sqlite_incremental_deduped_copy_delta_and_watermark(tmp_path):
    suffix = uuid.uuid4().hex[:8]
    src_path = tmp_path / f"inc_ss_{suffix}.db"
    dst_path = tmp_path / f"inc_sd_{suffix}.db"
    src = "inc_src"
    dst = "inc_dst"
    t1 = "2024-05-01 12:00:00"
    t2 = "2024-05-02 12:00:00"
    t3 = "2024-05-03 12:00:00"
    conn = sqlite3.connect(src_path)
    try:
        conn.execute(
            f'CREATE TABLE "{src}" ('
            "id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
            "(1, 'one', ?), (2, 'two', ?), (3, 'three', ?)",
            (t1, t1, t2),
        )
        conn.commit()
        first, ddl1, summary1, _ = _run_inc_sqlite(
            src_path=src_path,
            dst_path=dst_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-a-{suffix}",
        )
        assert first == 3, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        assert wm1
        dest = sqlite3.connect(dst_path)
        try:
            n = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
        finally:
            dest.close()
        assert n == 3
        assert name == "one"
        conn.execute(
            f'UPDATE "{src}" SET name = ?, updated_at = ? WHERE id = 1',
            ("ONE", t3),
        )
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES (4, ?, ?)',
            ("four", t3),
        )
        conn.commit()
        second, ddl2, summary2, _ = _run_inc_sqlite(
            src_path=src_path,
            dst_path=dst_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        assert summary2.get("copy_fast_path") == "used"
        dest = sqlite3.connect(dst_path)
        try:
            dest_count = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
            staging_left = dest.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"_df_stg_{dst}",),
            ).fetchone()
        finally:
            dest.close()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        cursor_key = str(summary2.get("cursor_key") or "")
        stored = get_watermark(cursor_key)
        assert stored
        assert stored != wm1
        third, _ddl3, summary3, _ = _run_inc_sqlite(
            src_path=src_path,
            dst_path=dst_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        dest = sqlite3.connect(dst_path)
        try:
            assert int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0]) == 4
        finally:
            dest.close()
    finally:
        conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not on 3306")
def test_mysql_sqlite_incremental_deduped_copy_delta_and_watermark(tmp_path):
    """MySQL DATETIME → SQLite: delta then no-op. TIMESTAMP stays COPY-unsafe."""
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_msql_{suffix}"
    dst = "inc_dst"
    dst_path = tmp_path / f"inc_mysql_sqlite_{suffix}.db"
    my = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    t1 = datetime(2024, 6, 1, 12, 0, 0)
    t2 = datetime(2024, 6, 2, 12, 0, 0)
    t3 = datetime(2024, 6, 3, 12, 0, 0)
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id INT PRIMARY KEY, "
                "name VARCHAR(64) NOT NULL, "
                "updated_at DATETIME NOT NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES "
                "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
                (1, "one", t1, 2, "two", t1, 3, "three", t2),
            )
        first, ddl1, summary1, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-sqlite-a-{suffix}",
        )
        assert first == 3, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        assert "sqlite" in str(summary1.get("load_method") or "")
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        assert wm1
        dest = sqlite3.connect(dst_path)
        try:
            n = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
        finally:
            dest.close()
        assert n == 3
        assert name == "one"
        with my.cursor() as cur:
            cur.execute(
                f"UPDATE `{src}` SET name = 'ONE', updated_at = %s WHERE id = 1",
                (t3,),
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES (4, %s, %s)",
                ("four", t3),
            )
        second, ddl2, summary2, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-sqlite-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        assert summary2.get("copy_fast_path") == "used"
        assert int((summary2.get("source_snapshot") or {}).get("staging_count") or 0) == 2
        dest = sqlite3.connect(dst_path)
        try:
            dest_count = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
            staging_left = dest.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"_df_stg_{dst}",),
            ).fetchone()
        finally:
            dest.close()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        cursor_key = str(summary2.get("cursor_key") or "")
        stored = get_watermark(cursor_key)
        assert stored
        assert stored != wm1
        third, _ddl3, summary3, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_deduped",
            job_id=f"inc-mysql-sqlite-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        dest = sqlite3.connect(dst_path)
        try:
            assert int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0]) == 4
        finally:
            dest.close()
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        my.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_pg_sqlite_incremental_append_copy_delta_and_watermark(tmp_path):
    """PG DATE → SQLite: TIMESTAMP is COPY-unsafe; DATE lands as TEXT."""
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_pgsql_{suffix}"
    dst = "inc_dst"
    dst_path = tmp_path / f"inc_pg_sqlite_{suffix}.db"
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    d1 = date(2024, 7, 1)
    d2 = date(2024, 7, 2)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(
                f'CREATE TABLE "{src}" ('
                "id integer PRIMARY KEY, "
                "name varchar(64) NOT NULL, "
                "updated_at date NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
                f"(1, 'a', %s), (2, 'b', %s)",
                (d1, d1),
            )
        first, ddl1, summary1, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=False,
            cursor_type="DATE",
            sync_mode="incremental_append",
            job_id=f"inc-pg-sqlite-a-{suffix}",
        )
        assert first == 2, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        dest = sqlite3.connect(dst_path)
        try:
            n = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
        finally:
            dest.close()
        assert n == 2
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES (3, %s, %s)',
                ("c", d2),
            )
        second, ddl2, summary2, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=False,
            cursor_type="DATE",
            sync_mode="incremental_append",
            job_id=f"inc-pg-sqlite-b-{suffix}",
        )
        assert second == 1, (second, ddl2, summary2)
        dest = sqlite3.connect(dst_path)
        try:
            dest_count = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            names = [
                r[0]
                for r in dest.execute(f'SELECT name FROM "{dst}" ORDER BY id').fetchall()
            ]
            staging_left = dest.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"_df_stg_{dst}",),
            ).fetchone()
        finally:
            dest.close()
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        assert staging_left is None
        assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
        assert get_watermark(str(summary2.get("cursor_key") or ""))
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_pg_sqlite_incremental_deduped_copy_delta_and_watermark(tmp_path):
    """PG DATE → SQLite upsert: 3 → 2 dest COUNT 4 → no-op."""
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_pgsd_{suffix}"
    dst = "inc_dst"
    dst_path = tmp_path / f"inc_pg_sqlite_dedupe_{suffix}.db"
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    d1 = date(2024, 8, 1)
    d2 = date(2024, 8, 2)
    d3 = date(2024, 8, 3)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            cur.execute(
                f'CREATE TABLE "{src}" ('
                "id integer PRIMARY KEY, "
                "name varchar(64) NOT NULL, "
                "updated_at date NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
                f"(1, 'one', %s), (2, 'two', %s), (3, 'three', %s)",
                (d1, d1, d2),
            )
        first, ddl1, summary1, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=False,
            cursor_type="DATE",
            sync_mode="incremental_deduped",
            job_id=f"inc-pg-sqlite-d-a-{suffix}",
        )
        assert first == 3, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        dest = sqlite3.connect(dst_path)
        try:
            n = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
        finally:
            dest.close()
        assert n == 3
        assert name == "one"
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        with conn.cursor() as cur:
            cur.execute(
                f'UPDATE "{src}" SET name = %s, updated_at = %s WHERE id = 1',
                ("ONE", d3),
            )
            cur.execute(
                f'INSERT INTO "{src}" (id, name, updated_at) VALUES (4, %s, %s)',
                ("four", d3),
            )
        second, ddl2, summary2, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=False,
            cursor_type="DATE",
            sync_mode="incremental_deduped",
            job_id=f"inc-pg-sqlite-d-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        dest = sqlite3.connect(dst_path)
        try:
            dest_count = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            name = dest.execute(f'SELECT name FROM "{dst}" WHERE id = 1').fetchone()[0]
            staging_left = dest.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"_df_stg_{dst}",),
            ).fetchone()
        finally:
            dest.close()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        cursor_key = str(summary2.get("cursor_key") or "")
        stored = get_watermark(cursor_key)
        assert stored and stored != wm1
        third, _ddl3, summary3, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=False,
            cursor_type="DATE",
            sync_mode="incremental_deduped",
            job_id=f"inc-pg-sqlite-d-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        dest = sqlite3.connect(dst_path)
        try:
            assert int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0]) == 4
        finally:
            dest.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not on 3306")
def test_mysql_sqlite_incremental_append_copy_delta_and_watermark(tmp_path):
    """MySQL DATETIME → SQLite append: 2 → 1 dest COUNT 3. Duplicate PK fails closed."""
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src = f"inc_msqla_{suffix}"
    dst = "inc_dst"
    dst_path = tmp_path / f"inc_mysql_sqlite_append_{suffix}.db"
    my = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    t1 = datetime(2024, 9, 1, 8, 0, 0)
    t2 = datetime(2024, 9, 2, 8, 0, 0)
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id INT PRIMARY KEY, "
                "name VARCHAR(64) NOT NULL, "
                "updated_at DATETIME NOT NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES "
                "(%s, %s, %s), (%s, %s, %s)",
                (1, "a", t1, 2, "b", t1),
            )
        first, ddl1, summary1, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_append",
            job_id=f"inc-mysql-sqlite-ap-a-{suffix}",
        )
        assert first == 2, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        dest = sqlite3.connect(dst_path)
        try:
            n = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
        finally:
            dest.close()
        assert n == 2
        with my.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{src}` (id, name, updated_at) VALUES (3, %s, %s)",
                ("c", t2),
            )
        second, ddl2, summary2, _ = _run_inc_into_sqlite(
            src=src,
            dst_path=dst_path,
            dst=dst,
            src_mysql=True,
            cursor_type="DATETIME",
            sync_mode="incremental_append",
            job_id=f"inc-mysql-sqlite-ap-b-{suffix}",
        )
        assert second == 1, (second, ddl2, summary2)
        dest = sqlite3.connect(dst_path)
        try:
            dest_count = int(dest.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0])
            names = [
                r[0]
                for r in dest.execute(f'SELECT name FROM "{dst}" ORDER BY id').fetchall()
            ]
            staging_left = dest.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"_df_stg_{dst}",),
            ).fetchone()
        finally:
            dest.close()
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        assert staging_left is None
        assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
        assert get_watermark(str(summary2.get("cursor_key") or ""))
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        my.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not on 5432")
def test_sqlite_pg_incremental_deduped_copy_delta_and_watermark(tmp_path):
    """SQLite TEXT → PostgreSQL: DATETIME is COPY-unsafe. Delta then no-op."""
    psycopg2 = pytest.importorskip("psycopg2")
    suffix = uuid.uuid4().hex[:8]
    src_path = tmp_path / f"inc_sqlite_pg_{suffix}.db"
    src = "inc_src"
    dst = f"inc_spg_{suffix}"
    t1 = "2024-10-01 12:00:00"
    t2 = "2024-10-02 12:00:00"
    t3 = "2024-10-03 12:00:00"
    conn = sqlite3.connect(src_path)
    pg = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    pg.autocommit = True
    try:
        conn.execute(
            f'CREATE TABLE "{src}" ('
            "id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES '
            "(1, 'one', ?), (2, 'two', ?), (3, 'three', ?)",
            (t1, t1, t2),
        )
        conn.commit()
        first, ddl1, summary1, _ = _run_inc_from_sqlite(
            src_path=src_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-pg-a-{suffix}",
        )
        assert first == 3, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_deduped" in str(summary1.get("load_method") or "")
        wm1 = str(summary1.get("watermark") or summary1.get("incremental_watermark") or "")
        assert wm1
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 3
            cur.execute(f'SELECT name FROM "{dst}" WHERE id = 1')
            assert cur.fetchone()[0] == "one"
        conn.execute(
            f'UPDATE "{src}" SET name = ?, updated_at = ? WHERE id = 1',
            ("ONE", t3),
        )
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES (4, ?, ?)',
            ("four", t3),
        )
        conn.commit()
        second, ddl2, summary2, _ = _run_inc_from_sqlite(
            src_path=src_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-pg-b-{suffix}",
        )
        assert second == 2, (second, ddl2, summary2)
        assert summary2.get("copy_fast_path") == "used"
        assert int((summary2.get("source_snapshot") or {}).get("staging_count") or 0) == 2
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            dest_count = int(cur.fetchone()[0])
            cur.execute(f'SELECT name FROM "{dst}" WHERE id = 1')
            name = cur.fetchone()[0]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (f"_df_stg_{dst}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 4
        assert name == "ONE"
        assert staging_left is None
        cursor_key = str(summary2.get("cursor_key") or "")
        stored = get_watermark(cursor_key)
        assert stored and stored != wm1
        third, _ddl3, summary3, _ = _run_inc_from_sqlite(
            src_path=src_path,
            src=src,
            dst=dst,
            sync_mode="incremental_deduped",
            job_id=f"inc-sqlite-pg-c-{suffix}",
        )
        assert third == 0
        assert summary3.get("source_row_count") == 0
        assert get_watermark(cursor_key) == stored
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{dst}"')
            assert int(cur.fetchone()[0]) == 4
    finally:
        conn.close()
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
        pg.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not on 3306")
def test_sqlite_mysql_incremental_append_copy_delta_and_watermark(tmp_path):
    """SQLite TEXT → MySQL: DATETIME is COPY-unsafe. 2 → 1 dest COUNT 3."""
    pymysql = pytest.importorskip("pymysql")
    suffix = uuid.uuid4().hex[:8]
    src_path = tmp_path / f"inc_sqlite_mysql_{suffix}.db"
    src = "inc_src"
    dst = f"inc_smy_{suffix}"
    t1 = "2024-11-01 08:00:00"
    t2 = "2024-11-02 08:00:00"
    conn = sqlite3.connect(src_path)
    my = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    try:
        conn.execute(
            f'CREATE TABLE "{src}" ('
            "id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES (1, ?, ?), (2, ?, ?)',
            ("a", t1, "b", t1),
        )
        conn.commit()
        first, ddl1, summary1, _ = _run_inc_from_sqlite(
            src_path=src_path,
            src=src,
            dst=dst,
            dest_mysql=True,
            sync_mode="incremental_append",
            job_id=f"inc-sqlite-mysql-a-{suffix}",
        )
        assert first == 2, (first, ddl1, summary1)
        assert summary1.get("copy_fast_path") == "used"
        assert "incremental_append" in str(summary1.get("load_method") or "")
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            assert int(cur.fetchone()[0]) == 2
        conn.execute(
            f'INSERT INTO "{src}" (id, name, updated_at) VALUES (3, ?, ?)',
            ("c", t2),
        )
        conn.commit()
        second, ddl2, summary2, _ = _run_inc_from_sqlite(
            src_path=src_path,
            src=src,
            dst=dst,
            dest_mysql=True,
            sync_mode="incremental_append",
            job_id=f"inc-sqlite-mysql-b-{suffix}",
        )
        assert second == 1, (second, ddl2, summary2)
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dst}`")
            dest_count = int(cur.fetchone()[0])
            cur.execute(f"SELECT name FROM `{dst}` ORDER BY id")
            names = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (f"_df_stg_{dst}",),
            )
            staging_left = cur.fetchone()
        assert dest_count == 3
        assert names == ["a", "b", "c"]
        assert staging_left is None
        assert int((summary2.get("source_snapshot") or {}).get("dest_count") or 0) == 3
        assert get_watermark(str(summary2.get("cursor_key") or ""))
    finally:
        conn.close()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        my.close()
