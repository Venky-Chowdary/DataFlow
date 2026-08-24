"""PROPERTY 7 — referential integrity travels with the data, and we prove it.

AWS DMS does not migrate foreign keys. Full load is alphabetical, so operators
are told to disable enforcement (FOREIGN_KEY_CHECKS=0 / session_replication_role
= replica) and re-add constraints by hand. Airbyte/Fivetran drop FKs. DataFlow
measures the source references, loads parents first, then issues
``ALTER TABLE … ADD CONSTRAINT`` so the destination validates the rows it just
received. A successful add is proof there are no orphans; a rejection is an RI
finding, not a green checksum.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.checkpoint_service import Checkpoint
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from src.transfer.stream_row_accounting import begin_table_population


def test_each_table_starts_its_own_population_on_the_shared_job_checkpoint():
    """Sequential multi-stream must not inherit the previous table's offset."""
    ck = Checkpoint(job_id="j", offset=40, chunk_index=3, rows_processed=40, cursor_value="9")
    begin_table_population(ck)
    assert ck.offset == 0
    assert ck.chunk_index == 0
    assert ck.rows_processed == 0
    assert ck.cursor_value is None


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _pg_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _mysql_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P7_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P7_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P7_PG_DB", os.environ.get("P2_PG_DB", "dataflow")),
        "username": os.environ.get("P7_PG_USER", os.environ.get("P2_PG_USER", "dataflow")),
        "password": os.environ.get("P7_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")),
    }


def _mysql_creds() -> dict:
    return {
        "host": os.environ.get("P7_MYSQL_HOST", os.environ.get("P2_MYSQL_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P7_MYSQL_PORT", os.environ.get("P2_MYSQL_PORT", "3306"))),
        "database": os.environ.get("P7_MYSQL_DB", os.environ.get("P2_MYSQL_DB", "dataflow")),
        "username": os.environ.get("P7_MYSQL_USER", os.environ.get("P2_MYSQL_USER", "dataflow")),
        "password": os.environ.get(
            "P7_MYSQL_PASSWORD", os.environ.get("P2_MYSQL_PASSWORD", "dataflow")
        ),
    }


def _col(source: str, target: str, typ: str) -> dict:
    return {
        "source": source,
        "target": target,
        "source_type": typ,
        "target_type": typ,
        "approved": True,
        "confidence": 0.99,
    }


def _seed_sqlite(path: Path, parent: str, child: str, *, orphan: bool = False) -> None:
    """Parent name sorts *after* child, so alphabetical load would insert orphans."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            f"""
            CREATE TABLE "{parent}" (
              id INTEGER NOT NULL PRIMARY KEY,
              name TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE "{child}" (
              id INTEGER NOT NULL PRIMARY KEY,
              customer_id INTEGER NOT NULL,
              amount INTEGER NOT NULL,
              FOREIGN KEY (customer_id) REFERENCES "{parent}"(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(f'INSERT INTO "{parent}" (id, name) VALUES (1, \'acme\')')
        conn.execute(
            f'INSERT INTO "{child}" (id, customer_id, amount) VALUES (10, 1, 50)'
        )
        if orphan:
            conn.execute(
                f'INSERT INTO "{child}" (id, customer_id, amount) VALUES (11, 999, 1)'
            )
        conn.commit()
    finally:
        conn.close()


def _contracts(parent: str, child: str) -> list[dict]:
    # Child first: operator order and alphabetical order both put the child ahead.
    return [
        {
            "name": child,
            "selected": True,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "mappings": [
                _col("id", "id", "BIGINT"),
                _col("customer_id", "customer_id", "BIGINT"),
                _col("amount", "amount", "INTEGER"),
            ],
        },
        {
            "name": parent,
            "selected": True,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "mappings": [_col("id", "id", "BIGINT"), _col("name", "name", "TEXT")],
        },
    ]


def _sqlite_source(path: Path, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(path),
        table=table,
    )


def _fk_summary(result) -> dict:
    return (result.destination_summary or {}).get("foreign_keys") or {}


def _carried_child(summary: dict, child: str) -> dict:
    decisions = [
        d
        for d in (summary.get("decisions") or [])
        if d.get("dest_table") == child and d.get("status") != "skipped"
    ]
    assert decisions, summary
    return decisions[0]


def test_sqlite_dest_orders_parents_first_and_refuses_rebuild(tmp_path: Path):
    """SQLite cannot ALTER ADD CONSTRAINT; the order is still the algorithm."""
    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    src = tmp_path / "p7_src.db"
    dst = tmp_path / "p7_dst.db"
    _seed_sqlite(src, parent, child)

    result = _run(
        TransferRequest(
            source=_sqlite_source(src, child),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(dst),
                table=child,
            ),
            mappings=[],
            stream_contracts=_contracts(parent, child),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    assert result.success, result.error
    summary = _fk_summary(result)
    assert summary.get("dependency_order") == [parent, child], summary
    child_decision = _carried_child(summary, child)
    assert child_decision["status"] == "unsupported"
    assert "rebuild" in child_decision["reason"]

    dest = sqlite3.connect(str(dst))
    try:
        assert dest.execute(f'SELECT id, name FROM "{parent}"').fetchall() == [(1, "acme")]
        assert dest.execute(
            f'SELECT id, customer_id, amount FROM "{child}"'
        ).fetchall() == [(10, 1, 50)]
        assert dest.execute(f'PRAGMA foreign_key_list("{child}")').fetchall() == []
    finally:
        dest.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening on 5432")
def test_sqlite_to_pg_carries_fk_and_dest_catalog_rejects_orphans(tmp_path: Path):
    import psycopg2

    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    src = tmp_path / "p7_pg_src.db"
    _seed_sqlite(src, parent, child)
    creds = _pg_creds()

    result = _run(
        TransferRequest(
            source=_sqlite_source(src, child),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=child,
                ssl=False,
            ),
            mappings=[],
            stream_contracts=_contracts(parent, child),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    try:
        assert result.success, result.error
        summary = _fk_summary(result)
        assert summary.get("dependency_order") == [parent, child], summary
        assert summary.get("carried") >= 1, summary
        child_decision = _carried_child(summary, child)
        assert child_decision["status"] == "carried"
        assert child_decision.get("integrity_violation") is not True

        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.table_constraints
                     WHERE table_schema = 'public' AND table_name = %s
                       AND constraint_type = 'FOREIGN KEY'
                    """,
                    (child,),
                )
                assert cur.fetchone(), "destination catalog missing FOREIGN KEY"
                cur.execute(f'SELECT id, name FROM public."{parent}"')
                assert cur.fetchall() == [(1, "acme")]
                cur.execute(
                    f'SELECT id, customer_id, amount FROM public."{child}" ORDER BY id'
                )
                assert cur.fetchall() == [(10, 1, 50)]
                with pytest.raises(psycopg2.IntegrityError):
                    cur.execute(
                        f'INSERT INTO public."{child}" (id, customer_id, amount) '
                        "VALUES (99, 999, 1)"
                    )
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{child}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{parent}"')
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening on 5432")
def test_orphan_source_rows_are_an_ri_finding_not_a_green_checksum(tmp_path: Path):
    """ALTER that the engine rejects because of orphans is a data finding.

    SQLite stored the FK but did not enforce it (the MySQL FOREIGN_KEY_CHECKS=0
    class of source). Rows copy. The destination ADD CONSTRAINT is what proves
    the defect — a checksum of the copied rows would have been green.
    """
    import psycopg2

    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    src = tmp_path / "p7_orphan_src.db"
    _seed_sqlite(src, parent, child, orphan=True)
    creds = _pg_creds()

    result = _run(
        TransferRequest(
            source=_sqlite_source(src, child),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=child,
                ssl=False,
            ),
            mappings=[],
            stream_contracts=_contracts(parent, child),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    try:
        assert result.success, result.error
        summary = _fk_summary(result)
        child_decision = _carried_child(summary, child)
        assert child_decision.get("integrity_violation") is True, child_decision
        assert child_decision["status"] == "unsupported"
        assert summary.get("verdict") == "referential_integrity_violated"
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT id, customer_id FROM public."{child}" ORDER BY id'
                )
                assert cur.fetchall() == [(10, 1), (11, 999)]
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.table_constraints
                     WHERE table_schema = 'public' AND table_name = %s
                       AND constraint_type = 'FOREIGN KEY'
                    """,
                    (child,),
                )
                assert cur.fetchone() is None
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{child}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{parent}"')
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening on 5432")
def test_pg_to_pg_same_engine_carries_via_dest_schema():
    """Source catalog is pg_constraint; dest schema isolation avoids clobber."""
    import psycopg2

    creds = _pg_creds()
    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    dest_schema = f"p7dst_{suffix}"
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{dest_schema}"')
            cur.execute(
                f'''
                CREATE TABLE public."{parent}" (
                  id bigint NOT NULL PRIMARY KEY,
                  name text NOT NULL
                )
                '''
            )
            cur.execute(
                f'''
                CREATE TABLE public."{child}" (
                  id bigint NOT NULL PRIMARY KEY,
                  customer_id bigint NOT NULL,
                  amount integer NOT NULL,
                  CONSTRAINT "{child}_fk" FOREIGN KEY (customer_id)
                    REFERENCES public."{parent}"(id) ON DELETE CASCADE
                )
                '''
            )
            cur.execute(f'''INSERT INTO public."{parent}" (id, name) VALUES (1, 'acme')''')
            cur.execute(
                f'''INSERT INTO public."{child}" (id, customer_id, amount)
                    VALUES (10, 1, 50)'''
            )
    finally:
        conn.close()

    result = _run(
        TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=child,
                ssl=False,
            ),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema=dest_schema,
                table=child,
                ssl=False,
            ),
            mappings=[],
            stream_contracts=_contracts(parent, child),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    try:
        assert result.success, result.error
        summary = _fk_summary(result)
        child_decision = _carried_child(summary, child)
        assert child_decision["status"] == "carried", child_decision
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.table_constraints
                     WHERE table_schema = %s AND table_name = %s
                       AND constraint_type = 'FOREIGN KEY'
                    """,
                    (dest_schema, child),
                )
                assert cur.fetchone()
                with pytest.raises(psycopg2.IntegrityError):
                    cur.execute(
                        f'INSERT INTO "{dest_schema}"."{child}" '
                        "(id, customer_id, amount) VALUES (99, 999, 1)"
                    )
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{dest_schema}" CASCADE')
                cur.execute(f'DROP TABLE IF EXISTS public."{child}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{parent}"')
        finally:
            conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL/MariaDB not listening on 3306")
def test_sqlite_to_mariadb_carries_fk_and_dest_catalog_rejects_orphans(tmp_path: Path):
    pymysql = pytest.importorskip("pymysql")

    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    src = tmp_path / "p7_my_src.db"
    _seed_sqlite(src, parent, child)
    creds = _mysql_creds()

    result = _run(
        TransferRequest(
            source=_sqlite_source(src, child),
            destination=EndpointConfig(
                kind="database",
                format="mysql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="",
                table=child,
                ssl=False,
            ),
            mappings=[],
            stream_contracts=_contracts(parent, child),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    try:
        assert result.success, result.error
        summary = _fk_summary(result)
        child_decision = _carried_child(summary, child)
        assert child_decision["status"] == "carried", child_decision

        conn = pymysql.connect(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            user=creds["username"],
            password=creds["password"],
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT CONSTRAINT_TYPE FROM information_schema.table_constraints
                     WHERE table_schema = DATABASE() AND table_name = %s
                       AND CONSTRAINT_TYPE = 'FOREIGN KEY'
                    """,
                    (child,),
                )
                assert cur.fetchone(), "destination catalog missing FOREIGN KEY"
                cur.execute(f"SELECT id, name FROM `{parent}`")
                assert cur.fetchall() == ((1, "acme"),)
                try:
                    cur.execute(
                        f"INSERT INTO `{child}` (id, customer_id, amount) "
                        "VALUES (99, 999, 1)"
                    )
                    raise AssertionError("destination FOREIGN KEY did not reject orphan")
                except pymysql.err.IntegrityError:
                    pass
                except pymysql.err.OperationalError as exc:
                    # MariaDB: 1452 foreign key constraint fails.
                    if not exc.args or exc.args[0] not in (1452, 1216):
                        raise
        finally:
            conn.close()
    finally:
        conn = pymysql.connect(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            user=creds["username"],
            password=creds["password"],
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{child}`")
                cur.execute(f"DROP TABLE IF EXISTS `{parent}`")
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening on 5432")
def test_single_table_child_carries_fk_when_parent_already_on_dest(tmp_path: Path):
    import psycopg2

    suffix = uuid.uuid4().hex[:8]
    parent = f"zzz_cust_{suffix}"
    child = f"aaa_ord_{suffix}"
    src = tmp_path / "p7_child_only.db"
    _seed_sqlite(src, parent, child)
    creds = _pg_creds()
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'''
                CREATE TABLE public."{parent}" (
                  id bigint NOT NULL PRIMARY KEY,
                  name text NOT NULL
                )
                '''
            )
            cur.execute(f'''INSERT INTO public."{parent}" (id, name) VALUES (1, 'acme')''')
    finally:
        conn.close()

    result = _run(
        TransferRequest(
            source=_sqlite_source(src, child),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=child,
                ssl=False,
            ),
            mappings=[
                _col("id", "id", "BIGINT"),
                _col("customer_id", "customer_id", "BIGINT"),
                _col("amount", "amount", "INTEGER"),
            ],
            stream_contracts=[
                {
                    "name": child,
                    "selected": True,
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "id",
                }
            ],
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="warn",
        )
    )
    try:
        assert result.success, result.error
        summary = _fk_summary(result)
        child_decision = _carried_child(summary, child)
        assert child_decision["status"] == "carried", child_decision
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                with pytest.raises(psycopg2.IntegrityError):
                    cur.execute(
                        f'INSERT INTO public."{child}" (id, customer_id, amount) '
                        "VALUES (99, 999, 1)"
                    )
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{child}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{parent}"')
        finally:
            conn.close()
