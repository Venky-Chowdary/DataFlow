"""PROPERTY 9 — every row is accounted for by dest COUNT(*), not writer ack.

AWS DMS can report Full Load success and later MISSING_TARGET: the writer
counted rows the destination engine does not hold. DataFlow closes

    reader == dest COUNT(*) + hold_outs + skipped

from an independent dest-engine population, then certifies that identity on
the migration certificate. Writer ``records_processed`` is diagnostic only.
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

from services.migration_certificate import build_migration_certificate, row_accounting
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


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
        "host": os.environ.get("P9_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P9_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P9_PG_DB", os.environ.get("P2_PG_DB", "dataflow")),
        "username": os.environ.get("P9_PG_USER", os.environ.get("P2_PG_USER", "dataflow")),
        "password": os.environ.get(
            "P9_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")
        ),
    }


def _mysql_creds() -> dict:
    return {
        "host": os.environ.get("P9_MYSQL_HOST", os.environ.get("P2_MYSQL_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P9_MYSQL_PORT", os.environ.get("P2_MYSQL_PORT", "3306"))),
        "database": os.environ.get("P9_MYSQL_DB", os.environ.get("P2_MYSQL_DB", "dataflow")),
        "username": os.environ.get("P9_MYSQL_USER", os.environ.get("P2_MYSQL_USER", "dataflow")),
        "password": os.environ.get(
            "P9_MYSQL_PASSWORD", os.environ.get("P2_MYSQL_PASSWORD", "dataflow")
        ),
    }


def _maps():
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "label",
            "target": "label",
            "source_type": "TEXT",
            "target_type": "VARCHAR(32)",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _job_from_result(result, *, sync_mode: str, src_format: str, dst_format: str) -> dict:
    return {
        "id": result.job_id,
        "status": "completed" if result.success else "failed",
        "records_processed": result.records_transferred,
        "sync_mode": sync_mode,
        "source": {"format": src_format},
        "destination": {"format": dst_format},
        "reconciliation": result.reconciliation or {},
        "destination_summary": result.destination_summary or {},
    }


def test_sqlite_overwrite_certificate_uses_dest_count_not_writer_ack(tmp_path: Path):
    src_path = tmp_path / "p9_src.db"
    dst_path = tmp_path / "p9_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
        )
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    result = _run(req)
    assert result.success, result.error

    dest = sqlite3.connect(str(dst_path))
    try:
        dest_count = dest.execute("SELECT COUNT(*) FROM items_out").fetchone()[0]
    finally:
        dest.close()
    assert dest_count == 4

    job = _job_from_result(
        result, sync_mode="full_refresh_overwrite", src_format="sqlite", dst_format="sqlite"
    )
    # Even if the writer over-claimed, dest COUNT(*) is the written figure.
    job["records_processed"] = 10_000
    ledger = row_accounting(job)
    assert ledger["rows_read"] == 4, ledger
    assert ledger["rows_written"] == dest_count, ledger
    assert ledger["rows_written_source"] == "gate8_dest_readback", ledger
    assert ledger["writer_ack"] == 10_000
    assert ledger["balanced"] is True, ledger
    assert ledger["unaccounted"] == 0

    cert = build_migration_certificate(job)
    assert cert["row_accounting"]["rows_written"] == 4
    assert cert["row_accounting"]["writer_ack"] == 10_000


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_to_mariadb_dest_count_closes_the_certificate():
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p9_src_{suffix}"
    dst_table = f"p9_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  label TEXT NOT NULL
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, label)
                    VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')'''
            )
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"],
            schema="public", table=src_table, ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql",
            host=my["host"], port=my["port"], database=my["database"],
            username=my["username"], password=my["password"],
            schema="", table=dst_table, ssl=False,
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error

        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{dst_table}`")
                dest_count = int(cur.fetchone()[0])
        finally:
            dest.close()
        assert dest_count == 4

        job = _job_from_result(
            result,
            sync_mode="full_refresh_overwrite",
            src_format="postgresql",
            dst_format="mysql",
        )
        job["records_processed"] = 10_000
        ledger = row_accounting(job)
        assert ledger["rows_read"] == 4, ledger
        assert ledger["rows_written"] == dest_count, ledger
        assert ledger["rows_written_source"] == "gate8_dest_readback", ledger
        assert ledger["writer_ack"] == 10_000
        assert ledger["balanced"] is True, ledger
        recon = result.reconciliation or {}
        assert int(recon.get("target_rows") or -1) == dest_count, recon
    finally:
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            dest.close()


def _upsert_contracts(name: str) -> list[dict]:
    return [
        {
            "name": name,
            "selected": True,
            "sync_mode": "upsert",
            "primary_key": "id",
        }
    ]


def test_sqlite_upsert_updates_do_not_change_dest_count(tmp_path: Path):
    """Dest-engine proof: 3 updates + 1 insert → COUNT(*) delta 1, writer ack 4."""
    src_path = tmp_path / "p9_up_src.db"
    dst_path = tmp_path / "p9_up_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany(
            "INSERT INTO items (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        src.commit()
    finally:
        src.close()

    seed = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    seeded = _run(seed)
    assert seeded.success, seeded.error

    src = sqlite3.connect(str(src_path))
    try:
        src.execute("UPDATE items SET label = 'A' WHERE id = 1")
        src.execute("INSERT INTO items (id, label) VALUES (4, 'd')")
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps(),
        stream_contracts=_upsert_contracts("items"),
        sync_mode="upsert",
        validation_mode="warn",
        skip_preflight=True,
    )
    result = _run(req)
    assert result.success, result.error

    dest = sqlite3.connect(str(dst_path))
    try:
        dest_count = dest.execute("SELECT COUNT(*) FROM items_out").fetchone()[0]
        labels = {
            row[0]: row[1]
            for row in dest.execute("SELECT id, label FROM items_out ORDER BY id")
        }
    finally:
        dest.close()
    assert dest_count == 4
    assert labels == {1: "A", 2: "b", 3: "c", 4: "d"}

    job = _job_from_result(result, sync_mode="upsert", src_format="sqlite", dst_format="sqlite")
    job["records_processed"] = 10_000
    ledger = row_accounting(job)
    assert ledger["conservation_kind"] == "keyed", ledger
    assert ledger["inserts"] == 1, ledger
    assert ledger["updates"] == 3, ledger
    assert ledger["dest_delta"] == 1, ledger
    assert ledger["rows_written"] == 1, ledger
    assert ledger["balanced"] is True, ledger
    assert ledger["writer_ack"] == 10_000
    census = (result.destination_summary or {}).get("keyed_census") or {}
    assert int(census.get("dest_preexisting") or -1) == 3, census
    assert int(census.get("unique_batch_keys") or -1) == 4, census


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_to_mariadb_upsert_updates_do_not_change_dest_count():
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p9_up_src_{suffix}"
    dst_table = f"p9_up_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  label TEXT NOT NULL
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, label)
                    VALUES (1, 'a'), (2, 'b'), (3, 'c')'''
            )
    finally:
        conn.close()

    seed = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"],
            schema="public", table=src_table, ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql",
            host=my["host"], port=my["port"], database=my["database"],
            username=my["username"], password=my["password"],
            schema="", table=dst_table, ssl=False,
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        seeded = _run(seed)
        assert seeded.success, seeded.error

        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'''UPDATE public."{src_table}" SET label = 'A' WHERE id = 1''')
                cur.execute(
                    f'''INSERT INTO public."{src_table}" (id, label) VALUES (4, 'd')'''
                )
        finally:
            conn.close()

        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="postgresql",
                host=pg["host"], port=pg["port"], database=pg["database"],
                username=pg["username"], password=pg["password"],
                schema="public", table=src_table, ssl=False,
            ),
            destination=EndpointConfig(
                kind="database", format="mysql",
                host=my["host"], port=my["port"], database=my["database"],
                username=my["username"], password=my["password"],
                schema="", table=dst_table, ssl=False,
            ),
            mappings=_maps(),
            stream_contracts=_upsert_contracts(src_table),
            sync_mode="upsert",
            validation_mode="warn",
            skip_preflight=True,
        )
        result = _run(req)
        assert result.success, result.error

        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{dst_table}`")
                dest_count = int(cur.fetchone()[0])
                cur.execute(f"SELECT id, label FROM `{dst_table}` ORDER BY id")
                labels = {int(r[0]): r[1] for r in cur.fetchall()}
        finally:
            dest.close()
        assert dest_count == 4
        assert labels == {1: "A", 2: "b", 3: "c", 4: "d"}

        job = _job_from_result(
            result, sync_mode="upsert", src_format="postgresql", dst_format="mysql"
        )
        job["records_processed"] = 10_000
        ledger = row_accounting(job)
        assert ledger["conservation_kind"] == "keyed", ledger
        assert ledger["inserts"] == 1, ledger
        assert ledger["updates"] == 3, ledger
        assert ledger["dest_delta"] == 1, ledger
        assert ledger["rows_written"] == 1, ledger
        assert ledger["balanced"] is True, ledger
        census = (result.destination_summary or {}).get("keyed_census") or {}
        assert int(census.get("dest_preexisting") or -1) == 3, census
    finally:
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            dest.close()


def _maps_deleted():
    return _maps() + [
        {
            "source": "is_deleted",
            "target": "is_deleted",
            "source_type": "BOOLEAN",
            "target_type": "BOOLEAN",
            "approved": True,
            "confidence": 0.99,
        }
    ]


def test_sqlite_upsert_tombstone_drops_dest_count(tmp_path: Path):
    """Hard-DELETE dest-held tombstones: dest COUNT 3 → 3 (1 insert, 1 delete)."""
    src_path = tmp_path / "p9_del_src.db"
    dst_path = tmp_path / "p9_del_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL, "
            "is_deleted INTEGER NOT NULL DEFAULT 0)"
        )
        src.executemany(
            "INSERT INTO items (id, label, is_deleted) VALUES (?, ?, ?)",
            [(1, "a", 0), (2, "b", 0), (3, "c", 0)],
        )
        src.commit()
    finally:
        src.close()

    seed = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps_deleted(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    seeded = _run(seed)
    assert seeded.success, seeded.error

    src = sqlite3.connect(str(src_path))
    try:
        src.execute("UPDATE items SET label = 'A' WHERE id = 1")
        src.execute("UPDATE items SET is_deleted = 1 WHERE id = 2")
        src.execute("INSERT INTO items (id, label, is_deleted) VALUES (4, 'd', 0)")
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="items"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="items_out"
        ),
        mappings=_maps_deleted(),
        stream_contracts=_upsert_contracts("items"),
        sync_mode="upsert",
        validation_mode="warn",
        skip_preflight=True,
    )
    result = _run(req)
    assert result.success, result.error

    dest = sqlite3.connect(str(dst_path))
    try:
        dest_count = dest.execute("SELECT COUNT(*) FROM items_out").fetchone()[0]
        labels = {
            row[0]: row[1]
            for row in dest.execute("SELECT id, label FROM items_out ORDER BY id")
        }
    finally:
        dest.close()
    assert dest_count == 3
    assert labels == {1: "A", 3: "c", 4: "d"}

    job = _job_from_result(result, sync_mode="upsert", src_format="sqlite", dst_format="sqlite")
    job["records_processed"] = 10_000
    ledger = row_accounting(job)
    assert ledger["conservation_kind"] == "keyed", ledger
    assert ledger["inserts"] == 1, ledger
    assert ledger["deletes"] == 1, ledger
    assert ledger["dest_delta"] == 0, ledger
    assert ledger["rows_written"] == 0, ledger
    assert ledger["balanced"] is True, ledger
    census = (result.destination_summary or {}).get("keyed_census") or {}
    assert int(census.get("deletes") or -1) == 1, census
    assert int(census.get("unique_tombstone_keys") or -1) == 1, census


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_to_mariadb_upsert_tombstone_drops_dest_count():
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p9_del_src_{suffix}"
    dst_table = f"p9_del_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  label TEXT NOT NULL,
                  is_deleted BOOLEAN NOT NULL DEFAULT FALSE
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, label, is_deleted)
                    VALUES (1, 'a', FALSE), (2, 'b', FALSE), (3, 'c', FALSE)'''
            )
    finally:
        conn.close()

    seed = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"],
            schema="public", table=src_table, ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql",
            host=my["host"], port=my["port"], database=my["database"],
            username=my["username"], password=my["password"],
            schema="", table=dst_table, ssl=False,
        ),
        mappings=_maps_deleted(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        seeded = _run(seed)
        assert seeded.success, seeded.error

        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'''UPDATE public."{src_table}" SET label = 'A' WHERE id = 1'''
                )
                cur.execute(
                    f'''UPDATE public."{src_table}" SET is_deleted = TRUE WHERE id = 2'''
                )
                cur.execute(
                    f'''INSERT INTO public."{src_table}" (id, label, is_deleted)
                        VALUES (4, 'd', FALSE)'''
                )
        finally:
            conn.close()

        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="postgresql",
                host=pg["host"], port=pg["port"], database=pg["database"],
                username=pg["username"], password=pg["password"],
                schema="public", table=src_table, ssl=False,
            ),
            destination=EndpointConfig(
                kind="database", format="mysql",
                host=my["host"], port=my["port"], database=my["database"],
                username=my["username"], password=my["password"],
                schema="", table=dst_table, ssl=False,
            ),
            mappings=_maps_deleted(),
            stream_contracts=_upsert_contracts(src_table),
            sync_mode="upsert",
            validation_mode="warn",
            skip_preflight=True,
        )
        result = _run(req)
        assert result.success, result.error

        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{dst_table}`")
                dest_count = int(cur.fetchone()[0])
                cur.execute(f"SELECT id, label FROM `{dst_table}` ORDER BY id")
                labels = {int(r[0]): r[1] for r in cur.fetchall()}
        finally:
            dest.close()
        assert dest_count == 3
        assert labels == {1: "A", 3: "c", 4: "d"}

        job = _job_from_result(
            result, sync_mode="upsert", src_format="postgresql", dst_format="mysql"
        )
        job["records_processed"] = 10_000
        ledger = row_accounting(job)
        assert ledger["conservation_kind"] == "keyed", ledger
        assert ledger["inserts"] == 1, ledger
        assert ledger["deletes"] == 1, ledger
        assert ledger["dest_delta"] == 0, ledger
        assert ledger["balanced"] is True, ledger
        census = (result.destination_summary or {}).get("keyed_census") or {}
        assert int(census.get("deletes") or -1) == 1, census
    finally:
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            dest.close()

