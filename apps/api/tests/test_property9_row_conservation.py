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
    stamped = result.row_accounting or {}
    assert stamped.get("dest_count") == 4, stamped
    assert stamped.get("rows_written") == 4, stamped
    assert stamped.get("rows_written_source") == "gate8_dest_readback", stamped

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


def test_sqlite_two_table_overwrite_job_is_not_last_table(tmp_path: Path):
    """Last-table dest COUNT(*) is not the job. customers 2 + orders 3 = 5."""
    src_path = tmp_path / "p9_multi_src.db"
    dst_path = tmp_path / "p9_multi_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany("INSERT INTO customers (id, label) VALUES (?, ?)", [(1, "a"), (2, "b")])
        src.executemany(
            "INSERT INTO orders (id, label) VALUES (?, ?)",
            [(1, "o1"), (2, "o2"), (3, "o3")],
        )
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="customers"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="customers"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "customers",
                "selected": True,
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "mappings": _maps(),
            },
            {
                "name": "orders",
                "selected": True,
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "mappings": _maps(),
            },
        ],
    )
    result = _run(req)
    assert result.success, result.error
    stamped = result.row_accounting or {}
    assert stamped.get("conservation_kind") == "job_rollup", stamped
    assert stamped.get("dest_count") == 5, stamped
    assert stamped.get("balanced") is True, stamped
    assert stamped.get("stream_count") == 2, stamped
    # Last table is orders (3). Writer ack must not close, last-table must not close.
    result_job = _job_from_result(
        result, sync_mode="full_refresh_overwrite", src_format="sqlite", dst_format="sqlite"
    )
    result_job["records_processed"] = 10_000
    ledger = row_accounting(result_job)
    assert ledger["dest_count"] == 5, ledger
    assert ledger["conservation_kind"] == "job_rollup", ledger
    dest = sqlite3.connect(str(dst_path))
    try:
        customers = dest.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        orders = dest.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    finally:
        dest.close()
    assert customers == 2
    assert orders == 3
    streams = (result.destination_summary or {}).get("streams") or []
    assert len(streams) == 2, streams
    assert streams[0]["row_accounting"]["dest_count"] == 2
    assert streams[1]["row_accounting"]["dest_count"] == 3


def test_sqlite_two_table_upsert_job_uses_dest_before_not_summed_count(tmp_path: Path):
    """Keyed dest_delta needs dest-before. Job dest COUNT is not 3+4."""
    src_path = tmp_path / "p9_keyed_src.db"
    dst_path = tmp_path / "p9_keyed_dst.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany("INSERT INTO customers (id, label) VALUES (?, ?)", [(1, "a"), (2, "b")])
        src.executemany(
            "INSERT INTO orders (id, label) VALUES (?, ?)",
            [(1, "o1"), (2, "o2"), (3, "o3")],
        )
        src.commit()
    finally:
        src.close()

    contracts = [
        {
            "name": "customers",
            "selected": True,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "mappings": _maps(),
        },
        {
            "name": "orders",
            "selected": True,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "mappings": _maps(),
        },
    ]
    seed = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="customers"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst_path), table="customers"
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        stream_contracts=contracts,
    )
    seeded = _run(seed)
    assert seeded.success, seeded.error

    src = sqlite3.connect(str(src_path))
    try:
        src.execute("UPDATE customers SET label = 'A' WHERE id = 1")
        src.execute("INSERT INTO customers (id, label) VALUES (3, 'c')")
        src.execute("UPDATE orders SET label = 'O1' WHERE id = 1")
        src.commit()
    finally:
        src.close()

    upsert_contracts = [
        {**c, "sync_mode": "upsert"} for c in contracts
    ]
    result = _run(
        TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(src_path), table="customers"
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst_path), table="customers"
            ),
            mappings=_maps(),
            sync_mode="upsert",
            validation_mode="warn",
            skip_preflight=True,
            stream_contracts=upsert_contracts,
        )
    )
    assert result.success, result.error
    stamped = result.row_accounting or {}
    assert stamped.get("conservation_kind") == "job_rollup", stamped
    assert stamped.get("balanced") is True, stamped
    assert stamped.get("dest_count") is None, stamped
    assert stamped.get("summable") is False, stamped
    streams = (result.destination_summary or {}).get("streams") or []
    assert len(streams) == 2, streams
    customers = streams[0]["row_accounting"]
    orders = streams[1]["row_accounting"]
    assert customers["conservation_kind"] == "keyed", customers
    assert orders["conservation_kind"] == "keyed", orders
    assert customers["dest_count_before"] == 2, customers
    assert customers["dest_count"] == 3, customers
    assert customers["dest_delta"] == 1, customers
    assert customers["inserts"] == 1, customers
    assert customers["balanced"] is True, customers
    assert orders["dest_count_before"] == 3, orders
    assert orders["dest_count"] == 3, orders
    assert orders["dest_delta"] == 0, orders
    assert orders["balanced"] is True, orders
    dest = sqlite3.connect(str(dst_path))
    try:
        n_customers = dest.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        n_orders = dest.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        labels = dest.execute("SELECT id, label FROM customers ORDER BY id").fetchall()
    finally:
        dest.close()
    assert n_customers == 3
    assert n_orders == 3
    assert labels == [(1, "A"), (2, "b"), (3, "c")]
    job = _job_from_result(result, sync_mode="upsert", src_format="sqlite", dst_format="sqlite")
    job["records_processed"] = 10_000
    ledger = row_accounting(job)
    assert ledger["conservation_kind"] == "job_rollup", ledger
    assert ledger["dest_count"] is None, ledger
    assert ledger["balanced"] is True, ledger
    # Last table is orders (3). Summing dest COUNT(*) would invent 6. Writer ack
    # is not the job. Job dest stays per-stream.
    assert ledger["writer_ack"] != ledger.get("dest_count")



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


def test_sqlite_overwrite_e2e_clears_preexisting_leftover_dest_keys(tmp_path: Path):
    """execute_tracked overwrite: dest {1,2,3,99} vs S {1,2,3} → dest COUNT=3."""
    src_path = tmp_path / "p9_leftover_src.db"
    dst_path = tmp_path / "p9_leftover_dst.db"
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
    dest = sqlite3.connect(str(dst_path))
    try:
        dest.execute("CREATE TABLE items_out (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        dest.executemany(
            "INSERT INTO items_out (id, label) VALUES (?, ?)",
            [(1, "old"), (2, "old"), (3, "old"), (99, "ghost")],
        )
        dest.commit()
    finally:
        dest.close()

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
        stream_contracts=[
            {
                "name": "items",
                "selected": True,
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "mappings": _maps(),
            }
        ],
    )
    result = _run(req)
    assert result.success, result.error
    dest = sqlite3.connect(str(dst_path))
    try:
        ids = [int(r[0]) for r in dest.execute("SELECT id FROM items_out ORDER BY id").fetchall()]
        dest_count = dest.execute("SELECT COUNT(*) FROM items_out").fetchone()[0]
    finally:
        dest.close()
    assert dest_count == 3, dest_count
    assert ids == [1, 2, 3]
    stamped = result.row_accounting or {}
    assert stamped.get("dest_count") == 3, stamped
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    leftover = (result.destination_summary or {}).get("leftover_deleted")
    assert leftover in {1, None} or int(leftover or 0) >= 1 or recon.get("leftover_deleted") == 1


def test_iceberg_overwrite_e2e_merges_leftover_snapshot_keys(tmp_path: Path):
    """execute_tracked sqlite→iceberg overwrite replaces dest {1,2,3,99} with S.

    Iceberg drop_table is unsupported. First overwrite chunk is snapshot replace
    so dest-only key 99 cannot survive. Payload is label (not qty) so Map does
    not invent decimal and quarantine the load.
    """
    from connectors.iceberg_writer import write_mapped_rows
    from services.dest_precount import destination_key_list, destination_row_count

    warehouse = tmp_path / "wh"
    dest_uri = str(warehouse)
    iceberg_maps = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "label", "target": "label", "transform": "direct"},
    ]
    seed = write_mapped_rows(
        connection_string=dest_uri,
        table_name="leftover_e2e",
        headers=["id", "label"],
        data_rows=[["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]],
        mappings=iceberg_maps,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert seed.ok, seed.error
    src_path = tmp_path / "leftover_src.db"
    src = sqlite3.connect(str(src_path))
    try:
        src.execute("CREATE TABLE leftover_src (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        src.executemany(
            "INSERT INTO leftover_src (id, label) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        src.commit()
    finally:
        src.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src_path), table="leftover_src"
        ),
        destination=EndpointConfig(
            kind="database",
            format="iceberg",
            database=dest_uri,
            connection_string=dest_uri,
            table="leftover_e2e",
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "leftover_src",
                "selected": True,
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "mappings": _maps(),
            }
        ],
    )
    result = _run(req)
    assert result.success, result.error
    cfg = {
        "connection_string": dest_uri,
        "database": dest_uri,
        "host": "",
        "schema": "",
    }
    dest_count = destination_row_count("iceberg", cfg, schema="", table_name="leftover_e2e")
    dest_keys = destination_key_list(
        "iceberg", cfg, schema="", table_name="leftover_e2e", key_columns=["id"]
    )
    assert dest_count == 3, dest_count
    assert dest_keys is not None
    assert {str(t[0]) for t in dest_keys} == {"1", "2", "3"}

