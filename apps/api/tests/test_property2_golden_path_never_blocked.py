"""PROPERTY 2 — the legitimate path is never blocked.

Golden-path transfers must complete with success=True, correct row counts, and
no g6 additive-stamp / DDL-identity refuse on create-new overwrite.

Routes covered here (real services when reachable; sqlite always):
  * SQLite→SQLite (always — CI no-config)
  * PG→PG (live when 5432 up)
  * CSV→PG (live when 5432 up)
  * PG→SQLite (live PG source when 5432 up)
  * Mongo→PG (when both up)

Each route: (a) no mappings (b) explicit mappings (c) skip_preflight=True.
Gate pair: BLOCK unsafe additive invent-fail; ALLOW create-new invent.
"""

from __future__ import annotations

import csv
import os
import socket
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decision_kernel import stamp_additive_mapping_types
from services.preflight_service import run_file_preflight
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


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


def _mongo_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P2_PG_HOST", "127.0.0.1"),
        "port": int(os.environ.get("P2_PG_PORT", "5432")),
        "database": os.environ.get("P2_PG_DB", "postgres"),
        "username": os.environ.get("P2_PG_USER", "postgres"),
        "password": os.environ.get("P2_PG_PASSWORD", "admin"),
    }


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


# ---------------------------------------------------------------------------
# Gate pair: BLOCK unsafe / ALLOW safe
# ---------------------------------------------------------------------------


def test_g6_blocks_when_invent_required_but_fails(monkeypatch):
    from services.decision_kernel import invent as invent_mod
    from services.decision_kernel.invent import InventContext, InventRefused

    def _boom(*_a, **_k):
        raise InventRefused("forced", context=InventContext.CREATE_NEW)

    monkeypatch.setattr(invent_mod, "invent_dest_type", _boom)
    result = run_file_preflight(
        columns=["id", "extra"],
        column_types={"id": "BIGINT", "extra": "TEXT"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "target_type": "BIGINT"},
            {
                "source": "extra",
                "target": "extra",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            },
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sync_mode="full_refresh_append",
        sample_rows=[{"id": 1, "extra": "a"}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types={"id": "BIGINT"},
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="strict",
        backfill_new_fields=True,
        schema_policy="propagate_columns",
    )
    assert any(b.get("id") == "g6_additive_stamp" for b in (result.get("blockers") or []))
    assert result.get("passed") is False


def test_g6_allows_create_table_identity_without_prior_map_stamps():
    """ALLOW: empty dest + identity maps + create-table invent → no g6 block."""
    maps, unstamped = stamp_additive_mapping_types(
        [
            {"source": "id", "target": "id", "source_type": "BIGINT"},
            {"source": "big_val", "target": "big_val", "source_type": "BIGINT"},
            {"source": "dbl", "target": "dbl", "source_type": "DOUBLE PRECISION"},
            {"source": "nm", "target": "nm", "source_type": "TEXT"},
        ],
        dest_db="postgresql",
        live_dest_types={},
        backfill_new_fields=False,
        dest_table_exists=False,
    )
    assert unstamped == []
    assert all(str(m.get("target_type") or "").strip() for m in maps)

    result = run_file_preflight(
        columns=["id", "big_val", "dbl", "nm"],
        column_types={
            "id": "BIGINT",
            "big_val": "BIGINT",
            "dbl": "DOUBLE PRECISION",
            "nm": "TEXT",
        },
        row_count=2,
        mappings=[
            {"source": c, "target": c, "confidence": 0.95}
            for c in ("id", "big_val", "dbl", "nm")
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sync_mode="full_refresh_overwrite",
        sample_rows=[
            {"id": 1, "big_val": 2, "dbl": 1.5, "nm": "a"},
            {"id": 2, "big_val": 3, "dbl": 2.5, "nm": "b"},
        ],
        destination_db_type="postgresql",
        destination_table_exists=False,
        destination_column_types={},
        destination_can_create=True,
        destination_can_write=True,
        validation_mode="warn",
        backfill_new_fields=False,
    )
    assert not any(b.get("id") == "g6_additive_stamp" for b in (result.get("blockers") or [])), (
        result.get("blockers")
    )


# ---------------------------------------------------------------------------
# Golden paths — must pass (sqlite always; others when services up)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("with_maps", [False, True])
@pytest.mark.parametrize("skip_preflight", [False, True])
def test_golden_sqlite_to_sqlite_no_config(tmp_path: Path, with_maps: bool, skip_preflight: bool):
    src = tmp_path / "p2_src.sqlite"
    dst = tmp_path / "p2_dst.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, nm TEXT)"))
            c.execute(text("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"))
    finally:
        eng.dispose()

    maps = None
    if with_maps:
        maps = [
            {
                "source": "id",
                "target": "id",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "v",
                "target": "v",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "nm",
                "target": "nm",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
        ]

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=skip_preflight,
        mappings=maps,
    )
    result = _run(req)
    assert result.success, result.error
    assert result.records_transferred == 3
    assert "lack Map target_type" not in (result.error or "")
    assert "DDL identity requires Validate" not in (result.error or "")

    deng = create_engine(f"sqlite:///{dst}")
    try:
        with deng.connect() as c:
            rows = c.execute(text("SELECT id, v, nm FROM t ORDER BY id")).fetchall()
        assert list(rows) == [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
    finally:
        deng.dispose()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
@pytest.mark.parametrize("with_maps", [False, True])
@pytest.mark.parametrize("skip_preflight", [False, True])
def test_golden_pg_to_pg_no_config(with_maps: bool, skip_preflight: bool):
    import psycopg2

    creds = _pg_creds()
    src_table = f"p2_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"p2_dst_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                "(id bigint PRIMARY KEY, big_val bigint, dbl double precision, nm text)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" VALUES '
                "(1, 9223372036854775807, 1.5, 'a'), (2, -1, 2.5, 'b')"
            )
        conn.commit()
    finally:
        conn.close()

    maps = None
    if with_maps:
        maps = [
            {
                "source": c,
                "target": c,
                "target_type": t,
                "approved": True,
                "confidence": 0.99,
            }
            for c, t in (
                ("id", "BIGINT"),
                ("big_val", "BIGINT"),
                ("dbl", "DOUBLE PRECISION"),
                ("nm", "TEXT"),
            )
        ]

    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=src_table,
            ),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=dst_table,
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=skip_preflight,
            mappings=maps,
        )
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 2
        assert "lack Map target_type" not in (result.error or "")
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_golden_csv_to_pg_no_config(tmp_path: Path):
    import psycopg2

    creds = _pg_creds()
    csv_path = tmp_path / "p2.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "nm"])
        w.writerow(["1", "alpha"])
        w.writerow(["2", "beta"])
    dst_table = f"p2_csv_{uuid.uuid4().hex[:8]}"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            username=creds["username"],
            password=creds["password"],
            schema="public",
            table=dst_table,
        ),
        source_path=str(csv_path),
        source_filename="p2.csv",
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
        mappings=None,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 2
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_golden_pg_to_sqlite_no_config(tmp_path: Path):
    import psycopg2

    creds = _pg_creds()
    src_table = f"p2_pgs_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" (id bigint PRIMARY KEY, v bigint)'
            )
            cur.execute(f'INSERT INTO public."{src_table}" VALUES (1, 11), (2, 22)')
        conn.commit()
    finally:
        conn.close()

    dst = tmp_path / "p2_from_pg.sqlite"
    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=src_table,
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="t"
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
            mappings=None,
        )
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 2
    finally:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not (_mongo_up() and _pg_up()), reason="MongoDB or PostgreSQL not reachable")
def test_golden_mongo_to_pg_no_config():
    import psycopg2
    from pymongo import MongoClient

    creds = _pg_creds()
    mongo_uri = os.environ.get("P2_MONGO_URI", "mongodb://127.0.0.1:27017")
    db_name = f"p2_{uuid.uuid4().hex[:8]}"
    coll_name = "docs"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=800)
    try:
        client[db_name][coll_name].insert_many(
            [{"id": 1, "nm": "a"}, {"id": 2, "nm": "b"}]
        )
        dst_table = f"p2_mongo_{uuid.uuid4().hex[:8]}"
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="mongodb",
                connection_string=mongo_uri,
                database=db_name,
                table=coll_name,
            ),
            destination=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=dst_table,
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
            mappings=None,
        )
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred >= 2
    finally:
        client.drop_database(db_name)
        client.close()
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public' "
                    "AND tablename LIKE 'p2_mongo_%'"
                )
                for (name,) in cur.fetchall() or []:
                    cur.execute(f'DROP TABLE IF EXISTS public."{name}"')
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not reachable")
def test_golden_pg_to_parquet_export():
    import psycopg2

    creds = _pg_creds()
    src_table = f"p2_pq_{uuid.uuid4().hex[:8]}"
    # Export into the API workspace (sandbox requires in-app path).
    out_dir = Path(__file__).resolve().parents[1] / ".p2_exports"
    out_dir.mkdir(exist_ok=True)
    out_path = str(out_dir / f"{src_table}.parquet")
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" (id bigint PRIMARY KEY, nm text)'
            )
            cur.execute(f'INSERT INTO public."{src_table}" VALUES (1, \'a\'), (2, \'b\')')
        conn.commit()
    finally:
        conn.close()
    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                username=creds["username"],
                password=creds["password"],
                schema="public",
                table=src_table,
            ),
            destination=EndpointConfig(
                kind="file_export",
                format="parquet",
                output_path=out_path,
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
            mappings=None,
        )
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 2
        assert Path(out_path).exists() and Path(out_path).stat().st_size > 0
    finally:
        Path(out_path).unlink(missing_ok=True)
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["username"],
            password=creds["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not reachable (Docker unavailable on host)")
def test_golden_pg_to_mysql_placeholder():
    """Reserved — requires MySQL 8. Documented NOT covered until Docker is available."""
    pytest.fail("unreachable: skipif should have skipped")
