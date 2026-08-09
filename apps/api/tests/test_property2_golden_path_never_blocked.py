"""PROPERTY 2 — the legitimate path is never blocked.

Golden-path transfers must complete with success=True, correct row counts,
reconciliation.passed, and no g6 additive-stamp / DDL-identity refuse on
create-new overwrite.

Routes covered here (real services when reachable; sqlite/csv always):
  * SQLite→SQLite (always — CI no-config)
  * CSV→SQLite resume-after-kill (always)
  * PG→PG / CSV→PG / PG→SQLite / PG→Parquet (when 5432 up)
  * PG→MySQL (when 3306 up — CI services)
  * Mongo→PG (when both up)

Each route: (a) no mappings (b) explicit mappings (c) skip_preflight=True
where parametrized; (d) resume-after-kill on CSV→SQLite + SQLite checkpoint.
Gate pair: BLOCK unsafe additive invent-fail; ALLOW create-new invent.
"""

from __future__ import annotations

import csv
import io
import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.checkpoint_service import Checkpoint
from services.decision_kernel import stamp_additive_mapping_types
from services.preflight_service import run_file_preflight
import src.transfer.engine as engine_mod
from src.transfer import file_stream
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


def _mysql_creds() -> dict:
    return {
        "host": os.environ.get("P2_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("P2_MYSQL_PORT", "3306")),
        "database": os.environ.get("P2_MYSQL_DB", "dataflow"),
        "username": os.environ.get("P2_MYSQL_USER", "dataflow"),
        "password": os.environ.get("P2_MYSQL_PASSWORD", "dataflow"),
    }


def _run(req: TransferRequest, job_id: str | None = None, *, resume: bool = False):
    jid = job_id or uuid.uuid4().hex[:24]
    return UniversalTransferEngine().execute_tracked(req, jid, resume=resume)


def _assert_transfer_ok(result, expected_rows: int) -> None:
    assert result.success, result.error
    assert result.records_transferred == expected_rows
    assert "lack Map target_type" not in (result.error or "")
    assert "DDL identity requires Validate" not in (result.error or "")
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    if recon.get("target_rows") is not None:
        assert int(recon["target_rows"]) == expected_rows, recon
    if recon.get("source_checksum") and recon.get("target_checksum"):
        assert recon["source_checksum"] == recon["target_checksum"], recon


class _FakeMongo:
    """Minimal job store so resume tests can seed a durable checkpoint."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = job_data.get("job_id") or uuid.uuid4().hex[:24]
        self.jobs[job_id] = dict(job_data)
        return job_id


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
    _assert_transfer_ok(result, 3)

    deng = create_engine(f"sqlite:///{dst}")
    try:
        with deng.connect() as c:
            rows = c.execute(text("SELECT id, v, nm FROM t ORDER BY id")).fetchall()
        assert list(rows) == [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")]
    finally:
        deng.dispose()


def test_golden_csv_to_sqlite_resume_after_kill(tmp_path: Path, monkeypatch):
    """(d) resume-after-kill: partial CSV write, then resume — no dupes, recon passes."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAWRAP_JOB_STORE", "memory")
    import services.mongodb_service as mongo_mod

    monkeypatch.setattr(mongo_mod, "_mongodb_service", None)
    old = file_stream.CHUNK_SIZE
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 2)

    def _csv_bytes(rows: list[dict]) -> bytes:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["id", "v", "nm"])
        w.writeheader()
        w.writerows(rows)
        return buf.getvalue().encode("utf-8")

    db_path = tmp_path / "p2_resume.sqlite"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(db_path),
        table="t",
    )

    def _req(rows: list[dict]) -> TransferRequest:
        return TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="p2.csv",
            source_content=_csv_bytes(rows),
            destination=dest,
            sync_mode="upsert",
            stream_contracts=[
                {
                    "name": "t",
                    "sync_mode": "upsert",
                    "primary_key": "id",
                    "selected": True,
                }
            ],
            skip_preflight=True,
            validation_mode="warn",
            mappings=None,
        )

    engine = UniversalTransferEngine()
    job_id = uuid.uuid4().hex[:24]
    first = engine.execute_tracked(
        _req(
            [
                {"id": "1", "v": "10", "nm": "a"},
                {"id": "2", "v": "20", "nm": "b"},
            ]
        ),
        job_id,
    )
    assert first.success, first.error
    assert first.records_transferred == 2

    full = _req(
        [
            {"id": "1", "v": "10", "nm": "a"},
            {"id": "2", "v": "20", "nm": "b"},
            {"id": "3", "v": "30", "nm": "c"},
            {"id": "4", "v": "40", "nm": "d"},
            {"id": "5", "v": "50", "nm": "e"},
        ]
    )
    result = engine.execute_tracked(full, job_id, resume=True)
    _assert_transfer_ok(result, 5)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT id FROM t ORDER BY id").fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5]
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", old)
    monkeypatch.setattr(mongo_mod, "_mongodb_service", None)


def test_golden_sqlite_to_sqlite_resume_from_checkpoint(tmp_path: Path, monkeypatch):
    """(d) DB→DB: seed checkpoint mid-table, resume overwrite — full row set, no dupes."""
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    src = tmp_path / "p2_resume_src.sqlite"
    dst = tmp_path / "p2_resume_dst.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))
            c.execute(
                text(
                    "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)"
                )
            )
    finally:
        eng.dispose()

    # Simulate kill after first 3 rows committed.
    deng = create_engine(f"sqlite:///{dst}")
    try:
        with deng.begin() as c:
            c.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"))
            c.execute(text("INSERT INTO t VALUES (1,10),(2,20),(3,30)"))
    finally:
        deng.dispose()

    job_id = "p2resume" + uuid.uuid4().hex[:16]
    checkpoint = Checkpoint(
        job_id=job_id,
        chunk_index=1,
        offset=3,
        rows_processed=3,
        cursor_column="id",
        cursor_value=3,
    )
    fake.update_job_status(
        job_id,
        "running",
        checkpoint=checkpoint.to_dict(),
        transfer_request={},
    )

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[
            {
                "name": "t",
                "primary_key": "id",
                "sync_mode": "full_refresh_overwrite",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="warn",
        mappings=None,
    )
    result = UniversalTransferEngine().execute_tracked(req, job_id, resume=True)
    _assert_transfer_ok(result, 6)

    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute("SELECT id, v FROM t ORDER BY id").fetchall()
        distinct = conn.execute("SELECT count(DISTINCT id) FROM t").fetchone()[0]
    finally:
        conn.close()
    assert len(rows) == 6
    assert distinct == 6
    assert rows == [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]


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
        _assert_transfer_ok(result, 2)
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
@pytest.mark.parametrize("with_maps", [False, True])
@pytest.mark.parametrize("skip_preflight", [False, True])
def test_golden_csv_to_pg_no_config(tmp_path: Path, with_maps: bool, skip_preflight: bool):
    import psycopg2

    creds = _pg_creds()
    csv_path = tmp_path / "p2.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "nm"])
        w.writerow(["1", "alpha"])
        w.writerow(["2", "beta"])
    dst_table = f"p2_csv_{uuid.uuid4().hex[:8]}"
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
                "source": "nm",
                "target": "nm",
                "target_type": "TEXT",
                "approved": True,
                "confidence": 0.99,
            },
        ]
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
        skip_preflight=skip_preflight,
        mappings=maps,
    )
    try:
        result = _run(req)
        _assert_transfer_ok(result, 2)
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
        _assert_transfer_ok(result, 2)
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
        recon = result.reconciliation or {}
        assert recon.get("passed") is True, recon
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
        # File export may skip checksum readback; still must not fail recon.
        recon = result.reconciliation or {}
        assert recon.get("passed") is True or recon.get("skipped_readback") is True, recon
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


@pytest.mark.skipif(
    not (_pg_up() and _mysql_up()),
    reason="PostgreSQL or MySQL not reachable (CI provides both via services)",
)
@pytest.mark.parametrize("with_maps", [False, True])
@pytest.mark.parametrize("skip_preflight", [False, True])
def test_golden_pg_to_mysql_no_config(with_maps: bool, skip_preflight: bool):
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    src_table = f"p2_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"p2_dst_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["username"],
        password=pg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                "(id bigint PRIMARY KEY, big_val bigint, nm text)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" VALUES '
                "(1, 9223372036854775807, 'a'), (2, -1, 'b')"
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
                ("nm", "TEXT"),
            )
        ]

    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database",
                format="postgresql",
                host=pg["host"],
                port=pg["port"],
                database=pg["database"],
                username=pg["username"],
                password=pg["password"],
                schema="public",
                table=src_table,
            ),
            destination=EndpointConfig(
                kind="database",
                format="mysql",
                host=my["host"],
                port=my["port"],
                database=my["database"],
                username=my["username"],
                password=my["password"],
                table=dst_table,
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=skip_preflight,
            mappings=maps,
        )
        result = _run(req)
        _assert_transfer_ok(result, 2)
    finally:
        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            dbname=pg["database"],
            user=pg["username"],
            password=pg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            conn.commit()
        finally:
            conn.close()
        mconn = pymysql.connect(
            host=my["host"],
            port=my["port"],
            database=my["database"],
            user=my["username"],
            password=my["password"],
            autocommit=True,
        )
        try:
            with mconn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            mconn.close()
