"""SFTP glob pickup into Postgres, with the manifest in the load transaction."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from services.file_manifest import claim_file, list_loaded
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sftp_test_server import start_sftp_server

_A = b"id,amount\n1,10\n"
_B = b"id,amount\n2,20\n"
_A2 = b"id,amount\n1,99\n"


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.fail(f"PostgreSQL is required for the file-manifest proof: {exc}")
    return conn


def test_postgres_claim_rolls_back_with_the_inserted_row():
    conn = _pg()
    table = "mf_" + uuid.uuid4().hex[:10]
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}" (id INTEGER PRIMARY KEY, amount TEXT)')
    conn.autocommit = False
    try:
        cur = conn.cursor()
        assert claim_file(
            conn,
            dialect="postgresql",
            schema="public",
            connector="sftp-proof",
            path="orders_a.csv",
            size=len(_A),
            mtime="1",
            sha256="ab" * 32,
            job_id="job-crash",
        ) == "load"
        cur.execute(f'INSERT INTO public."{table}" (id, amount) VALUES (1, %s)', ("10",))
        conn.rollback()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
        assert cur.fetchone()[0] == 0
        assert list_loaded(conn, dialect="postgresql", schema="public", connector="sftp-proof") == []
    finally:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


def test_sftp_glob_loads_in_order_and_a_rerun_does_not_double():
    root = Path("/tmp") / f"sftp-manifest-{uuid.uuid4().hex[:8]}"
    root.mkdir()
    (root / "orders_b.csv").write_bytes(_B)
    (root / "orders_a.csv").write_bytes(_A)
    server, runner = start_sftp_server(str(root))
    table = "ord_" + uuid.uuid4().hex[:10]
    pg = _pg()
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS public.dataflow_file_manifest ("
            "connector TEXT NOT NULL, path TEXT NOT NULL, size BIGINT NOT NULL, "
            "mtime TEXT NOT NULL, sha256 TEXT NOT NULL, status TEXT NOT NULL, "
            "job_id TEXT NOT NULL, rows BIGINT NOT NULL DEFAULT 0, "
            "PRIMARY KEY (connector, path, sha256))"
        )
        cur.execute(
            "DELETE FROM public.dataflow_file_manifest WHERE connector LIKE %s",
            ("sftp:/%",),
        )
    try:
        cfg = server.endpoint_config("/orders_*.csv")
        source = EndpointConfig(
            kind="database",
            format="sftp",
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"],
            database="/",
            table="orders_*.csv",
            extra={"host_key": cfg["host_key"], "duplicate_policy": "upsert"},
        )
        dest = EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table,
        )
        request = TransferRequest(
            source=source,
            destination=dest,
            mappings=[
                {"source": "id", "target": "id", "confidence": 1},
                {"source": "amount", "target": "amount", "confidence": 1},
            ],
            stream_contracts=[{"primary_key": "id", "sync_mode": "full_refresh_append"}],
            sync_mode="full_refresh_append",
            skip_preflight=True,
            validation_mode="strict",
        )
        first = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert first.success is True, first.error
        assert first.records_transferred == 2
        picked = list((first.destination_summary or {}).get("files_picked") or [])
        assert [row["name"] for row in picked] == ["orders_a.csv", "orders_b.csv"]

        with pg.cursor() as cur:
            cur.execute(f'SELECT id, amount FROM public."{table}" ORDER BY id')
            assert cur.fetchall() == [(1, 10), (2, 20)]
            cur.execute(
                "SELECT path, status, rows FROM public.dataflow_file_manifest "
                "WHERE connector=%s ORDER BY path",
                ("sftp:/:orders_*.csv",),
            )
            manifest = cur.fetchall()
        assert manifest == [("orders_a.csv", "loaded", 1), ("orders_b.csv", "loaded", 1)]

        second = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert second.success is True, second.error
        assert int(second.records_transferred or 0) == 0
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            assert cur.fetchone()[0] == 2

        (root / "orders_a.csv").write_bytes(_A2)
        third = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert third.success is True, third.error
        assert int(third.records_transferred or 0) == 1
        with pg.cursor() as cur:
            cur.execute(f'SELECT amount FROM public."{table}" WHERE id=1')
            assert cur.fetchone()[0] == 99
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            assert cur.fetchone()[0] == 2

        missing = EndpointConfig(
            kind="database",
            format="sftp",
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"],
            database="/",
            table="missing_*.csv",
            extra={"host_key": cfg["host_key"], "duplicate_policy": "upsert", "file_wait_seconds": 0},
        )
        missed = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=missing,
                destination=dest,
                mappings=request.mappings,
                stream_contracts=request.stream_contracts,
                sync_mode="full_refresh_append",
                skip_preflight=True,
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert missed.success is False
        assert "expected file not arrived" in (missed.error or "").lower()
        assert "file_wait_seconds" in (missed.error or "")
    finally:
        runner.stop()
        try:
            pg.autocommit = True
            with pg.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
                cur.execute(
                    "DELETE FROM public.dataflow_file_manifest WHERE connector LIKE %s",
                    ("sftp:/%",),
                )
        except Exception:
            pass
        pg.close()
