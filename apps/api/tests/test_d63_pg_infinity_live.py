"""D63 — PostgreSQL ``infinity`` / ``-infinity`` must never become 9999-12-31.

psycopg2 decodes those dates and timestamps as ``date.max`` /
``datetime.max`` / ``datetime.min``, so every PostgreSQL read handed the
pipeline a finite 9999-12-31 / 0001-01-01. PG->PG landed the wrong value and
verification — reading back through the same decode — called it verified.
The reads now keep the literal; the PG family carries it, and a destination
without the value refuses it by name.

Live PostgreSQL (and MySQL for the refusal leg).
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")
MY = dict(host="127.0.0.1", port=3306, database="dataflow", username="dataflow", password="dataflow")


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(5432), reason="PostgreSQL not reachable")


def _pg(sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def source():
    name = f"d63_src_{uuid.uuid4().hex[:8]}"
    _pg(
        f'CREATE TABLE public."{name}" (id BIGINT PRIMARY KEY, d DATE, ts TIMESTAMP, tz TIMESTAMPTZ);'
        f"INSERT INTO public.\"{name}\" VALUES "
        "(1, 'infinity', 'infinity', '-infinity'), "
        "(2, '-infinity', '-infinity', 'infinity'), "
        "(3, '2024-01-01', '2024-01-01 10:00', '2024-01-01 10:00+00')"
    )
    yield name
    _pg(f'DROP TABLE IF EXISTS public."{name}"')


def _run(fake, src: str, destination: EndpointConfig):
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=src, **PG),
        destination=destination,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = "d63" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("copy_path", ["1", "0"], ids=["copy", "row_path"])
def test_pg_to_pg_keeps_infinity(isolated, source, monkeypatch, copy_path):
    monkeypatch.setenv("DATAFLOW_PG_PG_COPY", copy_path)
    dst = f"d63_dst_{uuid.uuid4().hex[:8]}"
    try:
        result = _run(
            isolated,
            source,
            EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG),
        )
        assert result.success, result.error
        assert _pg(f'SELECT id, d::text, ts::text, tz::text FROM public."{dst}" ORDER BY id')[:2] == [
            (1, "infinity", "infinity", "-infinity"),
            (2, "-infinity", "-infinity", "infinity"),
        ]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


@pytest.mark.skipif(not _up(3306), reason="MySQL not reachable")
def test_pg_to_mysql_refuses_infinity_by_name(isolated, source):
    import pymysql

    # Only +infinity: 9999-12-31 fits MySQL, so the clamp landed silently.
    _pg(f'DELETE FROM public."{source}" WHERE id = 2')
    _pg(f"UPDATE public.\"{source}\" SET tz = '2024-01-01 00:00+00' WHERE id = 1")

    dst = f"d63_my_{uuid.uuid4().hex[:8]}"
    try:
        result = _run(
            isolated,
            source,
            EndpointConfig(kind="database", format="mysql", table=dst, **MY),
        )
        assert not result.success
        assert "cannot fit the destination carrier" in str(result.error or ""), result.error
        # The gate names the value and why (preflight detail on the result).
        assert "PostgreSQL 'infinity' has no DATE carrier on mysql" in repr(result)
    finally:
        conn = pymysql.connect(
            host=MY["host"], port=MY["port"], user=MY["username"],
            password=MY["password"], database=MY["database"], autocommit=True,
        )
        with conn.cursor() as cur:
            rows = []
            try:
                cur.execute(f"SELECT CAST(d AS CHAR) FROM `{dst}`")
                rows = [r[0] for r in cur.fetchall()]
            except pymysql.err.ProgrammingError:
                pass
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        conn.close()
        # Never a silently clamped calendar date.
        assert "9999-12-31" not in rows and "0001-01-01" not in rows
