"""QA round 11 (build 9c75ada4) M10: ``*_df_staging`` tables left behind.

QA: write_via_staging loaded 1000 rows correctly, and every run left its
``<table>_df_staging`` table in the destination schema. The stage was only
ever dropped by the *next* run (or by discarding it), so a clean run's
scratch table sat in the customer's schema forever.

Contract now:

* a clean promote drops the stage: no staging table remains after the run;
* a blocked promote (strict, a row failed validation) or a promote that held
  rows back (balanced) keeps the stage so the operator can inspect it, and the
  summary says so;
* a retained stage older than the TTL is reaped by the next staging run in
  that schema.

Route: the Studio's ``POST /api/v1/transfer/run`` with ``write_via_staging``
(MCP ``start_transfer`` has no staging argument), PG -> PG, with NULL, '',
whitespace and unicode cells, and a second run into the table the first run
created.
"""

from __future__ import annotations

import time
import uuid

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r

pytestmark = pytest.mark.timeout(600)

ROWS = [(1, "a"), (2, None), (3, ""), (4, "   "), (5, "Łukasz 山田 ✓")]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _staged_run(client, source: str, src: str, dst: str, *, mode: str = "balanced") -> dict:
    cfg = r.pg_cfg()
    resp = client.post("/api/v1/transfer/run", data={
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": source, "source_table": src,
        "dest_kind": "database", "dest_format": "postgresql", "dest_host": cfg["host"],
        "dest_port": str(cfg["port"]), "dest_database": cfg["database"],
        "dest_username": cfg["username"], "dest_password": cfg["password"],
        "dest_schema": "public", "dest_table": dst, "async_mode": "false",
        "sync_mode": "full_refresh_overwrite", "validation_mode": mode,
        "write_via_staging": "true",
    })
    body = resp.json()
    if resp.status_code != 200:
        return {"status": "failed", "http": resp.status_code, **(body.get("detail") or {})}
    job_id = body.get("job_id") or (body.get("job") or {}).get("id")
    if body.get("status") not in {"completed", "failed", "partial"} and job_id:
        body = h.wait_job(str(job_id), timeout_s=300)
    return body


def _stage_tables(dst: str) -> list[str]:
    return [str(t) for (t,) in r.pg(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
        "AND table_name LIKE %s", (f"{dst}%staging",),
    )]


def test_clean_staged_run_leaves_no_staging_table(client):
    token = uuid.uuid4().hex[:8]
    src, dst = f"r11st_s_{token}", f"r11st_d_{token}"
    r.pg(f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, note text)')
    for row in ROWS:
        r.pg(f'INSERT INTO public."{src}" VALUES (%s, %s)', row)
    source = r.connector("source", r.pg_cfg())
    try:
        for run in (1, 2):
            job = _staged_run(client, source, src, dst)
            assert str(job.get("status") or ("completed" if job.get("success") else "")) == "completed", (run, job)
            assert r.pg(f'SELECT id, note FROM public."{dst}" ORDER BY id') == ROWS
            assert _stage_tables(dst) == [], (run, _stage_tables(dst))
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
        for name in _stage_tables(dst):
            h.drop_table("postgresql", name)


def _pg_endpoint(table: str):
    from src.transfer.models import EndpointConfig

    cfg = r.pg_cfg()
    return EndpointConfig(kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                          database=cfg["database"], username=cfg["username"],
                          password=cfg["password"], schema="public", table=table)


def test_blocked_promote_keeps_the_stage_and_ttl_reaps_it(client, monkeypatch):
    """A blocked promote keeps its stage (and says so); the TTL reaper drops it later.

    Validate refuses every bad cell it can see, so a blocked promote is the
    write-time guard for a value no preflight saw. The route cannot reach it
    while preflight is thorough, so this drives the staging writer itself
    against the real PostgreSQL, strict, with a cell the integer column cannot
    hold.
    """
    from services.pre_ingestion_staging import write_via_pre_ingestion_staging

    token = uuid.uuid4().hex[:8]
    dst = f"r11sb_d_{token}"
    r.pg(f'CREATE TABLE public."{dst}" (id integer PRIMARY KEY, qty integer, note text)')
    records = [
        {"id": 1, "qty": "7", "note": "Zoë"}, {"id": 2, "qty": None, "note": None},
        {"id": 3, "qty": "not a number", "note": ""}, {"id": 4, "qty": "0", "note": "   "},
        {"id": 5, "qty": "8", "note": "日本語"},
    ]
    mappings = [{"source": c, "target": c, "target_type": t}
                for c, t in (("id", "INTEGER"), ("qty", "INTEGER"), ("note", "TEXT"))]
    try:
        written, _ddl, summary = write_via_pre_ingestion_staging(
            _pg_endpoint(dst), records, ["id", "qty", "note"],
            {"id": "INTEGER", "qty": "VARCHAR", "note": "TEXT"}, mappings,
            validation_mode="strict", job_id=f"r11sb{token}",
        )
        assert written == 0 and summary.get("promote_blocked") is True, summary
        stages = _stage_tables(dst)
        assert len(stages) == 1, stages
        meta = summary.get("pre_ingestion_staging") or {}
        assert meta.get("retained") is True and stages[0] in str(meta.get("retained_reason")), meta
        assert r.pg(f'SELECT count(*) FROM public."{dst}"') == [(0,)]

        # Balanced: the clean rows promote, the held-back row keeps the stage.
        written, _ddl, summary = write_via_pre_ingestion_staging(
            _pg_endpoint(dst), records, ["id", "qty", "note"],
            {"id": "INTEGER", "qty": "VARCHAR", "note": "TEXT"}, mappings,
            validation_mode="balanced", job_id=f"r11sb{token}b",
        )
        assert written == 4, summary
        assert (summary.get("pre_ingestion_staging") or {}).get("retained") is True, summary
        assert len(_stage_tables(dst)) == 1
        assert r.pg(f'SELECT id, qty, note FROM public."{dst}" ORDER BY id') == [
            (1, 7, "Zoë"), (2, None, None), (4, 0, "   "), (5, 8, "日本語"),
        ]

        # A retained stage past its TTL is reaped by the next staging run in the
        # schema, which itself runs clean and leaves no stage behind.
        from services import staging_retention

        monkeypatch.setattr(staging_retention, "STAGING_RETENTION_TTL_SECONDS", 0)
        time.sleep(1.1)
        token2 = uuid.uuid4().hex[:8]
        other_src, other_dst = f"r11sc_s_{token2}", f"r11sc_d_{token2}"
        r.pg(f'CREATE TABLE public."{other_src}" (id integer PRIMARY KEY, note text)')
        for row in ROWS:
            r.pg(f'INSERT INTO public."{other_src}" VALUES (%s, %s)', row)
        try:
            job2 = _staged_run(client, r.connector("source", r.pg_cfg()), other_src, other_dst)
            assert str(job2.get("status") or ("completed" if job2.get("success") else "")) == "completed", job2
            assert _stage_tables(dst) == [], "the expired retained stage was not reaped"
            assert _stage_tables(other_dst) == []
            assert r.pg(f'SELECT id, note FROM public."{other_dst}" ORDER BY id') == ROWS
        finally:
            h.drop_table("postgresql", other_src)
            h.drop_table("postgresql", other_dst)
    finally:
        h.drop_table("postgresql", dst)
        for name in _stage_tables(dst):
            h.drop_table("postgresql", name)
