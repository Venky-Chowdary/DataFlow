"""Parity Studio on a live PostgreSQL: a scale cut, widened in place, then re-synced.

The destination already exists with ``amount NUMERIC(12,2)`` while the source
holds ``NUMERIC(12,4)``; PostgreSQL rounds every value on insert. The run uses
``lenient`` validation so the failed batch is kept for inspection (``strict``
restores the table), which is what lets parity see the rows. Expected: 12 rows
with an unexplained numeric scale cut, a ``widen_column`` fix through the
existing widen DDL helper (``NUMERIC(14,4)``: the source's scale without losing
the destination's integer digits), then ``resync_keys`` through the upsert
path, ending at zero unexplained differences.

Skipped with a message when no PostgreSQL is reachable.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

psycopg2 = pytest.importorskip("psycopg2")

from tests.test_parity_e2e_engine import _Store, run_inline  # noqa: E402


def _pg_cfg() -> dict:
    from tests.conftest import pg_test_cfg

    if not os.environ.get("DATAFLOW_PG_TEST_DSN") and os.environ.get("PGHOST"):
        return {
            "host": os.environ["PGHOST"], "port": int(os.environ.get("PGPORT") or 5432),
            "database": os.environ.get("PGDATABASE") or "postgres",
            "username": os.environ.get("PGUSER") or "postgres",
            "password": os.environ.get("PGPASSWORD") or "",
        }
    return pg_test_cfg()


@pytest.fixture()
def pg(monkeypatch, tmp_path):
    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable for the live parity test: {type(exc).__name__}")
    conn.autocommit = True
    import services.mongodb_service as mongodb_service
    import src.transfer.engine as engine_mod
    from services import audit_log

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    from services import parity_apply

    monkeypatch.setattr(parity_apply, "_launch", run_inline)  # the re-sync job, on this thread
    suffix = uuid.uuid4().hex[:10]
    tables = (f"parity_src_{suffix}", f"parity_dst_{suffix}")
    try:
        yield conn, cfg, store, tables
    finally:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608 - generated test name
        conn.close()


def test_scale_cut_is_widened_and_resynced_on_postgres(pg):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    conn, cfg, store, (src, dst) = pg
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id int PRIMARY KEY, amount numeric(12,4), note text)")
        cur.execute(f"INSERT INTO {src} SELECT g, g + 0.1249, 'n' || g FROM generate_series(1, 12) g")
        cur.execute(f"CREATE TABLE {dst} (id int PRIMARY KEY, amount numeric(12,2), note text)")
        cur.execute(f"INSERT INTO {dst} VALUES (1000, 1.5, 'kept')")

    def endpoint(table: str) -> EndpointConfig:
        return EndpointConfig(kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                              database=cfg["database"], username=cfg["username"],
                              password=cfg["password"], table=table, schema="public")

    req = TransferRequest(
        source=endpoint(src), destination=endpoint(dst),
        # Approved mappings: the re-sync job keeps preflight (as the re-run route does).
        mappings=[{"source": c, "target": c, "user_override": True} for c in ("id", "amount", "note")],
        stream_contracts=[{"name": src, "primary_key": ["id"], "sync_mode": "upsert", "selected": True}],
        sync_mode="upsert", skip_preflight=True, validation_mode="lenient",
    )
    job_id = "pgparity" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request=transfer_request_to_dict(req))
    UniversalTransferEngine().execute_tracked(req, job_id)

    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    assert report["verdict"] == "unexplained", report["verdict_reason"]
    scale = next(d for d in report["differences"] if d["cause"]["code"] == "numeric_scale")
    assert scale["column"] == "amount" and scale["rows"] == 12
    assert scale["example"] == {"source": "1.1249", "destination": "1.12"}
    kept = next(d for d in report["differences"] if d["kind"] == "extra_in_destination")
    assert kept["cause"]["code"] == "out_of_scope_population" and kept["rows"] == 1
    assert report["parity"]["unexplained"] == 12
    widen = next(f for f in report["fixes"] if f["action"] == "widen_column")
    assert widen["applicable"], widen["not_applicable_reason"]
    assert widen["preview"].endswith('ALTER COLUMN "amount" TYPE NUMERIC(14,4)')

    widened = approve_fix(job_id, widen["id"], actor="dba@example.com", note="match the source scale")
    assert widened["rounds"][-1]["unexplained_before"] == 12
    with conn.cursor() as cur:
        cur.execute(
            "SELECT numeric_precision, numeric_scale FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = 'amount'", (dst,),
        )
        assert cur.fetchone() == (14, 4)

    resync = next(f for f in widened["fixes"] if f["action"] == "resync_keys" and f["status"] == "proposed")
    final = approve_fix(job_id, resync["id"], actor="dba@example.com")
    assert final["parity"]["unexplained"] == 0
    assert final["verdict"] == "explained"  # the row that was already there stays out of scope
    assert [r["unexplained_after"] for r in final["rounds"]] == [12, 0]
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {dst} d JOIN {src} s USING (id) WHERE d.amount = s.amount")
        assert cur.fetchone()[0] == 12
