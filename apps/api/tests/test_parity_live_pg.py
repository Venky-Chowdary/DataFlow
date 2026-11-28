"""Parity Studio on a live PostgreSQL: a scale cut is held back, never rounded.

The destination already exists with ``amount NUMERIC(12,2)`` while the source
holds ``NUMERIC(12,4)``. Until edc9573b4 the PostgreSQL merge COPY let the
server round every value on insert (1.1249 landed as 1.12) and parity then
found and repaired the cut. Now the merge declines a narrower live column and
the row path quarantines a value that would round. Half the source values fit
12,2 exactly (1.1200), half do not (2.1249). Expected, end to end: the six that
fit land exactly; the six that would round are held in quarantine and none is
written rounded; the job is completed_with_quarantine, not migration-proven;
Parity at Scale names the held keys as quarantined, never says proven, and the
source rows balance as landed + held; the rows that landed are compared by the
parity judge and 1.1200 against 1.12 is no difference.

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


def test_scale_cut_holds_the_rows_that_would_round_and_parity_names_them(pg):
    from services.parity_report import build_parity_report
    from services.signed_proof_pack import classify_post_write_assurance, harvest_reconciliation_for_proof
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    conn, cfg, store, (src, dst) = pg
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id int PRIMARY KEY, amount numeric(12,4), note text)")
        # Odd ids fit NUMERIC(12,2) exactly (g.1200); even ids would round (g.1249).
        cur.execute(
            f"INSERT INTO {src} SELECT g, CASE WHEN g % 2 = 1 THEN g + 0.12 ELSE g + 0.1249 END, "
            "'n' || g FROM generate_series(1, 12) g"
        )
        cur.execute(f"CREATE TABLE {dst} (id int PRIMARY KEY, amount numeric(12,2), note text)")
        cur.execute(f"INSERT INTO {dst} VALUES (1000, 1.5, 'kept')")

    def endpoint(table: str) -> EndpointConfig:
        return EndpointConfig(kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                              database=cfg["database"], username=cfg["username"],
                              password=cfg["password"], table=table, schema="public")

    req = TransferRequest(
        source=endpoint(src), destination=endpoint(dst),
        mappings=[{"source": c, "target": c, "user_override": True} for c in ("id", "amount", "note")],
        stream_contracts=[{"name": src, "primary_key": ["id"], "sync_mode": "upsert", "selected": True}],
        sync_mode="upsert", skip_preflight=True, validation_mode="lenient",
    )
    job_id = "pgparity" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request=transfer_request_to_dict(req))
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    job = store.get_job(job_id)

    fits = {i for i in range(1, 13) if i % 2 == 1}
    rounds = {i for i in range(1, 13) if i % 2 == 0}
    # Only the values that fit landed, exactly; nothing was written rounded.
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, amount FROM {dst} ORDER BY id")
        landed = dict(cur.fetchall())
        cur.execute(f"SELECT id, amount FROM {src}")
        source = dict(cur.fetchall())
    assert set(landed) == fits | {1000}, landed
    assert all(landed[i] == source[i] for i in fits), landed
    assert not rounds & set(landed)

    # The job is not green for the held rows.
    assert result.records_transferred == len(fits)
    assert job["status"] == "completed_with_quarantine", job.get("status")
    assert int(job.get("rejected_rows") or 0) == len(rounds)
    assurance = classify_post_write_assurance(
        harvest_reconciliation_for_proof(job, job.get("destination_summary") or {})
    )
    assert assurance.get("migration_proven") is False, assurance

    report = build_parity_report(job, job_id=job_id)
    # Never greener than explained: six source rows are not at the destination.
    assert report["verdict"] in {"explained", "no_difference_found"}, report["verdict_reason"]
    held = next(d for d in report["differences"] if d["cause"]["code"] == "quarantined")
    assert held["kind"] == "missing_in_destination" and held["cause"]["explained"] is True
    assert held["rows"] == len(rounds) and not held["rows_at_least"]
    assert sorted(int(k) for k in held["keys_sample"]) == sorted(rounds)
    kept = next(d for d in report["differences"] if d["cause"]["code"] == "out_of_scope_population")
    assert kept["rows"] == 1
    # Read = landed + held, and the destination = landed + the row it already held.
    parity = report["parity"]
    assert parity["rows_source"] == len(fits) + held["rows"] == 12
    assert parity["rows_destination"] == len(fits) + kept["rows"]
    assert parity["unexplained"] == 0
    # The scale cut on the rows that DID land: the judge compared them, and 1.1200
    # (NUMERIC(12,4)) against 1.12 (NUMERIC(12,2)) is the same value, not a difference.
    (table,) = report["tables"]
    assert table["rows_checked"] == len(fits) and table["unexplained"] == 0, table
    assert not any(d["cause"]["code"] == "numeric_scale" for d in report["differences"])
    assert not any(f["action"] in {"widen_column", "resync_keys"} for f in report["fixes"]), report["fixes"]
