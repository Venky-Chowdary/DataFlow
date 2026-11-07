"""B2 dedupe PG-live regressions — the recipe dedupe step through
UniversalTransferEngine with Gate-8 winner-uniqueness and ledger-balance
checks on a real PostgreSQL destination.

Gated on DATAFLOW_PG_TEST_DSN.
"""
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.environ.get("DATAFLOW_PG_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DATAFLOW_PG_TEST_DSN unset")


def _parse_dsn():
    from urllib.parse import urlparse

    u = urlparse(DSN)
    return {
        "host": u.hostname,
        "port": u.port or 5432,
        "database": (u.path or "/postgres").lstrip("/"),
        "username": u.username or "postgres",
        "password": u.password or "",
    }


def _pg():
    p = _parse_dsn()
    conn = psycopg2.connect(
        host=p["host"], port=p["port"], dbname=p["database"],
        user=p["username"], password=p["password"],
    )
    conn.autocommit = True
    return conn


def _engine_env(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setenv(
        "DATAFLOW_EGRESS_ALLOWLIST",
        "127.0.0.0/8,::1/128,localhost",
    )
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


COLS = ["id", "v", "ts", "email"]

ROWS1 = [
    {"id": "1", "v": "old", "ts": "1", "email": "a@x.com"},
    {"id": "2", "v": "b", "ts": "2", "email": "b@x.com"},
    {"id": "1", "v": "new", "ts": "3", "email": "a2@x.com"},
    {"id": "3", "v": "c", "ts": "1", "email": "c@x.com"},
]


def _dedupe_recipe(**over):
    opts = {"keys": ["id"], "keep": "last", "order_by": "ts"}
    opts.update(over)
    return {"steps": [{"op": "dedupe_rows", "options": opts}]}


def _req(src_table, dst_table, recipe=None, mode="strict"):
    from src.transfer.models import EndpointConfig, TransferRequest
    from services.shape_models import ShapeRecipe

    p = _parse_dsn()
    ep = lambda tbl: EndpointConfig(  # noqa: E731
        kind="database",
        format="postgresql",
        host=p["host"], port=p["port"], database=p["database"],
        username=p["username"], password=p["password"], table=tbl,
    )
    recipe_hash = (
        ShapeRecipe.parse(recipe, source_columns=COLS).recipe_hash
        if recipe else ""
    )
    return TransferRequest(
        source=ep(src_table),
        destination=ep(dst_table),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {"source": c, "target": c, "confidence": 1.0,
             "user_override": True, "transform": "none"}
            for c in COLS
        ],
        shape_recipe=recipe or {},
        approved_shape_recipe_hash=recipe_hash,
        skip_preflight=True,
        validation_mode=mode,
    )


def _exec(fake, req, prefix="ddb2"):
    from src.transfer.engine import UniversalTransferEngine

    job_id = prefix + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _mk_src(cur, table, rows):
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.execute(
        f"CREATE TABLE {table} (id TEXT, v TEXT, ts TEXT, email TEXT)"
    )
    for r in rows:
        cur.execute(
            f"INSERT INTO {table} VALUES (%s,%s,%s,%s)",
            (r["id"], r["v"], r["ts"], r["email"]),
        )


def _dst(cur, table):
    cur.execute(f"SELECT id, v, ts, email FROM {table} ORDER BY id")
    return cur.fetchall()


def test_dedupe_recipe_runs_winners_land_losers_quarantined_pg(
    tmp_path, monkeypatch
):
    """A dedupe recipe through the real engine: the winner set lands unique,
    losers are in the quarantine ledger with reason, and Gate-8 reports the
    balance + uniqueness checks passed."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    src, dst = "ddb2_src", "ddb2_dst"
    cur.execute(f"DROP TABLE IF EXISTS {dst} CASCADE")
    _mk_src(cur, src, ROWS1)
    recipe = _dedupe_recipe()

    res = _exec(fake, _req(src, dst, recipe, mode="strict"))
    assert res.success, res.error
    rows = _dst(cur, dst)
    # Winners: id=1's latest (ts=3), plus 2 and 3 — unique keys.
    assert sorted(r[0] for r in rows) == ["1", "2", "3"]
    assert next(r for r in rows if r[0] == "1")[1] == "new"

    # The loser is in the quarantine ledger with its reason named — under
    # the test's in-memory job store the DLQ events land in the JSONL store.
    from services.quarantine_dlq import quarantine_details_from_dlq

    details = quarantine_details_from_dlq(res.job_id) or []
    assert any(
        "dedupe loser" in (d.get("reason") or d.get("failure_reason") or "")
        and "order_by=ts" in (d.get("reason") or "")
        for d in details
    ), details

    # Gate-8 ran the dedupe checks: ledger balances and the winner set is
    # unique on the destination.
    ded = (res.reconciliation or {}).get("dedupe_checks") or []
    checks = {c["check"]: c["status"] for c in ded}
    assert checks.get("ledger_balance") == "passed", ded
    assert checks.get("winner_uniqueness") == "passed", ded
    conn.close()


def test_dedupe_winner_uniqueness_tamper_fails_gate8_pg(tmp_path, monkeypatch):
    """A second row for a dedupe key landed post-write is caught by Gate-8 —
    verdict failed and the job fails under strict."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    src, dst = "ddb2_src", "ddb2_dst"
    cur.execute(f"DROP TABLE IF EXISTS {dst} CASCADE")
    _mk_src(cur, src, ROWS1)
    recipe = _dedupe_recipe()

    res = _exec(fake, _req(src, dst, recipe, mode="strict"))
    assert res.success, res.error

    # Tamper: a duplicate winner key on the destination between write and
    # read-back — the next run's Gate-8 must see it fail. Since overwrite
    # replaces the table each run, tamper via a check patch: run the recipe
    # again but corrupt the dest inside the read-back window.
    import src.transfer.reconcile_step as recon_mod

    real = recon_mod._run_dedupe_gate_checks

    def _tamper_then_check(report, ctx, ds, sync):
        cur.execute(
            f"INSERT INTO {dst} (id, v, ts, email) "
            "VALUES ('2','rogue','9','x@x.com')"
        )
        try:
            return real(report, ctx, ds, sync)
        finally:
            cur.execute(f"DELETE FROM {dst} WHERE v='rogue'")

    monkeypatch.setattr(recon_mod, "_run_dedupe_gate_checks", _tamper_then_check)
    res = _exec(fake, _req(src, dst, recipe, mode="strict"))
    assert not res.success, "a duplicated winner key must fail strict Gate-8"
    ded = (res.reconciliation or {}).get("dedupe_checks") or []
    assert any(
        c["check"] == "winner_uniqueness" and c["status"] == "failed"
        for c in ded
    ), ded
    assert (res.reconciliation or {}).get("verification_verdict") == "failed"
    conn.close()


def test_dedupe_last_seen_and_merge_run_pg(tmp_path, monkeypatch):
    """Last-seen and non-null merge land their merged winners through the
    engine on real PG."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    src, dst = "ddb2_src", "ddb2_dst"
    cur.execute(f"DROP TABLE IF EXISTS {dst} CASCADE")
    _mk_src(cur, src, [
        {"id": "1", "v": "old", "ts": "1", "email": "keep@x.com"},
        {"id": "1", "v": "new", "ts": "3", "email": None},
        {"id": "1", "v": "mid", "ts": "2", "email": "mid@x.com"},
    ])
    recipe = _dedupe_recipe(merge="non_null")

    res = _exec(fake, _req(src, dst, recipe, mode="strict"))
    assert res.success, res.error
    rows = _dst(cur, dst)
    assert len(rows) == 1
    # Winner ts=3 keeps v=new; null email fills from the best-ranked member
    # (ts=2 -> mid@x.com, not ts=1).
    assert rows[0][1] == "new"
    assert rows[0][3] == "mid@x.com"
    conn.close()
