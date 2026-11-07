"""DD-1/DD-2/DD-3 regressions — typed dedupe ordering, merge fill direction,
and honest ledger accounting, proven through execute_tracked on live PG.

DD-1: the wire carries ordering values as text; a bare string compare keeps
'9' over '10' on an INT column and '9.50' over '10.00' on NUMERIC — and the
job still reported verified. Ordering must compare under the source column's
declared type, and Gate-8 must re-derive the winner's ordering tuple per key
from an independent source re-read (not just uniqueness).

DD-2: merge=non_null filled from the worst-ranked member whenever the rule
wasn't descending.

DD-3: buffered keep=last counted every winner twice — committed_offset ate
the drained tail, and still-held rows were booked as recipe removals, so a
500-row source read as 550 with "50 removed by the approved transform
recipe" while zero rows were removed.
"""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

import psycopg2
import pytest

DSN = os.environ.get(
    "DATAFLOW_PG_TEST_DSN", "postgresql://dataflow:dataflow@127.0.0.1:5432/dataflow"
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATAFLOW_PG_TEST_DSN"), reason="DATAFLOW_PG_TEST_DSN not set"
)


def _parse_dsn():
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
        "DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,localhost"
    )
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _req(src_table, dst_table, recipe, cols, *, col_types=None, mode="strict"):
    from src.transfer.models import EndpointConfig, TransferRequest
    from services.shape_models import ShapeRecipe

    p = _parse_dsn()
    ep = lambda tbl: EndpointConfig(  # noqa: E731
        kind="database",
        format="postgresql",
        host=p["host"], port=p["port"], database=p["database"],
        username=p["username"], password=p["password"], table=tbl,
    )
    recipe_hash = ShapeRecipe.parse(
        recipe, source_columns=cols, source_types=col_types
    ).recipe_hash
    return TransferRequest(
        source=ep(src_table),
        destination=ep(dst_table),
        sync_mode="full_refresh_overwrite",
        mappings=[
            {"source": c, "target": c, "confidence": 1.0,
             "user_override": True, "transform": "none"}
            for c in cols
        ],
        shape_recipe=recipe,
        approved_shape_recipe_hash=recipe_hash,
        skip_preflight=True,
        validation_mode=mode,
    )


def _exec(fake, req):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "dd1" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _mk_typed_src(cur, table, ddl, rows):
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.execute(f"CREATE TABLE {table} ({ddl})")
    ph = ",".join(["%s"] * len(rows[0]))
    for r in rows:
        cur.execute(f"INSERT INTO {table} VALUES ({ph})", r)


def _dedupe_recipe(**over):
    opts = {"keys": ["id"], "keep": "last", "order_by": "ts"}
    opts.update(over)
    return {"steps": [{"op": "dedupe_rows", "options": opts}]}


COLS = ["id", "v", "ts", "email"]


def test_dd1_multi_digit_int_ordering_keeps_numeric_max_pg(
    tmp_path, monkeypatch
):
    """INT ordering: '9' must NOT outrank '10' — the DD-1 wire-text compare."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd1_int_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [
            ("1", "old", 9, "a@x"),
            ("1", "new", 10, "a2@x"),
            ("2", "b", -5, "b@x"),       # negatives: -5 < 7
            ("2", "b2", 7, "b2@x"),
            ("3", "c", -100, "c@x"),     # -100 vs -3: string says -100 < -3
            ("3", "c2", -3, "c2@x"),     # numerically -3 > -100 wins
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd1_int_dst")
    res = _exec(fake, _req("dd1_int_src", "dd1_int_dst", _dedupe_recipe(), COLS))
    assert res.success, res.error
    cur.execute("SELECT id, v, ts, email FROM dd1_int_dst ORDER BY id")
    got = cur.fetchall()
    assert got == [
        (1, "new", 10, "a2@x"),
        (2, "b2", 7, "b2@x"),
        (3, "c2", -3, "c2@x"),
    ]
    # Gate-8 must have proven the winner ordering from a source re-read —
    # uniqueness alone let this class pass as verified.
    checks = (res.reconciliation or {}).get("dedupe_checks") or []
    ordering = [c for c in checks if c.get("check") == "winner_ordering"]
    assert ordering and ordering[0]["status"] == "passed", checks


def test_dd1_numeric_scales_compare_by_value_pg(tmp_path, monkeypatch):
    """NUMERIC(10,2): '9.50' must not outrank '10.00'."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd1_num_src",
        "id INT, v TEXT, ts NUMERIC(10,2), email TEXT",
        [
            ("1", "old", "9.50", "a@x"),
            ("1", "new", "10.00", "a2@x"),
            ("2", "b", "10.0", "b@x"),   # 10.0 > 9.99 though '10.0' < '9.99'
            ("2", "b2", "9.99", "b2@x"),
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd1_num_dst")
    res = _exec(fake, _req("dd1_num_src", "dd1_num_dst", _dedupe_recipe(), COLS))
    assert res.success, res.error
    cur.execute("SELECT id, v, ts::text, email FROM dd1_num_dst ORDER BY id")
    got = cur.fetchall()
    assert got == [(1, "new", "10.00", "a2@x"), (2, "b", "10.00", "b@x")]
    checks = (res.reconciliation or {}).get("dedupe_checks") or []
    ordering = [c for c in checks if c.get("check") == "winner_ordering"]
    assert ordering and ordering[0]["status"] == "passed", checks


def test_dd2_merge_asc_fills_from_best_ranked_pg(tmp_path, monkeypatch):
    """merge=non_null under order_dir=asc fills from the best-ranked member
    (smallest ts), not the worst — DD-2."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd2_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [
            ("1", "w", 0, None),          # winner (asc), email NULL
            ("1", "l1", 1, "second@x"),   # best-ranked fill source
            ("1", "l2", 2, "third@x"),    # worst-ranked — must NOT fill
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd2_dst")
    recipe = _dedupe_recipe(order_dir="asc", merge="non_null")
    res = _exec(fake, _req("dd2_src", "dd2_dst", recipe, COLS))
    assert res.success, res.error
    cur.execute("SELECT id, v, ts, email FROM dd2_dst ORDER BY id")
    got = cur.fetchall()
    assert got == [(1, "w", 0, "second@x")], got


def test_dd3_ledger_counts_source_once_pg(tmp_path, monkeypatch):
    """Buffered keep=last: in = written + quarantined + removed, literally,
    and source_rows is the true independent count — DD-3."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    rows = [
        (k, f"v{v}", v, f"e{v}@x") for k in range(50) for v in range(10)
    ]
    _mk_typed_src(cur, "dd3_src", "id INT, v TEXT, ts INT, email TEXT", rows)
    cur.execute("DROP TABLE IF EXISTS dd3_dst")
    res = _exec(fake, _req("dd3_src", "dd3_dst", _dedupe_recipe(), COLS))
    assert res.success, res.error
    job = fake.jobs[res.job_id]
    ds = job.get("destination_summary") or {}
    assert ds.get("source_row_count") == 500, ds.get("source_row_count")
    # 450 losers quarantined, 50 winners written, ZERO removed — the old
    # ledger invented "50 removed by the approved transform recipe".
    assert int(ds.get("rows_diverted") or 0) == 450
    assert not ds.get("rows_shaped_out"), ds.get("rows_shaped_out")
    assert not ds.get("rows_source_filtered"), ds.get("rows_source_filtered")
    assert int(job.get("rejected_rows") or 0) == 450
    recon = res.reconciliation or {}
    assert int(recon.get("source_rows") or 0) == 500
    cur.execute("SELECT COUNT(*) FROM dd3_dst")
    assert cur.fetchone()[0] == 50


def test_dd1_gate8_catches_winner_tamper_pg(tmp_path, monkeypatch):
    """A destination tamper that swaps in a non-extremal winner must fail
    verification — winner_ordering is the check that catches this class."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd1_tam_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [("1", "old", 9, "a@x"), ("1", "new", 10, "a2@x")],
    )
    cur.execute("DROP TABLE IF EXISTS dd1_tam_dst")
    res = _exec(fake, _req("dd1_tam_src", "dd1_tam_dst", _dedupe_recipe(), COLS))
    assert res.success, res.error
    # Simulate the DD-1 wrong winner landing: replace the kept row with the
    # non-extremal member, then ask the check directly.
    cur.execute("UPDATE dd1_tam_dst SET ts = 9, v = 'old' WHERE id = 1")
    from src.transfer.reconcile_step import (
        _dedupe_dest_winner_tuples,
        _dedupe_source_extrema,
    )
    from services.dest_precount import count_dialect

    p = _parse_dsn()
    cfg = {
        "host": p["host"], "port": p["port"], "database": p["database"],
        "username": p["username"], "password": p["password"], "type": "postgresql",
    }
    ext = _dedupe_source_extrema(
        count_dialect("postgresql"), cfg, schema="public", table="dd1_tam_src",
        keys=["id"], order_cols=[("ts", "desc", "last")],
    )
    assert ext == [(1, 10)]
    dest = _dedupe_dest_winner_tuples(
        count_dialect("postgresql"), cfg, schema="public", table="dd1_tam_dst",
        keys=["id"], order_cols=["ts"],
    )
    assert dest == [(1, 9)]  # tampered tuple differs from the extremal


def test_dd1_volume_vs_distinct_on_pg(tmp_path, monkeypatch):
    """60k rows / 20k keys: winners must equal the DISTINCT ON reference."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    n_keys, per = 20_000, 3
    cur.execute("DROP TABLE IF EXISTS dd1_vol_src CASCADE")
    cur.execute(
        "CREATE TABLE dd1_vol_src (id INT, v TEXT, ts INT, email TEXT)"
    )
    cur.executemany(
        "INSERT INTO dd1_vol_src VALUES (%s,%s,%s,%s)",
        [
            (k, f"v{k}_{m}", (k * 7 + m * 13) % 1000, f"e{k}_{m}@x")
            for k in range(n_keys) for m in range(per)
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd1_vol_dst")
    res = _exec(fake, _req("dd1_vol_src", "dd1_vol_dst", _dedupe_recipe(), COLS))
    assert res.success, res.error
    cur.execute(
        "SELECT v, ts FROM (SELECT DISTINCT ON (id) * FROM dd1_vol_src "
        "ORDER BY id, ts DESC) w"
    )
    expected = sorted(cur.fetchall())
    cur.execute("SELECT v, ts FROM dd1_vol_dst")
    got = sorted(cur.fetchall())
    assert got == expected
    checks = (res.reconciliation or {}).get("dedupe_checks") or []
    ordering = [c for c in checks if c.get("check") == "winner_ordering"]
    assert ordering and ordering[0]["status"] == "passed", checks


def test_dd4_null_ordering_value_never_wins_pg(tmp_path, monkeypatch):
    """DD-4: NULL ts must never outrank a real ts under desc OR asc —
    'keep the latest' cannot let an unknown stamp displace a known one."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd4_null_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [
            ("1", "real", 5, "a@x"),
            ("1", "nullts", None, "a2@x"),
            ("2", "nullfirst", None, "b@x"),
            ("2", "real2", 3, "b2@x"),
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd4_null_dst CASCADE")
    recipe = _dedupe_recipe(keys=["id"], keep="last", order_by="ts")
    req = _req("dd4_null_src", "dd4_null_dst", recipe, COLS, mode="strict")
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT id, v FROM dd4_null_dst ORDER BY id")
    assert cur.fetchall() == [(1, "real"), (2, "real2")]
    # Gate-8 mirrors the same null rule — the run must verify, not merely
    # agree with a wrong business answer.
    checks = (res.reconciliation or {}).get("dedupe_checks") or []
    ordering = [c for c in checks if c.get("check") == "winner_ordering"]
    assert ordering and ordering[0].get("status") == "passed", checks

    # asc direction: NULL still never wins.
    cur.execute("DROP TABLE IF EXISTS dd4_null_dst2 CASCADE")
    recipe = _dedupe_recipe(
        keys=["id"], keep="last", order_by="ts", order_dir="asc"
    )
    req = _req("dd4_null_src", "dd4_null_dst2", recipe, COLS, mode="strict")
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT id, v FROM dd4_null_dst2 ORDER BY id")
    assert cur.fetchall() == [(1, "real"), (2, "real2")]

    # Every member NULL: the NULL row wins (all-NULL key group).
    _mk_typed_src(
        cur, "dd4_allnull_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [
            ("9", "n1", None, "a@x"),
            ("9", "n2", None, "b@x"),
        ],
    )
    cur.execute("DROP TABLE IF EXISTS dd4_allnull_dst CASCADE")
    recipe = _dedupe_recipe(keys=["id"], keep="last", order_by="ts")
    req = _req(
        "dd4_allnull_src", "dd4_allnull_dst", recipe, COLS, mode="strict"
    )
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT COUNT(*) FROM dd4_allnull_dst")
    assert cur.fetchone()[0] == 1
    conn.close()


def test_dd4_nulls_first_opt_in_pg(tmp_path, monkeypatch):
    """DD-4: nulls='first' is the explicit opt-in that restores NULLS FIRST."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd4_nf_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [("1", "real", 5, "a@x"), ("1", "nullts", None, "a2@x")],
    )
    cur.execute("DROP TABLE IF EXISTS dd4_nf_dst CASCADE")
    recipe = _dedupe_recipe(
        keys=["id"], keep="last", order_by="ts", nulls="first"
    )
    req = _req("dd4_nf_src", "dd4_nf_dst", recipe, COLS, mode="strict")
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT v FROM dd4_nf_dst WHERE id = 1")
    assert cur.fetchone()[0] == "nullts"
    conn.close()


def test_dd5_dd6_unverified_dedupe_keeps_dest_readback_pg(
    tmp_path, monkeypatch
):
    """DD-5/DD-6: an unverifiable winner rule still measures dest COUNT(*)
    and balances — with a 'partial' verdict, never 'Transfer verified'."""
    fake = _engine_env(monkeypatch)
    conn = _pg()
    cur = conn.cursor()
    _mk_typed_src(
        cur, "dd5_src",
        "id INT, v TEXT, ts INT, email TEXT",
        [("1", "a", 1, "a@x"), ("1", "b", 2, "b@x"), ("2", "c", 3, "c@x")],
    )
    cur.execute("DROP TABLE IF EXISTS dd5_dst CASCADE")
    recipe = {"steps": [{"op": "dedupe_rows",
                         "options": {"keys": ["id"], "keep": "first"}}]}
    req = _req("dd5_src", "dd5_dst", recipe, COLS, mode="balanced")
    res = _exec(fake, req)
    assert res.success, res.error
    recon = res.reconciliation or {}
    # DD-6: partial verdict, message leads with the unverified aspect.
    assert recon.get("verification_verdict") == "partial", recon.get("message")
    assert str(recon.get("message") or "").startswith(
        "Dedupe winners unverified"
    ), recon.get("message")
    # DD-5: the independent dest COUNT(*) and conservation are restored.
    ra = recon.get("row_accounting") or {}
    assert ra.get("dest_count") == 2, ra
    assert ra.get("rows_written") == 2, ra
    assert ra.get("balanced") is True, ra
    assert ra.get("rows_written_source") == "gate8_dest_readback", ra
    conn.close()
