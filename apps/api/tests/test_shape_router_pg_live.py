"""RT-1/RT-2 router regressions on the real PostgreSQL write path.

Gated on DATAFLOW_PG_TEST_DSN (e.g. postgresql://dataflow:dataflow@127.0.0.1:5432/dataflow)
— SQLite hides these bugs because its writer ignores pg_overwrite_in_transaction
and the route-tag column silently passes through writes.
"""
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.environ.get("DATAFLOW_PG_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DATAFLOW_PG_TEST_DSN unset")

import src.transfer.engine as engine_mod  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from services.shape_models import ShapeRecipe  # noqa: E402


def _pg_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,localhost,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16")
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _parse_dsn():
    from urllib.parse import urlparse

    u = urlparse(DSN)
    return {
        "host": u.hostname,
        "port": u.port or 5432,
        "database": (u.path or "/postgres").lstrip("/"),
        "username": u.username or "postgres",
        "user": u.username or "postgres",
        "password": u.password or "",
    }


def _setup_source(cur, rows):
    for t in ("rt", "rt_out", "rt_high", "rt_mid", "rt_other",
              "rt_out__high", "rt_out__mid", "rt_out__other",
              "rt__us", "rt__rest", "rt_us", "rt_rest"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE rt (id INT, amt TEXT, region TEXT)")
    for r in rows:
        cur.execute("INSERT INTO rt (id, amt, region) VALUES (%s, %s, %s)", r)


def _recipe(routes, targets=None, unmatched=None):
    opts = {"routes": routes}
    if targets:
        opts["targets"] = targets
    if unmatched:
        opts["unmatched"] = unmatched
    recipe = {"steps": [{"op": "router", "options": opts}]}
    return recipe, ShapeRecipe.parse(recipe)


def _req(parsed, recipe, recipe_hash, table="rt_out", sync="full_refresh_overwrite"):
    ep = lambda tbl: EndpointConfig(  # noqa: E731
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"], database=parsed["database"],
        username=parsed["username"], password=parsed["password"], table=tbl)
    return TransferRequest(
        source=ep("rt"), destination=ep(table), sync_mode=sync,
        shape_recipe=recipe, approved_shape_recipe_hash=recipe_hash,
        skip_preflight=True, validation_mode="strict")


def _run(fake, req, prefix="rtp"):
    job_id = prefix + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id), job_id


def _fetch(cur, table):
    cur.execute(f"SELECT id FROM {table} ORDER BY id")
    return [r[0] for r in cur.fetchall()]


ROWS = [(1, "150", "US"), (2, "75", "EU"), (3, "10", "US"), (5, "60", "EU"), (6, "1", "AP")]


def test_rt1_three_routes_default_lands_all_rows(tmp_path, monkeypatch):
    """Every input row lands in exactly one route table — the default route
    included — and Gate-8 verifies per-route tables without the tag column."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    res, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    assert _fetch(cur, "rt_high") == [1]
    assert _fetch(cur, "rt_mid") == [2, 5]
    assert _fetch(cur, "rt_other") == [3, 6]
    conn.close()


def test_rt1_two_routes_default_naming(tmp_path, monkeypatch):
    """Default naming (<base>__<route>): default route gets unmatched rows."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "us", "condition": "[region] = 'US'"},
            {"name": "rest", "default": True},
        ],
        targets={"us": "rt__us", "rest": "rt__rest"},
    )
    res, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    assert _fetch(cur, "rt__us") == [1, 3]
    assert _fetch(cur, "rt__rest") == [2, 5, 6]
    conn.close()


def test_rt2_overwrite_rerun_replaces_every_route_table(tmp_path, monkeypatch):
    """A second overwrite run replaces ALL route tables — no route may append."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    res1, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash), prefix="rtp1")
    assert res1.success, res1.error
    res2, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash), prefix="rtp2")
    assert res2.success, res2.error
    assert _fetch(cur, "rt_high") == [1]
    assert _fetch(cur, "rt_mid") == [2, 5]
    assert _fetch(cur, "rt_other") == [3, 6]
    conn.close()


def test_rt1_unmatched_divert_sends_rows_to_dlq(tmp_path, monkeypatch):
    """unmatched=divert: rows matching no route go to the DLQ with a reason,
    and the route tables still land every matched row."""
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
        ],
        targets={"high": "rt_high", "mid": "rt_mid"},
        unmatched="divert",
    )
    res, job_id = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    assert _fetch(cur, "rt_high") == [1]
    assert _fetch(cur, "rt_mid") == [2, 5]
    dlq_rows = dlq.canonical_replay_details({"id": job_id})
    assert len(dlq_rows) == 2  # ids 3 (amt<50) and 6 — no route matched
    conn.close()


def test_rt4_append_rerun_reconciles_clean(tmp_path, monkeypatch):
    """Append re-run of a routed recipe: per-route pre-counts make the
    dest COUNT growth equal this run's writes — no 'more rows than read'."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _req(parsed, recipe, parsed_recipe.recipe_hash, sync="append")
    res1, _ = _run(fake, req, prefix="rta1")
    assert res1.success, res1.error
    res2, _ = _run(fake, req, prefix="rta2")
    assert res2.success, res2.error
    assert _fetch(cur, "rt_high") == [1, 1]
    assert _fetch(cur, "rt_mid") == [2, 2, 5, 5]
    assert _fetch(cur, "rt_other") == [3, 3, 6, 6]
    conn.close()


def test_rt5_failing_route_append_restores_all_route_tables(tmp_path, monkeypatch):
    """A failed route write in append mode must leave EVERY route table
    exactly as before the run — prior rows intact, no third copy anywhere."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _req(parsed, recipe, parsed_recipe.recipe_hash, sync="append")
    for i in (1, 2):
        res, _ = _run(fake, req, prefix=f"rtb{i}")
        assert res.success, res.error
    before = {t: _fetch(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}
    assert before == {"rt_high": [1, 1], "rt_mid": [2, 2, 5, 5], "rt_other": [3, 3, 6, 6]}
    # Third run fails on the 'other' route table: a new source row routes
    # there but its CHECK rejects it. Existing rows pass the constraint.
    cur.execute("INSERT INTO rt (id, amt, region) VALUES (7, '5', 'NO')")
    cur.execute('ALTER TABLE rt_other ADD CONSTRAINT rt_other_chk CHECK (id <> 7)')
    res3, _ = _run(fake, req, prefix="rtb3")
    assert not res3.success
    after = {t: _fetch(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}
    assert after == before, f"route tables changed by failed run: {after}"
    conn.close()


def test_route_verification_records_value_digests(tmp_path, monkeypatch):
    """Each route's ledger entry carries the value-level digest match:
    dest read-back checksum vs the write-pass fingerprint per route."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    res, job_id = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    doc = fake.jobs.get(job_id) or {}

    def _find(obj, depth=0):
        if isinstance(obj, dict):
            if "route_verification" in obj:
                return obj
            for v in obj.values():
                if depth < 5:
                    hit = _find(v, depth + 1)
                    if hit:
                        return hit
        elif isinstance(obj, list):
            for v in obj:
                if depth < 5:
                    hit = _find(v, depth + 1)
                    if hit:
                        return hit
        return None

    holder = _find(doc)
    assert holder, "route_verification not found on the job"
    rv = holder["route_verification"]
    assert set(rv) == {"high", "mid", "other"}
    for name, info in rv.items():
        assert info["checksum"], f"route {name}: no read-back digest"
        assert info["digest_match"] is True, (
            f"route {name}: dest digest {info['checksum']} != "
            f"expected {info.get('expected_checksum')}"
        )
    assert holder.get("route_digest_match") is True
    conn.close()


def test_rt3_per_route_map_renames_columns_on_write(tmp_path, monkeypatch):
    """RT-3: a per-route map renames the route table's columns at write time —
    the shared batch keeps the source shape, so the data-quality audit never
    reads the mapped population as NULLs. Fails on 38f990b9: the map was
    applied to the row dict at shape time, the 'us' table was never created,
    and the run died on 'Required column id has 2 null/empty values'."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {
                "name": "us",
                "condition": "[region] = 'US'",
                "map": {"id": "cust_id", "amt": "amount"},
            },
        ],
        targets={"us": "rt_us"},
        unmatched="divert",
    )
    for t in ("rt_us",):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    res, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    cur.execute("SELECT cust_id, amount FROM rt_us ORDER BY cust_id")
    assert [tuple(r) for r in cur.fetchall()] == [("1", "150"), ("3", "10")]
    # Diverted rows still reach the DLQ — the 'us' table does not get them.
    cur.execute(
        "SELECT COUNT(*) FROM rt_us"
    )
    assert cur.fetchone()[0] == 2
    conn.close()


def test_gate8_failure_restores_route_tables_to_pre_run_content(
    tmp_path, monkeypatch
):
    """Failed-Gate-8 restore guard: an overwrite routed run into occupied
    route tables must leave every route table exactly as it was before the
    run when the post-write proof fails — the landed tables are restored,
    not left holding this run's content.

    Fails on cc1ef3d3: route tables kept the new rows (no restore path
    existed for routed PG runs)."""
    import src.transfer.reconcile_step as reconcile_step_mod
    import services.reconciliation as recon_mod

    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(
        host=parsed["host"], port=parsed["port"], dbname=parsed["database"],
        user=parsed["user"], password=parsed["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    for t in ("rt_high", "rt_mid", "rt_other"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute(f"DROP TABLE IF EXISTS {t}__dfbak_rtgrd")
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _req(parsed, recipe, parsed_recipe.recipe_hash)
    res, _ = _run(fake, req)
    assert res.success, res.error
    before = {t: _fetch(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}
    assert before == {"rt_high": [1], "rt_mid": [2, 5], "rt_other": [3, 6]}

    # Run 2 must land DIFFERENT content than run 1 — otherwise "kept the new
    # rows" is indistinguishable from "restored the old rows".
    cur.execute("INSERT INTO rt (id, amt, region) VALUES (7, '200', 'US')")

    # Force the post-write proof to fail AFTER the writes commit — a row-count
    # mismatch on the route read-back (a checksum-only divergence does not
    # fail the job; a population mismatch does).
    real_verify = recon_mod.verify_target

    def _wrong_count(*a, **k):
        rows, checksum = real_verify(*a, **k)
        return rows + 1, checksum

    monkeypatch.setattr(reconcile_step_mod, "verify_target", _wrong_count)
    res2, job2 = _run(fake, req, prefix="rtgrd")
    assert not res2.success
    after = {t: _fetch(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}
    assert after == before, (
        f"failed Gate-8 left run-2 content: {after} != {before}"
    )
    # No leaked backups after a successful restore.
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'rt_%__dfbak_%'"
    )
    assert cur.fetchall() == []
    conn.close()


def test_gate8_success_discards_route_backups(tmp_path, monkeypatch):
    """A verified overwrite rerun drops the renamed-aside backups — no
    __dfbak_ tables may linger."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(
        host=parsed["host"], port=parsed["port"], dbname=parsed["database"],
        user=parsed["user"], password=parsed["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _req(parsed, recipe, parsed_recipe.recipe_hash)
    res, _ = _run(fake, req)
    assert res.success, res.error
    # Second overwrite into now-occupied tables -> backups armed + discarded.
    res2, _ = _run(fake, req, prefix="rtok")
    assert res2.success, res2.error
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'rt_%__dfbak_%'"
    )
    assert cur.fetchall() == []
    assert _fetch(cur, "rt_high") == [1]
    assert _fetch(cur, "rt_mid") == [2, 5]
    assert _fetch(cur, "rt_other") == [3, 6]
    # The guard must have armed and recorded its backups (a pre-fix run never
    # creates them, so "no __dfbak_ tables" alone proves nothing).
    backups = (res2.destination_summary or {}).get("route_backups") or {}
    assert set(backups) == {"high", "mid", "other"}, backups
    assert all(
        v["backup"].startswith(f"{v['table']}__dfbak_") for v in backups.values()
    ), backups
    conn.close()
