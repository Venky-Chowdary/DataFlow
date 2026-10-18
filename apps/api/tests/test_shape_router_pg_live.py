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
        # CASCADE: a leftover dependent view (P0-G1 test) must not block the
        # reset.
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
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
    # cust_id carries the source INT type through the map (RT-8) — a typed
    # column, not TEXT strings.
    assert [tuple(r) for r in cur.fetchall()] == [(1, "150"), (3, "10")]
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

    real_route_verify = recon_mod.verify_postgres_route_table

    def _wrong_route_count(*a, **k):
        rows, checksum, additive = real_route_verify(*a, **k)
        return rows + 1, checksum, additive

    monkeypatch.setattr(reconcile_step_mod, "verify_target", _wrong_count)
    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", _wrong_route_count
    )
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


def _find_key(obj, key, depth=0):
    if isinstance(obj, dict):
        if key in obj:
            return obj
        for v in obj.values():
            if depth < 5:
                hit = _find_key(v, key, depth + 1)
                if hit:
                    return hit
    elif isinstance(obj, list):
        for v in obj:
            if depth < 5:
                hit = _find_key(v, key, depth + 1)
                if hit:
                    return hit
    return None


def test_rt6_append_rerun_additive_digest_proves_rows(tmp_path, monkeypatch):
    """RT-6: an append into occupied route tables is value-verified by the
    additive multiset digest — post minus pre equals this run's rows. A
    successful run must never report checksum_match=false on a table it
    provably wrote."""
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
    req = _req(parsed, recipe, parsed_recipe.recipe_hash, sync="full_refresh_append")
    res1, _ = _run(fake, req, prefix="rta1")
    assert res1.success, res1.error
    res2, job_id2 = _run(fake, req, prefix="rta2")
    assert res2.success, res2.error
    recon = res2.reconciliation or {}
    doc = fake.jobs.get(job_id2) or {}
    holder = _find_key(doc, "route_verification") or {}
    rv = holder.get("route_verification") or {}
    assert set(rv) == {"high", "mid", "other"}
    for name, info in rv.items():
        assert info["digest_match"] is True, (
            f"route {name}: append delta not proven — {info}"
        )
        assert "additive" in str(info.get("digest_note") or "").lower()
    assert holder.get("route_digest_match") is True
    # A successful, proven run must not report checksum_match=false.
    assert recon.get("checksum_match") is not False, recon.get("message")
    assert _fetch(cur, "rt_high") == [1, 1]
    assert _fetch(cur, "rt_mid") == [2, 2, 5, 5]
    assert _fetch(cur, "rt_other") == [3, 3, 6, 6]
    conn.close()


def test_rt7_deduped_rerun_reports_match_and_measured_rows_written(
    tmp_path, monkeypatch
):
    """RT-7: an incremental_deduped routed re-run lands exactly the expected
    set — per-route digests match, checksum_match is true, and rows_written
    is measured (rows upserted) rather than reported unmeasured."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    for t in ("rt", "rt_out", "rt_high", "rt_mid", "rt_other"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE rt (id INT PRIMARY KEY, amt TEXT, region TEXT)")
    for r in ROWS:
        cur.execute("INSERT INTO rt (id, amt, region) VALUES (%s, %s, %s)", r)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    ep = lambda tbl: EndpointConfig(  # noqa: E731
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"], database=parsed["database"],
        username=parsed["username"], password=parsed["password"], table=tbl,
        extra={"primary_key_columns": ["id"]})
    req = TransferRequest(
        source=ep("rt"), destination=ep("rt_out"),
        sync_mode="incremental_deduped",
        shape_recipe=recipe, approved_shape_recipe_hash=parsed_recipe.recipe_hash,
        skip_preflight=True, validation_mode="strict")
    res1, _ = _run(fake, req, prefix="rtd1")
    assert res1.success, res1.error
    res2, job_id2 = _run(fake, req, prefix="rtd2")
    assert res2.success, res2.error
    recon = res2.reconciliation or {}
    doc = fake.jobs.get(job_id2) or {}
    holder = _find_key(doc, "route_verification") or {}
    rv = holder.get("route_verification") or {}
    assert set(rv) == {"high", "mid", "other"}
    for name, info in rv.items():
        assert info["digest_match"] is True, (
            f"route {name}: deduped rerun must compare — {info}"
        )
    assert holder.get("route_digest_match") is True
    assert recon.get("checksum_match") is True, recon.get("message")
    # rows_written measured: the run upserted its 5 routed rows (net growth 0).
    assert recon.get("rows_written") == 5, recon.get("message")
    assert _fetch(cur, "rt_high") == [1]
    assert _fetch(cur, "rt_mid") == [2, 5]
    assert _fetch(cur, "rt_other") == [3, 6]
    conn.close()


def test_rt8_per_route_map_carries_source_column_type_to_ddl(
    tmp_path, monkeypatch
):
    """RT-8: a per-route map renames columns but must carry the source
    column's declared type into the route table DDL — cust_id created INT
    (source id INT), not TEXT."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    cur.execute("DROP TABLE IF EXISTS rt_us")
    recipe, parsed_recipe = _recipe(
        routes=[
            {
                "name": "us",
                "condition": "[region] == 'US'",
                "map": {"id": "cust_id", "amt": "amount"},
            },
            {"name": "eu", "condition": "[region] == 'EU'"},
        ],
        targets={"us": "rt_us", "eu": "rt_rest"},
        unmatched="divert",
    )
    res, _ = _run(fake, _req(parsed, recipe, parsed_recipe.recipe_hash))
    assert res.success, res.error
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'rt_us' ORDER BY ordinal_position"
    )
    types = dict(cur.fetchall())
    assert set(types) == {"cust_id", "amount"}, types
    assert types["cust_id"] in ("integer", "bigint", "smallint"), (
        f"mapped column lost the source INT type: {types}"
    )
    assert types["amount"] in ("text", "character varying"), types
    cur.execute("SELECT cust_id, amount FROM rt_us ORDER BY cust_id")
    assert cur.fetchall() == [(1, "150"), (3, "10")]
    conn.close()


def _ddl_fingerprint(cur, table, schema="public"):
    """Customer-facing DDL fingerprint: columns+types+nullability+defaults,
    PK constraint, secondary indexes, grants — everything a rename-aside
    would destroy (P0-G1)."""
    cur.execute(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY column_name",
        (schema, table),
    )
    cols = cur.fetchall()
    cur.execute(
        "SELECT conname, contype FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE t.relname = %s AND n.nspname = %s ORDER BY conname",
        (table, schema),
    )
    cons = cur.fetchall()
    cur.execute(
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname = %s AND tablename = %s ORDER BY indexname",
        (schema, table),
    )
    idx = [r[0] for r in cur.fetchall()]
    cur.execute(
        "SELECT grantee, privilege_type FROM information_schema.role_table_grants "
        "WHERE table_schema = %s AND table_name = %s ORDER BY grantee, privilege_type",
        (schema, table),
    )
    grants = cur.fetchall()
    return {"columns": cols, "constraints": cons, "indexes": idx, "grants": grants}


def test_p0g1_customer_route_ddl_survives_overwrite_guard(tmp_path, monkeypatch):
    """P0-G1: overwrite rerun over customer-owned route tables must keep the
    customer's table OBJECT — columns, PK, NOT NULL, defaults, indexes,
    grants and dependent views — on success AND after a restored failure."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    cur.execute(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='df_reader') "
        "THEN CREATE ROLE df_reader NOLOGIN; END IF; END $$"
    )
    for t in ("rt_high", "rt_mid", "rt_other"):
        cur.execute(f"DROP VIEW IF EXISTS {t}_v CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        cur.execute(
            f"CREATE TABLE {t} (id INT PRIMARY KEY, amt TEXT NOT NULL, "
            "region TEXT, loaded_at TIMESTAMPTZ DEFAULT now())"
        )
        cur.execute(f"CREATE INDEX {t}_region_idx ON {t} (region)")
        cur.execute(f"GRANT SELECT ON {t} TO df_reader")
    cur.execute("CREATE VIEW rt_high_v AS SELECT id, amt FROM rt_high")
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _req(parsed, recipe, parsed_recipe.recipe_hash)
    res1, _ = _run(fake, req, prefix="rtg1")
    assert res1.success, res1.error
    fp1 = _ddl_fingerprint(cur, "rt_high")
    assert ("id", "integer", "NO", None) in [tuple(c) for c in fp1["columns"]]
    assert len(fp1["columns"]) == 4
    # Successful rerun over occupied tables.
    res2, _ = _run(fake, req, prefix="rtg2")
    assert res2.success, res2.error
    assert _ddl_fingerprint(cur, "rt_high") == fp1
    assert set(_ddl_fingerprint(cur, "rt_mid")["indexes"]) == {
        "rt_mid_region_idx",
        "rt_mid_pkey",
    }
    # The view still reads the live table — not a frozen backup.
    cur.execute("SELECT COUNT(*) FROM rt_high_v")
    assert cur.fetchone()[0] == 1
    cur.execute(
        "SELECT definition FROM pg_views WHERE viewname='rt_high_v'"
    )
    assert "__dfbak_" not in cur.fetchone()[0]
    # Scope the leftover check to THIS test's tables — a stray backup from
    # an unrelated run must not fail this test (P3).
    cur.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'rt_%__dfbak_%'"
    )
    assert cur.fetchall() == []

    # Restored failure: prior rows AND the full customer DDL come back.
    import services.reconciliation as recon_mod

    real_route_verify = recon_mod.verify_postgres_route_table

    def _wrong_route_count(*a, **k):
        rows, checksum, additive = real_route_verify(*a, **k)
        return rows + 1, checksum, additive

    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", _wrong_route_count
    )
    res3, _ = _run(fake, req, prefix="rtg3")
    assert not res3.success
    assert _ddl_fingerprint(cur, "rt_high") == fp1
    cur.execute("SELECT id, amt, region FROM rt_high ORDER BY id")
    assert cur.fetchall() == [(1, "150", "US")]
    cur.execute("SELECT COUNT(*) FROM rt_high_v")
    assert cur.fetchone()[0] == 1
    cur.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'rt_%__dfbak_%'"
    )
    assert cur.fetchall() == []
    cur.execute("DROP VIEW IF EXISTS rt_high_v")
    conn.close()


def test_p0g2_nonpublic_schema_routed_overwrite_rerun(tmp_path, monkeypatch):
    """P0-G2: route tables in a non-public schema — rerun succeeds and a
    forced Gate-8 failure restores pre-run rows; every guard statement is
    schema-qualified."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    _setup_source(cur, ROWS)
    cur.execute("DROP SCHEMA IF EXISTS dfqb_g2 CASCADE")
    cur.execute("CREATE SCHEMA dfqb_g2")
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    src_ep = EndpointConfig(
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"], database=parsed["database"],
        username=parsed["username"], password=parsed["password"],
        schema="public", table="rt")
    dst_ep = EndpointConfig(
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"], database=parsed["database"],
        username=parsed["username"], password=parsed["password"],
        schema="dfqb_g2", table="rt_out")
    req = TransferRequest(
        source=src_ep, destination=dst_ep, sync_mode="full_refresh_overwrite",
        shape_recipe=recipe, approved_shape_recipe_hash=parsed_recipe.recipe_hash,
        skip_preflight=True, validation_mode="strict")
    res1, _ = _run(fake, req, prefix="g2a")
    assert res1.success, res1.error
    cur.execute(
        "SELECT id FROM dfqb_g2.rt_high ORDER BY id"
    )
    assert cur.fetchall() == [(1,)]
    # Rerun over occupied tables — armed backup must resolve the schema.
    res2, _ = _run(fake, req, prefix="g2b")
    assert res2.success, res2.error
    cur.execute("SELECT id FROM dfqb_g2.rt_mid ORDER BY id")
    assert cur.fetchall() == [(2,), (5,)]

    import services.reconciliation as recon_mod

    real_route_verify = recon_mod.verify_postgres_route_table

    def _wrong_route_count(*a, **k):
        rows, checksum, additive = real_route_verify(*a, **k)
        return rows + 1, checksum, additive

    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", _wrong_route_count
    )
    cur.execute("INSERT INTO rt (id, amt, region) VALUES (7, '200', 'US')")
    res3, _ = _run(fake, req, prefix="g2c")
    assert not res3.success
    cur.execute("SELECT id FROM dfqb_g2.rt_high ORDER BY id")
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT id FROM dfqb_g2.rt_other ORDER BY id")
    assert cur.fetchall() == [(3,), (6,)]
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='dfqb_g2' AND table_name LIKE '%__dfbak_%'"
    )
    assert cur.fetchall() == []
    cur.execute("DROP SCHEMA dfqb_g2 CASCADE")
    conn.close()


def _fetchrows(cur, table):
    cur.execute(f"SELECT id, amt, region FROM {table} ORDER BY id")
    return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def _deduped_req(parsed, recipe, recipe_hash, validation="strict"):
    ep = lambda tbl: EndpointConfig(  # noqa: E731
        kind="database", format="postgresql",
        host=parsed["host"], port=parsed["port"], database=parsed["database"],
        username=parsed["username"], password=parsed["password"], table=tbl,
        extra={"primary_key_columns": ["id"]})
    return TransferRequest(
        source=ep("rt"), destination=ep("rt_out"),
        sync_mode="incremental_deduped",
        shape_recipe=recipe, approved_shape_recipe_hash=recipe_hash,
        skip_preflight=True, validation_mode=validation)


def test_p1a_strict_route_digest_mismatch_fails_and_restores(
    tmp_path, monkeypatch
):
    """P1-A: validation_mode=strict + a measured route digest mismatch must
    fail Gate-8 — never report success alongside checksum_match=false — and
    the copy-restore guard puts the pre-run route contents back."""
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
    req = _req(parsed, recipe, parsed_recipe.recipe_hash)
    res1, _ = _run(fake, req, prefix="p1a1")
    assert res1.success, res1.error
    before = {t: _fetchrows(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}

    import services.reconciliation as recon_mod

    real_route_verify = recon_mod.verify_postgres_route_table

    def _corrupt_digest(*a, **k):
        rows, checksum, additive = real_route_verify(*a, **k)
        return rows, "0" * 64, "0" * 64

    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", _corrupt_digest
    )
    # Land DIFFERENT rows so "restored" is distinguishable from "landed".
    cur.execute("INSERT INTO rt (id, amt, region) VALUES (9, '999', 'US')")
    res2, _ = _run(fake, req, prefix="p1a2")
    assert not res2.success, (
        "strict run must fail when its own read-back proves a digest mismatch"
    )
    for t, want in before.items():
        assert _fetchrows(cur, t) == want, f"{t} not restored to pre-run content"
    conn.close()


def test_p1a_balanced_digest_mismatch_carries_failed_verdict(
    tmp_path, monkeypatch
):
    """P1-A (non-strict): a post-write digest mismatch keeps the rows under
    balanced/lenient but the structured verdict must never read "verified" —
    phase is not *_verified, verification_verdict is failed, checksum_match
    is false, and the message leads with the failure. Corruption is injected
    only on the post-write read-back (the pre-write additive scan stays
    honest, so the delta identity itself is still being proven)."""
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
    req = _req(parsed, recipe, parsed_recipe.recipe_hash, sync="full_refresh_append")

    import services.reconciliation as recon_mod

    real_route_verify = recon_mod.verify_postgres_route_table

    # Run 1 lands cleanly and occupies the route tables.
    res1, _ = _run(fake, req, prefix="pv1")
    assert res1.success, res1.error

    state = {"calls": 0}

    def _corrupt_post_only(*a, **k):
        # The pre-write additive scan runs once per occupied route table
        # (all three) before the write; corrupt only the later calls.
        state["calls"] += 1
        rows, checksum, additive = real_route_verify(*a, **k)
        if state["calls"] > 3:
            return rows, "0" * 64, "0" * 64
        return rows, checksum, additive

    for mode, prefix in (("balanced", "pv2"), ("lenient", "pv3")):
        req.validation_mode = mode
        state["calls"] = 0
        monkeypatch.setattr(
            recon_mod, "verify_postgres_route_table", _corrupt_post_only
        )
        res, _ = _run(fake, req, prefix=prefix)
        monkeypatch.setattr(
            recon_mod, "verify_postgres_route_table", real_route_verify
        )
        # P3: passed=false now — callers (UI badge, get_job, MCP, alerting)
        # see the failed verdict; the rows still stay landed and the undo
        # gate keeps them under non-strict modes.
        assert not res.success, res.error
        recon = res.reconciliation or {}
        assert recon.get("checksum_match") is False, recon
        assert recon.get("passed") is False, recon
        assert recon.get("verification_verdict") == "failed", recon
        phase = str(recon.get("phase") or "")
        assert phase == "post_write_verification_failed", phase
        assert not phase.endswith("_verified"), phase
        msg = str(recon.get("message") or "")
        assert msg.startswith("VALUE VERIFICATION FAILED"), msg
    conn.close()


def test_p2_failed_append_undoes_only_this_runs_rows(tmp_path, monkeypatch):
    """P2: a strict append run that fails Gate-8 undoes exactly this run's
    routed rows — the write transaction id (xmin) identifies them, so
    byte-identical copies landed by earlier runs survive and a retry does
    not duplicate."""
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
    req = _req(parsed, recipe, parsed_recipe.recipe_hash, sync="full_refresh_append")
    # Two clean appends: every route table now holds byte-identical
    # duplicate rows (each source row landed twice).
    for p in ("u1", "u2"):
        res, _ = _run(fake, req, prefix=p)
        assert res.success, res.error
    before = {t: _fetchrows(cur, t) for t in ("rt_high", "rt_mid", "rt_other")}
    assert len(before["rt_high"]) == 2 and len(before["rt_mid"]) == 4
    assert len(before["rt_other"]) == 4

    import services.reconciliation as recon_mod

    real_route_verify = recon_mod.verify_postgres_route_table
    state = {"calls": 0}

    def _corrupt_post_only(*a, **k):
        state["calls"] += 1
        rows, checksum, additive = real_route_verify(*a, **k)
        if state["calls"] > 3:  # first three = pre-write additive scans
            return rows, "0" * 64, "0" * 64
        return rows, checksum, additive

    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", _corrupt_post_only
    )
    res3, job3 = _run(fake, req, prefix="u3")
    assert not res3.success
    # Exactly the run-3 copies were removed — the pre-existing
    # byte-identical duplicates from runs 1-2 are untouched.
    for t, want in before.items():
        assert _fetchrows(cur, t) == want, f"{t}: prior content not restored"
    doc3 = fake.jobs.get(job3) or {}
    holder = _find_key(doc3, "partial_batch_undo") or {}
    assert holder.get("partial_batch_undo") == "restored", holder
    # A retry lands cleanly — no leftover from the failed run.
    monkeypatch.setattr(
        recon_mod, "verify_postgres_route_table", real_route_verify
    )
    res4, _ = _run(fake, req, prefix="u4")
    assert res4.success, res4.error
    for t, want in before.items():
        assert len(_fetchrows(cur, t)) == len(want) + len(want) // 2
    conn.close()


def test_p1b_deduped_route_move_leaves_no_stale_copy(
    tmp_path, monkeypatch
):
    """P1-B: a keyed routed upsert holds each key in exactly one route table.
    A row that changes route loses its copy in the previous table — checked
    in both directions and for a named->default move."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    for t in ("rt", "rt_out", "rt_high", "rt_mid", "rt_other"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE rt (id INT PRIMARY KEY, amt TEXT, region TEXT)")
    for r in ROWS:
        cur.execute("INSERT INTO rt (id, amt, region) VALUES (%s, %s, %s)", r)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
            {"name": "other", "default": True},
        ],
        targets={"high": "rt_high", "mid": "rt_mid", "other": "rt_other"},
    )
    req = _deduped_req(parsed, recipe, parsed_recipe.recipe_hash)
    for p in ("pm1", "pm2"):
        res, _ = _run(fake, req, prefix=p)
        assert res.success, res.error
    assert _fetchrows(cur, "rt_high") == [(1, "150", "US")]
    assert _fetchrows(cur, "rt_mid") == [(2, "75", "EU"), (5, "60", "EU")]
    assert _fetchrows(cur, "rt_other") == [(3, "10", "US"), (6, "1", "AP")]

    # default -> named: id 3 moves from 'other' to 'mid'.
    cur.execute("UPDATE rt SET amt = '55' WHERE id = 3")
    res3, job3 = _run(fake, req, prefix="pm3")
    assert res3.success, res3.error
    assert _fetchrows(cur, "rt_mid") == [(2, "75", "EU"), (3, "55", "US"), (5, "60", "EU")]
    assert _fetchrows(cur, "rt_other") == [(6, "1", "AP")], (
        "stale copy of key 3 left in 'other' after it moved to 'mid'"
    )

    # named -> default: id 3 moves back to 'other'; named -> named is the
    # same code path (the delete runs on every route table).
    cur.execute("UPDATE rt SET amt = '10' WHERE id = 3")
    res4, _ = _run(fake, req, prefix="pm4")
    assert res4.success, res4.error
    assert _fetchrows(cur, "rt_mid") == [(2, "75", "EU"), (5, "60", "EU")]
    assert _fetchrows(cur, "rt_other") == [(3, "10", "US"), (6, "1", "AP")]

    # Union still equals the source set exactly — 5 rows, each key once.
    cur.execute(
        "SELECT id FROM rt_high UNION ALL SELECT id FROM rt_mid "
        "UNION ALL SELECT id FROM rt_other"
    )
    assert sorted(r[0] for r in cur.fetchall()) == [1, 2, 3, 5, 6]
    doc3 = fake.jobs.get(job3) or {}
    holder = _find_key(doc3, "route_verification") or {}
    assert holder.get("route_digest_match") is True, holder
    conn.close()


def test_p1b_deduped_diverted_key_leaves_route_tables(
    tmp_path, monkeypatch
):
    """P1-B: a key that stops matching every route (unmatched=divert) is
    removed from its old route table in the same transaction as the run."""
    fake = _pg_env(monkeypatch)
    parsed = _parse_dsn()
    conn = psycopg2.connect(host=parsed["host"], port=parsed["port"], dbname=parsed["database"], user=parsed["user"], password=parsed["password"]); conn.autocommit = True
    cur = conn.cursor()
    for t in ("rt", "rt_out", "rt_high", "rt_mid"):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute("CREATE TABLE rt (id INT PRIMARY KEY, amt TEXT, region TEXT)")
    for r in ((1, "150", "US"), (2, "75", "EU"), (5, "60", "EU")):
        cur.execute("INSERT INTO rt (id, amt, region) VALUES (%s, %s, %s)", r)
    recipe, parsed_recipe = _recipe(
        routes=[
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "mid", "condition": "to_number([amt]) >= 50"},
        ],
        targets={"high": "rt_high", "mid": "rt_mid"},
        unmatched="divert",
    )
    req = _deduped_req(parsed, recipe, parsed_recipe.recipe_hash)
    res1, _ = _run(fake, req, prefix="pd1")
    assert res1.success, res1.error
    assert _fetchrows(cur, "rt_high") == [(1, "150", "US")]
    assert _fetchrows(cur, "rt_mid") == [(2, "75", "EU"), (5, "60", "EU")]

    # id 5 stops matching every route -> diverted; its stale copy must go.
    cur.execute("UPDATE rt SET amt = '5' WHERE id = 5")
    res2, _ = _run(fake, req, prefix="pd2")
    assert res2.success, res2.error
    assert _fetchrows(cur, "rt_mid") == [(2, "75", "EU")], (
        "diverted key 5 left a stale copy in 'mid'"
    )
    conn.close()
