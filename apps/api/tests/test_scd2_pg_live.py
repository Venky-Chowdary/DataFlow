"""B1 SCD2 Type-2 PG-live regressions.

Gated on DATAFLOW_PG_TEST_DSN — SQLite cannot prove the history contract
against a real destination (type fidelity of valid_from/valid_to and the
per-table write transactions differ).
"""
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.environ.get("DATAFLOW_PG_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DATAFLOW_PG_TEST_DSN unset")

from services.scd2_engine import (  # noqa: E402
    apply_scd2,
    preview_scd2_merge,
    verify_scd2_history,
)
from src.transfer.models import EndpointConfig  # noqa: E402


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


def _cfg(endpoint):
    from src.transfer.adapters import resolve_connector_config

    return resolve_connector_config(endpoint)


def _pg(schema="public"):
    parsed = _parse_dsn()
    conn = psycopg2.connect(
        host=parsed["host"], port=parsed["port"], dbname=parsed["database"],
        user=parsed["username"], password=parsed["password"],
    )
    conn.autocommit = True
    return conn, parsed


def _endpoint(table):
    p = _parse_dsn()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=p["host"],
        port=p["port"],
        database=p["database"],
        username=p["username"],
        password=p["password"],
        table=table,
    )


ROWS = [
    {"id": "1", "name": "A", "price": "10.00", "note": "n1"},
    {"id": "2", "name": "B", "price": "20.00", "note": "n2"},
    {"id": "3", "name": "C", "price": "30.00", "note": "n3"},
]
COLS = ["id", "name", "price", "note"]
SCHEMA = {"id": "string", "name": "string", "price": "decimal", "note": "string"}


def _fetch(cur, table):
    cur.execute(
        f"SELECT id, name, price, note, version, valid_from, valid_to, "
        f"is_current FROM {table} ORDER BY id, version"
    )
    return cur.fetchall()


def test_scd2_full_lifecycle_pg(tmp_path):
    table = f"scd2_t_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    cur = conn.cursor()
    opts = {
        "version": "version",
        "tracked": ["name", "price"],  # note is untracked
        "type1": True,
        "close_missing": True,
    }
    ep = _endpoint(table)

    # 1. First load — one current version per key, version 1.
    s1 = apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    assert s1["ok"], s1
    assert s1["rows_written"] == 3 and s1["updated_rows"] == 0
    rows = _fetch(cur, table)
    assert len(rows) == 3
    assert all(r[4] == 1 and r[7] is True and r[6] is None for r in rows)

    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    assert hist["current_rows"] == 3 and hist["keys"] == 3

    # 2. Unchanged rerun — a complete no-op.
    s2 = apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    assert s2["ok"] and s2["rows_written"] == 0 and s2["updated_rows"] == 0
    assert len(_fetch(cur, table)) == 3

    # 3. Tracked change (price 10.00 -> 15.00 on id=1): close v1, open v2
    #    in the same transaction — one is_current per key still.
    changed = [dict(r) for r in ROWS]
    changed[0]["price"] = "15.00"
    s3 = apply_scd2(ep, changed, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s3["ok"] and s3["rows_written"] == 1 and s3["updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    v1 = [r for r in rows if str(r[0]) == "1" and r[4] == 1][0]
    v2 = [r for r in rows if str(r[0]) == "1" and r[4] == 2][0]
    assert v1[7] is False and v1[6] is not None
    assert v2[7] is True and v2[6] is None
    assert str(v2[5]) == str(v1[6])  # contiguous, no gap/overlap
    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    assert hist["current_rows"] == 3 and hist["history_rows"] == 1

    # 4. Untracked change (note on id=2): Type-1 overwrite in place —
    #    no new version, no expire.
    untracked = [dict(r) for r in changed]
    untracked[1]["note"] = "n2-updated"
    s4 = apply_scd2(ep, untracked, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s4["ok"] and s4["rows_written"] == 0 and s4["updated_rows"] == 0
    assert s4["type1_updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    row2 = [r for r in rows if str(r[0]) == "2"][0]
    assert row2[3] == "n2-updated" and row2[4] == 1 and row2[7] is True

    # 5. Key removal (id=3 absent from a full snapshot): soft-close —
    #    the version stays in history, stops being current.
    reduced = [r for r in untracked if r["id"] != "3"]
    s5 = apply_scd2(
        ep, reduced, COLS, SCHEMA, None, ["id"],
        complete_snapshot=True, scd2_opts=opts,
    )
    assert s5["ok"] and s5["closed_missing_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 4
    row3 = [r for r in rows if str(r[0]) == "3"][0]
    assert row3[7] is False and row3[6] is not None

    # 6. Same key changes twice across runs: id=1 price again -> 18.00.
    #    v2 closes, v3 opens; v1 keeps its original close.
    twice = [dict(r) for r in reduced]
    twice[0]["price"] = "18.00"
    s6 = apply_scd2(ep, twice, COLS, SCHEMA, None, ["id"], scd2_opts=opts)
    assert s6["ok"] and s6["rows_written"] == 1 and s6["updated_rows"] == 1
    rows = _fetch(cur, table)
    assert len(rows) == 5
    versions_1 = sorted(r[4] for r in rows if str(r[0]) == "1")
    assert versions_1 == [1, 2, 3]
    currents_1 = [r for r in rows if str(r[0]) == "1" and r[7] is True]
    assert len(currents_1) == 1 and currents_1[0][4] == 3

    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], opts
    )
    assert hist["ok"], hist["violations"]
    conn.close()


def test_scd2_custom_stamp_names_pg(tmp_path):
    """Recipe-declared stamp column names are written to the destination."""
    table = f"scd2_c_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    cur = conn.cursor()
    resolved = {
        "version": "ver",
        "valid_from": "eff_from",
        "valid_to": "eff_to",
        "is_current": "current_flag",
        "row_hash": "attr_hash",
    }
    ep = _endpoint(table)
    s1 = apply_scd2(
        ep, [dict(r) for r in ROWS[:2]], COLS, SCHEMA, None, ["id"],
        scd2_opts=resolved,
    )
    assert s1["ok"], s1
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s",
        (table,),
    )
    cols = {r[0] for r in cur.fetchall()}
    assert {"eff_from", "eff_to", "current_flag", "attr_hash", "ver"} <= cols
    hist = verify_scd2_history(
        _cfg(ep), "public", table, ["id"], resolved
    )
    assert hist["ok"], hist["violations"]
    conn.close()


def test_scd2_per_step_preview_pg(tmp_path):
    """The per-step preview names which rows would open/close — before any write."""
    table = f"scd2_p_{uuid.uuid4().hex[:8]}"
    conn, parsed = _pg()
    ep = _endpoint(table)
    opts = {"version": "version", "tracked": ["name", "price"], "type1": True}
    apply_scd2(
        ep, [dict(r) for r in ROWS], COLS, SCHEMA, None, ["id"],
        scd2_opts=opts,
    )
    # Incoming: id=1 tracked change, id=2 untracked change, id=3 gone, id=4 new.
    incoming = [
        {"id": "1", "name": "A", "price": "99.00", "note": "n1"},
        {"id": "2", "name": "B", "price": "20.00", "note": "n2-new"},
        {"id": "4", "name": "D", "price": "40.00", "note": "n4"},
    ]
    p = preview_scd2_merge(
        ep, incoming, COLS, SCHEMA, None, ["id"],
        complete_snapshot=True, scd2_opts=opts,
    )
    assert p["ok"], p
    assert p["counts"]["open"] == 2      # id=1 new version + id=4 first load
    assert p["counts"]["close"] == 2     # id=1 close + id=3 missing-close
    assert p["counts"]["type1"] == 1     # id=2 untracked Type-1
    assert set(p["opens"]) == {"1", "4"}
    assert set(p["closes"]) == {"1", "3"}
    # Preview wrote nothing — history unchanged.
    hist = verify_scd2_history(_cfg(ep), "public", table, ["id"], opts)
    assert hist["ok"] and hist["history_rows"] == 0
    conn.close()


def test_scd2_recipe_step_parses_contract():
    """The recipe ``scd2`` step declares the contract the merge honors."""
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "scd2",
                    "enabled": True,
                    "options": {
                        "keys": ["id"],
                        "tracked": ["name", "price"],
                        "type1": True,
                        "version": "version",
                    },
                }
            ]
        },
        source_columns=COLS,
    )
    names = recipe.scd2_column_names()
    assert names["keys"] == ["id"]
    assert names["tracked"] == ["name", "price"]
    assert names["type1"] is True
    assert names["close_missing"] is True
    assert names["version"] == "version"


# ---------------------------------------------------------------------------
# Engine-level: the recipe scd2 step through UniversalTransferEngine — the
# path apply_scd2-level tests cannot reach (B1-E1/B1-E2).
# ---------------------------------------------------------------------------


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


def _scd2_recipe(**over):
    opts = {
        "keys": ["id"],
        "tracked": ["name", "price"],
        "type1": True,
        "close_missing": True,
        "version": "ver",
        "effective_from": "eff_from",
        "end_date": "eff_to",
        "is_current": "cur_flag",
        "row_hash": "rh",
    }
    opts.update(over)
    recipe = {"steps": [{"op": "scd2", "options": opts}]}
    return recipe


def _engine_req(src_table, dst_table, recipe=None, mode="strict"):
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
        sync_mode="scd2",
        stream_contracts=[
            {"selected": True, "primary_key": "id", "sync_mode": "scd2"}
        ],
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


def _exec(fake, req, prefix="scde"):
    import uuid as _uuid

    from src.transfer.engine import UniversalTransferEngine

    job_id = prefix + _uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _mk_src(cur, rows, table="scd2_src"):
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.execute(
        f"CREATE TABLE {table} (id INT, name TEXT, price TEXT, note TEXT)"
    )
    for r in rows:
        cur.execute(
            f"INSERT INTO {table} (id, name, price, note) "
            "VALUES (%s, %s, %s, %s)",
            (r["id"], r["name"], r["price"], r["note"]),
        )


def _dst_rows(cur, table="scd2_dst"):
    cur.execute(
        f"SELECT id, name, price, note, ver, eff_from, eff_to, cur_flag "
        f"FROM {table} ORDER BY id, ver"
    )
    return cur.fetchall()


def test_scd2_recipe_runs_through_engine_pg(tmp_path, monkeypatch):
    """B1-E1: an scd2-only recipe reaches the streaming scd2 route — declared
    stamps, tracked-hash diff, Type-1, close-missing all live on PG."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_e1_src")
    cur.execute("DROP TABLE IF EXISTS scd2_e1_dst CASCADE")
    recipe = _scd2_recipe()
    req = _engine_req("scd2_e1_src", "scd2_e1_dst", recipe, mode="strict")

    res = _exec(fake, req)
    assert res.success, res.error
    rows = _dst_rows(cur, table="scd2_e1_dst")
    assert len(rows) == 3
    assert all(r[4] == 1 and r[7] is True and r[6] is None for r in rows)

    # Unchanged rerun — no-op.
    res = _exec(fake, req)
    assert res.success, res.error
    assert len(_dst_rows(cur, table="scd2_e1_dst")) == 3

    # Tracked change id=1 price -> 15.00: v1 closes, v2 opens.
    _mk_src(cur, [{**ROWS[0], "price": "15.00"}, ROWS[1], ROWS[2]], table="scd2_e1_src")
    res = _exec(fake, req)
    assert res.success, res.error
    rows = _dst_rows(cur, table="scd2_e1_dst")
    assert len(rows) == 4
    v2 = [r for r in rows if str(r[0]) == "1" and r[4] == 2]
    assert v2 and v2[0][7] is True and v2[0][2] == "15.00"

    # Untracked change id=2 note: Type-1 in place.
    changed = [{**ROWS[0], "price": "15.00"},
               {**ROWS[1], "note": "n2-new"}, ROWS[2]]
    _mk_src(cur, changed, table="scd2_e1_src")
    res = _exec(fake, req)
    assert res.success, res.error
    rows = _dst_rows(cur, table="scd2_e1_dst")
    assert len(rows) == 4
    row2 = [r for r in rows if str(r[0]) == "2"][0]
    assert row2[3] == "n2-new" and row2[4] == 1

    # Key removal: id=3 absent -> soft-close; id=3 returns -> new version.
    reduced = [r for r in changed if r["id"] != "3"]
    _mk_src(cur, reduced, table="scd2_e1_src")
    res = _exec(fake, req)
    assert res.success, res.error
    rows = _dst_rows(cur, table="scd2_e1_dst")
    row3 = [r for r in rows if str(r[0]) == "3"][0]
    assert row3[7] is False and row3[6] is not None

    _mk_src(cur, changed, table="scd2_e1_src")
    res = _exec(fake, req)
    assert res.success, res.error
    rows = _dst_rows(cur, table="scd2_e1_dst")
    v3 = [r for r in rows if str(r[0]) == "3"]
    assert sorted(r[4] for r in v3) == [1, 2]
    assert sum(1 for r in v3 if r[7] is True) == 1
    conn.close()


def test_scd2_history_tamper_fails_gate8_pg(tmp_path, monkeypatch):
    """B1-E2: the Gate-8 history proof runs on the streaming path — a second
    is_current row and a broken interval are caught, strict fails the job,
    balanced carries a failed verdict."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_e2_src")
    cur.execute("DROP TABLE IF EXISTS scd2_e2_dst CASCADE")
    recipe = _scd2_recipe()
    req_strict = _engine_req("scd2_e2_src", "scd2_e2_dst", recipe, mode="strict")
    res = _exec(fake, req_strict)
    assert res.success, res.error

    # Tamper: insert a second is_current row for key 1 (duplicates its
    # current values under ver=99) then re-run unchanged under strict.
    cur.execute(
        "INSERT INTO scd2_e2_dst (id, name, price, note, ver, eff_from, "
        "eff_to, cur_flag, rh) "
        "SELECT id, name, price, note, 99, eff_from, NULL, true, rh "
        "FROM scd2_e2_dst WHERE id = '1'"
    )
    res = _exec(fake, req_strict)
    assert not res.success, "second is_current row must fail strict Gate-8"
    cur.execute("DELETE FROM scd2_e2_dst WHERE id = '1' AND ver = 99")

    # Same tamper under balanced: rows kept, verdict failed.
    req_bal = _engine_req("scd2_e2_src", "scd2_e2_dst", recipe, mode="balanced")
    cur.execute(
        "INSERT INTO scd2_e2_dst (id, name, price, note, ver, eff_from, "
        "eff_to, cur_flag, rh) "
        "SELECT id, name, price, note, 99, eff_from, NULL, true, rh "
        "FROM scd2_e2_dst WHERE id = '1'"
    )
    res = _exec(fake, req_bal)
    cur.execute("DELETE FROM scd2_e2_dst WHERE id = '1' AND ver = 99")
    recon = getattr(res, "reconciliation", None) or {}
    assert res.success is False or recon.get("passed") is False
    assert recon.get("verification_verdict") == "failed" or (
        "SCD2 history verification failed"
        in str(recon.get("message") or "")
    )

    # Overlapping interval: extend v1's valid_to past v2's valid_from.
    _mk_src(cur, [{**ROWS[0], "price": "15.00"}, ROWS[1], ROWS[2]], table="scd2_e2_src")
    res = _exec(fake, req_strict)
    assert res.success, res.error
    cur.execute(
        "UPDATE scd2_e2_dst SET eff_to = eff_to + interval '1 day' "
        "WHERE id = '1' AND ver = 1"
    )
    res = _exec(fake, req_strict)
    assert not res.success, "an overlapping interval must fail strict Gate-8"
    conn.close()


def _mk_src_bulk(cur, rows, table="scd2_src"):
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    cur.execute(
        f"CREATE TABLE {table} (id INT, name TEXT, price TEXT, note TEXT)"
    )
    cur.executemany(
        f"INSERT INTO {table} (id, name, price, note) "
        "VALUES (%s, %s, %s, %s)",
        [(r["id"], r["name"], r["price"], r["note"]) for r in rows],
    )


def _bulk_rows(n, start=0):
    return [
        {"id": str(i), "name": f"n{i}", "price": f"{i}.00", "note": f"note{i}"}
        for i in range(start, start + n)
    ]


def test_scd2_multi_batch_recount_default_path_pg(tmp_path, monkeypatch):
    """B1-E3: the version recount is per run, not per timestamp — each
    streaming batch and the close pass stamps its own instant. A >2-batch
    first load plus a change run (tracked changes + deletes + inserts
    across batches) must recount every row the run opened and closed on
    the DEFAULT scd2 path (no recipe)."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    # 2500 source rows -> 3 merge batches of 1000, each with its own stamp.
    _mk_src_bulk(cur, _bulk_rows(2500))
    cur.execute("DROP TABLE IF EXISTS scd2_dst CASCADE")
    req = _engine_req("scd2_src", "scd2_dst", None, mode="strict")

    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT COUNT(*) FROM scd2_dst WHERE is_current")
    assert cur.fetchone()[0] == 2500

    # Change run across batches: 1200 tracked changes + 300 deletes + 400
    # inserts = 1600 opened / 1500 closed / 1200 changed this run.
    changed = _bulk_rows(2500)
    for i in range(1200):
        changed[i]["name"] = f"changed{i}"
    changed = [r for r in changed if int(r["id"]) < 2200]
    changed += _bulk_rows(400, start=2500)
    _mk_src_bulk(cur, changed)

    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT COUNT(*) FROM scd2_dst WHERE is_current")
    assert cur.fetchone()[0] == 2600
    recon = getattr(res, "reconciliation", None) or {}
    hist = recon.get("scd2_history") or {}
    deltas = hist.get("deltas") or {}
    assert deltas.get("opened") == 1600, deltas
    assert deltas.get("closed") == 1500, deltas
    assert deltas.get("changed") == 1200, deltas
    conn.close()


def test_scd2_multi_batch_recount_recipe_path_pg(tmp_path, monkeypatch):
    """B1-E3 on the recipe path — same multi-batch recount with declared
    stamp names and a version column."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(2500))
    cur.execute("DROP TABLE IF EXISTS scd2_dst CASCADE")
    # version=None: >400 missing keys trips B1-E4's unchunked
    # _fetch_max_versions — that column+chunking regression is covered by
    # the B1-E4 fix commit; the recount contract itself does not need it.
    recipe = _scd2_recipe(version=None)
    req = _engine_req("scd2_src", "scd2_dst", recipe, mode="strict")

    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute("SELECT COUNT(*) FROM scd2_dst WHERE cur_flag")
    assert cur.fetchone()[0] == 2500

    changed = _bulk_rows(2500)
    for i in range(1100):
        changed[i]["price"] = f"{i}.99"
    changed = [r for r in changed if int(r["id"]) < 2400]
    changed += _bulk_rows(300, start=2500)
    _mk_src_bulk(cur, changed)

    res = _exec(fake, req)
    assert res.success, res.error
    recon = getattr(res, "reconciliation", None) or {}
    hist = recon.get("scd2_history") or {}
    deltas = hist.get("deltas") or {}
    assert deltas.get("opened") == 1400, deltas
    assert deltas.get("closed") == 1200, deltas
    assert deltas.get("changed") == 1100, deltas
    conn.close()


def test_scd2_recount_catches_deleted_opened_row_pg(tmp_path, monkeypatch):
    """B1-E3 regression: deleting one row this run opened, mid read-back,
    must still fail the recount — the window must not swallow tampers."""
    import services.scd2_engine as scd2_mod

    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(1500))
    cur.execute("DROP TABLE IF EXISTS scd2_dst CASCADE")
    req = _engine_req("scd2_src", "scd2_dst", None, mode="strict")

    real = scd2_mod.verify_scd2_history

    def _delete_then_verify(*a, **kw):
        cur.execute("DELETE FROM scd2_dst WHERE id = '7' AND is_current")
        return real(*a, **kw)

    monkeypatch.setattr(
        scd2_mod, "verify_scd2_history", _delete_then_verify
    )
    res = _exec(fake, req)
    assert not res.success, (
        "deleting a row this run opened must fail the version recount"
    )
    recon = getattr(res, "reconciliation", None) or {}
    hist = recon.get("scd2_history") or {}
    assert "recount" in str(hist.get("violations") or res.error or "")
    conn.close()


def test_scd2_version_chunked_over_400_keys_pg(tmp_path, monkeypatch):
    """B1-E4: a recipe scd2 WITH a version column must chunk the
    max-version lookup — 401 and 1,200-key first loads plus a full
    soft-close + reopen (every key misses current and hits the chunked
    max-versions fetch) all run through execute_tracked."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(1200), table="scd2_e4_src")
    cur.execute("DROP TABLE IF EXISTS scd2_e4_dst CASCADE")
    recipe = _scd2_recipe()  # version column declared
    req = _engine_req("scd2_e4_src", "scd2_e4_dst", recipe, mode="strict")

    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute(
        "SELECT COUNT(*), MIN(ver), MAX(ver) FROM scd2_e4_dst WHERE cur_flag"
    )
    n, lo, hi = cur.fetchone()
    assert (n, lo, hi) == (1200, 1, 1)

    # Soft-close 500 keys (keep ids 0-699 unchanged), then reopen them —
    # each reopened key resolves max(version) over chunked predicates and
    # must land ver=2. 500 missing keys exercises >1 chunk.
    _mk_src_bulk(cur, _bulk_rows(700), table="scd2_e4_src")
    res = _exec(fake, req)
    assert res.success, res.error
    _mk_src_bulk(cur, _bulk_rows(1200), table="scd2_e4_src")
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE ver = 2), "
        "COUNT(*) FILTER (WHERE ver = 1) FROM scd2_e4_dst WHERE cur_flag"
    )
    n, v2, v1 = cur.fetchone()
    assert (n, v2, v1) == (1200, 500, 700)
    conn.close()


def test_scd2_type1_batched_at_volume_pg(tmp_path, monkeypatch):
    """B1-E4: 20k untracked-only changes take the Type-1 path — batched
    executemany, in place, no new versions — measured through the engine."""
    import time

    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(20000))
    cur.execute("DROP TABLE IF EXISTS scd2_dst CASCADE")
    recipe = _scd2_recipe(type1=True, tracked=["name"])
    req = _engine_req("scd2_src", "scd2_dst", recipe, mode="strict")
    res = _exec(fake, req)
    assert res.success, res.error

    changed = _bulk_rows(20000)
    for r in changed:
        r["note"] = f"u{r['id']}"  # untracked column only
    _mk_src_bulk(cur, changed)
    started = time.monotonic()
    res = _exec(fake, req)
    elapsed = time.monotonic() - started
    assert res.success, res.error
    # In place: still one version per key, note overwritten.
    cur.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE ver = 1), "
        "COUNT(*) FILTER (WHERE note LIKE 'u%') FROM scd2_dst"
    )
    n, v1, noted = cur.fetchone()
    assert (n, v1, noted) == (20000, 20000, 20000)
    print(f"\nType-1 20k untracked updates: {elapsed:.1f}s")
    conn.close()


def test_scd2_recount_ignores_overlapping_writer_pg(tmp_path, monkeypatch):
    """P3: a second writer's stamps inside the run window must not count
    as this run's work. Delete one row the run opened AND inject a
    foreign open inside the window — a window recount nets zero and
    masks the shortfall; the exact-stamp recount must still fail."""
    import services.scd2_engine as scd2_mod
    from datetime import datetime, timezone

    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(1500), table="scd2_ol_src")
    cur.execute("DROP TABLE IF EXISTS scd2_ol_dst CASCADE")
    req = _engine_req("scd2_ol_src", "scd2_ol_dst", None, mode="strict")

    real = scd2_mod.verify_scd2_history

    def _foreign_write_then_verify(*a, **kw):
        ts = datetime.now(timezone.utc)
        cur.execute(
            "DELETE FROM scd2_ol_dst WHERE id = '7' AND is_current"
        )
        # A foreign writer opens a brand-new current key inside the
        # window: window recount sees +1/-1 = expected, masking the
        # deletion. Exact-stamp membership excludes it.
        cur.execute(
            "INSERT INTO scd2_ol_dst (id, name, price, note,"
            " valid_from, valid_to, is_current, row_hash)"
            " VALUES (77777,'x','1.00','n',%s,NULL,TRUE,'fk')",
            (ts,),
        )
        return real(*a, **kw)

    monkeypatch.setattr(
        scd2_mod, "verify_scd2_history", _foreign_write_then_verify
    )
    res = _exec(fake, req)
    assert not res.success, (
        "a deleted run row masked by a foreign in-window open must "
        "still fail the recount"
    )
    recon = getattr(res, "reconciliation", None) or {}
    hist = recon.get("scd2_history") or {}
    assert "recount" in str(hist.get("violations") or res.error or "")
    conn.close()


def test_scd2_close_missing_false_retained_keys_pg(tmp_path, monkeypatch):
    """B1-E5: close_missing=False keeps deleted keys current — the
    expected-current count must include the retained keys, and the
    comparable digest covers only source-keyed rows."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_e5_src")
    cur.execute("DROP TABLE IF EXISTS scd2_e5_dst CASCADE")
    recipe = _scd2_recipe(close_missing=False)
    req = _engine_req("scd2_e5_src", "scd2_e5_dst", recipe, mode="strict")

    res = _exec(fake, req)
    assert res.success, res.error

    # Delete key 2 at source — close_missing=off retains it as current.
    cur.execute("DELETE FROM scd2_e5_src WHERE id = 2")
    res = _exec(fake, req)
    assert res.success, res.error
    cur.execute(
        "SELECT COUNT(*) FROM scd2_e5_dst WHERE cur_flag"
    )
    # 2 source keys + 1 retained deleted key = 3 current rows.
    assert cur.fetchone()[0] == 3
    recon = res.reconciliation or {}
    assert recon.get("passed") is True, recon.get("message")
    hist = (res.destination_summary or {}).get("retained_missing_rows")
    assert hist == 1, res.destination_summary
    ra = recon.get("row_accounting") or {}
    if ra:
        assert ra.get("balanced") is True, ra.get("note")
    conn.close()


def test_scd2_overlap_foreign_stamp_excluded_pg(tmp_path, monkeypatch):
    """B1-E7a: an overlapping writer's stamps inside this run's window
    are never counted — recount is by exact-stamp membership. A second
    execute_tracked run on different keys into the same destination
    recounts only its own work."""
    from datetime import timedelta

    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_ov_src")
    rows_b = [
        {"id": 11, "name": "P", "price": "11.00", "note": "np"},
        {"id": 12, "name": "Q", "price": "12.00", "note": "nq"},
    ]
    _mk_src(cur, rows_b, table="scd2_ov_src2")
    cur.execute("DROP TABLE IF EXISTS scd2_ov_dst CASCADE")
    recipe = _scd2_recipe()
    req = _engine_req("scd2_ov_src", "scd2_ov_dst", recipe, mode="strict")

    res_a = _exec(fake, req, prefix="scdoa")
    assert res_a.success, res_a.error
    ds_a = res_a.destination_summary or {}
    stamps_a = sorted(ds_a.get("run_stamps") or [])
    assert stamps_a
    cfg = _cfg(_endpoint("scd2_ov_dst"))

    # Simulate an overlapping writer: a foreign row whose valid_from sits
    # INSIDE run A's stamp window but is not one of run A's stamps — an
    # interleaved writer run B could stamp exactly this.
    from services.scd2_engine import _as_dt, verify_scd2_history

    hi = max(_as_dt(s) for s in stamps_a)
    foreign_stamp = (hi + timedelta(microseconds=1)).isoformat()
    # A row for a key run A never touched, stamped inside its window.
    cur.execute(
        "INSERT INTO scd2_ov_dst (id, name, price, note, ver, cur_flag,"
        " eff_from, eff_to, rh) VALUES (99, 'X', '9', 'nx', 1, true,"
        f" '{foreign_stamp}', NULL, 'fx')"
    )
    _o = recipe["steps"][0]["options"]
    cols = {
        "valid_from": _o["effective_from"],
        "valid_to": _o["end_date"],
        "is_current": _o["is_current"],
        "row_hash": _o["row_hash"],
        "version": _o["version"],
        "type1": _o.get("type1"),
        "close_missing": _o.get("close_missing", True),
    }
    hist = verify_scd2_history(
        cfg, "public", "scd2_ov_dst", ["id"], cols,
        expected={
            "inserted": 3,
            "closed": 0,
            "closed_missing": 0,
            "run_started_at": ds_a.get("run_started_at"),
            "run_stamps": stamps_a,
        },
    )
    # Membership recount counts only run A's stamps: 3 opened, not 4 —
    # and zero structural violations.
    assert hist["ok"], hist.get("violations")
    d = hist["deltas"]
    assert d["opened"] == 3 and d["run_stamp_count"] == len(stamps_a)
    assert d["run_ts_min"] and d["run_ts_max"]

    # A second run on different keys into the same destination recounts
    # only its own work — the overlapping row and run A's are ignored.
    req2 = _engine_req("scd2_ov_src2", "scd2_ov_dst", recipe, mode="strict")
    res_b = _exec(fake, req2, prefix="scdob")
    assert res_b.success, res_b.error
    hist_b = (res_b.reconciliation or {}).get("scd2_history") or {}
    db_ = hist_b.get("deltas") or {}
    assert db_.get("opened") == 2, hist_b
    assert db_.get("changed") == 0
    conn.close()


def test_scd2_proven_run_stamps_verified_verdict_pg(tmp_path, monkeypatch):
    """V-1: a fully proven run carries verification_verdict="verified"
    and its message leads with the structured verdict, never a bare
    'not migration_proven' contradiction under passed=true."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_v1_src")
    cur.execute("DROP TABLE IF EXISTS scd2_v1_dst CASCADE")
    recipe = _scd2_recipe()
    req = _engine_req("scd2_v1_src", "scd2_v1_dst", recipe, mode="strict")

    res = _exec(fake, req, prefix="scdv1")
    assert res.success, res.error
    recon = res.reconciliation or {}
    assert recon.get("passed") is True
    assert recon.get("verification_verdict") == "verified", recon
    msg = str(recon.get("message") or "")
    assert "not migration_proven" not in msg or "verified" in msg.lower()
    conn.close()


def test_scd2_untracked_prestep_applies_pg(tmp_path, monkeypatch):
    """B1-E6: a row-local step on an UNTRACKED column before the scd2
    step must actually transform the written rows — never accepted and
    silently skipped. case mode=upper on note lands N1, not n1."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src(cur, ROWS, table="scd2_e6_src")
    cur.execute("DROP TABLE IF EXISTS scd2_e6_dst CASCADE")
    recipe = {
        "steps": [
            {"op": "case", "column": "note", "options": {"mode": "upper"}},
            *_scd2_recipe()["steps"],
        ]
    }
    req = _engine_req("scd2_e6_src", "scd2_e6_dst", recipe, mode="strict")

    res = _exec(fake, req, prefix="scde6")
    assert res.success, res.error
    cur.execute("SELECT id, note FROM scd2_e6_dst ORDER BY id")
    notes = {r[0]: r[1] for r in cur.fetchall()}
    assert notes == {1: "N1", 2: "N2", 3: "N3"}, notes
    conn.close()


def test_scd2_close_missing_false_multi_source_pg(tmp_path, monkeypatch):
    """B1-E8: two sources consolidate into one SCD2 dimension with
    close_missing=false — the main reason the option exists. Source A
    keys 1..5000, source B keys 100001..105000 into the same destination:
    B's load must not fail Gate-8 on A's retained keys — the scoped
    digest and row count exclude keys the source never had, and foreign
    counts are reported."""
    fake = _engine_env(monkeypatch)
    conn, parsed = _pg()
    cur = conn.cursor()
    _mk_src_bulk(cur, _bulk_rows(5000), table="scd2_ms_srca")
    _mk_src_bulk(
        cur,
        [
            {
                "id": 100000 + i,
                "name": f"B{i}",
                "price": f"{i}.50",
                "note": f"bn{i}",
            }
            for i in range(1, 5001)
        ],
        table="scd2_ms_srcb",
    )
    cur.execute("DROP TABLE IF EXISTS scd2_ms_dst CASCADE")
    recipe = _scd2_recipe(close_missing=False)
    req_a = _engine_req("scd2_ms_srca", "scd2_ms_dst", recipe, mode="strict")
    res_a = _exec(fake, req_a, prefix="scmsa")
    assert res_a.success, res_a.error

    req_b = _engine_req("scd2_ms_srcb", "scd2_ms_dst", recipe, mode="strict")
    res_b = _exec(fake, req_b, prefix="scmsb")
    assert res_b.success, res_b.error
    recon_b = res_b.reconciliation or {}
    assert recon_b.get("passed") is True, recon_b.get("message")
    # A's 5000 keys are foreign to B's snapshot: reported, not digested —
    # the message names the source-keyed population and the retained count
    # separately, never a merged "10000 rows" (B1-E5a).
    msg_b = str(recon_b.get("message") or "")
    assert "5000 source-keyed rows" in msg_b, msg_b
    assert "5000 retained key(s)" in msg_b, msg_b
    ds_b = res_b.destination_summary or {}
    assert ds_b.get("retained_missing_rows") == 5000
    cur.execute("SELECT COUNT(*) FROM scd2_ms_dst WHERE cur_flag")
    assert cur.fetchone()[0] == 10000
    conn.close()


def test_scd2_small_change_run_dq_gate_pg(tmp_path, monkeypatch):
    """DQ gate: rate-based anomaly signals need a minimum sample.

    A 5-row SCD2 change batch where one name goes NULL is a 20% null-rate
    "shift" on pure noise — the gate must not fail the run. The same shift
    on a >=25-row run must still block under strict.
    """
    monkeypatch.setenv("QUALITY_PROFILES_DIR", str(tmp_path / "qprof"))
    fake = _engine_env(monkeypatch)
    conn, _ = _pg()
    cur = conn.cursor()

    rows5 = _bulk_rows(5)
    _mk_src(cur, rows5, table="scd2_dq_src")
    req = _engine_req("scd2_dq_src", "scd2_dq_dst", None, mode="strict")
    res1 = _exec(fake, req, prefix="scdq1")
    assert res1.success, res1.error

    # Change run: one tracked name -> NULL on a 5-row batch.
    cur.execute("UPDATE scd2_dq_src SET name=NULL WHERE id='2'")
    res2 = _exec(fake, req, prefix="scdq2")
    assert res2.success, res2.error
    assert not str(res2.error or "").startswith("Data quality anomaly")

    # Above the minimum sample the same shift must still block under strict.
    _mk_src_bulk(cur, _bulk_rows(30), table="scd2_dq_src2")
    req2 = _engine_req("scd2_dq_src2", "scd2_dq_dst2", None, mode="strict")
    res3 = _exec(fake, req2, prefix="scdq3")
    assert res3.success, res3.error
    cur.execute(
        "UPDATE scd2_dq_src2 SET name=NULL WHERE id::int % 3 = 0"
    )
    res4 = _exec(fake, req2, prefix="scdq4")
    assert not res4.success
    assert "null-rate" in str(res4.error or "")
    conn.close()
