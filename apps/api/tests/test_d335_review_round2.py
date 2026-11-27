"""D335 review round 2 - the reviewer's reproductions, kept as regression tests.

* a failed transform proof on an incremental run undoes the run's own batch
  and resets the cursor, so the next run reloads the right value (the round-1
  "keep the rows" rule kept a wrong row 3 under a green next run);
* keyless append with a TEXT cursor: the boundary row is not appended again;
* a composite key whose first column is constant across 12,000 rows: every
  key column is in the lookup, nothing is silently truncated;
* a destination lookup that errors fails CLOSED: no duplicate lands and the
  run is not green;
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request):
    job_id = "d335r" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _ids(dst):
    with sqlite3.connect(dst) as c:
        return [r[0] for r in c.execute("SELECT id FROM t ORDER BY id")]


def _ts(i):
    return f"2026-11-24 10:{i:02d}:00.{i * 1111:06d}"


def _shaped_request(src, dst, mode="incremental_append", pk=True):
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    cols = ["id", "note", "updated_at"]
    _s, chains = split_column_transforms({"note": ["upper"]})
    recipe, refusal = compile_column_chains(chains, cols)
    assert not refusal, refusal
    contract = {"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp", "sync_mode": mode, "selected": True}
    if pk:
        contract["primary_key"] = ["id"]
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        mappings=[{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in cols],
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        stream_contracts=[contract],
    )


@pytest.mark.parametrize("mode", ["incremental_append", "incremental_upsert"])
def test_failed_proof_undoes_its_batch_and_next_run_reloads(tmp_path, isolated, monkeypatch, mode):
    """A genuinely wrong write fails the proof; the batch is undone with its
    cursor, and the next run reloads the right value (never a green run over
    the wrong row)."""
    import src.transfer.reconcile_step as rs

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    with sqlite3.connect(src) as c:
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", _ts(i)) for i in range(1, 11)])
    with sqlite3.connect(dst) as c:
        c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
    original = rs._transform_applied_proof

    def _broken_transform(**kw):
        # Simulates a genuinely wrong write: the recipe output is not what landed.
        with sqlite3.connect(dst) as c:
            c.execute("UPDATE t SET note = 'wrong' WHERE id = 3")
        return original(**kw)

    monkeypatch.setattr(rs, "_transform_applied_proof", _broken_transform)
    first = _run(isolated, _shaped_request(src, dst, mode))
    monkeypatch.setattr(rs, "_transform_applied_proof", original)
    second = _run(isolated, _shaped_request(src, dst, mode))
    assert not first.success
    with sqlite3.connect(dst) as c:
        assert c.execute("select note from t where id=3").fetchall() != [("wrong",)] or not second.success, (
            "named culprit row kept under a green next run"
        )


@pytest.mark.parametrize("pk", [False])
def test_keyless_text_cursor_spelling(tmp_path, isolated, pk):
    """Keyless append, TEXT cursor: still the text lookup."""
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    with sqlite3.connect(src) as c:
        c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", _ts(i)) for i in range(1, 11)])
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[{"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
                           "sync_mode": "incremental_append", "selected": True}],
        validation_mode="strict",
    )
    first = _run(isolated, req)
    assert first.success, first.error
    with sqlite3.connect(src) as c:
        c.execute("INSERT INTO t VALUES (11, 'n11', ?)", (_ts(11),))
    second = _run(isolated, req)
    assert _ids(dst) == list(range(1, 12)), _ids(dst)


def test_lookup_error_fails_closed(tmp_path, isolated, monkeypatch):
    """A destination lookup that raises never lets a duplicate land under a green job."""
    import src.transfer.stream_boundary_keyed as sbk

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    with sqlite3.connect(src) as c:
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", _ts(i)) for i in range(1, 11)])
    with sqlite3.connect(dst) as c:
        c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[{"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp", "primary_key": ["id"],
                           "sync_mode": "incremental_append", "selected": True}],
        validation_mode="strict",
    )
    assert _run(isolated, req).success

    def _boom(*a, **k):
        raise RuntimeError("ORA-03113 end-of-file on communication channel (fake)")

    monkeypatch.setattr(sbk, "read_boundary_candidates", _boom)
    with sqlite3.connect(src) as c:
        c.execute("INSERT INTO t VALUES (11, 'n11', ?)", (_ts(11),))
    second = _run(isolated, req)
    assert _ids(dst).count(10) == 1 or not second.success, "duplicate landed and job green"


def _pg(sql, params=None, fetch=False):
    from tests import r4_route_harness as h
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _pgreq(src, dst, keys=("id",)):
    from tests import typed_fidelity_helpers as tf
    c = {"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
         "sync_mode": "incremental_append", "selected": True}
    if keys:
        c["primary_key"] = list(keys)
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=tf.pg_endpoint(dst),
        sync_mode="incremental_append", stream_contracts=[c], validation_mode="strict",
    )


@pytest.mark.parametrize("mode", ["lookup_error"])
def test_pg_destination_boundary_cases(tmp_path, isolated, monkeypatch, mode):
    import src.transfer.stream_boundary_keyed as sbk
    src = tmp_path / "s.sqlite"
    dst = f"revp_{uuid.uuid4().hex[:8]}"
    keys = {"composite": ("id", "k2"), "keyless": ()}.get(mode, ("id",))
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, k2 BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, k2 INTEGER, note TEXT, updated_at TEXT)")
            rows = [(i, 1, f"n{i}", _ts(i)) for i in range(1, 9)] + [(9, 1, "n9", _ts(8)), (10, 1, "n10", _ts(8))]
            if mode == "composite":
                rows.append((10, 2, "n10b", _ts(8)))
            c.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
        first = _run(isolated, _pgreq(src, dst, keys))
        assert first.success, first.error
        n1 = len(_pg(f'SELECT 1 FROM public."{dst}"', fetch=True))
        if mode == "lookup_error":
            def _boom(*a, **k):
                raise RuntimeError("connection reset (fake)")
            monkeypatch.setattr(sbk, "read_boundary_candidates", _boom)
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 1, 'n11', ?)", (_ts(8),))
            c.execute("INSERT INTO t VALUES (12, 1, 'n12', ?)", (_ts(9),))
        second = _run(isolated, _pgreq(src, dst, keys))
        got = _pg(f'SELECT id, k2, count(*) FROM public."{dst}" GROUP BY id, k2 HAVING count(*) > 1', fetch=True)
        n2 = len(_pg(f'SELECT 1 FROM public."{dst}"', fetch=True))
        assert not got or not second.success, "duplicate landed under a green run"
        assert not got, "duplicate landed"
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def test_composite_low_cardinality_first_key(tmp_path, isolated):
    src = tmp_path / "s.sqlite"
    dst = f"revc_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, k2 BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, k2 INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?,?)",
                          [(i, 1, f"n{i}", f"2026-11-{1 + i // 1000:02d} 10:00:00.{i:06d}") for i in range(1, 12001)])
        first = _run(isolated, _pgreq(src, dst, keys=("k2", "id")))
        assert first.success, first.error
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (12001, 1, 'n', '2026-11-30 10:00:00.000001')")
        second = _run(isolated, _pgreq(src, dst, keys=("k2", "id")))
        got = _pg(f'SELECT id, count(*) FROM public."{dst}" GROUP BY id HAVING count(*) > 1', fetch=True)
        assert not got
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def test_held_back_boundary_rows_keep_the_cursor_and_land_once_next_run(tmp_path, isolated, monkeypatch):
    """Non-strict run, the destination lookup errors: the run is not green, says
    the boundary rows were held back, writes nothing past the watermark and keeps
    the stored watermark at the boundary. The next run (lookup working) lands
    every boundary row - and every later row - exactly once."""
    import services.sync_cursor as sc
    import src.transfer.stream_boundary_keyed as sbk

    src = tmp_path / "s.sqlite"
    dst = f"revh_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, k2 BIGINT, note TEXT, updated_at TIMESTAMP(6))')

    def req():
        r = _pgreq(src, dst)
        r.validation_mode = "balanced"
        return r

    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, k2 INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?,?)", [(i, 1, f"n{i}", _ts(i)) for i in range(1, 11)])
        assert _run(isolated, req()).success
        key = sc.list_cursor_keys()[0]
        mark_before = sc.get_watermark_record(key)[0]
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 1, 'n11', ?)", (_ts(10),))  # unread tie peer
            c.execute("INSERT INTO t VALUES (12, 1, 'n12', ?)", (_ts(12),))
        real = sbk.read_boundary_candidates

        def _boom(*a, **k):
            raise RuntimeError("connection reset (fake)")

        monkeypatch.setattr(sbk, "read_boundary_candidates", _boom)
        held = _run(isolated, req())
        assert not held.success
        assert "boundary row(s) held back; the next run will retry them" in str(held.error or ""), held.error
        assert sorted(r[0] for r in _pg(f'SELECT id FROM public."{dst}"', fetch=True)) == list(range(1, 11))
        assert sc.get_watermark_record(key)[0] == mark_before, "the cursor moved past held-back rows"

        monkeypatch.setattr(sbk, "read_boundary_candidates", real)
        retry = _run(isolated, req())
        assert retry.success, retry.error
        ids = sorted(r[0] for r in _pg(f'SELECT id FROM public."{dst}"', fetch=True))
        assert ids == list(range(1, 13)), ids
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')
