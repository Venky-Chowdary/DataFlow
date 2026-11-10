"""D331 — no "N of M keys changed in source during run" on a source nobody touched.

The transform proof pins every key the write read (hash of the raw row as the
reader put it on the wire) and re-reads the source afterwards; a key whose
re-read hash differs is "drift", never blamed. The re-read side hashed driver
values with its own normalization, so a value the reader carries as text hashed
differently from the same untouched value read back: ``Decimal('1.500')`` vs
wire ``'1.500'`` (unconstrained NUMERIC), PG ``inet`` (the reader's ``::text``
keeps ``/32``, the driver does not). Any digest difference then reported every such row as "changed
in source during run" and the proof was skipped — including rows whose
destination held the exact recipe output.

The fix hashes the re-read through the write ledger's own chain
(``cell_to_string`` → ``_pin_norm``), re-reads PostgreSQL through the reader's
text-carry projection, and never counts a key whose destination row equals the
recipe output as changed.
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

pytestmark = pytest.mark.timeout(600)

_TRANSFORMS = {"name": ["normalize_unicode", "lower"], "note": "replace:note:memo"}


def _request(source, dest, cols: list[str], mode: str = "upsert") -> TransferRequest:
    """What MCP start_transfer stages for the QA recipe (D331 repro 1)."""
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    singles, chains = split_column_transforms(_TRANSFORMS)
    recipe, refusal = compile_column_chains(chains, cols)
    assert not refusal, refusal
    mappings = []
    for c in cols:
        m = {"source": c, "target": c, "approved": True, "confidence": 0.99}
        if c in singles:
            m.update(transform=singles[c], transform_override=True)
        mappings.append(m)
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=mappings,
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        stream_contracts=[
            {"name": dest.table, "primary_key": ["id"], "sync_mode": mode, "selected": True}
        ],
    )


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d331" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _tamper_before_proof(monkeypatch, tamper) -> None:
    """A real defect on one key, landed between the write and the proof."""
    import src.transfer.reconcile_step as rs

    original = rs._transform_applied_proof

    def _wrapped(**kwargs):
        tamper()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _wrapped)


# --------------------------------------------------------------------------- #
# The change detector compares raw to raw — never post-transform values.
# --------------------------------------------------------------------------- #


def test_detector_pins_raw_rows_on_a_frozen_source(tmp_path, fake_jobs, monkeypatch):
    """lower + replace change every row; a frozen source pins every key, so
    the one wrong destination row fails by key instead of reading as drift."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, f"Name {i} ZOË", f"note {i}") for i in range(1, 30)] + [(30, None, None)],
        )
    conn.close()

    def _tamper():
        c = sqlite3.connect(str(dst))
        with c:
            c.execute("UPDATE t SET note = 'wrong' WHERE id = 7")
        c.close()

    _tamper_before_proof(monkeypatch, _tamper)
    result = _run(
        fake_jobs,
        _request(
            EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            ["id", "name", "note"],
        ),
    )
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert proof.get("culprit_keys") == {"changed": ["7"]}, proof
    assert proof.get("culprit_columns") == ["note"], proof


def test_a_key_holding_the_recipe_output_is_never_counted_as_changed():
    """drift_stats counts only unpinned keys whose destination row is wrong."""
    from src.transfer.proof_scratch import proof_scratch_scope
    from src.transfer.reconcile_step import _PinStore

    with proof_scratch_scope():
        store = _PinStore()
        try:
            # 3 keys, none pinned (the re-read renders a value another way).
            store.put("pin", [("1", "w1"), ("2", "w2"), ("3", "w3")])
            store.put("rawh", [("1", "r1"), ("2", "r2"), ("3", "r3")])
            store.put("exp", [("1", "fpA", "{}"), ("2", "fpB", "{}"), ("3", "fpC", "{}")])
            # Keys 1 and 2 hold the recipe output; key 3 does not.
            store.put("destk", [("1", "fpA", "{}"), ("2", "fpB", "{}"), ("3", "fpX", "{}")])
            store.flush()
            assert store.drift_stats() == (1, 3, 0)
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# Live PostgreSQL source: values the reader carries in another form.
# --------------------------------------------------------------------------- #

_PG_ROWS = [
    (1, "  Ada ", "note one", "1.500", "10.0.0.1"),
    (2, "BOB", "my note", "2.250", "10.0.0.2"),
    (3, "Zoë", None, "3.000", "10.0.0.3"),
    (4, None, "note", None, None),
]
_PG_COLS = ["id", "name", "note", "amount", "ip"]


def _seed_pg_frozen() -> str:
    from tests import r4_route_harness as h

    table = f"d331_src_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()  # skips when PostgreSQL is absent
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{table}" (id integer PRIMARY KEY, name varchar(40), '  # nosec B608
            "note varchar(40), amount numeric, ip inet)"
        )
        cur.executemany(
            f'INSERT INTO public."{table}" VALUES (%s, %s, %s, %s, %s)',  # nosec B608
            _PG_ROWS,
        )
    finally:
        conn.close()
    return table


def test_pg_frozen_source_one_wrong_row_fails_by_key_not_as_drift(fake_jobs, monkeypatch):
    from tests import r4_route_harness as h
    from tests.typed_fidelity_helpers import mysql_endpoint, pg_endpoint

    h.dest_config("mysql")  # skips when MySQL is absent
    src = _seed_pg_frozen()
    dst = f"d331_dst_{uuid.uuid4().hex[:8]}"
    _tamper_before_proof(
        monkeypatch,
        lambda: h.execute("mysql", f"UPDATE `{dst}` SET `note` = 'wrong' WHERE `id` = 2"),  # nosec B608
    )
    try:
        result = _run(fake_jobs, _request(pg_endpoint(src), mysql_endpoint(dst), _PG_COLS))
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        reason = str(proof.get("reason") or "")
        assert "keys changed in source during run" not in reason, proof
        assert proof.get("status") == "failed", proof
        assert proof.get("culprit_keys") == {"changed": ["2"]}, proof
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("mysql", dst)


def test_pg_frozen_source_correct_destination_verifies(fake_jobs):
    from tests import r4_route_harness as h
    from tests.typed_fidelity_helpers import mysql_endpoint, pg_endpoint

    h.dest_config("mysql")
    src = _seed_pg_frozen()
    dst = f"d331_dst_{uuid.uuid4().hex[:8]}"
    try:
        result = _run(fake_jobs, _request(pg_endpoint(src), mysql_endpoint(dst), _PG_COLS))
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        got = h.query("mysql", f"SELECT id, name, note FROM `{dst}` ORDER BY id")  # nosec B608
        assert [tuple(r) for r in got] == [
            # normalize_unicode in a pipeline is NFKC + whitespace collapse (D332).
            (1, "ada", "memo one"),
            (2, "bob", "my memo"),
            (3, "zoë", None),
            (4, None, "memo"),
        ]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("mysql", dst)
