"""D325 — a pipeline next to a single-step transform must not fail a correct job.

MCP ``column_transforms={"name": ["trim", "upper"], "status": "replace:active:ACT",
"note": "default:NA"}`` compiles the list into the shape recipe and stamps each
single step on its mapping as a Map transform. The writer runs both: the recipe
on the read, then each mapping's transform. The transform-applied proof
recomputed the recipe only, so every correct Map-transformed cell (``ACT``,
``NA`` on '' and NULL) read as "Destination drift on column status, note is not
explained by any recipe step" — the job failed at 99% and an incremental run
dropped the table it created.

Real runs through the engine (SQLite in-process; PostgreSQL → MySQL live when
the engines are configured). A destination that does not hold the Map output
still fails the proof.
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

_COLS = ["id", "name", "status", "note"]
_ROWS = [
    (1, "  ada ", "active", "x"),
    (2, "bob", "inactive", ""),
    (3, None, "active", None),
    (4, "Zoë", "active", "y"),
]
_WANT = [
    (1, "ADA", "ACT", "x"),
    (2, "BOB", "inACT", "NA"),
    (3, None, "ACT", "NA"),
    (4, "ZOË", "ACT", "y"),
]


def _mixed_request(source, dest, mode: str) -> TransferRequest:
    """What MCP start_transfer stages for the mixed column_transforms."""
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    singles, chains = split_column_transforms(
        {"name": ["trim", "upper"], "status": "replace:active:ACT", "note": "default:NA"}
    )
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    mappings = []
    for c in _COLS:
        m = {"source": c, "target": c, "approved": True, "confidence": 0.99}
        if c in singles:
            m.update(transform=singles[c], transform_override=True)
        mappings.append(m)
    extra = {}
    if mode != "full_refresh_overwrite":
        extra["stream_contracts"] = [
            {"name": dest.table, "primary_key": ["id"], "sync_mode": mode, "selected": True}
        ]
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=mappings,
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        **extra,
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
    job_id = "d325" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite_pair(tmp_path: Path) -> tuple[Path, Path]:
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, status TEXT, note TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", _ROWS)
    conn.close()
    return src, dst


def _sqlite_ep(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="t")


def _dest_rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id, name, status, note FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "mode", ["full_refresh_overwrite", "incremental_upsert", "incremental_append"]
)
def test_pipeline_plus_single_step_transforms_verify_and_keep_the_table(
    tmp_path, fake_jobs, mode
):
    src, dst = _sqlite_pair(tmp_path)
    result = _run(fake_jobs, _mixed_request(_sqlite_ep(src), _sqlite_ep(dst), mode))

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert "not explained by any recipe step" not in str(result.error or "")
    # The table this run created holds exactly the recipe + Map output.
    assert _dest_rows(dst) == _WANT


def test_a_destination_without_the_map_output_still_fails(tmp_path, fake_jobs, monkeypatch):
    """Not weakened: a row whose Map transform did not land fails the proof."""
    import src.transfer.reconcile_step as rs

    src, dst = _sqlite_pair(tmp_path)
    original = rs._transform_applied_proof

    def _tamper_then_prove(**kwargs):
        conn = sqlite3.connect(str(dst))
        with conn:
            conn.execute("UPDATE t SET status = 'active' WHERE id = 1")
        conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _tamper_then_prove)
    result = _run(
        fake_jobs, _mixed_request(_sqlite_ep(src), _sqlite_ep(dst), "full_refresh_overwrite")
    )

    assert not result.success
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert proof.get("culprit_columns") == ["status"], proof
    assert (proof.get("culprit_keys") or {}).get("changed") == ["1"], proof


def test_pg_to_mysql_mixed_recipe_with_default_on_blank_verifies(fake_jobs):
    """The QA pair: PG → MySQL, pipeline + replace + default:NA on '' and NULL."""
    from tests import r4_route_harness as h
    from tests.typed_fidelity_helpers import mysql_endpoint, pg_endpoint

    h.dest_config("mysql")  # skips when MySQL is absent
    src = f"d325_src_{uuid.uuid4().hex[:8]}"
    dst = f"d325_dst_{uuid.uuid4().hex[:8]}"
    conn = h.pg_conn()  # skips when PostgreSQL is absent
    try:
        cur = conn.cursor()
        cur.execute(
            f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name varchar(40), '  # nosec B608
            "status varchar(20), note varchar(20))"
        )
        cur.executemany(f'INSERT INTO public."{src}" VALUES (%s, %s, %s, %s)', _ROWS)  # nosec B608
    finally:
        conn.close()
    try:
        result = _run(
            fake_jobs,
            _mixed_request(pg_endpoint(src), mysql_endpoint(dst), "full_refresh_overwrite"),
        )
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        got = h.query("mysql", f"SELECT id, name, status, note FROM `{dst}` ORDER BY id")  # nosec B608
        assert [tuple(r) for r in got] == _WANT
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("mysql", dst)
