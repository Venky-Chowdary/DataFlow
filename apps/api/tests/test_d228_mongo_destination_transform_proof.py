"""D228: a shaped run into MongoDB must prove its recipe, not stay green unproven.

QA on 087ac4bf: PG -> Mongo with a ``case upper`` recipe ended green while the
transform proof was ``unverifiable`` — the proof read the destination through
SQLAlchemy, which has no ``mongodb`` dialect. A dropped recipe step on a Mongo
destination therefore could never fail. The proof now reads the written
collection back through the Mongo reader (the same projection as a Mongo
source re-read): a correct run is ``verified``; a dropped step fails, naming it.

Live PostgreSQL (``PG*``) + MongoDB (``MONGO_URI``); skipped when absent.
"""
from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.typed_fidelity_helpers import pg_dsn_parts, pg_endpoint
from tests.test_x5_same_run_artifact_readback import MONGO_URI

RECIPE = {"steps": [{"op": "case", "column": "name", "options": {"mode": "upper"}}]}


def _live():
    try:
        import psycopg2
        import pymongo

        p = pg_dsn_parts()
        conn = psycopg2.connect(
            host=p["host"], port=p["port"], dbname=p["database"],
            user=p["username"], password=p["password"], connect_timeout=3,
        )
        conn.autocommit = True
        mc = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        mc.admin.command("ping")
        return conn, mc
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live PostgreSQL + MongoDB required: {exc}")


@pytest.fixture
def route(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    conn, mc = _live()
    tok = uuid.uuid4().hex[:8]
    src_t, coll = f"d228_src_{tok}", f"d228_dst_{tok}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{src_t}" (id INT PRIMARY KEY, name TEXT)')
        cur.executemany(
            f'INSERT INTO "{src_t}" VALUES (%s, %s)',
            [(1, "bob"), (2, "zoë"), (3, None)],
        )
    yield src_t, coll, mc["d228"][coll]
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{src_t}"')
    conn.close()
    mc["d228"][coll].drop()
    mc.close()


def _run(src_t: str, coll: str):
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=pg_endpoint(src_t),
        destination=EndpointConfig(
            kind="database", format="mongodb", connection_string=MONGO_URI,
            database="d228", table=coll,
        ),
        mappings=[
            {"source": c, "target": c, "approved": True, "confidence": 0.99}
            for c in ("id", "name")
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=RECIPE,
        approved_shape_recipe_hash=ShapeRecipe.parse(RECIPE).recipe_hash,
    )
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def test_shaped_run_into_mongo_proves_the_recipe(route):
    src_t, coll, mcoll = route
    res = _run(src_t, coll)
    assert res.success, res.error
    proof = (res.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert proof.get("rows") == 3, proof
    docs = sorted(mcoll.find({}, {"_id": 0}), key=lambda d: d["id"])
    assert [d.get("name") for d in docs] == ["BOB", "ZOË", None]


def test_dropped_step_into_mongo_fails_naming_the_step(route, monkeypatch):
    from services import shape_apply as shape_apply_mod

    # The runner silently no-ops: raw rows reach the collection.
    monkeypatch.setattr(
        shape_apply_mod.ShapeRunner, "matrix", lambda self, headers, rows: (headers, rows)
    )
    src_t, coll, mcoll = route
    res = _run(src_t, coll)
    recon = res.reconciliation or {}
    proof = recon.get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    assert any("case" in s for s in (proof.get("culprit_steps") or [])), proof
    assert not res.success, "an unapplied recipe must not end green"
