"""QA demo (557f556c): schema drift under the manual-review policy.

1. The loop. A new source column held Validate with "open Map to include the
   changed columns". Map included it (an approved create-new mapping), and
   Validate held again; only switching the policy to Propagate escaped. The
   drift gate re-derived the ADD COLUMN from the live table and never read the
   approval Map wrote (``user_override`` on the mapping).
2. The silent rename. ``amount`` renamed to ``total_amount`` at the source was
   auto-matched onto the existing ``amount`` column at 82% by semantic mapping
   and nothing called it drift.

Driven through the REST calls Studio makes (tests/r7_studio_harness.py:
introspect -> /transfer/map -> plan PATCH + /mappings + /preflight ->
/transfer/run), PG -> PG, ``schema_policy=manual_review``, balanced mode (whose
confidence floor an 82% match clears). Each run appends a batch with NULL, '',
whitespace and unicode into the table run 1 created.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.r7_studio_harness import Studio, blockers, preflight_mappings

pytestmark = pytest.mark.timeout(600)

_POLICIES = {
    "sync_mode": "full_refresh_append",
    "schema_policy": "manual_review",
    "validation_mode": "balanced",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import services.transfer_plan_store as plan_store

    monkeypatch.setattr(plan_store, "STORE_PATH", tmp_path / "transfer_plans.json")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _lit(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def _studio(client, token: str):
    from services.connector_store import create_connector

    st, dt = f"r7sd_s_{token}", f"r7sd_d_{token}"
    h.execute("postgresql", f'CREATE TABLE "{st}" (id INT PRIMARY KEY, amount NUMERIC(10,2), note TEXT)')
    h.execute("postgresql", f"INSERT INTO \"{st}\" VALUES (1, 1.50, 'a'), (2, NULL, ''), (3, 3.00, 'Zoë ✓')")
    src = create_connector({"name": f"r7sd-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"r7sd-d-{token}", "role": "destination", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    studio = Studio(
        client,
        source={"kind": "database", "format": "postgresql", "connector_id": src.id, "table": st},
        destination={"kind": "database", "format": "postgresql", "connector_id": dst.id, "table": dt},
        policies=_POLICIES,
    )
    return studio, st, dt


def _run_one(studio: Studio) -> None:
    si, _di = studio.introspect()
    sc, ss = studio.columns(si)
    maps = preflight_mappings(studio.map(sc, ss, [], {}, table_exists=False))
    pf = studio.validate(sc, ss, [], {}, maps)
    assert pf.get("passed"), blockers(pf)
    job = studio.execute(maps)
    assert job.get("status") == "completed", job.get("error") or job.get("message")


def _completed(job: dict) -> None:
    assert job.get("status") == "completed", job.get("error") or job.get("message")


def test_rename_is_drift_needing_approval_and_map_approval_clears_it(client):
    token = uuid.uuid4().hex[:8]
    studio, st, dt = _studio(client, token)
    try:
        _run_one(studio)
        h.execute("postgresql", f'ALTER TABLE "{st}" RENAME COLUMN amount TO total_amount')
        h.execute("postgresql", f'DELETE FROM "{st}"')
        batch = [(4, 4.25, None), (5, None, "   "), (6, 6.00, "日本語 🎉")]
        for rid, amt, note in batch:
            h.execute("postgresql", f'INSERT INTO "{st}" VALUES ({rid}, {_lit(amt)}, {_lit(note)})')  # nosec B608

        si, di = studio.introspect()
        sc, ss = studio.columns(si)
        dc, ds = studio.columns(di)
        assert di.get("table_exists") is True
        raw = studio.map(sc, ss, dc, ds, table_exists=True)
        rename = [m for m in raw if m.get("source") == "total_amount"]
        assert rename and rename[0].get("target") == "amount", raw
        # Auto-match only suggests a rename; it is never carried as matched.
        assert float(rename[0].get("confidence") or 0) < 1.0
        assert rename[0].get("requires_review") is True, rename[0]
        assert rename[0].get("review_kind") == "rename", rename[0]

        pf = studio.validate(sc, ss, dc, ds, preflight_mappings(raw, dest_columns=dc))
        drift = [b for b in blockers(pf) if b[0] == "schema_drift"]
        assert drift, blockers(pf)
        assert "amount" in drift[0][1] and "total_amount" in drift[0][1], drift

        # Approve the rename in Map: the manual-review block clears.
        approved = preflight_mappings(raw, approve=True, dest_columns=dc)
        pf = studio.validate(sc, ss, dc, ds, approved)
        assert pf.get("passed"), blockers(pf)
        _completed(studio.execute(approved))
        rows = h.query("postgresql", f'SELECT id, amount, note FROM "{dt}" ORDER BY id')  # nosec B608
        got = [(int(r[0]), None if r[1] is None else float(r[1]), r[2]) for r in rows]
        assert got == [(1, 1.5, "a"), (2, None, ""), (3, 3.0, "Zoë ✓"),
                       (4, 4.25, None), (5, None, "   "), (6, 6.0, "日本語 🎉")], got
    finally:
        h.drop_table("postgresql", st)
        h.drop_table("postgresql", dt)


def test_new_column_approved_in_map_clears_the_manual_review_block(client):
    token = uuid.uuid4().hex[:8]
    studio, st, dt = _studio(client, token)
    try:
        _run_one(studio)
        h.execute("postgresql", f'ALTER TABLE "{st}" ADD COLUMN region TEXT')
        h.execute("postgresql", f'DELETE FROM "{st}"')
        batch = [(7, 7.00, "x", None), (8, None, None, ""), (9, 9.10, "y", "  "), (10, 1.00, "z", "Ünïcødé")]
        for rid, amt, note, region in batch:
            h.execute("postgresql", f'INSERT INTO "{st}" VALUES ({rid}, {_lit(amt)}, {_lit(note)}, {_lit(region)})')  # nosec B608

        si, di = studio.introspect()
        sc, ss = studio.columns(si)
        dc, ds = studio.columns(di)
        raw = studio.map(sc, ss, dc, ds, table_exists=True)
        assert any(m.get("source") == "region" and m.get("target") == "region" for m in raw), raw

        pf = studio.validate(sc, ss, dc, ds, preflight_mappings(raw, dest_columns=dc))
        assert any(b[0] == "schema_drift" for b in blockers(pf)), blockers(pf)

        # "Open Map" -> approve the new column. Validate must not loop.
        approved = preflight_mappings(raw, approve=True, dest_columns=dc)
        pf = studio.validate(sc, ss, dc, ds, approved)
        assert pf.get("passed"), blockers(pf)
        _completed(studio.execute(approved))
        rows = h.query("postgresql", f'SELECT id, region FROM "{dt}" WHERE id >= 7 ORDER BY id')  # nosec B608
        assert [(int(r[0]), r[1]) for r in rows] == [
            (7, None), (8, ""), (9, "  "), (10, "Ünïcødé")
        ], rows
        assert len(h.query("postgresql", f'SELECT id FROM "{dt}"')) == 7  # nosec B608
    finally:
        h.drop_table("postgresql", st)
        h.drop_table("postgresql", dt)
