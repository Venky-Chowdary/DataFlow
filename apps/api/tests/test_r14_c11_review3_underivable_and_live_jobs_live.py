"""C11, third review #5 and #6: the in-use checks fail closed and see every live job.

#5 — the underivable fallback failed open. A schedule whose slot names
cannot be derived was matched to slots by ``_<table>_`` in the name, which a
multi-table slot (``df_<db>_mt_<digest>_…``) or a name cut at 63 characters
never contains; and a CDC schedule whose source connector is gone was skipped
outright. Either way an unregistered look-alike of its capture was dropped
with ``include_unregistered``. Now, while any such schedule exists, no
unregistered slot or publication is dropped.

#6 — ``_live_jobs`` read the first 500 live jobs, and ``_job_holds`` only
compared ``source.table``, so a queued run past the 500th, or a multi-table
run whose tables are its stream contracts, did not hold its capture.

Live: logical PG (``R11_PG_LOGICAL_*``), the real app on MongoDB
(``r8_ent_harness``), cleanup through ``POST /api/v1/ops/cdc-captures/cleanup``.
"""

from __future__ import annotations

import uuid

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_r14_c11_review_capture_scope_live import (  # noqa: F401 - fixture
    _item,
    _logical,
    _pubs,
    _slots,
    _sql,
    world,
)

pytestmark = [
    pytest.mark.timeout(600),
    pytest.mark.skipif(not _logical(), reason="needs wal_level=logical (R11_PG_LOGICAL_*)"),
]


def _cdc_schedule(world, **fields) -> str:
    from services.schedule_store import create_schedule

    body = {
        "name": f"c11r3-{world.token}-{uuid.uuid4().hex[:4]}", "source_connector_id": world.x,
        "source_table": world.table, "dest_connector_id": world.dst,
        "dest_table": f"{world.table}_dst", "interval": "hourly", "sync_mode": "cdc",
        "enabled": False, "workspace_id": world.ws_a,
        "stream_contracts": [{"name": world.table, "selected": True, "primary_key": ["id"],
                              "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
    }
    body.update(fields)
    return create_schedule(body).id


def _underivable() -> list[dict]:
    from services.cdc_capture_inventory import UNDERIVABLE, schedule_slot_users

    return schedule_slot_users({}).get(UNDERIVABLE, [])


def test_r3_5_a_multi_table_underivable_schedule_blocks_unregistered_drops(world):
    other = f"{world.table}_two"
    _sql(f'CREATE TABLE "{other}" (id INT PRIMARY KEY)')
    try:
        sched = _cdc_schedule(
            world, dest_connector_id=f"gone-{world.token}",
            stream_contracts=[
                {"name": t, "selected": True, "primary_key": ["id"], "sync_mode": "cdc",
                 "cursor_semantics": "cdc_position"} for t in (world.table, other)
            ],
        )
        assert [u["schedule_id"] for u in _underivable()] == [sched], _underivable()
        # Its capture, as the multi-table naming spells it: no table in the name.
        digest = uuid.uuid4().hex[:10]
        slot = f"df_dataflow_mt_{digest}_{uuid.uuid4().hex[:8]}"
        pub = f"df_pub_dataflow_mt_{digest}"
        _sql("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
        _sql(f'CREATE PUBLICATION "{pub}" FOR TABLE "{world.table}", "{other}"')
        out = world.cleanup(world.ent.admin, world.x, slot_names=[slot], publication_names=[pub],
                            include_unregistered=True)
        assert out["dropped"] == [], out
        assert {r["action"] for r in out["results"]} == {"skipped"}, out
        assert all("cannot be derived" in r["reason"] for r in out["results"]), out
        assert slot in _slots() and pub in _pubs()
        listing = world.listing(world.ent.admin, world.x)
        assert _item(listing, slot)["unregistered_drop_blocked_by"] == 1, _item(listing, slot)
        assert "cannot name" in _item(listing, slot)["warning"]
    finally:
        _sql(f'DROP TABLE IF EXISTS "{other}" CASCADE')


def test_r3_5_a_schedule_whose_source_connector_is_gone_blocks_unregistered_drops(world):
    sched = _cdc_schedule(world, source_connector_id=f"deleted-{world.token}")
    assert [u["schedule_id"] for u in _underivable()] == [sched], _underivable()
    slot = f"df_dataflow_{world.table}_{uuid.uuid4().hex[:8]}"
    _sql("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    out = world.cleanup(world.ent.admin, world.x, slot_names=[slot], include_unregistered=True)
    assert out["dropped"] == [], out
    assert slot in _slots()
    # A registered capture on the same table may be that schedule's: in use.
    job = world.run_cdc()
    ours = world.ours() - {slot}
    assert len(ours) == 1
    item = _item(world.listing(world.a, world.x), next(iter(ours)))
    assert item["state"] == "in_use" and item["used_by_schedules"] == [sched], item
    assert world.cleanup(world.a, world.x)["dropped"] == []
    assert ours <= _slots(), job


def _route_row(world) -> dict:
    from services.cdc_capture_inventory import list_registered_captures

    world.run_cdc()
    slot = world.ours().pop()
    return [e for e in list_registered_captures() if e["slot_name"] == slot][0]


def test_r3_6_a_queued_run_past_the_500th_live_job_holds_its_capture(world):
    from services.mongodb_service import get_mongodb_service

    row = _route_row(world)
    jobs = get_mongodb_service().get_database()["transfer_jobs"]
    marker = f"noise-{world.token}"
    jobs.insert_many([
        {"_id": f"{marker}-{n}", "job_id": f"{marker}-{n}", "status": "pending", "name": marker}
        for n in range(600)
    ])
    try:
        jobs.insert_one({"_id": f"route-{world.token}", "job_id": f"route-{world.token}",
                         "status": "pending", "name": marker, "cursor_key": row["cursor_key"]})
        item = _item(world.listing(world.a, world.x), row["slot_name"])
        assert item["state"] == "running", item
        assert world.cleanup(world.a, world.x)["dropped"] == []
        assert row["slot_name"] in _slots()
    finally:
        jobs.delete_many({"name": marker})


def test_r3_6_a_multi_table_run_holds_its_capture_through_its_stream_contracts(world):
    from services.mongodb_service import get_mongodb_service

    row = _route_row(world)
    jobs = get_mongodb_service().get_database()["transfer_jobs"]
    jobs.insert_one({
        "_id": f"mt-{world.token}", "job_id": f"mt-{world.token}", "status": "running",
        "name": f"mt-{world.token}",
        "transfer_request": {
            "sync_mode": "cdc",
            "source": {"connector_id": world.x, "table": f"{world.table}_first"},
            "stream_contracts": [{"name": f"{world.table}_first", "selected": True},
                                 {"name": f"public.{world.table}", "selected": True}],
        },
    })
    try:
        item = _item(world.listing(world.a, world.x), row["slot_name"])
        assert item["state"] == "running", item
        assert world.cleanup(world.a, world.x)["dropped"] == []
        assert row["slot_name"] in _slots()
    finally:
        jobs.delete_many({"name": f"mt-{world.token}"})
