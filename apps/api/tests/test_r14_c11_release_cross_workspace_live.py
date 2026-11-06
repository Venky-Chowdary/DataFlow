"""C11 follow-up: releasing one workspace's CDC capture never touches another's.

The CDC cursor key was built from database, table and destination names only
(``build_cursor_key``: no host, no connector), and ``route_still_in_use`` only
looked at schedules of the same workspace. Two workspaces with the same names
therefore shared a watermark (and the PostgreSQL slot derived from it): a
schedule delete — or a workspace teardown, which calls the same release — in
one workspace cleared the other's watermark and, on a shared source server,
dropped its inactive slot.

Now the release uses the cross-workspace in-use rule: any other CDC schedule
(any workspace) that uses the same capture keeps it, and a slot another
workspace recorded, or a run holds, is never dropped. Live: logical PG (``R11_PG_LOGICAL_*``), the
real app on MongoDB (``r8_ent_harness``).
"""

from __future__ import annotations

import uuid

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_r14_c11_review_capture_scope_live import World, _logical, _slots

pytestmark = [
    pytest.mark.timeout(600),
    pytest.mark.skipif(not _logical(), reason="needs wal_level=logical (R11_PG_LOGICAL_*)"),
]


@pytest.fixture
def world(ent, monkeypatch):
    w = World(ent, monkeypatch)
    w.dst_b = w._connector(w.b, "dstb", __import__("tests.r4_route_harness", fromlist=["x"]).dest_config("postgresql"))
    try:
        yield w
    finally:
        w.close()


def _run(world, src: str, dst: str, ws: str) -> str:
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public",
                              table=world.table, connector_id=src),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=f"{world.table}_dst", connector_id=dst),
        sync_mode="cdc",
        stream_contracts=[{"name": world.table, "selected": True, "primary_key": ["id"],
                           "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
        skip_preflight=True, validation_mode="strict", workspace_id=ws,
    )
    job_id = "r14c11x" + uuid.uuid4().hex[:12]
    get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return job_id


def _schedule(world, src: str, dst: str, ws: str, job_id: str) -> str:
    from services.schedule_store import create_schedule

    return create_schedule({
        "name": f"c11x-{ws[:6]}-{world.token}", "source_connector_id": src,
        "source_table": world.table, "dest_connector_id": dst,
        "dest_table": f"{world.table}_dst", "interval": "hourly", "sync_mode": "cdc",
        "enabled": False, "last_job_id": job_id, "workspace_id": ws,
        "stream_contracts": [{"name": world.table, "selected": True, "primary_key": ["id"],
                              "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
    }).id


def test_a_slot_another_workspace_recorded_is_never_released(world):
    """A's schedule derives a slot name another workspace's run recorded: the
    per-slot guard keeps it (and its watermark)."""
    from services.cdc_capture_inventory import _coll
    from services.cdc_capture_release import release_schedule_cdc_capture
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import get_schedule
    from services.sync_cursor import get_watermark

    job_a = _run(world, world.x, world.dst, world.ws_a)
    slot = world.ours().pop()
    key = get_mongodb_service().get_job(job_a)["cursor_key"]
    _coll("cdc_capture_registry").update_many({"slot_name": slot}, {"$set": {"workspace_id": world.ws_b}})
    sched_a = _schedule(world, world.x, world.dst, world.ws_a, job_a)
    out = release_schedule_cdc_capture(get_schedule(sched_a))
    kept = [r for r in out.get("slots", []) if r["slot_name"] == slot]
    assert kept and kept[0]["slot"] == "kept", out
    assert slot in _slots() and get_watermark(key) is not None


def test_a_capture_two_workspaces_shared_before_the_upgrade_is_kept(world):
    """Historical key K shared by both workspaces' jobs: releasing A must not
    clear K nor drop its slot while B still uses it."""
    from services.cdc_capture_release import release_schedule_cdc_capture, route_still_in_use
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import get_schedule
    from services.sync_cursor import get_watermark

    job_a = _run(world, world.x, world.dst, world.ws_a)
    slot = world.ours().pop()
    svc = get_mongodb_service()
    key = svc.get_job(job_a)["cursor_key"]
    job_b = "r14c11xb" + uuid.uuid4().hex[:8]
    svc.update_job_status(job_b, "completed", cursor_key=key, transfer_request={})
    sched_a = _schedule(world, world.x, world.dst, world.ws_a, job_a)
    _schedule(world, world.y_other_ws, world.dst_b, world.ws_b, job_b)
    assert route_still_in_use(get_schedule(sched_a)) is True
    out = release_schedule_cdc_capture(get_schedule(sched_a))
    assert out["released"] is False and out["reason"] == "route_shared", out
    assert slot in _slots() and get_watermark(key) is not None
