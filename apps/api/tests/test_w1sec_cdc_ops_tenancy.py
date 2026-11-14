"""SEC-1: CDC watermarks and leases are scoped to the workspace that owns them.

Workspace B's administrator could list, read and clear workspace A's
watermarks, and list and force-release A's live CDC lease, through
``/ops/cdc-cursors*`` / ``/ops/cdc-leases*`` and the Pilot confirm of
``clear_cursor_resnapshot``. Each must now answer B exactly as for a key that
does not exist, leave A's records alone, and still serve A and the platform
administrator.
"""

from __future__ import annotations

import pytest

from tests.tenancy_world import tenancy_world  # noqa: F401
from tests.test_tenancy_route_scoping import SEEDERS, SENTINEL, world  # noqa: F401


def _a_cursor(world, *, stamped: bool = True, job: bool = False):  # noqa: F811
    """A watermark and a held lease on workspace A's route.

    ``stamped=False`` writes them the way older releases did (no workspace in
    the metadata): ownership then comes from the job or the connector id.
    """
    from services import cdc_lease, sync_cursor

    conn = SEEDERS["saved_connector"](world).params["connector_id"]
    key = sync_cursor.build_cursor_key(
        source_type="postgresql", source_database=f"{SENTINEL}_payroll", source_object="public.salaries",
        dest_type="postgresql", dest_database="warehouse", dest_object="salaries",
        dest_identity=f"id:{conn}",
    )
    job_id = world.job(world.workspace_a) if job else "ajob1"
    meta = {"workspace_id": world.workspace_a} if stamped else {"job_id": job_id} if job else {}
    with pytest.MonkeyPatch.context() as mp:
        if not stamped:
            mp.setattr("services.cdc_ops_scope.stamp_owner", lambda m: m)
        sync_cursor.set_watermark(key, "2026-11-01T00:00:00Z", metadata=meta)
        cdc_lease.acquire_lease(
            key, resource=f"pg:{SENTINEL}-db.internal:5432/payroll",
            holder_id=cdc_lease.new_holder_id(job_id), ttl_sec=600,
            meta={"job_id": job_id, **({"workspace_id": world.workspace_a} if stamped else {})},
        )
    if not stamped:
        assert "workspace_id" not in sync_cursor.get_watermark_record(key)[1]
        assert "workspace_id" not in (cdc_lease.get_lease(key).meta or {})
    return key


@pytest.fixture(params=["stamped", "legacy_by_connector", "legacy_by_job"])
def cursor(request, world):  # noqa: F811
    return _a_cursor(world, stamped=request.param == "stamped", job=request.param == "legacy_by_job")


def test_b_admin_cannot_list_read_clear_or_release_a_cursor(world, cursor):  # noqa: F811
    from services import cdc_lease, sync_cursor

    key = cursor
    b = world.admin_b
    keys = b.get("/api/v1/ops/cdc-cursors/keys")
    assert keys.status_code == 200 and key not in keys.text and SENTINEL not in keys.text, keys.text
    read = b.get("/api/v1/ops/cdc-cursors", params={"cursor_key": key})
    missing = b.get("/api/v1/ops/cdc-cursors", params={"cursor_key": key + "-nope"})
    assert read.json()["found"] is False, read.text
    assert {**read.json(), "cursor_key": ""} == {**missing.json(), "cursor_key": ""}
    leases = b.get("/api/v1/ops/cdc-leases/list")
    assert leases.status_code == 200 and SENTINEL not in leases.text, leases.text
    lease = b.get("/api/v1/ops/cdc-leases", params={"cursor_key": key})
    assert lease.json()["found"] is False and "internal" not in lease.text, lease.text

    clear = b.post("/api/v1/ops/cdc-cursors/clear", json={"cursor_key": key, "reason": "probe"})
    release = b.post("/api/v1/ops/cdc-leases/force-release", json={"cursor_key": key, "reason": "probe"})
    assert clear.status_code == 404 and release.status_code == 404, (clear.text, release.text)
    assert sync_cursor.get_watermark(key) == "2026-11-01T00:00:00Z"
    assert cdc_lease.get_lease(key) is not None


def test_owner_and_platform_admin_keep_their_view(world, cursor):  # noqa: F811
    from services import cdc_lease, sync_cursor

    key = cursor
    a = world.admin_a
    assert key in a.get("/api/v1/ops/cdc-cursors/keys").json()["cursor_keys"]
    assert a.get("/api/v1/ops/cdc-cursors", params={"cursor_key": key}).json()["found"] is True
    assert any(v["cursor_key"] == key for v in a.get("/api/v1/ops/cdc-leases/list").json()["leases"])
    assert a.get("/api/v1/ops/cdc-leases", params={"cursor_key": key}).json()["found"] is True
    # The platform administrator, no workspace header: every workspace.
    assert key in world.admin.get("/api/v1/ops/cdc-cursors/keys").json()["cursor_keys"]
    assert any(v["cursor_key"] == key for v in world.admin.get("/api/v1/ops/cdc-leases/list").json()["leases"])

    released = a.post("/api/v1/ops/cdc-leases/force-release", json={"cursor_key": key, "reason": "own"})
    assert released.status_code == 200 and released.json()["released"] is True, released.text
    assert cdc_lease.get_lease(key) is None
    cleared = a.post("/api/v1/ops/cdc-cursors/clear", json={"cursor_key": key, "reason": "own"})
    assert cleared.status_code == 200 and cleared.json()["cleared"] is True, cleared.text
    assert sync_cursor.get_watermark(key) is None


def test_a_viewer_of_the_owner_cannot_clear(world):  # noqa: F811
    from services import sync_cursor

    key = _a_cursor(world)
    viewer = world.viewer_elsewhere  # viewer of A, editor of B, no header
    r = viewer.post("/api/v1/ops/cdc-cursors/clear", json={"cursor_key": key})
    assert r.status_code in (403, 404), r.text
    assert sync_cursor.get_watermark(key) is not None


def test_pilot_confirm_clear_is_scoped_like_the_route(world):  # noqa: F811
    from services import sync_cursor
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from tests.test_tenancy_route_scoping import ADMIN_A, ADMIN_B

    key = _a_cursor(world)
    with world.as_workspace(world.workspace_b, actor=ADMIN_B):
        ack = get_ack_ledger().put(kind="clear_cursor_resnapshot", payload={"cursor_key": key, "reason": "x"})
    r = world.admin_b.post("/api/v1/copilot/confirm", json={"ack_id": ack, "reason": "probe"})
    assert r.status_code == 404, r.text
    assert sync_cursor.get_watermark(key) == "2026-11-01T00:00:00Z"

    with world.as_workspace(world.workspace_a, actor=ADMIN_A):
        ack = get_ack_ledger().put(kind="clear_cursor_resnapshot", payload={"cursor_key": key, "reason": "x"})
    own = world.admin_a.post("/api/v1/copilot/confirm", json={"ack_id": ack, "reason": "owner"})
    assert own.status_code == 200, own.text
    assert sync_cursor.get_watermark(key) is None


def test_new_watermarks_and_leases_carry_their_workspace(world):  # noqa: F811
    from services import cdc_lease, sync_cursor

    job_id = world.job(world.workspace_a)
    key = f"w1sec:src→dst:{job_id}"
    sync_cursor.set_watermark(key, "5", metadata={"job_id": job_id})
    assert sync_cursor.get_watermark_record(key)[1]["workspace_id"] == world.workspace_a
    lease = cdc_lease.acquire_lease(key, resource="w1sec:res", holder_id=cdc_lease.new_holder_id(job_id))
    assert lease.meta["workspace_id"] == world.workspace_a
