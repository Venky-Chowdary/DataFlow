"""B3 governance: per-step upgrade preview + explicit schedule re-pin.

An upgrade must be opt-in and reviewable: the preview returns the exact
per-step diff between the bound version and the target before anything
re-pins, and the upgrade endpoint applies it only on demand.
"""
from types import SimpleNamespace

import pytest
from starlette.requests import Request

import services.schedule_store as schedule_store
from services.transform_library import recipe_step_diff
from src.routers import schedules_router as sched_router
from src.routers import transform_library_router as router


def _scope(method="POST", path="/"):
    return {
        "type": "http", "method": method, "path": path,
        "headers": [], "query_string": b"", "server": ("t", 80),
    }


_V1_STEPS = [
    {"op": "case", "column": "note", "options": {"mode": "upper"}},
    {"op": "trim", "column": "name", "options": {}},
]
_V2_STEPS = [
    {"op": "case", "column": "note", "options": {"mode": "lower"}},
    {"op": "trim", "column": "name", "options": {}},
    {"op": "null_if", "column": "price", "options": {"values": ["0"]}},
]


def _recipe(version, steps):
    return SimpleNamespace(
        id="rec-1", name="govern", workspace_id="",
        version=version, enabled=True, status="active",
        steps=[SimpleNamespace(to_dict=lambda s=st: dict(s)) for st in steps],
    )


class _Store:
    def __init__(self):
        self._versions = {1: _recipe(1, _V1_STEPS), 2: _recipe(2, _V2_STEPS)}

    def get(self, recipe_id, workspace_id="", version=None):
        if version is None:
            version = max(self._versions)
        return self._versions.get(version)


class _BadDiff:
    pass


def test_recipe_step_diff_positional():
    diff = recipe_step_diff(_V1_STEPS, _V2_STEPS)
    assert [d["change"] for d in diff] == ["modified", "added"]
    assert diff[0]["index"] == 0
    assert diff[0]["after"]["options"]["mode"] == "lower"
    assert "options" in diff[0]["changed_keys"]
    assert diff[1]["index"] == 2
    assert diff[1]["after"]["op"] == "null_if"


def test_upgrade_preview_returns_diff_and_mutates_nothing(monkeypatch):
    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    sched = SimpleNamespace(
        id="sched-1", name="nightly", transform_recipe_id="rec-1",
        transform_recipe_version=1, transform_recipe_version_mode="pin",
    )
    monkeypatch.setattr(schedule_store, "list_schedules", lambda *_filter: [sched])
    monkeypatch.setattr(schedule_store, "get_schedule", lambda sid: sched)
    body = router.UpgradePreviewPayload(schedule_id="sched-1")
    out = router.upgrade_preview("rec-1", body, Request(_scope()), workspace_id="")
    assert out["from_version"] == 1
    assert out["to_version"] == 2
    assert [d["change"] for d in out["step_diff"]] == ["modified", "added"]
    assert out["used_by"][0]["schedule_id"] == "sched-1"
    assert out["mutates_nothing"] is True


def test_upgrade_preview_404_on_missing_version(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    body = router.UpgradePreviewPayload(target_version=9)
    with pytest.raises(HTTPException) as exc:
        router.upgrade_preview("rec-1", body, Request(_scope()), workspace_id="")
    assert exc.value.status_code == 404


def test_schedule_upgrade_repins_with_diff(monkeypatch):
    sched = SimpleNamespace(
        id="sched-1", name="nightly", transform_recipe_id="rec-1",
        transform_recipe_version=1, transform_recipe_version_mode="pin",
        workspace_id="",
    )
    updated = SimpleNamespace(
        id="sched-1", transform_recipe_version=2,
    )
    monkeypatch.setattr(sched_router, "get_schedule", lambda sid: sched)
    monkeypatch.setattr(
        sched_router, "update_schedule",
        lambda sid, data: updated if data["transform_recipe_version"] == 2 else None,
    )
    monkeypatch.setattr(
        sched_router, "record_security_event", lambda *a, **k: None
    )
    import services.transform_library as tl
    monkeypatch.setattr(tl, "get_transform_library", lambda: _Store())
    body = sched_router.ScheduleRecipeUpgrade(target_version=2)
    out = sched_router.upgrade_schedule_recipe(
        "sched-1", body, Request(_scope()), workspace_id=""
    )
    assert out["from_version"] == 1
    assert out["to_version"] == 2
    assert out["bound_version"] == 2
    assert [d["change"] for d in out["step_diff"]] == ["modified", "added"]


def test_schedule_upgrade_409_without_binding(monkeypatch):
    from fastapi import HTTPException

    sched = SimpleNamespace(
        id="sched-2", transform_recipe_id="", workspace_id=""
    )
    monkeypatch.setattr(sched_router, "get_schedule", lambda sid: sched)
    body = sched_router.ScheduleRecipeUpgrade(target_version=2)
    with pytest.raises(HTTPException) as exc:
        sched_router.upgrade_schedule_recipe(
            "sched-2", body, Request(_scope()), workspace_id=""
        )
    assert exc.value.status_code == 409


def _sched(version=1, workspace_id=""):
    return SimpleNamespace(
        id="sched-1", name="nightly", transform_recipe_id="rec-1",
        transform_recipe_version=version, transform_recipe_version_mode="pin",
        workspace_id=workspace_id,
    )


def _wire_sched(monkeypatch, sched):
    monkeypatch.setattr(sched_router, "get_schedule", lambda sid: sched)
    monkeypatch.setattr(
        sched_router, "update_schedule",
        lambda sid, data: SimpleNamespace(
            id=sid, transform_recipe_version=data["transform_recipe_version"]
        ),
    )
    monkeypatch.setattr(
        sched_router, "record_security_event", lambda *a, **k: None
    )
    import services.transform_library as tl
    monkeypatch.setattr(tl, "get_transform_library", lambda: _Store())


def _scope_ws(ws, method="POST", path="/"):
    return {
        "type": "http", "method": method, "path": path,
        "headers": [(b"x-workspace-id", ws.encode())],
        "query_string": b"", "server": ("t", 80),
    }


def test_upgrade_preview_returns_diff_hash(monkeypatch):
    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    sched = _sched()
    monkeypatch.setattr(schedule_store, "list_schedules", lambda *_filter: [sched])
    monkeypatch.setattr(schedule_store, "get_schedule", lambda sid: sched)
    body = router.UpgradePreviewPayload(schedule_id="sched-1")
    out = router.upgrade_preview("rec-1", body, Request(_scope()), workspace_id="")
    assert out["diff_hash"]
    assert out["rollback"] is False


def test_upgrade_409_on_expected_from_version_mismatch(monkeypatch):
    from fastapi import HTTPException

    _wire_sched(monkeypatch, _sched(version=2))
    body = sched_router.ScheduleRecipeUpgrade(
        target_version=3, expected_from_version=1
    )
    with pytest.raises(HTTPException) as exc:
        sched_router.upgrade_schedule_recipe(
            "sched-1", body, Request(_scope()), workspace_id=""
        )
    assert exc.value.status_code == 409


def test_upgrade_requires_matching_diff_hash(monkeypatch):
    from fastapi import HTTPException

    _wire_sched(monkeypatch, _sched(version=1))
    body = sched_router.ScheduleRecipeUpgrade(
        target_version=2, expected_diff_hash="deadbeef"
    )
    with pytest.raises(HTTPException) as exc:
        sched_router.upgrade_schedule_recipe(
            "sched-1", body, Request(_scope()), workspace_id=""
        )
    assert exc.value.status_code == 409


def test_upgrade_accepts_preview_diff_hash(monkeypatch):
    """The hash the preview returns is accepted by upgrade end-to-end."""
    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    sched = _sched(version=1)
    monkeypatch.setattr(schedule_store, "list_schedules", lambda *_filter: [sched])
    monkeypatch.setattr(schedule_store, "get_schedule", lambda sid: sched)
    prev = router.upgrade_preview(
        "rec-1",
        router.UpgradePreviewPayload(schedule_id="sched-1"),
        Request(_scope()),
        workspace_id="",
    )
    _wire_sched(monkeypatch, sched)
    body = sched_router.ScheduleRecipeUpgrade(
        target_version=2,
        expected_from_version=1,
        expected_diff_hash=prev["diff_hash"],
    )
    out = sched_router.upgrade_schedule_recipe(
        "sched-1", body, Request(_scope()), workspace_id=""
    )
    assert out["to_version"] == 2
    assert out["bound_version"] == 2


def test_downgrade_is_labeled_rollback_in_response_and_audit(monkeypatch):
    events = []
    sched = _sched(version=2)
    monkeypatch.setattr(sched_router, "get_schedule", lambda sid: sched)
    monkeypatch.setattr(
        sched_router, "update_schedule",
        lambda sid, data: SimpleNamespace(
            id=sid, transform_recipe_version=data["transform_recipe_version"]
        ),
    )
    monkeypatch.setattr(
        sched_router, "record_security_event",
        lambda *a, **k: events.append(k),
    )
    import services.transform_library as tl
    monkeypatch.setattr(tl, "get_transform_library", lambda: _Store())
    body = sched_router.ScheduleRecipeUpgrade(target_version=1)
    out = sched_router.upgrade_schedule_recipe(
        "sched-1", body, Request(_scope()), workspace_id=""
    )
    assert out["rollback"] is True
    assert out["to_version"] == 1
    assert events and events[0]["details"]["rollback"] is True


def test_forward_upgrade_not_labeled_rollback(monkeypatch):
    events = []
    sched = _sched(version=1)
    monkeypatch.setattr(sched_router, "get_schedule", lambda sid: sched)
    monkeypatch.setattr(
        sched_router, "update_schedule",
        lambda sid, data: SimpleNamespace(
            id=sid, transform_recipe_version=data["transform_recipe_version"]
        ),
    )
    monkeypatch.setattr(
        sched_router, "record_security_event",
        lambda *a, **k: events.append(k),
    )
    import services.transform_library as tl
    monkeypatch.setattr(tl, "get_transform_library", lambda: _Store())
    body = sched_router.ScheduleRecipeUpgrade(target_version=2)
    out = sched_router.upgrade_schedule_recipe(
        "sched-1", body, Request(_scope()), workspace_id=""
    )
    assert out["rollback"] is False
    assert events and events[0]["details"]["rollback"] is False


def _patch_ws_membership(monkeypatch):
    """Actor is an editor of workspace b only (the tenant resolver's seam)."""
    import services.tenant_scope as ts
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setattr(ts, "ordered_memberships", lambda email: (("b", "editor"),))
    monkeypatch.setattr(ts, "require_workspace_isolation", lambda: False)


def _member_of_b_request():
    user = {"email": "editor@b.test", "role": "member"}
    return Request({**_scope_ws("b"), "state": {"user_email": user["email"], "user": user}})


def test_cross_workspace_preview_denied(monkeypatch):
    """Workspace B cannot preview a workspace A recipe — 404/403."""
    from fastapi import HTTPException

    _patch_ws_membership(monkeypatch)
    store = _Store()
    store._versions[1].workspace_id = "a"
    store._versions[2].workspace_id = "a"
    monkeypatch.setattr(router, "get_transform_library", lambda: store)
    body = router.UpgradePreviewPayload(target_version=2)
    with pytest.raises(HTTPException) as exc:
        router.upgrade_preview(
            "rec-1", body, _member_of_b_request(), workspace_id="b"
        )
    assert exc.value.status_code in (403, 404)


def test_cross_workspace_upgrade_denied(monkeypatch):
    """Workspace B cannot re-pin a workspace A schedule — 404/403."""
    from fastapi import HTTPException

    _patch_ws_membership(monkeypatch)
    _wire_sched(monkeypatch, _sched(version=1, workspace_id="a"))
    body = sched_router.ScheduleRecipeUpgrade(target_version=2)
    with pytest.raises(HTTPException) as exc:
        sched_router.upgrade_schedule_recipe(
            "sched-1", body, _member_of_b_request(), workspace_id="b"
        )
    assert exc.value.status_code in (403, 404)
