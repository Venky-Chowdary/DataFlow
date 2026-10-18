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
    monkeypatch.setattr(schedule_store, "list_schedules", lambda: [sched])
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
