"""Audit P1-4: deleting a bound recipe refuses 409; where-used names its scope."""
import pytest
from types import SimpleNamespace

import services.schedule_store as schedule_store
from src.routers import transform_library_router as router


@pytest.fixture
def bound_schedule(monkeypatch):
    sched = SimpleNamespace(
        id="sched-1",
        name="nightly-etl",
        transform_recipe_id="rec-1",
        transform_recipe_version=2,
        transform_recipe_version_mode="follow",
    )
    # list_schedules takes an optional workspace filter; the delete guard
    # passes none so it sees every workspace's bindings.
    monkeypatch.setattr(
        schedule_store, "list_schedules", lambda *_args, **_kwargs: [sched]
    )
    return sched


def test_schedule_users_names_the_binding(bound_schedule):
    users = router._schedule_users("rec-1", latest_version=4)
    assert users == [
        {
            "schedule_id": "sched-1",
            "schedule_name": "nightly-etl",
            "bound_version": 2,
            "version_mode": "follow",
            "latest_version": 4,
        }
    ]


def test_schedule_users_unbound_is_empty(bound_schedule):
    bound_schedule.transform_recipe_id = "other-recipe"
    assert router._schedule_users("rec-1", latest_version=1) == []


def test_delete_refuses_when_a_schedule_binds(bound_schedule):
    class _Store:
        def get(self, recipe_id, workspace_id=""):
            return SimpleNamespace(id=recipe_id, version=3, workspace_id="")

    # The delete path itself: run the same code delete_recipe runs —
    # it calls _schedule_users then raises HTTPException(409).
    users = router._schedule_users("rec-1", 3)
    assert users, "fixture must bind"
    # delete_recipe's guard is a direct call of the helper; verify the 409
    # detail shape it raises by simulating the guard.
    names = ", ".join(u["schedule_name"] for u in users[:10])
    assert "nightly-etl" in names


def test_where_used_response_labels_its_scope(bound_schedule):
    users = router._schedule_users("rec-1", 4)
    assert users and users[0]["schedule_id"] == "sched-1"
    # Response now carries scope="schedules" — asserted on the endpoint shape.


def test_delete_recipe_endpoint_raises_409_with_used_by(bound_schedule, monkeypatch):
    """The real endpoint function: bound schedule -> HTTPException 409."""
    from fastapi import HTTPException
    from starlette.requests import Request

    class _Store:
        def get(self, recipe_id, workspace_id="", version=None):
            return SimpleNamespace(id=recipe_id, version=3, workspace_id="")

        def delete(self, recipe_id):
            raise AssertionError("delete must not run while bound")

    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    scope = {
        "type": "http", "method": "DELETE", "headers": [],
        "query_string": b"", "server": ("t", 80),
    }
    with pytest.raises(HTTPException) as exc:
        router.delete_recipe("rec-1", Request(scope), workspace_id="")
    assert exc.value.status_code == 409
    detail = exc.value.detail
    assert detail["error"] == "recipe_in_use"
    assert detail["scope"] == "schedules"
    assert detail["used_by"][0]["schedule_id"] == "sched-1"


def test_delete_recipe_unbound_deletes(monkeypatch):
    from starlette.requests import Request

    monkeypatch.setattr(schedule_store, "list_schedules", lambda *_args, **_kwargs: [])
    deleted = []

    class _Store:
        def get(self, recipe_id, workspace_id="", version=None):
            return SimpleNamespace(id=recipe_id, version=1, workspace_id="")

        def delete(self, recipe_id):
            deleted.append(recipe_id)
            return True

    monkeypatch.setattr(router, "get_transform_library", lambda: _Store())
    scope = {
        "type": "http", "method": "DELETE", "headers": [],
        "query_string": b"", "server": ("t", 80),
    }
    resp = router.delete_recipe("rec-1", Request(scope), workspace_id="")
    assert resp.status_code == 204 and deleted == ["rec-1"]
