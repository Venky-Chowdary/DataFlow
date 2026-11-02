"""D22: a cron that can never fire (Feb 30) is refused with a clear reason on every path.

The check lives in ``services.cron_schedule.validate_cron`` so create, update,
the REST router, the Copilot cadence parser and the GitOps plan/apply all share
one rule. It is analytic (one pass over the calendar days of the horizon), not a
two-second minute-by-minute walk, and it names why the schedule never fires.
"""

from __future__ import annotations

import time

import pytest

import services.schedule_store as store
from services.cron_schedule import CronError, validate_cron

_NEVER = ["0 0 30 2 *", "0 0 31 2 *", "0 0 31 4,6,9,11 *", "*/5 * 31 11 *"]
_FIRES = ["0 0 29 2 *", "0 0 30 2 6", "0 0 31 * *", "0 0 1 1 *", "*/5 * * * *"]


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)


def _base(name: str) -> dict:
    return {
        "name": name,
        "source_connector_id": "a",
        "source_table": "t",
        "dest_connector_id": "b",
        "dest_table": "u",
    }


@pytest.mark.parametrize("expr", _NEVER)
def test_validate_cron_rejects_never_firing_with_reason_and_fast(expr):
    started = time.perf_counter()
    with pytest.raises(CronError) as exc:
        validate_cron(expr)
    assert time.perf_counter() - started < 0.5
    msg = str(exc.value).lower()
    assert "never fire" in msg and expr in str(exc.value)


@pytest.mark.parametrize("expr", _FIRES)
def test_validate_cron_accepts_firing_expressions(expr):
    validate_cron(expr)


def test_create_and_update_refuse_never_firing_cron(temp_store):
    with pytest.raises(ValueError, match="never fire"):
        store.create_schedule({**_base("bad"), "interval": "cron", "cron": "0 0 30 2 *"})
    ok = store.create_schedule({**_base("ok"), "interval": "daily"})
    with pytest.raises(ValueError, match="never fire"):
        store.update_schedule(ok.id, {"interval": "cron", "cron": "0 0 30 2 *"})
    assert store.get_schedule(ok.id).cron == ""


def test_gitops_plan_and_apply_refuse_never_firing_cron(temp_store):
    from services.gitops_manifest import apply_manifest, plan_manifest

    manifest = {
        "kind": "PipelineSchedule",
        "spec": {**_base("gitops-bad"), "interval": "cron", "cron": "0 0 30 2 *"},
    }
    plan = plan_manifest(manifest)
    assert plan["actions"][0]["action"] == "error"
    assert "never fire" in plan["actions"][0]["error"]
    applied = apply_manifest(manifest)
    assert applied["failed"] == 1
    assert "never fire" in applied["results"][0]["error"]
    assert store.list_schedules() == []
