"""The scheduler reads one clock (services.schedule_clock), so tests can pin it.

Review: test_mx324 asserted ``next_run_at > datetime.now()`` and failed when
the run crossed a minute boundary between the scheduler computing the */5 slot
and the assertion reading the wall clock. schedule_store and schedule_runner
now read "now" only through schedule_clock.utc_now (or a ``now`` argument),
and the ``frozen_schedule_clock`` fixture pins it.
"""

from __future__ import annotations

import ast
from datetime import timedelta
from pathlib import Path

from services.cron_schedule import next_run

SERVICES = Path(__file__).resolve().parents[1] / "services"


def test_scheduler_modules_never_read_the_wall_clock_directly():
    for name in ("schedule_store.py", "schedule_runner.py"):
        tree = ast.parse((SERVICES / name).read_text(encoding="utf-8"))
        direct = [
            node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"now", "utcnow"} and getattr(node.func.value, "id", "") == "datetime"
        ]
        assert direct == [], f"{name} reads datetime.now() at lines {direct}: use schedule_clock.utc_now()"


def test_a_pinned_clock_pins_the_schedulers_answers(frozen_schedule_clock, tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    assert store.compute_next_run("hourly") == (frozen_schedule_clock + timedelta(hours=1)).isoformat()
    assert store.compute_next_run("daily", cron="*/5 * * * *", tz="UTC") == next_run(
        "*/5 * * * *", frozen_schedule_clock, "UTC").isoformat()
    sched = store.create_schedule({
        "name": "pinned", "source_connector_id": "a", "source_table": "s", "dest_connector_id": "b",
        "dest_table": "d", "cron": "*/5 * * * *", "timezone": "UTC", "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert sched.next_run_at == "2026-11-27T12:05:00+00:00"
    store.update_schedule(sched.id, {"next_run_at": "2026-11-27T12:02:00+00:00"})
    assert sched.id in {s.id for s in store.due_schedules()}
    store.update_schedule(sched.id, {"next_run_at": "2026-11-27T12:03:00+00:00"})
    assert sched.id not in {s.id for s in store.due_schedules()}
