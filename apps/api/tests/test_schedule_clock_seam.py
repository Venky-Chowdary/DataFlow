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


def _scheduler_modules() -> list[Path]:
    """Every scheduler module: services/schedule_*.py, scheduler_mode.py and cron_schedule.py."""
    found = sorted(SERVICES.glob("schedule_*.py")) + [SERVICES / "scheduler_mode.py", SERVICES / "cron_schedule.py"]
    return [p for p in found if p.name != "schedule_clock.py"]


def _direct_clock_reads(tree: ast.AST) -> list[int]:
    """Lines calling datetime.now / utcnow / today, date.today, or time.time (any spelling of the import)."""
    lines = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "time" and any(a.name == "time" for a in node.names):
            lines.append(node.lineno)
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        owner = node.func.value
        owner_name = owner.id if isinstance(owner, ast.Name) else owner.attr if isinstance(owner, ast.Attribute) else ""
        if (owner_name, node.func.attr) in {("datetime", "now"), ("datetime", "utcnow"), ("datetime", "today"),
                                            ("date", "today"), ("time", "time")}:
            lines.append(node.lineno)
    return lines


def test_scheduler_modules_never_read_the_wall_clock_directly():
    modules = _scheduler_modules()
    assert {"schedule_store.py", "schedule_runner.py", "schedule_run_lifecycle.py", "schedule_approvals.py",
            "schedule_recovery.py", "cron_schedule.py"} <= {p.name for p in modules}
    offenders = {}
    for path in modules:
        lines = _direct_clock_reads(ast.parse(path.read_text(encoding="utf-8")))
        if lines:
            offenders[path.name] = lines
    assert offenders == {}, f"read the clock through schedule_clock.utc_now(): {offenders}"


def test_the_scan_sees_every_spelling():
    sample = ast.parse(
        "import datetime as dt\nfrom time import time\n"
        "a = datetime.now(timezone.utc)\nb = dt.datetime.utcnow()\nc = date.today()\nd = time.time()\n"
    )
    assert _direct_clock_reads(sample) == [2, 3, 4, 5, 6]


def test_a_pinned_clock_pins_the_schedulers_answers(frozen_schedule_clock, tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    assert store.compute_next_run("hourly") == (frozen_schedule_clock + timedelta(hours=1)).isoformat()
    assert store.compute_next_run("daily", cron="*/5 * * * *", tz="UTC") == next_run(
        "*/5 * * * *", frozen_schedule_clock, "UTC").isoformat()
    from services.schedule_run_lifecycle import _now as lifecycle_now

    assert lifecycle_now() == frozen_schedule_clock.isoformat()
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
