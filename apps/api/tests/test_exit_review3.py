"""Exit rehearsal: regressions from the third adversarial review (review 3) and its CI findings.

Each test names what it pins: the background work a rehearsal starts (tracked,
joined, stoppable, never started by a read), the verification lease and cap
Lows, H1 (rehearsal jobs are not re-run), H2 (exports read back by the writer's
dialect), M1 / M2 (the PII acknowledgement), M7 and the remaining Lows.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test, feature_threads  # noqa: F401

import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.test_exit_rehearsal_e2e import _run, _system, _wait, world  # noqa: E402,F401


def _doc(rid):
    from services.exit_store import REHEARSALS, get

    return get(REHEARSALS, rid)


def _extracted(tables=({"name": "events"},), **over):
    from services import exit_rehearsal

    started = exit_rehearsal.start(_system(tables=list(tables), **over), actor="owner@example.com")
    _wait(started)
    return started


# ------------------------------------- background work: tracked, joined, stoppable, never started by a read


def test_ci_a_page_read_never_starts_a_verification(world):  # noqa: F811
    import ast
    from pathlib import Path

    from services import exit_rehearsal

    started = _extracted()
    for _ in range(3):
        seen = exit_rehearsal.settle(started["id"], inline=False, start=False)
        assert seen["verdict"] == "running" and seen["tables"][0]["reason"] == "Verifying."
    assert feature_threads() == [] and exit_rehearsal.background_work() == []
    assert _doc(started["id"])["_verify"] is None, "a read claimed the verification"
    # The GET route asks exactly that way (a read reports; it never starts work).
    tree = ast.parse((Path(exit_rehearsal.__file__).parents[1] / "src/routers/exit_router.py").read_text("utf-8"))
    handler = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_exit_rehearsal")
    calls = [c for c in ast.walk(handler) if isinstance(c, ast.Call) and getattr(c.func, "id", "") == "settle"]
    assert calls and all(any(k.arg == "start" and k.value.value is False for k in c.keywords) for c in calls)
    assert exit_rehearsal.settle(started["id"])["verdict"] == "proven"


def test_ci_the_beat_starts_one_tracked_verifier_and_it_is_joined(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal

    release = threading.Event()
    real_verify = exit_rehearsal.verify

    def slow_verify(doc, jobs, **kw):
        release.wait(20)
        return real_verify(doc, jobs, **kw)

    monkeypatch.setattr(exit_rehearsal, "verify", slow_verify)
    started = _extracted()
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda rid: exit_rehearsal.settle(rid))
    for _ in range(5):  # five beats: one verifier, not five
        exit_rehearsal.settle(started["id"], inline=False)
    assert feature_threads().count(f"exit-verify-{started['id']}") == 1
    assert exit_rehearsal.background_work() == [f"exit-verify-{started['id']}"]
    assert exit_rehearsal.join_background(0.2) is False
    release.set()
    assert exit_rehearsal.join_background(30) is True
    assert feature_threads() == [] and exit_rehearsal.background_work() == []
    assert _doc(started["id"])["verdict"] == "proven"


def test_ci_shutdown_stops_a_running_verification_without_closing_the_rehearsal(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal, exit_verify_guard

    reading = threading.Event()
    real_verify = exit_rehearsal.verify

    def endless_verify(doc, jobs, **kw):
        reading.set()
        for _ in range(2000):  # a long read, row by row
            exit_verify_guard.tick(1)
            time.sleep(0.01)
        return real_verify(doc, jobs, **kw)

    monkeypatch.setattr(exit_rehearsal, "verify", endless_verify)
    started = _extracted()
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda rid: exit_rehearsal.settle(rid))
    exit_rehearsal.settle(started["id"], inline=False)
    assert reading.wait(10)
    assert exit_rehearsal.stop_background(15) is True
    assert feature_threads() == []
    doc = _doc(started["id"])
    assert doc["verdict"] == "running" and doc["_verify"]["id"] is None, "a stopped verifier closed or kept its claim"
    assert not [a for a, _ in world["audits"] if a.startswith("exit.rehearsal.") and a != "exit.rehearsal.started"]
    # The next start verifies it (the claim is free at once, and the time already spent is kept).
    monkeypatch.setattr(exit_rehearsal, "verify", real_verify)
    assert exit_rehearsal.settle(started["id"])["verdict"] == "proven"


# ----------------------------------------------- Low: the verification lease and its cap


def test_low_the_cap_counts_only_time_a_verifier_held_the_claim(world):  # noqa: F811
    from services import exit_rehearsal, exit_store
    from services.exit_store import REHEARSALS

    started = _extracted()
    old = datetime.now(timezone.utc) - timedelta(hours=7)

    def dead(doc):  # claimed 7 h ago by a replica that was restarted ten minutes later
        doc["_verify"] = {"id": "dead", "claimed_at": old.isoformat(), "started": old.isoformat(),
                          "at": (old + timedelta(minutes=10)).isoformat(), "held_seconds": 0.0}

    exit_store.update(REHEARSALS, started["id"], dead)
    out = exit_rehearsal.settle(started["id"])
    assert out["verdict"] == "proven", out["tables"][0]["reason"]
    # Ten minutes of verifying plus the stale window it was presumed alive for - not seven hours.
    assert exit_rehearsal._held_seconds({"claimed_at": old.isoformat(),
                                         "at": (old + timedelta(minutes=10)).isoformat()}) == 4200.0


def test_low_time_held_by_earlier_verifiers_still_counts_against_the_cap(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal, exit_store, exit_verify_guard
    from services.exit_store import REHEARSALS

    monkeypatch.setattr(exit_verify_guard, "max_verify_seconds", lambda: 3600.0)
    started = _extracted()
    old = datetime.now(timezone.utc) - timedelta(hours=3)

    def spent(doc):  # two earlier verifiers held it for 50 minutes; the last one for 5 more, then died
        doc["_verify"] = {"id": "dead", "claimed_at": old.isoformat(), "started": old.isoformat(),
                          "at": (old + timedelta(minutes=5)).isoformat(), "held_seconds": 3000.0}

    exit_store.update(REHEARSALS, started["id"], spent)
    out = exit_rehearsal.settle(started["id"])
    assert out["verdict"] == "not_proven" and "did not finish within its" in out["tables"][0]["reason"]


def test_low_a_verification_that_finishes_just_past_the_cap_is_kept(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal, exit_verify_guard

    real_verify = exit_rehearsal.verify

    def verify(doc, jobs, **kw):
        out = real_verify(doc, jobs, **kw)  # every row read and compared
        if kw.get("timed_out_after") is None:
            time.sleep(1.2)  # the last, row-less step crosses the cap
        return out

    monkeypatch.setattr(exit_verify_guard, "max_verify_seconds", lambda: 1.0)
    monkeypatch.setattr(exit_rehearsal, "verify", verify)
    started = _extracted()
    out = exit_rehearsal.settle(started["id"])
    assert out["verdict"] == "proven", out["tables"][0]["reason"]
    assert all("_read_failed" not in t for t in _doc(started["id"])["tables"])


def test_low_a_read_the_cap_interrupted_is_still_a_timeout(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal, exit_verify_guard

    def slow_source(*_a, **_k):
        time.sleep(1.3)
        exit_verify_guard.tick(1)  # past the cap: the next row read stops the verifier
        raise AssertionError("unreachable")

    monkeypatch.setattr(exit_verify_guard, "max_verify_seconds", lambda: 1.0)
    monkeypatch.setattr(exit_rehearsal, "_declared_types", slow_source)
    started = _extracted()
    out = exit_rehearsal.settle(started["id"])
    assert out["verdict"] == "not_proven" and "did not finish within its" in out["tables"][0]["reason"]


def test_low_a_verifier_that_lost_its_claim_stops_reading_and_closes_nothing(world, monkeypatch):  # noqa: F811
    from services import exit_rehearsal, exit_store, exit_verify_guard
    from services.exit_store import REHEARSALS

    monkeypatch.setattr(exit_rehearsal, "VERIFY_STALE", timedelta(seconds=0.4))
    reading = threading.Event()
    ticks: list[str] = []
    real_verify = exit_rehearsal.verify

    def reading_verify(doc, jobs, **kw):
        reading.set()
        try:
            for _ in range(400):
                exit_verify_guard.tick(1)
                ticks.append("row")
                time.sleep(0.02)
        except BaseException as exc:
            ticks.append(type(exc).__name__)
            raise
        return real_verify(doc, jobs, **kw)

    monkeypatch.setattr(exit_rehearsal, "verify", reading_verify)
    started = _extracted()
    outcome: list = []
    verifier = threading.Thread(target=lambda: outcome.append(exit_rehearsal.settle(started["id"])), daemon=True)
    verifier.start()
    assert reading.wait(10)

    def steal(doc):  # another replica took the verification over
        doc["_verify"] = {**doc["_verify"], "id": "another-replica", "at": exit_rehearsal.now_iso()}

    exit_store.update(REHEARSALS, started["id"], steal)
    verifier.join(15)
    assert not verifier.is_alive() and ticks[-1] == "VerificationTimedOut", ticks[-3:]
    assert len(ticks) < 400, "the superseded verifier read to the end"
    doc = _doc(started["id"])
    assert doc["verdict"] == "running" and doc["_verify"]["id"] == "another-replica"
    assert not [a for a, _ in world["audits"] if a in {"exit.rehearsal.completed", "exit.rehearsal.failed"}]

    def free(doc):
        doc["_verify"] = None

    exit_store.update(REHEARSALS, started["id"], free)
    monkeypatch.setattr(exit_rehearsal, "verify", real_verify)
    assert exit_rehearsal.settle(started["id"])["verdict"] == "proven"
