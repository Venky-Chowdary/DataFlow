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
    assert exit_rehearsal.join_background(30)  # the extract futures' completion callbacks have run too
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


# --------------------------------------------- H1: a rehearsal's extract job is never re-run or reused


import sqlite3  # noqa: E402

from tests.test_exit_pii_review import _ack, audit_events, people  # noqa: E402,F401


def _job(job_id):
    from services.parity_state import job_store

    return job_store().get_job(job_id)


def test_h1_every_path_that_rebuilds_a_stored_request_refuses_a_rehearsal_job(world):  # noqa: F811
    """rerun / retry / resume / quarantine replay (routes), Sync now, Parity re-sync and re-run, drift repair."""
    from fastapi import HTTPException

    from services import sync_now
    from services.exit_job_guard import refusal, rehearsal_of_request, request_refusal
    from services.parity_fixes import rerun_refusal
    from src.routers.connectors_router import _refuse_rehearsal_extract

    out = _run(_system(tables=[{"name": "events"}]))
    assert out["verdict"] == "proven"
    job = _job(out["tables"][0]["job_id"])
    assert job["exit_rehearsal_id"] == out["id"] and rehearsal_of_request(job["transfer_request"]) == out["id"]
    for verb in ("re-run", "retried", "resumed", "replayed from quarantine"):
        with pytest.raises(HTTPException) as caught:
            _refuse_rehearsal_extract(job, verb)
        assert caught.value.status_code == 409 and f"cannot be {verb}" in caught.value.detail
        assert "Start a new rehearsal instead" in caught.value.detail
    # A copy of the stored request (a job document that lost the link) is still recognised by what it carries.
    orphan = {"transfer_request": dict(job["transfer_request"])}
    assert refusal(orphan) and "Start a new rehearsal instead" in rerun_refusal(job)
    with pytest.raises(sync_now.SyncNowRefused, match="Start a new rehearsal instead"):
        sync_now.prepare_sync(mode="sync", parent_request=job["transfer_request"], parent_id="j",
                              source_connected=True, destination_connected=True)
    with pytest.raises(sync_now.SyncNowRefused, match="Start a new rehearsal instead"):
        sync_now.begin_sync_now({"transfer_request": job["transfer_request"], "parent_job_id": "j"})
    # An ordinary job is not touched by any of this.
    plain = {"transfer_request": {"destination": {"kind": "file_export", "output_path": "orders.csv"},
                                  "idempotency_key": "exit-strategy-7", "triggered_by": "exit rehearsal notes"}}
    assert refusal(plain) is None and request_refusal(plain["transfer_request"]) is None
    _refuse_rehearsal_extract(plain, "re-run")


def test_h1_the_four_job_routes_answer_409_for_a_rehearsal_extract():
    """Each job route calls the refusal right after its access checks (nothing is rebuilt first)."""
    import ast
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "src/routers/connectors_router.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    expected = {"retry_transfer_job": "retried", "rerun_transfer_job": "re-run", "resume_transfer_job": "resumed",
                "replay_job_quarantine": "replayed from quarantine"}
    seen = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name in expected:
            calls = [c for c in ast.walk(node) if isinstance(c, ast.Call)
                     and getattr(c.func, "id", "") == "_refuse_rehearsal_extract"]
            rebuilt = [c.lineno for c in ast.walk(node) if isinstance(c, ast.Call)
                       and getattr(c.func, "id", "") == "transfer_request_from_dict"]
            assert calls and rebuilt and calls[0].lineno < min(rebuilt), node.name
            seen[node.name] = calls[0].args[1].value
    assert seen == expected


def test_h1_a_rehearsal_request_never_runs_as_another_job_and_the_sealed_part_is_never_overwritten(people):  # noqa: F811
    """The reviewer's replay: re-run the extract after the pack is signed. It must not run, at any layer."""
    import hashlib
    from pathlib import Path

    from services.exit_inventory import get_system
    from services.exit_job_guard import ExtractRefused, before_run
    from services.file_export_append import land_export_bytes
    from services.parity_state import job_store
    from src.transfer.background import _run_transfer
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import transfer_request_from_dict

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    out = _run(get_system(system["id"]))
    assert out["verdict"] == "proven", out["tables"][0]["reason"]
    job = _job(out["tables"][0]["job_id"])
    path = Path(job["destination_summary"]["path"])
    sealed = hashlib.sha256(path.read_bytes()).hexdigest()
    assert sealed == out["tables"][0]["part"]["sha256"]

    request = transfer_request_from_dict(job["transfer_request"])  # what a re-run would rebuild
    assert request.compliance_acknowledged is True
    new_id = get_transfer_engine()._create_pending_job(request)
    with pytest.raises(ExtractRefused, match="Start a new rehearsal instead"):
        before_run(new_id, request, job_store())
    _run_transfer(new_id, request)  # the scheduler's own entry point: the guard runs first
    after = _job(new_id)
    assert after["status"] == "failed" and "Start a new rehearsal instead" in str(after["error"])
    assert not (after.get("preflight") or {}), "the acknowledged request reached the PII gate as an ordinary job"
    # The rehearsal's own job cannot run again either once the rehearsal is closed.
    with pytest.raises(ExtractRefused, match="is closed"):
        before_run(str(job["_id"]) if job.get("_id") else out["tables"][0]["job_id"], request, job_store())
    # And the file layer never replaces a sealed extract, whoever asks.
    with pytest.raises(ValueError, match="never overwritten"):
        land_export_bytes(request.destination, b'{"id": 1}\n', job_id="other", export_name=path.name,
                          sync_mode="full_refresh_overwrite")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == sealed


# ------------------------------------------- M1: the acknowledgement is re-checked at job start and at settle


def _plain_table(world):  # noqa: F811
    conn = sqlite3.connect(str(world["src"]))
    with conn:
        conn.execute("CREATE TABLE plain (id INTEGER PRIMARY KEY, qty INTEGER)")
        conn.executemany("INSERT INTO plain VALUES (?, ?)", [(i, i * 2) for i in range(1, 6)])
    conn.close()


def test_m1_withdrawing_before_the_extracts_start_holds_them(people, monkeypatch, audit_events):  # noqa: F811
    import src.transfer.background as background
    from services.exit_inventory import get_system
    from services.exit_pii import withdraw
    from services.exit_rehearsal import public, settle, start

    _plain_table(people)
    system = _system(name="People", tables=[{"name": "plain"}, {"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    queued: list = []
    real = background.run_transfer_async
    monkeypatch.setattr(background, "run_transfer_async", lambda job_id, req, resume=False: queued.append((job_id, req)))
    started = start(get_system(system["id"]), actor="owner@example.com")
    assert [r.compliance_acknowledged for _j, r in queued] == [True, True]
    withdraw(system["id"], actor="dpo@example.com")  # withdrawn while both extracts are still queued
    for job_id, req in queued:
        real(job_id, req, resume=False)
    _wait(started)
    out = public(settle(started["id"]))
    by_name = {t["name"]: t for t in out["tables"]}
    assert out["verdict"] == "needs_pii_review"
    assert by_name["people"]["status"] == "needs_pii_review" and by_name["people"]["part"] is None
    assert "the PII acknowledgement was withdrawn" in by_name["people"]["reason"]
    assert [c["column"] for c in by_name["people"]["pii_held"]] == ["email", "name", "ssn"] or by_name["people"][
        "pii_held"], by_name["people"]["pii_held"]
    assert by_name["plain"]["status"] == "proven"  # nothing to acknowledge there
    assert _job(by_name["people"]["job_id"])["exit_pii_acknowledgement"]["used"] is False
    assert "exit.rehearsal.extract_acknowledged" not in [e["action"] for e in audit_events]


def test_m1_withdrawing_after_the_extract_ran_leaves_its_part_unsealed(people):  # noqa: F811
    from services.exit_inventory import get_system
    from services.exit_pii import withdraw
    from services.exit_rehearsal import public, settle, start

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    started = start(get_system(system["id"]), actor="owner@example.com")
    _wait(started)  # the extract ran acknowledged and completed ...
    assert _job(started["tables"][0]["job_id"])["status"] == "completed"
    withdraw(system["id"], actor="dpo@example.com")  # ... and the acknowledgement is withdrawn before settle
    out = public(settle(started["id"]))
    table = out["tables"][0]
    assert out["verdict"] == "needs_pii_review" and table["status"] == "needs_pii_review"
    assert "no longer stands (the PII acknowledgement was withdrawn)" in table["reason"]
    assert table["part"] is None and out["seal"]["parts_count"] == 0 and out["signoff"]["status"] == "blocked"


def test_m1_and_low_a_run_that_used_an_acknowledgement_says_so_in_the_audit_trail(people, audit_events):  # noqa: F811
    from services.exit_inventory import get_system

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    first = [e for e in audit_events if e["action"] == "exit.rehearsal.started"][-1]["details"]
    assert first["pii_acknowledged"] is False and first["pii_acknowledged_by"] is None
    _ack(system["id"])
    out = _run(get_system(system["id"]))
    assert out["verdict"] == "proven"
    started = [e for e in audit_events if e["action"] == "exit.rehearsal.started"][-1]["details"]
    assert started["pii_acknowledged"] is True and started["pii_acknowledged_by"] == "dpo@example.com"
    per_extract = [e for e in audit_events if e["action"] == "exit.rehearsal.extract_acknowledged"]
    assert len(per_extract) == 1 and per_extract[0]["actor"] == "dpo@example.com"
    assert per_extract[0]["details"]["job_id"] == out["tables"][0]["job_id"]
    assert per_extract[0]["details"]["table"] == "people"
    assert _job(out["tables"][0]["job_id"])["exit_pii_acknowledgement"]["used"] is True


# ------------------------------------------------ M2: the acknowledgement is bound to what the gate found


def test_m2_an_acknowledgement_needs_a_hold_and_stores_what_it_covers(people):  # noqa: F811
    from services.exit_inventory import ExitInvalid, get_system, public_system

    _plain_table(people)
    system = _system(name="People", tables=[{"name": "plain"}, {"name": "people"}])
    with pytest.raises(ExitInvalid, match="nothing to acknowledge") as refused:
        _ack(system["id"], reason="acknowledged before any rehearsal ran")
    assert refused.value.status_code == 422
    held = _run(system)
    assert held["verdict"] == "needs_pii_review"
    hold = public_system(get_system(system["id"]))["pii_hold"]
    assert hold["rehearsal_id"] == held["id"] and [t["table"] for t in hold["tables"]] == ["people"]
    columns = {c["column"]: c["categories"] for c in hold["tables"][0]["columns"]}
    assert "ssn" in columns and columns["ssn"], columns
    view = public_system(_ack(system["id"]))
    assert view["pii_acknowledgement"]["covers"] == hold["tables"]
    assert view["pii_hold"] is None or view["pii_hold"]["rehearsal_id"] == held["id"]
    # A proven system has nothing held: a second, blank acknowledgement is refused too.
    assert _run(get_system(system["id"]))["verdict"] == "proven"
    with pytest.raises(ExitInvalid, match="nothing to acknowledge"):
        _ack(system["id"])


def test_m2_pii_beyond_the_acknowledged_columns_reads_needs_pii_review(people):  # noqa: F811
    from services.exit_inventory import get_system, public_system

    _plain_table(people)
    system = _system(name="People", tables=[{"name": "plain"}, {"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    assert _run(get_system(system["id"]))["verdict"] == "proven"
    conn = sqlite3.connect(str(people["src"]))  # PII appears where none was acknowledged
    with conn:
        conn.execute("ALTER TABLE plain ADD COLUMN ssn TEXT")
        conn.execute("UPDATE plain SET ssn = '123-45-' || (6000 + id)")
    conn.close()
    out = _run(get_system(system["id"]))
    by_name = {t["name"]: t for t in out["tables"]}
    assert out["verdict"] == "needs_pii_review", [(t["name"], t["status"]) for t in out["tables"]]
    assert by_name["plain"]["status"] == "needs_pii_review" and by_name["plain"]["part"] is None
    assert "found PII beyond what was acknowledged: ssn" in by_name["plain"]["reason"]
    assert "123-45" not in str(out)
    assert by_name["people"]["status"] == "proven"
    assert public_system(get_system(system["id"]))["pii_acknowledgement"]["current"] is True
    # Acknowledging the new hold adds to what was covered; then the system is proven again.
    again = public_system(_ack(system["id"]))["pii_acknowledgement"]["covers"]
    assert [t["table"] for t in again] == ["people", "plain"]
    assert _run(get_system(system["id"]))["verdict"] == "proven"


def test_m2_changing_the_definition_and_back_does_not_revive_an_acknowledgement(people):  # noqa: F811
    from services.exit_inventory import get_system, public_system, update

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    assert public_system(get_system(system["id"]))["pii_acknowledgement"]["current"] is True
    update(system["id"], {"exit_window_hours": 48}, actor="owner@example.com", expected_version=None)
    assert public_system(get_system(system["id"]))["pii_acknowledgement"]["current"] is False
    update(system["id"], {"exit_window_hours": 720}, actor="owner@example.com", expected_version=None)
    back = public_system(get_system(system["id"]))
    assert back["pii_acknowledgement"]["current"] is False, "the old acknowledgement came back"
    assert _run(get_system(system["id"]))["verdict"] == "needs_pii_review"
    # A change that is not part of the definition (the name) lapses nothing.
    _ack(system["id"])
    update(system["id"], {"name": "People (EU)"}, actor="owner@example.com", expected_version=None)
    assert public_system(get_system(system["id"]))["pii_acknowledgement"]["current"] is True


# -------------------------------------- Low: the digest and the pack both cover the acknowledgement


def test_low_the_attested_digest_and_the_signed_pack_cover_the_pii_acknowledgement(people):  # noqa: F811
    import copy

    from services.exit_evidence import content_sha256, verify_pack
    from services.exit_inventory import get_system
    from services.exit_store import REHEARSALS, get
    from tests.test_exit_evidence_pack import _sign_all

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    _ack(system["id"])
    out = _run(get_system(system["id"]))
    doc = get(REHEARSALS, out["id"])
    # What a signer attests names who acknowledged PII and what it covered ...
    attested = content_sha256(doc)
    assert content_sha256({**doc, "pii_acknowledgement": None}) != attested
    assert content_sha256({**doc, "pii_acknowledgement": {**doc["pii_acknowledgement"], "actor": "x@y"}}) != attested
    # ... while a rehearsal without one keeps the digest it always had.
    plain = {**doc, "pii_acknowledgement": None}
    assert content_sha256(plain) == content_sha256({k: v for k, v in plain.items() if k != "pii_acknowledgement"})
    signed = _sign_all(out["id"])
    pack = signed["signed"]["pack"] if "signed" in signed else get(REHEARSALS, out["id"])["signed"]["pack"]
    assert pack["rehearsal"]["pii_acknowledgement"] == doc["pii_acknowledgement"]
    assert pack["rehearsal"]["pii_acknowledgement"]["covers"][0]["table"] == "people"
    assert pack["content_sha256_attested"] == attested
    assert "current" not in pack["rehearsal"] and "stale_reason" not in pack["rehearsal"]
    assert verify_pack(copy.deepcopy(pack))["ok"] is True
    for tamper in (None, {**doc["pii_acknowledgement"], "actor": "someone@else"}):
        forged = copy.deepcopy(pack)
        forged["rehearsal"]["pii_acknowledgement"] = tamper
        assert verify_pack(forged)["ok"] is False
    dropped = copy.deepcopy(pack)
    del dropped["rehearsal"]["pii_acknowledgement"]
    assert verify_pack(dropped)["ok"] is False


# ------------------------------- Low: trend, the rehearsal's own `current`, the Pilot tool, one lookup per row


def test_low_the_projection_uses_proven_rehearsals_only(world):  # noqa: F811
    from services import exit_rehearsal

    def row(n, verdict, seconds, rows):
        return {"id": f"exr_{n:012x}", "system_id": "s", "n": n, "verdict": verdict,
                "started_at": f"2026-01-0{n}T00:00:00Z", "duration": {"measured_seconds": seconds},
                "tables": [{"rows_extracted": rows}], "seal": {"total_bytes": 1}}

    history = [row(1, "proven", 100, 1000), row(2, "failed", 5, 10), row(3, "incomplete", 900, 9000),
               row(4, "proven", 200, 2000)]
    import services.exit_rehearsal as module

    original = module.rehearsals_of
    module.rehearsals_of = lambda _sid: list(reversed(history))
    try:
        trend = exit_rehearsal.trend(history[-1])
    finally:
        module.rehearsals_of = original
    assert [p["verdict"] for p in trend["points"]] == ["proven", "failed", "incomplete", "proven"]
    assert trend["projection"]["seconds"] == 400  # 200 s * 2000 / 1000: the two proven runs
    assert "rehearsal 4" in trend["projection"]["basis"] and "rehearsal 1" in trend["projection"]["basis"]
    module.rehearsals_of = lambda _sid: list(reversed(history[:3]))
    try:
        assert exit_rehearsal.trend(history[2])["projection"] is None  # one proven run projects nothing
    finally:
        module.rehearsals_of = original


def test_low_a_rehearsal_says_whether_it_still_proves_the_current_definition(world):  # noqa: F811
    from services.exit_inventory import get_system, remove, update
    from services.exit_rehearsal import public
    from src.ai.copilot.exit_tools import explain_exit_rehearsal

    system = _system(tables=[{"name": "events"}])
    out = _run(system)
    assert out["current"] is True and out["stale_reason"] is None
    update(system["id"], {"exit_window_hours": 48}, actor="owner@example.com", expected_version=None)
    stale = public(_doc(out["id"]))
    assert stale["current"] is False and "changed since this rehearsal" in stale["stale_reason"]
    told = explain_exit_rehearsal(rehearsal_id=out["id"])
    body = told.output
    assert body["stale"] is True and body["current"] is False and "changed" in body["stale_reason"]
    assert body["pii_acknowledgement"] is None
    assert get_system(system["id"])
    remove(system["id"], actor="owner@example.com")
    assert public(_doc(out["id"]))["current"] is None


def test_low_the_board_resolves_each_systems_connector_once(world, monkeypatch):  # noqa: F811
    from services import exit_inventory, schedule_runner

    systems = [_system(name=f"S{i}", tables=[{"name": "events"}]) for i in range(3)]
    for system in systems:
        _run(system)
    lookups: list[str] = []
    real = schedule_runner._resolve_connector_for_schedule

    def counting(connector_id, schedule):
        lookups.append(connector_id)
        return real(connector_id, schedule)

    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", counting)
    board = exit_inventory.board([exit_inventory.get_system(s["id"]) for s in systems])
    assert len(board["systems"]) == 3 and len(lookups) == 3, lookups
    lookups.clear()
    exit_inventory.public_system(exit_inventory.get_system(systems[0]["id"]))
    assert len(lookups) == 1, lookups
