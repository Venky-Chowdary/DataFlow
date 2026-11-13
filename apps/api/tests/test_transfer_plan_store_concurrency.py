"""PS-1: concurrent edits of one transfer plan must never drop an edit.

Every plan mutator used to read the whole plan file, change one plan and
rewrite the file with no lock, so two edits that overlapped kept only the last
writer's copy: 16 concurrent ``sync_ui_mappings`` calls left 2-3 revisions and
could reuse a version number. A plan is the record Validate, Execute and the
decision artifacts bind to, so a lost edit means the operator approves
something other than what they edited.

These tests start every writer together on a barrier (threads, and separate
processes for the cross-process file lock) and then check that each edit is
in the plan exactly once, under unique consecutive versions.
"""

from __future__ import annotations

import json
import threading

import pytest

from services import transfer_plan_store as store
from services.transfer_plan_store import (
    PlanVersionConflict,
    add_mapping_revision,
    add_preflight_run,
    approve_plan_version,
    attach_job,
    create_plan,
    get_plan,
    sync_ui_mappings,
    update_plan,
)

WRITERS = 16


@pytest.fixture(autouse=True)
def isolated_plan_store(tmp_path, monkeypatch):
    path = tmp_path / "transfer_plans.json"
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", path)
    yield path


def _plan() -> str:
    return create_plan(
        {
            "source_columns": ["customer_id"],
            "source_schema": {"customer_id": "INTEGER"},
            "target_columns": ["customer_id"],
            "target_schema": {"customer_id": "int"},
        }
    ).id


def _mapping(tag: str) -> list[dict]:
    return [{"source": "customer_id", "target": "customer_id", "reason": tag}]


def _model(tag: str) -> dict:
    return {"collection": tag, "root": {"source": "public.customers", "key": ["customer_id"]}}


def _run_together(calls) -> list[BaseException]:
    """Start every call at once (a barrier, not sleeps) and collect errors."""
    barrier = threading.Barrier(len(calls))
    errors: list[BaseException] = []

    def run(call):
        barrier.wait()
        try:
            call()
        except BaseException as exc:  # noqa: BLE001 - reported by the test
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(c,)) for c in calls]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "a writer never finished"
    return errors


def _assert_every_edit_kept(plan_id: str, before: int, tags: list[str]) -> None:
    plan = get_plan(plan_id)
    added = plan.revisions[before:]
    versions = [r.version for r in added]
    assert len(added) == len(tags), f"{len(tags)} edits, {len(added)} revisions kept"
    assert versions == list(range(before + 1, before + len(tags) + 1)), versions
    assert sorted(r.mappings[0]["reason"] for r in added) == sorted(tags)
    assert plan.active_version == versions[-1]


def test_concurrent_ui_saves_each_become_one_revision():
    plan_id = _plan()
    tags = [f"edit-{i}" for i in range(WRITERS)]
    errors = _run_together(
        [
            (lambda t=t: sync_ui_mappings(plan_id, _mapping(t), document_model=_model(t)))
            for t in tags
        ]
    )
    assert errors == []
    _assert_every_edit_kept(plan_id, 0, tags)
    # The document model of each edit is stored with that edit's revision.
    for rev in get_plan(plan_id).revisions:
        assert rev.document_model["collection"] == rev.mappings[0]["reason"]


def test_concurrent_mapping_runs_and_ui_saves_interleave_without_loss():
    plan_id = _plan()
    ui = [f"ui-{i}" for i in range(WRITERS // 2)]
    auto = [f"map-{i}" for i in range(WRITERS // 2)]
    calls = [(lambda t=t: sync_ui_mappings(plan_id, _mapping(t))) for t in ui]
    calls += [
        (lambda t=t: add_mapping_revision(plan_id, {"mappings": _mapping(t)}))
        for t in auto
    ]
    assert _run_together(calls) == []
    _assert_every_edit_kept(plan_id, 0, ui + auto)


def test_every_plan_mutator_keeps_the_others_writes():
    """Preflight runs, job links, policy edits and approvals race mapping edits."""
    plan_id = _plan()
    sync_ui_mappings(plan_id, _mapping("base"))
    edits = [f"edit-{i}" for i in range(4)]
    calls = [(lambda t=t: sync_ui_mappings(plan_id, _mapping(t))) for t in edits]
    calls += [
        (lambda i=i: add_preflight_run(plan_id, {"run_id": f"run-{i}", "passed": True}))
        for i in range(4)
    ]
    calls += [(lambda i=i: attach_job(plan_id, f"job-{i}")) for i in range(4)]
    calls += [
        (lambda i=i: update_plan(plan_id, {"policies": {f"policy_{i}": i}}))
        for i in range(4)
    ]
    calls += [lambda: approve_plan_version(plan_id, 1)]
    assert _run_together(calls) == []

    plan = get_plan(plan_id)
    assert sorted(r.mappings[0]["reason"] for r in plan.revisions[1:]) == sorted(edits)
    assert [r.version for r in plan.revisions] == [1, 2, 3, 4, 5]
    assert sorted(run["id"] for run in plan.preflight_runs) == [f"run-{i}" for i in range(4)]
    assert sorted(plan.job_ids) == [f"job-{i}" for i in range(4)]
    assert {f"policy_{i}" for i in range(4)} <= set(plan.policies)


def test_concurrent_creates_and_edits_keep_every_plan():
    plan_id = _plan()
    names = [f"plan-{i}" for i in range(8)]
    edits = [f"edit-{i}" for i in range(8)]
    calls = [(lambda n=n: create_plan({"name": n})) for n in names]
    calls += [(lambda t=t: sync_ui_mappings(plan_id, _mapping(t))) for t in edits]
    assert _run_together(calls) == []

    stored = store._load_all()
    assert sorted(p.name for p in stored if p.id != plan_id) == sorted(names)
    _assert_every_edit_kept(plan_id, 0, edits)


#: A separate Python process that edits the plan once the parent says "go".
_WRITER = """
import sys
from pathlib import Path

from services import transfer_plan_store as store

store.STORE_PATH = Path(sys.argv[1])
plan_id, worker, edits = sys.argv[2], sys.argv[3], int(sys.argv[4])
print("ready", flush=True)
sys.stdin.readline()
for n in range(edits):
    store.sync_ui_mappings(
        plan_id, [{"source": "customer_id", "target": "customer_id", "reason": f"p{worker}-{n}"}]
    )
print("done", flush=True)
"""


def test_edits_from_separate_processes_are_all_kept(isolated_plan_store):
    """Two API processes (or replicas sharing one data volume) write the same file."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    plan_id = _plan()
    workers, edits = 4, 4
    env = {**os.environ, "PYTHONPATH": str(Path(store.__file__).resolve().parents[1])}
    procs = [
        subprocess.Popen(  # nosec B603 - this interpreter, fixed script
            [sys.executable, "-c", _WRITER, str(isolated_plan_store), plan_id, str(w), str(edits)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env=env,
        )
        for w in range(workers)
    ]
    try:
        # Barrier: every writer has imported the store before any starts.
        assert [p.stdout.readline().strip() for p in procs] == ["ready"] * workers
        for p in procs:
            p.stdin.write("go\n")
            p.stdin.flush()
        outs = [p.communicate(timeout=120)[0].strip() for p in procs]
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    assert [p.returncode for p in procs] == [0] * workers
    assert outs == ["done"] * workers
    tags = [f"p{w}-{n}" for w in range(workers) for n in range(edits)]
    _assert_every_edit_kept(plan_id, 0, tags)


def test_a_writer_that_cannot_get_the_store_fails_and_writes_nothing(
    isolated_plan_store, monkeypatch
):
    """Another process holding the store past the timeout: refuse, never write blind."""
    import fcntl

    plan_id = _plan()
    monkeypatch.setattr(store, "LOCK_TIMEOUT_SECONDS", 0.2)
    lock_path = isolated_plan_store.with_name(isolated_plan_store.name + ".lock")
    with open(lock_path, "a+") as other_process:
        fcntl.flock(other_process.fileno(), fcntl.LOCK_EX)
        with pytest.raises(store.PlanStoreBusy):
            sync_ui_mappings(plan_id, _mapping("blocked"))
        fcntl.flock(other_process.fileno(), fcntl.LOCK_UN)
    assert get_plan(plan_id).revisions == []
    sync_ui_mappings(plan_id, _mapping("after"))
    assert [r.version for r in get_plan(plan_id).revisions] == [1]


def test_plan_document_carries_a_version_bumped_by_every_write():
    plan_id = _plan()
    assert get_plan(plan_id).doc_version == 1
    sync_ui_mappings(plan_id, _mapping("a"))
    assert get_plan(plan_id).doc_version == 2
    add_preflight_run(plan_id, {"passed": True})
    attach_job(plan_id, "job-1")
    assert get_plan(plan_id).doc_version == 4
    assert get_plan(plan_id).to_dict()["doc_version"] == 4


def test_legacy_plan_without_a_version_loads_and_gets_one_on_first_write(isolated_plan_store):
    legacy = {
        "id": "legacy-plan",
        "name": "Stored before PS-1",
        "status": "mapped",
        "source": {},
        "destination": {},
        "source_columns": ["customer_id"],
        "source_schema": {"customer_id": "INTEGER"},
        "target_columns": ["customer_id"],
        "target_schema": {"customer_id": "int"},
        "revisions": [
            {
                "version": 1,
                "mappings": _mapping("legacy"),
                "transforms": [],
                "validation": {},
                "mapping_hash": "h",
                "source_schema_hash": "s",
                "target_schema_hash": "t",
                "agents_used": [],
            }
        ],
        "active_version": 1,
    }
    isolated_plan_store.write_text(json.dumps({"plans": [legacy]}), encoding="utf-8")

    loaded = get_plan("legacy-plan")
    assert loaded is not None and loaded.doc_version == 0
    assert loaded.active_revision().mappings == _mapping("legacy")

    updated = sync_ui_mappings("legacy-plan", _mapping("next"), base_version=1)
    assert updated.doc_version == 1
    raw = json.loads(isolated_plan_store.read_text(encoding="utf-8"))["plans"][0]
    assert raw["doc_version"] == 1
    assert [r["version"] for r in raw["revisions"]] == [1, 2]


def test_stale_base_version_is_refused_and_writes_nothing():
    plan_id = _plan()
    sync_ui_mappings(plan_id, _mapping("one"))
    sync_ui_mappings(plan_id, _mapping("two"), base_version=1)
    with pytest.raises(PlanVersionConflict) as caught:
        sync_ui_mappings(plan_id, _mapping("stale"), base_version=1)
    assert caught.value.current_version == 2
    assert caught.value.base_version == 1
    plan = get_plan(plan_id)
    assert [r.mappings[0]["reason"] for r in plan.revisions] == ["one", "two"]
    # No base version (today's web client): still appended, nothing lost.
    sync_ui_mappings(plan_id, _mapping("unversioned"))
    assert [r.version for r in get_plan(plan_id).revisions] == [1, 2, 3]


def test_racing_saves_from_the_same_base_one_wins_the_others_conflict():
    plan_id = _plan()
    sync_ui_mappings(plan_id, _mapping("base"))
    outcomes: list[str] = []

    def save(tag):
        try:
            sync_ui_mappings(plan_id, _mapping(tag), base_version=1)
            outcomes.append("saved")
        except PlanVersionConflict:
            outcomes.append("conflict")

    assert _run_together([(lambda t=t: save(t)) for t in ("a", "b", "c", "d")]) == []
    assert sorted(outcomes) == ["conflict", "conflict", "conflict", "saved"]
    assert [r.version for r in get_plan(plan_id).revisions] == [1, 2]


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    return TestClient(app)


def test_mappings_route_answers_409_for_a_stale_base_version(tmp_path, monkeypatch):
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    plan_id = _plan()
    client = _client()
    url = f"/api/v1/transfer/plans/{plan_id}/mappings"
    mapping = [{"source": "customer_id", "target": "customer_id", "confidence": 1.0}]

    first = client.post(url, json={"mappings": mapping, "base_version": 0})
    assert first.status_code == 200, first.text
    assert first.json()["plan"]["active_version"] == 1

    second = client.post(url, json={"mappings": mapping, "base_version": 1})
    assert second.status_code == 200, second.text

    stale = client.post(url, json={"mappings": mapping, "base_version": 1})
    assert stale.status_code == 409, stale.text
    detail = stale.json()["detail"]
    assert detail["code"] == "plan_version_conflict"
    assert detail["current_version"] == 2
    assert detail["base_version"] == 1
    assert [r.version for r in get_plan(plan_id).revisions] == [1, 2]

    # A client that sends no base version (today's web app) keeps working.
    legacy = client.post(url, json={"mappings": mapping})
    assert legacy.status_code == 200, legacy.text
    assert legacy.json()["plan"]["active_version"] == 3


def test_a_write_started_inside_another_write_is_refused():
    """The outer write would overwrite whatever the nested one committed."""
    plan_id = _plan()
    before = get_plan(plan_id).doc_version
    with pytest.raises(store.PlanStoreReentry):
        store._mutate(plan_id, lambda _plan: attach_job(plan_id, "job-nested"))
    with pytest.raises(store.PlanStoreReentry):
        store._mutate(plan_id, lambda _plan: create_plan({"name": "nested"}))
    plan = get_plan(plan_id)
    assert "job-nested" not in plan.job_ids
    assert plan.doc_version == before
    assert [p.name for p in store._load_all()] == ["Transfer plan"]
    # The store is usable afterwards (the refusal released everything).
    attach_job(plan_id, "job-after")
    assert get_plan(plan_id).job_ids == ["job-after"]


def test_the_lock_file_is_private_opened_for_writing_and_never_written(isolated_plan_store, monkeypatch):
    """Owner-only, opened read-write, empty.

    On NFS/EFS Linux emulates flock() with fcntl locks, and an exclusive fcntl
    lock needs a descriptor open for writing: a read-only one fails with EBADF
    and every plan write would fail on a shared volume.
    """
    import fcntl
    import os
    import stat

    lock = isolated_plan_store.with_name("transfer_plans.json.lock")
    access_modes: list[int] = []
    real_open = os.open

    def recording_open(path, flags, *args, **kwargs):
        fd = real_open(path, flags, *args, **kwargs)
        if str(path) == str(lock):
            access_modes.append(fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE)
        return fd

    with monkeypatch.context() as patched:
        patched.setattr(store.os, "open", recording_open)
        plan_id = _plan()
        sync_ui_mappings(plan_id, _mapping("x"))

    assert access_modes and set(access_modes) == {os.O_RDWR}, access_modes
    assert stat.S_IMODE(os.stat(lock).st_mode) & 0o077 == 0
    assert os.stat(lock).st_size == 0, "the lock file is never written"
