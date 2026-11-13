"""A transfer-plan store file that cannot be read must never be overwritten.

``_load_all()`` used to answer ``[]`` for a store file it could not read or
parse (truncated JSON, an empty file, garbage, permission denied). Reads then
showed no plans, and the next write — creating a plan, saving a mapping —
rewrote the file with only the plan in hand: every other plan was gone.

Now an absent file is an empty store, and a present but unreadable one raises
``PlanStoreUnreadable``: nothing is written, the file stays exactly as it was,
and the API answers 503 naming the file and its backup. Each write keeps the
previous good file as ``transfer_plans.json.bak`` for the operator; reads
never fall back to it.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from services import transfer_plan_store as store
from services.transfer_plan_store import (
    PlanStoreUnreadable,
    add_mapping_revision,
    add_preflight_run,
    approve_plan_version,
    assign_unscoped_workspaces,
    attach_job,
    create_plan,
    get_plan,
    list_plans,
    sync_ui_mappings,
    update_plan,
)

MAPPING = [{"source": "customer_id", "target": "customer_id"}]


@pytest.fixture(autouse=True)
def isolated_plan_store(tmp_path, monkeypatch):
    path = tmp_path / "transfer_plans.json"
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", path)
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield path


def _two_plans() -> tuple[str, str]:
    a = create_plan({"name": "A"}).id
    b = create_plan({"name": "B"}).id
    sync_ui_mappings(a, MAPPING)
    return a, b


def _truncated(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    path.write_text(text[: len(text) // 2], encoding="utf-8")


def _empty(path: Path) -> None:
    path.write_bytes(b"")


def _garbage(path: Path) -> None:
    path.write_bytes(b"\x00\xff\xfe\x89PNG\r\n\x1a\n\x00garbage")


def _not_a_store(path: Path) -> None:
    path.write_text(json.dumps(["not", "a", "plan", "store"]), encoding="utf-8")


def _bad_record(path: Path) -> None:
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["plans"].append({"name": "record without an id"})
    path.write_text(json.dumps(raw), encoding="utf-8")


CORRUPTIONS = {
    "truncated": _truncated,
    "empty": _empty,
    "binary_garbage": _garbage,
    "wrong_shape": _not_a_store,
    "unparseable_record": _bad_record,
}


def _every_mutator(a: str):
    return [
        lambda: create_plan({"name": "C"}),
        lambda: sync_ui_mappings(a, MAPPING),
        lambda: add_mapping_revision(a, {"mappings": MAPPING}),
        lambda: add_preflight_run(a, {"passed": True}),
        lambda: update_plan(a, {"name": "renamed"}),
        lambda: approve_plan_version(a, 1),
        lambda: attach_job(a, "job-1"),
        lambda: assign_unscoped_workspaces(lambda _plan: "ws-1", apply=True),
    ]


@pytest.mark.parametrize("corrupt", list(CORRUPTIONS), ids=list(CORRUPTIONS))
def test_an_unreadable_store_is_never_overwritten(isolated_plan_store, corrupt):
    a, _b = _two_plans()
    CORRUPTIONS[corrupt](isolated_plan_store)
    damaged = isolated_plan_store.read_bytes()
    bak = isolated_plan_store.with_name("transfer_plans.json.bak")
    backup = bak.read_bytes() if bak.exists() else None

    raised = []
    for call in [lambda: get_plan(a), lambda: list_plans(), *_every_mutator(a)]:
        try:
            call()
            raised.append(False)
        except PlanStoreUnreadable:
            raised.append(True)
        assert isolated_plan_store.read_bytes() == damaged, "the damaged file was rewritten"
    assert all(raised), f"calls that did not refuse the damaged store: {raised}"
    assert (bak.read_bytes() if bak.exists() else None) == backup


def test_permission_denied_is_unreadable_not_empty(isolated_plan_store, monkeypatch):
    a, _b = _two_plans()
    before = isolated_plan_store.read_bytes()
    real_read_text = Path.read_text

    def denied(self, *args, **kwargs):
        if self == isolated_plan_store:
            raise PermissionError(13, "Permission denied", str(self))
        return real_read_text(self, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(Path, "read_text", denied)
        with pytest.raises(PlanStoreUnreadable) as caught:
            get_plan(a)
        assert caught.value.error_class == "PermissionError"
        with pytest.raises(PlanStoreUnreadable):
            create_plan({"name": "C"})
    assert isolated_plan_store.read_bytes() == before
    assert {p.name for p in list_plans()} == {"A", "B"}


def test_no_plan_is_lost_once_the_file_is_repaired(isolated_plan_store):
    a, b = _two_plans()
    good = isolated_plan_store.read_bytes()
    _truncated(isolated_plan_store)
    with pytest.raises(PlanStoreUnreadable):
        create_plan({"name": "C"})
    isolated_plan_store.write_bytes(good)  # the operator repairs the file
    assert {p.id for p in list_plans()} == {a, b}
    assert len(get_plan(a).revisions) == 1


def test_an_absent_store_is_an_empty_store(isolated_plan_store):
    assert not isolated_plan_store.exists()
    assert list_plans() == []
    assert get_plan("missing") is None
    assert sync_ui_mappings("missing", MAPPING) is None
    plan = create_plan({"name": "first"})
    assert [p.id for p in list_plans()] == [plan.id]


def test_each_write_keeps_the_previous_good_file_as_a_backup(isolated_plan_store):
    backup = isolated_plan_store.with_name("transfer_plans.json.bak")
    a = create_plan({"name": "A"}).id
    assert not backup.exists(), "nothing to back up before the first write"
    b = create_plan({"name": "B"}).id
    assert [p["id"] for p in json.loads(backup.read_text())["plans"]] == [a]
    before_sync = isolated_plan_store.read_bytes()
    sync_ui_mappings(a, MAPPING)
    assert backup.read_bytes() == before_sync
    assert {p["id"] for p in json.loads(isolated_plan_store.read_text())["plans"]} == {a, b}


def test_reads_never_fall_back_to_the_backup(isolated_plan_store):
    a, _b = _two_plans()
    _garbage(isolated_plan_store)
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    message = str(caught.value)
    assert "transfer_plans.json" in message and "transfer_plans.json.bak" in message
    assert caught.value.error_class == "UnicodeDecodeError"
    # The full locations stay on the exception (for the log), not in its text.
    assert caught.value.path == str(isolated_plan_store)
    assert caught.value.backup_path == str(isolated_plan_store) + ".bak"


def test_refusal_texts_never_carry_the_data_directory(isolated_plan_store, monkeypatch):
    """str(exc) reaches clients (async Validate's job error): file names only."""
    a, _b = _two_plans()
    data_dir = str(isolated_plan_store.parent)
    _truncated(isolated_plan_store)
    with pytest.raises(PlanStoreUnreadable) as unreadable:
        get_plan(a)
    assert data_dir not in str(unreadable.value)

    isolated_plan_store.write_bytes(isolated_plan_store.with_name("transfer_plans.json.bak").read_bytes())
    monkeypatch.setattr(store, "LOCK_TIMEOUT_SECONDS", 0.05)
    import fcntl

    with open(isolated_plan_store.with_name("transfer_plans.json.lock"), "a+") as other:
        fcntl.flock(other.fileno(), fcntl.LOCK_EX)
        with pytest.raises(store.PlanStoreBusy) as busy:
            update_plan(a, {"name": "x"})
    assert data_dir not in str(busy.value)
    assert "transfer_plans.json" in str(busy.value)


class _InlineThread:
    """Runs the async Validate worker inline, so its outcome is ready on return."""

    def __init__(self, *, target, args=(), **_kwargs):
        self._target, self._args = target, args

    def start(self):
        self._target(*self._args)


@pytest.mark.parametrize("failure", ["unreadable", "busy", "other"])
def test_async_validate_job_error_does_not_leak_the_data_directory(
    isolated_plan_store, monkeypatch, failure
):
    from types import SimpleNamespace

    import services.plan_preflight_job as jobs

    a, _b = _two_plans()
    data_dir = str(isolated_plan_store.parent)
    # Real layout: uploads are a sibling of the data directory (apps/api/uploads,
    # <volume>/uploads), not inside it.
    upload_dir = isolated_plan_store.parent.parent / f"uploads-{isolated_plan_store.parent.name}"
    monkeypatch.delenv("DATAWRAP_UPLOAD_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(upload_dir))

    def failing_preflight(plan_id, **_kwargs):
        if failure == "unreadable":
            raise PlanStoreUnreadable(isolated_plan_store, ValueError("bad json"))
        if failure == "busy":
            raise store.PlanStoreBusy(isolated_plan_store)
        raise FileNotFoundError(2, "No such file or directory", str(upload_dir / "f_1_people.csv"))

    monkeypatch.setattr(jobs, "threading", SimpleNamespace(Thread=_InlineThread))
    monkeypatch.setattr("services.transfer_plan_service.run_plan_preflight", failing_preflight)
    jobs.reset_plan_preflight_jobs()
    try:
        accepted = jobs.start_plan_preflight_job(a)
        view = jobs.get_plan_preflight_job(a, accepted["run_id"])
    finally:
        jobs.reset_plan_preflight_jobs()
    assert view["status"] == "failed"
    assert view["error"], "the failure is still reported"
    assert data_dir not in view["error"], view["error"]
    assert str(upload_dir) not in view["error"], view["error"]
    if failure == "other":
        assert "f_1_people.csv" in view["error"]
    else:
        assert "transfer_plans.json" in view["error"]


def test_a_relative_data_dir_setting_does_not_garble_job_errors(monkeypatch):
    """Only absolute roots are replaced: DATAFLOW_DATA_DIR=data must not touch prose."""
    from services.plan_preflight_job import public_job_error

    monkeypatch.delenv("DATAWRAP_DATA_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", "data")
    message = "the data directory is full; uploads could not be staged"
    assert public_job_error(RuntimeError(message)) == message


def test_the_failure_is_logged_once_with_path_and_error_class_only(isolated_plan_store, caplog):
    a, _b = _two_plans()
    secret_name = "PLAN-NAME-SENTINEL"
    update_plan(a, {"name": secret_name})
    _truncated(isolated_plan_store)
    caplog.set_level(logging.ERROR, logger=store.logger.name)
    for _ in range(3):
        with pytest.raises(PlanStoreUnreadable):
            get_plan(a)
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR and r.name == store.logger.name]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    text = errors[0].getMessage()
    assert str(isolated_plan_store) in text and "JSONDecodeError" in text
    assert secret_name not in text and not errors[0].exc_info


def _client():
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_plan_routes_answer_503_naming_the_file_when_the_store_is_unreadable(isolated_plan_store):
    a, _b = _two_plans()
    _truncated(isolated_plan_store)
    damaged = isolated_plan_store.read_bytes()
    client = _client()
    responses = [
        client.get("/api/v1/transfer/plans"),
        client.get(f"/api/v1/transfer/plans/{a}"),
        client.post("/api/v1/transfer/plans", json={"name": "C"}),
        client.post(f"/api/v1/transfer/plans/{a}/mappings", json={"mappings": MAPPING}),
        client.patch(f"/api/v1/transfer/plans/{a}", json={"name": "renamed"}),
        client.post(f"/api/v1/transfer/plans/{a}/approve", json={}),
    ]
    for res in responses:
        assert res.status_code == 503, (res.request.method, res.request.url.path, res.text)
        body = res.json()
        assert body["error"] == "plan_store_unreadable"
        assert "transfer_plans.json.bak" in body["detail"]
    assert isolated_plan_store.read_bytes() == damaged


def test_a_failed_write_leaves_the_backup_alone_and_unlinked(isolated_plan_store, monkeypatch):
    """The backup is only rotated by a write that landed, and never shares the store's inode."""
    import os

    a, _b = _two_plans()
    bak = isolated_plan_store.with_name("transfer_plans.json.bak")
    backup_before = bak.read_bytes()
    store_before = isolated_plan_store.read_bytes()

    def disk_full(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store, "write_json_atomic", disk_full)
    with pytest.raises(OSError):
        update_plan(a, {"name": "never written"})
    monkeypatch.undo()
    assert isolated_plan_store.read_bytes() == store_before
    assert bak.read_bytes() == backup_before
    assert os.stat(bak).st_ino != os.stat(isolated_plan_store).st_ino
    assert not [p.name for p in isolated_plan_store.parent.iterdir() if p.name.endswith(".staging")]


def test_a_partial_backup_copy_leaves_no_staging_file(isolated_plan_store, monkeypatch):
    """Hard link refused (other device) and the copy fails half-way: nothing is left behind."""
    import errno

    a, _b = _two_plans()
    bak = isolated_plan_store.with_name("transfer_plans.json.bak")
    backup_before, store_before = bak.read_bytes(), isolated_plan_store.read_bytes()

    def no_link(*_args):
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    def partial_copy(_src, dst, *_args, **_kwargs):
        Path(dst).write_text("partial")
        raise OSError(errno.ENOSPC, "No space left on device")

    with monkeypatch.context() as patched:
        patched.setattr(store.os, "link", no_link)
        patched.setattr(store.shutil, "copy2", partial_copy)
        with pytest.raises(OSError):
            attach_job(a, "job-1")
    assert not [p.name for p in isolated_plan_store.parent.iterdir() if p.name.endswith(".staging")]
    assert isolated_plan_store.read_bytes() == store_before
    assert bak.read_bytes() == backup_before


def test_a_write_that_landed_is_not_reported_failed_when_the_backup_rename_fails(
    isolated_plan_store, caplog, monkeypatch
):
    """The new store is committed; a failing .bak rename must not make the caller retry it."""
    import os

    a, _b = _two_plans()
    real_replace = os.replace

    def failing_backup_rename(src, dst):
        if str(src).endswith(".staging"):
            raise OSError(30, "Read-only file system")
        return real_replace(src, dst)

    caplog.set_level(logging.WARNING, logger=store.logger.name)
    with monkeypatch.context() as patched:
        patched.setattr(store.os, "replace", failing_backup_rename)
        updated = attach_job(a, "job-2")
    assert updated is not None and updated.job_ids == ["job-2"]
    assert get_plan(a).job_ids == ["job-2"]
    assert not [p.name for p in isolated_plan_store.parent.iterdir() if p.name.endswith(".staging")]
    assert any("transfer_plans.json.bak" in r.getMessage() for r in caplog.records)


def test_a_failed_final_backup_rename_keeps_every_backup_generation(
    isolated_plan_store, caplog, monkeypatch
):
    """If staging cannot become .bak, no existing generation is dropped (PS-34: the
    older ones have already been shifted by copy, so .bak and .bak.1 hold the
    same generation), and the warning says the rotation stopped."""
    import os

    a = create_plan({"name": "A"}).id
    for i in range(3):
        update_plan(a, {"name": f"A{i}"})
    names = ("transfer_plans.json.bak", "transfer_plans.json.bak.1", "transfer_plans.json.bak.2")
    before = {n: isolated_plan_store.with_name(n).read_bytes() for n in names}
    real_replace = os.replace

    def failing_final_rename(src, dst):
        if str(src).endswith(".staging") and str(dst).endswith(".bak"):
            raise OSError(5, "Input/output error")
        return real_replace(src, dst)

    caplog.set_level(logging.WARNING, logger=store.logger.name)
    with monkeypatch.context() as patched:
        patched.setattr(store.os, "replace", failing_final_rename)
        update_plan(a, {"name": "landed"})
    assert get_plan(a).name == "landed"
    after = [isolated_plan_store.with_name(n).read_bytes() for n in names]
    assert before["transfer_plans.json.bak"] in after and before["transfer_plans.json.bak.1"] in after
    leftovers = [p.name for p in isolated_plan_store.parent.iterdir() if p.name.startswith(".") and "bak" in p.name]
    assert leftovers == []
    warning = next(r.getMessage() for r in caplog.records if "backup" in r.getMessage())
    assert "not rotated" in warning and "no existing backup generation was dropped" in warning


def test_a_store_saved_with_a_utf8_bom_is_readable(isolated_plan_store):
    a, b = _two_plans()
    isolated_plan_store.write_bytes(b"\xef\xbb\xbf" + isolated_plan_store.read_bytes())
    assert {p.id for p in list_plans()} == {a, b}
    sync_ui_mappings(a, MAPPING)
    assert len(get_plan(a).revisions) == 2


def test_three_backup_generations_are_kept(isolated_plan_store):
    a = create_plan({"name": "A"}).id
    snapshots = []
    for i in range(4):
        snapshots.append(isolated_plan_store.read_bytes())
        update_plan(a, {"name": f"A{i}"})
    gen = [isolated_plan_store.with_name(n) for n in ("transfer_plans.json.bak", "transfer_plans.json.bak.1", "transfer_plans.json.bak.2")]
    assert [g.read_bytes() for g in gen] == [snapshots[3], snapshots[2], snapshots[1]]
    assert not isolated_plan_store.with_name("transfer_plans.json.bak.3").exists()


def test_the_refusal_names_the_newest_readable_backup(isolated_plan_store):
    a = create_plan({"name": "A"}).id
    for i in range(3):
        update_plan(a, {"name": f"A{i}"})
    _truncated(isolated_plan_store)
    _garbage(isolated_plan_store.with_name("transfer_plans.json.bak"))
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup == "transfer_plans.json.bak.1"
    assert "transfer_plans.json.bak.1" in str(caught.value)
    assert caught.value.backup_path == str(isolated_plan_store) + ".bak.1"

    client = _client()
    res = client.get(f"/api/v1/transfer/plans/{a}")
    assert res.status_code == 503
    assert "transfer_plans.json.bak.1" in res.json()["detail"]

    for name in ("transfer_plans.json.bak.1", "transfer_plans.json.bak.2"):
        _garbage(isolated_plan_store.with_name(name))
    with pytest.raises(PlanStoreUnreadable) as none_left:
        get_plan(a)
    assert none_left.value.readable_backup == ""
    assert "no readable backup" in str(none_left.value)
    assert "no readable backup" in client.get(f"/api/v1/transfer/plans/{a}").json()["detail"]


@pytest.mark.parametrize(
    "bad_backup",
    [
        {"plans": [1, 2]},
        {"plans": [{"name": "row without an id"}]},
        {"plans": [{"id": "x", "revisions": [{"mappings": []}]}]},  # revision without a version
    ],
    ids=["scalar_rows", "row_without_id", "revision_without_version"],
)
def test_a_backup_the_loader_would_reject_is_never_named(isolated_plan_store, bad_backup):
    import json

    a = create_plan({"name": "A"}).id
    for i in range(3):
        update_plan(a, {"name": f"A{i}"})
    _truncated(isolated_plan_store)
    isolated_plan_store.with_name("transfer_plans.json.bak").write_text(json.dumps(bad_backup))
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup == "transfer_plans.json.bak.1"


def test_a_lock_file_that_cannot_be_opened_for_writing_is_a_clear_refusal(isolated_plan_store, monkeypatch):
    """E.g. chmod 0400 by hand (root ignores modes, so the refusal is injected)."""
    import errno
    import os

    a, _b = _two_plans()
    store_before = isolated_plan_store.read_bytes()
    lock = str(isolated_plan_store.with_name("transfer_plans.json.lock"))
    real_open = os.open

    def denied(path, flags, *args, **kwargs):
        if str(path) == lock:
            raise PermissionError(errno.EACCES, "Permission denied", lock)
        return real_open(path, flags, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(store.os, "open", denied)
        with pytest.raises(store.PlanStoreLockUnavailable) as caught:
            update_plan(a, {"name": "x"})
        assert str(isolated_plan_store.parent) not in str(caught.value)
        assert "transfer_plans.json.lock" in str(caught.value) and "operator" in str(caught.value)
        res = _client().patch(f"/api/v1/transfer/plans/{a}", json={"name": "x"})
    assert res.status_code == 503, res.text
    body = res.json()
    assert body["error"] == "plan_store_lock_unavailable"
    assert "transfer_plans.json.lock" in body["detail"] and str(isolated_plan_store.parent) not in body["detail"]
    assert isolated_plan_store.read_bytes() == store_before


# ---------------------------------------------------------------------------
# PS-34: backup rotation never drops a generation, at any step
# ---------------------------------------------------------------------------

GENERATIONS = ("transfer_plans.json.bak", "transfer_plans.json.bak.1", "transfer_plans.json.bak.2")


def _three_generations(isolated_plan_store):
    a = create_plan({"name": "A"}).id
    for i in range(3):
        update_plan(a, {"name": f"A{i}"})
    contents = {n: isolated_plan_store.with_name(n).read_bytes() for n in GENERATIONS}
    contents["store"] = isolated_plan_store.read_bytes()
    return a, contents


@pytest.mark.parametrize("failing_step", ["bak1_to_bak2", "bak_to_bak1", "staging_to_bak"])
def test_a_rotation_failure_at_any_step_loses_no_generation(isolated_plan_store, monkeypatch, failing_step):
    import os

    a, before = _three_generations(isolated_plan_store)
    real_replace = os.replace

    def failing(src, dst):
        dst = str(dst)
        step = (
            "bak1_to_bak2" if dst.endswith(".bak.2")
            else "bak_to_bak1" if dst.endswith(".bak.1")
            else "staging_to_bak" if dst.endswith(".bak")
            else None
        )
        if step == failing_step:
            raise OSError(5, "Input/output error")
        return real_replace(src, dst)

    with monkeypatch.context() as patched:
        patched.setattr(store.os, "replace", failing)
        update_plan(a, {"name": "landed"})
    assert get_plan(a).name == "landed"
    # No existing backup generation is dropped: the two newest are still on disk
    # (.bak.2, the oldest, is the one a completed rotation retires). Only the
    # generation being added (the replaced store) may not have made it in.
    now = [isolated_plan_store.with_name(n).read_bytes() for n in GENERATIONS]
    for name in GENERATIONS[:2]:
        assert before[name] in now, f"{name} was lost when {failing_step} failed"
    leftovers = [p.name for p in isolated_plan_store.parent.iterdir() if p.name.endswith(".staging")]
    assert leftovers == []


def test_hard_linked_generations_rotate_without_leaving_staging_files(isolated_plan_store):
    """rename() between two links to one inode is a no-op; rotation must still finish."""
    import os

    a = create_plan({"name": "A"}).id
    update_plan(a, {"name": "B"})
    bak, bak1, bak2 = (isolated_plan_store.with_name(n) for n in GENERATIONS)
    bak1.unlink(missing_ok=True)
    bak2.unlink(missing_ok=True)
    os.link(bak, bak1)
    os.link(bak, bak2)
    for i in range(3):
        update_plan(a, {"name": f"C{i}"})
    assert [p.name for p in isolated_plan_store.parent.iterdir() if p.name.endswith(".staging")] == []
    assert len({bak.read_bytes(), bak1.read_bytes(), bak2.read_bytes()}) == 3


def test_staging_files_left_by_a_crashed_writer_are_swept(isolated_plan_store):
    a = create_plan({"name": "A"}).id
    stale = [
        isolated_plan_store.with_name(".transfer_plans.json.bak.99999.staging"),
        isolated_plan_store.with_name(".transfer_plans.json.bak.1.99999.staging"),
    ]
    for path in stale:
        path.write_text("{}")
    update_plan(a, {"name": "B"})
    assert not any(p.exists() for p in stale)


def test_rotation_fsyncs_the_directory(isolated_plan_store, monkeypatch):
    a = create_plan({"name": "A"}).id
    synced: list[str] = []
    monkeypatch.setattr(store, "fsync_directory", lambda d: synced.append(str(d)))
    update_plan(a, {"name": "B"})
    assert str(isolated_plan_store.parent) in synced


def test_the_readable_backup_is_not_reparsed_on_every_refused_request(isolated_plan_store, monkeypatch):
    a = create_plan({"name": "A"}).id
    for i in range(3):
        update_plan(a, {"name": f"A{i}"})
    _truncated(isolated_plan_store)
    _garbage(isolated_plan_store.with_name("transfer_plans.json.bak"))
    parsed: list[str] = []
    real = store._parse_store_text

    def counting(text):
        parsed.append(text[:10])
        return real(text)

    monkeypatch.setattr(store, "_parse_store_text", counting)
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup == "transfer_plans.json.bak.1"
    first = len(parsed)  # the store, then .bak.1 (.bak is not even text)
    for _ in range(4):
        with pytest.raises(PlanStoreUnreadable) as caught:
            get_plan(a)
        assert caught.value.readable_backup == "transfer_plans.json.bak.1"
    assert len(parsed) == first + 4, "each request parses the store; unchanged backups are not parsed again"

    # A backup that changes is looked at again.
    import os
    bak1 = isolated_plan_store.with_name("transfer_plans.json.bak.1")
    _garbage(bak1)
    os.utime(bak1, ns=(1, 1))
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup == "transfer_plans.json.bak.2"


def test_a_backup_replaced_with_the_same_size_and_mtime_is_looked_at_again(isolated_plan_store):
    """`cp -p`, a restore tool or a coarse clock can give a replacement file the
    old size and mtime; its inode / ctime still tell it apart (review probe)."""
    import os

    a = create_plan({"name": "A"}).id
    update_plan(a, {"name": "A1"})
    _truncated(isolated_plan_store)
    bak = isolated_plan_store.with_name("transfer_plans.json.bak")
    good = bak.read_text(encoding="utf-8")
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup == "transfer_plans.json.bak"

    st = bak.stat()
    keep = bak.with_name("keep-inode-busy")  # so the new file cannot reuse the inode
    os.link(bak, keep)
    os.unlink(bak)
    bak.write_text("X" * len(good), encoding="utf-8")
    os.utime(bak, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert (bak.stat().st_size, bak.stat().st_mtime_ns) == (st.st_size, st.st_mtime_ns)
    with pytest.raises(PlanStoreUnreadable) as caught:
        get_plan(a)
    assert caught.value.readable_backup != "transfer_plans.json.bak"
    keep.unlink()
