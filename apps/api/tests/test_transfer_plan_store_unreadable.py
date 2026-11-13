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
    assert str(isolated_plan_store) in message
    assert "transfer_plans.json.bak" in message
    assert caught.value.error_class == "UnicodeDecodeError"


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
