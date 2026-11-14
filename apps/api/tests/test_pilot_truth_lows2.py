"""Pilot ground truth — the second review's low findings.

* Two workers on one spill file: the last writer won and the other worker's
  result (or approval, or consume) was gone.
* A legacy plaintext approval stayed on disk after load until the next write.
* A failed ``create_connector`` echoed a URL-with-password ``host`` in its
  output, and failed tool results reached a model ungated.
* The context reuse cache grew with every actor and never shrank.
* "is anything failing right now" was answered with the all-time failures.
* A chain of elliptical follow-ups lost its window after about six turns.
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)
URL_PASSWORD = "UrlPw-FAKE-888"


@pytest.fixture
def keyed(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", base64.urlsafe_b64encode(os.urandom(32)).decode())


# ---- concurrent spill writers -------------------------------------------------


def test_two_workers_on_one_result_spill_keep_both_results(keyed, tmp_path):
    from src.ai.copilot.result_store import PilotResultStore

    path = tmp_path / "pilot_results.json"
    first, second = PilotResultStore(path=path), PilotResultStore(path=path)
    r1 = first.put(rows=[{"id": 1}], columns=["id"], session_id="s1")
    r2 = second.put(rows=[{"id": 2}], columns=["id"], session_id="s2")  # loaded before r1 was written
    restarted = PilotResultStore(path=path)
    assert restarted.get(r1) is not None, "the first worker's result was overwritten"
    assert restarted.get(r2) is not None


def test_two_workers_on_one_ack_file_keep_both_approvals_and_the_consume(keyed, tmp_path):
    from src.ai.copilot.ack_ledger import PilotAckLedger

    path = tmp_path / "pilot_acks.json"
    first, second = PilotAckLedger(path=path), PilotAckLedger(path=path)
    a1 = first.put(kind="create_connector", payload={"name": "a", "password": "pw-a"})
    second_view = PilotAckLedger(path=path)  # a second worker that has a1 pending
    a2 = second.put(kind="create_connector", payload={"name": "b", "password": "pw-b"})
    payload, err = first.consume(a1, actor="alice")
    assert payload and not err
    second_view.put(kind="create_connector", payload={"name": "c"})  # writes after the consume
    restarted = PilotAckLedger(path=path)
    assert restarted.peek(a2) is not None, "the second worker's approval was overwritten"
    again, err = restarted.consume(a1, actor="mallory")
    assert again is None and "already" in err, "a consumed approval came back pending"


def test_legacy_plaintext_acks_are_sealed_at_load(keyed, tmp_path):
    import time

    from src.ai.copilot.ack_ledger import PilotAckLedger

    path = tmp_path / "pilot_acks.json"
    now = time.time()
    path.write_text(json.dumps({"entries": {"ack_legacy": {
        "ack_id": "ack_legacy", "kind": "create_connector", "created_at": now, "expires_at": now + 600,
        "payload": {"name": "legacy", "password": "Legacy-PW-123"}, "preview": {},
        "consumed_at": None,
    }}}))
    ledger = PilotAckLedger(path=path)
    assert "Legacy-PW-123" not in path.read_text(), "legacy plaintext stays on disk after load"
    assert ledger.peek("ack_legacy") is not None


# ---- URL userinfo and failed tool outputs -------------------------------------


def test_redact_payload_masks_url_userinfo():
    from src.ai.copilot.ack_ledger import redact_payload

    shown = redact_payload({"host": f"postgresql://svc:{URL_PASSWORD}@db.internal:5432/app",
                            "nested": {"endpoint": f"https://u:{URL_PASSWORD}@x.example"}})
    assert URL_PASSWORD not in json.dumps(shown), shown
    assert "db.internal" in shown["host"]


def test_a_failed_create_connector_does_not_echo_a_url_password(monkeypatch):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.pilot_egress import tool_results_for_model
    from src.ai.copilot.tool_permissions import caller_role
    from src.ai.copilot.tools import get_pilot_tools

    pt.reset_workspace(monkeypatch)
    with bound_workspace_filter(WorkspaceFilter.only("ws-A"), stamp="ws-A", actor="alice@a.example"), \
            caller_role("admin"):
        result = get_pilot_tools().execute("create_connector", {
            "name": "PG by url", "type": "postgresql", "database": "app", "username": "svc", "test_first": False,
            "host": f"postgresql://svc:{URL_PASSWORD}@db.internal:5432/app",
        })
    assert not result.success
    blob = json.dumps({"output": result.output, "error": result.error}, default=str)
    assert URL_PASSWORD not in blob, blob
    model = tool_results_for_model([result])[0]
    assert URL_PASSWORD not in json.dumps({"o": model.output, "e": model.error}, default=str)


def test_a_failed_tool_result_is_gated_like_a_successful_one(monkeypatch):
    from src.ai.copilot.pilot_egress import tool_results_for_model
    from src.ai.copilot.tools import ToolResult

    monkeypatch.delenv("DATAFLOW_AI_METADATA_ONLY", raising=False)
    failed = ToolResult(
        name="run_query", success=False,
        output={"rows": [{"email": pt.pii_email(4)}], "row_count": 1, "columns": ["email"]},
        error=f"duplicate key (email)=({pt.pii_email(4)}) already exists",
    )
    model = tool_results_for_model([failed])[0]
    blob = json.dumps({"o": model.output, "e": model.error}, default=str)
    assert pt.pii_email(4) not in blob, blob
    assert model.success is False


# ---- the context reuse cache --------------------------------------------------


def test_the_context_reuse_cache_is_bounded(monkeypatch):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot import context_builder

    context_builder.reset_context_cooldowns()
    for i in range(context_builder.MAX_REUSED_READS + 50):
        with bound_workspace_filter(WorkspaceFilter.only(f"ws-{i}"), stamp=f"ws-{i}", actor=f"user{i}@x"):
            context_builder._reused("jobs", lambda: {"total": 1})()
    assert len(context_builder._recent) <= context_builder.MAX_REUSED_READS
    context_builder.reset_context_cooldowns()


# ---- smaller items ------------------------------------------------------------


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(3):
        pt.seed_job("failed", at=NOW - timedelta(days=2, minutes=i))
    pt.seed_job("running", at=NOW - timedelta(minutes=1))
    pt.seed_job("failed", at=NOW - timedelta(hours=2))
    return pt.agent()


@pytest.mark.parametrize("question", ["is anything failing right now", "are any jobs failing right now",
                                      "how many jobs have failed currently"])
def test_failing_right_now_is_not_answered_with_the_whole_history(ledger, question):
    response = pt.ask(ledger, question)
    assert 4 not in pt.bold_ints(response.answer), response.answer
    assert not pt.grounded(response) or "running" in response.answer.lower(), response.answer


def test_a_long_follow_up_chain_keeps_its_window(ledger):
    agent, history = ledger, []
    turns = ["how many jobs failed today", "and yesterday?", "and completed?", "and failed?", "and running?",
             "and completed?", "and failed?", "and running?", "and failed?"]
    for question in turns:
        response = pt.ask(agent, question, list(history), pilot_session_id="chain")
        history.extend([{"role": "user", "content": question}, {"role": "assistant", "content": response.answer}])
    assert "yesterday" in response.answer, response.answer
    assert 4 not in pt.bold_ints(response.answer)
