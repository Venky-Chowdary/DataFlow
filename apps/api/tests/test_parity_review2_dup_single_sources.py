"""Re-review duplication: Parity Studio uses the platform's one source for each rule.

* a finished job: ``services.job_trust.is_terminal_status`` (was a copied set);
* Gate-8's culprit-key cap: ``reconcile_step._CULPRIT_KEYS_LIMIT`` (was a copied 32);
* the explanation's deadline: ``run_with_deadline`` (was a ThreadPoolExecutor of its own);
* re-run eligibility, the widen DDL and stale claims are covered by the f7, f11 and f9 tests.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import parity_evidence


@pytest.mark.parametrize("status", [
    "completed", "COMPLETED ", "completed_with_quarantine", "completed_with_warnings", "failed", "cancelled",
    "success", "error", "running", "pending", "queued", "retrying", "", None,
])
def test_a_finished_job_is_the_trust_cards_rule(status):
    from services.job_trust import is_terminal_status

    assert parity_evidence.job_is_terminal({"status": status}) is is_terminal_status(status)


def test_the_culprit_cap_is_gate8s_own(monkeypatch):
    from src.transfer import reconcile_step

    recon = {"culprit_keys": {"changed": ["1", "2", "3"]}}
    assert parity_evidence.evidence_capped(recon) is (3 >= reconcile_step._CULPRIT_KEYS_LIMIT)
    monkeypatch.setattr(reconcile_step, "_CULPRIT_KEYS_LIMIT", 3)
    assert parity_evidence.evidence_capped(recon) is True


def test_the_explanation_runs_under_the_shared_deadline(monkeypatch):
    from services import parity_explain
    from services.parity_report import build_parity_report
    from src.ai.copilot import tool_deadline

    monkeypatch.setattr("services.llm_policy.is_llm_enabled", lambda: True)
    monkeypatch.setattr("services.llm_mapping.llm_provider_available", lambda: True)
    monkeypatch.setattr(parity_explain, "_generate", lambda prompt, job_id, columns: SimpleNamespace(
        success=True, content='{"text": "Two rows differ.", "fix_order": []}'))
    tools: list[str] = []
    real = tool_deadline.run_with_deadline

    def spy(tool, fn, on_timeout, *, seconds=None):
        tools.append(tool)
        return real(tool, fn, on_timeout, seconds=seconds)

    monkeypatch.setattr(tool_deadline, "run_with_deadline", spy)
    report = build_parity_report({
        "_id": "j", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 10, "target_rows": 9,
                           "culprit_keys": {"missing_in_dest": ["4"]}},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"]},
    }, reread=False)
    out = parity_explain.explain_report(report)
    assert out["source"] == "llm" and tools == ["parity_explain"]
