"""Parity Studio explanation layer and the ``explain_parity`` Pilot / MCP tool.

The model only words the report: it gets classifier output and catalog metadata
(no row values under the default metadata-only egress policy), may reorder the
proposed fixes but never add one, and a timeout or bad answer falls back to the
deterministic template with the same structure.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from services import parity_explain
from services.parity_report import build_parity_report

_MISMATCH = {"key": "5", "source": "amount", "target": "amount",
             "source_value": "10.1049", "target_value": "10.10"}


def _job() -> dict:
    return {
        "_id": "job-x",
        "status": "completed",
        "workspace_id": "",
        "reconciliation": {"passed": False, "source_rows": 50, "target_rows": 50,
                           "sample_compare": {"rows_compared": 50, "mismatches": [_MISMATCH]},
                           "culprit_keys": {"changed": ["5"]}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
        "transfer_request": {"source": {"kind": "database"}, "destination": {"kind": "database"}},
    }


@pytest.fixture()
def model(monkeypatch):
    seen: dict = {}
    monkeypatch.setattr("services.llm_policy.is_llm_enabled", lambda: True)
    monkeypatch.setattr("services.llm_mapping.llm_provider_available", lambda: True)

    def install(answer: str, *, delay: float = 0.0, success: bool = True):
        def fake(prompt, job_id, columns):
            seen.update(prompt=prompt, job_id=job_id, columns=columns)
            time.sleep(delay)
            return SimpleNamespace(success=success, content=answer)

        monkeypatch.setattr(parity_explain, "_generate", fake)
        return seen

    return install


def test_template_without_a_model(monkeypatch):
    monkeypatch.setattr("services.llm_policy.is_llm_enabled", lambda: False)
    report = build_parity_report(_job(), reread=False)
    out = parity_explain.explain_report(report)
    assert out["source"] == "template"
    assert set(out) == {"text", "source", "generated_at", "fix_order"}
    assert "payments.amount" in out["text"] and "numeric scale" in out["text"]
    assert "Proposed fix:" in out["text"]


def test_model_words_and_reorders_but_cannot_invent(model, monkeypatch):
    monkeypatch.delenv("DATAFLOW_AI_METADATA_ONLY", raising=False)
    monkeypatch.delenv("DATAWRAP_AI_METADATA_ONLY", raising=False)
    report = build_parity_report(_job(), reread=False)
    ids = [f["id"] for f in report["fixes"]]
    answer = '{"text": "Four decimal places became two.", "fix_order": ["%s", "f_invented", "%s"]}' % (
        ids[-1], ids[0])
    seen = model(answer)
    out = parity_explain.explain_report(report)
    assert out["source"] == "llm" and out["text"] == "Four decimal places became two."
    assert out["fix_order"] == [ids[-1], ids[0]]  # the invented id is dropped
    assert "10.1049" not in seen["prompt"] and "10.10\"" not in seen["prompt"]  # metadata only
    assert '"cause": "numeric_scale"' in seen["prompt"] and seen["columns"] == ["amount"]
    assert seen["job_id"] == "job-x"


def test_values_reach_the_model_only_when_the_egress_policy_allows(model, monkeypatch):
    monkeypatch.setenv("DATAFLOW_AI_METADATA_ONLY", "false")
    seen = model('{"text": "ok", "fix_order": []}')
    parity_explain.explain_report(build_parity_report(_job(), reread=False))
    assert "10.1049" in seen["prompt"]


@pytest.mark.parametrize(("answer", "delay", "success"), [
    ("not json at all", 0.0, True),
    ('{"text": ""}', 0.0, True),
    ('{"text": "fine"}', 0.0, False),
    ('{"text": "too late"}', 1.6, True),
])
def test_fallback_to_the_template(model, monkeypatch, answer, delay, success):
    monkeypatch.setenv("DATAFLOW_PARITY_EXPLAIN_TIMEOUT", "1")
    model(answer, delay=delay, success=success)
    out = parity_explain.explain_report(build_parity_report(_job(), reread=False))
    assert out["source"] == "template"


# ── the Pilot / MCP tool ─────────────────────────────────────────────────────


class _Store:
    def __init__(self, jobs):
        self.jobs = jobs
        self.client = None

    def get_job(self, job_id):
        job = self.jobs.get(job_id)
        return dict(job) if job else None


def test_explain_parity_tool_is_registered_gated_and_answers(monkeypatch):
    import services.mongodb_service as mongodb_service
    from services.mcp_protocol import handle_jsonrpc
    from services.rbac import Permission
    from src.ai.copilot.tool_permissions import READ, TOOL_PERMISSIONS
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    assert TOOL_PERMISSIONS["explain_parity"] == (Permission.JOB_READ, READ)
    assert "explain_parity" in {t["name"] for t in TOOL_DEFINITIONS}
    listed = handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, authenticated=True)
    assert "explain_parity" in {t["name"] for t in listed["result"]["tools"]}

    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: _Store({"job-x": _job()}))
    monkeypatch.setattr("services.llm_policy.is_llm_enabled", lambda: False)
    result = DataPilotTools().execute("explain_parity", {"job_id": "job-x"})
    assert result.success, result.error
    summary = result.output["summary"]
    assert summary["verdict"] == "unexplained" and summary["unexplained"] == 1
    assert summary["differences"][0]["cause"] == "Numeric scale"
    assert any(f["action"] == "resync_keys" for f in summary["fixes"])
    assert result.output["explanation"]["source"] == "template"
    assert "10.1049" not in str(result.output)

    missing = DataPilotTools().execute("explain_parity", {"job_id": "nope"})
    assert not missing.success and "not found" in missing.error


def test_remediation_menu_is_one_registry():
    from services.remediation_actions import PARITY, STUDIO, kinds_for
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    schema = next(t for t in TOOL_DEFINITIONS if t["name"] == "remediate_validation")["input_schema"]
    assert schema["properties"]["kind"]["enum"] == list(kinds_for(STUDIO))
    assert set(kinds_for(PARITY)) == {"widen_column", "resync_keys", "accept", "rerun"}
    ok = DataPilotTools().execute("remediate_validation", {"kind": "review_mappings"})
    assert ok.success and ok.output["label"] == "Open Map step to review mappings"
    staged = DataPilotTools().execute("remediate_validation", {"kind": "quarantine_and_rerun"})
    assert staged.output["requires_confirm"] and staged.output["destructive"]
    refused = DataPilotTools().execute("remediate_validation", {"kind": "resync_keys"})
    assert not refused.success  # a Parity Studio action is not a Validate remediation
