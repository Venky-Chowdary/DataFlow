"""Pilot / MCP tools of the Evidence Ledger: registered, gated like their REST twins, read only, scoped."""

from __future__ import annotations

from tests.test_ledger_log import ledger  # noqa: F401
from tests.test_ledger_packs import _job
from tests.test_parity_review2_f12_f13_seal_and_verify import _signed, store  # noqa: F401


def test_tools_are_registered_read_only_and_listed_over_mcp():
    from services.mcp_protocol import handle_jsonrpc
    from services.rbac import Permission
    from src.ai.copilot.tool_permissions import READ, TOOL_PERMISSIONS
    from src.ai.copilot.tools import TOOL_DEFINITIONS, TOOL_FAMILIES

    assert TOOL_PERMISSIONS["list_evidence_entries"] == (Permission.AUDIT_READ, READ)
    assert TOOL_PERMISSIONS["verify_evidence"] == (Permission.AUDIT_READ, READ)
    names = {t["name"] for t in TOOL_DEFINITIONS}
    assert {"list_evidence_entries", "verify_evidence"} <= names
    assert {"list_evidence_entries", "verify_evidence"} <= {t for f in TOOL_FAMILIES for t in f["tools"]}
    listed = {t["name"] for t in handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                                                authenticated=True)["result"]["tools"]}
    assert {"list_evidence_entries", "verify_evidence"} <= listed
    # Read only: nothing that creates keys, withdraws or anchors is a tool.
    ledger_tools = {n for n in names if "evidence" in n or "ledger" in n or "withdraw" in n or "signing_key" in n}
    assert ledger_tools == {"list_evidence_entries", "verify_evidence"}


def test_listing_and_verifying_answer_only_inside_the_workspace(ledger, store):  # noqa: F811
    from services.ledger_packs import withdraw
    from services.signed_proof_pack import export_proof_pack_for_job
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    mine = export_proof_pack_for_job(_job("job-mine", "ws-a"), actor="ops")
    theirs = export_proof_pack_for_job(_job("job-theirs", "ws-b"), actor="ops")
    job_id = _signed(store)  # a signed Parity report in workspace "w"
    with bound_workspace_filter(WorkspaceFilter.only("ws-a")):
        listed = DataPilotTools().execute("list_evidence_entries", {})
        assert listed.success, listed.error
        # The workspace's pack, its own key event and the log's key event; nothing of workspace ws-b.
        packs = [e for e in listed.output["entries"] if e["kind"] == "proof_pack"]
        assert [e["content_sha256"] for e in packs] == [mine["content_sha256"]]
        assert sorted(e["subject"] for e in listed.output["entries"] if e["kind"] == "key_set") == [
            "key_set:checkpoint", "key_set:statement"]
        assert packs[0]["status"] == "issued" and listed.output["open"] == "/#/evidence"
        assert "does not show that the recorded facts are true" in listed.output["note"]
        bad = DataPilotTools().execute("list_evidence_entries", {"kind": "certificate"})
        assert not bad.success and "Kind must be one of" in bad.error

        checked = DataPilotTools().execute("verify_evidence", {"content_sha256": mine["content_sha256"]})
        assert checked.success, checked.error
        statuses = {c["id"]: c["status"] for c in checked.output["checks"]}
        # Only the ledger's record was at hand: the pack's own content is "not checked".
        assert statuses["content_digest"] == "not_checked" and statuses["signatures"] == "passed"
        assert statuses["inclusion"] == statuses["consistency"] == statuses["withdrawal"] == "passed"
        assert checked.output["verdict"] == "incomplete" and checked.output["kind"] == "proof_pack"
        assert "not a full verification" in checked.output["summary"]
        foreign = DataPilotTools().execute("verify_evidence", {"content_sha256": theirs["content_sha256"]})
        assert not foreign.success and "No pack with that digest" in foreign.error
        nothing = DataPilotTools().execute("verify_evidence", {})
        assert not nothing.success and "Which pack" in nothing.error

        withdraw(ledger.find_entry(mine["content_sha256"]), reason="wrong run", superseded_by=None, actor="lead")
        again = DataPilotTools().execute("verify_evidence", {"content_sha256": mine["content_sha256"]})
        assert again.output["verdict"] == "withdrawn" and again.output["issuance"] == "issued_later_withdrawn"
        assert again.output["withdrawal"]["reason"] == "wrong run"
        relisted = DataPilotTools().execute("list_evidence_entries", {"kind": "proof_pack"})
        assert relisted.output["entries"][0]["status"] == "withdrawn"
        assert relisted.output["entries"][0]["withdrawal_reason"] == "wrong run"

    with bound_workspace_filter(WorkspaceFilter.only("w")):
        parity = DataPilotTools().execute("verify_evidence", {"job_id": job_id})
        assert parity.success, parity.error
        statuses = {c["id"]: c["status"] for c in parity.output["checks"]}
        assert parity.output["kind"] == "parity_report" and parity.output["verdict"] == "incomplete"
        # The stored pack was at hand, so its content was hashed too.
        assert statuses["content_digest"] == statuses["statement"] == statuses["signatures"] == "passed"
        assert statuses["anchor"] == "not_checked"
        missing = DataPilotTools().execute("verify_evidence", {"rehearsal_id": "exr_none"})
        assert not missing.success and "not found" in missing.error
