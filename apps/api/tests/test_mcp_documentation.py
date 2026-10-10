from pathlib import Path

from src.ai.copilot.tool_permissions import TOOL_PERMISSIONS


def test_mcp_documentation_mentions_every_registered_tool():
    text = (Path(__file__).parents[3] / "docs" / "MCP.md").read_text()
    missing = sorted(
        name
        for name, (permission, effect) in TOOL_PERMISSIONS.items()
        if f"| `{name}` | `{permission}` | `{effect}` |" not in text
    )
    assert not missing, f"MCP documentation drift: {missing}"


def test_mcp_invocation_logs_classify_required_error_kinds(monkeypatch, tmp_path):
    from services import mcp_invocation_log

    monkeypatch.setattr(mcp_invocation_log, "STORE_PATH", tmp_path / "invocations.jsonl")
    monkeypatch.setattr(mcp_invocation_log, "append_audit_event", lambda **_kwargs: None)
    cases = (
        ("ok", None, "ok"),
        ("error", "Your role cannot run this tool", "permission_denied"),
        ("error", "MCP rate limit exceeded", "rate_limited"),
        ("error", "Authentication required", "auth"),
        ("error", "MCP is disabled by an administrator", "policy_denied"),
        ("error", "database unavailable", "tool_error"),
    )

    rows = [
        mcp_invocation_log.log_mcp_invocation(tool="probe", status=status, error=error)
        for status, error, _expected in cases
    ]

    assert [row["error_kind"] for row in rows] == [
        expected for _status, _error, expected in cases
    ]
