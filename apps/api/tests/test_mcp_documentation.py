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


def test_mcp_documentation_lists_rate_limit_settings_and_error_kinds():
    text = (Path(__file__).parents[3] / "docs" / "MCP.md").read_text()
    required = (
        "DATAFLOW_MCP_RATE_LIMIT",
        "DATAFLOW_MCP_RATE_BURST",
        "DATAFLOW_MCP_RATE_QPS",
        "DATAFLOW_MCP_RATE_MAX_KEYS",
        "ok",
        "permission_denied",
        "rate_limited",
        "auth",
        "policy_denied",
        "tool_error",
    )
    missing = [value for value in required if f"`{value}`" not in text]
    assert not missing, f"MCP documentation drift: {missing}"


def test_mcp_invocation_logs_accept_explicit_error_kinds_and_fallback(monkeypatch, tmp_path):
    from services import mcp_invocation_log

    monkeypatch.setattr(mcp_invocation_log, "STORE_PATH", tmp_path / "invocations.jsonl")
    monkeypatch.setattr(mcp_invocation_log, "append_audit_event", lambda **_kwargs: None)
    explicit = (
        ("ok", None, "ok"),
        ("error", "permission denied", "permission_denied"),
        ("error", "MCP rate limit exceeded", "rate_limited"),
        ("error", "Authentication required", "auth"),
        ("error", "MCP is disabled", "policy_denied"),
        ("error", "database unavailable", "tool_error"),
    )

    explicit_rows = [
        mcp_invocation_log.log_mcp_invocation(
            tool="probe",
            status=status,
            error=error,
            error_kind=expected,
        )
        for status, error, expected in explicit
    ]
    fallback_rows = [
        mcp_invocation_log.log_mcp_invocation(
            tool="probe",
            status=status,
            error=error,
        )
        for status, error in (
            ("ok", None),
            ("error", "Your role cannot run this tool"),
            ("error", "source token expired"),
        )
    ]

    assert [row["error_kind"] for row in explicit_rows] == [
        expected for _status, _error, expected in explicit
    ]
    assert [row["error_kind"] for row in fallback_rows] == [
        "ok",
        "permission_denied",
        "tool_error",
    ]
