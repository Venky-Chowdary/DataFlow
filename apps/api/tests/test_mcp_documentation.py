from src.ai.copilot.tool_permissions import TOOL_PERMISSIONS


def test_every_registered_mcp_tool_has_a_permission_and_an_effect():
    """The MCP tool table lives in code (docs/ was removed in 1415f1f0f).

    Every tool the assistant and MCP expose is gated by exactly one permission
    and declares whether it reads, plans or mutates; no permission row is left
    for a tool that no longer exists.
    """
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    registered = {tool["name"] for tool in TOOL_DEFINITIONS}
    assert registered - set(TOOL_PERMISSIONS) == set(), "tool without a permission"
    assert set(TOOL_PERMISSIONS) - registered == set(), "permission for a removed tool"
    bad = sorted(
        name
        for name, (permission, effect) in TOOL_PERMISSIONS.items()
        if effect not in {"read", "plan", "mutate"} or "." not in permission
    )
    assert not bad, f"tool permission drift: {bad}"


def test_mcp_rate_limit_settings_are_the_ones_the_limiter_reads(monkeypatch):
    """DATAFLOW_MCP_RATE_LIMIT / _BURST / _QPS / _MAX_KEYS steer the limiter."""
    from services import mcp_rate_limit

    principal = "mcp-settings-contract"
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")
    assert mcp_rate_limit.check_mcp_rate_limit(principal)["allowed"] is True
    assert mcp_rate_limit.check_mcp_rate_limit(principal).get("disabled") is True

    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_BURST", "1")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_QPS", "0.001")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_MAX_KEYS", "10")
    assert mcp_rate_limit.check_mcp_rate_limit(principal)["allowed"] is True
    assert mcp_rate_limit.check_mcp_rate_limit(principal)["allowed"] is False


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
