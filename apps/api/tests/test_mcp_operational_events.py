from __future__ import annotations

import json
from types import SimpleNamespace


def test_mcp_tool_events_and_invocations_omit_arguments_and_error_text(
    monkeypatch, caplog
):
    import importlib

    from services import audit_log, mcp_invocation_log
    from services.mcp_protocol import McpCallContext, _execute_tool

    class _Tools:
        def execute(self, name, _arguments):
            if name == "successful_tool":
                return SimpleNamespace(success=True, output={"ok": True}, error=None)
            if name == "denied_tool":
                return SimpleNamespace(
                    success=False,
                    output=None,
                    error="Your role cannot run this tool",
                )
            raise RuntimeError("SENTINEL_EXCEPTION_DETAIL")

    tools_module = importlib.import_module("src.ai.copilot.tools")
    monkeypatch.setattr(tools_module, "get_pilot_tools", lambda: _Tools())
    context = McpCallContext(
        actor="mcp-test-actor",
        client="mcp-test-client",
        correlation_id="mcp-event-correlation",
    )
    arguments = {"note": "SENTINEL_ARGUMENT_DETAIL"}
    with caplog.at_level("INFO"):
        _execute_tool("successful_tool", arguments, context)
        _execute_tool("denied_tool", arguments, context)
        _execute_tool("errored_tool", arguments, context)

    invocation_text = mcp_invocation_log.STORE_PATH.read_text(encoding="utf-8")
    audit_text = audit_log.STORE_PATH.read_text(encoding="utf-8")
    for text in (caplog.text, invocation_text, audit_text):
        assert "SENTINEL_ARGUMENT_DETAIL" not in text
        assert "SENTINEL_EXCEPTION_DETAIL" not in text

    events = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "mcp.tool.call"
    ]
    assert [event.outcome for event in events] == [
        "succeeded",
        "failed",
        "errored",
    ]
    assert events[1].reason_code == "permission_denied"
    assert events[-1].error_type == "RuntimeError"
    assert all(event.client == "mcp-test-client" for event in events)
    assert all(event.correlation_id == "mcp-event-correlation" for event in events)

    with mcp_invocation_log.STORE_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({
            "id": "legacy-row",
            "time": "2026-01-01T00:00:00+00:00",
            "tool": "legacy_tool",
            "client": "legacy_client",
            "status": "error",
            "arguments": {"note": "LEGACY_ARGUMENT"},
            "error": "LEGACY_ERROR",
            "error_kind": "tool_error",
        }) + "\n")
    rows = mcp_invocation_log.list_mcp_invocations(limit=10)
    assert all("arguments" not in row and "error" not in row for row in rows)
