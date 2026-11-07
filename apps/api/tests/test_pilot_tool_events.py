"""Every DataPilotTools.execute dispatch emits one ``pilot.tool.executed`` event.

The event carries the tool name, a bounded reason, the caller role and the
error class — never the prompt, the arguments, the output or the error text.
Unhandled exceptions log at ERROR; refused/rejected calls at WARNING.
"""

from __future__ import annotations

import json
import logging

import pytest

from services.logging_config import ContextFilter, JsonFormatter, TextFormatter
from src.ai.copilot.tool_permissions import caller_role, is_tool_allowed
from src.ai.copilot.tools import DataPilotTools, ToolResult

SENTINEL = "SENTINEL-PROMPT-5d1e postgresql://u:hunter2@db/x"


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []
        self.addFilter(ContextFilter())

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def events(self) -> list[logging.LogRecord]:
        return [r for r in self.records if getattr(r, "event", "") == "pilot.tool.executed"]


@pytest.fixture
def capture():
    log = logging.getLogger("src.ai.copilot.tools")
    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)


def _only_event(capture: _Capture) -> logging.LogRecord:
    events = capture.events()
    assert len(events) == 1, [r.__dict__ for r in events]
    record = events[0]
    assert record.getMessage() == "pilot.tool.executed"
    assert record.duration_ms >= 0
    for fmt in (JsonFormatter(), TextFormatter()):
        rendered = fmt.format(record)
        assert "SENTINEL-PROMPT" not in rendered
        assert "hunter2" not in rendered
    json.loads(JsonFormatter().format(record))
    return record


def test_success_logs_tool_name_without_arguments(capture):
    tools = DataPilotTools()

    def handler(**kwargs):
        return ToolResult(name="describe_pilot", success=True, output={"echo": SENTINEL})

    tools._describe_pilot = handler
    with caller_role("admin"):
        result = tools.execute("describe_pilot", {"prompt": SENTINEL})
    assert result.success is True
    record = _only_event(capture)
    assert (record.outcome, record.levelno) == ("succeeded", logging.INFO)
    assert record.tool == "describe_pilot"
    assert record.caller_role == "admin"
    assert getattr(record, "error_type", None) is None


def test_unhandled_exception_logs_error_type_only_and_keeps_tool_result(capture):
    tools = DataPilotTools()

    def handler(**kwargs):
        raise RuntimeError(SENTINEL)

    tools._describe_pilot = handler
    with caller_role("admin"):
        result = tools.execute("describe_pilot", {})
    assert result.success is False
    assert result.error == SENTINEL  # caller-facing contract unchanged
    record = _only_event(capture)
    assert (record.outcome, record.levelno) == ("failed", logging.ERROR)
    assert record.reason == "exception"
    assert record.error_type == "RuntimeError"
    assert record.error_category == "unknown"
    assert record.exc_info is None


def test_tool_reported_failure_is_warning(capture):
    tools = DataPilotTools()
    tools._describe_pilot = lambda **kwargs: ToolResult(
        name="describe_pilot", success=False, output=None, error=SENTINEL
    )
    with caller_role("admin"):
        tools.execute("describe_pilot", {})
    record = _only_event(capture)
    assert (record.outcome, record.levelno, record.reason) == (
        "failed",
        logging.WARNING,
        "tool_reported_failure",
    )


def test_unknown_tool_does_not_log_requested_name(capture):
    result = DataPilotTools().execute("drop_" + SENTINEL, {})
    assert result.success is False
    record = _only_event(capture)
    assert (record.outcome, record.levelno) == ("failed", logging.WARNING)
    assert (record.tool, record.reason) == ("unknown", "unknown_tool")


def test_permission_denial_is_warning_with_role(capture):
    assert not is_tool_allowed("viewer", "delete_connector")
    with caller_role("viewer"):
        result = DataPilotTools().execute("delete_connector", {"connector_id": SENTINEL})
    assert result.success is False
    record = _only_event(capture)
    assert (record.outcome, record.levelno) == ("failed", logging.WARNING)
    assert (record.tool, record.reason, record.caller_role) == (
        "delete_connector",
        "permission_denied",
        "viewer",
    )
