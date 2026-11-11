"""Pilot's own env dials answer to the DATAWRAP_ name, and still to DATAFLOW_.

Five flags under ``src/ai`` read ``os.environ`` with the legacy prefix only,
so an operator who set ``DATAWRAP_TOOL_DEADLINE_SECONDS`` (the documented
brand) changed nothing. They go through ``services.brand_env.getenv_brand``
like every other dial: DATAWRAP_ first, DATAFLOW_ as the fallback.
"""

from __future__ import annotations

import pytest

from src.ai.copilot import tool_deadline
from src.ai.first_party import engine

_DEADLINES = [
    ("TOOL_DEADLINE_SECONDS", "list_jobs"),
    ("QUERY_TOOL_DEADLINE_SECONDS", sorted(tool_deadline.QUERY_DEADLINE_TOOLS)[0]),
]
_SWITCHES = [
    ("FP_REWRITE", engine.rewrite_enabled),
    ("FP_NARRATE", engine.narrate_enabled),
    ("FP_LM", engine.lm_enabled),
]


def _clear(monkeypatch, suffix: str) -> None:
    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        monkeypatch.delenv(prefix + suffix, raising=False)


@pytest.mark.parametrize("suffix, tool", _DEADLINES)
def test_deadline_reads_the_brand_name_and_keeps_the_legacy_one(monkeypatch, suffix, tool):
    _clear(monkeypatch, suffix)
    default = tool_deadline.deadline_seconds(tool)

    monkeypatch.setenv(f"DATAFLOW_{suffix}", "7")
    assert tool_deadline.deadline_seconds(tool) == 7.0, "the legacy name must keep working"
    monkeypatch.setenv(f"DATAWRAP_{suffix}", "11")
    assert tool_deadline.deadline_seconds(tool) == 11.0, "the brand name wins when both are set"
    monkeypatch.delenv(f"DATAFLOW_{suffix}")
    assert tool_deadline.deadline_seconds(tool) == 11.0

    monkeypatch.setenv(f"DATAWRAP_{suffix}", "not-a-number")
    assert tool_deadline.deadline_seconds(tool) == default
    monkeypatch.setenv(f"DATAWRAP_{suffix}", "-3")
    assert tool_deadline.deadline_seconds(tool) == default


@pytest.mark.parametrize("suffix, enabled", _SWITCHES)
def test_first_party_switch_reads_the_brand_name_and_keeps_the_legacy_one(monkeypatch, suffix, enabled):
    _clear(monkeypatch, suffix)
    assert enabled() is True, "on by default"

    monkeypatch.setenv(f"DATAFLOW_{suffix}", "0")
    assert enabled() is False, "the legacy name must keep working"
    monkeypatch.setenv(f"DATAWRAP_{suffix}", "1")
    assert enabled() is True, "the brand name wins when both are set"
    monkeypatch.delenv(f"DATAFLOW_{suffix}")
    monkeypatch.setenv(f"DATAWRAP_{suffix}", "off")
    assert enabled() is False
    monkeypatch.setenv(f"DATAWRAP_{suffix}", "maybe")
    assert enabled() is True, "an unrecognised value falls back to the default"
