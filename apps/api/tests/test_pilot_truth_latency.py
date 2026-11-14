"""Pilot ground truth — ledger reads are bounded and the greeting does not re-count (review M6).

``list_jobs`` and ``brief_workspace`` had no tool deadline: a 6 s count made a
13.7 s turn. Every turn, "hi" included, issued a whole-collection aggregate
for the context preamble.
"""

from __future__ import annotations

import time

import pytest

from tests.helpers import pilot_truth as pt


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    for i in range(3):
        pt.seed_job("completed", minutes_ago=10 + i)
    return pt.mongo()


@pytest.mark.parametrize("tool", ["list_jobs", "brief_workspace"])
def test_a_slow_ledger_read_is_cut_off_at_the_tool_deadline(ledger, monkeypatch, tool):
    from src.ai.copilot.tools import get_pilot_tools

    real = type(ledger).count_jobs

    def slow(self, *a, **k):
        time.sleep(3)
        return real(self, *a, **k)

    monkeypatch.setattr(type(ledger), "count_jobs", slow)
    monkeypatch.setenv("DATAFLOW_LEDGER_TOOL_DEADLINE_SECONDS", "0.5")
    started = time.monotonic()
    result = get_pilot_tools().execute(tool, {})
    elapsed = time.monotonic() - started
    assert elapsed < 2.0, f"{tool} waited {elapsed:.1f}s on a slow store"
    assert result.success is False and "could not read" in (result.error or ""), result


def test_greeting_does_not_reissue_the_whole_ledger_count_every_turn(ledger, monkeypatch):
    calls = []
    real = type(ledger).count_jobs

    def counted(self, *a, **k):
        calls.append(k)
        return real(self, *a, **k)

    monkeypatch.setattr(type(ledger), "count_jobs", counted)
    agent = pt.agent()
    first = pt.ask(agent, "hi")
    n_first = len(calls)
    pt.ask(agent, "hi")
    pt.ask(agent, "hello")
    assert n_first >= 1
    assert len(calls) == n_first, f"each greeting re-ran the ledger count ({len(calls)} calls)"
    assert "3" in first.answer
