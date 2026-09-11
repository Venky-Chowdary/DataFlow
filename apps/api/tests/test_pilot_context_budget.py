"""The optional workspace context a Pilot turn reads must be time-bounded.

Measured before this was bounded: every turn cost 10.05s against an unreachable
metadata store, while the retrieval and composition work the answer actually
rests on cost 0.02s. All of it was ``PilotContextBuilder.build`` — the connector
read spending one pymongo server-selection window and the job read spending two,
serially, on every question asked, because ``MongoService.connect`` builds a new
client per call and does not remember that the last one failed.

Two claims are tested here, and they are separate claims. The first is about
time: a source that has stopped answering costs the turn one budget and then
costs nothing until it is worth redialling. The second is about honesty: a read
that failed must not be reported as a workspace that is empty.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot import context_builder as cb  # noqa: E402


@pytest.fixture(autouse=True)
def _forget_cooldowns():
    """Each test starts with every source eligible to be redialled."""
    cb.reset_context_cooldowns()
    yield
    cb.reset_context_cooldowns()


def _reads(**fns):
    """Name a set of reads with an empty-list fallback for ``_gather``."""
    return {name: (fn, []) for name, fn in fns.items()}


def _slow(seconds: float, value):
    def read():
        time.sleep(seconds)
        return value
    return read


def _broken(exc=RuntimeError("store unreachable")):
    def read():
        raise exc
    return read


# --- what a failed read costs, and what it reports -------------------------


def test_a_read_that_raises_falls_back_instead_of_propagating():
    gathered, degraded = cb._gather(_reads(connectors=_broken()))
    assert gathered["connectors"] == []
    assert degraded == ["connectors"]


def test_each_read_falls_back_to_its_own_shape():
    """Capabilities are a mapping, not a list; the fallback must match."""
    gathered, degraded = cb._gather(
        {"transfer_capabilities": (_broken(), {}), "connectors": (_broken(), [])}
    )
    assert gathered["transfer_capabilities"] == {}
    assert gathered["connectors"] == []
    assert sorted(degraded) == ["connectors", "transfer_capabilities"]


def test_an_empty_workspace_is_not_reported_as_an_outage():
    """The distinction the old code could not draw: nothing saved vs no answer."""
    gathered, degraded = cb._gather(_reads(connectors=lambda: []))
    assert gathered["connectors"] == []
    assert degraded == []


def test_a_successful_read_is_returned_unchanged():
    rows = [{"name": "Audit SQLite", "type": "sqlite"}]
    gathered, degraded = cb._gather(_reads(connectors=lambda: rows))
    assert gathered["connectors"] == rows
    assert degraded == []


# --- the budget ------------------------------------------------------------


def test_a_read_slower_than_the_budget_is_abandoned(monkeypatch):
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 0.25)
    start = time.monotonic()
    gathered, degraded = cb._gather(_reads(recent_jobs=_slow(5.0, ["never seen"])))
    elapsed = time.monotonic() - start
    assert degraded == ["recent_jobs"]
    assert gathered["recent_jobs"] == []
    # The turn walks away from the read rather than waiting it out.
    assert elapsed < 2.0, f"turn waited {elapsed:.2f}s on an abandoned read"


def test_a_read_inside_the_budget_is_kept(monkeypatch):
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 2.0)
    gathered, degraded = cb._gather(_reads(connectors=_slow(0.1, ["kept"])))
    assert gathered["connectors"] == ["kept"]
    assert degraded == []


def test_slow_reads_cost_one_budget_and_not_the_sum(monkeypatch):
    """Why the reads are submitted before any is collected.

    Run serially these three cost 1.2s and the budget would cut two of them off.
    Run together they cost one wait, which is the whole point of a shared
    deadline: a turn's optional context is bounded by the slowest source, not by
    how many sources there happen to be.
    """
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 1.0)
    start = time.monotonic()
    gathered, degraded = cb._gather(
        _reads(
            connectors=_slow(0.4, ["c"]),
            recent_jobs=_slow(0.4, ["j"]),
            transfer_capabilities=_slow(0.4, ["k"]),
        )
    )
    elapsed = time.monotonic() - start
    assert degraded == []
    assert gathered == {
        "connectors": ["c"],
        "recent_jobs": ["j"],
        "transfer_capabilities": ["k"],
    }
    assert elapsed < 1.0, f"three 0.4s reads took {elapsed:.2f}s — they ran serially"


def test_one_dead_source_does_not_cost_a_healthy_one_its_answer(monkeypatch):
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 0.4)
    gathered, degraded = cb._gather(
        _reads(recent_jobs=_slow(5.0, ["never"]), connectors=lambda: ["kept"])
    )
    assert gathered["connectors"] == ["kept"]
    assert degraded == ["recent_jobs"]


# --- the cooldown ----------------------------------------------------------


def test_a_failed_source_is_not_redialled_while_it_cools_off():
    calls = []

    def read():
        calls.append(1)
        raise RuntimeError("store unreachable")

    cb._gather(_reads(connectors=read))
    cb._gather(_reads(connectors=read))
    cb._gather(_reads(connectors=read))
    assert len(calls) == 1, f"redialled a known-dead source {len(calls)} times"


def test_a_cooling_source_is_still_reported_degraded():
    cb._gather(_reads(connectors=_broken()))
    gathered, degraded = cb._gather(_reads(connectors=lambda: ["would have worked"]))
    assert degraded == ["connectors"]
    assert gathered["connectors"] == []


def test_the_cooldown_expires_and_the_source_is_tried_again(monkeypatch):
    monkeypatch.setattr(cb, "CONTEXT_COOLDOWN_S", 0.0)
    cb._gather(_reads(connectors=_broken()))
    gathered, degraded = cb._gather(_reads(connectors=lambda: ["back"]))
    assert gathered["connectors"] == ["back"]
    assert degraded == []


def test_the_cooldown_is_per_source():
    """A dead job read must not stop the connector read from being attempted."""
    cb._gather(_reads(recent_jobs=_broken()))
    gathered, degraded = cb._gather(
        _reads(recent_jobs=lambda: ["j"], connectors=lambda: ["c"])
    )
    assert gathered["connectors"] == ["c"]
    assert degraded == ["recent_jobs"]


def test_resetting_cooldowns_redials_every_source():
    cb._gather(_reads(connectors=_broken(), recent_jobs=_broken()))
    cb.reset_context_cooldowns()
    gathered, degraded = cb._gather(
        _reads(connectors=lambda: ["c"], recent_jobs=lambda: ["j"])
    )
    assert degraded == []
    assert gathered == {"connectors": ["c"], "recent_jobs": ["j"]}


# --- what the model is told ------------------------------------------------


def _ctx(**over):
    base = {
        "dataset_count": 3,
        "connectors": [],
        "recent_jobs": [],
        "transfer_capabilities": {"live_count": 0, "operations": [], "auto_ddl": None},
        "context_degraded": [],
    }
    base.update(over)
    return base


def test_an_unread_source_is_not_described_as_zero():
    block = cb.PilotContextBuilder().to_system_context(
        _ctx(context_degraded=["connectors", "recent_jobs"])
    )
    assert "0 saved connectors" not in block
    assert "0 recent transfer jobs" not in block
    assert "could not be read this turn" in block


def test_a_real_count_is_still_stated_plainly():
    block = cb.PilotContextBuilder().to_system_context(
        _ctx(connectors=[{"name": "a"}, {"name": "b"}])
    )
    assert "2 saved connectors" in block
    assert "could not be read" not in block


def test_a_genuinely_empty_workspace_still_reports_zero():
    """Zero is a true statement when the read succeeded — only the outage lies."""
    block = cb.PilotContextBuilder().to_system_context(_ctx())
    assert "0 saved connectors" in block
    assert "could not be read" not in block


def test_one_unread_source_does_not_hide_another_that_was_read():
    block = cb.PilotContextBuilder().to_system_context(
        _ctx(connectors=[{"name": "a"}], context_degraded=["recent_jobs"])
    )
    assert "1 saved connectors" in block
    assert "recent transfer jobs: could not be read this turn" in block


# --- the builder end to end ------------------------------------------------


def test_build_reports_which_sources_it_could_not_read(monkeypatch):
    builder = cb.PilotContextBuilder()
    monkeypatch.setattr(builder, "_read_connectors", _broken())
    ctx = builder.build(None, "what gets quarantined")
    assert "connectors" in ctx["context_degraded"]
    assert ctx["connectors"] == []


def test_a_turn_against_a_dead_store_stays_under_the_budget(monkeypatch):
    """The measured defect, as a test: 10.05s per turn became one bounded wait."""
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 0.5)
    builder = cb.PilotContextBuilder()
    monkeypatch.setattr(builder, "_read_connectors", _slow(30.0, []))
    monkeypatch.setattr(builder, "_read_jobs", _slow(30.0, []))
    start = time.monotonic()
    ctx = builder.build(None, "what gets quarantined")
    elapsed = time.monotonic() - start
    # A subset, not an equality: the budget here is tightened well below the
    # real 3s so the test is quick, and a cold capability scan can miss a bound
    # that tight. Losing it is the bound working. The claim under test is that
    # the two store-backed reads are the ones reported and that the turn returns.
    assert {"connectors", "recent_jobs"} <= set(ctx["context_degraded"])
    assert elapsed < 3.0, f"turn spent {elapsed:.2f}s on context it could not read"


def test_the_second_turn_against_a_dead_store_does_not_wait_at_all(monkeypatch):
    monkeypatch.setattr(cb, "CONTEXT_BUDGET_S", 0.5)
    builder = cb.PilotContextBuilder()
    monkeypatch.setattr(builder, "_read_connectors", _slow(30.0, []))
    monkeypatch.setattr(builder, "_read_jobs", _slow(30.0, []))
    builder.build(None, "first")
    start = time.monotonic()
    builder.build(None, "second")
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, f"second turn still waited {elapsed:.2f}s on a dead store"


def test_the_rag_read_is_only_attempted_when_it_is_switched_on(monkeypatch):
    """RAG snippets are opt-in; the read must not be submitted when they are off."""
    monkeypatch.delenv("DATAFLOW_PILOT_RAG", raising=False)
    monkeypatch.delenv("PILOT_RAG", raising=False)
    called = []
    builder = cb.PilotContextBuilder()
    monkeypatch.setattr(
        builder, "_read_rag", lambda message: called.append(message) or []
    )
    ctx = builder.build(None, "what gets quarantined")
    assert called == []
    assert ctx["rag_knowledge"] == []
