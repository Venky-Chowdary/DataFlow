"""A refused run must come with a way to resolve it.

Blocking a scheduled run on source drift is only half a feature. The message
asks the operator to review the change, and until there is a control that
performs that review the refusal is the same dead end this product exists to
remove — a failure whose only offered action cannot succeed.

Two exits exist and both are deliberate: open the mapping, or record the
source's current shape as the new baseline. Re-baselining is the operator
asserting the change is understood, so it is never something a retry does on
their behalf.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.source_schema_memory import evaluate_source_drift, fingerprint_source

BASE = {"id": "BIGINT", "currency": "VARCHAR(3)", "amount": "DECIMAL(12,2)"}
DRIFTED = {**BASE, "currency": "INTEGER"}
MAPPINGS = [{"source": c, "target": c} for c in BASE]


class _Sched:
    """Minimal stand-in for the fields the guard reads."""

    def __init__(self, schema: dict[str, str] | None, policy: str = "manual_review"):
        self.id = "sched-1"
        self.source_schema = dict(schema or {})
        self.source_schema_fingerprint = (
            fingerprint_source(list(schema or {}), schema) if schema else ""
        )
        self.schema_policy = policy


class _Endpoint:
    format = "postgresql"


class _Request:
    def __init__(self):
        self.source = _Endpoint()
        self.destination = _Endpoint()
        self.mappings = MAPPINGS


def _guard(monkeypatch, sched, introspected: dict[str, Any] | None, recorded: list):
    import services.schedule_runner as runner

    def fake_introspect(_endpoint):
        if introspected is None:
            raise RuntimeError("probe unavailable")
        return introspected

    monkeypatch.setattr(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        fake_introspect,
        raising=False,
    )
    monkeypatch.setattr(
        runner,
        "_remember_source_schema",
        lambda s, schema, fp: recorded.append((schema, fp)),
    )
    runner._guard_source_schema_drift(sched, _Request())


def test_a_read_column_changing_type_refuses_the_run(monkeypatch):
    recorded: list = []
    with pytest.raises(ValueError) as excinfo:
        _guard(monkeypatch, _Sched(BASE), {"schema": DRIFTED}, recorded)
    message = str(excinfo.value)
    assert "currency" in message
    # The refusal has to name an exit, or it is the dead end it replaced.
    assert "schema_policy" in message or "Review" in message
    assert recorded == [], "a refused run must not move the baseline"


def test_an_unchanged_source_runs_and_keeps_its_baseline(monkeypatch):
    recorded: list = []
    _guard(monkeypatch, _Sched(BASE), {"schema": dict(BASE)}, recorded)
    assert recorded, "the observed shape is recorded so the next run can compare"


def test_a_first_run_records_a_baseline_rather_than_refusing(monkeypatch):
    recorded: list = []
    _guard(monkeypatch, _Sched(None), {"schema": dict(BASE)}, recorded)
    assert recorded and recorded[0][0] == BASE


def test_an_unreadable_source_does_not_refuse_the_run(monkeypatch):
    """A probe that failed is not evidence of a change.

    Refusing a nightly load because introspection timed out is the false alarm
    that gets the check switched off.
    """
    recorded: list = []
    _guard(monkeypatch, _Sched(BASE), None, recorded)
    assert recorded == []


def test_an_empty_schema_is_not_read_as_every_column_dropped(monkeypatch):
    recorded: list = []
    _guard(monkeypatch, _Sched(BASE), {"schema": {}}, recorded)
    assert recorded == []


def test_accepting_the_new_shape_clears_the_finding():
    """After re-baselining, the same shape no longer blocks."""
    assert evaluate_source_drift(
        previous_schema=BASE, current_schema=DRIFTED, mappings=MAPPINGS
    ).blocks
    assert not evaluate_source_drift(
        previous_schema=DRIFTED, current_schema=DRIFTED, mappings=MAPPINGS
    ).blocks


def test_the_accept_endpoint_is_registered():
    """The control the refusal points at has to exist."""
    from src.routers.schedules_router import router

    paths = {getattr(r, "path", "") for r in router.routes}
    assert any("accept-source-schema" in p for p in paths)
