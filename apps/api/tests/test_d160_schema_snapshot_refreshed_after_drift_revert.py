"""D160 guard — an add-then-drop of a source column must not poison the next run.

The schedule drift guard compares each run's source shape with the baseline the
previous run recorded. A refused run leaves the baseline alone; a run that
proceeds refreshes it. So a column that appears and disappears again never
leaves a stale shape behind, whichever policy was in force (D165 changed which
policies adopt a new column, and must not change this).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import schedule_runner
from services.schedule_approvals import ApprovalRequired

BASE = {"id": "INTEGER", "name": "TEXT"}
GROWN = {**BASE, "extra": "TEXT"}


def _stored_maps():
    return [
        {"source": c, "target": c, "confidence": 1.0, "create_new": True}
        for c in BASE
    ]


@pytest.fixture()
def guard(monkeypatch):
    from services.schema_fingerprint import fingerprint_schema

    state = {"probe": dict(BASE), "updates": []}
    sched = SimpleNamespace(
        id="sched-d160",
        source_schema=dict(BASE),
        source_primary_key=[],
        cursor_column="",
        schema_policy="manual_review",
        backfill_new_fields=False,
        source_schema_fingerprint=fingerprint_schema(list(BASE), BASE),
    )
    request = SimpleNamespace(
        source=SimpleNamespace(format="sqlite"), mappings=_stored_maps()
    )

    monkeypatch.setattr(
        schedule_runner,
        "probe_schedule_source_schema",
        lambda _source: {"schema": dict(state["probe"]), "columns": list(state["probe"])},
    )

    def update_schedule(_schedule_id, fields):
        state["updates"].append(dict(fields))
        sched.source_schema = dict(fields["source_schema"])
        sched.source_schema_fingerprint = fields["source_schema_fingerprint"]

    monkeypatch.setattr("services.schedule_store.update_schedule", update_schedule)
    return state, sched, request


def test_manual_review_refuses_the_new_column_and_the_revert_passes(guard):
    state, sched, request = guard

    state["probe"] = dict(GROWN)
    with pytest.raises(ApprovalRequired):
        schedule_runner._guard_source_schema_drift(sched, request)
    assert state["updates"] == [], "a refused run must not move the baseline"

    state["probe"] = dict(BASE)
    assert schedule_runner._guard_source_schema_drift(sched, request) is False


def test_propagate_adopts_the_column_then_the_drop_refreshes_the_baseline(guard):
    state, sched, request = guard
    sched.schema_policy = "propagate_columns"

    state["probe"] = dict(GROWN)
    schedule_runner._guard_source_schema_drift(sched, request)
    assert state["updates"][-1]["source_schema"] == GROWN

    state["probe"] = dict(BASE)
    schedule_runner._guard_source_schema_drift(sched, request)
    assert state["updates"][-1]["source_schema"] == BASE
    # And the run after the revert is quiet.
    before = len(state["updates"])
    schedule_runner._guard_source_schema_drift(sched, request)
    assert len(state["updates"]) == before


def test_backfill_toggle_behaves_as_propagate(guard):
    state, sched, request = guard
    sched.backfill_new_fields = True

    state["probe"] = dict(GROWN)
    schedule_runner._guard_source_schema_drift(sched, request)

    assert state["updates"][-1]["source_schema"] == GROWN
