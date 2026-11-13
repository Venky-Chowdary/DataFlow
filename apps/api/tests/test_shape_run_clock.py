"""Run start time as a computed column: one value per run, recorded, replayed.

The Map step's "Run start time" source becomes a ``run_timestamp`` Transform
step. The Transform engine has no clock of its own (a recipe is a pure function
of its rows), so the value comes from the run: fixed once when the job's scope
opens, stamped on the job record, read back by a resumed pass, and recorded in
the run's evidence so Gate-8 recomputes the expected rows with the same value.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from services import run_clock
from services.shape_apply import ShapeRunner, recipe_output_schema
from services.shape_engine import ShapeEngine
from services.shape_models import ShapeError, ShapeRecipe

RECIPE = {"steps": [{"op": "run_timestamp", "options": {"to": "loaded_at"}}]}
T0 = "2026-11-13T10:00:00+00:00"


class _Store:
    """A job store with just what the run clock touches."""

    def __init__(self, jobs: dict[str, dict[str, Any]] | None = None) -> None:
        self.jobs = jobs or {}
        self.writes = 0

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.get(job_id)

    def update_job_fields(self, job_id: str, fields: dict[str, Any], **_: Any) -> None:
        self.writes += 1
        self.jobs.setdefault(job_id, {}).update(fields)


def test_the_step_parses_and_its_identity_carries_no_clock():
    first = ShapeRecipe.parse(RECIPE, source_columns=["id"])
    again = ShapeRecipe.parse(RECIPE, source_columns=["id"])
    assert first.recipe_hash == again.recipe_hash
    assert first.output_columns == ("id", "loaded_at")


def test_a_literal_value_cannot_ride_on_the_step():
    with pytest.raises(ShapeError):
        ShapeRecipe.parse(
            {"steps": [{"op": "run_timestamp", "options": {"to": "loaded_at", "value": T0}}]},
            source_columns=["id"],
        )


def test_every_row_and_every_batch_gets_the_one_run_value():
    engine = ShapeEngine(ShapeRecipe.parse(RECIPE, source_columns=["id"]), run_started_at=T0)
    rows = engine.apply_batch([{"id": i} for i in range(3)]) + engine.apply_batch([{"id": 9}])
    expected = datetime(2026, 11, 13, 10, 0, tzinfo=timezone.utc)
    assert [r["loaded_at"] for r in rows] == [expected] * 4
    assert engine.run_clock == {"started_at": T0, "source": "run"}


def test_a_resumed_pass_replays_the_recorded_value_not_a_new_clock():
    store = _Store()
    with run_clock.run_clock_scope("job-1", store=lambda: store):
        first = run_clock.current_run_started_at()
    assert store.jobs["job-1"]["run_clock"]["started_at"] == first
    # Later (a resume after a crash): the same job replays the same value.
    with run_clock.run_clock_scope("job-1", store=lambda: store):
        resumed = run_clock.current_run_started_at()
        engine = ShapeEngine(ShapeRecipe.parse(RECIPE, source_columns=["id"]))
    assert resumed == first
    assert engine.run_clock == {"started_at": first, "source": "run"}
    assert store.writes == 1, "the clock is stamped once per job"


def test_a_run_without_the_step_never_touches_the_job_record():
    store = _Store()
    with run_clock.run_clock_scope("job-2", store=lambda: store):
        ShapeEngine(ShapeRecipe.parse({"steps": [{"op": "drop_column", "column": "id"}]}, source_columns=["id"]))
    assert store.writes == 0 and "job-2" not in store.jobs


def test_the_clock_is_whole_seconds_utc():
    store = _Store()
    with run_clock.run_clock_scope("job-3", store=lambda: store):
        value = run_clock.current_run_started_at()
    moment = datetime.fromisoformat(value)
    assert moment.tzinfo is not None and moment.utcoffset().total_seconds() == 0
    assert moment.microsecond == 0


def test_preview_and_validate_use_one_labelled_design_clock():
    engine = ShapeEngine(ShapeRecipe.parse(RECIPE, source_columns=["id"]))
    rows = engine.apply_batch([{"id": 1}, {"id": 2}])
    assert rows[0]["loaded_at"] == rows[1]["loaded_at"]
    assert engine.run_clock["source"] == "preview"


def test_validate_types_the_column_as_an_instant():
    recipe = ShapeRecipe.parse(RECIPE, source_columns=["id"])
    runner = ShapeRunner(recipe, run_started_at=T0)
    shaped = runner.records([{"id": 1}])
    schema = recipe_output_schema(recipe, ["id", "loaded_at"], shaped, {"id": "INTEGER"})
    assert schema["loaded_at"] == "TIMESTAMPTZ"
    assert runner.run_clock == {"started_at": T0, "source": "run"}


def test_gate8_recompute_with_the_recorded_value_matches_the_write():
    recipe = ShapeRecipe.parse(RECIPE, source_columns=["id"])
    written = ShapeRunner(recipe, run_started_at=T0).records([{"id": 1}])
    # Gate-8 runs later, maybe in another process: it reads the evidence.
    replayed = ShapeRunner(recipe, run_started_at=T0).records([{"id": 1}])
    assert written == replayed


def test_a_design_time_probe_keeps_the_preview_label_and_value():
    runner = ShapeRunner(ShapeRecipe.parse(RECIPE, source_columns=["id"]))
    probe = runner.fork()
    assert probe.run_clock == runner.run_clock
    assert probe.run_clock["source"] == "preview"


@pytest.fixture
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_the_transform_preview_says_a_run_fixes_its_own_start(client):
    res = client.post(
        "/api/v1/shape/preview",
        json={"recipe": RECIPE, "sample_rows": [{"id": "1"}, {"id": "2"}], "source_columns": ["id"]},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    stamps = {r["loaded_at"] for r in body["after"]}
    assert len(stamps) == 1, "one value for the whole preview"
    assert "a run fixes its own value once" in body["preview_note"]
