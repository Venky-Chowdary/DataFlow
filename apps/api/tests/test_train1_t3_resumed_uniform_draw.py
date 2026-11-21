"""T3 (train review): a resumed run's Gate-8 sample must cover the whole run.

The merger only saw pages after a checkpoint resume, yet the draw was named
uniform_random and the Trust Report stated the 95% bound for the whole table
(900k rows written, crash, 100k more: the sample came from the last 100k).
The draw now rides in the checkpoint and a resumed pass continues it; without
it the draw is partial, not named uniform, and no bound is stated.
"""

from __future__ import annotations

from services.checkpoint_service import Checkpoint
from services.gate8_uniform_sample import METHOD, UniformSampleMerger, resume_draw
from src.transfer.reconcile_step import _stamp_uniform_draw

K = 5


def _page(idx: int, n: int = 40) -> tuple[list[dict], int]:
    rows = [{"id": idx * 1000 + i} for i in range(n)]
    from services.gate8_uniform_sample import page_sample_indices

    seed_rows = page_sample_indices(_page.seed, idx, n, K)
    return [rows[i] for i in seed_rows], n


def _run(merger: UniformSampleMerger, pages: range) -> None:
    for idx in pages:
        sample, n = _page(idx)
        merger.add(idx, sample, n)


def test_a_resumed_pass_continues_the_checkpointed_draw():
    draw, whole = resume_draw(None, K, resumed=False)
    _page.seed = draw["seed"]
    _run(whole, range(10))

    _, first = resume_draw(None, K, resumed=False)
    first.seed = draw["seed"]
    _run(first, range(6))
    cp = Checkpoint(job_id="j", chunk_index=5, rows_processed=240)
    cp.reconcile_sample_draw_state = first.state(draw, 5)
    cp = Checkpoint.from_dict(cp.to_dict())  # through the stored document

    resumed_draw, resumed = resume_draw(cp, K, resumed=True)
    assert resumed_draw["seed"] == draw["seed"] and not resumed.partial
    _run(resumed, range(6, 10))
    assert resumed.population == whole.population == 400
    assert resumed.rows == whole.rows
    record = resumed.draw_record(resumed_draw)
    assert record["covers_whole_run"] is True and record["population_rows"] == 400


def test_a_resume_without_the_draw_is_partial_and_states_no_bound():
    cp = Checkpoint(job_id="j", chunk_index=5, rows_processed=900_000)
    draw, merger = resume_draw(cp, K, resumed=True)
    assert merger.partial is True
    merger.add(6, [{"id": 1}], 100_000)
    assert merger.draw_record(draw)["covers_whole_run"] is False
    # A copy saved at an older page (job document behind the resume token) too.
    stale = Checkpoint(job_id="j", chunk_index=9, rows_processed=900_000)
    stale.reconcile_sample_draw_state = UniformSampleMerger("s", K).state(
        {"method": METHOD, "seed": "s", "size": K, "algorithm": "a"}, 5,
    )
    assert resume_draw(stale, K, resumed=True)[1].partial is True


def test_gate8_does_not_name_a_partial_resumed_draw_uniform():
    compare = {"alignment": "keyed", "rows_compared": 1, "sample_seed": {"method": "first_rows"}}
    records = [{"id": 1}]
    draw = {"method": METHOD, "seed": "s", "drawn_rows": 1, "population_rows": 100_000}
    for covers in (None, False):
        summary = {"resumed_checkpoint_rows": 900_000,
                   "reconcile_sample_draw": {**draw, **({} if covers is None else {"covers_whole_run": covers})}}
        out = _stamp_uniform_draw(compare, summary, records, streamed=True)
        assert out["sample_seed"]["method"] == "first_rows"
        assert "resumed from a checkpoint" in out["uniform_draw_incomplete"]["reason"]
    whole = {"resumed_checkpoint_rows": 900_000,
             "reconcile_sample_draw": {**draw, "population_rows": 1_000_000, "covers_whole_run": True}}
    out = _stamp_uniform_draw(compare, whole, records, streamed=True)
    assert out["sample_seed"]["method"] == "uniform_random"
    assert out["sample_seed"]["population_rows"] == 1_000_000


def test_shown_checkpoint_masks_the_draw_rows(monkeypatch):
    """The draw's rows are source values: shown like reconcile_sample is."""
    from src.transfer import models

    monkeypatch.setattr(models, "_shown_wire_sample", lambda rows: [{"masked": len(rows)}])
    job = {"checkpoint": {"reconcile_sample_draw_state": {"seed": "s", "rows": [{"id": 1, "ssn": "123"}]}}}
    models._project_operator_quarantine(job)
    assert job["checkpoint"]["reconcile_sample_draw_state"]["rows"] == [{"masked": 1}]
