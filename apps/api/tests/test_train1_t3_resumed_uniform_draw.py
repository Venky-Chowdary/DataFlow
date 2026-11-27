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


def test_shown_checkpoint_masks_the_draw_rows_and_hides_the_seed():
    """Review 2 item 8: the persisted draw rows are masked like any sample the
    product shows, and the seed (it fixes every later sampled position) is not
    in the operator view while the job can still draw."""
    from src.transfer.models import sanitize_job_for_api

    rows = [{"id": 1, "ssn": "123-45-6789", "note": "call 555-867-5309"}]
    job = {
        "status": "running",
        "checkpoint": {
            "reconcile_sample": [dict(rows[0])],
            "reconcile_sample_draw_state": {"seed": "s3cr3t", "method": "uniform_random", "rows": rows},
        },
        "destination_summary": {
            "reconcile_sample": [dict(rows[0])],
            "reconcile_sample_draw": {"seed": "s3cr3t", "method": "uniform_random"},
        },
    }
    shown = sanitize_job_for_api(job)
    text = str(shown)
    assert "123-45-6789" not in text and "555-867-5309" not in text
    assert "s3cr3t" not in text
    assert shown["checkpoint"]["reconcile_sample_draw_state"]["method"] == "uniform_random"
    # A finished run's report keeps the seed (the draw is recorded for audit).
    done = sanitize_job_for_api({**job, "status": "completed"})
    assert done["destination_summary"]["reconcile_sample_draw"]["seed"] == "s3cr3t"
    assert "s3cr3t" not in str(done["checkpoint"])


def test_the_seed_stays_hidden_while_a_run_can_still_draw():
    """Review 3 item 6: a paused run, and a failed or cancelled one a retry
    resumes from its checkpoint, can still draw; only a completed run shows it."""
    from src.transfer.models import sanitize_job_for_api

    for status in ("paused", "failed", "cancelled", "pending", "queued", "running", "retrying", ""):
        job = {"status": status, "destination_summary": {
            "reconcile_sample_draw": {"seed": "s3cr3t", "method": "uniform_random"}}}
        assert "s3cr3t" not in str(sanitize_job_for_api(job)), status
    for status in ("completed", "completed_with_quarantine", "completed_with_warnings"):
        job = {"status": status, "destination_summary": {
            "reconcile_sample_draw": {"seed": "s3cr3t", "method": "uniform_random"}}}
        assert sanitize_job_for_api(job)["destination_summary"]["reconcile_sample_draw"]["seed"] == "s3cr3t"


def test_the_checkpointed_draw_rows_are_sealed_at_rest_and_resume_exactly():
    """Review 3 item 6: the draw rows are source values; the job store holds
    them sealed, never in the clear, and a resume reads them back typed."""
    from datetime import date, datetime, timezone
    from decimal import Decimal

    rows = [{"id": 1, "ssn": "123-45-6789", "amt": Decimal("10.50"),
             "at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "d": date(2026, 1, 2),
             "blob": b"\x00\x01", "doc": {"$t": "not a tag", "n": [1, 2]}}]
    merger = UniformSampleMerger("seed-1", 50)
    merger.add(0, rows, 1)
    state = merger.state({"method": METHOD, "seed": "seed-1", "size": 50}, 0)
    stored = Checkpoint(job_id="j", chunk_index=0, reconcile_sample_draw_state=state).to_dict()
    assert "123-45-6789" not in str(stored)
    assert "rows" not in stored["reconcile_sample_draw_state"]
    _draw, resumed = resume_draw(Checkpoint.from_dict(stored), 50, resumed=True)
    assert not resumed.partial
    assert resumed.rows == rows
    assert resumed.population == 1
