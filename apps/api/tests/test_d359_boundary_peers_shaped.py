"""D359 — the append boundary dedupe compares re-read peers as the recipe wrote them.

Unit level of ``test_d359_ia_chain_transforms_e2e``: a keyless tie peer is
landed only when its *shaped* row equals a stored row; a row the recipe removes
is never "landed"; a buffering step (sort_rows, dedupe_rows) drains a single
row as itself; only a step with no per-row image (aggregate, rank_rows, an
unnest that yields several rows) is refused, by step and op; run-stamped
columns are never matched; one stored copy absorbs one peer.
"""

from __future__ import annotations

from collections import Counter

import pytest

from connectors.writer_common import row_fingerprints
from services.shape_apply import build_shape_runner
from src.transfer.stream_boundary import _boundary_append_dedupe, shape_boundary_peers
from src.transfer.stream_boundary_read import BoundaryStructural
from src.transfer.stream_boundary_recipe import (
    recipe_touches,
    refusal_text,
    run_stamped_targets,
)

HEADERS = ["id", "name", "updated_at"]
TARGETS = ["id", "name", "updated_at"]
MAPPINGS = [{"source": c, "target": c} for c in TARGETS]
TRIM_LOWER = (
    {"op": "trim", "column": "name"},
    {"op": "case", "column": "name", "options": {"mode": "lower"}},
)


def _runner(*steps, columns=HEADERS):
    return build_shape_runner({"steps": list(steps)}, source_columns=columns, typed_values=True)


def _dest_fps(rows, cols=TARGETS):
    return Counter(
        fp
        for _k, fp in row_fingerprints(
            [tuple(r) for r in rows], list(cols), dest_db_type="postgresql", dest_types=None
        )
    )


def _dedupe(peers, stored, runner, match_cols=TARGETS):
    positions = [TARGETS.index(c) for c in match_cols]
    return _boundary_append_dedupe(
        HEADERS,
        peers,
        _dest_fps([[r[p] for p in positions] for r in stored], match_cols),
        match_cols=list(match_cols),
        mappings=MAPPINGS,
        target_cols=TARGETS,
        column_types=None,
        stream_error_policy=None,
        digest_dest_types=None,
        dest_bind_dialect="postgresql",
        shape_runner=runner,
    )


def test_a_transformed_watermark_row_is_recognised_as_already_landed():
    runner = _runner(*TRIM_LOWER)
    peers = [["30", "  Name30 ", "2026-11-01 10:30:00"], ["31", " New31 ", "2026-11-01 10:30:00"]]
    stored = [["30", "name30", "2026-11-01 10:30:00"]]

    assert _dedupe(peers, stored, runner) == {0}
    # The raw comparison (the defect): the stored copy never matches.
    assert _dedupe(peers, stored, None) == set()
    # The probe runs on a fork: the run's own effect ledger is untouched.
    assert runner.report()["effect"] == _runner(*TRIM_LOWER).report()["effect"]


def test_a_row_the_recipe_removes_is_never_landed():
    runner = _runner({"op": "filter_rows", "options": {"condition": "[id] <> 30"}})
    headers, shaped = shape_boundary_peers(runner, HEADERS, [["30", "a", "t"], ["31", "b", "t"]])
    assert shaped[0] is None and shaped[1] == ["31", "b", "t"]
    assert headers == HEADERS


@pytest.mark.parametrize(
    "step",
    [
        {"op": "sort_rows", "options": {"order_by": "id", "direction": "desc"}},
        {"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "last", "order_by": "updated_at"}},
    ],
    ids=["sort-rows", "dedupe-keep-last"],
)
def test_a_buffering_step_drains_each_row_as_itself(step):
    """Reviewer repro (a): these recipes preserve every value; they must not be refused."""
    runner = _runner(step)
    rows = [["30", "a", "t"], ["30", "b", "t"], ["31", "c", "t"]]
    headers, shaped = shape_boundary_peers(runner, HEADERS, rows)
    assert headers == HEADERS
    assert shaped == rows


@pytest.mark.parametrize(
    "step,op",
    [
        ({"op": "rank_rows", "options": {"partition_by": ["id"], "order_by": "id", "limit": 1}}, "rank_rows"),
        ({"op": "aggregate", "options": {"group_by": ["id"], "measures": [{"function": "count", "as": "n"}]}},
         "aggregate"),
    ],
    ids=["rank_rows", "aggregate"],
)
def test_a_step_with_no_per_row_image_is_refused_by_step_and_op(step, op):
    runner = _runner(step)
    with pytest.raises(BoundaryStructural) as caught:
        shape_boundary_peers(runner, HEADERS, [["30", "a", "t"]])
    text = refusal_text(caught.value)
    assert f"Step 1 ({op})" in text
    assert text.rstrip().endswith("'accept possible boundary duplicates'.")
    assert "Declare a primary key" not in text


def test_an_unnest_that_yields_several_rows_is_refused():
    cols = ["id", "tags", "updated_at"]
    runner = _runner({"op": "unnest_json", "column": "tags", "options": {"to": "tag"}}, columns=cols)
    with pytest.raises(BoundaryStructural, match=r"Step 1 \(unnest_json\)"):
        shape_boundary_peers(runner, cols, [["30", '["a", "b"]', "t"]])


def test_a_keyed_match_the_recipe_does_not_touch_is_not_shaped():
    """(cursor, key) are not written by a sort or a trim on name: no shaping, no refusal."""
    sort_only = _runner({"op": "sort_rows", "options": {"order_by": "id"}})
    assert not recipe_touches(sort_only, MAPPINGS, ["updated_at", "id"])
    assert recipe_touches(_runner(*TRIM_LOWER), MAPPINGS, TARGETS)
    peers = [["30", "  Name30 ", "t"]]
    stored = [["30", "name30", "t"]]
    assert _dedupe(peers, stored, _runner(*TRIM_LOWER), match_cols=["updated_at", "id"]) == {0}


def test_run_stamped_columns_and_what_derives_from_them_are_targets_to_skip():
    runner = _runner(
        {"op": "run_timestamp", "options": {"to": "loaded_at"}},
        {"op": "derive_column", "options": {"to": "batch_tag", "expression": "[loaded_at]"}},
        *TRIM_LOWER,
    )
    mappings = [*MAPPINGS, {"source": "loaded_at", "target": "loaded_at_dst"},
                {"source": "batch_tag", "target": "batch_tag"}]
    stamped = run_stamped_targets(runner, mappings)
    assert {"loaded_at_dst", "batch_tag"} <= stamped
    assert not stamped & set(TARGETS)


def test_one_stored_copy_absorbs_one_peer():
    """Two re-read rows with one image and one stored copy: only one is landed."""
    peers = [["30", "  Name30 ", "t"], ["30", "name30", "t"]]
    stored = [["30", "name30", "t"]]
    assert len(_dedupe(peers, stored, _runner(*TRIM_LOWER))) == 1
    assert len(_dedupe(peers, stored + stored, _runner(*TRIM_LOWER))) == 2


def test_no_recipe_passes_the_rows_through():
    rows = [["1", "a", "t"]]
    assert shape_boundary_peers(None, HEADERS, rows) == (HEADERS, rows)
