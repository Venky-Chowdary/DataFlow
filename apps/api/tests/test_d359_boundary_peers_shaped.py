"""D359 — the append boundary dedupe compares re-read peers as the recipe wrote them.

Unit level of ``test_d359_ia_chain_transforms_e2e``: a keyless tie peer is
landed only when its *shaped* row equals a stored row; a row the recipe removes
is never "landed"; a recipe without a one-to-one image is refused by name.
"""

from __future__ import annotations

import pytest

from connectors.writer_common import row_fingerprints
from services.shape_apply import build_shape_runner
from src.transfer.stream_boundary import _boundary_append_dedupe, shape_boundary_peers
from src.transfer.stream_boundary_read import BoundaryStructural

HEADERS = ["id", "name", "updated_at"]
TARGETS = ["id", "name", "updated_at"]
MAPPINGS = [{"source": c, "target": c} for c in TARGETS]


def _runner(*steps):
    return build_shape_runner({"steps": list(steps)}, source_columns=HEADERS, typed_values=True)


def _dest_fps(rows):
    return {
        fp
        for _k, fp in row_fingerprints(
            [tuple(r) for r in rows], TARGETS, dest_db_type="postgresql", dest_types=None
        )
    }


def _dedupe(peers, stored, runner):
    return _boundary_append_dedupe(
        HEADERS,
        peers,
        _dest_fps(stored),
        match_cols=TARGETS,
        mappings=MAPPINGS,
        target_cols=TARGETS,
        column_types=None,
        stream_error_policy=None,
        digest_dest_types=None,
        dest_bind_dialect="postgresql",
        shape_runner=runner,
    )


def test_a_transformed_watermark_row_is_recognised_as_already_landed():
    runner = _runner(
        {"op": "trim", "column": "name"},
        {"op": "case", "column": "name", "options": {"mode": "lower"}},
    )
    peers = [["30", "  Name30 ", "2026-11-01 10:30:00"], ["31", " New31 ", "2026-11-01 10:30:00"]]
    stored = [["30", "name30", "2026-11-01 10:30:00"]]

    assert _dedupe(peers, stored, runner) == {0}
    # The raw comparison (the defect): the stored copy never matches.
    assert _dedupe(peers, stored, None) == set()
    # The probe runs on a fork: the run's own effect ledger is untouched.
    assert runner.report()["effect"] == _runner(
        {"op": "trim", "column": "name"},
        {"op": "case", "column": "name", "options": {"mode": "lower"}},
    ).report()["effect"]


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
def test_a_recipe_without_a_one_to_one_image_is_refused_by_name(step):
    runner = _runner(step)
    with pytest.raises(BoundaryStructural, match="exactly one written row"):
        shape_boundary_peers(runner, HEADERS, [["30", "a", "t"]])


def test_no_recipe_passes_the_rows_through():
    rows = [["1", "a", "t"]]
    assert shape_boundary_peers(None, HEADERS, rows) == (HEADERS, rows)
