"""P1 set-operations: the buffering drain + stateful row ops.

A dedupe/aggregate/sort/rank step cannot judge a row until it has seen the
population, so it holds rows and the engine drains them at end-of-stream.
These tests prove the held rows still reach the writer, that downstream steps
run on the drained winners, and that the conservation ledger balances.
"""

from __future__ import annotations

import pytest

from services.shape_apply import ShapeRunner
from services.shape_models import ShapeError, ShapeRecipe


def _runner(payload: dict) -> ShapeRunner:
    return ShapeRunner(ShapeRecipe.parse(payload))


def _run(payload: dict, rows: list[dict]) -> tuple[list[dict], ShapeRunner]:
    runner = _runner(payload)
    out = runner.records(rows)
    out += runner.drain()
    return out, runner


ROWS = [
    {"id": "1", "v": "old", "ts": 1, "dept": "ENG", "amt": 10},
    {"id": "2", "v": "b", "ts": 2, "dept": "OPS", "amt": 30},
    {"id": "1", "v": "new", "ts": 3, "dept": "ENG", "amt": 15},
]


def test_dedupe_keep_first_is_streaming_no_drain() -> None:
    out, runner = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "first"}}],
        },
        ROWS,
    )
    assert [r["id"] for r in out] == ["1", "2"]
    assert out[0]["v"] == "old"
    assert runner.drain() == []
    assert runner._engine.rows_accounted_for()


def test_dedupe_keep_latest_by_order_column() -> None:
    out, runner = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {
                        "keys": ["id"],
                        "keep": "last",
                        "order_by": "ts",
                        "order_dir": "desc",
                    },
                }
            ],
        },
        ROWS,
    )
    assert sorted(r["id"] for r in out) == ["1", "2"]
    winner = next(r for r in out if r["id"] == "1")
    assert winner["v"] == "new" and winner["ts"] == 3
    assert runner._engine.rows_accounted_for()


def test_dedupe_composite_keys() -> None:
    out, _ = _run(
        {
            "input_columns": ["id", "dept", "ts"],
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {"keys": ["id", "dept"], "keep": "last", "order_by": "ts"},
                }
            ],
        },
        ROWS,
    )
    # id=1/ENG twice — latest wins; id=2/OPS is a different composite key.
    assert len(out) == 2
    assert next(r for r in out if r["id"] == "1")["v"] == "new"


def test_aggregate_group_by_sum_and_count() -> None:
    out, runner = _run(
        {
            "input_columns": ["dept", "amt"],
            "steps": [
                {
                    "op": "aggregate",
                    "options": {
                        "group_by": ["dept"],
                        "measures": [
                            {"function": "sum", "column": "amt", "as": "total"},
                            {"function": "count", "as": "n"},
                        ],
                    },
                }
            ],
        },
        ROWS,
    )
    by_dept = {r["dept"]: r for r in out}
    assert by_dept["ENG"]["total"] == 25 and by_dept["ENG"]["n"] == 2
    assert by_dept["OPS"]["total"] == 30 and by_dept["OPS"]["n"] == 1
    assert runner._engine.rows_accounted_for()


def test_aggregate_avg_min_max() -> None:
    out, _ = _run(
        {
            "input_columns": ["amt"],
            "steps": [
                {
                    "op": "aggregate",
                    "options": {
                        "group_by": [],
                        "measures": [
                            {"function": "avg", "column": "amt", "as": "m"},
                            {"function": "min", "column": "amt", "as": "lo"},
                            {"function": "max", "column": "amt", "as": "hi"},
                        ],
                    },
                }
            ],
        },
        [{"amt": 10}, {"amt": 20}, {"amt": 30}],
    )
    assert len(out) == 1
    assert out[0]["m"] == 20 and out[0]["lo"] == 10 and out[0]["hi"] == 30


def test_sort_rows_mixed_direction() -> None:
    out, runner = _run(
        {
            "input_columns": ["dept", "amt"],
            "steps": [
                {
                    "op": "sort_rows",
                    "options": {
                        "order": [
                            {"column": "dept", "direction": "asc"},
                            {"column": "amt", "direction": "desc"},
                        ]
                    },
                }
            ],
        },
        ROWS,
    )
    assert [(r["dept"], r["amt"]) for r in out] == [
        ("ENG", 15),
        ("ENG", 10),
        ("OPS", 30),
    ]
    assert runner._engine.rows_accounted_for()


def test_rank_top_n_per_partition() -> None:
    out, _ = _run(
        {
            "input_columns": ["id", "dept", "amt"],
            "steps": [
                {
                    "op": "rank_rows",
                    "options": {
                        "order_by": "amt",
                        "direction": "top",
                        "limit": 1,
                        "partition_by": ["dept"],
                    },
                }
            ],
        },
        ROWS,
    )
    # Top amt per dept: ENG→15, OPS→30
    assert sorted(r["amt"] for r in out) == [15, 30]


def test_lookup_match_unmatched_modes() -> None:
    table = [{"code": "ENG", "label": "Engineering"}]
    matched, _ = _run(
        {
            "input_columns": ["dept", "v"],
            "steps": [
                {
                    "op": "lookup",
                    "column": "dept",
                    "options": {
                        "lookup_key": "code",
                        "rows": table,
                        "map": {"label": "dept_name"},
                    },
                }
            ],
        },
        ROWS,
    )
    hit = next(r for r in matched if r["dept"] == "ENG")
    assert hit["dept_name"] == "Engineering"
    miss = next(r for r in matched if r["dept"] == "OPS")
    assert miss["dept_name"] is None


def test_lookup_unmatched_drop() -> None:
    out, _ = _run(
        {
            "input_columns": ["dept"],
            "steps": [
                {
                    "op": "lookup",
                    "column": "dept",
                    "options": {
                        "lookup_key": "code",
                        "rows": [{"code": "ENG", "label": "Engineering"}],
                        "map": {"label": "dept_name"},
                        "unmatched": "drop",
                    },
                }
            ],
        },
        ROWS,
    )
    assert {r["dept"] for r in out} == {"ENG"}


def test_lookup_unmatched_divert() -> None:
    runner = _runner(
        {
            "input_columns": ["dept"],
            "steps": [
                {
                    "op": "lookup",
                    "column": "dept",
                    "options": {
                        "lookup_key": "code",
                        "rows": [{"code": "ENG", "label": "Engineering"}],
                        "map": {"label": "dept_name"},
                        "unmatched": "divert",
                    },
                }
            ],
        }
    )
    out = runner.records(ROWS) + runner.drain()
    assert {r["dept"] for r in out} == {"ENG"}
    assert runner._engine.effect.rows_diverted == 1
    assert runner._engine.rows_accounted_for()


def test_surrogate_key_increments_across_batches() -> None:
    runner = _runner(
        {
            "input_columns": ["id"],
            "steps": [{"op": "surrogate_key", "options": {"to": "sk", "start": 100}}],
        }
    )
    first = runner.records([{"id": "a"}])
    second = runner.records([{"id": "b"}])
    assert first[0]["sk"] == 100 and second[0]["sk"] == 101


def test_buffering_step_then_downstream_step() -> None:
    """A step after the buffering step still applies to the drained rows."""
    out, _ = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {"keys": ["id"], "keep": "last", "order_by": "ts"},
                },
                {"op": "case", "column": "v", "options": {"mode": "upper"}},
            ],
        },
        ROWS,
    )
    assert next(r for r in out if r["id"] == "1")["v"] == "NEW"


def test_two_buffering_steps_chain() -> None:
    """dedupe then sort: both hold, drain emits in step order."""
    out, runner = _run(
        {
            "input_columns": ["id", "v", "ts"],
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {"keys": ["id"], "keep": "last", "order_by": "ts"},
                },
                {"op": "sort_rows", "options": {"order_by": "id", "direction": "desc"}},
            ],
        },
        ROWS,
    )
    assert [r["id"] for r in out] == ["2", "1"]
    assert runner._engine.rows_accounted_for()


def test_drain_is_idempotent() -> None:
    runner = _runner(
        {
            "input_columns": ["id", "ts"],
            "steps": [
                {
                    "op": "dedupe_rows",
                    "options": {"keys": ["id"], "keep": "last", "order_by": "ts"},
                }
            ],
        }
    )
    runner.records(ROWS)
    once = runner.drain()
    assert len(once) == 2
    assert runner.drain() == []


def test_dedupe_alias_names_the_real_op() -> None:
    """The old 'dedupe' name still refuses — and teaches the real op."""
    with pytest.raises(ShapeError, match="dedupe_rows"):
        ShapeRecipe.parse(
            {
                "input_columns": ["id"],
                "steps": [{"op": "dedupe", "options": {"keys": ["id"]}}],
            }
        )


def test_rank_respects_limit_zero() -> None:
    out, _ = _run(
        {
            "input_columns": ["id", "amt"],
            "steps": [
                {
                    "op": "rank_rows",
                    "options": {"order_by": "amt", "direction": "top", "limit": 0},
                }
            ],
        },
        ROWS,
    )
    # limit=0 means "no cap" — every row ranks in.
    assert len(out) == 3


def test_aggregate_column_contract() -> None:
    """The recipe's declared output is group cols + measure aliases."""
    runner = _runner(
        {
            "input_columns": ["dept", "amt", "v"],
            "steps": [
                {
                    "op": "aggregate",
                    "options": {
                        "group_by": ["dept"],
                        "measures": [{"function": "sum", "column": "amt", "as": "total"}],
                    },
                }
            ],
        }
    )
    runner.records([{"dept": "A", "amt": 1, "v": 9}])
    runner.drain()
    assert tuple(runner.output_columns) == ("dept", "total")
