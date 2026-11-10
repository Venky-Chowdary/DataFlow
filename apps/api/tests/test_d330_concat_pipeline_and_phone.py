"""D330: a ``concat:`` step runs inside a column_transforms pipeline, and the
``phone`` transform normalizes instead of copying the formatted source.

QA: ``column_transforms={"notes": ["strip_controls", "concat:_x"]}`` was refused
("no faithful shape-step equivalent") although ``concat:_x`` works as a single
step, and ``phone`` wrote ``(555) 010-0101`` / ``555.010.0102`` unchanged while
``describe_connector_transforms`` said "normalize phone number".
"""

from __future__ import annotations

import pytest

from services.column_transform_chain import compile_column_chains
from services.shape_engine import ShapeEngine
from services.shape_expr import _BLANK_AS_NULL
from services.shape_models import ShapeRecipe
from services.transform_engine import TRANSFORM_SEMANTICS, apply_transform


def _shape(chains, columns, rows, *, typed: bool):
    payload, refusal = compile_column_chains(chains, columns)
    assert refusal == ""
    engine = ShapeEngine(ShapeRecipe.parse(payload, source_columns=columns))
    token = _BLANK_AS_NULL.set(not typed)
    try:
        return engine.apply_batch(rows)
    finally:
        _BLANK_AS_NULL.reset(token)


def test_pipeline_with_concat_compiles_and_appends_the_suffix():
    rows = [
        {"id": 1, "notes": "a\x01b"},
        {"id": 2, "notes": "plain"},
        {"id": 3, "notes": None},
        {"id": 4, "notes": ""},
    ]
    out = _shape(
        {"notes": ["strip_controls", "concat:_x"]}, ["id", "notes"], rows, typed=True
    )
    assert [r["notes"] for r in out] == ["ab_x", "plain_x", None, "_x"]


def test_pipeline_concat_matches_the_single_step_map_transform():
    values = ["plain", "with space", None, ""]
    out = _shape(
        {"notes": ["concat:-sfx"]},
        ["notes"],
        [{"notes": v} for v in values],
        typed=True,
    )
    single = [apply_transform(v, "concat:-sfx")[0] for v in values]
    assert [r["notes"] for r in out] == single


def test_concat_suffix_is_a_literal_not_expression_text():
    suffix = "it's ], [notes] + 1"
    out = _shape(
        {"notes": ["trim", f"concat:{suffix}"]},
        ["notes"],
        [{"notes": " v "}],
        typed=True,
    )
    assert out[0]["notes"] == "v" + suffix


def test_concat_on_unreferenceable_column_is_refused_by_name():
    _payload, refusal = compile_column_chains({"a]b": ["trim", "concat:_x"]}, ["a]b"])
    assert "concat:" in refusal and "a]b" in refusal


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("(555) 010-0101", "5550100101"),
        ("555.010.0102", "5550100102"),
        ("5550100104", "5550100104"),
        ("+1 (555) 123-4567", "+15551234567"),
        ("+44 20 7946 0958", "+442079460958"),
    ],
)
def test_phone_normalizes_to_documented_form(raw, expected):
    assert apply_transform(raw, "phone") == (expected, None)


@pytest.mark.parametrize("raw", ["abc", "12-34", "+1+555 123 4567", "555-0199 ext 12"])
def test_phone_still_refuses_garbage(raw):
    value, err = apply_transform(raw, "phone")
    assert value is None and err and "Invalid phone" in err


def test_phone_catalog_names_the_form_and_the_region_rule():
    text = TRANSFORM_SEMANTICS["phone"]["semantics"]
    assert "E.164" in text and "no region" in text
