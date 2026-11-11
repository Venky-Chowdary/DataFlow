"""D332: ``normalize_unicode`` in a column_transforms pipeline does what the
catalog says — "NFKC normalize + collapse whitespace" — so internal tabs and
newlines become one space.

QA (Oracle -> SQL Server, ``NAME: ["normalize_unicode", "lower"]``): rows with
an internal TAB and LF kept them. The pipeline compiled ``normalize_unicode``
to the shape step of the same name, which applies only a Unicode form (NFC by
default) and never collapses whitespace.
"""

from __future__ import annotations

import re
import unicodedata

import pytest

from services.column_transform_chain import compile_column_chains
from services.shape_engine import ShapeEngine
from services.shape_expr import _BLANK_AS_NULL
from services.shape_models import ShapeError, ShapeRecipe
from services.transform_engine import TRANSFORM_SEMANTICS, apply_transform

VALUES = [
    "O'Brien \\ back\\slash, comma \"q\" \ttab",
    "line1\nline2",
    "cr\rhere",
    "mixed \t \n  spaces",
    "ＡＢＣ full-width",  # compatibility characters: ＡＢＣ -> ABC
    "ﬁ ligature",
    "  ends  ",
]


def _independent(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip().lower()


def _shape(chains, columns, rows, *, typed: bool = True):
    payload, refusal = compile_column_chains(chains, columns)
    assert refusal == ""
    engine = ShapeEngine(ShapeRecipe.parse(payload, source_columns=columns))
    token = _BLANK_AS_NULL.set(not typed)
    try:
        return engine.apply_batch(rows)
    finally:
        _BLANK_AS_NULL.reset(token)


@pytest.mark.parametrize("typed", [True, False])
def test_pipeline_collapses_tab_newline_cr_and_applies_nfkc(typed):
    out = _shape(
        {"NAME": ["normalize_unicode", "lower"]},
        ["ID", "NAME"],
        [{"ID": i, "NAME": v} for i, v in enumerate(VALUES)],
        typed=typed,
    )
    got = [r["NAME"] for r in out]
    assert got == [_independent(v) for v in VALUES]
    assert all("\t" not in g and "\n" not in g and "\r" not in g for g in got)
    assert got[4] == "abc full-width"


def test_pipeline_step_matches_the_single_step_map_transform():
    values = VALUES + ["zero​width", "bom﻿x", None]
    out = _shape({"NAME": ["normalize_unicode"]}, ["NAME"], [{"NAME": v} for v in values])
    assert [r["NAME"] for r in out] == [apply_transform(v, "normalize_unicode")[0] for v in values]


def test_catalog_still_promises_nfkc_and_collapse():
    assert TRANSFORM_SEMANTICS["normalize_unicode"]["semantics"] == (
        "NFKC normalize + collapse whitespace"
    )


def test_shape_normalize_unicode_without_the_option_keeps_its_form_only():
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "normalize_unicode", "column": "n", "options": {"form": "NFC"}}]},
        source_columns=["n"],
    )
    assert ShapeEngine(recipe).apply_batch([{"n": "a\tb"}]) == [{"n": "a\tb"}]


def test_collapse_whitespace_refuses_a_non_nfkc_form():
    with pytest.raises(ShapeError, match="NFKC"):
        ShapeRecipe.parse(
            {
                "steps": [
                    {
                        "op": "normalize_unicode",
                        "column": "n",
                        "options": {"form": "NFC", "collapse_whitespace": True},
                    }
                ]
            },
            source_columns=["n"],
        )
