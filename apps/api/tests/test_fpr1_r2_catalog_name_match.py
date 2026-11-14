"""PR 411 review 2, F3: a source column matches its catalog entry by exact name.

The catalog lookup lower-cased every name and let the last entry win, so with
columns ``b`` (text) and ``B`` (boolean) the text column ``b`` was marked
``SOURCE_BOOLEAN`` and its ``'true'`` verified as a stored ``'1'``. The exact
name decides; a case-insensitive match is used only when it is unique.
"""

from __future__ import annotations

import pytest

from services.fingerprint_rules import mark_typed_text_carriers
from services.reconciliation import fingerprint_for_reconcile

_MAPPINGS = [
    {"source": "b", "target": "b", "target_type": "TEXT"},
    {"source": "B", "target": "B2", "target_type": "TEXT"},
]
_BASIS = {"b": "TEXT", "B2": "TEXT"}


@pytest.mark.parametrize(
    "catalog",
    [{"b": "text", "B": "boolean"}, {"B": "boolean", "b": "text"}],
    ids=["text-first", "boolean-first"],
)
def test_case_colliding_columns_match_by_exact_name(catalog):
    out = mark_typed_text_carriers(_BASIS, _MAPPINGS, catalog)
    assert out == {"b": "TEXT", "B2": "TEXT CARRYING SOURCE_BOOLEAN"}
    # The text column keeps exact text: 'true' is not a stored '1'.
    assert fingerprint_for_reconcile("true", ddl_type=out["b"]) != fingerprint_for_reconcile(
        "1", ddl_type=out["b"]
    )


def test_a_unique_case_insensitive_match_still_marks():
    mappings = [{"source": "flag", "target": "flag", "target_type": "TEXT"}]
    assert mark_typed_text_carriers({"flag": "TEXT"}, mappings, {"FLAG": "boolean"}) == {
        "flag": "TEXT CARRYING SOURCE_BOOLEAN"
    }


@pytest.mark.parametrize(
    "catalog",
    [{"Flag": "boolean", "FLAG": "text"}, {"FLAG": "text", "Flag": "boolean"}],
)
def test_an_ambiguous_case_insensitive_match_does_not_mark(catalog):
    mappings = [{"source": "flag", "target": "flag", "target_type": "TEXT"}]
    assert mark_typed_text_carriers({"flag": "TEXT"}, mappings, catalog) == {"flag": "TEXT"}
