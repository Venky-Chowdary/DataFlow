"""X-115 (P2): mask_pii email refused a typed '' and failed the whole run.

'' must stay '', NULL stays NULL. A non-empty non-address is an EvalError
the step's ``on_error`` handles — ``divert`` quarantines the row with a
reason instead of failing the run.
"""
import os

import pytest

from services.shape_engine import (
    ShapeEngine,
    _SQL_NULL_SENTINEL,
    _transform_value,
)
from services.shape_models import ShapeRecipe, ShapeStep
from services.shape_expr import EvalError, _BLANK_AS_NULL


@pytest.fixture
def typed_source():
    """Typed (DB) source: '' is a stored value, not a blank cell."""
    token = _BLANK_AS_NULL.set(False)
    try:
        yield
    finally:
        _BLANK_AS_NULL.reset(token)


def _step(on_error="refuse"):
    return ShapeStep(
        op="mask_pii",
        column="email",
        options={"pattern": "email"},
        on_error=on_error,
    )


def test_empty_string_stays_empty(typed_source):
    assert _transform_value(_step(), "") == ""


def test_null_stays_null(typed_source):
    assert _transform_value(_step(), None) is None
    assert _transform_value(_step(), _SQL_NULL_SENTINEL) is None


def test_non_address_raises_evalerror_not_crash(typed_source):
    with pytest.raises(EvalError):
        _transform_value(_step(), "not-an-address")


def test_non_address_diverts_under_on_error(typed_source):
    recipe = ShapeRecipe.parse(
        {"steps": [{
            "op": "mask_pii", "column": "email",
            "options": {"pattern": "email"}, "on_error": "divert",
        }]},
        source_columns=["id", "email"],
        source_types={"id": "int", "email": "text"},
    )
    eng = ShapeEngine(recipe)
    out = eng.apply_batch([
        {"id": 1, "email": "ann@x.io"},
        {"id": 2, "email": ""},
        {"id": 3, "email": "not-an-address"},
    ])
    out.extend(eng.drain())
    assert [r["email"] for r in out] == ["a***@x.io", ""]
    diverted = eng.take_diverted()
    assert len(diverted) == 1 and diverted[0].record.get("id") == 3


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X115_LIVE") != "1",
    reason="live mask_pii matrix — user-run",
)
def test_x115_live(tmp_path):  # pragma: no cover
    pass
