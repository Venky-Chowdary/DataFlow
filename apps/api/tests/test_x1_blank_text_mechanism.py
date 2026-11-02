"""X-1 unit gate: typed database sources keep '' and '   ' as stored values.

The live read-back cells live in test_x1_blank_text_fidelity.py (PG-gated).
This file proves the mechanism without a live warehouse: the ShapeRunner
``typed_values`` path routes blank detection through NULL wire sentinels
only, while untyped (file/spreadsheet) sources keep the legacy collapse.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.shape_apply import ShapeRunner, is_typed_value_source  # noqa: E402
from services.shape_expr import _BLANK_AS_NULL, is_blank  # noqa: E402
from services.shape_models import ShapeRecipe  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def _runner(step: dict, typed: bool) -> ShapeRunner:
    recipe = ShapeRecipe.parse({"steps": [step]}, source_columns=["v"])
    return ShapeRunner(recipe, typed_values=typed)


def test_typed_source_preserves_blank_text_through_trim():
    out = _runner({"op": "trim", "column": "v"}, typed=True).records(
        [{"v": "   "}, {"v": ""}, {"v": None}, {"v": " x "}]
    )
    assert out[0]["v"] == ""   # '   ' trims to a real ''
    assert out[1]["v"] == ""   # '' stays a stored value
    assert out[2]["v"] is None  # NULL stays NULL
    assert out[3]["v"] == "x"


def test_untyped_source_keeps_legacy_collapse():
    """File/spreadsheet semantics unchanged: blank text is null-ish."""
    out = _runner({"op": "trim", "column": "v"}, typed=False).records(
        [{"v": "   "}, {"v": ""}, {"v": None}, {"v": " x "}]
    )
    assert out[0]["v"] is None
    assert out[1]["v"] is None
    assert out[2]["v"] is None
    assert out[3]["v"] == "x"


def test_is_blank_typed_mode_only_null_sentinels():
    token = _BLANK_AS_NULL.set(False)
    try:
        assert is_blank("") is False
        assert is_blank("   ") is False
        assert is_blank(SQL_NULL_SENTINEL) is True
        assert is_blank(None) is True
    finally:
        _BLANK_AS_NULL.reset(token)


def test_is_blank_default_mode_collapses_blanks():
    assert is_blank("") is True
    assert is_blank("   ") is True
    assert is_blank(None) is True


def test_typed_value_source_table():
    for fmt in ("postgresql", "mysql", "sqlite", "sqlserver", "mongodb", "snowflake"):
        assert is_typed_value_source(fmt), fmt
    for fmt in ("csv", "json", "excel", "parquet", "rest_api", ""):
        assert not is_typed_value_source(fmt), fmt


def test_contextvar_isolation_between_calls():
    """A typed run must not leak its blank policy into the next file run."""
    recipe = ShapeRecipe.parse({"steps": [{"op": "trim", "column": "v"}]},
                               source_columns=["v"])
    typed = ShapeRunner(recipe, typed_values=True)
    untyped = ShapeRunner(recipe)
    assert typed.records([{"v": "   "}])[0]["v"] == ""
    assert untyped.records([{"v": "   "}])[0]["v"] is None
    assert typed.records([{"v": "   "}])[0]["v"] == ""


def test_engine_shape_runner_is_typed_for_db_source():
    """X-1 live: _open_shape_runner ran the write-side recipe untyped —
    database '' / '   ' collapsed to NULL on the materialized path while
    the typed proof expected '' — the 'did not apply as declared' culprit
    rows 1,2 on every shaped DB run."""
    from src.transfer.engine_shape import _open_shape_runner

    class _Src:
        kind = "database"
        format = "postgresql"

    class _Req:
        source = _Src()
        shape_recipe = {"steps": [{"op": "trim", "column": "v"}]}
        approved_shape_recipe_hash = ""
        approved_set_hash = ""
        workspace_id = ""
        column_types = {}

    runner = _open_shape_runner(_Req(), ["v"], {})
    assert runner is not None and runner.typed_values is True


def test_engine_shape_runner_stays_untyped_for_file_source():
    from src.transfer.engine_shape import _open_shape_runner

    class _Src:
        kind = "file"
        format = "csv"

    class _Req:
        source = _Src()
        shape_recipe = {"steps": [{"op": "trim", "column": "v"}]}
        approved_shape_recipe_hash = ""
        approved_set_hash = ""
        workspace_id = ""
        column_types = {}

    runner = _open_shape_runner(_Req(), ["v"], {})
    assert runner is not None and runner.typed_values is False
