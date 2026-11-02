"""Case A — a rounded decimal is an integer carrier, at Validate and at Execute.

The operator's recipe rounds ``22.6``, ``21.4``, ``22.0`` to whole numbers so
they fit an existing ``INT`` / ``int4`` destination. Two layers used to disagree:

* Validate inferred ``DECIMAL(p,s)`` from the transformed sample (or left the
  declared decimal in place) and blocked the run as a fidelity collapse;
* Execute applied the same recipe and would have written ``23, 21, 22``.

The fixture is chosen so rounding and truncation disagree: rounded sum is 66,
truncated sum is 65. A product that truncates silently fails this test even if
the row count matches.
"""

from __future__ import annotations

from decimal import Decimal

from services.shape_apply import apply_shape_type_ceilings, build_shape_runner, shaped_schema
from services.shape_models import ShapeRecipe
from services.shape_preflight import shaped_column_types, shaped_preflight_image
from services.preflight_service import run_file_preflight

# 22.6 + 21.4 + 22.0 = 66.0 as source.
# Round-half-up to 0 places: 23 + 21 + 22 = 66.
# Truncate:                 22 + 21 + 22 = 65.
CASE_A_ROWS = [
    {"id": 1, "arr_time": "22.6"},
    {"id": 2, "arr_time": "21.4"},
    {"id": 3, "arr_time": "22.0"},
]
CASE_A_RECIPE = {
    "steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]
}
DECLARED = {"id": "INTEGER", "arr_time": "DECIMAL(12,9)"}


def _recipe() -> ShapeRecipe:
    return ShapeRecipe.parse(CASE_A_RECIPE, source_columns=["id", "arr_time"])


def _findings(result: dict) -> str:
    parts = [str(b.get("message") or "") for b in result.get("blockers") or []]
    parts += [str(w) for w in result.get("warnings") or []]
    parts += [str(g.get("message") or "") for g in result.get("gates") or []]
    return " | ".join(parts).casefold()


def test_round_to_zero_places_reports_integer_not_decimal_p0() -> None:
    """The shared ceiling owner — not a pair-specific special case."""
    capped = apply_shape_type_ceilings(
        {"arr_time": "DECIMAL(12,9)", "other": "DECIMAL(9,2)"},
        _recipe(),
    )
    assert capped["arr_time"] == "INTEGER"
    assert capped["other"] == "DECIMAL(9,2)"


def test_wide_whole_numbers_stay_decimal_not_signed_bigint() -> None:
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "round_number", "column": "wide", "options": {"places": 0}}]},
        source_columns=["wide"],
    )
    capped = apply_shape_type_ceilings({"wide": "DECIMAL(38,10)"}, recipe)
    assert capped["wide"] == "DECIMAL(28,0)"


def test_validate_and_execute_report_the_same_integer_carrier() -> None:
    recipe = _recipe()
    image = shaped_preflight_image(
        CASE_A_RECIPE,
        columns=["id", "arr_time"],
        column_types=DECLARED,
        sample_rows=CASE_A_ROWS,
    )
    assert image.applied is True
    assert image.column_types["arr_time"] == "INTEGER"
    assert image.retyped_columns["arr_time"] == "INTEGER"
    assert [str(row["arr_time"]) for row in (image.sample_rows or [])] == [
        "23",
        "21",
        "22",
    ]
    assert sum(Decimal(str(row["arr_time"])) for row in (image.sample_rows or [])) == 66

    runner = build_shape_runner(recipe)
    shaped = runner.records(CASE_A_ROWS)
    execute_types = shaped_schema(runner, shaped, DECLARED)
    assert execute_types["arr_time"] == image.column_types["arr_time"] == "INTEGER"

    types, retyped = shaped_column_types(
        ["id", "arr_time"],
        declared_types=DECLARED,
        touched=recipe.touched_columns,
        rows=shaped,
        recipe=recipe,
    )
    assert types["arr_time"] == "INTEGER"
    assert retyped["arr_time"] == "INTEGER"


def test_validate_passes_an_existing_int_destination_after_round() -> None:
    """Case A at the gate — both dialect families the operator asked us to prove."""
    image = shaped_preflight_image(
        CASE_A_RECIPE,
        columns=["id", "arr_time"],
        column_types=DECLARED,
        sample_rows=CASE_A_ROWS,
    )
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0, "target_type": "INT"},
        {"source": "arr_time", "target": "arr_time", "confidence": 1.0, "target_type": "INT"},
    ]
    dest = {"id": "INT", "arr_time": "INT"}
    for dialect in ("postgresql", "mysql"):
        result = run_file_preflight(
            columns=image.columns,
            column_types=image.column_types,
            row_count=len(image.sample_rows or []),
            mappings=mappings,
            destination_connected=True,
            destination_column_types=dest,
            destination_db_type=dialect,
            estimated_bytes=4096,
            sample_rows=image.sample_rows,
        )
        text = _findings(result)
        assert "invalid integer" not in text, (dialect, text)
        assert "fidelity collapse" not in text, (dialect, text)
        assert not [
            b
            for b in result.get("blockers") or []
            if "integer" in str(b.get("message") or "").casefold()
            or "narrow" in str(b.get("message") or "").casefold()
        ], (dialect, result.get("blockers"))
        assert result["passed"] is True, (dialect, text)


def test_without_the_recipe_the_fractional_values_still_fail() -> None:
    """Honesty: the unshaped source is still a collapse into INT."""
    raw = run_file_preflight(
        columns=["id", "arr_time"],
        column_types=DECLARED,
        row_count=3,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0, "target_type": "INT"},
            {
                "source": "arr_time",
                "target": "arr_time",
                "confidence": 1.0,
                "target_type": "INT",
            },
        ],
        destination_connected=True,
        destination_column_types={"id": "INT", "arr_time": "INT"},
        destination_db_type="mysql",
        estimated_bytes=4096,
        sample_rows=CASE_A_ROWS,
    )
    assert raw["passed"] is False
