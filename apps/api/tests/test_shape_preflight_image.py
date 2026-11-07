"""Validate judges the transformed image, because that is what the write carries."""

import pytest

from services.shape_preflight import (
    ShapePreflightRefused,
    shaped_preflight_image,
)


def test_no_recipe_passes_the_declared_image_through_untouched() -> None:
    image = shaped_preflight_image(
        None,
        columns=["id", "name"],
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        sample_rows=[{"id": 1, "name": "  a "}],
    )
    assert image.applied is False
    assert image.recipe_hash == ""
    assert image.columns == ["id", "name"]
    assert image.sample_rows == [{"id": 1, "name": "  a "}]


def test_a_recipe_of_only_disabled_steps_has_no_image_of_its_own() -> None:
    image = shaped_preflight_image(
        {"steps": [{"op": "trim", "column": "name", "enabled": False}]},
        columns=["name"],
        column_types={"name": "VARCHAR"},
        sample_rows=[{"name": " a "}],
    )
    assert image.applied is False
    assert image.sample_rows == [{"name": " a "}]


def test_strip_characters_removes_the_control_character_before_the_gates_see_it() -> None:
    """The defect this exists for: a gate blocked on a value the transform deletes."""
    image = shaped_preflight_image(
        {
            "steps": [
                {
                    "op": "strip_characters",
                    "column": "note",
                    "options": {"characters": "non_printable"},
                }
            ]
        },
        columns=["note"],
        column_types={"note": "VARCHAR"},
        sample_rows=[{"note": "ok\u0001"}, {"note": "fine"}],
    )
    assert image.applied is True
    assert image.recipe_hash
    assert image.sample_rows is not None
    assert all("\u0001" not in str(row["note"]) for row in image.sample_rows)
    assert image.rows_in == 2
    assert image.rows_out == 2
    assert image.rows_removed == 0


def test_a_removed_row_is_counted_and_not_offered_to_the_gates() -> None:
    image = shaped_preflight_image(
        {
            "steps": [
                {
                    "op": "filter_rows",
                    "options": {"condition": "[status] <> 'void'"},
                }
            ]
        },
        columns=["status"],
        column_types={"status": "VARCHAR"},
        sample_rows=[{"status": "void"}, {"status": "paid"}],
    )
    assert image.rows_removed == 1
    assert image.sample_rows == [{"status": "paid"}]
    assert "1 sampled row(s) removed by transform" in image.note()


def test_a_renamed_column_is_the_column_the_gates_score() -> None:
    image = shaped_preflight_image(
        {
            "steps": [
                {"op": "rename_column", "column": "amt", "options": {"to": "amount"}}
            ]
        },
        columns=["amt"],
        column_types={"amt": "VARCHAR"},
        sample_rows=[{"amt": "10"}],
    )
    assert image.columns == ["amount"]
    assert image.sample_rows == [{"amount": "10"}]


def test_an_untouched_column_keeps_its_declared_carrier() -> None:
    """Catalog truth beats inference: only columns the recipe wrote are re-read."""
    image = shaped_preflight_image(
        {"steps": [{"op": "trim", "column": "name"}]},
        columns=["id", "name"],
        column_types={"id": "BIGINT", "name": "VARCHAR(40)"},
        sample_rows=[{"id": 7, "name": " a "}],
    )
    assert image.column_types["id"] == "BIGINT"
    assert "id" not in image.retyped_columns


def test_a_recipe_the_source_cannot_run_refuses_validate_rather_than_scoring_it() -> None:
    with pytest.raises(ShapePreflightRefused):
        shaped_preflight_image(
            {"steps": [{"op": "trim", "column": "missing_column"}]},
            columns=["name"],
            column_types={"name": "VARCHAR"},
            sample_rows=[{"name": "a"}],
        )


def test_a_refused_row_refuses_validate_instead_of_promising_a_run() -> None:
    with pytest.raises(ShapePreflightRefused) as err:
        shaped_preflight_image(
            {
                "steps": [
                    {
                        "op": "cast_type",
                        "column": "amount",
                        "options": {"to": "integer"},
                        "on_error": "refuse",
                    }
                ]
            },
            columns=["amount"],
            column_types={"amount": "VARCHAR"},
            sample_rows=[{"amount": "not-a-number"}],
        )
    assert "transform" in str(err.value)
