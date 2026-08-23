"""A recipe must be runnable, accountable, and identical for Validate and Execute."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from services.shape_engine import ShapeEngine, ShapeRowError, shape_records
from services.shape_models import MAX_STEPS, ShapeError, ShapeRecipe


COLUMNS = ["id", "name", "amount", "code", "when"]

ROWS = [
    {"id": "1", "name": "  Ada  Lovelace ", "amount": "1,234.50", "code": "ab-1", "when": "03/04/2026"},
    {"id": "2", "name": "grace hopper", "amount": "(9.99)", "code": "cd-2", "when": "01/12/2025"},
    {"id": "3", "name": "", "amount": "", "code": "ef-3", "when": ""},
]


def recipe(*steps, columns=COLUMNS):
    return ShapeRecipe.parse({"steps": list(steps)}, source_columns=columns)


# --- classification and refusals -------------------------------------------


def test_a_global_operation_is_refused_with_where_it_can_be_done():
    """A join cannot be evaluated row-locally; saying so is the product teaching."""
    with pytest.raises(ShapeError, match="post-load model on the Transforms page"):
        recipe({"op": "join", "column": "id"})
    with pytest.raises(ShapeError, match="post-load model"):
        recipe({"op": "aggregate", "column": "amount"})
    with pytest.raises(ShapeError, match="post-load model"):
        recipe({"op": "sort", "column": "amount"})


def test_an_unknown_operation_lists_the_ones_that_exist():
    with pytest.raises(ShapeError, match="is not a shaping operation"):
        recipe({"op": "sprinkle", "column": "id"})


def test_a_step_reading_a_column_an_earlier_step_dropped_is_refused_at_design_time():
    with pytest.raises(ShapeError, match="not available at this point in the recipe"):
        recipe(
            {"op": "drop_column", "column": "code"},
            {"op": "trim", "column": "code"},
        )


def test_a_renamed_column_is_addressable_by_its_new_name_and_not_the_old_one():
    parsed = recipe(
        {"op": "rename_column", "column": "code", "options": {"to": "sku"}},
        {"op": "trim", "column": "sku"},
    )
    assert parsed.output_columns == ("id", "name", "amount", "sku", "when")
    with pytest.raises(ShapeError, match="not available"):
        recipe(
            {"op": "rename_column", "column": "code", "options": {"to": "sku"}},
            {"op": "trim", "column": "code"},
        )


def test_an_unknown_option_is_refused_rather_than_silently_ignored():
    with pytest.raises(ShapeError, match="has no option 'mode'"):
        recipe({"op": "trim", "column": "name", "options": {"mode": "upper"}})


def test_a_missing_required_option_names_it():
    with pytest.raises(ShapeError, match="case needs 'mode'"):
        recipe({"op": "case", "column": "name"})


def test_a_recipe_longer_than_the_ceiling_is_refused():
    steps = [{"op": "trim", "column": "name"}] * (MAX_STEPS + 1)
    with pytest.raises(ShapeError, match=f"{MAX_STEPS} steps"):
        ShapeRecipe.parse({"steps": steps}, source_columns=COLUMNS)


def test_active_steps_are_declared_so_the_ledger_knows_to_move():
    assert recipe({"op": "trim", "column": "name"}).has_active_step is False
    assert recipe(
        {"op": "filter_rows", "options": {"condition": "[id] <> '3'"}}
    ).has_active_step is True


# --- identity ---------------------------------------------------------------


def test_the_recipe_hash_ignores_formatting_but_not_meaning():
    a = recipe({"op": "derive_column", "options": {"to": "up", "expression": "upper( [name] )"}})
    b = recipe({"op": "derive_column", "options": {"to": "up", "expression": "upper([name])"}})
    c = recipe({"op": "derive_column", "options": {"to": "up", "expression": "lower([name])"}})
    assert a.recipe_hash == b.recipe_hash
    assert a.recipe_hash != c.recipe_hash


def test_a_disabled_step_is_not_part_of_the_identity_but_enabling_it_is():
    off = recipe({"op": "trim", "column": "name", "enabled": False})
    on = recipe({"op": "trim", "column": "name"})
    assert off.recipe_hash == ShapeRecipe.parse({}, source_columns=COLUMNS).recipe_hash
    assert on.recipe_hash != off.recipe_hash


def test_step_order_is_part_of_the_identity():
    """trim-then-pad and pad-then-trim are different programs."""
    a = recipe(
        {"op": "trim", "column": "name"},
        {"op": "pad", "column": "name", "options": {"width": 20}},
    )
    b = recipe(
        {"op": "pad", "column": "name", "options": {"width": 20}},
        {"op": "trim", "column": "name"},
    )
    assert a.recipe_hash != b.recipe_hash


# --- value behaviour --------------------------------------------------------


def test_cleansing_steps_produce_the_values_an_operator_expects():
    shaped, effect = shape_records(
        recipe(
            {"op": "trim", "column": "name"},
            {"op": "collapse_whitespace", "column": "name"},
            {"op": "case", "column": "name", "options": {"mode": "title"}},
            {"op": "parse_number", "column": "amount"},
            {"op": "parse_date", "column": "when", "options": {"format": "%d/%m/%Y"}},
        ),
        ROWS,
    )
    assert [r["name"] for r in shaped] == ["Ada Lovelace", "Grace Hopper", ""]
    assert shaped[0]["amount"] == Decimal("1234.50")
    assert shaped[1]["amount"] == Decimal("-9.99")
    assert shaped[2]["amount"] is None
    # Naive on purpose: a parsed source date declares no zone.
    assert shaped[0]["when"] == datetime(2026, 4, 3)  # noqa: DTZ001
    assert effect.rows_in == effect.rows_out == 3


def test_round_is_the_honest_fix_for_a_narrowing_decimal_column():
    """The Snowflake NUMBER(11,8) failure, decided at design time by one step."""
    rows = [{"arr_time": "1.234567891"}, {"arr_time": "2.5"}]
    shaped, effect = shape_records(
        ShapeRecipe.parse(
            {"steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 8}}]},
            source_columns=["arr_time"],
        ),
        rows,
    )
    assert [str(r["arr_time"]) for r in shaped] == ["1.23456789", "2.50000000"]
    assert effect.cells_changed == 2


def test_a_cast_declares_the_type_instead_of_letting_a_sample_infer_it():
    shaped, _ = shape_records(
        ShapeRecipe.parse(
            {
                "steps": [
                    {"op": "cast_column", "column": "n", "options": {"to_type": "integer"}},
                    {"op": "cast_column", "column": "d", "options": {"to_type": "date", "format": "%d/%m/%Y"}},
                    {"op": "cast_column", "column": "b", "options": {"to_type": "boolean"}},
                ]
            },
            source_columns=["n", "d", "b"],
        ),
        [{"n": "42", "d": "03/04/2026", "b": "Y"}],
    )
    assert shaped[0] == {"n": 42, "d": date(2026, 4, 3), "b": True}


def test_a_cast_that_would_lose_information_refuses_the_row():
    with pytest.raises(ShapeRowError, match="not a whole number"):
        shape_records(
            ShapeRecipe.parse(
                {"steps": [{"op": "cast_column", "column": "n", "options": {"to_type": "integer"}}]},
                source_columns=["n"],
            ),
            [{"n": "4.5"}],
        )


def test_structural_steps_keep_column_order_predictable():
    shaped, _ = shape_records(
        recipe(
            {"op": "rename_column", "column": "code", "options": {"to": "sku"}},
            {"op": "drop_column", "column": "when"},
            {"op": "constant_column", "options": {"to": "batch", "value": "B7"}},
            {"op": "keep_columns", "options": {"columns": ["id", "sku", "batch"]}},
        ),
        ROWS,
    )
    assert list(shaped[0]) == ["id", "sku", "batch"]
    assert shaped[0]["sku"] == "ab-1"
    assert shaped[0]["batch"] == "B7"


def test_split_and_concat_move_columns_without_touching_the_source():
    source = [{"full": "Ada Lovelace"}]
    shaped, _ = shape_records(
        ShapeRecipe.parse(
            {
                "steps": [
                    {"op": "split_column", "column": "full", "options": {"separator": " ", "into": ["first", "last"]}},
                    {"op": "concat_columns", "options": {"to": "sortable", "columns": ["last", "first"], "separator": ", "}},
                ]
            },
            source_columns=["full"],
        ),
        source,
    )
    assert shaped[0]["first"] == "Ada"
    assert shaped[0]["sortable"] == "Lovelace, Ada"
    assert source == [{"full": "Ada Lovelace"}], "the source rows must not be mutated"


def test_a_derive_step_can_read_columns_an_earlier_step_created():
    shaped, _ = shape_records(
        recipe(
            {"op": "constant_column", "options": {"to": "rate", "value": "2"}},
            {"op": "parse_number", "column": "amount"},
            {"op": "derive_column", "options": {"to": "doubled", "expression": "coalesce([amount], 0) * to_number([rate])"}},
        ),
        ROWS,
    )
    assert shaped[0]["doubled"] == Decimal("2469.00")
    assert shaped[2]["doubled"] == Decimal("0")


# --- accounting -------------------------------------------------------------


def test_a_filtered_row_moves_the_shaped_out_term_and_is_not_a_quarantine_finding():
    shaped, effect = shape_records(
        recipe({"op": "filter_rows", "options": {"condition": "[id] <> '3'"}}),
        ROWS,
    )
    assert len(shaped) == 2
    assert effect.rows_shaped_out == 1
    assert effect.rows_diverted == 0
    assert effect.balanced, "rows_in must equal rows_out + shaped_out + diverted"


def test_a_diverted_row_is_kept_with_its_reason_rather_than_dropped_silently():
    shaped, effect = shape_records(
        recipe(
            {
                "op": "divert_rows",
                "options": {"condition": "is_null([name])", "reason": "missing customer name"},
            }
        ),
        ROWS,
    )
    assert len(shaped) == 2
    assert effect.rows_diverted == 1
    assert effect.rows_shaped_out == 0
    sample = effect.diverted_samples[0].to_dict()
    assert sample["reason"] == "missing customer name"
    assert sample["record"]["id"] == "3"
    assert effect.balanced


def test_cells_changed_counts_only_cells_that_actually_changed():
    shaped, effect = shape_records(
        ShapeRecipe.parse(
            {"steps": [{"op": "trim", "column": "a"}]},
            source_columns=["a"],
        ),
        [{"a": "clean"}, {"a": " dirty "}],
    )
    assert [r["a"] for r in shaped] == ["clean", "dirty"]
    assert effect.cells_changed == 1
    assert effect.steps[0].cells_changed == 1


def test_a_step_that_empties_a_cell_is_counted_as_introducing_a_null():
    _, effect = shape_records(
        ShapeRecipe.parse(
            {"steps": [{"op": "null_if", "column": "a", "options": {"values": ["N/A"]}}]},
            source_columns=["a"],
        ),
        [{"a": "N/A"}, {"a": "keep"}],
    )
    assert effect.nulls_introduced == 1


def test_per_step_counts_are_reported_so_the_ui_can_show_each_step_s_effect():
    _, effect = shape_records(
        recipe(
            {"op": "trim", "column": "name"},
            {"op": "filter_rows", "options": {"condition": "[id] <> '3'"}},
            {"op": "case", "column": "name", "options": {"mode": "upper"}},
        ),
        ROWS,
    )
    trim, filtered, upper = effect.steps
    assert trim.rows_in == 3 and trim.rows_out == 3
    assert filtered.rows_in == 3 and filtered.rows_out == 2 and filtered.rows_removed == 1
    assert upper.rows_in == 2, "a removed row must not reach later steps"


# --- error policy -----------------------------------------------------------


def test_the_default_error_policy_refuses_the_run_and_names_the_row():
    with pytest.raises(ShapeRowError) as raised:
        shape_records(
            ShapeRecipe.parse(
                {"steps": [{"op": "parse_number", "column": "amount"}]},
                source_columns=["amount"],
            ),
            [{"amount": "1"}, {"amount": "twelve"}],
        )
    assert raised.value.as_dict()["row"] == 2
    assert raised.value.as_dict()["column"] == "amount"


def test_divert_policy_quarantines_the_unparseable_row_instead_of_the_run():
    shaped, effect = shape_records(
        ShapeRecipe.parse(
            {"steps": [{"op": "parse_number", "column": "amount", "on_error": "divert"}]},
            source_columns=["amount"],
        ),
        [{"amount": "1"}, {"amount": "twelve"}],
    )
    assert len(shaped) == 1
    assert effect.rows_diverted == 1
    assert "could not be applied" in effect.diverted_samples[0].reason


def test_null_policy_is_opt_in_and_its_losses_are_counted():
    shaped, effect = shape_records(
        ShapeRecipe.parse(
            {"steps": [{"op": "parse_number", "column": "amount", "on_error": "null"}]},
            source_columns=["amount"],
        ),
        [{"amount": "twelve"}],
    )
    assert shaped[0]["amount"] is None
    assert effect.steps[0].errors == 1
    assert effect.nulls_introduced == 1


def test_an_invalid_error_policy_is_refused():
    with pytest.raises(ShapeError, match="on_error must be one of"):
        recipe({"op": "trim", "column": "name", "on_error": "ignore"})


# --- streaming properties ---------------------------------------------------


def test_chunking_the_same_population_yields_the_same_rows_and_the_same_counts():
    """Streaming safety: chunk boundaries must not be observable in the output."""
    plan = recipe(
        {"op": "trim", "column": "name"},
        {"op": "filter_rows", "options": {"condition": "[id] <> '3'"}},
        {"op": "derive_column", "options": {"to": "len", "expression": "length([name])"}},
    )
    whole, whole_effect = shape_records(plan, ROWS)

    engine = ShapeEngine(plan)
    chunked = engine.apply_batch(ROWS[:1]) + engine.apply_batch(ROWS[1:])

    assert chunked == whole
    assert engine.effect.to_dict()["rows_out"] == whole_effect.rows_out
    assert engine.effect.rows_shaped_out == whole_effect.rows_shaped_out


def test_an_empty_recipe_is_a_pass_through_that_still_balances():
    shaped, effect = shape_records(ShapeRecipe.parse(None, source_columns=COLUMNS), ROWS)
    assert shaped == ROWS
    assert effect.rows_in == effect.rows_out == 3
    assert effect.cells_changed == 0
    assert effect.balanced


def test_row_numbers_continue_across_batches_so_a_failure_names_the_real_row():
    plan = ShapeRecipe.parse(
        {"steps": [{"op": "parse_number", "column": "amount"}]},
        source_columns=["amount"],
    )
    engine = ShapeEngine(plan)
    engine.apply_batch([{"amount": "1"}, {"amount": "2"}])
    with pytest.raises(ShapeRowError) as raised:
        engine.apply_batch([{"amount": "nope"}])
    assert raised.value.as_dict()["row"] == 3
