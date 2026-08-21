"""The expression language must be exact, closed and honest about failure."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from services.shape_expr import (
    EvalError,
    ExpressionError,
    compile_expression,
)


def value(source: str, row: dict | None = None):
    return compile_expression(source).evaluate(row or {})


def test_arithmetic_is_decimal_so_money_survives_the_trip():
    """A float engine turns 0.1 + 0.2 into 0.30000000000000004 and loses a cent."""
    assert value("0.1 + 0.2") == Decimal("0.3")
    assert str(value("[amount] * 3", {"amount": "1.005"})) == "3.015"


def test_a_column_name_with_spaces_is_addressable():
    assert value("[Order Total] + 1", {"Order Total": "9"}) == Decimal("10")


def test_text_and_number_addition_is_refused_rather_than_guessed():
    """`'1' + '2'` meaning '12' in one language and 3 in another is a trap."""
    with pytest.raises(EvalError, match="concat"):
        value("[code] + 'x'", {"code": "AB"})
    assert value("concat([code], 'x')", {"code": "AB"}) == "ABx"


def test_comparison_against_null_is_unknown_and_a_predicate_does_not_match():
    assert value("[x] > 1", {"x": None}) is None
    assert compile_expression("[x] > 1").matches({"x": None}) is False


def test_and_or_short_circuit_so_a_guard_clause_can_be_written():
    expression = compile_expression("is_not_null([x]) and to_number([x]) > 1")
    assert expression.matches({"x": None}) is False
    assert expression.matches({"x": "5"}) is True


def test_if_does_not_evaluate_the_branch_it_did_not_choose():
    assert value("if([x] = 0, 0, 100 / [x])", {"x": 0}) == Decimal(0)


def test_round_is_half_up_like_a_spreadsheet_not_bankers():
    assert value("round(2.5)") == Decimal("3")
    assert value("round(1.005, 2)") == Decimal("1.01")
    assert value("round([v], 8)", {"v": "1.234567891"}) == Decimal("1.23456789")


def test_to_number_reads_what_a_human_typed():
    assert value("to_number('(1,234.50)')") == Decimal("-1234.50")
    assert value("to_number('$1,000')") == Decimal("1000")
    assert value("to_number('')") is None


def test_to_date_never_guesses_day_month_order():
    with pytest.raises(EvalError, match="explicit format"):
        value("to_date('03/04/2026')")
    # Naive on purpose: a source date declares no zone (see _comparable_moment).
    assert value("to_date('03/04/2026', '%d/%m/%Y')") == datetime(2026, 4, 3)  # noqa: DTZ001
    assert value("to_date('2026-04-03')") == datetime(2026, 4, 3)  # noqa: DTZ001


def test_blank_and_null_are_the_same_emptiness():
    assert value("is_null([x])", {"x": "   "}) is True
    assert value("coalesce([a], [b], 'z')", {"a": "", "b": None}) == "z"


def test_split_part_and_substr_are_one_based_and_refuse_zero():
    assert value("split_part('a-b-c', '-', 2)") == "b"
    assert value("substr('abcdef', 2, 3)") == "bcd"
    with pytest.raises(EvalError, match="1-based"):
        value("substr('abc', 0)")


def test_an_unknown_function_is_refused_with_the_catalog():
    with pytest.raises(ExpressionError, match="unknown function 'now'"):
        compile_expression("now()")


def test_there_is_no_clock_and_no_randomness_in_the_catalog():
    """Determinism is a load-bearing property: Validate and Execute must agree."""
    from services.shape_expr import FUNCTIONS

    forbidden = {"now", "today", "current_date", "random", "rand", "uuid", "sysdate"}
    assert forbidden & set(FUNCTIONS) == set()


def test_wrong_arity_is_a_design_time_refusal():
    with pytest.raises(ExpressionError, match=r"substr\(\) takes 2-3"):
        compile_expression("substr('a')")


def test_a_misspelled_column_names_the_real_one():
    with pytest.raises(ExpressionError, match="the source spells it 'Amount'"):
        compile_expression("[amount] + 1", known_columns=["Amount"])
    with pytest.raises(ExpressionError, match="which the source does not have"):
        compile_expression("[nope] + 1", known_columns=["Amount"])


def test_an_unbounded_pattern_is_refused_rather_than_backtracked():
    with pytest.raises(EvalError, match="the limit is 200"):
        value(f"regex_matches([x], '{'a' * 201}')", {"x": "a"})


def test_division_by_zero_is_an_error_not_an_infinity():
    with pytest.raises(EvalError, match="division by zero"):
        value("1 / 0")


def test_precedence_and_parentheses_behave_like_sql():
    assert value("2 + 3 * 4") == Decimal("14")
    assert value("(2 + 3) * 4") == Decimal("20")
    assert compile_expression("not [a] = 1 or [a] = 2").matches({"a": 2}) is True


def test_columns_are_reported_for_design_time_checking():
    expression = compile_expression("concat([first], ' ', upper([last]))")
    assert expression.columns == frozenset({"first", "last"})


def test_the_canonical_form_ignores_spelling_so_a_reformat_is_not_a_new_recipe():
    a = compile_expression("upper( [x] )")
    b = compile_expression("upper([x])")
    assert a.canonical() == b.canonical()


def test_evaluating_the_same_row_twice_returns_the_same_value():
    expression = compile_expression("concat(upper([a]), '-', round([b], 2))")
    row = {"a": "x", "b": "1.239"}
    assert expression.evaluate(row) == expression.evaluate(row) == "X-1.24"
