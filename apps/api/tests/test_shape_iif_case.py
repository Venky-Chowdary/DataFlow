"""Audit P1-3: expression parity — iif() alias + CASE WHEN ... END syntax."""
import pytest

from services.shape_expr import compile_expression, ExpressionError


def test_iif_is_if_alias():
    row = {"amt": "10"}
    assert compile_expression("iif([amt] > 5, 'big', 'small')").evaluate(row) == "big"
    assert compile_expression("iif([amt] > 50, 'big', 'small')").evaluate(row) == "small"
    # 2-arg form: no else -> None
    assert compile_expression("iif([amt] > 50, 'big')").evaluate(row) is None


def test_case_when_then_else_end():
    row = {"amt": "10"}
    expr = "case when [amt] > 50 then 'large' when [amt] > 5 then 'mid' else 'small' end"
    assert compile_expression(expr).evaluate(row) == "mid"
    assert compile_expression(expr).evaluate({"amt": "90"}) == "large"
    assert compile_expression(expr).evaluate({"amt": "1"}) == "small"


def test_case_without_else_is_null():
    assert compile_expression("case when [x] = 1 then 'a' end").evaluate({"x": 2}) is None


def test_case_is_lazy_like_if():
    # An untaken branch must not evaluate (would divide by blank).
    expr = "case when is_null([x]) then 'guarded' else 1 / [x] end"
    assert compile_expression(expr).evaluate({"x": ""}) == "guarded"


@pytest.mark.parametrize("bad", [
    "case end",                      # no WHEN
    "case [x] end",                  # WHEN missing
    "case when [x] then 'a'",        # END missing
    "case when [x] then 'a' else",   # ELSE value missing
    "iif([x] > 1)",                  # iif needs 2-3 args
])
def test_malformed_case_and_iif_refuse(bad):
    with pytest.raises((ExpressionError, Exception)):
        compile_expression(bad)


def test_iif_in_function_catalog():
    from services.shape_expr import describe_functions
    names = {f["name"] for f in describe_functions()}
    assert "iif" in names and "if" in names
