"""X-3: the runtime transform-proof oracle must cover the ops the test-level
oracles already prove — a production verdict on a trim/case/cast recipe was
reporting ``oracle_uncovered_steps ["step 3 (cast_column)"]``.

These regressions pin the emitted SQL per dialect for every newly wired op.
Runtime coverage is a separate counter from the test-level oracle coverage:
it counts ops whose faithful SQL fragment ``_recipe_step_sql`` can emit for
every oracle dialect family (PG, MySQL, T-SQL).
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.reconcile_step as _rs  # noqa: E402

_recipe_step_sql = getattr(_rs, "_recipe_step_sql", None)


def _call(op, opts, col, src_type):
    assert _recipe_step_sql is not None, "_recipe_step_sql missing (pre-fix)"
    return _recipe_step_sql(op, opts, col, {}, src_type=src_type)


def test_cast_integer_pg_rounds_like_engine():
    # Engine: to_number -> to_integral_value(ROUND_HALF_UP), refusing
    # non-whole input. PG numeric -> bigint casts half-away-from-zero.
    sql = _call("cast_column", {"to_type": "integer"}, '"qty"', "postgresql")
    assert sql == 'CAST(CAST("qty" AS numeric) AS bigint)'


def test_cast_integer_mysql_uses_signed_cast():
    sql = _call("cast_column", {"to_type": "integer"}, "`qty`", "mysql")
    assert sql == "CAST(`qty` AS SIGNED)"


def test_cast_integer_tsql_try_casts_then_rounds():
    sql = _call("cast_column", {"to_type": "bigint"}, "[qty]", "sqlserver")
    assert sql == "CAST(ROUND(TRY_CAST([qty] AS decimal(38,10)),0) AS bigint)"


def test_cast_decimal_uses_declared_precision():
    sql = _call(
        "cast_column",
        {"to_type": "decimal", "precision": 38, "scale": 10},
        '"amt"',
        "postgresql",
    )
    assert sql == 'CAST("amt" AS DECIMAL(38,10))'
    sql = _call(
        "cast_column",
        {"to_type": "decimal", "precision": 38, "scale": 10},
        "[amt]",
        "mssql",
    )
    assert sql == "TRY_CAST([amt] AS decimal(38,10))"


def test_cast_decimal_without_precision_stays_uncovered():
    assert _call("cast_column", {"to_type": "decimal"}, '"amt"', "postgresql") is None
    assert _call(
        "cast_column", {"to_type": "decimal", "precision": "abc"}, '"amt"', "postgresql"
    ) is None


def test_loop_recipe_now_fully_covered():
    """The exact reported recipe — trim + case + cast integer — must emit a
    faithful fragment for every step on every oracle dialect family."""
    steps = [
        ("trim", {}, "note"),
        ("case", {"mode": "upper"}, "note"),
        ("cast_column", {"to_type": "integer"}, "qty"),
    ]
    quote = {"postgresql": '"%s"', "mysql": "`%s`", "sqlserver": "[%s]"}
    for dialect, q in quote.items():
        for i, (op, opts, col) in enumerate(steps):
            sql = _call(op, opts, q % col, dialect)
            assert sql is not None, (
                f"step {i + 1} ({op}) uncovered on {dialect} — "
                "would report oracle_uncovered_steps"
            )
