"""Review 4 of the exit rehearsal's value proof (C-1, H-2): unit level, no database needed.

* An array is compared element by element by exact token: a numeric element
  keeps its scale (``1.50`` is not ``1.5``) and a float keeps its sign
  (``-0.0`` is not ``0.0``), in JSON Lines and in a CSV cell alike.
* A value the driver folds is named from the server's own text (PostgreSQL).
* On the other engines, a type whose driver can read two stored values as one
  is named as not provable unless its adapter is one-to-one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.exit_completeness import judge_table, profile_rows

PARITY = {"check": "full_checksum", "verdict": "proven", "unexplained": 0}


def _judge(src_rows, ext_rows, types, *, engine="postgresql", server_text=None):
    cols = list(types)
    src = profile_rows(src_rows, cols, ["id"], None, types=types, engine=engine,
                       **({"server_text": server_text} if server_text is not None else {}))
    ext = profile_rows(ext_rows, cols, ["id"], ["id"], types=types, engine=engine)
    return judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=True, parity=PARITY,
                       not_measured=[])


@pytest.mark.parametrize("source, extract", [
    ([Decimal("1.50"), Decimal("2")], [1.5, 2]),
    ([-0.0, 0.1], [0.0, 0.1]),
    ([[Decimal("1.0")]], [[1]]),
])
def test_h2_an_array_element_that_lost_its_scale_or_sign_differs(source, extract):
    types = {"id": "BIGINT", "a": "ARRAY<DECIMAL>" if isinstance(source[0], (Decimal, list)) else
             "ARRAY<DOUBLE PRECISION>"}
    out = _judge([{"id": 1, "a": source}], [{"id": 1, "a": extract}], types)
    assert out["status"] == "incomplete" and "column(s) a" in out["reason"], out["reason"]


def test_h2_an_array_element_scale_is_compared_in_a_csv_cell_too():
    types = {"id": "BIGINT", "a": "ARRAY<DECIMAL>"}
    cols = list(types)
    src = profile_rows([{"id": 1, "a": [Decimal("1.50"), Decimal("2")]}], cols, ["id"], None, types=types,
                       engine="postgresql", extract_is_delimited=True)
    same = profile_rows([{"id": "1", "a": "[1.50,2]"}], cols, ["id"], ["id"], types=types, engine="postgresql",
                        documents_as_text=True)
    lost = profile_rows([{"id": "1", "a": "[1.5,2]"}], cols, ["id"], ["id"], types=types, engine="postgresql",
                        documents_as_text=True)
    assert src.column_digests["a"] == same.column_digests["a"]
    assert src.column_digests["a"] != lost.column_digests["a"]


def test_h2_an_honest_array_still_matches_its_extract():
    types = {"id": "BIGINT", "a": "ARRAY<DECIMAL>", "f": "ARRAY<DOUBLE PRECISION>", "s": "ARRAY<VARCHAR>",
             "d": "ARRAY<DATE>", "b": "ARRAY<BOOLEAN>"}
    from datetime import date

    src = [{"id": 1, "a": [Decimal("1.5"), Decimal("2")], "f": [-0.0, 0.1], "s": ["NULL", None, "a,b"],
            "d": [date(2024, 1, 1)], "b": [True, False]}]
    ext = [{"id": 1, "a": [1.5, 2], "f": [-0.0, 0.1], "s": ["NULL", None, "a,b"], "d": ["2024-01-01"],
            "b": [True, False]}]
    out = _judge(src, ext, types)
    assert out["status"] == "proven", out["reason"]


@pytest.mark.parametrize("ddl, server, driver, said", [
    ("ARRAY<INTEGER>", "[2:3]={1,2}", [1, 2], "a holds an array whose lower bound is not 1"),
    ("TIME", "24:00:00", __import__("datetime").time(0, 0), "a holds 24:00:00, which the driver reads as 00:00:00"),
    ("JSONB", "[1.0, 1.00]", [1.0, 1.0], "a holds a JSON number whose scale the driver drops"),
    ("INTERVAL", "1 mon", __import__("datetime").timedelta(days=30),
     "a holds an interval the driver reads as a plain duration"),
])
def test_c1_a_value_the_driver_folds_is_named_from_the_server_text(ddl, server, driver, said):
    types = {"id": "BIGINT", "a": ddl}
    out = _judge([{"id": 1, "a": driver, "__srv_a": server}], [{"id": 1, "a": driver}], types,
                 server_text={"a": "__srv_a"})
    assert out["status"] == "not_proven" and said in out["reason"], out["reason"]


@pytest.mark.parametrize("ddl, server, driver", [
    ("ARRAY<INTEGER>", "{{1,2},{3,4}}", [[1, 2], [3, 4]]),
    ("ARRAY<VARCHAR>", '{"NULL",NULL,"a,b","\\"q\\""}', ["NULL", None, "a,b", '"q"']),
    ("ARRAY<DOUBLE PRECISION>", "{0.1,-0,NaN}", [0.1, -0.0, float("nan")]),
    ("TIMETZ", "23:59:59.5-05:30", __import__("datetime").time.fromisoformat("23:59:59.5-05:30")),
    ("INTERVAL", "-1 days +02:00:00", __import__("datetime").timedelta(days=-1, seconds=7200)),
    ("JSONB", '{"a": 0.0000001, "b": [1.0, 2]}', {"a": 1e-07, "b": [1.0, 2]}),
])
def test_c1_a_value_the_driver_reads_one_to_one_is_not_named(ddl, server, driver):
    types = {"id": "BIGINT", "a": ddl}
    out = _judge([{"id": 1, "a": driver, "__srv_a": server}], [{"id": 1, "a": driver}], types,
                 server_text={"a": "__srv_a"})
    assert "a holds" not in out["reason"], out["reason"]


@pytest.mark.parametrize("engine, ddl", [
    ("sqlserver", "DATETIME2"), ("sqlserver", "DATETIME2(7)"), ("sqlserver", "TIME"),
    ("sqlserver", "DATETIMEOFFSET(7)"), ("sqlserver", "SQL_VARIANT"),
    ("oracle", "TIMESTAMP(9)"), ("oracle", "TIMESTAMP(6) WITH TIME ZONE"), ("oracle", "INTERVAL YEAR(2) TO MONTH"),
    ("oracle", "INTERVAL DAY(2) TO SECOND(6)"),
    ("mysql", "FLOAT"),
])
def test_c1_other_engines_a_type_the_driver_can_fold_is_named_not_provable(engine, ddl):
    types = {"id": "BIGINT", "c": ddl}
    src = profile_rows([{"id": 1, "c": "x"}], list(types), ["id"], None, types=types, engine=engine)
    assert "c" in src.uncompared and "not provable" in src.uncompared["c"], (engine, ddl, src.uncompared)


@pytest.mark.parametrize("engine, ddl", [
    ("sqlserver", "DATETIME2(6)"), ("sqlserver", "DATETIME"), ("sqlserver", "DECIMAL(12,2)"),
    ("oracle", "TIMESTAMP(6)"), ("oracle", "DATE"), ("oracle", "NUMBER"),
    ("mysql", "DOUBLE"), ("mysql", "DATETIME(6)"), ("mysql", "TIME"), ("mysql", "DECIMAL(10,2)"),
    ("sqlite", "REAL"), ("sqlite", "NUMERIC"), ("sqlite", "TEXT"),
    ("postgresql", "NUMERIC"), ("postgresql", "TIMESTAMPTZ"), ("postgresql", "DOUBLE PRECISION"),
])
def test_c1_a_type_the_driver_reads_one_to_one_is_not_named(engine, ddl):
    types = {"id": "BIGINT", "c": ddl}
    src = profile_rows([{"id": 1, "c": None}], list(types), ["id"], None, types=types, engine=engine)
    assert "c" not in src.uncompared, (engine, ddl, src.uncompared)
