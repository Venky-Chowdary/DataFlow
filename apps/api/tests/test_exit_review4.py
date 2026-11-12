"""Review 4 of the exit rehearsal's value proof (C-1): unit level, no database needed.

* A value the driver folds is named from the server's own text (PostgreSQL).
* On the other engines, a type whose driver can read two stored values as one
  is named as not provable unless its adapter is one-to-one.
"""

from __future__ import annotations


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
