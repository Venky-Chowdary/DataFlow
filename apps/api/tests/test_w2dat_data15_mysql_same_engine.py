"""DATA-15: a MySQL -> MySQL same-engine copy is not refused.

Before the fix BIGINT UNSIGNED -> DECIMAL(38,0) was graded lossy and a
TIMESTAMP(3) holding '2038-01-19 03:14:07.999' was refused as outside the
epoch range (the bound ignored the fraction). A TINYINT(1) holding 2 was
cast to boolean and refused ("Invalid boolean: '2'"); it now stays a native
TINYINT(1) on MySQL and maps to an integer carrier elsewhere.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live

CASES = {
    "ubig": ("BIGINT UNSIGNED", ["18446744073709551615", "0", "42", "7"]),
    "ts3": ("TIMESTAMP(3) NULL", ["'2038-01-19 03:14:07.999'", "'2001-02-03 04:05:06.120'", "NULL", "'1970-01-02 00:00:00.000'"]),
}


def test_bigint_unsigned_into_decimal_38_is_not_lossy():
    from services.source_engine_scope import bind_source_engine
    from services.type_coercion import is_lossy_coercion

    with bind_source_engine("mysql"):
        assert not is_lossy_coercion("BIGINT UNSIGNED", "DECIMAL(38,0)", dest_db="mysql")
        assert is_lossy_coercion("BIGINT UNSIGNED", "DECIMAL(19,0)", dest_db="mysql")


@pytest.mark.parametrize("case", sorted(CASES))
def test_mysql_to_mysql_copy_lands_exactly(case):
    live.need_mysql()
    col_type, values = CASES[case]
    u = live.uid()
    src, dst = f"w2dat_d15_{u}", f"w2dat_d15d_{u}"
    try:
        live.my(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, v {col_type})")
        live.my(f"INSERT INTO `{src}` VALUES " + ", ".join(f"({i}, {v})" for i, v in enumerate(values, 1)))
        res = live.run(live.ep_my(src), live.ep_my(dst), "full_refresh_overwrite", key="id", cols=["id", "v"])
        assert res.success, res.error
        cast = "CAST(v AS CHAR)"
        assert live.my(f"SELECT id, {cast} FROM `{dst}` ORDER BY id") == live.my(
            f"SELECT id, {cast} FROM `{src}` ORDER BY id")
    finally:
        live.drop_my(src, dst)


def _tinyint1_source(src: str, rows: int = 5) -> list[tuple[int, int | None]]:
    """TINYINT(1) holding 2 and -1; with ``rows`` > 5 the 2 sits past the 200-row sample."""
    live.my(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, f TINYINT(1))")
    vals: list[tuple[int, int | None]] = (
        [(1, 2), (2, 0), (3, 1), (4, None), (5, -1)] if rows <= 5
        else [(i, i % 2) for i in range(1, rows)] + [(rows, 2)]
    )
    live.my(f"INSERT INTO `{src}` VALUES " + ", ".join(
        f"({i}, {'NULL' if v is None else v})" for i, v in vals))
    return vals


@pytest.mark.parametrize("rows", [5, 300])
def test_tinyint1_holding_2_stays_native_mysql_to_mysql(rows):
    live.need_mysql()
    u = live.uid()
    src, dst = f"w2dat_d15t_{u}", f"w2dat_d15td_{u}"
    try:
        vals = _tinyint1_source(src, rows)
        res = live.run(live.ep_my(src), live.ep_my(dst), key="id", cols=["id", "f"])
        assert res.success, res.error
        assert [tuple(r) for r in live.my(f"SELECT id, f FROM `{dst}` ORDER BY id")] == vals
        ddl = live.my(f"SHOW CREATE TABLE `{dst}`")[0][1]
        assert "`f` tinyint(1)" in ddl, ddl
    finally:
        live.drop_my(src, dst)


@pytest.mark.parametrize("rows", [5, 300])
def test_tinyint1_holding_2_maps_to_an_integer_on_postgres(rows):
    live.need_mysql()
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_d15t_{u}", f"w2dat_d15tp_{u}"
    try:
        vals = _tinyint1_source(src, rows)
        res = live.run(live.ep_my(src), live.ep_pg(dst), key="id", cols=["id", "f"])
        assert res.success, res.error
        got = live.pg(f'SELECT id, f, pg_typeof(f)::text FROM "{dst}" ORDER BY id')
        assert [(i, v) for i, v, _ in got] == vals
        assert {t for _, _, t in got} == {"smallint"}
    finally:
        live.drop_my(src)
        live.drop_pg(dst)


def test_tinyint1_holding_only_0_1_is_still_boolean_like():
    live.need_mysql()
    from services.schema_introspect import introspect_schema

    t = f"w2dat_d15b_{live.uid()}"
    try:
        live.my(f"CREATE TABLE `{t}` (id INT PRIMARY KEY, f TINYINT(1), g TINYINT(1))")
        live.my(f"INSERT INTO `{t}` VALUES (1, 0, 1), (2, 1, 7)")
        cols = {c["name"]: c for c in introspect_schema("mysql", table=t, ssl=False, **live.MY)["columns"]}
        assert cols["f"]["inferred_type"] == "BOOLEAN"
        assert cols["g"]["inferred_type"] == "TINYINT"
    finally:
        live.drop_my(t)
