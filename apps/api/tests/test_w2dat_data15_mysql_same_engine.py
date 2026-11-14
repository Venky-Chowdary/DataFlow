"""DATA-15: a MySQL -> MySQL same-engine copy is not refused.

Before the fix BIGINT UNSIGNED -> DECIMAL(38,0) was graded lossy and a
TIMESTAMP(3) holding '2038-01-19 03:14:07.999' was refused as outside the
epoch range (the bound ignored the fraction). TINYINT(1) holding 2 is not
fixed here (see the report).
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
