"""DATA-17: MySQL 1117 / 1118 are refused at Validate, never by the driver.

Before the fix a PostgreSQL table whose create-new MySQL table was too wide
passed Validate and the run died with the raw driver error: 1118 "Row size
too large" (wide VARCHARs past the 65,535-byte row, or fixed-width columns
past InnoDB's 8,126-byte record) or 1117 "Too many columns" (InnoDB stops at
1,017 columns; MySQL's own cap is 4,096).
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live


def _copy(cols_sql: str, cols: list[str], **extra):
    live.need_pg()
    live.need_mysql()
    u = live.uid()
    src, dst = f"w2dat_d17r_{u}", f"w2dat_d17rd_{u}"
    try:
        live.pg(f'CREATE TABLE "{src}" (id INT PRIMARY KEY, {cols_sql})')
        live.pg(f'INSERT INTO "{src}" (id) VALUES (1), (2)')
        res = live.run(live.ep_pg(src), live.ep_my(dst), key="id", cols=["id"] + cols, extra_req=extra or None)
        exists = live.my(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
            (dst,),
        )[0][0]
        return res, exists
    finally:
        live.drop_pg(src)
        live.drop_my(dst)


def _refused(res, exists, *needles: str) -> None:
    assert not res.success
    err = res.error or ""
    assert "1118" not in err and "1117" not in err and "Row size too large" not in err, err
    assert "MySQL cannot create" in err, err
    for n in needles:
        assert n in err, err
    assert res.records_transferred == 0
    assert exists == 0


def test_wide_varchars_past_65535_bytes_are_refused_at_validate():
    cols = [f"c{i}" for i in range(70)]
    res, exists = _copy(", ".join(f"{c} VARCHAR(255)" for c in cols), cols)
    _refused(res, exists, "65,535", "TEXT")


def test_fixed_width_columns_past_the_innodb_record_are_refused_at_validate():
    cols = [f"c{i}" for i in range(1000)]
    res, exists = _copy(", ".join(f"{c} BIGINT" for c in cols), cols)
    _refused(res, exists, "8,126")


def test_too_many_columns_is_refused_at_validate():
    cols = [f"c{i}" for i in range(1100)]
    res, exists = _copy(", ".join(f"{c} SMALLINT" for c in cols), cols)
    _refused(res, exists, "1,101 columns", "1,017")


def test_the_writer_refuses_before_create_when_validate_is_skipped():
    cols = [f"c{i}" for i in range(70)]
    res, exists = _copy(", ".join(f"{c} VARCHAR(255)" for c in cols), cols, skip_preflight=True)
    assert not res.success
    assert "1118" not in (res.error or "") and "65,535" in (res.error or ""), res.error
    assert exists == 0


@pytest.mark.parametrize("n", [10, 60])
def test_a_table_that_fits_still_copies(n):
    cols = [f"c{i}" for i in range(n)]
    res, _exists = _copy(", ".join(f"{c} VARCHAR(255)" for c in cols), cols)
    assert res.success, res.error


def test_the_rule_matches_what_mysql_measures():
    """Each case was measured on MySQL 8.0 InnoDB (DYNAMIC, 16 KiB pages)."""
    from services.mysql_row_limits import mysql_create_limit_refusal as r

    def keyed(*types: str):
        return r(["id"] + [f"c{i}" for i in range(len(types))], ["INT", *types], key_cols=["id"])

    assert keyed("VARCHAR(16383)") is not None              # 1118, 65,535 row
    assert keyed("VARCHAR(8191)", "VARCHAR(8191)") is not None
    assert keyed("VARCHAR(16000)") is None
    assert keyed(*["VARCHAR(255)"] * 70) is not None        # 1118
    assert keyed(*["VARCHAR(255)"] * 60) is None
    assert keyed(*["BIGINT"] * 1000) is not None            # 1118, 8,126 record
    assert keyed(*["DECIMAL(65,30)"] * 500) is not None
    assert keyed(*["INT"] * 600, *["CHAR(20)"] * 200) is not None
    assert keyed(*["INT"] * 1017) is not None               # 1117 (1,018 incl. id)
    assert keyed(*["INT"] * 1016) is None
    assert keyed(*["TEXT"] * 196) is None                   # TEXT keeps 41 bytes in the page
    assert keyed(*["TEXT"] * 200) is not None
    assert keyed(*["VARCHAR(255) CHARACTER SET latin1"] * 200) is not None
    assert keyed(*["VARCHAR(9) CHARACTER SET latin1"] * 800) is None
    assert keyed(*["CHAR(10) CHARACTER SET latin1"] * 800) is None
    assert keyed(*["VARCHAR(60)"] * 190) is not None
    assert keyed(*["VARCHAR(10)"] * 190) is None
    assert keyed(*["VARCHAR(10)"] * 250) is not None
    assert keyed(*["DATETIME(6)"] * 1000) is not None
