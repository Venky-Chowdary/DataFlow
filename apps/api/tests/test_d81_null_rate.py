"""D81 regression: the strict null-rate check must count real absence only.
On SQLite a stored '' is a value (SQLite keeps '' unlike Oracle); counting it
as NULL inflated the reported rate (prod: 25.58% vs real 11.6%)."""
from __future__ import annotations


def test_d81_sqlite_strict_null_rate_ignores_empty_strings():
    """D81: the strict null-rate check counted stored '' as NULL on SQLite
    (reported 25.58%, real 11.6%). '' is a value; only real absence counts."""
    from services.data_quality import run_integrity_audit
    from services.value_serializer import SQL_NULL_SENTINEL

    # 13 rows: 1 real NULL (wire sentinel) + 1 stored '' + 11 values
    # real null rate = 1/13 ~= 7.7%; pre-fix reported (NULL+'')/13 ~= 15.4%.
    values = ["v"] * 11 + [SQL_NULL_SENTINEL, ""]
    report = run_integrity_audit(
        ["c"], rows=[[v] for v in values], column_types={"c": "TEXT"},
        validation_mode="strict", sync_mode="full_refresh_overwrite",
    )
    rate = report.stats["columns"]["c"]["null_rate"]
    assert rate == 1 / 13, f"null_rate {rate} counted '' as NULL"
