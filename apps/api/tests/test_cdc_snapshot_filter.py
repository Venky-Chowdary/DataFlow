"""Filtered CDC incremental snapshots: spec validation + per-dialect pushdown."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.cdc_snapshot_filter import (  # noqa: E402
    SnapshotFilterError,
    compile_snapshot_filter_mongo,
    compile_snapshot_filter_sql,
    describe_snapshot_filter,
    normalize_snapshot_filter,
)
from services.cdc_snapshot_resume import snapshot_keyset_sql  # noqa: E402

SPEC = {"and": [{"column": "region", "op": "eq", "value": "EU"},
                {"or": [{"column": "amount", "op": ">=", "value": 100},
                        {"column": "note", "op": "is_null"}]}]}


def test_compile_postgres_uses_binds_and_quotes():
    sql, params = compile_snapshot_filter_sql(normalize_snapshot_filter(SPEC), dialect="postgresql")
    assert sql == '("region" = %s AND ("amount" >= %s OR "note" IS NULL))'
    assert params == ["EU", 100]


def test_compile_sqlserver_brackets_and_like_escapes():
    spec = normalize_snapshot_filter({"column": "code", "op": "startswith", "value": "50%_[x]!"})
    sql, params = compile_snapshot_filter_sql(spec, dialect="sqlserver", quote_char="[")
    assert sql == "[code] LIKE %s ESCAPE '!'"
    assert params == ["50!%!_![x]!!%"]


def test_compile_oracle_upper_and_in_list():
    spec = normalize_snapshot_filter({"column": "status", "op": "not_in", "value": ["X", "Y"]})
    sql, params = compile_snapshot_filter_sql(spec, dialect="oracle", upper_columns=True)
    assert sql == '"STATUS" NOT IN (%s, %s)'
    assert params == ["X", "Y"]


@pytest.mark.parametrize(
    "bad",
    [
        {"column": "a", "op": "regex", "value": ".*"},
        {"column": "a; DROP TABLE t", "op": "eq", "value": 1},
        {"column": "a", "op": "eq", "value": {"$gt": 1}},
        {"column": "a", "op": "in", "value": []},
        {"column": "a", "op": "eq"},
        {"column": "a", "op": "contains", "value": 5},
        {"and": []},
        "not json",
    ],
)
def test_invalid_filters_fail_closed(bad):
    with pytest.raises(SnapshotFilterError):
        normalize_snapshot_filter(bad)


def test_value_is_never_inlined_into_sql():
    spec = normalize_snapshot_filter({"column": "name", "op": "eq", "value": "x' OR '1'='1"})
    sql, params = compile_snapshot_filter_sql(spec, dialect="mysql")
    assert "OR" not in sql and params == ["x' OR '1'='1"]


def test_keyset_sql_combines_seek_and_filter_all_dialects():
    filt, fparams = compile_snapshot_filter_sql(
        normalize_snapshot_filter({"column": "region", "op": "eq", "value": "EU"}), dialect="postgresql"
    )
    sql, params = snapshot_keyset_sql(
        table_ref="t", quoted_pk_columns=['"id"'], last_pk="5", limit=10,
        dialect="postgresql", filter_sql=filt, filter_params=fparams,
    )
    assert sql.startswith('SELECT * FROM t WHERE (("id" > %s)) AND ("region" = %s)')
    assert params == ["5", "EU", 10]
    sql, params = snapshot_keyset_sql(
        table_ref="t", quoted_pk_columns=['"ID"'], last_pk="", limit=10,
        dialect="oracle", filter_sql='"REGION" = %s', filter_params=["EU"],
    )
    assert 'WHERE ("REGION" = :k0)' in sql and params == {"k0": "EU", "lim": 10}
    sql, params = snapshot_keyset_sql(
        table_ref="[t]", quoted_pk_columns=["[id]"], last_pk="", limit=10, dialect="sqlserver",
    )
    assert sql == "SELECT TOP (10) * FROM [t] ORDER BY [id]" and params == []


def test_mongo_compile():
    assert compile_snapshot_filter_mongo(normalize_snapshot_filter(SPEC)) == {
        "$and": [{"region": "EU"}, {"$or": [{"amount": {"$gte": 100}}, {"note": None}]}]
    }


def test_describe_and_signal_round_trip(tmp_path, monkeypatch):
    import services.cdc_incremental_snapshot as snap_mod

    monkeypatch.setattr(snap_mod, "_PATH", str(tmp_path / "signals.json"))
    monkeypatch.setattr(snap_mod, "_signals_coll", lambda: None)
    sig = snap_mod.request_incremental_snapshot("src", "orders", primary_key="id", row_filter=SPEC)
    loaded = snap_mod.get_signal(sig.id)
    assert loaded is not None and loaded.row_filter == normalize_snapshot_filter(SPEC)
    assert describe_snapshot_filter(loaded.row_filter) == '(region = "EU" AND (amount >= 100 OR note IS NULL))'
    with pytest.raises(ValueError):
        snap_mod.request_incremental_snapshot("src", "orders", primary_key="id", row_filter={"column": "a", "op": "regex", "value": "x"})
