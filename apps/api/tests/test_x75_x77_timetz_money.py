"""X-75: timetz read-backs compare the offset, not the fraction width —
DuckDB's '+00' bare-hour offset and '.789' vs '.789000' spellings are the
same instant. X-77: PG money is int64 cents (DECIMAL(19,2) domain), not the
MSSQL scale-4 stamp that leaves 15 integer digits and quarantines the max.
"""

from __future__ import annotations


def test_bare_hour_offset_folds_to_utc_wall_clock():
    from services.reconciliation import fingerprint_for_reconcile as fp

    a = fp("12:34:56.789+00", ddl_type="TIMETZ", engine="duckdb")
    b = fp("12:34:56.789000+00", ddl_type="TIMETZ", engine="duckdb")
    assert a == b == "12:34:56.789"


def test_offset_still_changes_the_fingerprint():
    from services.reconciliation import fingerprint_for_reconcile as fp

    a = fp("10:30:00+05:30", ddl_type="TIMETZ", engine="duckdb")
    b = fp("10:30:00+00:00", ddl_type="TIMETZ", engine="duckdb")
    assert a != b  # zone tamper must keep failing (X-74 contract)


def test_pg_money_introspects_decimal_19_2():
    from services.schema_introspect import _pg_to_logical

    assert _pg_to_logical("money") == "DECIMAL(19,2)"


def test_pg_money_ddl_holds_full_domain():
    from services.decision_kernel.type_invent import ddl_type
    from services.source_engine_scope import bind_source_engine
    from services.type_system import parse_numeric_precision_scale

    with bind_source_engine("postgresql"):
        for dest in ("mysql", "sqlserver", "mssql", "duckdb", "postgresql"):
            ddl = ddl_type(dest, "MONEY")
            p, s = parse_numeric_precision_scale(ddl)
            # DECIMAL(19,4) holds 15 integer digits — PG money max needs 17.
            assert (p, s) == (19, 2) or (p or 0) - (s or 0) >= 17, (dest, ddl)


def test_mssql_money_stays_scale_4():
    from services.decision_kernel.type_invent import ddl_type
    from services.source_engine_scope import bind_source_engine

    with bind_source_engine("sqlserver"):
        assert ddl_type("sqlserver", "MONEY") == "MONEY"
        assert ddl_type("postgresql", "MONEY") == "DECIMAL(19,4)"


def test_money_carrier_recognizes_19_2_and_no_collapse():
    from services.type_system import is_money_carrier, money_domain_would_collapse

    assert is_money_carrier("DECIMAL(19,2)")
    assert is_money_carrier("DECIMAL(19,4)")
    assert not money_domain_would_collapse("MONEY", "DECIMAL(19,2)")
    assert money_domain_would_collapse("MONEY", "DECIMAL(10,4)")


def test_nan_is_named_nonfinite_not_null_sentinel():
    # X-78: "Null sentinel 'NaN'" mislabels an IEEE value — refuse honestly.
    from services.transform_engine import apply_transform

    _, err = apply_transform("NaN", "decimal")
    assert "non-finite" in err and "NaN" in err
    assert "Null sentinel" not in err
    _, err = apply_transform("Infinity", "decimal")
    assert "non-finite" in err
    # Real null sentinels keep their sentinel label.
    _, err = apply_transform("null", "decimal")
    assert "Null sentinel" in err
