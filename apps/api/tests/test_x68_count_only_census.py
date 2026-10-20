"""X-68: count-only engine routes run the key census as the value compare.

A tamper that changes a value (not a count) must fail on count-only engine
copies — 'partial + passed' is only honest when the (pk, row-hash) census
actually ran or a user-visible finding says no compare could run.
"""
from __future__ import annotations


def test_x68_generic_census_reports_reason_without_pk() -> None:
    from src.transfer import reconcile_step as rs

    class _EP:
        format = "postgresql"
        table = "t"
        collection = ""
        kind = "database"
        schema = "public"

    changed, missing, extra, reason = rs._generic_key_census(
        source_endpoint=_EP(),
        endpoint=_EP(),
        dest_summary={},
        mappings=[{"source": "a", "target": "a"}],
    )
    assert (changed, missing, extra) == ([], [], [])
    assert reason is not None and "primary key" in reason


def test_x68_census_reason_when_no_mappings() -> None:
    from src.transfer import reconcile_step as rs

    class _EP:
        format = "postgresql"
        table = "t"
        collection = ""
        kind = "database"
        schema = "public"

    _c, _m, _e, reason = rs._generic_key_census(
        source_endpoint=_EP(),
        endpoint=_EP(),
        dest_summary={"primary_key_columns": ["id"]},
        mappings=[],
    )
    assert reason is not None and "mappings" in reason


def test_x68_name_gate8_wrapper_stamps_reason() -> None:
    from src.transfer import reconcile_step as rs

    class _EP:
        format = "postgresql"
        table = "t"
        collection = ""
        kind = "database"
        schema = "public"

    report: dict = {}
    rs._name_gate8_culprits_generic(
        report,
        source_endpoint=_EP(),
        endpoint=_EP(),
        dest_summary={},
        mappings=[],
    )
    assert "culprit_keys_unavailable_reason" in report
    assert "culprit keys could not be named" in report[
        "culprit_keys_unavailable_reason"
    ]


def test_x48b_pyodbc_decimals_bind_as_uniform_canonical_text() -> None:
    from decimal import Decimal

    from services.copy_pg_sqlserver import bind_row_for_sqlserver

    class _PyodbcCursor:
        pass

    inner = type("_cur", (), {})()
    inner.__class__ = type(
        "_cur", (), {"__module__": "pyodbc.fake"}
    )
    cur = _PyodbcCursor()
    cur._cur = inner

    rows = [
        (1, "1234567890123456789012345678.0123456789"),
        (2, Decimal("-1E-10")),
        (3, Decimal("0E-10")),
        (4, None),
        (5, Decimal("-9999999999999999999999999999.9999999999")),
    ]
    for _i, v in rows:
        bound = bind_row_for_sqlserver(cur, (1, v))[1]
        if v is not None:
            assert isinstance(bound, str), (v, bound)
            assert "E" not in bound.upper().replace("E-10", "e10") or "e" not in bound
    b2 = bind_row_for_sqlserver(cur, (1, Decimal("-1E-10")))[1]
    assert b2 == "-0.0000000001"
    b3 = bind_row_for_sqlserver(cur, (1, Decimal("0E-10")))[1]
    assert b3 == "0.0000000000"
