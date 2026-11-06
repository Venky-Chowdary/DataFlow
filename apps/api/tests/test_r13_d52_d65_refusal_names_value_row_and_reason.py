"""D52 / D65 — a refusal names the value, the row and the reason.

D52: a wall clock in a spring-forward gap (2024-03-10 02:30 America/New_York)
was refused under the headline "Sample transform / cast failures ... classes:
UNKNOWN" with the fix "widen 'wall' to TIMESTAMP_LTZ(9)" (already that type).
D65: a BC date ('0044-03-15 BC') was refused as "fix the source read
(connection, table name, timeout)" plus "open Map", and PG -> PG (which holds
BC dates) was blocked too.

Live PostgreSQL source, preflight ON, strict.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def test_dst_gap_refusal_names_row_value_zone_and_offers_quarantine(isolated):  # noqa: F811
    table = f"d52_{h.token()}"
    out = f"{table}_o"
    try:
        h.run_sql(
            "postgresql",
            f"CREATE TABLE {table} (id INT PRIMARY KEY, wall TIMESTAMP)",
            f"INSERT INTO {table} VALUES (1, '2024-03-10 01:30:00'), "  # nosec B608
            "(2, '2024-03-10 02:30:00'), (3, '2024-03-10 03:30:00')",
        )
        result = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
            sync_mode="full_refresh_overwrite", source_timezone="America/New_York",
        )
        assert not result.success
        error = result.error or ""
        assert "row 2" in error and "2024-03-10T02:30:00" in error, error
        assert "does not exist in timezone 'America/New_York'" in error, error
        assert "quarantine" in error.lower(), error
        assert "UNKNOWN" not in error and "widen" not in error.lower(), error
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)


def test_dst_gap_fix_text_does_not_offer_a_widen():
    from services.coercion_probe import _dst_gap_reason

    reason = "Wall clock '2024-03-10T02:30:00' does not exist in timezone 'America/New_York'"
    assert _dst_gap_reason(["Invalid integer", reason]) == reason
    assert _dst_gap_reason(["Invalid integer"]) == ""


_BC_ROWS = "(1,'2024-01-01','a'),(2,'1999-12-31',''),(3,'2000-02-29',NULL),(4,'0044-03-15 BC','ides')"


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "sqlserver"])
def test_bc_date_lands_where_it_fits_and_is_named_where_it_does_not(isolated, dest):  # noqa: F811
    table = f"d65_{h.token()}"
    out = f"{table}_o"
    try:
        h.run_sql(
            "postgresql",
            f"CREATE TABLE {table} (id INT PRIMARY KEY, d DATE, note TEXT)",
            f"INSERT INTO {table} VALUES {_BC_ROWS}",  # nosec B608
        )
        result = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint(dest, out),
            sync_mode="full_refresh_overwrite",
        )
        if dest == "postgresql":
            assert result.success, result.error
            got = h.run_sql("postgresql", f"SELECT id, d::text, note FROM {out} ORDER BY id")  # nosec B608
            assert got[3] == (4, "0044-03-15 BC", "ides") and got[1][2] == "" and got[2][2] is None
            return
        assert not result.success
        error = result.error or ""
        assert "'0044-03-15 BC' is a BC date" in error, error
        assert "data row 4" in error and "d" in error, error
        assert "connection" not in error.lower() and "timeout" not in error.lower(), error
    finally:
        h.drop("postgresql", table)
        h.drop(dest, out)
