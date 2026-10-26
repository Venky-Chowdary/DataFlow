"""D105 repro: aggregate_data must report the true group count and
truncated=true when LIMIT cuts groups (flagging ties at the cut); accept a
date column for a time grain (group_by "created_at:day", "created_at by day",
the emitted "<col>_day" alias, or a date_column field) instead of silently
grouping by the raw timestamp; and honor quoted filter values containing
commas/apostrophes.
"""
from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from src.ai.copilot.aggregate_tools import aggregate_connector_data

PG_HOST, PG_PORT = "localhost", 5432
PG_DSN = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": "dataflow",
    "user": "dataflow",
    "password": "dataflow",
}


def _pg_reachable() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(**PG_DSN)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def live_table():
    if not _pg_reachable():
        pytest.skip(f"Postgres {PG_HOST}:{PG_PORT} not reachable")
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    from services.connector_store import create_connector, delete_connector

    table = f"pilot_d105_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{table}" ('
            " id serial PRIMARY KEY,"
            " email text,"
            " region text,"
            " amount numeric(10,2),"
            " created_at timestamp NOT NULL,"
            " updated_at timestamp NOT NULL)"
        )
        # 12 distinct email groups; four of them tie at the high count 3, so
        # LIMIT 2 shows two 3-count groups and hides two tied groups.
        emails = [
            "a@x.io", "b@x.io", "c@x.io", "d@x.io", "e@x.io", "f@x.io",
            "g@x.io", "h@x.io", "i@x.io", "j@x.io", "k@x.io", "l@x.io",
        ]
        rows = []
        for i, e in enumerate(emails):
            n = 1 if i < 8 else 3  # i,j,k,l@x.io each get 3 rows
            for j in range(n):
                rows.append((
                    e,
                    "Central, HQ" if i == 0 else ("South's" if i == 1 else "emea"),
                    Decimal("10.00"),
                    f"2024-01-{(i % 28) + 1:02d} 10:00:00",
                    f"2024-02-{(i % 28) + 1:02d} 10:00:00",
                ))
        cur.executemany(
            f'INSERT INTO "{table}" (email, region, amount, created_at, updated_at)'
            " VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total_rows = cur.fetchone()[0]

    connector_id = ""
    try:
        saved = create_connector(
            {
                "name": f"PilotD105-{uuid.uuid4().hex[:6]}",
                "type": "postgresql",
                "host": PG_HOST,
                "port": PG_PORT,
                "database": PG_DSN["dbname"],
                "username": PG_DSN["user"],
                "password": PG_DSN["password"],
                "schema": "public",
            }
        )
        connector_id = str(getattr(saved, "id", "") or "")
        if not connector_id:
            pytest.skip("connector store did not return an id")
        yield {"connector_id": connector_id, "table": table, "total_rows": total_rows}
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()
        if connector_id:
            try:
                delete_connector(connector_id)
            except Exception:
                pass


def test_group_limit_reports_true_count_and_truncated(live_table):
    """LIMIT 5 over 12 groups must say truncated + the real group count."""
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="email",
        limit=5,
    )
    assert res.success, res.error
    out = res.output
    assert out["truncated"] is True, out
    # The true number of groups — not the LIMIT.
    assert out["group_count"] == 12, out
    assert len(out["rows"]) == 5
    assert out["exact"] is True  # shown rows are still exact


def test_untruncated_group_count_stays_exact(live_table):
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="email",
        limit=20,
    )
    assert res.success, res.error
    assert res.output["truncated"] is False
    assert res.output["group_count"] == 12
    assert len(res.output["rows"]) == 12


def test_ties_at_cut_are_flagged(live_table):
    """Groups tied at the boundary value beyond the limit get a tie flag."""
    # With limit 2, the DESC order puts the two 3-row groups first and the
    # 1-row groups are cut — many ties at the cut value 1.
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="email",
        limit=2,
        order="desc",
    )
    assert res.success, res.error
    out = res.output
    assert out["truncated"] is True
    assert out["group_count"] == 12
    # Two 3-row groups are shown; the cut-off value is 1 with 10 tied groups.
    assert out.get("ties_at_cut") is True, out


def test_grain_column_colon_form(live_table):
    """group_by "created_at:day" answers the two-date-column question."""
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="created_at:day",
        limit=50,
    )
    assert res.success, res.error
    out = res.output
    assert out["grain"] == "day"
    assert out["group_count"] >= 10  # 12 distinct days, not 1-row-per-row
    assert all(r.get("created_at_day") is not None for r in out["rows"])


def test_grain_column_by_form(live_table):
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="created_at by day",
        limit=50,
    )
    assert res.success, res.error
    assert res.output["grain"] == "day"


def test_grain_column_alias_form(live_table):
    """The "<col>_<grain>" alias the tool itself emits must round-trip."""
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="created_at_day",
        limit=50,
    )
    assert res.success, res.error
    assert res.output["grain"] == "day"
    assert res.output["group_count"] >= 10


def test_grain_date_column_param(live_table):
    """A bare grain plus date_column= answers the 'which column' question."""
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="day",
        date_column="updated_at",
        limit=50,
    )
    assert res.success, res.error
    assert res.output["grain"] == "day"
    assert res.output["group_by"] == "updated_at_day"


def test_grain_ambiguous_without_date_column_still_asks(live_table):
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="day",
    )
    assert res.success is False
    assert "date column" in (res.error or "").lower()


def test_grain_nontemporal_refuses_clearly(live_table):
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        group_by="region:day",
    )
    assert res.success is False
    assert "not a date column" in (res.error or "").lower()


def test_filter_value_with_comma_in_quotes(live_table):
    """where region = 'Central, HQ' must parse — quoted commas are data."""
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        where="region = 'Central, HQ'",
    )
    assert res.success, res.error
    assert res.output["value"] == 1


def test_filter_value_with_apostrophe_in_quotes(live_table):
    res = aggregate_connector_data(
        connector_id=live_table["connector_id"],
        table=live_table["table"],
        metric="count",
        where='region = "South\'s"',
    )
    assert res.success, res.error
    assert res.output["value"] == 1
