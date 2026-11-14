"""Gate-8's NULL-marker probe compares bytes, form by form, table by table (review F4).

* **Exact.** The probe compared with the column's collation: a ``_ci`` or PAD
  SPACE MySQL column counted a user's ``__df_sql_null__`` or
  ``'__DF_SQL_NULL__ '`` as the marker, so a correct copy failed Gate-8. Text
  is now matched byte for byte.
* **Form by form.** The raw marker and its escaped spelling were added up:
  a destination holding the *escaped* text (an escape nobody decoded) passed
  against a source holding the raw literal. Each form is now compared with the
  same form on the source; escaped text the source does not hold is a leak.
* **Table by table.** A multi-table run probed only its last table; each table
  the run wrote is now probed.
"""

from __future__ import annotations

import uuid

import pytest

from services.value_serializer import ESCAPED_WIRE_PREFIX, SQL_NULL_SENTINEL
from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
from tests.test_reversibility_review3 import fake  # noqa: F401
from tests.test_reversibility_review6 import _resumable  # noqa: F401

ESCAPED = ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL
MAPS = [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}]


def _gate(endpoint, *, source=None, records=None, rows_written=0, summary=None, mappings=MAPS):
    from services.null_marker_guard import apply_null_marker_gate

    return apply_null_marker_gate(
        {"passed": True, "message": "Checksum matches."},
        endpoint=endpoint, source_endpoint=source, mappings=mappings,
        dest_summary=summary or {"table": endpoint.table}, records=records,
        rows_written=rows_written, validation_mode="strict",
    )


@pytest.fixture()
def pg_tables():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    tok = uuid.uuid4().hex[:8]
    # source, destination, a second destination, a second source
    names = [f"mxs_{tok}", f"mxd_{tok}", f"mxo_{tok}", f"mxt_{tok}"]
    for t in names:
        pg_sql(f'CREATE TABLE public."{t}" (id INT PRIMARY KEY, note TEXT)')
    try:
        yield names
    finally:
        for t in names:
            pg_sql(f'DROP TABLE IF EXISTS public."{t}"')


@pytest.fixture()
def mysql_dest(pg_tables):
    from tests.helpers.live_env import mysql_up

    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    table = f"mxm_{uuid.uuid4().hex[:8]}"
    try:
        yield table
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("collation", "text"),
    [("utf8mb4_0900_ai_ci", SQL_NULL_SENTINEL.lower()), ("utf8mb4_general_ci", SQL_NULL_SENTINEL + " ")],
)
def test_a_ci_or_pad_space_destination_does_not_count_a_near_spelling(pg_tables, mysql_dest, collation, text):
    src = pg_tables[0]
    # The user's text really is this near-spelling, on both sides.
    pg_sql(f'INSERT INTO public."{src}" VALUES (1, %s)', (text,))
    mysql_sql(f"CREATE TABLE {mysql_dest} (id INT PRIMARY KEY, note VARCHAR(40) COLLATE {collation})")
    mysql_sql(f"INSERT INTO {mysql_dest} VALUES (1, %s)", (text,))
    recon = _gate(mysql_endpoint(mysql_dest), source=pg_endpoint(src))
    assert recon["passed"] is True, recon
    assert recon["null_marker_check"]["destination_rows"] == 0, recon["null_marker_check"]


def test_the_marker_itself_still_counts_on_a_ci_destination(pg_tables, mysql_dest):
    src = pg_tables[0]
    pg_sql(f'INSERT INTO public."{src}" VALUES (1, NULL)')
    mysql_sql(f"CREATE TABLE {mysql_dest} (id INT PRIMARY KEY, note VARCHAR(40) COLLATE utf8mb4_0900_ai_ci)")
    mysql_sql(f"INSERT INTO {mysql_dest} VALUES (1, %s)", (SQL_NULL_SENTINEL,))
    recon = _gate(mysql_endpoint(mysql_dest), source=pg_endpoint(src))
    assert recon["passed"] is False and recon["null_marker_check"]["status"] == "leaked", recon


def test_escaped_text_the_source_does_not_hold_is_a_leak(pg_tables):
    src, dst, _, _ = pg_tables
    # The source holds the raw literal; the destination holds its undecoded escape.
    pg_sql(f'INSERT INTO public."{src}" VALUES (1, %s)', (SQL_NULL_SENTINEL,))
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s)', (ESCAPED,))
    recon = _gate(pg_endpoint(dst), source=pg_endpoint(src))
    check = recon["null_marker_check"]
    assert recon["passed"] is False, recon
    assert (check["destination_escaped_rows"], check["source_escaped_rows"]) == (1, 0), check
    assert (check["destination_raw_rows"], check["source_raw_rows"]) == (0, 1), check


def test_escaped_text_against_the_runs_records(pg_tables):
    _src, dst, _, _ = pg_tables
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s)', (ESCAPED,))
    # A file cell that says the raw marker reaches the run wire-escaped.
    literal = [{"id": "1", "note": ESCAPED}]
    assert _gate(pg_endpoint(dst), records=literal, rows_written=1)["passed"] is False
    # A file cell that really says the escaped text is escaped once more on the wire.
    escaped_literal = [{"id": "1", "note": ESCAPED_WIRE_PREFIX + ESCAPED}]
    assert _gate(pg_endpoint(dst), records=escaped_literal, rows_written=1)["passed"] is True


def _streams(people_src, people, orders_src, orders, *, people_rows=1, earlier=0):
    # A stream reads the source table its name names and writes its own table.
    return {
        "table": orders, "multi_stream": True, "last_stream_mappings": MAPS,
        "streams": [
            {"name": people_src, "table": people, "status": "completed", "records_processed": people_rows,
             "rows_from_earlier_pass": earlier, "sync_mode": "append"},
            {"name": orders_src, "table": orders, "status": "completed", "records_processed": 1,
             "sync_mode": "append"},
        ],
    }


def test_every_table_of_a_multi_table_run_is_probed(pg_tables):
    people_src, people, orders, orders_src = pg_tables
    pg_sql(f'INSERT INTO public."{people_src}" VALUES (1, NULL)')
    pg_sql(f'INSERT INTO public."{people}" VALUES (1, %s)', (SQL_NULL_SENTINEL,))
    pg_sql(f'INSERT INTO public."{orders_src}" VALUES (1, %s)', ("fine",))
    pg_sql(f'INSERT INTO public."{orders}" VALUES (1, %s)', ("fine",))
    recon = _gate(pg_endpoint(orders), source=pg_endpoint(orders_src),
                  summary=_streams(people_src, people, orders_src, orders))
    check = recon["null_marker_check"]
    assert recon["passed"] is False, recon
    assert {t["table"]: t["status"] for t in check["tables"]} == {people: "leaked", orders: "passed"}, check
    assert people in recon["message"], recon["message"]


def test_a_table_an_earlier_pass_wrote_is_not_probed(pg_tables):
    people_src, people, orders, orders_src = pg_tables
    pg_sql(f'INSERT INTO public."{people}" VALUES (1, %s)', (SQL_NULL_SENTINEL,))
    summary = _streams(people_src, people, orders_src, orders, people_rows=0, earlier=1)
    check = _gate(pg_endpoint(orders), source=pg_endpoint(orders_src), summary=summary)["null_marker_check"]
    assert {t["table"]: t["status"] for t in check["tables"]} == {people: "skipped", orders: "passed"}, check


def test_engine_multi_table_run_probes_each_table(tmp_path, _resumable):  # noqa: F811
    """The real engine path: a two-table SQLite run names both tables in the check."""
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_c5_resume_reversibility import _request, _seed

    src, dst, specs = _seed(tmp_path)
    # Strict (the helper's mode): an upsert into tables that held rows has no
    # whole-table scope otherwise.
    result = UniversalTransferEngine().execute_tracked(_request(src, dst, specs), "mx" + uuid.uuid4().hex[:12])
    assert result.success is True, result.error
    check = ((result.destination_summary or {}).get("reconciliation") or {}).get("null_marker_check") \
        or (getattr(result, "reconciliation", None) or {}).get("null_marker_check") or {}
    tables = check.get("tables") or []
    assert sorted(t["table"] for t in tables) == ["orders", "people"], check
    assert {t["status"] for t in tables} == {"passed"}, tables


def test_a_source_probe_that_stops_is_not_measured(pg_tables, monkeypatch):
    """The destination holds marker text and the source count stops at its cap."""
    import services.null_marker_probe as probe

    src, dst, _, _ = pg_tables
    monkeypatch.setattr(probe, "PROBE_LIMIT", 2)
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s), (2, %s)', (SQL_NULL_SENTINEL, ESCAPED))
    pg_sql(f'INSERT INTO public."{src}" SELECT g, %s FROM generate_series(1, 3) g', ("__DF_MISSING__",))
    recon = _gate(pg_endpoint(dst), source=pg_endpoint(src))
    check = recon["null_marker_check"]
    assert check["status"] == "not_measured" and check["reason"].startswith("source:"), check
    assert recon["unproven"] is True, recon
