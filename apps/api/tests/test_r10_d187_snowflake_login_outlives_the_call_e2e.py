"""D187 (QA round 10, still reproduces): PostgreSQL -> Snowflake plan/start
at a real account's login cost fits the 45s deadline, and a timeout names
Snowflake, not the source.

What still ran inside each call's deadline on the real path, measured with
fakesnow slowed to a remote account (``tests/injected_latency.py``):

* one Snowflake login per tool call: ``plan_transfer``, then ``start_transfer``
  for the same endpoint, then run 2's plan, each logged in again. A real login
  (TLS, OCSP, authentication, session setup) is the slowest single step and
  the most variable one: across regions or behind a proxy it takes seconds to
  tens of seconds, while ``test_connector`` / ``run_query`` (120s deadline)
  still look fine;
* the SQLAlchemy pool's reset ``ROLLBACK`` on the plan login, a round trip
  each, three per ``start_transfer`` on run 2, with no transaction open.

The costs here are a 15s login, 3s per statement and an 8s warehouse resume.
The deadline stays 45s. Only a real Snowflake account can confirm the real
numbers.
"""

from __future__ import annotations

import time
import uuid

import pytest

pytest.importorskip("fakesnow")

from injected_latency import slow_snowflake  # noqa: E402
from test_d187_snowflake_destination_fits_the_budget_e2e import (  # noqa: E402
    _assert_cells,
    _snowflake_rows,
)
from test_d187_snowflake_worst_case_latency_e2e import _call, _run_job  # noqa: E402
from test_d187_snowflake_worst_case_latency_e2e import pg_route as _pg_route  # noqa: E402,F401

DEADLINE = 45.0


@pytest.fixture
def pg_route(_pg_route, monkeypatch):  # noqa: F811
    from services import engine_pool

    monkeypatch.setenv("DATAFLOW_SNOWFLAKE_WARM_LOGIN_EMULATOR", "1")
    engine_pool.reset_for_tests()
    # A remote account's costs, heavier than the round-6 harness's 4s login
    # (that harness's own injection is switched off so costs do not stack).
    _pg_route["calls"].active.clear()
    _pg_route["calls"] = slow_snowflake(monkeypatch, login=15.0, query=3.0, warehouse_resume=8.0)
    yield _pg_route
    engine_pool.reset_for_tests()


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_plan_and_start_fit_the_deadline_at_a_remote_accounts_login_cost(pg_route, mode):
    from src.ai.copilot.tool_deadline import deadline_seconds

    assert deadline_seconds("start_transfer") == DEADLINE, "the deadline is not raised"
    dest = f"D187R_{uuid.uuid4().hex[:6].upper()}"
    logins: list[int] = []
    for run in (1, 2):
        if run == 2:
            with pg_route["db"].cursor() as cur:
                cur.execute(
                    f"UPDATE public.{pg_route['table']} SET name = 'tampered ✓', "
                    "updated_at = '2026-02-03 04:05:06' WHERE id IN (2, 4)"
                )
        for tool in ("plan_transfer", "start_transfer"):
            before = pg_route["calls"].counts.get("sf_login", 0)
            result, seconds = _call(tool, pg_route, dest, mode)
            assert result.success, (run, tool, result.error)
            assert seconds < DEADLINE - 5, (run, tool, round(seconds, 1))
            logins.append(pg_route["calls"].counts.get("sf_login", 0) - before)
            if tool == "start_transfer":
                job = _run_job(pg_route, result.output["ack_id"])
                assert job.get("status") == "completed", job
                pg_route["calls"].active.set()
        if run == 1:
            _assert_cells(dest)
    got = _snowflake_rows(dest)
    assert got[2][0] == "tampered ✓" and got[4][0] == "tampered ✓", (got[2], got[4])
    # One login for the whole conversation: start_transfer and run 2 take the
    # login plan_transfer opened instead of paying for it again.
    assert logins[0] == 1 and sum(logins[1:]) == 0, logins


def test_a_snowflake_connector_with_only_an_account_is_named_in_a_timeout():
    """The preflight step reads both systems; a Snowflake destination stored
    with its account and no host must not drop out of the message."""
    from src.ai.copilot import tool_deadline as td

    src = {"name": "pg", "type": "postgresql", "host": "pg.example.com", "port": 5432}
    dst = {"name": "sf-prod", "type": "snowflake", "host": "", "extra": {"account": "xy12345.us-east-1"}}
    progress = td._Progress(limit=45.0)
    token = td._progress.set(progress)
    try:
        td.note_step("running the preflight checks", src, role="source", also=(("destination", dst),))
        message = td._timeout_message("start_transfer", 45.0, progress)
    finally:
        td._progress.reset(token)
    assert "xy12345.us-east-1" in message, message
    assert not message.startswith("source unreachable"), message
    # A destination with neither host nor account is still named.
    progress = td._Progress(limit=45.0)
    token = td._progress.set(progress)
    try:
        td.note_step("running the preflight checks", src, role="source",
                     also=(("destination", {"name": "wh", "type": "snowflake"}),))
        message = td._timeout_message("start_transfer", 45.0, progress)
    finally:
        td._progress.reset(token)
    assert "snowflake connector 'wh'" in message, message


def test_a_parked_login_is_never_reused_after_the_session_changed():
    """Only a login that still holds the session it was set up with is kept."""
    from connectors import snowflake_warm_login as warm

    class _Conn:
        def __init__(self):
            self.closed = False

        def is_closed(self):
            return self.closed

        def close(self):
            self.closed = True

    warm.reset_for_tests()
    try:
        a = _Conn()
        assert warm.park(("k",), a, {"SCHEMA": "PUBLIC"})
        assert warm.take(("other",)) is None
        assert warm.take(("k",)) == (a, {"SCHEMA": "PUBLIC"})
        assert warm.take(("k",)) is None  # taken once
        b = _Conn()
        warm.park(("k",), b, {})
        b.closed = True  # the server ended it
        assert warm.take(("k",)) is None
        c = _Conn()
        warm.park(("k",), c, {})
        import os

        os.environ["DATAFLOW_SNOWFLAKE_WARM_LOGIN_SECONDS"] = "0.01"
        try:
            time.sleep(0.05)
            assert warm.take(("k",)) is None and c.closed
        finally:
            del os.environ["DATAFLOW_SNOWFLAKE_WARM_LOGIN_SECONDS"]
    finally:
        warm.reset_for_tests()
