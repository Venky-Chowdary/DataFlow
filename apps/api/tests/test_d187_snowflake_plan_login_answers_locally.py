"""D187: what a shared Snowflake plan login already knows is answered locally,
and nothing it answers can go stale.

Counted on fakesnow (``slow_snowflake`` with no delay counts statements).
"""

from __future__ import annotations

import pytest

pytest.importorskip("fakesnow")

from injected_latency import slow_snowflake  # noqa: E402

LOGIN = dict(
    account="localhost", username="test", password="test", database="dataflow",
    schema="PUBLIC", warehouse="COMPUTE_WH", connection_string="",
)


@pytest.fixture
def calls(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    return slow_snowflake(monkeypatch, login=0.0, query=0.0, warehouse_resume=0.0)


def _sent(calls, before: int) -> int:
    return calls.counts["sf_statement"] - before


def test_session_statements_the_login_already_set_are_not_sent(calls):
    from connectors.snowflake_conn import get_connection
    from services.endpoint_session import endpoint_session_scope

    with endpoint_session_scope():
        conn = get_connection(**LOGIN)
        assert calls.counts["sf_login"] == 1
        before = calls.counts["sf_statement"]
        cur = conn.cursor()
        for sql in (
            "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 45",
            'USE WAREHOUSE "COMPUTE_WH"',
            'USE DATABASE "DATAFLOW"',
            "use database dataflow",
            'USE SCHEMA "PUBLIC"',
            "SELECT CURRENT_ROLE()",
            "select current_database(), current_schema();",
            "SELECT CURRENT_VERSION()",
        ):
            cur.execute(sql)
        assert _sent(calls, before) == 0, calls.counts
        cur.execute("SELECT CURRENT_ROLE()")
        assert cur.fetchone()[0]
        cur.execute("select current_database(), current_schema();")
        assert cur.fetchall() == [("DATAFLOW", "PUBLIC")]
        # A different object is sent, and becomes the context.
        cur.execute('USE SCHEMA "public"')  # quoted lower case is another schema
        assert _sent(calls, before) == 1


def test_catalog_answers_are_shared_and_dropped_on_any_change(calls):
    from connectors.snowflake_conn import get_connection
    from services.endpoint_session import endpoint_session_scope

    with endpoint_session_scope():
        a = get_connection(**LOGIN).cursor()
        b = get_connection(**LOGIN).cursor()
        before = calls.counts["sf_statement"]
        # Not there yet: the error is the catalog's answer, for both steps.
        for cur in (a, b):
            with pytest.raises(Exception, match="(?i)does not exist|not found|catalog"):
                cur.execute('DESC TABLE "PUBLIC"."D187_T"')
        assert _sent(calls, before) == 1
        # Any statement that is not a read drops every kept answer.
        a.execute('CREATE TABLE "PUBLIC"."D187_T" (id INT, name VARCHAR)')
        b.execute('DESC TABLE "PUBLIC"."D187_T"')
        cols = [row[0].upper() for row in b.fetchall()]
        assert cols == ["ID", "NAME"]
        a.execute('DESC TABLE "PUBLIC"."D187_T"')
        assert [row[0].upper() for row in a] == ["ID", "NAME"]
        assert a.description and a.rowcount == 2
        assert _sent(calls, before) == 3  # error DESC, CREATE, one DESC
        a.execute('ALTER TABLE "PUBLIC"."D187_T" ADD COLUMN extra INT')
        b.execute('DESC TABLE "PUBLIC"."D187_T"')
        assert [row[0].upper() for row in b.fetchall()] == ["ID", "NAME", "EXTRA"]
        # A data read is never kept.
        a.execute('SELECT COUNT(*) FROM "PUBLIC"."D187_T"')
        a.execute('SELECT COUNT(*) FROM "PUBLIC"."D187_T"')
        assert _sent(calls, before) == 7


def test_outside_a_plan_nothing_is_answered_locally(calls):
    from connectors.snowflake_conn import get_connection

    conn = get_connection(**LOGIN)
    try:
        before = calls.counts["sf_statement"]
        cur = conn.cursor()
        cur.execute('USE SCHEMA "PUBLIC"')
        cur.execute("SELECT CURRENT_ROLE()")
        cur.execute("SELECT CURRENT_ROLE()")
        assert _sent(calls, before) == 3
    finally:
        conn.close()
