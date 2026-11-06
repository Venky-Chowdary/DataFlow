"""D187 (remaining): a preflight timeout names the endpoint that was slow.

QA: PG/MySQL -> Snowflake plans failed after 45 s "source unreachable/slow:
<PG tunnel> (step: running the preflight checks)" while the same source loaded
into SQL Server at the same minutes. The preflight reaches both systems, so
the step alone cannot say which one held it; the connection layer now marks
the endpoint it is talking to (``tool_deadline.waiting_on``) and the timeout
names that one.
"""

from __future__ import annotations

import time

from src.ai.copilot.tool_deadline import note_step, run_with_deadline

SOURCE = {"host": "pg.source.test", "port": 5432}
DEST = {"host": "acme-xy123.snowflakecomputing.com", "port": 443}


def _preflight(inner):
    def run():
        note_step("running the preflight checks", SOURCE, role="source", also=(("destination", DEST),))
        inner()

    return run_with_deadline("plan_transfer", run, lambda message: message, seconds=0.5)


def test_a_slow_snowflake_login_is_named_as_the_destination(monkeypatch):
    import connectors.snowflake_conn as sc

    monkeypatch.setattr(sc, "_login", lambda **_k: time.sleep(5))
    out = _preflight(lambda: sc._open_connection(account="acme-xy123", username="u", password="p",
                                                  database="D", schema="S", warehouse="W", connection_string=""))
    assert "destination unreachable/slow: acme-xy123.snowflakecomputing.com:443" in out
    assert "pg.source.test" not in out
    assert "running the preflight checks" in out


def test_a_slow_source_dial_is_named_as_the_source():
    from services.egress_guard import connector_dial_scope

    def slow_source():
        with connector_dial_scope({"host": "pg.source.test", "port": 5432}, db_type="postgresql", point="introspect"):
            time.sleep(5)

    out = _preflight(slow_source)
    assert "source unreachable/slow: pg.source.test:5432" in out
    assert "snowflakecomputing" not in out


def test_after_the_dial_returns_the_step_keeps_both_systems():
    from src.ai.copilot.tool_deadline import waiting_on

    def dial_then_slow():
        with waiting_on(DEST):
            pass
        time.sleep(5)

    out = _preflight(dial_then_slow)
    assert "pg.source.test:5432" in out and "acme-xy123.snowflakecomputing.com:443" in out
    assert "source unreachable/slow" not in out
