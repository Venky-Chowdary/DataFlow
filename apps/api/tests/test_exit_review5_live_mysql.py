"""Exit rehearsal on live MySQL: a single-precision FLOAT is never proven (review 5, C-2).

MySQL sends a ``FLOAT`` rounded to its six display digits: the stored
``0.10000000149011612`` and ``0.10000000894069672`` both read as ``0.1`` and
``16777216`` reads as ``16777200``, so the extract cannot hold the stored
value. Introspection reports the column as ``FLOAT32``; the rehearsal must name
it whatever connector type the server was saved under. Skipped when no MySQL is
reachable.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import uuid

import pytest

from tests.test_exit_rehearsal_e2e import _run, world  # noqa: F401

pymysql = pytest.importorskip("pymysql")


@pytest.fixture()
def mysql_table(world, monkeypatch):  # noqa: F811
    from services import audit_log
    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    try:
        conn = pymysql.connect(host=c["host"], port=c["port"], user=c["username"], password=c["password"],
                               database=c["database"], autocommit=True, connect_timeout=3)
    except pymysql.Error as exc:
        pytest.skip(f"MySQL not reachable for the live rehearsal test: {type(exc).__name__}")
    monkeypatch.setattr(audit_log, "STORE_PATH", world["tmp"] / "audit.jsonl")
    table = f"r5fl_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, f FLOAT, d DOUBLE)")  # nosec B608 - test name
        cur.execute(f"INSERT INTO {table} VALUES (1, 0.1, 0.5), (2, 0.10000001, 0.25), (3, 16777217, 1.5)")  # nosec B608
    try:
        yield c, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        conn.close()


@pytest.mark.parametrize("ctype", ["mysql", "amazon_rds_mysql"])
def test_c2_a_mysql_float_whose_stored_value_the_driver_rounds_is_named_never_proven(mysql_table, monkeypatch,
                                                                                    ctype):
    from services import schedule_runner
    from services.exit_inventory import declare

    c, table = mysql_table
    saved = {"name": "my", "type": ctype, "host": c["host"], "port": c["port"], "database": c["database"],
             "username": c["username"], "password": c["password"]}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _c, _s: dict(saved))
    system = declare({"name": f"review 5 {ctype}", "source": {"connector_id": "my"}, "extract_format": "jsonl",
                      "tables": [{"name": table}], "exit_window_hours": 24},
                     workspace_id="ws1", actor="owner@example.com")
    out = _run(system)["tables"][0]
    assert out["status"] == "not_proven", (out["status"], out["reason"])
    assert "f is a single-precision FLOAT" in out["reason"], out["reason"]
    assert " d " not in out["reason"] and "d is" not in out["reason"], out["reason"]
