"""QA MySQL D3 guard: a scheduled Rfam ``clan`` gets the one-off's type decision.

QA (build 8f7d246d): the scheduled ``clan`` (latin1, ``created DATETIME``,
``updated TIMESTAMP ... ON UPDATE``) was refused "lossy TIMESTAMPTZ →
TIMESTAMP" into PostgreSQL while the same table one-off passed.

Not reproducible on the integration head: this pins it. Live MySQL 8 ->
PostgreSQL, through the routes a user drives:

* MCP ``start_transfer`` (one-off) and MCP ``create_schedule`` -> Confirm ->
  scheduler beats, overwrite and incremental_append, two runs each (the second
  into the table run 1 created): every run completes and the destination
  columns are the one-off's (``created`` timestamp, ``updated`` timestamptz);
* the stored schedule mapping and a REST one-off with the *same* typed mapping
  reach the same verdict — a stored ``updated -> TIMESTAMP`` is refused by both
  with the same lossy-path reason, so a schedule never decides differently from
  a one-off given the same inputs.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(400)

_CLAN = """CREATE TABLE `{t}` (
  `clan_acc` varchar(7) NOT NULL, `id` varchar(40) DEFAULT NULL, `previous_id` tinytext,
  `description` varchar(100) DEFAULT NULL, `author` tinytext, `comment` longtext,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`clan_acc`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"""
_ROWS = [
    ("CL00001", "tRNA", None, "tRNA clan", "Gardner PP", "c1", "2011-03-02 14:30:31", "2013-10-03 20:41:44"),
    ("CL00002", "RNaseP", "", "Zoë clan", "Bateman", None, "2012-01-01 00:00:00", "2014-01-01 00:00:00"),
]
_INS = ("INSERT INTO `{t}` (clan_acc,id,previous_id,description,author,comment,created,updated) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _types(table: str) -> dict[str, str]:
    rows = h.query("postgresql", "SELECT column_name, data_type FROM information_schema.columns "
                                 f"WHERE table_name = '{table}'")  # nosec B608
    return {str(r[0]): str(r[1]) for r in rows}


def test_scheduled_clan_plans_like_the_one_off(client):
    from services.connector_store import create_connector

    my = h.mysql_conn()
    cur = my.cursor()
    token = uuid.uuid4().hex[:6]
    table = f"clan_{token}"
    dests = {"one_off": f"d3_one_{token}", "full_refresh_overwrite": f"d3_ow_{token}",
             "incremental_append": f"d3_ia_{token}", "rest": f"d3_rest_{token}"}
    try:
        cur.execute(_CLAN.format(t=table))
        cur.executemany(_INS.format(t=table), _ROWS)
        src = create_connector({"name": f"d3-my-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("mysql")})
        dst = create_connector({"name": f"d3-pg-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": table, "dest_connector_id": dst.id,
            "dest_table": dests["one_off"], "validation_mode": "strict",
            "sync_mode": "full_refresh_overwrite"})
        expected = _types(dests["one_off"])
        assert expected["created"] == "timestamp without time zone", expected
        assert expected["updated"] == "timestamp with time zone", expected

        extra_key = 3
        for mode in ("full_refresh_overwrite", "incremental_append"):
            args = {"source_connector_id": src.id, "source_table": table,
                    "dest_connector_id": dst.id, "dest_table": dests[mode], "sync_mode": mode,
                    "cadence": "every 15 minutes", "name": f"d3 {mode} {token}",
                    "validation_mode": "strict"}
            if mode == "incremental_append":
                args.update(cursor_column="updated", cursor_semantics="monotonic_sequence")
            ok, staged = h.mcp_call(client, "create_schedule", args)
            assert ok, staged
            confirmed = client.post("/api/v1/copilot/confirm",
                                    json={"ack_id": staged["ack_id"], "reason": "d3"}).json()
            assert confirmed.get("ok") is True, confirmed
            for n in (1, 2):
                if n == 2:
                    cur.execute(_INS.format(t=table), (f"CL0000{extra_key}",
                                                       # Unique: the append key probe binds
                                                       # a column named `id`, not clan_acc
                                                       # (separate finding, r14 report).
                                                       f"new{extra_key}", "   ", "Zoë ± ¿",
                                                       "x", "", "2020-01-01 00:00:00",
                                                       "2021-01-01 00:00:00"))
                    extra_key += 1
                run = _beat(confirmed["schedule_id"])
                assert run["job"].get("status") == "completed", (mode, n, run["job"].get("error"))
            got = _types(dests[mode])
            assert {k: got[k] for k in expected} == expected, (mode, got)

        # Same typed mapping, same verdict, one-off and schedule.
        cols = ["clan_acc", "id", "previous_id", "description", "author", "comment", "created", "updated"]
        typed = [{"source": c, "target": c, "confidence": 0.99} for c in cols]
        for m in typed:
            if m["source"] == "updated":
                m["target_type"] = "TIMESTAMP"
        one_off = client.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "mysql", "connector_id": src.id, "table": table},
            "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                            "table": dests["rest"], "schema": "public"},
            "mappings": typed, "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
        })
        assert one_off.status_code == 200, one_off.text
        one_job = h.wait_job(one_off.json()["job_id"])
        resp = client.post("/api/v1/schedules/", json={
            "name": f"d3 rest {token}", "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": dests["rest"], "interval": "daily",
            "sync_mode": "full_refresh_overwrite", "mappings": typed, "enabled": True})
        assert resp.status_code == 201, resp.text
        sched_job = _beat(resp.json()["id"])["job"]
        for job in (one_job, sched_job):
            assert job.get("status") == "failed", job.get("status")
            assert "updated TIMESTAMPTZ → TIMESTAMP" in str(job.get("error")), job.get("error")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        for d in dests.values():
            h.drop_table("postgresql", d)
