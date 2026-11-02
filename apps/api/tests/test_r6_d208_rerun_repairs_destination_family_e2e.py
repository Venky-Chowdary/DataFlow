"""D208 (verification across engines) — an upsert re-run repairs the destination.

QA (production 557f556c): "an upsert rerun doesn't repair changed or deleted
rows". The D208 fix (a re-run or retry of an incremental upsert is a full pass
that re-reads the whole source and upserts every row) is pinned for
PostgreSQL -> PostgreSQL in ``test_r6_d208_upsert_full_pass_e2e.py``. This file
holds QA's exact meaning on the other routes QA named, PostgreSQL -> MySQL and
MongoDB -> MariaDB (and PostgreSQL -> PostgreSQL for the source-side case):

* after run 1, rows are edited and deleted **in the destination**, and a row is
  added that exists only there; a re-run restores every destination row whose
  source row still exists (re-inserting the deleted one), keeps the
  destination-only row, and the job's ``read_scope`` says so;
* rows are edited **in the source** without moving the cursor, and one source
  row is deleted; the re-run picks up the edit, and the row the source dropped
  stays in the destination, counted as destination-only.

Both re-run routes: MCP ``rerun_job`` -> Confirm, and REST
``/api/v1/connectors/jobs/{id}/retry``. NULL, '', whitespace and unicode ride
on every row set.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_ROWS = [(1, "a"), (2, None), (3, ""), (4, "  "), (5, "Łukasz 山田 🎉"), (6, "f")]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #


def _mariadb_cfg() -> dict[str, Any]:
    pw = os.environ.get("DATAFLOW_TEST_MARIADB_PASSWORD", "")
    port = int(os.environ.get("DATAFLOW_TEST_MARIADB_PORT", "3307"))
    if not pw or not h.port_up(port):
        pytest.skip("MariaDB not configured (DATAFLOW_TEST_MARIADB_*, :3307)")
    return dict(type="mariadb", host="127.0.0.1", port=port,
                database=os.environ.get("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
                username=os.environ.get("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
                password=pw)


def _cfg(kind: str) -> dict[str, Any]:
    if kind == "mariadb":
        return _mariadb_cfg()
    if kind == "mongodb":
        if not h.port_up(27017):
            pytest.skip("MongoDB not reachable on :27017")
        return dict(type="mongodb", host="127.0.0.1", port=27017, database="dataflow_test")
    return h.dest_config(kind)


def _sql(kind: str, sql: str, fetch: bool = False) -> list[tuple]:
    if kind == "mariadb":
        import pymysql

        c = _mariadb_cfg()
        conn = pymysql.connect(host=c["host"], port=c["port"], user=c["username"],
                               password=c["password"], database=c["database"],
                               charset="utf8mb4", autocommit=True)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if fetch else []
        finally:
            conn.close()
    if fetch:
        return h.query(kind, sql)
    h.execute(kind, sql)
    return []


def _q(kind: str, name: str) -> str:
    return f"`{name}`" if kind in {"mysql", "mariadb"} else h.quote(kind, name)


def _lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def _mongo_coll(name: str):
    pymongo = pytest.importorskip("pymongo")
    return pymongo.MongoClient("mongodb://127.0.0.1:27017",
                               serverSelectionTimeoutMS=3000)["dataflow_test"][name]


class _Source:
    """The source table/collection: create, edit without the cursor, delete."""

    def __init__(self, kind: str, name: str):
        self.kind, self.name = kind, name
        self.key = "_id" if kind == "mongodb" else "id"

    def create(self) -> None:
        if self.kind == "mongodb":
            _mongo_coll(self.name).insert_many([
                {"_id": rid, "note": note, "updated_at": datetime(2026, 10, 1, 0, 0, rid)}
                for rid, note in _ROWS
            ])
            return
        _sql(self.kind, f"CREATE TABLE {_q(self.kind, self.name)} (id INT PRIMARY KEY, "
                        "note TEXT NULL, updated_at TIMESTAMP NOT NULL)")
        for rid, note in _ROWS:
            _sql(self.kind, f"INSERT INTO {_q(self.kind, self.name)} VALUES "
                            f"({rid}, {_lit(note)}, '2026-10-01 00:00:{rid:02d}')")

    def edit_without_cursor(self, rid: int, note: str) -> None:
        if self.kind == "mongodb":
            _mongo_coll(self.name).update_one({"_id": rid}, {"$set": {"note": note}})
            return
        _sql(self.kind, f"UPDATE {_q(self.kind, self.name)} SET note = {_lit(note)} "
                        f"WHERE id = {rid}")

    def delete(self, rid: int) -> None:
        if self.kind == "mongodb":
            _mongo_coll(self.name).delete_one({"_id": rid})
            return
        _sql(self.kind, f"DELETE FROM {_q(self.kind, self.name)} WHERE id = {rid}")

    def drop(self) -> None:
        if self.kind == "mongodb":
            _mongo_coll(self.name).drop()
            return
        _drop(self.kind, self.name)


def _drop(kind: str, table: str) -> None:
    try:
        _sql(kind, f"DROP TABLE IF EXISTS {_q(kind, table)}")
    except Exception:  # pragma: no cover - best-effort cleanup
        pass


def _dest_rows(kind: str, table: str, key: str) -> list[tuple]:
    rows = _sql(kind, f"SELECT {_q(kind, key)}, note FROM {_q(kind, table)}", fetch=True)
    return sorted(((int(r[0]), r[1]) for r in rows), key=lambda r: r[0])


def _tamper_destination(kind: str, table: str, key: str) -> None:
    t, k = _q(kind, table), _q(kind, key)
    _sql(kind, f"UPDATE {t} SET note = 'edited by hand' WHERE {k} = 1")
    _sql(kind, f"UPDATE {t} SET note = NULL WHERE {k} = 3")
    _sql(kind, f"DELETE FROM {t} WHERE {k} IN (2, 5)")
    _sql(kind, f"INSERT INTO {t} ({k}, note, updated_at) "
               "VALUES (99, 'only in destination', '2026-10-01 00:00:00')")


def _jid(job: dict) -> str:
    return str(job.get("job_id") or job.get("_id") or job.get("id") or "")


def _rerun(client, job: dict, action: str) -> dict:
    if action == "rerun_mcp":
        ok, staged = h.mcp_call(client, "rerun_job", {"job_id": _jid(job)})
        assert ok, staged
        resp = client.post("/api/v1/copilot/confirm",
                           json={"ack_id": staged["ack_id"], "reason": "d208"})
        assert resp.status_code == 200, resp.text
        new_id = str(resp.json().get("job_id") or "")
        if not new_id:
            from services.mongodb_service import get_mongodb_service

            jobs = [j for j in get_mongodb_service().list_jobs(limit=50)
                    if j.get("rerun_of") == _jid(job)]
            new_id = str(jobs[0]["job_id"]) if jobs else ""
    else:
        from services.mongodb_service import get_mongodb_service

        # QA's retry path: the same job record, left failed by an attempt.
        record = get_mongodb_service()._jobs[_jid(job)]
        record["status"] = "failed"
        record["error"] = "d208 probe: attempt failed after commit"
        resp = client.post(f"/api/v1/connectors/jobs/{_jid(job)}/retry")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["retry_decision"].get("read_scope") == "full_source", body
        new_id = str(body["job_id"])
    rerun = h.wait_job(new_id)
    assert str(rerun.get("status")) == "completed", (rerun.get("status"), rerun.get("error"))
    return rerun


def _assert_report(job: dict, *, rows_read: int, dest_only: int) -> None:
    scope = job.get("read_scope")
    assert isinstance(scope, dict), {k: job.get(k) for k in ("message", "incremental_watermark")}
    assert scope.get("mode") == "upsert", scope
    assert scope.get("scope") == "full_source", scope
    assert scope.get("watermark") == "ignored", scope
    assert int(scope.get("rows_read")) == rows_read, scope
    assert "kept" in str(scope.get("destination_only_rows")), scope
    assert scope.get("destination_only_count") in (None, dest_only), scope


_ROUTES = [("postgresql", "postgresql"), ("postgresql", "mysql"), ("mongodb", "mariadb")]


@pytest.mark.parametrize("side", ["destination", "source"])
@pytest.mark.parametrize("action", ["rerun_mcp", "retry_rest"])
@pytest.mark.parametrize("route", _ROUTES, ids=["pg-pg", "pg-mysql", "mongo-mariadb"])
def test_upsert_rerun_repairs_the_destination(client, route, action, side):
    src_kind, dst_kind = route
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    source = _Source(src_kind, f"d208f_s_{token}")
    dst_t = f"d208f_d_{token}"
    src = create_connector({"name": f"d208f-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **_cfg(src_kind)})
    dst = create_connector({"name": f"d208f-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **_cfg(dst_kind)})
    try:
        source.create()
        j1 = h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": source.name,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "incremental_upsert", "primary_key": source.key,
            "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
        })
        assert _dest_rows(dst_kind, dst_t, source.key) == sorted(_ROWS)

        if side == "destination":
            _tamper_destination(dst_kind, dst_t, source.key)
            expected = sorted(_ROWS + [(99, "only in destination")])
            rows_read, dest_only = len(_ROWS), 1
        else:
            # Edits that leave updated_at alone (a watermark would skip them),
            # and a row the source no longer has.
            source.edit_without_cursor(6, "src edit ✓")
            source.edit_without_cursor(2, "")
            source.delete(4)
            expected = sorted([r for r in _ROWS if r[0] not in (2, 6)]
                              + [(2, ""), (6, "src edit ✓")])
            rows_read, dest_only = len(_ROWS) - 1, 1

        rerun = _rerun(client, j1, action)
        assert _dest_rows(dst_kind, dst_t, source.key) == expected
        _assert_report(rerun, rows_read=rows_read, dest_only=dest_only)
    finally:
        source.drop()
        _drop(dst_kind, dst_t)
