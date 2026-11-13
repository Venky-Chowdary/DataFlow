"""Map-step remapping routes through the real app: scoping, RBAC, deadline, safe errors.

``POST /api/v1/transfer/mapping/destination-columns`` dials a customer
destination named by a saved connector, so it is held to the same contract as
its neighbours (``/transfer/introspect``, ``/query/schema``):

* a connector of another workspace is refused (404/403) and nothing of it leaks;
* the read runs off the event loop under the connector deadline — a silent
  destination answers 504 with a fixed message while ``/api/v1/health`` answers;
* an error never returns a filesystem path or a credential;
* a viewer cannot reach it (job.plan), the learned memory is per workspace.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

BASE = "/api/v1/transfer/mapping"


def _dest_db(tmp_path: Path, workspace_id: str) -> str:
    # The SQLite connector only opens files in its own workspace's area.
    area = tmp_path / "sqlite" / "_workspaces" / workspace_id
    area.mkdir(parents=True, exist_ok=True)
    path = area / "dest-orders.sqlite"
    con = sqlite3.connect(path)
    con.execute(
        "create table orders_dest (legacy_ref bigint primary key, given_name varchar(40) not null, "
        "surname varchar(60), amount decimal(12,2) not null default 0, tenant_id bigint not null, "
        "notes text)"
    )
    con.commit()
    con.close()
    return str(path)


def _connector(workspace_id: str, database: str) -> str:
    from services import connector_store

    return connector_store.create_connector(
        {"name": f"dest-{workspace_id[:6]}", "type": "sqlite", "database": database,
         "workspace_id": workspace_id}
    ).id


def _body(connector_id: str, table: str = "orders_dest", policy: str = "") -> dict:
    return {
        "destination": {"kind": "database", "format": "sqlite", "connector_id": connector_id, "table": table},
        "schema_policy": policy,
    }


def test_destination_columns_returns_the_catalog_for_the_owner(tenancy_world):
    world = tenancy_world
    cid = _connector(world.workspace_a, _dest_db(world.data_dir, world.workspace_a))
    r = world.a.post(f"{BASE}/destination-columns", json=_body(cid))
    assert r.status_code == 200, r.text
    cat = r.json()
    by = {c["name"]: c for c in cat["columns"]}
    assert set(by) == {"legacy_ref", "given_name", "surname", "amount", "tenant_id", "notes"}
    assert by["legacy_ref"]["primary_key"] is True
    assert by["given_name"]["nullable"] is False and by["given_name"]["required"] is True
    assert by["amount"]["has_default"] is True and by["amount"]["required"] is False
    assert by["surname"]["required"] is False
    assert cat["table_exists"] is True and cat["can_add_columns"] is True


def test_destination_columns_honours_a_policy_that_forbids_add_column(tenancy_world):
    world = tenancy_world
    cid = _connector(world.workspace_a, _dest_db(world.data_dir, world.workspace_a))
    r = world.a.post(f"{BASE}/destination-columns", json=_body(cid, policy="pause_on_change"))
    assert r.status_code == 200, r.text
    assert r.json()["can_add_columns"] is False
    assert "pause_on_change" in r.json()["add_columns_reason"]


def test_destination_columns_refuses_another_workspaces_connector(tenancy_world):
    world = tenancy_world
    db = _dest_db(world.data_dir, world.workspace_a)
    cid = _connector(world.workspace_a, db)
    r = world.b.post(f"{BASE}/destination-columns", json=_body(cid))
    assert r.status_code in (403, 404), r.text
    assert "given_name" not in r.text and "orders_dest" not in r.text and db not in r.text
    # The owner is served, so B's refusal is tenancy, not a broken route.
    assert world.a.post(f"{BASE}/destination-columns", json=_body(cid)).status_code == 200


def test_destination_columns_error_carries_no_path(tenancy_world, monkeypatch):
    world = tenancy_world
    db = _dest_db(world.data_dir, world.workspace_a)
    cid = _connector(world.workspace_a, db)
    from services import mapping_dest_catalog

    def _boom(*_a, **_k):
        raise RuntimeError(f"unable to open database file {db} password=hunter2")

    monkeypatch.setattr(mapping_dest_catalog, "read_destination_column_catalog", _boom)
    r = world.a.post(f"{BASE}/destination-columns", json=_body(cid))
    assert r.status_code == 502
    assert db not in r.text and "hunter2" not in r.text and str(world.data_dir) not in r.text


def test_viewer_cannot_read_destination_columns_or_record_memory(tenancy_world):
    world = tenancy_world
    cid = _connector(world.workspace_a, _dest_db(world.data_dir, world.workspace_a))
    viewer = world.member("viewer-map@tenancy.example.com", world.workspace_a, "viewer")
    assert viewer.post(f"{BASE}/destination-columns", json=_body(cid)).status_code == 403
    assert viewer.post(f"{BASE}/memory", json={"destination": _body(cid)["destination"],
                                                "pairs": [{"source": "a", "target": "b"}]}).status_code == 403
    assert viewer.get(f"{BASE}/memory").status_code == 200


def test_memory_records_only_confirmed_pairs_onto_real_destination_columns(tenancy_world):
    """Review M4: POST /memory reads the stated destination's catalog and refuses
    pairs whose target it does not have; another workspace's connector is refused."""
    world = tenancy_world
    cid = _connector(world.workspace_a, _dest_db(world.data_dir, world.workspace_a))
    dest = _body(cid)["destination"]
    pairs = {"destination": dest, "pairs": [{"source": "cust_fname", "target": "given_name"},
                                            {"source": "x", "target": "not_a_column"}]}
    first = world.a.post(f"{BASE}/memory", json=pairs)
    assert first.status_code == 200, first.text
    assert first.json()["recorded"] == 1 and first.json()["refused"] == ["not_a_column"]
    assert world.a.post(f"{BASE}/memory", json=pairs).json()["total"] == 1
    listed = world.a.get(f"{BASE}/memory").json()["pairs"]
    assert listed[0]["count"] == 2 and listed[0]["target"] == "given_name"
    assert world.b.get(f"{BASE}/memory").json()["pairs"] == []
    assert world.b.post(f"{BASE}/memory", json=pairs).status_code in (403, 404)

    body = {
        "source_columns": [{"name": "cust_fname", "type": "VARCHAR(40)"}, {"name": "order_id", "type": "BIGINT"}],
        "destination_columns": [{"name": "given_name", "type": "VARCHAR(40)"},
                                {"name": "first_name", "type": "VARCHAR(40)"},
                                {"name": "id", "type": "INTEGER"}],
        "dest_table": "orders", "dest_type": "postgresql", "destination": dest,
    }
    mine = {s["source"]: s for s in world.a.post(f"{BASE}/auto-map", json=body).json()["suggestions"]}
    assert mine["cust_fname"]["target"] == "given_name" and mine["cust_fname"]["kind"] == "learned"
    assert mine["order_id"]["target"] == "id" and mine["order_id"]["compat"]["verdict"] == "lossy"
    theirs = {s["source"]: s for s in world.b.post(f"{BASE}/auto-map", json={**body, "destination": {"kind": "database", "table": "orders_dest"}}).json()["suggestions"]}
    assert theirs.get("cust_fname", {}).get("kind") != "learned"

    assert world.a.delete(f"{BASE}/memory").json()["removed"] == 1
    assert world.a.get(f"{BASE}/memory").json()["pairs"] == []


def test_memory_post_is_capped(tenancy_world):
    world = tenancy_world
    cid = _connector(world.workspace_a, _dest_db(world.data_dir, world.workspace_a))
    too_many = {"destination": _body(cid)["destination"],
                "pairs": [{"source": f"s{i}", "target": "given_name"} for i in range(201)]}
    assert world.a.post(f"{BASE}/memory", json=too_many).status_code == 422


def test_auto_map_never_suggests_an_unwritable_destination_column(tenancy_world):
    body = {
        "source_columns": [{"name": "total", "type": "DECIMAL(12,2)"}],
        "destination_columns": [{"name": "total", "type": "DECIMAL(12,2)", "writable": False},
                                {"name": "amount", "type": "DECIMAL(12,2)"}],
    }
    out = tenancy_world.a.post(f"{BASE}/auto-map", json=body).json()["suggestions"]
    assert [(s["source"], s["target"]) for s in out] == [("total", "amount")]


def test_type_compatibility_grades_with_validate_rules(tenancy_world):
    r = tenancy_world.a.post(f"{BASE}/type-compatibility", json={
        "dest_type": "postgresql",
        "pairs": [
            {"source_type": "INTEGER", "target_type": "BIGINT"},
            {"source_type": "VARCHAR(255)", "target_type": "VARCHAR(50)"},
            {"source_type": "BLOB", "target_type": "INTEGER"},
        ],
    })
    assert r.status_code == 200, r.text
    assert [x["verdict"] for x in r.json()["results"]] == ["safe", "lossy", "incompatible"]


# --------------------------------------------------------------------------- off the event loop


def test_a_silent_destination_answers_504_while_the_api_stays_up(monkeypatch, tmp_path):
    """The catalog read is bounded by the connector deadline and never holds the loop."""
    httpx = pytest.importorskip("httpx")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "1")
    from fastapi import FastAPI

    from services import connector_executor, mapping_dest_catalog
    from src.routers import mapping_studio_router

    connector_executor.reset_for_tests()
    release = threading.Event()

    def _hang(*_a, **_k):
        release.wait(6)
        return {}

    monkeypatch.setattr(mapping_dest_catalog, "read_destination_column_catalog", _hang)
    app = FastAPI()
    app.include_router(mapping_studio_router.router, prefix="/api/v1")

    @app.get("/api/v1/health")
    async def health():
        return {"status": "ok"}

    async def main() -> tuple[int, str, float, float]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            started = time.monotonic()
            slow = asyncio.create_task(client.post(
                f"{BASE}/destination-columns",
                json={"destination": {"kind": "database", "format": "postgresql",
                                      "host": "10.255.255.1", "port": 5432, "password": "s3cr3t",
                                      "table": "t"}},
            ))
            await asyncio.sleep(0.2)
            t0 = time.monotonic()
            health = await client.get("/api/v1/health")
            health_latency = time.monotonic() - t0
            assert health.status_code == 200
            resp = await slow
            return resp.status_code, resp.text, health_latency, time.monotonic() - started

    try:
        status, text, health_latency, total = asyncio.run(main())
    finally:
        release.set()
        connector_executor.reset_for_tests()
    assert health_latency < 0.5, health_latency
    assert status == 504 and total < 4, (status, total)
    assert "10.255.255.1" not in text and "s3cr3t" not in text
