"""D363: MCP-created connectors and one-shot MCP jobs carry the caller's workspace.

QA (Nov 16, every round): a start_transfer job was readable by id and listed
with scope=all, but the default list_jobs (the workspace list) and
status filters did not show it; MCP-created connectors had workspace_id ''
while schedules carried the workspace. 8d2b01127/1d73523ea stamp the staged
start_transfer payload and the confirm; the review gap was the list proof
and the connector.

Through the real app with authentication and workspace isolation on, a
workspace editor's MCP key (the QA path) creates a connector and runs a
one-shot transfer; both are stamped with that workspace, appear in the
workspace's default lists (and the status filter), and stay invisible to an
owner key of another workspace. Postgres on 127.0.0.1:5432, synthetic rows.
"""

from __future__ import annotations

import time
import uuid

from fastapi.testclient import TestClient

from tests.test_d380_member_job_workspace import _pg, _tool, pytestmark, world  # noqa: F401


def test_mcp_connector_and_job_land_in_and_list_in_the_caller_workspace(world):
    from services.connector_store import get_connector

    app = world["app"]
    member, owner = TestClient(app), TestClient(app)
    mine, theirs = world["member_headers"], world["owner_headers"]
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"d363_src_{suffix}", f"d363_dst_{suffix}"
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INTEGER PRIMARY KEY, note TEXT)")
            cur.execute(f"INSERT INTO {src} (id, note) VALUES (1, 'one'), (2, 'two')")
            # Keyless destination: this test is about tenancy, not append-into-keyed (D395).
            cur.execute(f"CREATE TABLE {dst} (id INTEGER, note TEXT)")

        name = f"D363PG{suffix}"
        staged = _tool(member, mine, "create_connector", {
            "name": name, "type": "postgresql", "host": "127.0.0.1", "port": 5432,
            "database": "dataflow", "username": "dataflow", "password": "dataflow",
            "schema": "public", "test_first": False,
        })
        created = _tool(member, mine, "confirm_action", {"ack_id": staged["ack_id"], "reason": "d363"})
        connector_id = str(created.get("connector_id") or "")
        assert connector_id, created
        saved = get_connector(connector_id)
        assert saved is not None and saved.workspace_id == world["ws_a"], getattr(saved, "workspace_id", None)
        listed = member.get("/api/v1/connectors/saved", headers=mine).json()["connectors"]
        assert connector_id in {c["id"] for c in listed}
        hidden = owner.get("/api/v1/connectors/saved", headers=theirs).json()["connectors"]
        assert connector_id not in {c["id"] for c in hidden}

        staged = _tool(member, mine, "start_transfer", {
            "source_connector_name": name, "source_table": src,
            "dest_connector_name": name, "dest_table": dst,
        })
        confirmed = _tool(member, mine, "confirm_action", {"ack_id": staged["ack_id"], "reason": "d363"})
        job_id = str(confirmed.get("job_id") or "")
        assert job_id
        deadline = time.time() + 90
        status = ""
        while time.time() < deadline:
            status = str((world["memory"].get_job(job_id) or {}).get("status") or "")
            if status in {"completed", "failed", "cancelled", "completed_with_quarantine"}:
                break
            time.sleep(0.5)
        assert (world["memory"].get_job(job_id) or {}).get("workspace_id") == world["ws_a"]

        default_list = _tool(member, mine, "list_jobs", {})
        ids = {str(j.get("job_id") or j.get("id") or "") for j in default_list.get("jobs") or []}
        assert job_id in ids, (status, sorted(ids)[:5])
        by_status = _tool(member, mine, "list_jobs", {"status": status})
        assert job_id in {str(j.get("job_id") or j.get("id") or "") for j in by_status.get("jobs") or []}
        rest = member.get("/api/v1/jobs", headers=mine).json()
        rest_ids = {str(j.get("job_id") or j.get("id") or j.get("_id") or "") for j in rest.get("jobs") or []}
        assert job_id in rest_ids
        other = _tool(owner, theirs, "list_jobs", {})
        assert job_id not in {str(j.get("job_id") or j.get("id") or "") for j in other.get("jobs") or []}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()
