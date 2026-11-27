"""D380: a workspace member's Pilot transfer belongs to that workspace.

start_transfer plus confirm writes the rows. The job, the trust report and
the proof pack are then readable with the member's key and invisible to an
owner key of another workspace.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient


def _pg_up() -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _pg_up(), reason="Postgres is not reachable")


@pytest.fixture
def world(tmp_path, monkeypatch):
    from services import connector_store, mongodb_service, team_store, user_store
    from services.auth_rate_limit import reset_auth_rate_limits
    from src.main import app

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_SIGNUP_ENABLED", "0")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-d380@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "d380-test-secret")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    memory = mongodb_service.MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", memory)
    reset_auth_rate_limits()

    admin = TestClient(app)
    login = admin.post(
        "/api/v1/auth/login",
        json={"email": "root-d380@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    admin.headers["Authorization"] = f"Bearer {login.json()['token']}"
    tag = uuid.uuid4().hex[:8]
    ws_a = admin.post("/api/v1/team/workspaces", json={"name": f"Member-{tag}"}).json()["workspace"]["id"]
    ws_b = admin.post("/api/v1/team/workspaces", json={"name": f"Owner-{tag}"}).json()["workspace"]["id"]

    def _user(email: str, workspace_id: str, role: str) -> TestClient:
        created = admin.post(
            "/api/v1/team/users",
            json={
                "email": email,
                "platform_role": "member",
                "workspace_id": workspace_id,
                "workspace_role": role,
            },
        )
        assert created.status_code == 200, created.text
        client = TestClient(app)
        signed = client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": created.json()["temporary_password"]},
        )
        assert signed.status_code == 200, signed.text
        client.headers["Authorization"] = f"Bearer {signed.json()['token']}"
        client.headers["X-Workspace-Id"] = workspace_id
        return client

    member = _user(f"member-{tag}@example.com", ws_a, "editor")
    owner = _user(f"owner-{tag}@example.com", ws_b, "admin")

    def _key(workspace_id: str, role: str) -> dict[str, str]:
        # The platform admin mints the key inside that workspace. The key
        # then acts only there, which is how a member key and an owner key
        # of another workspace are issued.
        minted = admin.post(
            "/api/v1/workspace/api-keys",
            headers={"X-Workspace-Id": workspace_id},
            json={"name": f"key-{role}-{workspace_id[:8]}", "role": role, "expires_in": "7d"},
        )
        assert minted.status_code == 200, minted.text
        assert minted.json()["workspace_id"] == workspace_id
        return {"Authorization": f"Bearer {minted.json()['key']}"}

    return {
        "app": app,
        "memory": memory,
        "ws_a": ws_a,
        "ws_b": ws_b,
        "member": member,
        "owner": owner,
        "member_headers": _key(ws_a, "editor"),
        "owner_headers": _key(ws_b, "admin"),
    }


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    return conn


def _tool(client: TestClient, headers: dict[str, str], name: str, arguments: dict) -> dict:
    response = client.post(
        "/api/v1/mcp/tools/call",
        headers=headers,
        json={"name": name, "arguments": arguments},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("success") is True, body
    return body.get("output") or {}


def test_member_transfer_is_visible_only_in_the_caller_workspace(world):
    """Staging can lose the workspace stamp (the connector executor's worker).

    Confirm still has the member's request. The job has to land in that
    workspace, not in the unscoped bucket an owner key can read.
    """
    import contextvars

    from src.ai.copilot.tools import DataPilotTools
    app = world["app"]
    member = TestClient(app)
    owner = TestClient(app)
    suffix = uuid.uuid4().hex[:8]
    src = f"d380_src_{suffix}"
    dst = f"d380_dst_{suffix}"
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INTEGER PRIMARY KEY, note TEXT)")
            cur.execute(f"INSERT INTO {src} (id, note) VALUES (1, 'one'), (2, 'two')")
            cur.execute(f"CREATE TABLE {dst} (id INTEGER PRIMARY KEY, note TEXT)")
        saved = member.post(
            "/api/v1/connectors/saved",
            headers=world["member_headers"],
            json={
                "name": f"D380PG{suffix}",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": 5432,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
                "schema": "public",
                "ssl": False,
            },
        )
        assert saved.status_code == 200, saved.text
        name = saved.json().get("name") or f"D380PG{suffix}"
        # An empty context is what the worker has when it does not inherit the
        # request stamp. Confirm is the member's own call and must repair it.
        staged = contextvars.Context().run(
            lambda: DataPilotTools().execute("start_transfer", {
                "source_connector_name": name,
                "source_table": src,
                "dest_connector_name": name,
                "dest_table": dst,
            })
        )
        assert staged.success, staged.error
        staged = staged.output
        assert staged.get("ack_id")
        confirmed = _tool(member, world["member_headers"], "confirm_action", {
            "ack_id": staged["ack_id"],
            "reason": "d380",
        })
        job_id = str(confirmed.get("job_id") or "")
        assert job_id
        stored = world["memory"].get_job(job_id) or {}
        assert stored.get("workspace_id") == world["ws_a"], stored.get("workspace_id")

        import time
        deadline = time.time() + 90
        status = ""
        while time.time() < deadline:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {dst}")
                if int(cur.fetchone()[0]) == 2:
                    break
            time.sleep(0.5)
        else:
            pytest.fail(f"job {job_id} did not write both rows; status={status}")

        for path in (
            f"/api/v1/jobs/{job_id}",
            f"/api/v1/jobs/{job_id}/parity-report",
            f"/api/v1/transfer/{job_id}/proof-pack",
        ):
            visible = member.get(path, headers=world["member_headers"])
            assert visible.status_code != 404, (path, visible.status_code, visible.text[:300])
            hidden = owner.get(path, headers=world["owner_headers"])
            assert hidden.status_code == 404, (path, hidden.status_code, hidden.text[:300])
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()


# These flows confirm through MCP confirm_action: the workspace opts in to the
# agent confirm policy (services.mcp_confirm_policy; default is interactive).
pytestmark = [pytestmark, pytest.mark.usefixtures("mcp_confirm_agent_mode")]
