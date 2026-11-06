"""Fixture: an isolated, throwaway workspace for security retests (C18).

Provisioned through the real platform-admin route
(``POST /api/v1/team/sandbox-workspaces``) on the live-Mongo harness
(``r8_ent_harness``), with a synthetic tester as its only admin; torn down
through ``DELETE`` afterwards. Use it for MFA / session-timeout / IP-allowlist
retests so no real owner account or workspace is touched::

    from r8_ent_harness import ent  # noqa: F401
    from r14_sandbox_world import sandbox  # noqa: F401

    def test_something(sandbox):
        sandbox.tester.get("/api/v1/workspace/settings")
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi.testclient import TestClient


@dataclass
class Sandbox:
    ent: Any
    workspace_id: str
    tester_email: str
    tester: TestClient
    created: dict[str, Any]


def provision(ent, *, ttl_hours: int = 24, name: str = "security retest") -> Sandbox:
    tester = f"sec-tester-{uuid.uuid4().hex[:8]}@example.test"
    res = ent.admin.post(
        "/api/v1/team/sandbox-workspaces",
        json={
            "name": name,
            "tester_email": tester,
            "reason": "QA security retest (synthetic tester)",
            "ttl_hours": ttl_hours,
            "create_account": True,
        },
    )
    assert res.status_code == 200, res.text
    body = res.json()
    client = ent.login(tester, body["temporary_password"])
    client.headers["X-Workspace-Id"] = body["workspace"]["id"]
    return Sandbox(ent, body["workspace"]["id"], tester, client, body)


@pytest.fixture
def sandbox(ent):
    box = provision(ent)
    yield box
    ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params={"reason": "test teardown"})
