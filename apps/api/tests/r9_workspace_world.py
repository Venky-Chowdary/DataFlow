"""A real two-tenant deployment on the live local MongoDB for the round-9 tests.

Built on ``r8_security_world`` (auth on, isolation enforced, bootstrap platform
admin ``root@example.test``, the Acme owner as a stored account) with every
metadata store that matters here on one throwaway Mongo database: teams, users,
API keys, the job store (``transfer_jobs``) and the CDC snapshot signals. A
second tenant ("Bravo") is provisioned the way self-serve signup does it, so it
is exactly the "any new signup" QA describes. Skipped when Mongo is down.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import (  # noqa: E402
    OWNER,
    PASSWORD,
    ROOT,
    World,
    build_world,
)

BRAVO = "bravo-signup@example.test"


@dataclass
class R9:
    world: World
    bravo_workspace_id: str

    @property
    def app(self) -> Any:
        return self.world.app

    @property
    def acme(self) -> str:
        return self.world.workspace_id

    def login(self, email: str, password: str, workspace_id: str | None = None) -> TestClient:
        c = self.world.login(email, password)
        if workspace_id is None:
            c.headers.pop("X-Workspace-Id", None)
        else:
            c.headers["X-Workspace-Id"] = workspace_id
        return c

    @property
    def owner(self) -> TestClient:
        return self.world.owner

    @property
    def root(self) -> TestClient:
        return self.world.root

    def bravo(self) -> TestClient:
        return self.login(BRAVO, PASSWORD, self.bravo_workspace_id)

    @property
    def db(self) -> Any:
        return self.world.db


def _provision_signup(email: str) -> str:
    from services import signup_service
    from services.password_hash import hash_password_argon2

    _user, _org, workspace = signup_service._provision_verified_user({
        "email": email,
        "name": email.split("@")[0],
        "password_hash": hash_password_argon2(PASSWORD),
        "canonical_email": email,
    })
    return workspace.id


@pytest.fixture
def r9(tmp_path, monkeypatch):
    world, client = build_world("mongo", tmp_path, monkeypatch)
    from services import (
        cdc_incremental_snapshot,
        connector_store,
        mongodb_service,
        notification_store,
        schedule_store,
    )

    uri = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    jobs = mongodb_service.MongoDBService(uri)
    jobs.db_name = world.db.name
    assert jobs.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", jobs)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(notification_store, "STORE_PATH", tmp_path / "notifications.json")
    monkeypatch.delenv("DATAFLOW_NOTIFICATION_STORE", raising=False)
    monkeypatch.delenv("DATAWRAP_NOTIFICATION_STORE", raising=False)
    monkeypatch.setattr(cdc_incremental_snapshot, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        cdc_incremental_snapshot, "_PATH", str(tmp_path / "cdc_incremental_snapshots.json")
    )
    bravo_ws = _provision_signup(BRAVO)
    try:
        yield R9(world=world, bravo_workspace_id=bravo_ws)
    finally:
        jobs.disconnect()
        client.drop_database(world.db.name)
        client.close()
        from services import team_store

        team_store._PREPARED_DATABASES.discard(world.db.name)


__all__ = ["BRAVO", "OWNER", "PASSWORD", "R9", "ROOT", "r9"]
