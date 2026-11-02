"""Round-11 harness: the real app with every control-plane store on live MongoDB.

Builds on ``r8_ent_harness.ent`` (one throwaway database on the local MongoDB
behind ``get_mongodb_service``), so schedules, jobs, notification deliveries
and the audit chain are written and read back as BSON exactly as production
does. QA's earlier repro of these defects ran on the in-memory store, which
never decodes ``Int64``; this harness does.

Saved connectors stay file-backed (as in the single-node deployment), and
connectors dial the local PostgreSQL from ``PG*`` (see AGENTS.md).

Use::

    from r8_ent_harness import ent  # noqa: F401
    from r11_live_world import live  # noqa: F401
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests import r4_route_harness as h


@dataclass
class Live:
    ent: Any
    db: Any
    workspace_id: str
    admin: TestClient
    tmp: Any

    def pg_connector(self, name: str = "", **overrides: Any) -> str:
        """A saved PostgreSQL connector created through the REST route."""
        body = {
            "name": name or f"r11-pg-{uuid.uuid4().hex[:6]}",
            "type": "postgresql",
            "role": "both",
            "host": h.PG["host"],
            "port": h.PG["port"],
            "database": h.PG["database"],
            "username": h.PG["username"],
            "password": h.PG["password"],
            "schema": "public",
            "ssl": False,
        }
        body.update(overrides)
        res = self.admin.post("/api/v1/connectors/saved", json=body)
        assert res.status_code == 200, res.text
        return res.json()["id"]

    def schedule_doc(self, schedule_id: str) -> dict[str, Any]:
        doc = self.db["pipeline_schedules"].find_one({"_id": schedule_id})
        assert doc is not None, schedule_id
        return doc


def pg_table(columns_sql: str = "id BIGINT PRIMARY KEY, name TEXT", *, name: str = "") -> str:
    """A PG source table holding NULL, '', whitespace and unicode values."""
    table = name or "r11_" + uuid.uuid4().hex[:8]
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{table}" ({columns_sql})')  # nosec B608
        if columns_sql.startswith("id BIGINT PRIMARY KEY, name TEXT"):
            cur.executemany(
                f'INSERT INTO "{table}" (id, name) VALUES (%s, %s)',  # nosec B608
                [(1, "Ada"), (2, None), (3, ""), (4, "   "), (5, "naïve 中文 ✓")],
            )
    finally:
        conn.close()
    return table


def drop_pg(*tables: str) -> None:
    for table in tables:
        try:
            h.execute("postgresql", f'DROP TABLE IF EXISTS "{table}"')  # nosec B608
        except Exception:  # pragma: no cover - best-effort cleanup
            pass


@pytest.fixture
def live(ent, monkeypatch, tmp_path):
    h.pg_conn().close()  # skips when PostgreSQL is not configured
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")

    import services.connector_store as connector_store
    import services.schedule_store as schedule_store

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    # A fresh database has no unique name index yet.
    monkeypatch.setattr(schedule_store, "_NAME_INDEX_READY", False)
    assert schedule_store._mongo_backend() is not None, "schedules must live on MongoDB"

    ws = ent.workspace("R11 schedules")
    admin = ent.member(email=f"r11-admin-{uuid.uuid4().hex[:6]}@example.com", workspace_id=ws, role="admin")
    yield Live(ent=ent, db=ent.db, workspace_id=ws, admin=admin, tmp=tmp_path)
    # A finalize callback still running on a job thread must land before the
    # next test swaps the stores.
    time.sleep(0.2)
