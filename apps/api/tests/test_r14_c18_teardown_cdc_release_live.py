"""C18 S7: a workspace teardown releases its CDC capture through the scoped release path.

The teardown calls ``cdc_capture_release.release_schedule_cdc_capture`` (the
cancel-cdc agent's path, scoped in 80e16c405 "core-C11 review 2") for every
schedule of the deleted workspace, before its connectors go. Live: a
PostgreSQL with ``wal_level=logical`` on its own port (``R11_PG_LOGICAL_*``),
the real app on MongoDB, the CDC run through the real engine.

* workspace A's CDC route has a slot + publication + watermark; deleting A
  drops A's slot and publication and clears its watermark;
* workspace C runs its own CDC route on the same server: its slot,
  publication and watermark stay.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from r8_ent_harness import ent  # noqa: F401

pytestmark = pytest.mark.timeout(600)

_SRC = {
    "host": os.getenv("R11_PG_LOGICAL_HOST", "127.0.0.1"),
    "port": int(os.getenv("R11_PG_LOGICAL_PORT", "55433")),
    "database": os.getenv("R11_PG_LOGICAL_DATABASE", "dataflow"),
    "username": os.getenv("R11_PG_LOGICAL_USER", "dataflow"),
    "password": os.getenv("R11_PG_LOGICAL_PASSWORD", ""),
}


def _sql(sql, params=None, fetch=False):
    import psycopg2

    conn = psycopg2.connect(host=_SRC["host"], port=_SRC["port"], dbname=_SRC["database"],
                            user=_SRC["username"], password=_SRC["password"], connect_timeout=3)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


def _logical() -> bool:
    try:
        socket.create_connection((_SRC["host"], _SRC["port"]), timeout=1).close()
        return _sql("SHOW wal_level", fetch=True)[0][0] == "logical"
    except Exception:  # noqa: BLE001
        return False


pytestmark = [pytestmark, pytest.mark.skipif(not _logical(), reason="needs wal_level=logical (R11_PG_LOGICAL_*)")]


def _slots() -> set[str]:
    return {r[0] for r in _sql("SELECT slot_name FROM pg_replication_slots", fetch=True)}


def _pubs() -> set[str]:
    return {r[0] for r in _sql("SELECT pubname FROM pg_publication", fetch=True)}


def _name(workspace_id: str) -> str:
    from services import team_store

    return team_store.get_workspace(workspace_id).name


class Route:
    """One workspace's CDC route: source table, connectors, a run, a schedule."""

    def __init__(self, ent, label: str):
        self.ent = ent
        self.token = uuid.uuid4().hex[:8]
        self.ws = ent.workspace(f"S7 {label} {self.token}")
        self.admin = ent.member(email=f"s7{label}-{self.token}@example.com", workspace_id=self.ws, role="admin")
        self.table = f"r14s7_{label}_{self.token}"
        _sql(f'CREATE TABLE "{self.table}" (id INT PRIMARY KEY, name TEXT)')
        _sql(f'INSERT INTO "{self.table}" VALUES (1, \'a\'), (2, NULL), (3, \'\'), (4, \'Zoë ✓\')')
        fields = {"host": _SRC["host"], "port": _SRC["port"], "database": _SRC["database"],
                  "username": _SRC["username"], "password": _SRC["password"]}
        self.src = self._connector("src", fields)
        self.dst = self._connector("dst", fields)

    def _connector(self, name, fields):
        res = self.admin.post("/api/v1/connectors/saved", json={
            "name": f"{name}-{self.token}", "type": "postgresql", "schema": "public", "ssl": False, **fields})
        assert res.status_code in (200, 201), res.text
        out = res.json()
        return str(out.get("id") or (out.get("connector") or {}).get("id"))

    def run_and_schedule(self) -> tuple[str, str]:
        from services.mongodb_service import get_mongodb_service
        from services.schedule_store import create_schedule
        from src.transfer.engine import UniversalTransferEngine
        from src.transfer.models import EndpointConfig, TransferRequest

        contract = [{"name": self.table, "selected": True, "primary_key": ["id"],
                     "sync_mode": "cdc", "cursor_semantics": "cdc_position"}]
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", schema="public",
                                  table=self.table, connector_id=self.src),
            destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                       table=f"{self.table}_dst", connector_id=self.dst),
            sync_mode="cdc", stream_contracts=contract, skip_preflight=True,
            validation_mode="strict", workspace_id=self.ws,
        )
        job_id = "r14s7" + uuid.uuid4().hex[:12]
        get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        sched = create_schedule({
            "name": f"s7-{self.token}", "source_connector_id": self.src, "source_table": self.table,
            "dest_connector_id": self.dst, "dest_table": f"{self.table}_dst", "interval": "hourly",
            "sync_mode": "cdc", "enabled": False, "last_job_id": job_id, "workspace_id": self.ws,
            "stream_contracts": contract,
        })
        return job_id, sched.id

    def drop(self):
        _sql(f'DROP TABLE IF EXISTS "{self.table}"')
        _sql(f'DROP TABLE IF EXISTS "{self.table}_dst"')


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    before_slots, before_pubs = _slots(), _pubs()
    yield before_slots, before_pubs
    for name in _slots() - before_slots:
        try:
            _sql("SELECT pg_drop_replication_slot(%s)", (name,))
        except Exception:  # noqa: BLE001
            pass
    for pub in _pubs() - before_pubs:
        _sql(f'DROP PUBLICATION IF EXISTS "{pub}"')


def test_teardown_releases_the_deleted_workspaces_capture_and_keeps_workspace_c(ent, server):
    from services.cdc_capture_inventory import list_registered_captures
    from services.sync_cursor import get_watermark

    before_slots, before_pubs = server
    a, c = Route(ent, "a"), Route(ent, "c")
    try:
        a.run_and_schedule()
        a_slots = _slots() - before_slots
        a_pubs = _pubs() - before_pubs
        assert a_slots, "the CDC run created no slot"
        c.run_and_schedule()
        c_slots = _slots() - before_slots - a_slots
        c_pubs = _pubs() - before_pubs - a_pubs
        assert c_slots
        entries = {e["slot_name"]: e for e in list_registered_captures()}
        a_keys = [entries[s]["cursor_key"] for s in a_slots if s in entries]
        c_keys = [entries[s]["cursor_key"] for s in c_slots if s in entries]
        assert all(get_watermark(k) is not None for k in a_keys + c_keys)

        # The admin is looking at workspace C while deleting A (B1's shape).
        gone = ent.admin.delete(f"/api/v1/team/workspaces/{a.ws}", params={"confirm": _name(a.ws), "reason": "S7 live"},
                                headers={"X-Workspace-Id": c.ws})
        assert gone.status_code == 200, gone.text
        assert gone.json()["schedules_removed"] == 1

        remaining = _slots()
        assert not (a_slots & remaining), f"A's slot survived the teardown: {a_slots & remaining}"
        assert not (a_pubs & _pubs()), "A's publication survived the teardown"
        assert all(get_watermark(k) is None for k in a_keys), "A's watermark survived"
        # Workspace C's capture on the same server is untouched.
        assert c_slots <= remaining and c_pubs <= _pubs()
        assert all(get_watermark(k) is not None for k in c_keys)
    finally:
        a.drop()
        c.drop()
