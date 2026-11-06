"""C11 independent review: the capture cleanup never touches a slot it does not own.

Reproduced by the reviewer on a live PostgreSQL 16 with ``wal_level=logical``
against 830c47ae7 / 52268a5b1, each now a regression test here:

* B1 — a CDC schedule on connector X uses slot S; a second connector Y on the
  same server (same workspace, or another workspace) listed S as orphaned and
  ``cleanup`` from Y dropped it and cleared X's watermark; the other
  workspace's listing leaked the slot, its tables, cursor key and job.
* B2 — once the schedule's source table was dropped, its live slot became
  cleanable (``source_table_gone`` was checked before ``in_use``).
* S3 — a slot recorded before the registry existed has no publication on
  record, so its live ``df_pub_*`` read as an orphaned publication and was
  dropped with ``include_unregistered``.
* S4 — a one-shot CDC job still running looked orphaned and a default cleanup
  dropped its slot (and a publication recorded before its slot existed).
* S8 — the once-per-state alert was not atomic across replicas and a failed
  delivery was never retried.
* nits — the grace period counts from the moment auto-drop was enabled; a
  drop of an already-absent slot is not reported (or audited) as a drop.

Live: the real app on MongoDB with real auth (``r8_ent_harness``), connectors
created through REST in their workspaces, the CDC run through the real engine
from a PostgreSQL with logical decoding (``R11_PG_LOGICAL_*``) into ``PG*``.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_SRC = {
    "host": os.getenv("R11_PG_LOGICAL_HOST", os.getenv("PGHOST", "127.0.0.1")),
    "port": int(os.getenv("R11_PG_LOGICAL_PORT", os.getenv("PGPORT", "5432"))),
    "database": os.getenv("R11_PG_LOGICAL_DATABASE", os.getenv("PGDATABASE", "dataflow")),
    "username": os.getenv("R11_PG_LOGICAL_USER", os.getenv("PGUSER", "dataflow")),
    "password": os.getenv("R11_PG_LOGICAL_PASSWORD", os.getenv("PGPASSWORD", "")),
}


def _sql(sql: str, params=None, fetch: bool = False):
    import psycopg2

    conn = psycopg2.connect(
        host=_SRC["host"], port=_SRC["port"], dbname=_SRC["database"],
        user=_SRC["username"], password=_SRC["password"], connect_timeout=3,
    )
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


class World:
    def __init__(self, ent, monkeypatch):
        import services.cdc_capture_inventory as inv

        monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
        monkeypatch.setattr(inv, "_last_sweep", 0.0)
        self.ent = ent
        self.token = uuid.uuid4().hex[:8]
        self.ws_a = ent.workspace(f"C11 A {self.token}")
        self.ws_b = ent.workspace(f"C11 B {self.token}")
        self.a = ent.member(email=f"c11a-{self.token}@example.com", workspace_id=self.ws_a, role="admin")
        self.b = ent.member(email=f"c11b-{self.token}@example.com", workspace_id=self.ws_b, role="admin")
        self.table = f"r14c11r_{self.token}"
        _sql(f'CREATE TABLE "{self.table}" (id INT PRIMARY KEY, name TEXT)')
        _sql(f'INSERT INTO "{self.table}" VALUES (1, \'a\'), (2, NULL), (3, \'\'), (4, \'Zoë ✓\')')
        self.before_slots, self.before_pubs = _slots(), _pubs()
        self.x = self._connector(self.a, "x", _SRC)
        dsn = (
            f"postgresql://{_SRC['username']}:{_SRC['password']}@{_SRC['host']}:"
            f"{_SRC['port']}/{_SRC['database']}"
        )
        # Y is DSN-only (no host field): the same server through another spelling.
        self.y_same_ws = self._connector(self.a, "y", {"connection_string": dsn, "database": ""})
        self.y_other_ws = self._connector(self.b, "yb", {"connection_string": dsn, "database": ""})
        self.dst = self._connector(self.a, "dst", h.dest_config("postgresql"))

    def _connector(self, client, name: str, fields: dict) -> str:
        body = {"name": f"{name}-{self.token}", "type": "postgresql", "schema": "public",
                "ssl": False, **{k: v for k, v in fields.items() if k != "type"}}
        res = client.post("/api/v1/connectors/saved", json=body)
        assert res.status_code in (200, 201), res.text
        out = res.json()
        return str(out.get("id") or (out.get("connector") or {}).get("id"))

    def run_cdc(self) -> str:
        from services.mongodb_service import get_mongodb_service
        from src.transfer.engine import UniversalTransferEngine
        from src.transfer.models import EndpointConfig, TransferRequest

        request = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", schema="public",
                                  table=self.table, connector_id=self.x),
            destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                       table=f"{self.table}_dst", connector_id=self.dst),
            sync_mode="cdc",
            stream_contracts=[{"name": self.table, "selected": True, "primary_key": ["id"],
                               "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
            skip_preflight=True, validation_mode="strict", workspace_id=self.ws_a,
        )
        job_id = "r14c11r" + uuid.uuid4().hex[:12]
        get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        # The engine leaves the job terminal; the capture outlives it.
        return job_id

    def ours(self) -> set[str]:
        return {s for s in _slots() - self.before_slots if s.startswith("df_")}

    def schedule(self, job_id: str) -> str:
        from services.schedule_store import create_schedule

        sched = create_schedule({
            "name": f"c11-{self.token}", "source_connector_id": self.x,
            "source_table": self.table, "dest_connector_id": self.dst,
            "dest_table": f"{self.table}_dst", "interval": "hourly", "sync_mode": "cdc",
            "enabled": False, "last_job_id": job_id, "workspace_id": self.ws_a,
            "stream_contracts": [{"name": self.table, "selected": True, "primary_key": ["id"],
                                  "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
        })
        return sched.id

    def listing(self, client, connector: str) -> dict:
        res = client.get("/api/v1/ops/cdc-captures", params={"connector_id": connector})
        assert res.status_code == 200, res.text
        return res.json()

    def cleanup(self, client, connector: str, **body) -> dict:
        res = client.post("/api/v1/ops/cdc-captures/cleanup",
                          json={"connector_id": connector, "dry_run": False, **body})
        assert res.status_code == 200, res.text
        return res.json()

    def close(self) -> None:
        for name in _slots() - self.before_slots:
            try:
                _sql("SELECT pg_drop_replication_slot(%s)", (name,))
            except Exception:  # noqa: BLE001
                pass
        for pub in _pubs() - self.before_pubs:
            _sql(f'DROP PUBLICATION IF EXISTS "{pub}"')
        _sql(f'DROP TABLE IF EXISTS "{self.table}"')
        h.drop_table("postgresql", f"{self.table}_dst")


@pytest.fixture
def world(ent, monkeypatch):
    w = World(ent, monkeypatch)
    try:
        yield w
    finally:
        w.close()


def _item(listing: dict, slot: str) -> dict | None:
    return next((c for c in listing["captures"] if c.get("slot_name") == slot), None)


def test_b1_another_connector_on_the_same_server_never_drops_a_live_routes_slot(world):
    from services.cdc_capture_inventory import list_registered_captures
    from services.sync_cursor import get_watermark

    job = world.run_cdc()
    slot = world.ours().pop()
    world.schedule(job)
    entry = [e for e in list_registered_captures() if e["slot_name"] == slot][0]
    assert entry["workspace_id"] == world.ws_a and entry["connector_id"] == world.x
    assert entry["server"].startswith("pg:"), entry["server"]
    watermark = get_watermark(entry["cursor_key"])
    assert watermark is not None

    # Same workspace, other connector (DSN-only spelling of the same server).
    item = _item(world.listing(world.a, world.y_same_ws), slot)
    assert item is not None and item["state"] == "in_use", item
    assert item["cleanup_allowed"] is False
    out = world.cleanup(world.a, world.y_same_ws)
    assert out["dropped"] == [], out
    out = world.cleanup(world.a, world.y_same_ws, slot_names=[slot])
    assert out["dropped"] == [], out
    assert slot in _slots()
    assert get_watermark(entry["cursor_key"]) == watermark, "X's watermark was cleared"


def test_b1_another_workspace_sees_no_detail_and_cannot_drop(world):
    job = world.run_cdc()
    slot = world.ours().pop()
    pubs = _pubs() - world.before_pubs
    listing = world.listing(world.b, world.y_other_ws)
    text = json.dumps(listing)
    assert slot not in text and world.table not in text and job not in text, text
    assert listing["foreign_count"] >= 1, listing
    for pub in pubs:
        assert pub not in text
    out = world.cleanup(world.b, world.y_other_ws)
    assert out["dropped"] == [], out
    out = world.cleanup(world.b, world.y_other_ws, slot_names=[slot], include_unregistered=True)
    assert out["dropped"] == [], out
    assert slot in _slots() and pubs <= _pubs()


def test_b2_a_live_schedules_slot_stays_when_its_table_is_dropped(world, monkeypatch):
    import services.cdc_capture_inventory as inv

    job = world.run_cdc()
    slot = world.ours().pop()
    world.schedule(job)
    _sql(f'DROP TABLE "{world.table}"')
    item = _item(world.listing(world.a, world.x), slot)
    assert item["state"] == "in_use" and item["source_table_missing"] is True, item
    assert item["cleanup_allowed"] is False and "no longer exists" in item["warning"]
    assert world.cleanup(world.a, world.x)["dropped"] == []
    # Not by the beat either, even with auto-drop on and no grace.
    assert world.ent.admin.patch("/api/v1/workspace/settings",
                                 json={"cdc_orphan_auto_drop": True}).status_code == 200
    try:
        monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "0")
        for _ in range(3):
            inv.sweep_source_gone_captures(min_interval_sec=0)
        assert slot in _slots()
    finally:
        world.ent.admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": False})


def test_s3_a_pre_registry_routes_publication_is_never_offered_and_resume_adopts_it(world):
    from services.cdc_capture_inventory import _coll, list_registered_captures

    world.run_cdc()
    slot = world.ours().pop()
    pub = (_pubs() - world.before_pubs).pop()
    # Before the upgrade nothing recorded it.
    _coll("cdc_capture_registry").delete_many({"slot_name": slot})
    listing = world.listing(world.ent.admin, world.x)
    item = _item(listing, slot)
    # Not recorded: its publication is never guessed from the table stem
    # (that is how another route's publication got dropped, second review).
    assert item["ownership"] == "datawrap_named" and item["publication_name"] is None, item
    assert not [c for c in listing["captures"] if c["state"] == "orphaned_publication"], listing
    out = world.cleanup(world.ent.admin, world.x, publication_names=[pub], include_unregistered=True)
    assert out["dropped"] == [] and pub in _pubs(), out
    # A standalone look-alike publication is named for what it is.
    stray = f"df_pub_dataflow_stray_{uuid.uuid4().hex[:10]}"
    _sql(f'CREATE PUBLICATION "{stray}"')
    stray_item = next(c for c in world.listing(world.ent.admin, world.x)["captures"]
                      if c.get("publication_name") == stray)
    assert "named like a Datawrap publication" in stray_item["warning"], stray_item
    assert "created by Datawrap" not in stray_item["warning"]
    # The route's next run attaches to the slot and records it.
    world.run_cdc()
    adopted = [e for e in list_registered_captures() if e["slot_name"] == slot]
    assert adopted and adopted[0]["publication_name"] == pub, adopted


def test_s4_a_running_jobs_capture_is_never_dropped(world):
    from services.cdc_capture_inventory import list_registered_captures, record_pg_capture
    from services.mongodb_service import get_mongodb_service

    job = world.run_cdc()
    slot = world.ours().pop()
    # The one-shot job between peeks / in its snapshot: still running.
    get_mongodb_service().update_job_status(job, "running", allow_terminal_exit=True)
    item = _item(world.listing(world.a, world.x), slot)
    assert item["state"] == "running" and item["cleanup_allowed"] is False, item
    assert world.cleanup(world.a, world.x)["dropped"] == []
    assert slot in _slots()
    # A publication recorded before its slot exists, for a running job.
    other = f"{world.table}_p"
    _sql(f'CREATE TABLE "{other}" (id INT PRIMARY KEY)')
    try:
        pending_pub = f"df_pub_dataflow_{other}_{uuid.uuid4().hex[:10]}"
        _sql(f'CREATE PUBLICATION "{pending_pub}" FOR TABLE "{other}"')
        entry = [e for e in list_registered_captures() if e["slot_name"] == slot][0]
        record_pg_capture(
            {**_SRC, "connector_id": world.x, "job_id": job},
            slot_name=f"df_dataflow_{other}_{uuid.uuid4().hex[:8]}", publication_name=pending_pub,
            cursor_key=entry["cursor_key"] + "-p", schema="public", tables=[other],
            plugin="pgoutput",
        )
        listing = world.listing(world.a, world.x)
        pending = next(c for c in listing["captures"] if c.get("publication_name") == pending_pub)
        assert pending["state"] == "running" and pending["cleanup_allowed"] is False, pending
        world.cleanup(world.a, world.x)
        assert pending_pub in _pubs()
    finally:
        _sql(f'DROP TABLE IF EXISTS "{other}"')


def test_s8_the_orphan_alert_is_claimed_once_across_replicas_and_retried_on_failure(world, monkeypatch):
    import services.cdc_capture_inventory as inv
    import services.notification_service as ns

    world.run_cdc()
    slot = world.ours().pop()
    sent: list[str] = []
    gate = threading.Barrier(2)
    fail = {"on": True}

    def notify(ws, payload, **_kw):
        sent.append(payload["state"])
        return [{"ok": not fail["on"]}]

    monkeypatch.setattr(ns, "notify_workspace", notify)
    # First delivery fails: the claim is given back.
    inv.sweep_source_gone_captures(min_interval_sec=0)
    assert sent == ["orphaned"]
    entry = [e for e in inv.list_registered_captures() if e["slot_name"] == slot][0]
    assert entry.get("alerted_state") is None, entry
    fail["on"] = False
    # Two replicas' beats race: exactly one sends.
    real = inv._claim_alert

    def racing(*args, **kwargs):
        gate.wait(10)
        return real(*args, **kwargs)

    monkeypatch.setattr(inv, "_claim_alert", racing)
    threads = [threading.Thread(target=inv.sweep_source_gone_captures, kwargs={"min_interval_sec": 0})
               for _ in range(2)]
    monkeypatch.setattr(inv, "_last_sweep", 0.0)
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert sent == ["orphaned", "orphaned"], sent
    # A failing row does not abort the sweep.
    monkeypatch.setattr(inv, "_claim_alert", real)
    monkeypatch.setattr(inv, "_patch_entry", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    _sql(f'DROP TABLE "{world.table}"')
    out = inv.sweep_source_gone_captures(min_interval_sec=0)
    assert any(o.get("slot_name") == slot for o in out), out


def test_nit_grace_counts_from_enabling_and_absent_drops_are_not_drops(world, monkeypatch):
    import services.cdc_capture_inventory as inv
    import services.notification_service as ns

    monkeypatch.setattr(ns, "notify_workspace", lambda *a, **k: [])
    world.run_cdc()
    slot = world.ours().pop()
    _sql(f'DROP TABLE "{world.table}"')
    inv.sweep_source_gone_captures(min_interval_sec=0)
    entry = [e for e in inv.list_registered_captures() if e["slot_name"] == slot][0]
    long_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    inv._patch_entry(entry["id"], {"source_gone_since": long_ago})
    monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "600")
    assert world.ent.admin.patch("/api/v1/workspace/settings",
                                 json={"cdc_orphan_auto_drop": True}).status_code == 200
    try:
        inv.sweep_source_gone_captures(min_interval_sec=0)
        assert slot in _slots(), "dropped at once although auto-drop was enabled moments ago"
        inv._coll("platform_flags").update_one(
            {"_id": "cdc_orphan_auto_drop"}, {"$set": {"enabled_at": long_ago}}
        )
        out = inv.sweep_source_gone_captures(min_interval_sec=0)
        assert [o["action"] for o in out if o["slot_name"] == slot] == ["dropped"], out
        assert slot not in _slots()
    finally:
        world.ent.admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": False})
    absent = inv._drop({**_SRC, "connector_id": world.x}, {"slot_name": slot, "publication_name": ""})
    assert absent["action"] == "already_absent", absent


# ── Second review (dc2791d14) ──────────────────────────────────────────────


def _plain_slot(world, plugin: str = "pgoutput") -> str:
    """A Datawrap-named slot on the route's table with no publication on record."""
    name = f"df_dataflow_{world.table}_{uuid.uuid4().hex[:8]}"
    _sql("SELECT pg_create_logical_replication_slot(%s, %s)", (name, plugin))
    return name


def test_r2_an_orphan_on_the_same_table_never_takes_another_routes_publication(world):
    from services.cdc_capture_inventory import list_registered_captures, record_pg_capture

    job = world.run_cdc()
    live_slot = world.ours().pop()
    world.schedule(job)
    live_pub = [e for e in list_registered_captures() if e["slot_name"] == live_slot][0][
        "publication_name"
    ]
    assert live_pub in _pubs()
    # Workspace B: an orphaned slot on the same table, no publication recorded.
    orphan = _plain_slot(world, "test_decoding")
    record_pg_capture(
        {**_SRC, "connector_id": world.y_other_ws, "job_id": "gone"},
        slot_name=orphan, publication_name="", cursor_key=f"k-{world.token}",
        schema="public", tables=[world.table], plugin="test_decoding",
    )
    listing = world.listing(world.b, world.y_other_ws)
    assert live_pub not in json.dumps(listing), "B's listing names A's publication"
    out = world.cleanup(world.b, world.y_other_ws)
    assert out["dropped"] == [orphan], out
    assert live_pub in _pubs(), "B's cleanup dropped A's live publication"
    # A platform admin naming an unrecorded look-alike on the same table.
    stray = _plain_slot(world)
    out = world.cleanup(world.ent.admin, world.x, slot_names=[stray], include_unregistered=True)
    assert out["dropped"] == [stray], out
    assert live_pub in _pubs()
    # A's route still runs.
    world.run_cdc()


def test_r2_a_publication_another_row_uses_is_kept_when_the_slot_goes(world):
    from services.cdc_capture_inventory import list_registered_captures, record_pg_capture

    world.run_cdc()
    live_slot = world.ours().pop()
    live_pub = [e for e in list_registered_captures() if e["slot_name"] == live_slot][0][
        "publication_name"
    ]
    twin = _plain_slot(world)
    record_pg_capture(
        {**_SRC, "connector_id": world.x, "job_id": "gone"}, slot_name=twin,
        publication_name=live_pub, cursor_key=f"twin-{world.token}", schema="public",
        tables=[world.table], plugin="pgoutput",
    )
    out = world.cleanup(world.a, world.x, slot_names=[twin])
    assert out["dropped"] == [twin], out
    assert out["results"][0].get("publication_kept_for"), out
    assert live_pub in _pubs() and live_slot in _slots()


def test_r5_an_unreadable_connector_identity_still_counts_its_schedule(world):
    from services.connector_store import update_connector

    job = world.run_cdc()
    slot = world.ours().pop()
    world.schedule(job)
    # The schedule's connector can no longer log in (rotated password).
    update_connector(world.x, {"password": "rotated-wrong"})
    item = _item(world.listing(world.a, world.y_same_ws), slot)
    assert item["state"] == "in_use", item
    assert world.cleanup(world.a, world.y_same_ws)["dropped"] == []
    assert slot in _slots()


def test_r7_legacy_rows_are_migrated_and_a_pending_job_by_cursor_key_holds_the_slot(world):
    from services.cdc_capture_inventory import _coll, list_registered_captures
    from services.mongodb_service import get_mongodb_service

    world.run_cdc()
    slot = world.ours().pop()
    row = [e for e in list_registered_captures() if e["slot_name"] == slot][0]
    # A 52268a5b1 row: raw host:port/db key, no workspace.
    legacy_server = f"loopback:{_SRC['port']}/{_SRC['database']}"
    coll = _coll("cdc_capture_registry")
    coll.delete_many({"slot_name": slot})
    coll.insert_one({**{k: v for k, v in row.items() if k not in ("workspace_id",)},
                     "_id": f"{legacy_server}|{slot}", "id": f"{legacy_server}|{slot}",
                     "server": legacy_server})
    item = _item(world.listing(world.a, world.x), slot)
    assert item is not None and item["ownership"] == "datawrap", item
    moved = [e for e in list_registered_captures() if e["slot_name"] == slot]
    assert [m["server"] for m in moved] == [row["server"]] and moved[0]["workspace_id"] == world.ws_a
    # A queued run of the same route: no lease, not named on the row.
    svc = get_mongodb_service()
    queued = svc.create_transfer_job({"name": f"q-{world.token}", "workspace_id": world.ws_a})
    svc.get_database()["transfer_jobs"].update_one(
        {"name": f"q-{world.token}"}, {"$set": {"status": "pending", "cursor_key": row["cursor_key"]}}
    )
    assert queued
    item = _item(world.listing(world.a, world.x), slot)
    assert item["state"] == "running", item
    assert world.cleanup(world.a, world.x)["dropped"] == []


def test_r7_a_platform_admin_reads_and_cleans_the_same_scope(world):
    world.run_cdc()
    slot = world.ours().pop()
    listing = world.listing(world.ent.admin, world.x)
    assert _item(listing, slot) is not None
    res = world.ent.admin.post("/api/v1/ops/cdc-captures/cleanup",
                               json={"connector_id": world.x, "dry_run": True})
    assert res.status_code == 200, res.text
    assert slot in [r["slot_name"] for r in res.json()["results"]], res.json()


def test_r7_a_row_without_a_connector_is_swept_through_its_endpoint(world, monkeypatch):
    import services.cdc_capture_inventory as inv
    import services.notification_service as ns
    from services.cdc_capture_inventory import _coll, list_registered_captures

    monkeypatch.setattr(ns, "notify_workspace", lambda *a, **k: [])
    world.run_cdc()
    slot = world.ours().pop()
    _coll("cdc_capture_registry").update_many({"slot_name": slot}, {"$set": {"connector_id": ""}})
    assert [e["connector_id"] for e in list_registered_captures() if e["slot_name"] == slot] == [""]
    out = inv.sweep_source_gone_captures(min_interval_sec=0)
    assert any(o.get("slot_name") == slot for o in out), out
