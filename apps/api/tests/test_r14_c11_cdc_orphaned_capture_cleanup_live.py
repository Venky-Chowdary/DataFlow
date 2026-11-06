"""C11: a PostgreSQL CDC route's replication slot outlives the route.

QA: CDC on a test table, then the table removed — the replication slot and the
publication stayed on the source, retaining WAL. Only a schedule delete
released them; a one-shot route, an abandoned route, or a dropped source table
left them for ever, with no listing, no warning and no cleanup action.

Live: the CDC run is the real engine (``sync_mode=cdc``) from a PostgreSQL with
``wal_level=logical`` (``R11_PG_LOGICAL_{HOST,PORT,DATABASE,USER,PASSWORD}``,
defaults the ``PG*`` server; skipped unless logical) into the ``PG*`` server.
The listing and the cleanup go through REST (``/api/v1/ops/cdc-captures``) and
MCP (``list_cdc_captures`` / ``cleanup_cdc_captures`` + Confirm); the
source-gone sweep is the scheduler beat's: it reports and alerts by default
and drops only once the deployment setting ``cdc_orphan_auto_drop`` is on.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_SRC = {
    "host": os.getenv("R11_PG_LOGICAL_HOST", os.getenv("PGHOST", "127.0.0.1")),
    "port": int(os.getenv("R11_PG_LOGICAL_PORT", os.getenv("PGPORT", "5432"))),
    "database": os.getenv("R11_PG_LOGICAL_DATABASE", os.getenv("PGDATABASE", "dataflow")),
    "username": os.getenv("R11_PG_LOGICAL_USER", os.getenv("PGUSER", "dataflow")),
    "password": os.getenv("R11_PG_LOGICAL_PASSWORD", os.getenv("PGPASSWORD", "")),
}

_ROWS = [(1, "alpha"), (2, None), (3, ""), (4, "   "), (5, "naïve 中文 ✓")]


def _connect(cfg: dict):
    import psycopg2

    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"], connect_timeout=3,
    )
    conn.autocommit = True
    return conn


def _logical_source() -> bool:
    try:
        socket.create_connection((_SRC["host"], _SRC["port"]), timeout=1).close()
        conn = _connect(_SRC)
        try:
            cur = conn.cursor()
            cur.execute("SHOW wal_level")
            return cur.fetchone()[0] == "logical"
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return False


needs_logical = pytest.mark.skipif(
    not _logical_source(), reason="needs a PostgreSQL with wal_level=logical (R11_PG_LOGICAL_*)"
)


def _sql(sql: str, params=None, fetch: bool = False):
    conn = _connect(_SRC)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


def _slots() -> set[str]:
    return {r[0] for r in _sql("SELECT slot_name FROM pg_replication_slots", fetch=True)}


def _pubs() -> set[str]:
    return {r[0] for r in _sql("SELECT pubname FROM pg_publication", fetch=True)}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "data").mkdir()
    import services.cdc_capture_inventory as inv
    import services.workspace_store as workspace_store

    monkeypatch.setattr(inv, "_last_sweep", 0.0)
    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "data" / "workspace.json")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def route():
    """A source table, its saved connectors, and the CDC run's slot (one-shot)."""
    if not _logical_source():
        pytest.skip("needs a PostgreSQL with wal_level=logical")
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table = f"r14c11_{token}"
    _sql(f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name TEXT)')
    conn = _connect(_SRC)
    conn.cursor().executemany(f'INSERT INTO "{table}" VALUES (%s, %s)', _ROWS)  # nosec B608
    conn.close()
    src = create_connector({"name": f"logical-{token}", "type": "postgresql", "schema": "public", **_SRC})
    dst = create_connector({"name": f"pg-dst-{token}", **h.dest_config("postgresql")})
    env = {"table": table, "dest": f"{table}_dst", "src": src.id, "dst": dst.id,
           "before_slots": _slots(), "before_pubs": _pubs()}
    try:
        yield env
    finally:
        for name in _slots() - env["before_slots"]:
            try:
                _sql("SELECT pg_drop_replication_slot(%s)", (name,))
            except Exception:  # noqa: BLE001
                pass
        for pub in _pubs() - env["before_pubs"]:
            _sql(f'DROP PUBLICATION IF EXISTS "{pub}"')
        _sql(f'DROP TABLE IF EXISTS "{table}"')
        h.drop_table("postgresql", env["dest"])


def _run_cdc(env: dict) -> str:
    """One-shot CDC run through the engine: snapshot + slot + publication."""
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public",
                              table=env["table"], connector_id=env["src"]),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=env["dest"], connector_id=env["dst"]),
        sync_mode="cdc",
        stream_contracts=[{"name": env["table"], "selected": True, "primary_key": ["id"],
                           "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "r14c11" + uuid.uuid4().hex[:12]
    get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return job_id


def _ours(env: dict) -> set[str]:
    return {s for s in _slots() - env["before_slots"] if s.startswith("df_")}


def _listing(client, env: dict) -> dict:
    resp = client.get("/api/v1/ops/cdc-captures", params={"connector_id": env["src"]})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _item(listing: dict, slot: str) -> dict:
    found = [c for c in listing["captures"] if c.get("slot_name") == slot]
    assert found, (slot, [c.get("slot_name") for c in listing["captures"]])
    return found[0]


@needs_logical
def test_one_shot_route_slot_is_listed_orphaned_and_cleaned_up_over_rest(client, route):
    from services.cdc_capture_inventory import list_registered_captures

    _run_cdc(route)
    ours = _ours(route)
    assert len(ours) == 1, ours
    slot = ours.pop()
    # Ownership was recorded when the slot was created.
    owned = [e for e in list_registered_captures() if e["slot_name"] == slot]
    assert owned and owned[0]["tables"] == [route["table"]], owned
    assert owned[0]["connector_id"] == route["src"]

    item = _item(_listing(client, route), slot)
    assert item["ownership"] == "datawrap" and item["state"] == "orphaned", item
    assert item["tables_present"] == {route["table"]: True}
    assert item["cleanup_allowed"] is True and item["warning"], item
    assert isinstance(item["retained_wal_bytes"], int)

    dry = client.post("/api/v1/ops/cdc-captures/cleanup",
                      json={"connector_id": route["src"], "slot_names": [slot]})
    assert dry.status_code == 200, dry.text
    assert [r["action"] for r in dry.json()["results"]] == ["would_drop"]
    assert slot in _slots(), "a dry run dropped the slot"

    done = client.post("/api/v1/ops/cdc-captures/cleanup",
                       json={"connector_id": route["src"], "slot_names": [slot], "dry_run": False,
                             "reason": "route retired"})
    assert done.status_code == 200, done.text
    assert done.json()["dropped"] == [slot], done.json()
    assert slot not in _slots()
    pub = item["publication_name"]
    if pub:
        assert pub not in _pubs(), "the route's publication was left behind"
    assert not [e for e in list_registered_captures() if e["slot_name"] == slot]


@needs_logical
def test_dropped_source_table_is_flagged_and_cleaned_up_through_mcp(client, route):
    _run_cdc(route)
    slot = _ours(route).pop()
    _sql(f'DROP TABLE "{route["table"]}"')

    ok, listed = h.mcp_call(client, "list_cdc_captures", {"connector_id": route["src"]})
    assert ok, listed
    item = _item(listed, slot)
    assert item["state"] == "source_table_gone", item
    assert item["tables_present"] == {route["table"]: False}
    assert "no longer exists" in item["warning"], item["warning"]
    assert any(slot in w for w in listed["warnings"]), listed["warnings"]

    ok, staged = h.mcp_call(client, "cleanup_cdc_captures", {"connector_id": route["src"]})
    assert ok, staged
    assert staged.get("ack_id"), staged
    assert slot in _slots(), "staging dropped the slot before Confirm"
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "c11"})
    assert resp.status_code == 200, resp.text
    assert resp.json().get("ok") is True, resp.json()
    assert slot not in _slots()


def _capture_alerts(monkeypatch) -> list[dict]:
    """The payloads the beat hands the workspace notification path."""
    import services.notification_service as ns

    sent: list[dict] = []
    monkeypatch.setattr(ns, "notify_workspace", lambda ws, payload, **_kw: sent.append(payload) or [])
    return sent


@needs_logical
def test_beat_reports_and_alerts_but_never_drops_while_auto_drop_is_off(client, route, monkeypatch):
    """Off by default: a dropped table and a retired one-shot route are found,
    warned and alerted (WAL held named), and both slots stay."""
    import services.cdc_capture_inventory as inv
    from services.workspace_store import get_workspace_settings

    sent = _capture_alerts(monkeypatch)
    assert get_workspace_settings()["cdc_orphan_auto_drop"] is False
    monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "0")
    _run_cdc(route)
    slot = _ours(route).pop()
    first = inv.sweep_source_gone_captures(min_interval_sec=0)
    assert [(o["state"], o["action"]) for o in first if o["slot_name"] == slot] == [
        ("orphaned", "reported")
    ], first
    assert len(sent) == 1 and sent[0]["slot_name"] == slot and sent[0]["state"] == "orphaned"
    _sql(f'DROP TABLE "{route["table"]}"')
    for _ in range(3):  # first sight stamps the time; later passes are past the grace
        out = inv.sweep_source_gone_captures(min_interval_sec=0)
        assert [o["action"] for o in out if o["slot_name"] == slot] == ["reported"], out
    assert slot in _slots(), "the beat dropped a slot with cdc_orphan_auto_drop off"
    # One alert per state, naming the WAL held; not one per beat.
    assert [p["state"] for p in sent] == ["orphaned", "source_table_gone"], sent
    assert "WAL held" in sent[1]["text"] and "Automatic drop is off" in sent[1]["text"]
    listing = _listing(client, route)
    assert listing["auto_drop"]["enabled"] is False
    item = _item(listing, slot)
    assert item["state"] == "source_table_gone" and item["cleanup_allowed"] is True
    assert "cdc_orphan_auto_drop is off" in item["warning"], item["warning"]


@needs_logical
def test_beat_drops_after_the_grace_only_once_auto_drop_is_on(client, route, monkeypatch):
    import services.cdc_capture_inventory as inv

    _capture_alerts(monkeypatch)
    resp = client.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": True})
    assert resp.status_code == 200, resp.text
    assert resp.json()["cdc_orphan_auto_drop"] is True
    monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "0")
    _run_cdc(route)
    slot = _ours(route).pop()
    # Table still there: a one-shot slot is never auto-dropped.
    inv.sweep_source_gone_captures(min_interval_sec=0)
    assert slot in _slots()
    _sql(f'DROP TABLE "{route["table"]}"')
    # Inside the grace: first sight stamps source_gone_since, nothing dropped.
    monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "3600")
    inv.sweep_source_gone_captures(min_interval_sec=0)
    inv.sweep_source_gone_captures(min_interval_sec=0)
    assert slot in _slots(), "dropped inside the grace period"
    entry = [e for e in inv.list_registered_captures() if e["slot_name"] == slot][0]
    assert entry.get("source_gone_since"), entry
    # Past the grace: dropped, audited.
    monkeypatch.setenv("DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC", "0")
    out = inv.sweep_source_gone_captures(min_interval_sec=0)
    assert [o.get("action") for o in out if o.get("slot_name") == slot] == ["dropped"], out
    assert slot not in _slots()


@needs_logical
def test_cleanup_never_drops_a_slot_datawrap_did_not_create_or_a_live_route_uses(client, route):
    from services.schedule_store import create_schedule

    token = uuid.uuid4().hex[:6]
    external = f"ext_r14_{token}"
    lookalike = f"df_dataflow_lookalike_{token}"
    for name in (external, lookalike):
        _sql("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (name,))
    job_id = _run_cdc(route)
    slot = (_ours(route) - {lookalike}).pop()
    # A live CDC schedule on the same route uses the slot.
    sched = create_schedule({
        "name": f"r14c11-{token}", "source_connector_id": route["src"],
        "source_table": route["table"], "dest_connector_id": route["dst"],
        "dest_table": route["dest"], "interval": "hourly", "sync_mode": "cdc",
        "enabled": False, "last_job_id": job_id,
        "stream_contracts": [{"name": route["table"], "selected": True, "primary_key": ["id"],
                              "sync_mode": "cdc", "cursor_semantics": "cdc_position"}],
    })
    listing = _listing(client, route)
    assert _item(listing, external)["ownership"] == "external"
    assert _item(listing, lookalike)["state"] == "unregistered"
    in_use = _item(listing, slot)
    assert in_use["state"] == "in_use" and in_use["used_by_schedules"] == [sched.id], in_use

    resp = client.post("/api/v1/ops/cdc-captures/cleanup", json={
        "connector_id": route["src"], "slot_names": [external, lookalike, slot], "dry_run": False,
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["dropped"] == [], resp.json()
    assert {external, lookalike, slot} <= _slots()

    # Named explicitly with include_unregistered, the look-alike goes; the
    # external slot never does.
    resp = client.post("/api/v1/ops/cdc-captures/cleanup", json={
        "connector_id": route["src"], "slot_names": [external, lookalike],
        "include_unregistered": True, "dry_run": False,
    })
    assert resp.json()["dropped"] == [lookalike], resp.json()
    assert external in _slots() and lookalike not in _slots()
    _sql("SELECT pg_drop_replication_slot(%s)", (external,))

    # Deleting the schedule releases its slot and closes the ownership record.
    from services.cdc_capture_inventory import list_registered_captures

    resp = client.delete(f"/api/v1/schedules/{sched.id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["capture_release"]["released"] is True, resp.json()
    assert slot not in _slots()
    assert not [e for e in list_registered_captures() if e["slot_name"] == slot]
