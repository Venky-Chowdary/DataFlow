"""QA round 12, Snowflake run (build 8f7d246d): scheduling-layer integrity.

Every test drives the real app: REST routes, the MCP JSON-RPC door
(``POST /api/v1/mcp``: ``create_schedule`` → ``confirm_action``), and the real
scheduler beat (``schedule_runner._run_due_schedules``). The control plane
(schedules, jobs, audit chain) is the live local MongoDB (``r11_live_world``);
sources are real PostgreSQL tables carrying NULL, '', whitespace and unicode;
destinations are fakesnow (Snowflake) or PostgreSQL. Workspace isolation is
enforced, as on a self-serve deployment.

* D12 — a schedule created through MCP + confirm was stored with no
  ``workspace_id``: it ran, and it blocked every new pipeline on its table
  ("1 enabled pipeline(s) already write …"), while GET, list and DELETE
  answered 404 over REST and MCP, and nothing was audited.
* D10 — ``compliance_acknowledged=true`` on a REST schedule was not stored, so
  every scheduled run failed the PII gate.
* D11 — ``dest_schema`` / ``dest_database`` on a REST schedule came back null
  and runs wrote into the connector's default schema.
* D2 — ``qty`` was flagged as a phone number, and ``birth`` / ``name``
  columns that hold counts and labels were flagged as PII.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import drop_pg, live, pg_table  # noqa: F401
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

pytest.importorskip("fakesnow")

_ROWS = [
    (1, "plain", "1234567"),
    (2, None, "2345678"),
    (3, "", "3456789"),
    (4, "   ", "4567890"),
    (5, "naïve 中文 ✓", "5678901"),
]


# ── harness ─────────────────────────────────────────────────────────────────


@pytest.fixture
def world(live, monkeypatch, tmp_path):
    """``live`` + isolation enforced + a fakesnow destination in the workspace."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    from services.connector_store import create_connector
    from src.ai.copilot import ack_ledger

    # The approval ledger caches its Mongo collection; this test's database is new.
    monkeypatch.setattr(ack_ledger, "_ledger", None)

    sf = create_connector({
        "name": f"r12-sf-{uuid.uuid4().hex[:6]}", "type": "snowflake", "host": "localhost",
        "port": 443, "database": "dataflow", "username": "test", "password": "test",
        "schema": "PUBLIC", "warehouse": "COMPUTE_WH", "role": "both",
        "workspace_id": live.workspace_id,
    })
    _sf_exec("CREATE SCHEMA IF NOT EXISTS DATAFLOW.DST")
    _sf_exec("CREATE SCHEMA IF NOT EXISTS DATAFLOW.DST2")
    live.sf = sf.id
    yield live


def _sf_conn():
    import connectors.snowflake_conn as sc

    return sc._open_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="PUBLIC", warehouse="", connection_string="",
    )


def _sf_exec(sql: str) -> list[tuple]:
    conn = _sf_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        try:
            return list(cur.fetchall())
        except Exception:  # noqa: BLE001 — DDL has no result set
            return []
    finally:
        conn.close()


def _sf_count(schema: str, table: str) -> int | None:
    try:
        return int(_sf_exec(f'SELECT COUNT(*) FROM DATAFLOW.{schema}."{table}"')[0][0])
    except Exception:  # noqa: BLE001 — the table is not there
        return None


def _src_table(extra_cols: str = "", rows=None) -> str:
    table = pg_table("id BIGINT PRIMARY KEY, note TEXT, qty TEXT" + extra_cols)
    if rows is None:
        conn = h.pg_conn()
        try:
            conn.cursor().executemany(
                f'INSERT INTO "{table}" (id, note, qty) VALUES (%s, %s, %s)',  # nosec B608
                _ROWS,
            )
        finally:
            conn.close()
    return table


def _rpc(client, tool: str, arguments: dict) -> tuple[bool, object]:
    res = client.post(
        "/api/v1/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": tool, "arguments": arguments}},
        headers={"Accept": "application/json, text/event-stream"},
    )
    assert res.status_code == 200, res.text
    result = res.json()["result"]
    text = result["content"][0]["text"]
    try:
        body: object = json.loads(text)
    except ValueError:
        body = text
    return (not result.get("isError")), body


def _mcp_create_and_confirm(client, *, src: str, table: str, dst: str, dest_table: str, **extra) -> str:
    ok, staged = _rpc(client, "create_schedule", {
        "source_connector_id": src, "source_table": table,
        "dest_connector_id": dst, "dest_table": dest_table,
        "sync_mode": "full_refresh_overwrite", "cadence": "daily at 02:00 UTC",
        "name": f"r12 {dest_table}", **extra,
    })
    assert ok, staged
    ok, confirmed = _rpc(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "r12 test"})
    assert ok and confirmed.get("ok"), confirmed
    return str(confirmed["schedule_id"])


def _run_once(sid: str, timeout: float = 240.0) -> dict:
    """Make the schedule due and let the real beat run it to the end."""
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule
    from services.workspace_filter import bound_workspace_filter

    with bound_workspace_filter(None):
        before = len(get_schedule(sid).run_history or [])
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(microsecond=0).isoformat()
        update_schedule(sid, {"next_run_at": past})
        schedule_runner._run_due_schedules()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            sched = get_schedule(sid)
            history = sched.run_history or []
            if not sched.running and (len(history) > before or sched.approval_request):
                return {"schedule": sched, "entry": history[-1] if history else {}}
            time.sleep(0.5)
    raise AssertionError(f"schedule {sid} did not settle")


def _run_text(live, sid: str, outcome: dict) -> str:
    sched = outcome["schedule"]
    job = {}
    if sched.last_job_id:
        job = live.db["transfer_jobs"].find_one({"_id": sched.last_job_id}) or {}
    return json.dumps(
        {"entry": outcome["entry"], "approval": sched.approval_request,
         "status": sched.last_status, "job_error": job.get("error"),
         "job_status": job.get("status"), "blockers": (job.get("preflight") or {}).get("blockers")},
        default=str,
    )


def _succeeded(outcome: dict) -> bool:
    return str(outcome["schedule"].last_status or "").lower() in {"success", "completed", "succeeded"}


def _audit(live, resource: str) -> list[str]:
    events = live.ent.audit(resource=resource)
    return [str(e.get("action") or e.get("event") or "") for e in events if str(e.get("resource")) == resource]


def _platform_admin(live):
    return live.ent.admin


def _mcp_client(live):
    """What QA's MCP client held: a workspace admin API key, scoped by header."""
    key = live.ent.api_key(role="admin", workspace_id=live.workspace_id, name="r12 mcp")
    return live.ent.bearer(key, live.workspace_id)


# ── D12 ─────────────────────────────────────────────────────────────────────


def test_d12_mcp_schedule_stays_visible_audited_and_deletable_after_a_run(world):
    live = world
    table = _src_table()
    dest = f"QA_SF_SCH_{uuid.uuid4().hex[:6].upper()}"
    src = live.pg_connector()
    mcp = _mcp_client(live)
    try:
        sid = _mcp_create_and_confirm(mcp, src=src, table=table, dst=live.sf, dest_table=dest)
        assert live.schedule_doc(sid).get("workspace_id") == live.workspace_id

        outcome = _run_once(sid)
        assert _succeeded(outcome), _run_text(live, sid, outcome)
        assert _sf_count("PUBLIC", dest) == len(_ROWS)

        # Visible to its own workspace on every door.
        got = mcp.get(f"/api/v1/schedules/{sid}")
        assert got.status_code == 200, got.text
        assert got.json()["workspace_id"] == live.workspace_id
        listed = mcp.get("/api/v1/schedules/")
        assert listed.status_code == 200 and sid in {s["id"] for s in listed.json()}
        ok, mcp_list = _rpc(mcp, "list_schedules", {"limit": 50})
        assert ok and sid in json.dumps(mcp_list), mcp_list
        ok, mcp_get = _rpc(mcp, "get_schedule", {"schedule_id": sid})
        assert ok and sid in json.dumps(mcp_get), mcp_get
        assert "schedule.definition.create" in _audit(live, f"schedule:{sid}")

        # The conflict check names the same schedule the API shows.
        ok, refused = _rpc(mcp, "create_schedule", {
            "source_connector_id": src, "source_table": table, "dest_connector_id": live.sf,
            "dest_table": dest, "sync_mode": "full_refresh_overwrite",
            "cadence": "daily at 03:00 UTC", "name": f"r12 second {dest}",
        })
        assert not ok and sid in str(refused) and "not visible" not in str(refused), refused

        # Deletable through MCP; the delete is audited.
        ok, staged = _rpc(mcp, "delete_schedule", {"schedule_id": sid})
        assert ok, staged
        ok, confirmed = _rpc(mcp, "confirm_action", {"ack_id": staged["ack_id"], "reason": "r12"})
        assert ok and confirmed.get("ok"), confirmed
        assert mcp.get(f"/api/v1/schedules/{sid}").status_code == 404
        assert live.db["pipeline_schedules"].find_one({"_id": sid}) is None
        assert "schedule.definition.delete" in _audit(live, f"schedule:{sid}")

        # The table is free again: a new pipeline is accepted and its second
        # run writes into the table the first schedule created.
        sid2 = _mcp_create_and_confirm(
            mcp, src=src, table=table, dst=live.sf, dest_table=dest, name=f"r12 again {dest}"
        )
        outcome = _run_once(sid2)
        assert _succeeded(outcome), _run_text(live, sid2, outcome)
        assert _sf_count("PUBLIC", dest) == len(_ROWS)
        removed = mcp.delete(f"/api/v1/schedules/{sid2}")
        assert removed.status_code == 200, removed.text
        assert "schedule.definition.delete" in _audit(live, f"schedule:{sid2}")
    finally:
        drop_pg(table)


def _ghost(live, *, dest_connector: str, dest_table: str, source_connector: str, table: str) -> str:
    """A legacy unscoped registration, as the pre-fix confirm path wrote it."""
    from services.schedule_store import create_schedule
    from services.workspace_filter import bound_workspace_filter

    with bound_workspace_filter(None):
        sched = create_schedule({
            "name": f"ghost {uuid.uuid4().hex[:6]}", "source_connector_id": source_connector,
            "source_table": table, "dest_connector_id": dest_connector, "dest_table": dest_table,
            "interval": "daily", "workspace_id": "", "enabled": True,
            "mappings": [{"source": "id", "target": "id"}],
        })
    assert sched.workspace_id == ""
    return sched.id


def test_d12_orphaned_registration_is_named_listed_and_reconciled_by_admin_only(world):
    live = world
    table = _src_table()
    dest = f"QA_SF_SCH_{uuid.uuid4().hex[:6].upper()}"
    src = live.pg_connector()
    try:
        ghost = _ghost(live, dest_connector=live.sf, dest_table=dest, source_connector=src, table=table)
        assert live.admin.get(f"/api/v1/schedules/{ghost}").status_code == 404

        # The member is told what blocks the table and who can clear it,
        # without the hidden row's id or name.
        ok, refused = _rpc(_mcp_client(live), "create_schedule", {
            "source_connector_id": src, "source_table": table, "dest_connector_id": live.sf,
            "dest_table": dest, "sync_mode": "full_refresh_overwrite",
            "cadence": "daily at 03:00 UTC", "name": f"r12 blocked {dest}",
        })
        assert not ok, refused
        assert "not visible in this workspace" in str(refused) and "/schedules/admin/orphans" in str(refused)
        assert ghost not in str(refused)

        # Admin-only.
        assert live.admin.get("/api/v1/schedules/admin/orphans").status_code == 403
        assert live.admin.post("/api/v1/schedules/admin/orphans/reconcile", json={"dry_run": False}).status_code == 403

        admin = _platform_admin(live)
        listed = admin.get("/api/v1/schedules/admin/orphans")
        assert listed.status_code == 200, listed.text
        row = next(o for o in listed.json()["orphans"] if o["id"] == ghost)
        assert row["reason"] == "unscoped" and row["proposed_workspace_id"] == live.workspace_id

        # Dry run first: the plan, nothing changed.
        plan = admin.post("/api/v1/schedules/admin/orphans/reconcile", json={})
        assert plan.status_code == 200, plan.text
        assert plan.json()["dry_run"] is True and plan.json()["applied_count"] == 0
        planned = next(r for r in plan.json()["results"] if r["id"] == ghost)
        assert planned["action"] == "adopt" and planned["target_workspace_id"] == live.workspace_id
        assert live.schedule_doc(ghost).get("workspace_id") in ("", None)

        applied = admin.post("/api/v1/schedules/admin/orphans/reconcile", json={"dry_run": False})
        assert applied.status_code == 200, applied.text
        assert applied.json()["applied_count"] >= 1
        assert live.schedule_doc(ghost)["workspace_id"] == live.workspace_id
        actions = _audit(live, f"schedule:{ghost}")
        assert "schedule.orphan.adopt" in actions, actions
        assert "schedule.orphan.reconcile" in _audit(live, "schedules:orphans")
        assert "schedule.orphan.list" in _audit(live, "schedules:orphans")

        # Idempotent: a second apply finds nothing to do.
        again = admin.post("/api/v1/schedules/admin/orphans/reconcile", json={"dry_run": False})
        assert ghost not in {r["id"] for r in again.json()["results"]}

        # Now its workspace sees it, can delete it, and the delete is audited.
        assert live.admin.get(f"/api/v1/schedules/{ghost}").status_code == 200
        assert live.admin.delete(f"/api/v1/schedules/{ghost}").status_code == 200
        assert "schedule.definition.delete" in _audit(live, f"schedule:{ghost}")
    finally:
        drop_pg(table)


def test_d12_startup_scan_is_dry_run_first_and_adopts_only_settled_owners(world, monkeypatch):
    from services.connector_store import create_connector
    from services.schedule_orphans import startup_orphan_scan

    live = world
    table = _src_table()
    src = live.pg_connector()
    legacy_dst = create_connector({
        "name": f"r12-legacy-{uuid.uuid4().hex[:6]}", "type": "postgresql", "host": h.PG["host"],
        "port": h.PG["port"], "database": h.PG["database"], "username": h.PG["username"],
        "password": h.PG["password"], "schema": "public", "workspace_id": "",
    })
    try:
        settled = _ghost(live, dest_connector=live.sf, dest_table="R12_SETTLED", source_connector=src, table=table)
        unsettled = _ghost(
            live, dest_connector=legacy_dst.id, dest_table="r12_unsettled",
            source_connector=legacy_dst.id, table=table,
        )
        monkeypatch.delenv("DATAFLOW_SCHEDULE_ORPHAN_REPAIR", raising=False)
        report = startup_orphan_scan()
        assert report and report["dry_run"] is True and report["applied_count"] == 0
        assert {settled, unsettled} <= {r["id"] for r in report["results"]}
        assert live.schedule_doc(settled).get("workspace_id") in ("", None)

        monkeypatch.setenv("DATAFLOW_SCHEDULE_ORPHAN_REPAIR", "apply")
        report = startup_orphan_scan()
        assert live.schedule_doc(settled)["workspace_id"] == live.workspace_id
        assert live.schedule_doc(unsettled).get("workspace_id") in ("", None)
        assert startup_orphan_scan()["results"] == [
            r for r in startup_orphan_scan()["results"] if r["id"] == unsettled
        ]

        # The undecidable one is the admin's call, and it is audited.
        admin = _platform_admin(live)
        deleted = admin.post(
            "/api/v1/schedules/admin/orphans/reconcile",
            json={"dry_run": False, "actions": {unsettled: "delete"}, "schedule_ids": [unsettled]},
        )
        assert deleted.status_code == 200, deleted.text
        assert live.db["pipeline_schedules"].find_one({"_id": unsettled}) is None
        assert "schedule.definition.delete" in _audit(live, f"schedule:{unsettled}")
    finally:
        drop_pg(table)


# ── D10 ─────────────────────────────────────────────────────────────────────

_PII_COLS = ", birth_date DATE, first_name TEXT"
_PII_ROWS = [
    (1, "plain", "1234567", "1990-05-20", "Ada"),
    (2, None, "2345678", None, None),
    (3, "", "3456789", "1985-01-02", ""),
    (4, "   ", "4567890", "2001-12-31", "   "),
    (5, "naïve 中文 ✓", "5678901", "1977-07-07", "Zoë"),
]
_PII_MAPS = [{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in ("id", "note", "qty", "birth_date", "first_name")]


def _pii_table() -> str:
    table = pg_table("id BIGINT PRIMARY KEY, note TEXT, qty TEXT" + _PII_COLS)
    conn = h.pg_conn()
    try:
        conn.cursor().executemany(
            f'INSERT INTO "{table}" VALUES (%s, %s, %s, %s, %s)', _PII_ROWS  # nosec B608
        )
    finally:
        conn.close()
    return table


def _rest_body(src: str, table: str, dst: str, dest_table: str, maps, **extra) -> dict:
    body = {
        "name": f"r12 {dest_table}", "source_connector_id": src, "source_table": table,
        "dest_connector_id": dst, "dest_table": dest_table, "interval": "daily",
        "sync_mode": "full_refresh_overwrite", "mappings": maps, "enabled": True,
    }
    body.update(extra)
    return body


def test_d10_rest_schedule_stores_and_applies_the_pii_acknowledgement(world):
    live = world
    table = _pii_table()
    src = live.pg_connector()
    try:
        # Control: without the acknowledgement the PII gate holds the run.
        held = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, live.sf, f"R12_D10_HELD_{uuid.uuid4().hex[:4].upper()}", _PII_MAPS))
        assert held.status_code == 201, held.text
        outcome = _run_once(held.json()["id"])
        text = _run_text(live, held.json()["id"], outcome)
        assert not _succeeded(outcome) and "birth_date" in text, text

        dest = f"R12_D10_{uuid.uuid4().hex[:6].upper()}"
        created = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, live.sf, dest, _PII_MAPS, compliance_acknowledged=True,
            acknowledgment_reason="Marketing DPA covers these columns",
        ))
        assert created.status_code == 201, created.text
        body = created.json()
        sid = body["id"]
        assert body["compliance_acknowledged"] is True, body
        assert body["acknowledgment_actor"].startswith("r11-admin-"), body
        assert body["acknowledgment_at"], body
        doc = live.schedule_doc(sid)
        assert doc["compliance_acknowledged"] is True and doc["acknowledgment_at"]
        assert "schedule.compliance.acknowledged" in _audit(live, f"schedule:{sid}")

        # Every run carries it — and the second run into the table it created.
        for _ in range(2):
            outcome = _run_once(sid)
            assert _succeeded(outcome), _run_text(live, sid, outcome)
            assert _sf_count("PUBLIC", dest) == len(_PII_ROWS)
        assert live.admin.get(f"/api/v1/schedules/{sid}").json()["compliance_acknowledged"] is True

        # Withdrawn by PATCH: who/when are cleared and the gate holds again.
        patched = live.admin.patch(f"/api/v1/schedules/{sid}", json={"compliance_acknowledged": False})
        assert patched.status_code == 200, patched.text
        assert patched.json()["compliance_acknowledged"] is False
        assert patched.json()["acknowledgment_at"] == ""
        assert not _succeeded(_run_once(sid))
    finally:
        drop_pg(table)


def test_d10_mcp_schedule_carries_the_pii_acknowledgement(world):
    live = world
    table = _pii_table()
    src = live.pg_connector()
    dest = f"R12_D10M_{uuid.uuid4().hex[:6].upper()}"
    try:
        sid = _mcp_create_and_confirm(
            _mcp_client(live), src=src, table=table, dst=live.sf, dest_table=dest,
            pii_acknowledgement={"approved_by": "dpo@example.com", "reason": "DPA signed for this feed"},
        )
        got = live.admin.get(f"/api/v1/schedules/{sid}")
        assert got.status_code == 200, got.text
        body = got.json()
        assert body["compliance_acknowledged"] is True
        assert body["acknowledgment_actor"] == "dpo@example.com" and body["acknowledgment_at"]
        assert "schedule.compliance.acknowledged" in _audit(live, f"schedule:{sid}")
        outcome = _run_once(sid)
        assert _succeeded(outcome), _run_text(live, sid, outcome)
        assert _sf_count("PUBLIC", dest) == len(_PII_ROWS)
    finally:
        drop_pg(table)


# ── D11 ─────────────────────────────────────────────────────────────────────

_MAPS = [{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in ("id", "note", "qty")]


def test_d11_rest_schedule_writes_into_its_dest_schema_on_snowflake(world):
    live = world
    table = _src_table()
    src = live.pg_connector()
    dest = f"QA_SF_D11_{uuid.uuid4().hex[:6].upper()}"
    try:
        created = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, live.sf, dest, _MAPS, dest_schema="DST", dest_database="DATAFLOW"))
        assert created.status_code == 201, created.text
        sid = created.json()["id"]
        assert (created.json()["dest_schema"], created.json()["dest_database"]) == ("DST", "DATAFLOW")
        got = live.admin.get(f"/api/v1/schedules/{sid}").json()
        assert (got["dest_schema"], got["dest_database"]) == ("DST", "DATAFLOW")

        for _ in range(2):  # second run into the table DataFlow created
            outcome = _run_once(sid)
            assert _succeeded(outcome), _run_text(live, sid, outcome)
        assert _sf_count("DST", dest) == len(_ROWS)
        assert _sf_count("PUBLIC", dest) is None, "wrote into the connector's default schema"

        patched = live.admin.patch(f"/api/v1/schedules/{sid}", json={"dest_schema": "DST2"})
        assert patched.status_code == 200, patched.text
        assert patched.json()["dest_schema"] == "DST2"
        outcome = _run_once(sid)
        assert _succeeded(outcome), _run_text(live, sid, outcome)
        assert _sf_count("DST2", dest) == len(_ROWS)

        # One table per location: the same name in another schema is not a
        # conflict, the same schema is.
        other = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, live.sf, dest, _MAPS, name=f"r12 other {dest}", dest_schema="DST"))
        assert other.status_code == 201, other.text
        clash = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, live.sf, dest, _MAPS, name=f"r12 clash {dest}", dest_schema="DST2"))
        assert clash.status_code == 400 and sid in clash.text, clash.text
    finally:
        drop_pg(table)


def test_d11_rest_schedule_writes_into_its_dest_schema_on_postgres(world):
    live = world
    table = _src_table()
    schema = f"r12_dst_{uuid.uuid4().hex[:6]}"
    h.execute("postgresql", f'CREATE SCHEMA "{schema}"')
    src = live.pg_connector()
    dst = live.pg_connector(name=f"r12-pg-dst-{uuid.uuid4().hex[:4]}")
    dest = f"r12_d11_{uuid.uuid4().hex[:6]}"
    try:
        created = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, table, dst, dest, _MAPS, dest_schema=schema))
        assert created.status_code == 201, created.text
        sid = created.json()["id"]
        assert created.json()["dest_schema"] == schema
        for _ in range(2):
            outcome = _run_once(sid)
            assert _succeeded(outcome), _run_text(live, sid, outcome)
        conn = h.pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT id, note FROM "{schema}"."{dest}" ORDER BY id')  # nosec B608
            got = cur.fetchall()
            cur.execute("SELECT to_regclass(%s)", (f'public."{dest}"',))
            leaked = cur.fetchone()[0]
        finally:
            conn.close()
        assert got == [(r[0], r[1]) for r in _ROWS]
        assert leaked is None, "wrote into the connector's default schema"
    finally:
        drop_pg(table)
        h.execute("postgresql", f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# ── D2 ──────────────────────────────────────────────────────────────────────


def test_d2_quantities_counts_and_labels_are_not_pii_but_real_pii_still_is(world):
    """``qty`` (text digits), ``name`` labels and ``births`` counts load
    unacknowledged; ``birth_date`` and ``first_name`` still hold the run."""
    live = world
    benign = pg_table(
        "id BIGINT PRIMARY KEY, qty TEXT, units BIGINT, name TEXT, births INTEGER, rebirth_count INTEGER"
    )
    real = _pii_table()
    conn = h.pg_conn()
    try:
        conn.cursor().executemany(
            f'INSERT INTO "{benign}" VALUES (%s, %s, %s, %s, %s, %s)',  # nosec B608
            [
                (1, "1234567", 12345678, "Widget 3000", 12, 1),
                (2, "2345678", 23456789, "orders_2024", None, 0),
                (3, "3456789", 34567890, "", 40, 2),
                (4, "4567890", 45678901, "   ", 7, None),
                (5, "5678901", 56789012, "name 5 ✓", 3, 1),
            ],
        )
    finally:
        conn.close()
    src = live.pg_connector()
    try:
        maps = [{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in ("id", "qty", "units", "name", "births", "rebirth_count")]
        dest = f"R12_D2_{uuid.uuid4().hex[:6].upper()}"
        created = live.admin.post("/api/v1/schedules/", json=_rest_body(src, benign, live.sf, dest, maps))
        assert created.status_code == 201, created.text
        outcome = _run_once(created.json()["id"])
        assert _succeeded(outcome), _run_text(live, created.json()["id"], outcome)
        assert _sf_count("PUBLIC", dest) == 5

        held = live.admin.post("/api/v1/schedules/", json=_rest_body(
            src, real, live.sf, f"R12_D2R_{uuid.uuid4().hex[:4].upper()}", _PII_MAPS))
        assert held.status_code == 201, held.text
        outcome = _run_once(held.json()["id"])
        text = _run_text(live, held.json()["id"], outcome)
        assert not _succeeded(outcome), text
        assert "birth_date (dob" in text and "first_name (name" in text, text
        assert "qty (" not in text, text
    finally:
        drop_pg(benign, real)
