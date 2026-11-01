"""Schedule create/export contract through the real routes (QA round 11).

QA (production 9c75ada4):

* SC11 — ``GET /schedules/export/dataflow`` returned 500 while the JSON
  export returned 200. The YAML dumper met ``bson.int64.Int64`` read back from
  MongoDB (any integer above 2**31 is stored as int64).
* SC5 — a schedule naming a source table that does not exist was accepted;
  the first run parked it with a refusal that did not name the table. A plain
  API create could also silently rewrite an unrelated draft on the same
  connector pair.
* SC7 — ``enabled: true`` without mappings was stored as disabled, and the
  response carried no word of it.
* SC8 — a ``*/2`` cron showed ``interval_preset: "daily"``, and releasing an
  empty-mapping park set ``next_run_at`` to "now" instead of the next cron slot.

Every store is the live MongoDB (``r11_live_world``); sources are real
PostgreSQL tables holding NULL, '', whitespace and unicode values; runs go
through the real scheduler beat.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
import yaml

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import drop_pg, live, pg_table  # noqa: F401

pytestmark = pytest.mark.timeout(300)

_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _body(src: str, table: str, **extra):
    body = {
        "name": f"r11 {uuid.uuid4().hex[:6]}",
        "source_connector_id": src,
        "source_table": table,
        "dest_connector_id": src,
        "dest_table": f"{table}_out",
        "interval": "hourly",
        "timezone": "UTC",
        "mappings": _MAPS,
    }
    body.update(extra)
    return body


# ── SC11 ────────────────────────────────────────────────────────────────────


def test_sc11_gitops_yaml_export_survives_int64_and_decimal128_from_mongo(live):
    from bson.decimal128 import Decimal128

    table = pg_table()
    try:
        src = live.pg_connector()
        params = {
            "batch_rows": 5_000_000_000,  # > 2**31: MongoDB stores int64
            "label": "naïve 中文 ✓",
            "blank": "",
            "spaces": "   ",
            "missing": None,
        }
        created = live.admin.post(
            "/api/v1/schedules/", json=_body(src, table, procedure_params=params)
        )
        assert created.status_code == 201, created.text
        sid = created.json()["id"]
        # Another writer's Decimal lands as Decimal128 inside the mapping rows.
        live.db["pipeline_schedules"].update_one(
            {"_id": sid}, {"$set": {"mappings.0.sample_max": Decimal128("12.30")}}
        )
        stored = live.schedule_doc(sid)
        assert type(stored["procedure_params"]["batch_rows"]).__name__ == "Int64"

        for url in ("/api/v1/schedules/export/dataflow", f"/api/v1/schedules/{sid}/export"):
            res = live.admin.get(url)
            assert res.status_code == 200, (url, res.status_code, res.text[:500])
            doc = yaml.safe_load(res.text)
            spec = (
                next(r for r in doc["resources"] if r["kind"] == "PipelineSchedule")["spec"]
                if "resources" in doc
                else doc["spec"]
            )
            assert spec["procedure_params"] == params, spec["procedure_params"]
            assert spec["mappings"][0]["sample_max"] == 12.3
            as_json = live.admin.get(url, params={"format": "json"})
            assert as_json.status_code == 200, as_json.text

        # The record itself reads back as plain Python values everywhere.
        got = live.admin.get(f"/api/v1/schedules/{sid}")
        assert got.status_code == 200, got.text
        assert got.json()["procedure_params"]["batch_rows"] == 5_000_000_000
        from services.schedule_store import get_schedule

        sched = get_schedule(sid)
        assert type(sched.procedure_params["batch_rows"]) is int
        # The exported artifact applies back (second run of the same manifest).
        manifest = yaml.safe_load(live.admin.get("/api/v1/schedules/export/dataflow").text)
        plan = live.admin.post("/api/v1/schedules/gitops/plan", json=manifest)
        assert plan.status_code == 200, plan.text
    finally:
        drop_pg(table)


# ── SC7 ─────────────────────────────────────────────────────────────────────


def test_sc7_enabled_true_without_mappings_is_refused_not_silently_disabled(live):
    table = pg_table()
    try:
        src = live.pg_connector()
        refused = live.admin.post(
            "/api/v1/schedules/", json=_body(src, table, mappings=[], enabled=True)
        )
        assert refused.status_code == 422, refused.text
        err = refused.json()["error"]
        assert err["code"] == "EMPTY_MAPPING_CONTRACT", err
        assert "mapping" in err["message"].lower()
        # Blank / whitespace-only rows are not mappings either.
        blank = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, table, enabled=True, mappings=[{"source": "", "target": "  "}]),
        )
        assert blank.status_code == 422, blank.text
        assert live.db["pipeline_schedules"].count_documents({}) == 0

        # Not asking for enabled: a draft, and the response says so.
        draft = live.admin.post("/api/v1/schedules/", json=_body(src, table, mappings=[]))
        assert draft.status_code == 201, draft.text
        body = draft.json()
        assert body["enabled"] is False
        assert body["enabled_note"] and "mapping" in body["enabled_note"].lower()
        assert any("disabled" in w.lower() for w in body["warnings"]), body["warnings"]
        got = live.admin.get(f"/api/v1/schedules/{body['id']}").json()
        assert got["enabled_note"] == body["enabled_note"]

        # Asking for a paused draft is honoured without a warning.
        paused = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, table, mappings=[], enabled=False, dest_table=f"{table}_p"),
        )
        assert paused.status_code == 201, paused.text
        assert paused.json()["enabled"] is False and paused.json()["warnings"] == []
    finally:
        drop_pg(table)


# ── SC5 ─────────────────────────────────────────────────────────────────────


def test_sc5_create_and_enable_probe_the_source_table(live):
    from tests import r4_route_harness as h

    table = pg_table()
    unicode_table = pg_table(name=f"r11_ünï_表_{uuid.uuid4().hex[:6]}")
    mixed = pg_table(name=f"R11_Mixed_{uuid.uuid4().hex[:6]}")
    missing = f"r11_missing_{uuid.uuid4().hex[:6]}"
    try:
        src = live.pg_connector()
        res = live.admin.post("/api/v1/schedules/", json=_body(src, missing))
        # QA r12 #5: 422 like the empty-mapping refusal (round 11 answered 400).
        assert res.status_code == 422, res.text
        assert missing in res.text and "not found" in res.text, res.text
        assert live.db["pipeline_schedules"].count_documents({}) == 0

        for blank in ("", "   "):
            res = live.admin.post("/api/v1/schedules/", json=_body(src, blank, dest_table="r11_blank_out"))
            assert res.status_code == 422, (repr(blank), res.text)
            assert "source table" in res.text.lower()

        for present in (table, unicode_table, mixed, f"public.{table}", f"  {table}  "):
            res = live.admin.post(
                "/api/v1/schedules/",
                json=_body(src, present, dest_table=f"o_{uuid.uuid4().hex[:6]}"),
            )
            assert res.status_code == 201, (present, res.text)
            assert res.json()["warnings"] == [], res.json()["warnings"]

        # Unreachable source: saved, with a warning that nothing was verified.
        dead = live.pg_connector(port=1)
        res = live.admin.post("/api/v1/schedules/", json=_body(dead, table, dest_table="r11_dead_out"))
        assert res.status_code == 201, res.text
        assert any("could not verify" in w.lower() for w in res.json()["warnings"]), res.json()

        # Enabling a paused schedule, or re-pointing it, probes again.
        paused = live.admin.post(
            "/api/v1/schedules/", json=_body(src, table, enabled=False, dest_table="r11_paused_out")
        ).json()
        h.execute("postgresql", f'ALTER TABLE "{table}" RENAME TO "{table}_gone"')
        try:
            res = live.admin.patch(f"/api/v1/schedules/{paused['id']}", json={"enabled": True})
            assert res.status_code == 422, res.text
            assert table in res.text
            res = live.admin.patch(f"/api/v1/schedules/{paused['id']}", json={"source_table": missing})
            # QA r12 #5: a paused draft on a missing table is refused too.
            assert res.status_code == 422, res.text
        finally:
            h.execute("postgresql", f'ALTER TABLE "{table}_gone" RENAME TO "{table}"')
    finally:
        drop_pg(table, unicode_table, mixed)


def test_sc5_plain_create_never_rewrites_an_unrelated_draft(live):
    one, two = pg_table(), pg_table()
    try:
        src = live.pg_connector()
        draft = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, one, name="r11 draft keep", mappings=[], enabled=False),
        )
        assert draft.status_code == 201, draft.text
        draft_id = draft.json()["id"]

        other = live.admin.post("/api/v1/schedules/", json=_body(src, two, name="r11 other pipeline"))
        assert other.status_code == 201, other.text
        assert other.json()["id"] != draft_id
        kept = live.admin.get(f"/api/v1/schedules/{draft_id}").json()
        assert (kept["name"], kept["source_table"], kept["mapping_count"]) == ("r11 draft keep", one, 0)

        # An explicit replay id still persists the mappings onto that draft.
        replay = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, one, name="r11 draft keep", replay_schedule_id=draft_id),
        )
        assert replay.status_code == 201, replay.text
        assert replay.json()["id"] == draft_id and replay.json()["mapping_count"] == 2
        assert live.db["pipeline_schedules"].count_documents({}) == 2
    finally:
        drop_pg(one, two)


def _beat_until_settled(schedule_id: str, timeout: float = 120.0) -> None:
    from services import schedule_runner
    from services.schedule_store import get_schedule

    schedule_runner._run_due_schedules()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sched = get_schedule(schedule_id)
        if not sched.running and (sched.run_history or sched.approval_request):
            return
        time.sleep(0.5)
    raise AssertionError(f"schedule {schedule_id} did not settle")


def test_sc5_run_refusal_names_the_missing_source_table(live):
    from services.schedule_store import update_schedule

    table = pg_table()
    try:
        src = live.pg_connector()
        created = live.admin.post("/api/v1/schedules/", json=_body(src, table))
        assert created.status_code == 201, created.text
        sid = created.json()["id"]
        drop_pg(table)  # the table disappears after the schedule was saved
        past = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        update_schedule(sid, {"next_run_at": past})
        _beat_until_settled(sid)
        got = live.admin.get(f"/api/v1/schedules/{sid}").json()
        reason = " ".join(
            str(x) for x in (
                got.get("last_status_reason"),
                (got.get("approval_request") or {}).get("finding"),
            )
        )
        assert table in reason, got
        assert "does not exist" in reason.lower() or "not found" in reason.lower(), reason
        assert (got.get("approval_request") or {}).get("code") == "SOURCE_TABLE_MISSING", got["approval_request"]
    finally:
        drop_pg(table)


# ── SC8 ─────────────────────────────────────────────────────────────────────


def _on_cron_slot(iso: str, tz: str, step: int) -> bool:
    local = datetime.fromisoformat(iso).astimezone(ZoneInfo(tz))
    return local.second == 0 and local.microsecond == 0 and local.minute % step == 0


def test_sc8_cron_schedule_reports_cron_preset_and_aligned_next_run(live):
    table = pg_table()
    try:
        src = live.pg_connector()
        tz = "America/New_York"
        res = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, table, interval="daily", cron="*/2 * * * *", timezone=tz),
        )
        assert res.status_code == 201, res.text
        body = res.json()
        assert body["interval_preset"] == "cron", body
        assert _on_cron_slot(body["next_run_at"], tz, 2), body["next_run_at"]
        listed = [s for s in live.admin.get("/api/v1/schedules/").json() if s["id"] == body["id"]]
        assert listed[0]["interval_preset"] == "cron"

        # A row written before this fix (interval=daily beside a cron) reads as cron.
        live.db["pipeline_schedules"].update_one({"_id": body["id"]}, {"$set": {"interval": "daily"}})
        assert live.admin.get(f"/api/v1/schedules/{body['id']}").json()["interval_preset"] == "cron"

        # Draft without mappings, then the mapping contract plus enable.
        draft = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, table, cron="*/2 * * * *", timezone=tz, mappings=[],
                       dest_table=f"{table}_d"),
        ).json()
        assert draft["enabled"] is False and draft["next_run_at"] is None
        enabled = live.admin.patch(
            f"/api/v1/schedules/{draft['id']}", json={"mappings": _MAPS, "enabled": True}
        )
        assert enabled.status_code == 200, enabled.text
        nxt = enabled.json()["next_run_at"]
        assert _on_cron_slot(nxt, tz, 2), nxt
        assert datetime.fromisoformat(nxt) > datetime.now(timezone.utc)
    finally:
        drop_pg(table)


def test_sc8_released_empty_mapping_park_waits_for_the_next_cron_slot(live):
    from services.schedule_mapping_contract import EMPTY_MAPPING_CODE, EMPTY_MAPPING_REFUSAL
    from services.schedule_store import get_schedule, update_schedule

    table = pg_table()
    try:
        src = live.pg_connector()
        tz = "Asia/Kolkata"
        sid = live.admin.post(
            "/api/v1/schedules/",
            json=_body(src, table, cron="*/5 * * * *", timezone=tz, dest_table=f"{table}_k"),
        ).json()["id"]
        # The beat parked it on the empty-mapping finding (mappings were lost).
        update_schedule(sid, {
            "mappings": [],
            "approval_request": {
                "id": "apr-r11", "status": "open", "code": EMPTY_MAPPING_CODE,
                "finding": EMPTY_MAPPING_REFUSAL, "approvable": False,
            },
        })
        res = live.admin.patch(f"/api/v1/schedules/{sid}", json={"mappings": _MAPS})
        assert res.status_code == 200, res.text
        sched = get_schedule(sid)
        assert sched.approval_request.get("status") != "open", sched.approval_request
        assert _on_cron_slot(sched.next_run_at, tz, 5), sched.next_run_at
    finally:
        drop_pg(table)
