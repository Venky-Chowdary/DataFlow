"""QA r12 #5: a schedule could be created for a source table that does not exist.

QA (build 8f7d246d): ``POST /api/v1/schedules/`` naming a source table that is
not on the source connector answered 201. Expected: 422 "source table X not
found", the status and ``{"error": {"code", "message"}}`` shape of the existing
refusal for a schedule without mappings.

8f7d246d had no save-time probe at all (201). 2eb5e3c0 (#362,
services/schedule_source_probe.py) added one that answered 400 for an enabled
schedule and saved a paused draft on the missing table with a warning.

Contract, through the real routes on a live PostgreSQL source and the live
MongoDB schedule store (``r11_live_world``):

* a missing source table is refused with 422 ``SOURCE_TABLE_NOT_FOUND`` naming
  the table — armed or paused, ASCII or unicode, schema-qualified or not — and
  nothing is stored;
* a blank or whitespace-only source table is refused with 422
  ``SOURCE_TABLE_REQUIRED``;
* the refusal has the same shape as the empty-mapping refusal;
* an existing table (also padded with whitespace, unicode, mixed case) is
  saved with no warning;
* an unreachable source is still saved, with a warning that nothing was verified;
* enabling or re-pointing a saved schedule at a table that is gone is refused
  the same way.
"""

from __future__ import annotations

import uuid

import pytest

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import drop_pg, live, pg_table  # noqa: F401

pytestmark = pytest.mark.timeout(300)

_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _body(src: str, table: str, **extra):
    body = {
        "name": f"r12 {uuid.uuid4().hex[:6]}",
        "source_connector_id": src,
        "source_table": table,
        "dest_connector_id": src,
        "dest_table": f"r12_out_{uuid.uuid4().hex[:6]}",
        "interval": "hourly",
        "timezone": "UTC",
        "mappings": _MAPS,
    }
    body.update(extra)
    return body


def _refused(res, code: str) -> dict:
    assert res.status_code == 422, res.text
    err = res.json()["error"]
    assert err["code"] == code, err
    return err


def test_create_on_a_missing_source_table_is_422_like_the_mapping_check(live):
    table = pg_table()
    missing = f"r12_missing_{uuid.uuid4().hex[:6]}"
    missing_unicode = f"r12_ünï_表_{uuid.uuid4().hex[:6]}"
    try:
        src = live.pg_connector()
        for body in (
            _body(src, missing),                              # enabled by default
            _body(src, missing, enabled=True),
            _body(src, missing, enabled=False),               # a paused draft too
            _body(src, missing_unicode, enabled=False),
            _body(src, f"public.{missing}"),
            _body(src, f"  {missing}  "),
        ):
            err = _refused(live.admin.post("/api/v1/schedules/", json=body), "SOURCE_TABLE_NOT_FOUND")
            shown = body["source_table"].strip()
            assert "not found" in err["message"], err
            assert shown.split(".")[-1] in err["message"] and shown.split(".")[-1] in err["source_table"]
        assert live.db["pipeline_schedules"].count_documents({}) == 0

        for blank in ("", "   "):
            err = _refused(
                live.admin.post("/api/v1/schedules/", json=_body(src, blank)), "SOURCE_TABLE_REQUIRED"
            )
            assert "source table" in err["message"].lower()
        assert live.db["pipeline_schedules"].count_documents({}) == 0

        # Same status and shape as the existing refusal for a schedule without mappings.
        mapping = live.admin.post(
            "/api/v1/schedules/", json=_body(src, table, mappings=[], enabled=True)
        )
        assert mapping.status_code == 422, mapping.text
        assert set(mapping.json()) == {"error"}
        assert {"code", "message"} <= set(mapping.json()["error"])
        missing_res = live.admin.post("/api/v1/schedules/", json=_body(src, missing))
        assert set(missing_res.json()) == {"error"}

        # Existing tables are saved, with no warning.
        for present in (table, f"public.{table}", f"  {table}  "):
            res = live.admin.post("/api/v1/schedules/", json=_body(src, present))
            assert res.status_code == 201, (present, res.text)
            assert res.json()["warnings"] == [], res.json()["warnings"]

        # Unreachable source: saved, with a warning that nothing was verified.
        dead = live.pg_connector(port=1)
        res = live.admin.post("/api/v1/schedules/", json=_body(dead, missing, enabled=False))
        assert res.status_code == 201, res.text
        assert any("could not verify" in w.lower() for w in res.json()["warnings"]), res.json()
    finally:
        drop_pg(table)


def test_enabling_or_repointing_at_a_missing_table_is_422(live):
    from tests import r4_route_harness as h

    table = pg_table()
    missing = f"r12_missing_{uuid.uuid4().hex[:6]}"
    try:
        src = live.pg_connector()
        paused = live.admin.post("/api/v1/schedules/", json=_body(src, table, enabled=False))
        assert paused.status_code == 201, paused.text
        sid = paused.json()["id"]

        err = _refused(
            live.admin.patch(f"/api/v1/schedules/{sid}", json={"source_table": missing}),
            "SOURCE_TABLE_NOT_FOUND",
        )
        assert missing in err["message"]
        assert live.schedule_doc(sid)["source_table"] == table

        h.execute("postgresql", f'ALTER TABLE "{table}" RENAME TO "{table}_gone"')
        try:
            err = _refused(
                live.admin.patch(f"/api/v1/schedules/{sid}", json={"enabled": True}),
                "SOURCE_TABLE_NOT_FOUND",
            )
            assert table in err["message"]
            assert live.schedule_doc(sid)["enabled"] is False
        finally:
            h.execute("postgresql", f'ALTER TABLE "{table}_gone" RENAME TO "{table}"')
        res = live.admin.patch(f"/api/v1/schedules/{sid}", json={"enabled": True})
        assert res.status_code == 200, res.text
    finally:
        drop_pg(table)
