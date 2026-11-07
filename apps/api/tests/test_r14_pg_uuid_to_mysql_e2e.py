"""QA MySQL D6: PostgreSQL UUID -> MySQL was held at mapping confidence 0.70.

QA (job 6ae6981c…): a PG table with ``uuid`` columns into a new MySQL table
was blocked by ``g4_mapping_confidence`` at 0.70. MySQL has no UUID type; the
create-new carrier is ``CHAR(36)``, the strictest UUID wire the engine can
spell, yet the planner (1) stamped a ``uuid_domain`` review chip as if a UUID
type existed to remap to and (2) kept the name-triggered ``uuid`` rewrite on a
byte copy. The real planner path is exercised here (MCP ``start_transfer`` ->
confirm -> job), no hand-set confidence, run 2 into the created table, NULL
and mixed-case UUIDs included.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg, _connector, _pg_cfg


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _pg_rows(table: str) -> dict[int, tuple]:
    return {r[0]: (r[0], r[1], r[2], r[3]) for r in h.query(
        "postgresql", f'SELECT id, customer_ref::text, external_id::text, note FROM "{table}"')}


def test_pg_uuid_lands_as_char36_at_full_confidence_run1_and_run2(client):
    t = f"r14d6_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int PRIMARY KEY, customer_ref uuid, '
                            f'external_id uuid NOT NULL, note text)')
    h.execute("postgresql", f"INSERT INTO \"{t}\" SELECT g, gen_random_uuid(), "
                            f"gen_random_uuid(), 'n' || g FROM generate_series(1, 40) g")
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (41, NULL, "
                            f"'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', ''), "
                            f"(42, '00000000-0000-0000-0000-000000000000', gen_random_uuid(), '  ')")
    src = _connector("postgresql", _pg_cfg(), "source")
    dst = _connector("mysql", _cfg("mysql"), "destination")
    args = {"source_connector_id": src, "dest_connector_id": dst, "dest_table": t,
            "source_table": t, "sync_mode": "full_refresh_overwrite", "primary_key": "id",
            "validation_mode": "strict",
            # `customer_ref` is a PII-shaped name; the hold is not what is tested.
            "pii_acknowledgement": {"approved_by": "qa-lead", "reason": "synthetic UUIDs"}}
    try:
        for run in (1, 2):
            ok, staged = h.mcp_call(client, "start_transfer", args)
            assert ok, {"error": staged}
            assert not h.blockers_of(staged), h.blockers_of(staged)
            conf = {m["source"]: m.get("confidence") for m in staged["plan"]["mappings"]}
            assert conf["customer_ref"] >= 0.9 and conf["external_id"] >= 0.9, conf
            h.confirm(client, staged["ack_id"], reason=f"r14-d6-run{run}")
            types = dict(h.query("mysql", "SELECT column_name, column_type FROM "
                                          "information_schema.columns WHERE table_schema = "
                                          f"DATABASE() AND table_name = '{t}'"))
            assert types["customer_ref"].lower() == "char(36)", types
            got = {int(r[0]): (int(r[0]), r[1], r[2], r[3]) for r in h.query(
                "mysql", f"SELECT id, customer_ref, external_id, note FROM `{t}`")}
            assert got == _pg_rows(t)
            if run == 1:
                h.execute("postgresql", f"UPDATE \"{t}\" SET customer_ref = gen_random_uuid() "
                                        f"WHERE id = 41")
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


def test_uuid_carrier_chip_is_informational_on_mysql_only():
    from services.dest_dialect_facts import dest_has_no_uuid_type
    from services.type_coercion import assess_create_new_type_risk

    assert dest_has_no_uuid_type("mysql")
    assert dest_has_no_uuid_type("aurora_mysql")
    assert not dest_has_no_uuid_type("mariadb")
    assert not dest_has_no_uuid_type("postgresql")
    kinds = {r["kind"] for r in assess_create_new_type_risk(
        "UUID", "CHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        destination_db_type="mysql")}
    assert kinds == {"uuid_carrier_equivalent"}, kinds
