"""QA MySQL D13: ``sync_mode=upsert`` is one word for a one-off and a schedule.

QA: ``/transfer`` accepted ``sync_mode=upsert`` and ``POST /schedules`` answered
422 — its model restated the modes as a Literal without ``upsert``. Both now
read ``services.transfer_policy_values.canonical_sync_mode`` (the engine's
canonical set and its aliases); an unknown word is a 422 on both, listing the
same allowed values. Live MySQL 8 -> PostgreSQL upsert schedule, run twice.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_vocabulary_is_shared_and_canonical():
    from services.schedule_store import SYNC_MODES
    from services.sync_cursor import CANONICAL_SYNC_MODES
    from services.transfer_policy_values import PolicyValueError, canonical_sync_mode

    assert set(CANONICAL_SYNC_MODES) <= SYNC_MODES
    assert canonical_sync_mode(" Upsert ") == "upsert"
    assert canonical_sync_mode("merge") == "upsert"
    assert canonical_sync_mode("full-refresh-append") == "full_refresh_append"
    assert canonical_sync_mode("") == ""
    with pytest.raises(PolicyValueError) as exc:
        canonical_sync_mode("bogus")
    assert "upsert" in str(exc.value) and "sync_mode" in str(exc.value)


def test_upsert_schedule_is_created_and_runs_like_the_one_off(client):
    from services.connector_store import create_connector

    my = h.mysql_conn()
    cur = my.cursor()
    token = uuid.uuid4().hex[:8]
    table, dest = f"d13_src_{token}", f"d13_dst_{token}"
    rows = [(1, "Ada"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓")]
    try:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, name VARCHAR(40)) CHARSET=utf8mb4")
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s)", rows)
        src = create_connector({"name": f"d13-my-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **h.dest_config("mysql")})
        dst = create_connector({"name": f"d13-pg-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        base = {"source_connector_id": src.id, "source_table": table, "dest_connector_id": dst.id,
                "interval": "hourly", "primary_key": "id", "enabled": True,
                "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name")]}

        bogus = client.post("/api/v1/schedules/", json={**base, "name": f"b {token}",
                                                        "dest_table": dest, "sync_mode": "bogus"})
        assert bogus.status_code == 422 and "upsert" in bogus.text, bogus.text
        one_off = client.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "mysql", "connector_id": src.id, "table": table},
            "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                            "table": dest, "schema": "public"},
            "sync_mode": "bogus",
        })
        assert one_off.status_code == 422 and "upsert" in one_off.text, one_off.text

        resp = client.post("/api/v1/schedules/", json={**base, "name": f"u {token}",
                                                       "dest_table": dest, "sync_mode": "upsert"})
        assert resp.status_code == 201, resp.text
        assert resp.json()["sync_mode"] == "upsert"
        sid = resp.json()["id"]
        cur.execute(f"UPDATE `{table}` SET name = 'Ада' WHERE id = 1")
        for n in (1, 2):
            run = _beat(sid)
            assert run["job"].get("status") == "completed", (n, run["job"].get("error"))
        got = h.query("postgresql", f'SELECT id, name FROM public."{dest}" ORDER BY id')  # nosec B608
        assert [tuple(r) for r in got] == [(1, "Ада"), *rows[1:]]

        patched = client.patch(f"/api/v1/schedules/{sid}", json={"sync_mode": "merge"})
        assert patched.status_code == 200 and patched.json()["sync_mode"] == "upsert", patched.text
    finally:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        h.drop_table("postgresql", dest)


def test_a_blank_patch_sync_mode_changes_nothing_and_a_keyless_upsert_is_refused_at_both_doors(client):
    """Review of 80a29de37: PATCH {"sync_mode": ""} turned an incremental_append
    schedule into a truncating full_refresh_overwrite. Blank means "not given".
    And an upsert with no key anywhere is refused before it is stored/queued,
    the same on POST /schedules and /transfer/execute."""
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    keyed, keyless = f"d13k_{token}", f"d13n_{token}"
    try:
        cur.execute(f"CREATE TABLE {keyed} (id BIGINT PRIMARY KEY, name TEXT, seq BIGINT)")  # nosec B608
        cur.execute(f"CREATE TABLE {keyless} (id BIGINT, name TEXT)")  # nosec B608
        cur.execute(f"INSERT INTO {keyless} VALUES (1, 'Ada'), (2, '')")  # nosec B608
        src = create_connector({"name": f"d13b-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"d13b-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        maps = [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name", "seq")]
        resp = client.post("/api/v1/schedules/", json={
            "name": f"ia {token}", "source_connector_id": src.id, "source_table": keyed,
            "dest_connector_id": dst.id, "dest_table": f"{keyed}_o", "interval": "hourly",
            "sync_mode": "incremental_append", "cursor_column": "seq",
            "cursor_semantics": "monotonic_sequence", "primary_key": "id",
            "mappings": maps, "enabled": False})
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        for blank in ("", "   "):
            patched = client.patch(f"/api/v1/schedules/{sid}", json={"sync_mode": blank})
            assert patched.status_code == 200, patched.text
            assert patched.json()["sync_mode"] == "incremental_append", patched.json()["sync_mode"]
        default = client.post("/api/v1/schedules/", json={
            "name": f"default {token}", "source_connector_id": src.id, "source_table": keyed,
            "dest_connector_id": dst.id, "dest_table": f"{keyed}_d", "interval": "hourly",
            "sync_mode": "", "mappings": maps, "enabled": False})
        assert default.status_code == 201 and default.json()["sync_mode"] == "full_refresh_overwrite"

        two = [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name")]
        refused = client.post("/api/v1/schedules/", json={
            "name": f"nokey {token}", "source_connector_id": src.id, "source_table": keyless,
            "dest_connector_id": dst.id, "dest_table": f"{keyless}_o", "interval": "hourly",
            "sync_mode": "upsert", "mappings": two, "enabled": True})
        assert refused.status_code == 422, refused.text
        assert refused.json()["error"]["code"] == "UPSERT_KEY_REQUIRED", refused.text
        one_off = client.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": src.id,
                       "table": keyless},
            "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                            "table": f"{keyless}_x", "schema": "public"},
            "mappings": two, "sync_mode": "upsert"})
        assert one_off.status_code == 422, one_off.text
        assert one_off.json()["error"]["code"] == "UPSERT_KEY_REQUIRED", one_off.text
        # A declared key resolves it on both doors; a catalog key on the keyed table too.
        declared = client.post("/api/v1/schedules/", json={
            "name": f"declared {token}", "source_connector_id": src.id, "source_table": keyless,
            "dest_connector_id": dst.id, "dest_table": f"{keyless}_k", "interval": "hourly",
            "sync_mode": "upsert", "primary_key": "id", "mappings": two, "enabled": True})
        assert declared.status_code == 201, declared.text
        catalog = client.post("/api/v1/schedules/", json={
            "name": f"catalog {token}", "source_connector_id": src.id, "source_table": keyed,
            "dest_connector_id": dst.id, "dest_table": f"{keyed}_u", "interval": "hourly",
            "sync_mode": "merge", "mappings": maps, "enabled": True})
        assert catalog.status_code == 201, catalog.text
        # PATCHing a keyless schedule into upsert is the same door.
        nokey = client.post("/api/v1/schedules/", json={
            "name": f"append {token}", "source_connector_id": src.id, "source_table": keyless,
            "dest_connector_id": dst.id, "dest_table": f"{keyless}_a", "interval": "hourly",
            "sync_mode": "full_refresh_append", "mappings": two, "enabled": False})
        assert nokey.status_code == 201, nokey.text
        to_upsert = client.patch(f"/api/v1/schedules/{nokey.json()['id']}", json={"sync_mode": "upsert"})
        assert to_upsert.status_code == 422, to_upsert.text
    finally:
        for t in (keyed, keyless):
            cur.execute(f"DROP TABLE IF EXISTS {t}")  # nosec B608
