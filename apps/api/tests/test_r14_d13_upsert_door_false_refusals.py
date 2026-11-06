"""D13 review 2: the upsert key door refuses only what the run would refuse.

00f246a6c's door could refuse a run that resolves its key elsewhere:
(a) an SDK connector's stream whose manifest declares the key (the runner
seeds it through seed_sdk_contract_defaults), and (b) a query / procedure
extract, which has no catalog key to read. (c) A signed data contract
(contract_id) does not carry the run's key — pinned here so the door is
revisited if that ever changes.
"""

from __future__ import annotations

import inspect
import uuid

import pytest

from services.upsert_key_door import UpsertKeyRequired, assert_upsert_key_resolvable
from tests import r4_route_harness as h


@pytest.fixture
def no_catalog(monkeypatch):
    import services.upsert_key_door as door

    calls: list[tuple] = []

    def _catalog(*args, **_kw):
        calls.append(args)
        return []  # read, and keyless

    monkeypatch.setattr(door, "_catalog_key", _catalog)
    return calls


def test_a_keyless_table_is_still_refused(no_catalog):
    with pytest.raises(UpsertKeyRequired):
        assert_upsert_key_resolvable(sync_mode="upsert", source_connector_id="c",
                                     source_table="orders", source_type="postgresql")


def test_an_sdk_stream_with_a_manifest_key_is_not_refused(no_catalog):
    from connectors.sdk.transfer_bridge import sdk_manifest_stream_identity

    assert sdk_manifest_stream_identity("airtable", "records").get("primary_key") == ["id"]
    assert_upsert_key_resolvable(sync_mode="upsert", source_connector_id="c",
                                 source_table="records", source_type="airtable")
    assert no_catalog == []  # the manifest answered; no catalog probe
    with pytest.raises(UpsertKeyRequired):  # an unknown stream falls back to the catalog
        assert_upsert_key_resolvable(sync_mode="upsert", source_connector_id="c",
                                     source_table="no_such_stream", source_type="airtable")


def test_a_query_or_procedure_source_is_left_to_the_run(no_catalog):
    assert_upsert_key_resolvable(sync_mode="upsert", source_connector_id="c",
                                 source_table="orders", callable_source=True)
    assert no_catalog == []


def test_a_signed_contract_does_not_carry_the_run_key():
    """Tripwire for (c): the contract engine never resolves a key, so the door
    need not consult contract_id. If this starts failing, teach the door."""
    import src.transfer.contract_engine as contract_engine

    assert "primary_key" not in inspect.getsource(contract_engine)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_a_rest_query_schedule_upsert_is_not_refused_at_the_door(client):
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    table = f"d13q_{token}"
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT, name TEXT)")  # nosec B608
        src = create_connector({"name": f"d13q-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"d13q-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        resp = client.post("/api/v1/schedules/", json={
            "name": f"q {token}", "source_connector_id": src.id, "source_table": table,
            "source_read_mode": "query", "source_query": f"SELECT id, name FROM {table}",
            "dest_connector_id": dst.id, "dest_table": f"{table}_o", "interval": "hourly",
            "sync_mode": "upsert", "enabled": False,
            "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name")]})
        body = resp.text
        assert "UPSERT_KEY_REQUIRED" not in body, body
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
