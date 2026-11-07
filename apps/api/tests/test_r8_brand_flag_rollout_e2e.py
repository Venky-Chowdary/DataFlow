"""Owner decision 3: brand names for new objects only, rolled out flag-first.

``DATAWRAP_BRAND_OBJECT_NAMES`` is off by default. With it on, only objects
and keys created from then on take the Datawrap names (``dwk_`` keys,
``datawrap:managed`` tables); everything that already exists keeps its name
and keeps working — ``dfk_`` keys authenticate and tables marked
``dataflow:managed`` take run 2 — with the flag on and after it is turned off
again. This is the staged rollout in docs/DEPLOYMENT.md, end to end:

1. flag off (default): key A is ``dfk_``; run 1 with key A creates table T1
   (``dataflow:managed``);
2. flag on (staging): key B is ``dwk_``; run 2 with key B into T1 upserts and
   T1 keeps its old marker; run 2 with key A into a new table T2 creates it as
   ``datawrap:managed``;
3. flag off again (rollback): both keys authenticate, and runs into T1 and T2
   keep working without renaming either.

Every run goes through the REST route an API-key client uses
(``/api/v1/transfer/execute`` with ``Authorization: Bearer <key>``) into a
real PostgreSQL.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"


@pytest.fixture
def admin(tmp_path, monkeypatch):
    h.dest_config("postgresql")  # skip without PostgreSQL
    from services import audit_log, integrations_store, team_store, user_store
    from services.auth_rate_limit import reset_auth_rate_limits

    for key, value in {
        "DATAFLOW_TEAM_STORE": str(tmp_path / "teams.json"),
        "DATAFLOW_DATA_DIR": str(tmp_path),
        "DATAFLOW_REQUIRE_AUTH": "1",
        "DATAFLOW_ADMIN_EMAIL": ADMIN_EMAIL,
        "DATAFLOW_ADMIN_PASSWORD": ADMIN_PASSWORD,
        "DATAFLOW_AUTH_SECRET": "test-secret-not-production",
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_DISABLE_OBJECT_STORE": "1",
        "DATAFLOW_PII_HASH_KEY": "test-pii-key",
        "DATAFLOW_EGRESS_ALLOWLIST": "127.0.0.0/8,::1/128",
        "DATAFLOW_CONNECTOR_STORE": str(tmp_path / "connectors.json"),
        "DATAFLOW_CONNECTOR_STORE_BACKEND": "file",
        "DATAFLOW_SEED_DEMO": "",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.delenv("DATAFLOW_BRAND_OBJECT_NAMES", raising=False)
    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import services.tenant_store as tenant_store
    from services import brand
    from services.mongodb_service import MemoryMongoDBService

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    brand.reset_resolution_cache()
    reset_auth_rate_limits()
    from src.main import app

    client = TestClient(app)
    login = client.post("/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


@pytest.fixture
def tables():
    token = uuid.uuid4().hex[:8]
    names = {"src": f"r8b_s_{token}", "t1": f"r8b_t1_{token}", "t2": f"r8b_t2_{token}"}
    h.execute("postgresql", f'CREATE TABLE "{names["src"]}" (id INT PRIMARY KEY, note TEXT NULL)')
    h.execute(
        "postgresql",
        f"INSERT INTO \"{names['src']}\" VALUES (1, 'a'), (2, NULL), (3, ''), (4, 'Zoë ✓')",  # nosec B608
    )
    try:
        yield names
    finally:
        for name in names.values():
            h.drop_table("postgresql", name)


def _flag(monkeypatch, on: bool) -> None:
    from services import brand

    if on:
        monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "1")
    else:
        monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    brand.reset_resolution_cache()


def _mint(admin: TestClient, name: str) -> dict:
    res = admin.post("/api/v1/workspace/api-keys", json={"name": name, "role": "admin"})
    assert res.status_code == 200, res.text
    return res.json()


def _as_key(raw: str) -> TestClient:
    from src.main import app

    client = TestClient(app)
    client.headers["Authorization"] = f"Bearer {raw}"
    return client


def _connector(client: TestClient, name: str, role: str) -> str:
    res = client.post("/api/v1/connectors/saved", json={
        "name": name, "role": role, "ssl": False, **h.dest_config("postgresql"),
    })
    assert res.status_code in (200, 201), res.text
    body = res.json()
    return str(body.get("id") or (body.get("connector") or {}).get("id"))


def _run(client: TestClient, src_id: str, dst_id: str, src: str, dest: str) -> dict:
    mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "note")]
    contract = {"name": src, "selected": True, "sync_mode": "upsert", "primary_key": "id"}
    res = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "postgresql", "connector_id": src_id, "table": src},
        "destination": {"kind": "database", "format": "postgresql", "connector_id": dst_id, "table": dest},
        "mappings": mappings, "sync_mode": "upsert", "stream_contracts": [contract],
        "validation_mode": "strict", "async_mode": False, "skip_preflight": True,
        # A row filter keeps the run on the row writers, which mark the tables
        # they create. (The PostgreSQL->PostgreSQL COPY route marks none — the
        # documented scope of the marker; see docs/DEPLOYMENT.md.)
        "source_filter": {"column": "id", "operator": "gt", "value": 0},
    })
    assert res.status_code == 200, res.text
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(str(res.json().get("job_id") or "")) or {}
    assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error")}
    assert "copy" not in str((job.get("destination_summary") or {}).get("load_method") or "")
    return job


def _comment(table: str) -> str | None:
    rows = h.query(
        "postgresql",
        f"SELECT obj_description(to_regclass('public.\"{table}\"'), 'pg_class')",  # nosec B608
    )
    return rows[0][0]


def _cells(table: str) -> list[tuple]:
    return h.query("postgresql", f'SELECT id, note FROM "{table}" ORDER BY id')  # nosec B608


def test_flag_rollout_across_two_runs_and_two_keys(admin, tables, monkeypatch):
    from services import brand

    assert brand.object_names_flag_on() is False  # off unless the owner sets it
    src, t1, t2 = tables["src"], tables["t1"], tables["t2"]

    # 1. Flag off (production default): legacy key, legacy-marked table.
    key_a = _mint(admin, "key A")
    assert key_a["key"].startswith("dfk_")
    as_a = _as_key(key_a["key"])
    src_id = _connector(as_a, "r8b-src", "source")
    dst_id = _connector(as_a, "r8b-dst", "destination")
    _run(as_a, src_id, dst_id, src, t1)
    assert _comment(t1) == "dataflow:managed"
    assert _cells(t1) == _cells(src)

    # 2. Flag on (staging): only new things take the new names.
    _flag(monkeypatch, True)
    key_b = _mint(admin, "key B")
    assert key_b["key"].startswith("dwk_")
    as_b = _as_key(key_b["key"])
    h.execute("postgresql", f"UPDATE \"{src}\" SET note = 'b2' WHERE id = 2")  # nosec B608
    _run(as_b, src_id, dst_id, src, t1)  # new key, old-named table: run 2 upserts
    assert _comment(t1) == "dataflow:managed"  # never renamed
    assert _cells(t1) == _cells(src)
    _run(as_a, src_id, dst_id, src, t2)  # old key still authenticates, new table
    assert _comment(t2) == "datawrap:managed"
    assert _cells(t2) == _cells(src)
    events = admin.get("/api/v1/audit/events?limit=300").json()["events"]
    kinds = {
        (e.get("details") or {}).get("kind")
        for e in events
        if e.get("action") == "brand.object_name.created"
    }
    assert {"api_key_prefix", "managed_table_comment"} <= kinds, kinds

    # 3. Flag off again (rollback): everything created either way keeps working.
    _flag(monkeypatch, False)
    h.execute("postgresql", f"UPDATE \"{src}\" SET note = NULL WHERE id = 4")  # nosec B608
    _run(as_b, src_id, dst_id, src, t2)
    _run(as_a, src_id, dst_id, src, t1)
    assert (_comment(t1), _comment(t2)) == ("dataflow:managed", "datawrap:managed")
    assert _cells(t1) == _cells(src) == _cells(t2)
    for raw in (key_a["key"], key_b["key"]):
        assert _as_key(raw).get("/api/v1/workspace/pilot-engine").status_code == 200
    assert _mint(admin, "key C")["key"].startswith("dfk_")
