"""QA D276 — on the hosted cloud SQLite is not offered as a destination.

The hosted cloud runs the API, the worker and the scheduler as separate
services, each with its own filesystem (a Railway volume attaches to one
service). A SQLite destination file was written by the worker and listed by
the API from its own disk: "Data kept at <table> (31 rows)", then the
connector showed 0 tables. Owner decision: on the hosted cloud (a deployed
Railway service) SQLite is refused as a destination, before any row is written,
with one operator message; ``DATAWRAP_SQLITE_DESTINATION=on|off`` decides
explicitly; self-hosted installs, tests and dev keep it; SQLite as a source is
unchanged.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from concurrent.futures import Future
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

REFUSAL = "isn't available as a destination"
_RAILWAY_MARKERS = ("RAILWAY_ENVIRONMENT_NAME", "RAILWAY_DEPLOYMENT_ID", "RAILWAY_REPLICA_ID")
_OVERRIDES = (
    "DATAWRAP_SQLITE_DESTINATION",
    "DATAFLOW_SQLITE_DESTINATION",
    "DATAWRAP_LOCAL_FILE_DESTINATIONS",
    "DATAFLOW_LOCAL_FILE_DESTINATIONS",
)


def _not_hosted(monkeypatch) -> None:
    for key in (*_RAILWAY_MARKERS, *_OVERRIDES):
        monkeypatch.delenv(key, raising=False)


def _hosted(monkeypatch) -> None:
    """A deployed Railway service (deploy-time markers), production policy off.

    ``ENV=test`` keeps the rest of the process out of production policy, which
    also shows the SQLite decision does not hang off ``is_production()``.
    """
    _not_hosted(monkeypatch)
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "d276-deploy")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_NAME", "production")


def _seed(path: Path, rows: int = 31) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            conn.executemany(
                "INSERT INTO customers VALUES (?, ?)", [(i, f"c{i}") for i in range(1, rows + 1)]
            )
    finally:
        conn.close()


def _tables(path: Path) -> list[str]:
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    try:
        return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    finally:
        conn.close()


def _count(path: Path, table: str) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])  # nosec B608
    finally:
        conn.close()


# --------------------------------------------------------------------------- policy


def test_policy_decision_order(monkeypatch):
    from services.local_file_policy import sqlite_destination_allowed, sqlite_destination_refusal

    _not_hosted(monkeypatch)
    assert sqlite_destination_allowed()  # tests / dev / self-hosted
    monkeypatch.setenv("ENV", "production")  # self-hosted production keeps SQLite
    assert sqlite_destination_allowed()
    # A linked `railway run` shell is not a deployed service.
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("RAILWAY_SERVICE_ID", "svc")
    assert sqlite_destination_allowed()

    _hosted(monkeypatch)
    assert not sqlite_destination_allowed()
    message = sqlite_destination_refusal("sqlite")
    assert message and REFUSAL in message and "self-hosted" in message
    assert sqlite_destination_refusal("postgresql") is None
    # A generic SQL connector on a sqlite: URL writes the same server-local file.
    assert sqlite_destination_refusal("generic_sql", "sqlite:////data/x.db")
    assert sqlite_destination_refusal("generic_sql", "postgresql://h/db") is None

    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "on")  # explicit setting wins
    assert sqlite_destination_allowed()
    monkeypatch.delenv("DATAWRAP_SQLITE_DESTINATION")
    monkeypatch.setenv("DATAFLOW_SQLITE_DESTINATION", "on")  # legacy brand name
    assert sqlite_destination_allowed()
    _not_hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "off")
    assert not sqlite_destination_allowed()


# --------------------------------------------------------------------------- engine


@pytest.fixture()
def fake_store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(dict(fields or {}))
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    # The engine's destination refusal records through the service module itself.
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, destination: EndpointConfig, src: Path):
    job_id = "d276" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    return job_id, UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite_dest(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="customers")


def test_engine_refuses_a_sqlite_destination_before_any_row(tmp_path, fake_store, monkeypatch):
    """Every run (REST, Confirm, schedule, retry) reaches the engine: zero rows land."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    job_id, result = _run(fake_store, _sqlite_dest(dst), src)
    assert not result.success
    assert REFUSAL in str(result.error), result.error
    job = fake_store.jobs[job_id]
    assert job.get("status") == "failed" and REFUSAL in str(job.get("error")), job
    assert _tables(dst) == []  # nothing written, not even a table

    # A SQLite export written to a file is the same server-local database.
    job_id, result = _run(
        fake_store,
        EndpointConfig(kind="file_export", format="sqlite", output_path="out.sqlite", table="customers"),
        src,
    )
    assert not result.success and REFUSAL in str(result.error), result.error


def test_direct_destination_writes_refuse_sqlite_when_hosted(tmp_path, monkeypatch):
    """Quarantine replay, query export, CDC drift repair and staging write through
    ``write_destination_database`` without the engine; it is the last door."""
    from src.transfer.adapters import write_destination_database

    dst = tmp_path / "direct.sqlite"
    _hosted(monkeypatch)
    with pytest.raises(ValueError, match=REFUSAL):
        write_destination_database(
            _sqlite_dest(dst), [{"id": 1, "name": "a"}], ["id", "name"],
            {"id": "integer", "name": "text"}, [{"source": "id", "target": "id"}],
        )
    assert _tables(dst) == []


def test_override_turns_sqlite_destination_back_on(tmp_path, fake_store, monkeypatch):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "on")
    _job, result = _run(fake_store, _sqlite_dest(dst), src)
    assert result.success, result.error
    assert _count(dst, "customers") == 31

    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    assert "sqlite" in caps["destination_databases"]
    assert caps["destination_databases_unavailable"] == []


def test_non_hosted_install_is_unaffected(tmp_path, fake_store, monkeypatch):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    _not_hosted(monkeypatch)
    monkeypatch.setenv("ENV", "production")  # a self-hosted production install
    _job, result = _run(fake_store, _sqlite_dest(dst), src)
    assert result.success, result.error
    assert _count(dst, "customers") == 31

    from src.transfer.registry import get_capabilities, validate_transfer

    assert validate_transfer("database", "postgresql", "database", "sqlite")[0]
    caps = get_capabilities()
    assert "sqlite" in caps["destination_databases"]
    assert caps["destination_databases_unavailable"] == []


def test_sqlite_source_still_works_when_hosted(tmp_path, fake_store, monkeypatch):
    from src.transfer.connector_capabilities import destination_refusal
    from src.transfer.registry import validate_transfer

    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    ok, msg = validate_transfer("database", "sqlite", "database", "postgresql")
    assert ok, msg
    assert destination_refusal("postgresql") is None

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    # A file export lands on the server's disk too, refused on the hosted cloud
    # since the local-disk rule (test_local_disk_destinations_not_on_hosted_cloud);
    # with the override the SQLite source still exports.
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "on")
    _job, result = _run(
        fake_store,
        EndpointConfig(kind="file_export", format="csv", output_path="d276.csv", table="customers"),
        src,
    )
    assert result.success, result.error
    assert int(result.records_transferred or 0) == 31


# --------------------------------------------------------------------------- catalog


def test_catalog_flags_sqlite_destination_not_available_on_cloud(monkeypatch):
    from services.catalog_service import get_connector_by_id, search_catalog
    from src.transfer.registry import get_capabilities

    _hosted(monkeypatch)
    caps = get_capabilities()
    assert "sqlite" not in caps["destination_databases"]
    assert "sqlite" in caps["source_databases"]  # still a source
    flagged = {d["id"]: d for d in caps["destination_databases_unavailable"]}
    assert flagged["sqlite"]["reason"] == "Not available on cloud"
    assert REFUSAL in flagged["sqlite"]["message"]
    assert not [
        c for c in caps["live_combinations"]
        if c["dest_kind"] == "database" and c["dest_format"].lower() == "sqlite"
    ]
    assert [c for c in caps["live_combinations"] if c["source_format"].lower() == "sqlite"]

    assert "sqlite" not in {c["id"] for c in search_catalog(role="destination", limit=200)["connectors"]}
    sources = {c["id"]: c for c in search_catalog("sqlite", role="source")["connectors"]}
    assert "sqlite" in sources
    tile = get_connector_by_id("sqlite") or {}
    assert tile.get("dest_ready") is False and tile.get("source_ready") is True
    assert tile.get("dest_unavailable_reason") == "Not available on cloud"

    _not_hosted(monkeypatch)
    tile = get_connector_by_id("sqlite") or {}
    assert tile.get("dest_ready") is True and "dest_unavailable_reason" not in tile
    assert "sqlite" in get_capabilities()["destination_databases"]


# --------------------------------------------------------------------------- MCP / REST


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    _not_hosted(monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _saved_pair(tmp_path: Path):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src, dst = tmp_path / f"src_{token}.sqlite", tmp_path / f"dst_{token}.sqlite"
    _seed(src)
    s = create_connector({"name": f"d276-src-{token}", "type": "sqlite", "role": "source",
                          "workspace_id": "", "database": str(src)})
    d = create_connector({"name": f"d276-dst-{token}", "type": "sqlite", "role": "destination",
                          "workspace_id": "", "database": str(dst)})
    return s, d, dst


def test_mcp_plan_start_and_schedule_refuse_sqlite_destination(client, tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    s, d, dst = _saved_pair(tmp_path)
    _hosted(monkeypatch)
    args = {"source_connector_id": s.id, "source_table": "customers",
            "dest_connector_id": d.id, "dest_table": "customers_copy"}
    for tool in ("plan_transfer", "start_transfer"):
        ok, out = h.mcp_call(client, tool, args)
        assert not ok, out
        assert REFUSAL in str(out), out
    ok, out = h.mcp_call(client, "create_schedule", {**args, "cadence": "daily"})
    assert not ok, out
    assert REFUSAL in str(out), out
    assert _tables(dst) == []


def test_rest_execute_run_preflight_and_schedule_refuse_sqlite_destination(client, tmp_path, monkeypatch):
    s, d, dst = _saved_pair(tmp_path)
    _hosted(monkeypatch)
    mappings = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]

    # A saved connector named by id alone: the route resolves its type.
    resp = client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "sqlite", "connector_id": s.id, "table": "customers"},
        "destination": {"kind": "database", "connector_id": d.id, "table": "customers_copy"},
        "mappings": mappings,
    })
    assert resp.status_code == 422, resp.text
    assert REFUSAL in resp.text

    resp = client.post("/api/v1/transfer/run", data={
        "source_kind": "database", "source_format": "sqlite", "source_connector_id": s.id,
        "source_table": "customers", "dest_kind": "database", "dest_format": "sqlite",
        "dest_connector_id": d.id, "dest_table": "customers_copy",
    })
    assert resp.status_code == 422, resp.text
    assert REFUSAL in resp.text

    resp = client.post("/api/v1/preflight/run", json={
        "columns": ["id", "name"], "mappings": mappings, "connector_id": d.id,
        "source_connector_id": s.id, "source_kind": "database", "source_type": "sqlite",
        "source_table": "customers", "dest_table": "customers_copy",
    })
    assert resp.status_code == 422, resp.text
    assert REFUSAL in resp.text

    resp = client.post("/api/v1/schedules/", json={
        "name": f"d276-{uuid.uuid4().hex[:6]}", "source_connector_id": s.id,
        "source_table": "customers", "dest_connector_id": d.id, "dest_table": "customers_copy",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", "mappings": mappings,
    })
    assert resp.status_code in (400, 422), resp.text
    assert REFUSAL in resp.text
    assert _tables(dst) == []


# --------------------------------------------------------------------------- schedule run


@pytest.fixture()
def schedule_stores(tmp_path, monkeypatch):
    import services.mongodb_service as mongo_mod
    import services.schedule_store as schedules

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setattr("services.connector_store._backend_choice", None, raising=False)
    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)
    store = mongo_mod.MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    dispatched: list = []

    def fake_run(job_id, request, **_kw):
        dispatched.append(request)
        fut: Future = Future()
        fut.set_result(None)
        return fut

    monkeypatch.setattr("src.transfer.background.run_transfer_async", fake_run)
    return store, dispatched


def test_schedule_into_saved_sqlite_is_refused_at_its_runs_preflight(
    tmp_path, schedule_stores, monkeypatch
):
    """Saved before the deploy, the schedule's next run is refused before a job exists."""
    import services.schedule_runner as runner
    import services.schedule_store as schedules

    _store, dispatched = schedule_stores
    _not_hosted(monkeypatch)
    s, d, dst = _saved_pair(tmp_path)
    sched = schedules.create_schedule({
        "name": f"d276-{uuid.uuid4().hex[:6]}", "source_connector_id": s.id,
        "source_table": "customers", "dest_connector_id": d.id, "dest_table": "customers_copy",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", "workspace_id": "",
        "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
    })

    _hosted(monkeypatch)
    assert runner._dispatch_transfer(sched.id, allow_paused=True) is None
    assert dispatched == []  # no job was submitted
    after = schedules.get_schedule(sched.id)
    last = (after.run_history or [{}])[-1]
    assert last.get("reason_code") == "preflight_refused", last
    assert REFUSAL in str(last.get("reason")), last
    assert _tables(dst) == []

    # The same schedule on a self-hosted install still dispatches.
    _not_hosted(monkeypatch)
    schedules.update_schedule(sched.id, {"enabled": True})
    assert runner._dispatch_transfer(sched.id, allow_paused=True)
    assert len(dispatched) == 1
