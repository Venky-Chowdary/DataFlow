"""D276, generalised — on the hosted cloud a destination is offered only if the
data does not land on the app's own disk.

The API, the worker and the scheduler are separate services with separate disks:
a file a job writes on the worker is invisible to the API (list, sample,
download) and is gone after a redeploy. Owner decision: besides SQLite (D276),
the hosted cloud refuses a local DuckDB file, Iceberg with a local warehouse or
a local SQLite catalog file, and file exports to the server's exports folder —
before any row is written, with one honest message each. Remote variants stay:
MotherDuck (``md:``), Iceberg on S3/GCS/Azure with a REST/Glue/Hive/SQL-database
catalog, and file delivery to S3/MinIO/GCS/ADLS/SFTP.
``DATAWRAP_LOCAL_FILE_DESTINATIONS=on|off`` decides explicitly; self-hosted
installs, dev, tests and CI are not the hosted cloud.
"""

from __future__ import annotations

import csv
import io
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

LOCAL = "DATAWRAP_LOCAL_FILE_DESTINATIONS=on keeps it"
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
    """A deployed Railway service (deploy-time markers); ENV=test keeps the rest
    of the process out of production policy."""
    _not_hosted(monkeypatch)
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "d276-local-deploy")
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


def _duckdb_count(path: Path, table: str = "customers") -> int:
    import duckdb

    conn = duckdb.connect(str(path), read_only=True)
    try:
        return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])  # nosec B608
    finally:
        conn.close()


def _files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file()] if root.exists() else []


def _duckdb(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="duckdb", database=str(path), table="customers")


# --------------------------------------------------------------------------- policy


@pytest.mark.parametrize(
    ("fields", "kind"),
    [
        # DuckDB: a path is a file on this server; MotherDuck is remote.
        ({"type": "duckdb", "database": "/srv/data/duckdb/x.duckdb"}, "duckdb"),
        ({"type": "duckdb", "database": "relative.duckdb"}, "duckdb"),
        ({"type": "duckdb", "database": ":memory:"}, "duckdb"),
        # DuckDB opens an s3:// "database" as a relative local path (no S3 mode).
        ({"type": "duckdb", "database": "s3://bucket/x.duckdb"}, "duckdb"),
        ({"type": "generic_sql", "connection_string": "duckdb:////srv/x.duckdb"}, "duckdb"),
        ({"type": "duckdb", "database": "md:analytics"}, None),
        ({"type": "duckdb", "connection_string": "duckdb:///md:analytics"}, None),
        ({"type": "duckdb", "database": "motherduck:analytics"}, None),
        ({"type": "motherduck", "database": "analytics"}, None),
        # Iceberg: local warehouse, or a local SQLite catalog even over S3.
        ({"type": "iceberg", "warehouse": "/srv/data/iceberg/wh"}, "iceberg"),
        ({"type": "iceberg", "connection_string": "file:///srv/wh"}, "iceberg"),
        ({"type": "iceberg", "database": "warehouse_dir"}, "iceberg"),
        ({"type": "iceberg", "warehouse": "s3://lake/wh", "connection_string": "sqlite:////srv/cat.db"}, "iceberg"),
        ({"type": "iceberg", "warehouse": "s3://lake/wh", "extra": {"catalog_type": "sql"}}, "iceberg"),
        ({"type": "iceberg", "warehouse": "s3://lake/wh", "catalog_type": "sqlite"}, "iceberg"),
        ({"type": "iceberg", "warehouse": "/srv/wh", "connection_string": "postgresql://u@db/cat"}, "iceberg"),
        ({"type": "iceberg", "warehouse": "/srv/wh", "region": "us-east-1"}, "iceberg"),
        ({"type": "iceberg", "warehouse": "s3://lake/wh", "connection_string": "postgresql://u@db/cat"}, None),
        ({"type": "iceberg", "warehouse": "s3://lake/wh", "region": "us-east-1"}, None),
        ({"type": "iceberg", "warehouse": "s3://lake/wh"}, None),  # Glue
        ({"type": "iceberg", "connection_string": "https://catalog.example/v1", "warehouse": "prod"}, None),
        ({"type": "iceberg", "connection_string": "thrift://metastore:9083", "warehouse": "gs://lake/wh",
          "extra": {"catalog_type": "hive"}}, None),
        # An Azure warehouse with no catalog named defaults to the local SQLite catalog.
        ({"type": "apache_iceberg", "warehouse": "abfss://c@acct.dfs.core.windows.net/wh"}, "iceberg"),
        ({"type": "apache_iceberg", "warehouse": "abfss://c@acct.dfs.core.windows.net/wh",
          "extra": {"catalog_type": "rest"}, "host": "catalog.example"}, None),
        # Files: an export lands in exports_dir(); object stores and SFTP are remote.
        ({"kind": "file_export", "format": "csv"}, "file_export"),
        ({"kind": "file", "format": "parquet"}, "file_export"),
        ({"type": "s3", "database": "bucket", "table": "out.csv"}, None),
        ({"type": "minio", "database": "bucket"}, None),
        ({"type": "gcs", "database": "bucket"}, None),
        ({"type": "adls", "database": "container"}, None),
        ({"type": "sftp", "host": "files.example", "table": "/in/out.csv"}, None),
        ({"type": "postgresql", "host": "db.example"}, None),
        ({"type": "sqlite", "database": "/srv/x.db"}, "sqlite"),
    ],
)
def test_classification_follows_where_the_writer_puts_the_file(fields, kind):
    from services.local_file_policy import local_disk_destination

    assert local_disk_destination(fields) == kind


def test_policy_decision_order(monkeypatch):
    from services.local_file_policy import (
        local_destination_refusal,
        local_file_destinations_allowed,
        sqlite_destination_allowed,
    )

    duck = {"type": "duckdb", "database": "/srv/x.duckdb"}
    export = {"kind": "file_export", "format": "csv"}

    _not_hosted(monkeypatch)
    monkeypatch.setenv("ENV", "production")  # a self-hosted production install
    assert local_file_destinations_allowed()
    assert local_destination_refusal(duck) is None and local_destination_refusal(export) is None

    _hosted(monkeypatch)
    assert not local_file_destinations_allowed() and not sqlite_destination_allowed()
    for fields, needle in (
        (duck, "DuckDB"),
        ({"type": "iceberg", "warehouse": "/srv/wh"}, "Iceberg"),
        (export, "File exports"),
    ):
        message = local_destination_refusal(fields)
        assert message and needle in message and "isn't kept between deploys" in message, message
        assert LOCAL in message
    # A bare type is judged on the type alone: DuckDB / Iceberg have remote variants.
    assert local_destination_refusal("duckdb") is None
    assert local_destination_refusal("iceberg") is None
    assert local_destination_refusal({"type": "duckdb", "database": "md:x"}) is None

    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "on")  # the override wins …
    assert local_destination_refusal(duck) is None and local_destination_refusal(export) is None
    assert sqlite_destination_allowed()  # … and covers SQLite unless it is decided on its own
    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "off")
    assert local_destination_refusal("sqlite") and local_destination_refusal(duck) is None
    monkeypatch.delenv("DATAWRAP_SQLITE_DESTINATION")
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "off")
    monkeypatch.setenv("DATAWRAP_SQLITE_DESTINATION", "on")  # the D276 override still works
    assert local_destination_refusal("sqlite") is None and local_destination_refusal(duck)

    _not_hosted(monkeypatch)
    monkeypatch.setenv("DATAFLOW_LOCAL_FILE_DESTINATIONS", "off")  # legacy brand name, self-hosted
    assert local_destination_refusal(export)


def test_planning_refusal_reads_the_saved_connector_fields(monkeypatch):
    """MCP plan / start / schedule ask ``destination_refusal`` with the saved connector."""
    from src.transfer.connector_capabilities import destination_refusal

    _hosted(monkeypatch)
    assert LOCAL in str(destination_refusal("duckdb", cfg={"type": "duckdb", "database": "/srv/x.duckdb"}))
    assert destination_refusal("duckdb", cfg={"type": "duckdb", "database": "md:prod"}) is None
    assert LOCAL in str(destination_refusal("iceberg", cfg={"type": "iceberg", "warehouse": "/srv/wh"}))
    assert destination_refusal(
        "iceberg", cfg={"type": "iceberg", "warehouse": "s3://lake/wh", "region": "eu-west-1"}
    ) is None
    assert destination_refusal("s3", cfg={"type": "s3", "database": "bucket"}) is None


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
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return fake


def _run(fake, destination: EndpointConfig, src: Path):
    job_id = "d276l" + uuid.uuid4().hex[:15]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    return job_id, UniversalTransferEngine().execute_tracked(request, job_id)


def _blocked_variants(tmp_path: Path) -> list[tuple[EndpointConfig, Path]]:
    """Each blocked variant with the folder its writer would fill."""
    return [
        (_duckdb(tmp_path / "duck" / "dst.duckdb"), tmp_path / "duck"),
        (
            EndpointConfig(kind="database", format="iceberg", warehouse=str(tmp_path / "wh"), table="customers"),
            tmp_path / "wh",
        ),
        (
            EndpointConfig(
                kind="database", format="iceberg", warehouse="s3://d276-lake/wh",
                connection_string=f"sqlite:///{tmp_path / 'cat' / 'catalog.db'}", table="customers",
            ),
            tmp_path / "cat",
        ),
        (
            EndpointConfig(kind="file_export", format="csv", output_path="d276_local.csv", table="customers"),
            tmp_path / "exports",
        ),
    ]


def test_engine_refuses_each_local_variant_before_any_row(tmp_path, fake_store, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    (tmp_path / "duck").mkdir()
    (tmp_path / "cat").mkdir()
    _hosted(monkeypatch)
    for destination, folder in _blocked_variants(tmp_path):
        job_id, result = _run(fake_store, destination, src)
        assert not result.success, (destination.format, result)
        assert LOCAL in str(result.error), result.error
        job = fake_store.jobs[job_id]
        assert job.get("status") == "failed" and LOCAL in str(job.get("error")), job
        assert int(result.records_transferred or 0) == 0
        assert _files(folder) == [], (destination.format, _files(folder))  # nothing landed


def test_direct_writes_refuse_local_variants_when_hosted(tmp_path, monkeypatch):
    """Quarantine replay, query export to a connector and staging write through
    ``write_destination_database`` / ``land_export_bytes`` without the engine."""
    from services.file_export_append import land_export_bytes
    from src.transfer.adapters import write_destination_database

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    _hosted(monkeypatch)
    dst = tmp_path / "direct.duckdb"
    with pytest.raises(ValueError, match="DuckDB"):
        write_destination_database(
            _duckdb(dst), [{"id": 1, "name": "a"}], ["id", "name"],
            {"id": "integer", "name": "text"}, [{"source": "id", "target": "id"}],
        )
    assert not dst.exists()
    with pytest.raises(ValueError, match="File exports"):
        land_export_bytes(
            EndpointConfig(kind="file_export", format="csv"), b"id\n1\n",
            job_id="d276l", sync_mode="full_refresh_overwrite", export_name="x.csv",
        )
    assert _files(tmp_path / "exports") == []


def test_remote_file_destination_still_runs_when_hosted(tmp_path, fake_store, monkeypatch):
    """S3 (moto) is not this server's disk: the hosted cloud keeps it."""
    moto = pytest.importorskip("moto")
    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    with moto.mock_aws():
        import boto3

        _job, result = _run(
            fake_store,
            EndpointConfig(
                kind="database", format="s3", host="s3.amazonaws.com", port=443,
                database="d276-remote", username="mock", password="mock", table="customers.csv",
            ),
            src,
        )
        assert result.success, result.error
        assert int(result.records_transferred or 0) == 31
        body = boto3.client("s3", region_name="us-east-1").get_object(
            Bucket="d276-remote", Key="customers.csv"
        )["Body"].read().decode()
    assert len(list(csv.DictReader(io.StringIO(body)))) == 31


def test_override_turns_local_variants_back_on(tmp_path, fake_store, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "on")
    dst = tmp_path / "dst.duckdb"
    _job, result = _run(fake_store, _duckdb(dst), src)
    assert result.success, result.error
    assert _duckdb_count(dst) == 31
    _job, result = _run(
        fake_store,
        EndpointConfig(kind="file_export", format="csv", output_path="d276_on.csv", table="customers"),
        src,
    )
    assert result.success, result.error
    assert (tmp_path / "exports" / "d276_on.csv").exists()

    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    assert caps["destination_file_formats"] and caps["destination_file_formats_unavailable"] == []
    assert caps["destination_local_variants_unavailable"] == []


def test_self_hosted_install_is_unaffected(tmp_path, fake_store, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _not_hosted(monkeypatch)
    monkeypatch.setenv("ENV", "production")
    dst = tmp_path / "dst.duckdb"
    _job, result = _run(fake_store, _duckdb(dst), src)
    assert result.success, result.error
    assert _duckdb_count(dst) == 31
    _job, result = _run(
        fake_store,
        EndpointConfig(kind="file_export", format="json", output_path="d276_self.json", table="customers"),
        src,
    )
    assert result.success, result.error

    from src.transfer.registry import get_capabilities, validate_transfer

    assert validate_transfer("database", "sqlite", "file_export", "csv")[0]
    caps = get_capabilities()
    assert "csv" in caps["destination_file_formats"]
    assert caps["destination_file_formats_unavailable"] == []
    assert caps["destination_local_variants_unavailable"] == []


# --------------------------------------------------------------------------- catalog


def test_capabilities_flag_only_the_local_variants(monkeypatch):
    from services.catalog_service import get_connector_by_id
    from src.transfer.registry import get_capabilities, validate_transfer

    _hosted(monkeypatch)
    caps = get_capabilities()
    # DuckDB and Iceberg stay offered (MotherDuck, remote warehouse + catalog) …
    assert {"duckdb", "apache_iceberg"} <= set(caps["destination_databases"])
    notes = {n["id"]: n for n in caps["destination_local_variants_unavailable"]}
    assert set(notes) == {"duckdb", "apache_iceberg"}
    assert notes["duckdb"]["reason"] == "Not available on cloud"
    assert notes["duckdb"]["variant"] == "Local file" and "MotherDuck" in notes["duckdb"]["message"]
    assert notes["apache_iceberg"]["engine"] == "iceberg"
    # … file exports are refused, listed with why, and have no live route.
    assert caps["destination_file_formats"] == []
    flagged = {f["id"]: f for f in caps["destination_file_formats_unavailable"]}
    assert "csv" in flagged and flagged["csv"]["reason"] == "Not available on cloud"
    assert LOCAL in flagged["csv"]["message"]
    assert not [c for c in caps["live_combinations"] if c["dest_kind"] == "file_export"]
    ok, msg = validate_transfer("database", "postgresql", "file_export", "csv")
    assert not ok and LOCAL in msg
    assert validate_transfer("database", "postgresql", "database", "duckdb")[0]
    assert validate_transfer("database", "postgresql", "database", "s3")[0]

    tile = get_connector_by_id("csv") or {}
    assert tile.get("dest_ready") is False and tile.get("source_ready") is True
    assert tile.get("dest_unavailable_reason") == "Not available on cloud"
    assert (get_connector_by_id("duckdb") or {}).get("dest_ready") is True

    _not_hosted(monkeypatch)
    assert (get_connector_by_id("csv") or {}).get("dest_ready") is True


# --------------------------------------------------------------------------- MCP / REST


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    _not_hosted(monkeypatch)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _saved(tmp_path: Path, dest_fields: dict):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src = tmp_path / f"src_{token}.sqlite"
    _seed(src)
    s = create_connector({"name": f"d276l-src-{token}", "type": "sqlite", "role": "source",
                          "workspace_id": "", "database": str(src)})
    d = create_connector({"name": f"d276l-dst-{token}", "role": "destination", "workspace_id": "",
                          **dest_fields})
    return s, d


def _local_destinations(tmp_path: Path) -> list[dict]:
    (tmp_path / "duck").mkdir(exist_ok=True)
    return [
        {"type": "duckdb", "database": str(tmp_path / "duck" / "dst.duckdb")},
        {"type": "iceberg", "warehouse": str(tmp_path / "wh")},
    ]


def test_mcp_plan_start_and_schedule_refuse_local_variants(client, tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    pairs = [_saved(tmp_path, fields) for fields in _local_destinations(tmp_path)]
    _hosted(monkeypatch)
    for s, d in pairs:
        args = {"source_connector_id": s.id, "source_table": "customers",
                "dest_connector_id": d.id, "dest_table": "customers_copy"}
        for tool in ("plan_transfer", "start_transfer"):
            ok, out = h.mcp_call(client, tool, args)
            assert not ok and LOCAL in str(out), (d.type, tool, out)
        ok, out = h.mcp_call(client, "create_schedule", {**args, "cadence": "daily"})
        assert not ok and LOCAL in str(out), (d.type, out)
    assert _files(tmp_path / "duck") == [] and _files(tmp_path / "wh") == []


def test_rest_refuses_file_export_and_saved_local_variant(client, tmp_path, monkeypatch):
    s, d = _saved(tmp_path, _local_destinations(tmp_path)[0])
    _hosted(monkeypatch)
    mappings = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]
    source = {"kind": "database", "format": "sqlite", "connector_id": s.id, "table": "customers"}

    resp = client.post("/api/v1/transfer/execute", json={
        "source": source, "mappings": mappings,
        "destination": {"kind": "file_export", "format": "csv", "output_path": "d276.csv"},
    })
    assert resp.status_code == 422 and LOCAL in resp.text, resp.text
    resp = client.post("/api/v1/transfer/execute", json={
        "source": source, "mappings": mappings,
        "destination": {"kind": "database", "connector_id": d.id, "table": "customers_copy"},
    })
    assert resp.status_code == 422 and "DuckDB" in resp.text, resp.text
    resp = client.post("/api/v1/transfer/run", data={
        "source_kind": "database", "source_format": "sqlite", "source_connector_id": s.id,
        "source_table": "customers", "dest_kind": "file_export", "dest_format": "json",
    })
    assert resp.status_code == 422 and LOCAL in resp.text, resp.text
    resp = client.post("/api/v1/preflight/run", json={
        "columns": ["id", "name"], "mappings": mappings, "connector_id": d.id,
        "source_connector_id": s.id, "source_kind": "database", "source_type": "sqlite",
        "source_table": "customers", "dest_table": "customers_copy",
    })
    assert resp.status_code == 422 and LOCAL in resp.text, resp.text
    resp = client.post("/api/v1/schedules/", json={
        "name": f"d276l-{uuid.uuid4().hex[:6]}", "source_connector_id": s.id,
        "source_table": "customers", "dest_connector_id": d.id, "dest_table": "customers_copy",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", "mappings": mappings,
    })
    assert resp.status_code in (400, 422) and LOCAL in resp.text, resp.text
    assert _files(tmp_path / "exports") == [] and _files(tmp_path / "duck") == []


def test_interactive_query_export_downloads_from_the_same_process(client, tmp_path, monkeypatch):
    """The API writes the file and serves it in the same service: kept on the
    hosted cloud. The download reads the folder the writer used
    (``DATAFLOW_EXPORTS_DIR``), not a hard-coded ``apps/api/exports``."""
    s, _d = _saved(tmp_path, {"type": "postgresql", "host": "127.0.0.1", "database": "unused"})
    _hosted(monkeypatch)
    resp = client.post("/api/v1/query/export", json={
        "connector_id": s.id, "query": "SELECT id, name FROM customers", "format": "csv",
        "full_population": True,
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"], body
    assert body["row_count"] == 31
    assert Path(body["path"]).parent == (tmp_path / "exports").resolve()
    got = client.get(body["download_url"])
    assert got.status_code == 200, got.text
    assert len(list(csv.DictReader(io.StringIO(got.text)))) == 31
    assert client.get("/api/v1/transfer/download/../d276.csv").status_code in (400, 404)


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


@pytest.mark.parametrize("variant", [0, 1], ids=["duckdb_file", "iceberg_local_warehouse"])
def test_schedule_into_a_local_variant_is_refused_at_its_runs_preflight(
    tmp_path, schedule_stores, monkeypatch, variant
):
    """Saved before the deploy, the schedule's next run is refused before a job exists."""
    import services.schedule_runner as runner
    import services.schedule_store as schedules

    _store, dispatched = schedule_stores
    _not_hosted(monkeypatch)
    s, d = _saved(tmp_path, _local_destinations(tmp_path)[variant])
    sched = schedules.create_schedule({
        "name": f"d276l-{uuid.uuid4().hex[:6]}", "source_connector_id": s.id,
        "source_table": "customers", "dest_connector_id": d.id, "dest_table": "customers_copy",
        "interval": "daily", "sync_mode": "full_refresh_overwrite", "workspace_id": "",
        "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
    })

    _hosted(monkeypatch)
    assert runner._dispatch_transfer(sched.id, allow_paused=True) is None
    assert dispatched == []  # no job was submitted
    last = (schedules.get_schedule(sched.id).run_history or [{}])[-1]
    assert last.get("reason_code") == "preflight_refused", last
    assert LOCAL in str(last.get("reason")), last
    assert _files(tmp_path / "duck") == [] and _files(tmp_path / "wh") == []

    _not_hosted(monkeypatch)  # the same schedule on a self-hosted install still dispatches
    schedules.update_schedule(sched.id, {"enabled": True})
    assert runner._dispatch_transfer(sched.id, allow_paused=True)
    assert len(dispatched) == 1


def test_quarantine_export_downloads_from_the_exports_folder(client, tmp_path, monkeypatch):
    """The other in-request export: written by the API into ``exports_dir()``,
    which is the folder the download route serves."""
    from services.mongodb_service import get_mongodb_service

    _hosted(monkeypatch)
    job_id = "d276q" + uuid.uuid4().hex[:15]
    get_mongodb_service().update_job_status(
        job_id, "completed", transfer_request={"mappings": []},
        rejected_rows=1,
        rejected_details=[{"row": 2, "column": "age", "target": "age", "value": "x", "reason": "not an integer"}],
    )
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/export")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] and body["row_count"] == 1, body
    assert (tmp_path / "exports" / body["filename"]).exists()
    got = client.get(body["download_url"])
    assert got.status_code == 200 and "not an integer" in got.text, got.text
