"""Multi-stream runs land every stream in its own destination object.

A multi-table run remaps the destination to each stream's name, the rule SQL
tables already follow (``destination.table`` names the first stream; every
stream lands in the table its name matches). MongoDB only remapped
``collection``: with ``table`` also set (the Studio fills both) the writer kept
resolving the request's one name, so stream 2 overwrote stream 1 and stream 3
overwrote stream 2 — a green job holding only the last table. Every other
non-SQL sink (Elasticsearch index, Redis key prefix, DynamoDB table, Kafka
topic, Iceberg table, object-store key) resolves its object through the same
``resolve_dest_table`` and had the same hole whenever ``collection`` was set
next to ``table``. The job-level Gate-8 rebinds its endpoints to the last
stream by the same rule, so it reads the object that stream wrote.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from src.transfer.adapters import resolve_dest_table
from src.transfer.connector_capabilities import resolve_driver_type
from src.transfer.models import EndpointConfig

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

STREAMS = ("people", "orders", "items")

# Every non-SQL sink plus one SQL control. ``database`` carries what each
# connector reads there (bucket, index, topic, key space) so a regression to a
# shared fallback name is visible too.
DEST_FORMATS = (
    "mongodb",
    "elasticsearch",
    "redis",
    "dynamodb",
    "kafka",
    "iceberg",
    "s3",
    "gcs",
    "adls",
    "postgresql",
)

# (table, collection) as a request can carry them.
NAME_FIELDS = {
    "table_and_collection": ("people", "people"),
    "table_only": ("people", ""),
    "collection_only": ("", "people"),
    "neither": ("", ""),
}


def _sqlite_source(path: Path, sizes: dict[str, int]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            for name, n in sizes.items():
                conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, label TEXT)")  # noqa: S608
                conn.executemany(
                    f"INSERT INTO {name} VALUES (?, ?)",  # noqa: S608
                    [(i, f"{name}-{i}") for i in range(1, n + 1)],
                )
    finally:
        conn.close()


def _contracts() -> list[dict]:
    return [{"name": n, "primary_key": ["id"], "selected": True} for n in STREAMS]


@pytest.mark.parametrize("fields", sorted(NAME_FIELDS))
@pytest.mark.parametrize("fmt", DEST_FORMATS)
def test_each_stream_resolves_its_own_destination_object(tmp_path, fmt, fields):
    """The orchestrator hands every stream a destination that resolves to that stream."""
    from src.transfer.stream_multi import run_non_cdc_multi_stream_sequential

    src_path = tmp_path / "s.sqlite"
    _sqlite_source(src_path, {n: 1 for n in STREAMS})
    table, collection = NAME_FIELDS[fields]
    source = EndpointConfig(kind="database", format="sqlite", database=str(src_path), table="people")
    destination = EndpointConfig(
        kind="database", format=fmt, database="ms1_target", table=table, collection=collection
    )
    resolved: list[str] = []

    def _fake_stream(src, dest, mappings, schema, *args, **kwargs):
        driver = resolve_driver_type(dest.format)
        resolved.append(resolve_dest_table(driver, dest, src.table or src.collection))
        return 1, [], {"sync_mode": "full_refresh_append"}, ["id"]

    with patch("src.transfer.stream.stream_database_transfer", side_effect=_fake_stream), patch(
        "src.transfer.stream._drop_destination_endpoint", return_value=True
    ), patch("src.transfer.stream_multi.record_stream_health"):
        rows, _ddl, _summary, _ = run_non_cdc_multi_stream_sequential(
            source,
            destination,
            [],
            {},
            None,
            sync_mode="full_refresh_append",
            stream_contracts=_contracts(),
            job_id="ms1job",
        )

    assert rows == len(STREAMS)
    assert resolved == list(STREAMS), (
        f"{fmt} ({fields}): streams resolved {resolved} — a shared object means "
        "a later stream overwrites or mixes into an earlier one"
    )
    # The request's own endpoint is restored for the job record.
    assert (destination.table, destination.collection) == (table, collection)


@pytest.mark.parametrize("fields", sorted(NAME_FIELDS))
@pytest.mark.parametrize("fmt", DEST_FORMATS)
def test_gate8_binds_the_object_the_last_stream_wrote(fmt, fields):
    """Job-level Gate-8 reads the last stream's object, by the orchestrator's rule."""
    from src.transfer.reconcile_step import _bind_endpoints_to_last_stream

    table, collection = NAME_FIELDS[fields]
    destination = EndpointConfig(
        kind="database", format=fmt, database="ms1_target", table=table, collection=collection
    )
    summary = {"multi_stream": True, "streams": [{"name": n} for n in STREAMS]}
    bound, _src = _bind_endpoints_to_last_stream(destination, None, summary)
    assert resolve_dest_table(resolve_driver_type(fmt), bound, "people") == "items"
    assert (destination.table, destination.collection) == (table, collection)


def _mongo_uri() -> str:
    return os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or ""


@pytest.fixture()
def job_store(monkeypatch, tmp_path):
    import src.transfer.engine as engine_mod
    from services import audit_log
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return store


@pytest.mark.parametrize("fields", ["table_and_collection", "table_only", "collection_only"])
@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_live_mongo_multi_stream_keeps_every_stream(tmp_path, job_store, fields, sync_mode):
    """Live MongoDB: three streams, three collections, every row of each."""
    pymongo = pytest.importorskip("pymongo")
    uri = _mongo_uri()
    if not uri:
        pytest.skip("live MongoDB not configured (DATAFLOW_TEST_MONGO_URI)")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - no server means skip, not fail
        client.close()
        pytest.skip(f"live MongoDB unreachable: {type(exc).__name__}")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    sizes = {"people": 5, "orders": 2, "items": 3}
    src_path = tmp_path / "s.sqlite"
    _sqlite_source(src_path, sizes)
    db = f"ms1_{uuid.uuid4().hex[:10]}"
    table, collection = NAME_FIELDS[fields]
    mappings = [
        {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
        {"source": "label", "target": "label", "source_type": "TEXT", "target_type": "TEXT"},
    ]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src_path), table="people"),
        destination=EndpointConfig(
            kind="database", format="mongodb", connection_string=uri, database=db,
            table=table, collection=collection,
        ),
        sync_mode=sync_mode,
        stream_contracts=[
            {"name": n, "primary_key": ["id"], "selected": True, "mappings": [dict(m) for m in mappings]}
            for n in sizes
        ],
        validation_mode="strict",
    )
    job_id = "ms1" + uuid.uuid4().hex[:12]
    job_store.update_job_status(job_id, "pending", transfer_request={})
    try:
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        landed = {c: client[db][c].count_documents({}) for c in client[db].list_collection_names()}
        assert landed == sizes, f"collections after the run: {landed}"
        for name in sizes:
            labels = {d["label"] for d in client[db][name].find({}, {"label": 1})}
            assert labels == {f"{name}-{i}" for i in range(1, sizes[name] + 1)}
        assert result.success, result.error
        assert result.records_transferred == sum(sizes.values())
    finally:
        client.drop_database(db)
        client.close()
