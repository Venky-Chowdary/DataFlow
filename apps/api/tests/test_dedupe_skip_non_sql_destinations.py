""""Skip duplicates" leaves stored keys as stored on every destination it runs on.

Defect: ``duplicate_policy=skip_duplicates`` (``dedupe_skip``) resolves to
``write_mode=upsert`` plus a match action ``skip`` carried in a contextvar
that only the SQL writers read (``mirror_engine.upsert_set_columns``). The
MongoDB, Redis, DynamoDB, Elasticsearch, BigQuery (MERGE ``WHEN MATCHED THEN
UPDATE``) and Iceberg (``txn.upsert`` update-all) writers ran a plain upsert,
so every stored key was overwritten with the incoming row — the opposite of
the product promise "Rows whose key is already in the destination are left as
stored; only new keys insert". A destination whose writer cannot keep a
stored key is now refused at Validate and at schedule save.

Each implemented writer reports a kept key as ``rows_skipped`` plus a
``skipped_identity_keys`` marker, which is what Gate-8 uses to not count kept
rows as missing (and not grade their stored values against the source).
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")
_MAP = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


mongo_live = pytest.mark.skipif(not _up(27017), reason="no MongoDB on :27017")
redis_live = pytest.mark.skipif(not _up(6379), reason="no Redis on :6379")


@pytest.fixture
def skip_policy():
    from services.duplicate_policy import (
        reset_match_action,
        resolve_duplicate_policy,
        set_match_action,
    )

    decision = resolve_duplicate_policy(
        duplicate_policy="skip_duplicates", business_key=True, write_mode="insert"
    )
    assert (decision.write_mode, decision.match_action) == ("upsert", "skip")
    token = set_match_action(decision.match_action)
    try:
        yield decision.write_mode
    finally:
        reset_match_action(token)


def _gate8_kept(result, key_cols):
    """What Gate-8 reads from this write: (rows_skipped, kept ladder keys)."""
    from src.transfer.reconcile_step import dedupe_skip_kept_keys
    from src.transfer.stream_write import _writer_diagnostics

    summary = {**_writer_diagnostics(result), "duplicate_policy": "dedupe_skip"}
    return summary["rows_skipped"], dedupe_skip_kept_keys(summary, key_cols)


def _assert_one_kept_one_new(result):
    assert result.ok, result.error
    assert result.rows_written == 1
    assert result.rows_skipped == 1
    assert result.rejected_rows == 0
    skipped, kept = _gate8_kept(result, ["id"])
    assert skipped == 1
    assert len(kept) == 1
    # source rows = written + skipped: the kept row is not "missing".
    assert result.rows_written + skipped == 2


# --------------------------------------------------------------------- MongoDB
@mongo_live
def test_mongodb_skip_keeps_stored_document(skip_policy):
    import pymongo

    from connectors.mongodb_writer import write_mapped_rows

    db = f"ws_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client[db].t.insert_one({"_id": "1", "id": "1", "name": "EXISTING", "tier": "gold"})
        result = write_mapped_rows(
            host="127.0.0.1", port=27017, database=db, username="", password="", schema="",
            connection_string=MONGO_URI, ssl=False, table_name="t", headers=["id", "name"],
            data_rows=[["1", "INCOMING"], ["2", "NEW"]],
            mappings=[{"source": "id", "target": "_id"}, {"source": "name", "target": "name"}],
            column_types={"id": "VARCHAR", "name": "VARCHAR"},
            write_mode=skip_policy, conflict_columns=["_id"], create_table=False,
        )
        docs = {d["_id"]: d for d in client[db].t.find()}
        assert docs["1"] == {"_id": "1", "id": "1", "name": "EXISTING", "tier": "gold"}
        assert docs["2"]["name"] == "NEW"
        _assert_one_kept_one_new(result)
        assert result.meta["skipped_identity_keys"] == ["1"]
    finally:
        client.drop_database(db)
        client.close()


def _sqlite_source(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?)", [(1, "INCOMING"), (2, "NEW-2"), (3, "NEW-3")]
        )
    conn.close()


def _skip_request(src: Path, destination):
    from src.transfer.models import TransferRequest

    from src.transfer.models import EndpointConfig

    return TransferRequest(
        mappings=[dict(m, confidence=1.0) for m in _MAP],
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=destination,
        sync_mode="full_refresh_append",
        duplicate_policy="skip_duplicates",
        stream_contracts=[{
            "name": "customers", "sync_mode": "full_refresh_append",
            "primary_key": "id", "selected": True,
        }],
        skip_preflight=True,
        validation_mode="balanced",
    )


@mongo_live
def test_sqlite_to_mongodb_skip_duplicates_gate8_passes(tmp_path):
    import pymongo

    import src.transfer.engine as engine_mod
    from src.transfer.models import EndpointConfig

    db = f"ws_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        coll = client[db].customers
        coll.insert_one({"id": 1, "name": "EXISTING", "tier": "gold"})
        src = tmp_path / "src.sqlite"
        _sqlite_source(src)
        dest = EndpointConfig(
            kind="database", format="mongodb", connection_string=MONGO_URI, database=db,
            collection="customers", extra={"duplicate_policy": "skip_duplicates"},
        )
        for run in (1, 2):
            result = engine_mod.UniversalTransferEngine().execute_tracked(
                _skip_request(src, dest), f"wsk{run}" + uuid.uuid4().hex[:12]
            )
            assert result.success, result.error
            rec = result.reconciliation or {}
            assert rec.get("passed") is True, rec
            docs = {d["id"]: d for d in coll.find({})}
            assert sorted(docs) == [1, 2, 3]
            assert (docs[1]["name"], docs[1]["tier"]) == ("EXISTING", "gold"), docs[1]
            assert docs[2]["name"] == "NEW-2" and docs[3]["name"] == "NEW-3"
    finally:
        client.drop_database(db)
        client.close()


# ----------------------------------------------------------------------- Redis
def _redis():
    import redis

    return redis.Redis(host="127.0.0.1", port=6379, db=0)


@redis_live
def test_redis_skip_keeps_stored_document_and_ttl(skip_policy):
    from connectors.redis_reader import redis_key_for
    from connectors.redis_writer import write_mapped_rows

    prefix = f"ws_skip_{uuid.uuid4().hex[:8]}"
    r = _redis()
    try:
        r.set(redis_key_for(prefix, "1"), json.dumps({"id": "1", "name": "EXISTING"}), ex=900)
        result = write_mapped_rows(
            host="127.0.0.1", port=6379, database="0", username="", password="", schema="",
            connection_string="", ssl=False, table_name=prefix, headers=["id", "name"],
            data_rows=[["1", "INCOMING"], ["2", "NEW"]], mappings=_MAP,
            column_types={"id": "VARCHAR", "name": "VARCHAR"},
            write_mode=skip_policy, conflict_columns=["id"],
        )
        k1 = redis_key_for(prefix, "1")
        assert json.loads(r.get(k1)) == {"id": "1", "name": "EXISTING"}
        assert r.ttl(k1) > 800
        assert json.loads(r.get(redis_key_for(prefix, "2")))["name"] == "NEW"
        _assert_one_kept_one_new(result)
        assert result.meta["skipped_identity_keys"] == ["1"]
    finally:
        for k in r.keys(f"{prefix}*"):
            r.delete(k)
        r.close()


@redis_live
def test_sqlite_to_redis_skip_duplicates_gate8_passes(tmp_path):
    import src.transfer.engine as engine_mod
    from connectors.redis_reader import redis_key_for
    from src.transfer.models import EndpointConfig

    prefix = f"ws_skip_{uuid.uuid4().hex[:8]}"
    r = _redis()
    try:
        k1 = redis_key_for(prefix, "1")
        r.set(k1, json.dumps({"id": 1, "name": "EXISTING", "tier": "gold"}))
        src = tmp_path / "src.sqlite"
        _sqlite_source(src)
        dest = EndpointConfig(
            kind="database", format="redis", host="127.0.0.1", port=6379, database="0",
            table=prefix, extra={"duplicate_policy": "skip_duplicates"},
        )
        for run in (1, 2):
            result = engine_mod.UniversalTransferEngine().execute_tracked(
                _skip_request(src, dest), f"wsr{run}" + uuid.uuid4().hex[:12]
            )
            assert result.success, result.error
            rec = result.reconciliation or {}
            assert rec.get("passed") is True, rec
            assert json.loads(r.get(k1)) == {"id": 1, "name": "EXISTING", "tier": "gold"}
            assert len(r.keys(f"{prefix}:*")) == 3
    finally:
        for k in r.keys(f"{prefix}*"):
            r.delete(k)
        r.close()


# -------------------------------------------------------------------- DynamoDB
def test_dynamodb_skip_keeps_stored_item(skip_policy, monkeypatch):
    moto = pytest.importorskip("moto")
    for k, v in {"AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
                 "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        import boto3

        from connectors.dynamodb_writer import write_mapped_rows

        c = boto3.client("dynamodb", region_name="us-east-1")
        c.create_table(
            TableName="ws_skip",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        c.put_item(TableName="ws_skip", Item={"id": {"S": "1"}, "name": {"S": "EXISTING"}})
        result = write_mapped_rows(
            host="", port=0, database="", username="x", password="x", schema="",
            connection_string="", ssl=False, table_name="ws_skip", headers=["id", "name"],
            data_rows=[["1", "INCOMING"], ["2", "NEW"]], mappings=_MAP,
            column_types={"id": "VARCHAR", "name": "VARCHAR"}, create_table=False,
            conflict_columns=["id"], write_mode=skip_policy,
        )
        items = {i["id"]["S"]: i["name"]["S"] for i in c.scan(TableName="ws_skip")["Items"]}
        assert items == {"1": "EXISTING", "2": "NEW"}
        _assert_one_kept_one_new(result)
        assert result.meta["skipped_identity_keys"] == ["1"]


# --------------------------------------------------------------- Elasticsearch
def test_elasticsearch_skip_uses_create_and_counts_409_as_skipped(skip_policy):
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    sent: list[dict] = []

    def _bulk(_client, actions, **_kw):
        actions = list(actions)
        sent.extend(actions)
        errors = [
            {"create": {"_id": a["_id"], "status": 409, "error": {
                "type": "version_conflict_engine_exception",
                "reason": f"[{a['_id']}]: version conflict, document already exists"}}}
            for a in actions if a["_id"] == "1"
        ]
        return len(actions) - len(errors), errors

    with patch("connectors.elasticsearch_writer._client", return_value=client), patch(
        "elasticsearch.helpers.bulk", side_effect=_bulk
    ):
        result = write_mapped_rows(
            host="localhost", port=9200, database="", username="", password="", schema="",
            connection_string="", ssl=False, table_name="ws_events", headers=["id", "name"],
            data_rows=[["1", "INCOMING"], ["2", "NEW"]], mappings=_MAP,
            column_types={"id": "TEXT", "name": "TEXT"}, create_table=False,
            write_mode=skip_policy, conflict_columns=["id"],
        )
    assert [a.get("_op_type") for a in sent] == ["create", "create"]
    assert all("doc_as_upsert" not in a for a in sent)
    _assert_one_kept_one_new(result)
    assert result.meta["skipped_identity_keys"] == ["1"]


# -------------------------------------------------------------------- BigQuery
def test_bigquery_merge_insert_only_sql():
    from connectors.bigquery_writer import build_bigquery_merge_sql

    sql = build_bigquery_merge_sql(
        "p.d.t", "p.d.stage", ["id", "name"], ["id"], insert_only=True
    )
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    upsert = build_bigquery_merge_sql("p.d.t", "p.d.stage", ["id", "name"], ["id"])
    assert "WHEN MATCHED THEN UPDATE" in upsert


def test_bigquery_skip_merge_only_inserts_new_keys(skip_policy):
    """The dense MERGE step: stored keys are read back as kept, MERGE inserts only."""
    import connectors.bigquery_writer as bw

    queries: list[str] = []

    class _Job:
        def __init__(self, rows=()):
            self._rows = list(rows)

        def result(self):
            return iter(self._rows)

    client = MagicMock()

    def _query(sql, *a, **k):
        queries.append(sql)
        return _Job([("1",)] if sql.lstrip().upper().startswith("SELECT") else [])

    client.query.side_effect = _query
    written, kept = bw._bq_merge_dense(
        client, "p.d.t", "p.d.stage", ["id", "name"], ["id"],
        [("1", "INCOMING"), ("2", "NEW")],
    )
    merges = [q for q in queries if q.lstrip().upper().startswith("MERGE")]
    assert len(merges) == 1 and "WHEN MATCHED" not in merges[0]
    assert "WHEN NOT MATCHED THEN INSERT" in merges[0]
    assert (written, kept) == (1, ["1"])

    # upsert (match action update) keeps WHEN MATCHED THEN UPDATE and reads nothing back
    from services.duplicate_policy import reset_match_action, set_match_action

    token = set_match_action("update")
    try:
        queries.clear()
        written, kept = bw._bq_merge_dense(
            client, "p.d.t", "p.d.stage", ["id", "name"], ["id"],
            [("1", "INCOMING"), ("2", "NEW")],
        )
    finally:
        reset_match_action(token)
    assert (written, kept) == (2, [])
    assert len(queries) == 1 and "WHEN MATCHED THEN UPDATE" in queries[0]


# --------------------------------------------------------------------- Iceberg
def test_iceberg_skip_keeps_stored_row(skip_policy, tmp_path):
    pytest.importorskip("pyiceberg")
    from connectors.iceberg_writer import write_mapped_rows

    warehouse, uri = str(tmp_path / "wh"), f"sqlite:///{tmp_path / 'catalog.db'}"
    common = dict(
        connection_string=uri, warehouse=warehouse, table_name="default.ws_orders",
        headers=["id", "name"], mappings=_MAP, column_types={"id": "VARCHAR", "name": "VARCHAR"},
        conflict_columns=["id"],
    )
    seeded = write_mapped_rows(data_rows=[["1", "EXISTING"]], write_mode="append",
                               create_table=True, **common)
    assert seeded.ok, seeded.error
    result = write_mapped_rows(data_rows=[["1", "INCOMING"], ["2", "NEW"]],
                               write_mode=skip_policy, create_table=False, **common)
    from pyiceberg.catalog.sql import SqlCatalog

    cat = SqlCatalog("dataflow", uri=uri, warehouse=Path(warehouse).resolve().as_uri())
    arrow = cat.load_table(("default", "ws_orders")).scan().to_arrow()
    rows = dict(zip(arrow.column("id").to_pylist(), arrow.column("name").to_pylist()))
    assert rows == {"1": "EXISTING", "2": "NEW"}
    _assert_one_kept_one_new(result)
    assert result.meta["skipped_identity_keys"] == ["1"]


def test_iceberg_filesystem_writer_refuses_skip(skip_policy, monkeypatch, tmp_path):
    import connectors.iceberg_writer as iw

    monkeypatch.setattr(iw, "resolve_iceberg_write_path", lambda endpoint: "filesystem")
    result = iw.write_mapped_rows(
        database=str(tmp_path), table_name="ws_orders", headers=["id", "name"],
        data_rows=[["1", "x"]], mappings=_MAP, column_types={"id": "VARCHAR", "name": "VARCHAR"},
        conflict_columns=["id"], write_mode=skip_policy,
    )
    assert not result.ok
    assert "skip_duplicates" in (result.error or "")
    assert result.rows_written == 0
    assert not any(tmp_path.iterdir())


# ------------------------------------------- destinations that cannot keep a key
def test_unsupported_destination_refused_at_validate_and_schedule_save():
    from services.duplicate_policy import DuplicatePolicyError, dedupe_skip_refusal

    for dest in ("qdrant", "kafka", "salesforce", "neo4j"):
        msg = dedupe_skip_refusal(dest)
        assert msg and "Skip" in msg and "duplicates" in msg.lower(), (dest, msg)
    for dest in ("postgresql", "mysql", "sqlite", "sqlserver", "duckdb", "snowflake",
                 "bigquery", "mongodb", "redis", "dynamodb", "elasticsearch", "iceberg", "s3"):
        assert dedupe_skip_refusal(dest) == "", dest

    from services.duplicate_policy import schedule_dedupe_skip_refusal

    with patch("services.connector_store.get_connector",
               return_value=MagicMock(type="qdrant")):
        assert schedule_dedupe_skip_refusal(
            {"duplicate_policy": "skip_duplicates", "dest_connector_id": "c1"}
        )
        assert not schedule_dedupe_skip_refusal(
            {"duplicate_policy": "upsert", "dest_connector_id": "c1"}
        )
    assert issubclass(DuplicatePolicyError, ValueError)


def test_validate_blocks_skip_duplicates_on_unsupported_destination():
    from services.duplicate_policy import dedupe_skip_validate_gate

    gate = dedupe_skip_validate_gate("qdrant")
    assert gate["status"] == "block"
    assert gate["details"]["duplicate_policy"] == "dedupe_skip"
    assert dedupe_skip_validate_gate("mongodb") is None
