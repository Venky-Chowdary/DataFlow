"""Third review of 4e2266415 — keep_last where the read defines "last".

1. Explicit keep_last on a cursor-ordered keyed sync (incremental_deduped with
   a cursor, the shape every schedule stamps) is the automatic choice spelled
   out: it runs and the latest version of each key lands. Where the read has
   no order (overwrite of a table) it is refused with routes for that sync.
3. The lenient note for a table names the table's routes, never keep_last.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h
from tests import test_r12_snowflake_transform_e2e as sfmod

pytestmark = pytest.mark.timeout(900)

PG_DEST = {
    "dest_format": "postgresql", "dest_host": h.PG["host"], "dest_port": str(h.PG["port"]),
    "dest_database": h.PG["database"], "dest_username": h.PG["username"],
    "dest_password": h.PG["password"], "dest_schema": "public",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


VERSIONS = [("K1", "v1-1", 1), ("K2", "v2-1", 2), ("K3", "v2-2", 3), ("K3", "v2-3", 4), ("K4", "Zoë ✓", 5)]


def _versioned_run(client, src, dst, dedupe: str | None):
    contract = {"name": src, "sync_mode": "incremental_deduped", "primary_key": "k",
                "cursor_field": "n", "cursor_semantics": "modification_timestamp", "selected": True}
    data = {"source_kind": "database", "dest_kind": "database", "source_table": src, "dest_table": dst,
            "async_mode": "false", "validation_mode": "strict", "sync_mode": "incremental_deduped",
            "source_format": "postgresql", "source_schema": "public", **sfmod._side("source", sfmod.PG_ENDPOINT),
            **PG_DEST, "stream_contracts_json": json.dumps([contract])}
    if dedupe is not None:
        # The schedule runner stamps both sides (schedule_runner: src/dst extra).
        stamp = {"in_file_dedupe": dedupe, "duplicate_policy": "upsert"}
        data["source_extra_json"] = json.dumps(stamp)
        data["dest_extra_json"] = json.dumps(stamp)
    return client.post("/api/v1/transfer/run", data=data)


@pytest.mark.parametrize("dedupe", [None, "keep_last"])
def test_schedule_shaped_keep_last_on_a_versioned_table_lands_the_latest_version(client, dedupe):
    src = sfmod._seed_pg("k text, v text, n int", VERSIONS)
    dst = f"r14vk_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            resp = _versioned_run(client, src, dst, dedupe)
            assert resp.status_code == 200, (run, resp.text[:1200])
            got = dict(h.query("postgresql", f'SELECT k, v FROM public."{dst}"'))  # nosec B608
            assert got == {"K1": "v1-1", "K2": "v2-1", "K3": "v2-3", "K4": "Zoë ✓"}, (run, got)
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_keyed_table_without_a_cursor_refuses_keep_last_with_keyed_routes():
    from services.data_integrity import (
        DUPLICATE_KEY_ROUTES_TABLE_KEYED,
        keep_last_table_refusal,
    )

    no_cursor = [{"name": "t", "sync_mode": "incremental_deduped", "primary_key": "k", "selected": True}]
    refusal = keep_last_table_refusal("incremental_deduped", stream_contracts=no_cursor, stream_name="t")
    assert DUPLICATE_KEY_ROUTES_TABLE_KEYED in refusal and "append" not in refusal
    cursor = [{**no_cursor[0], "cursor_field": "n"}]
    assert keep_last_table_refusal("incremental_deduped", stream_contracts=cursor, stream_name="t") == ""
    assert keep_last_table_refusal("incremental_deduped", source_type="kafka") == ""
    assert "append" in keep_last_table_refusal("full_refresh_overwrite", stream_contracts=cursor)


def test_lenient_note_on_a_table_names_the_table_routes():
    from services.data_integrity import DUPLICATE_KEY_ROUTES, DUPLICATE_KEY_ROUTES_TABLE, run_integrity_audit

    report = run_integrity_audit(
        mappings=[{"source": "sku", "target": "sku", "confidence": 1}],
        source_columns=["sku"],
        source_schemas=[{"name": "sku", "inferred_type": "VARCHAR"}],
        sample_rows=[{"sku": "A"}, {"sku": "A"}],
        validation_mode="lenient",
        destination_db_type="postgresql",
        sync_mode="full_refresh_overwrite",
        contract_primary_key="sku",
        database_extract=True,
    )
    check = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    assert check["blocks_transfer"] is True
    assert DUPLICATE_KEY_ROUTES_TABLE in check["note"], check["note"]
    assert DUPLICATE_KEY_ROUTES not in check["note"] and "keep_last" not in check["note"]


# ── 2. object-store sources (kind=database, record-batch) ───────────────────
def test_keep_last_on_an_object_store_source_reads_whole_and_collapses():
    from types import SimpleNamespace

    from services.duplicate_policy import keep_last_reads_whole, keep_last_records
    from src.transfer.engine import takes_streaming_route
    from src.transfer.models import EndpointConfig, TransferRequest

    rows = [{"sku": "A", "v": "1"}, {"sku": "B", "v": "2"}, {"sku": "A", "v": "3"}]
    contracts = [{"name": "t", "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]
    for fmt in ("s3", "gcs", "adls", "sftp"):
        req = SimpleNamespace(
            source=EndpointConfig(kind="database", format=fmt, extra={"in_file_dedupe": "keep_last"}),
            destination=EndpointConfig(kind="database", format="postgresql"),
            stream_contracts=contracts,
        )
        assert keep_last_reads_whole(req), fmt
        assert keep_last_records(req, rows) == [{"sku": "A", "v": "3"}, {"sku": "B", "v": "2"}], fmt
    table = SimpleNamespace(
        source=EndpointConfig(kind="database", format="postgresql", extra={"in_file_dedupe": "keep_last"}),
        destination=EndpointConfig(kind="database", format="postgresql"),
        stream_contracts=contracts,
    )
    assert not keep_last_reads_whole(table) and keep_last_records(table, rows) == rows
    # The table stream does not collapse, so such a run takes the buffered route.
    s3 = TransferRequest(
        source=EndpointConfig(kind="database", format="s3", table="k.csv", extra={"in_file_dedupe": "keep_last"}),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        stream_contracts=contracts,
        sync_mode="full_refresh_overwrite",
    )
    assert takes_streaming_route(s3) is False


def test_s3_csv_with_keep_last_lands_one_row_per_key(tmp_path, monkeypatch, local_object_store):
    if not local_object_store:
        pytest.skip("no local object store endpoint (moto)")
    from urllib.parse import urlparse

    import boto3

    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    key = f"r14/dups_{uuid.uuid4().hex[:6]}.csv"
    body = "sku,name\n" + "".join(f"SKU-{i:02d},item {i}\n" for i in range(1, 11)) + "SKU-07,dup item\n"
    boto3.client("s3", endpoint_url=local_object_store, aws_access_key_id="test",
                 aws_secret_access_key="test", region_name="us-east-1").put_object(
        Bucket=LOCAL_OBJECT_STORE_BUCKET, Key=key, Body=body.encode())
    from fastapi.testclient import TestClient
    from src.main import app

    parsed = urlparse(local_object_store)
    dst = f"r14s3_{uuid.uuid4().hex[:6]}"
    contract = [{"name": key, "sync_mode": "full_refresh_overwrite", "primary_key": "sku", "selected": True}]
    data = {"source_kind": "database", "dest_kind": "database", "source_format": "s3",
            "source_host": parsed.hostname or "127.0.0.1", "source_port": str(parsed.port or 443),
            "source_database": LOCAL_OBJECT_STORE_BUCKET, "source_username": "test",
            "source_password": "test", "source_connection_string": local_object_store,
            "source_table": key, "dest_table": dst, "async_mode": "false",
            "validation_mode": "lenient", "sync_mode": "full_refresh_overwrite", **PG_DEST,
            "stream_contracts_json": json.dumps(contract),
            "source_extra_json": json.dumps({"in_file_dedupe": "keep_last"})}
    try:
        resp = TestClient(app).post("/api/v1/transfer/run", data=data)
        assert resp.status_code == 200, resp.text[:1200]
        got = h.query("postgresql", f'SELECT count(*), count(DISTINCT sku) FROM public."{dst}"')[0]  # nosec B608
        assert got == (10, 10), got
        assert h.query("postgresql", f'SELECT name FROM public."{dst}" WHERE sku = \'SKU-07\'') == [("dup item",)]  # nosec B608
    finally:
        h.drop_table("postgresql", dst)
