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
