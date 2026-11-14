"""CLM-2: CDC into a destination that can't apply LSN-guarded deletes.

Before the fix, CDC into Redis passed Validate and the snapshot, applied the
update, then failed at the first source DELETE with "LSN delete fetch is not
wired for destination type 'redis'" and every later run failed the same way.
"""

from __future__ import annotations

import json

import pytest

from tests import w2dat_live as live


def test_support_function_matches_the_delete_dispatch():
    from services.cdc_destination_support import cdc_deletes_supported

    for dest in ("postgresql", "postgres", "amazon_rds_postgresql", "mysql", "aurora_mysql",
                 "sqlite", "mongodb", "sqlserver", "oracle", "snowflake", "bigquery",
                 "duckdb", "redis", "elasticsearch", "opensearch", "dynamodb", "s3",
                 "iceberg", "qdrant", "pgvector"):
        assert cdc_deletes_supported(dest) is True, dest
    for dest in ("salesforce", "hubspot", "milvus", "sftp", "kafka", "csv", "", "neo4j"):
        assert cdc_deletes_supported(dest) is False, dest


def test_lsn_fetch_refuses_exactly_where_the_support_function_says_no():
    from connectors.table_manager import _fetch_pk_lsn_map
    from services.cdc_destination_support import lsn_fetch_route

    assert lsn_fetch_route("salesforce") is None
    with pytest.raises(RuntimeError, match="not wired"):
        _fetch_pk_lsn_map("salesforce", {}, "t", "id", ["1"], None, lsn_column="_df_lsn")


def test_validate_blocks_cdc_into_a_destination_without_deletes():
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(sync_mode="cdc", dest_type="salesforce",
                                      source_type="postgresql", source_kind="database")
    gate = next(g for g in gates if g and g.get("id") == "g9d_cdc_dest_deletes")
    assert gate["status"] == "block"
    assert gate["details"]["error_code"] == "CDC_DEST_DELETES_UNSUPPORTED"
    assert "delete" in gate["message"].lower()
    ok = run_transfer_policy_gates(sync_mode="cdc", dest_type="redis",
                                   source_type="postgresql", source_kind="database")
    assert not any(g and g.get("id") == "g9d_cdc_dest_deletes" for g in ok)


def test_a_failed_delete_names_the_keys_the_batch_already_applied(monkeypatch):
    from services.cdc_engine import ChangeBatch
    from services.data_errors import CdcBatchPartiallyApplied
    from src.transfer import cdc_transfer

    def boom(**_kw):
        raise RuntimeError("delete refused by the destination")

    monkeypatch.setattr(cdc_transfer, "delete_by_primary_keys", boom)
    change = ChangeBatch(inserts=[{"id": 7, "v": "a"}], updates=[{"id": 8, "v": "b"}], deletes=["9"])
    with pytest.raises(CdcBatchPartiallyApplied) as info:
        cdc_transfer._apply_cdc_deletes("postgresql", {}, "orders", change, ["id"])
    err = info.value
    assert err.code == "CDC_BATCH_PARTIALLY_APPLIED"
    assert err.failure_details["applied_keys"] == ["7", "8"]
    assert "7, 8" in str(err) and "orders" in str(err)


def test_redis_lsn_map_reads_the_stored_position():
    live.need_redis()
    import redis

    from connectors.table_manager import _fetch_pk_lsn_map

    prefix = f"w2dat_lsn_{live.uid()}"
    client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    try:
        client.set(f"{prefix}:1", json.dumps({"id": 1, "_df_lsn": "0/16B3748"}))
        cfg = {"host": "127.0.0.1", "port": 6379, "database": "0", "key_format": "raw"}
        got = _fetch_pk_lsn_map("redis", cfg, prefix, "id", ["1", "2"], None, lsn_column="_df_lsn")
        assert got == {"1": "0/16B3748", "2": None}
    finally:
        live.redis_drop(prefix)


def test_pg_to_redis_cdc_applies_a_source_delete():
    live.need_logical_pg()
    live.need_redis()
    u = live.uid()
    src = f"w2dat_c2_{u}"
    prefix = f"w2dat_c2r_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id integer primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')')
        first = live.run(live.ep_pg(src), live.ep_redis(prefix), "cdc", key="id", cols=["id", "v"])
        assert first.success, first.error
        live.pg(f'UPDATE public."{src}" SET v = \'b2\' WHERE id = 2')
        live.pg(f'DELETE FROM public."{src}" WHERE id = 3')
        second = live.run(live.ep_pg(src), live.ep_redis(prefix), "cdc", key="id", cols=["id", "v"])
        assert second.success, second.error
        import redis

        client = redis.Redis(host="127.0.0.1", port=6379, db=0)
        keys = sorted(k.decode() for k in client.scan_iter(match=f"{prefix}:*"))
        docs = {json.loads(client.get(k))["id"]: json.loads(client.get(k))["v"] for k in keys}
        assert {int(k): v for k, v in docs.items()} == {1: "a", 2: "b2"}
    finally:
        live.redis_drop(prefix)
        live.drop_pg(src)
        live.drop_cdc_artifacts(src)
