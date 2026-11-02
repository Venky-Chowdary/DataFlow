"""Redis → Redis COPY — dest prefix COUNT, never DBSIZE."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_redis_common import (  # noqa: E402
    redis_family_name,
    redis_key_type_is_copy_safe,
    redis_type_is_copy_safe,
)
from services.copy_redis_redis import copy_redis_to_redis, redis_redis_copy_enabled  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _redis_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=1):
            pass
    except OSError:
        pytest.skip("Redis 6379 not reachable")


def _redis_cfg(prefix: str, *, database: str = "0") -> dict:
    return {
        "type": "redis",
        "format": "redis",
        "host": "127.0.0.1",
        "port": 6379,
        "database": database,
        "table": prefix,
    }


def _client():
    _redis_or_skip()
    redis = pytest.importorskip("redis")
    client = redis.Redis(host="127.0.0.1", port=6379, db=0, socket_timeout=5)
    client.ping()
    return client


def _dest_count(prefix: str) -> int:
    n = destination_row_count("redis", _redis_cfg(prefix), schema="", table_name=prefix)
    assert n is not None
    return int(n)


def _seed(client, prefix: str, rows: int) -> None:
    pipe = client.pipeline(transaction=False)
    for i in range(1, rows + 1):
        pipe.set(
            f"{prefix}:{i}",
            json.dumps({"id": i, "label": f"r{i}"}, separators=(",", ":")),
        )
        if i % 256 == 0:
            pipe.execute()
            pipe = client.pipeline(transaction=False)
    pipe.execute()


def _delete_prefix(client, prefix: str) -> None:
    keys = list(client.scan_iter(match=f"{prefix}:*", count=500))
    if keys:
        for i in range(0, len(keys), 256):
            client.delete(*keys[i : i + 256])


def test_redis_family_and_copy_safe_types():
    assert redis_family_name("redis_cloud") == "redis"
    assert redis_family_name("valkey") == "redis"
    assert redis_family_name("keydb") == "redis"
    assert redis_type_is_copy_safe("long") is True
    assert redis_type_is_copy_safe("string") is True
    assert redis_key_type_is_copy_safe("hash") is True
    assert redis_key_type_is_copy_safe("none") is False
    assert redis_key_type_is_copy_safe("module") is False


def test_redis_redis_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REDIS_REDIS_COPY", "0")
    assert redis_redis_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_redis_to_redis(
            source_cfg=_redis_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_redis_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            redis_ddls=["long"],
            replace_destination=True,
        )


def test_redis_redis_same_prefix_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    cfg = _redis_cfg("same_prefix")
    with pytest.raises(FastPathUnavailable, match="same prefix"):
        copy_redis_to_redis(
            source_cfg=cfg,
            source_table="same_prefix",
            dest_cfg=cfg,
            dest_table="same_prefix",
            pairs=[("id", "id")],
            redis_ddls=["long"],
            replace_destination=True,
        )


def test_redis_redis_public_proxy_declines():
    dest = {
        **_redis_cfg("b"),
        "host": "",
        "connection_string": "redis://caboose.proxy.rlwy.net:6379/0",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_redis_to_redis(
            source_cfg=_redis_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            redis_ddls=["long"],
            replace_destination=True,
        )


def test_redis_redis_cross_endpoint_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    dest = {**_redis_cfg("b"), "port": 6380}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_redis_to_redis(
            source_cfg=_redis_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            redis_ddls=["long"],
            replace_destination=True,
        )


def test_redis_redis_column_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_redis_to_redis(
            source_cfg=_redis_cfg("a"),
            source_table="a",
            dest_cfg=_redis_cfg("b"),
            dest_table="b",
            pairs=[("id", "user_id")],
            redis_ddls=["long"],
            replace_destination=True,
        )


def test_live_redis_redis_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        _seed(client, src, 800)
        _delete_prefix(client, dest)
        result = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("redis_read") == "copy"
        assert result.source_snapshot.get("redis_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_copy_is_not_get_set(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    _seed(client, src, 80)
    _delete_prefix(client, dest)
    import redis as redis_mod

    orig_get = redis_mod.Redis.get
    orig_set = redis_mod.Redis.set
    orig_mget = redis_mod.Redis.mget
    orig_mset = redis_mod.Redis.mset
    orig_dump = redis_mod.Redis.dump
    orig_restore = redis_mod.Redis.restore

    def _no_get(self, *a, **k):
        raise AssertionError("Redis→Redis COPY must not GET payload bytes")

    def _no_set(self, *a, **k):
        raise AssertionError("Redis→Redis COPY must not SET payload bytes")

    def _no_dump(self, *a, **k):
        raise AssertionError("Redis→Redis COPY must not DUMP payload bytes")

    def _no_restore(self, *a, **k):
        raise AssertionError("Redis→Redis COPY must not RESTORE payload bytes")

    monkeypatch.setattr(redis_mod.Redis, "get", _no_get)
    monkeypatch.setattr(redis_mod.Redis, "set", _no_set)
    monkeypatch.setattr(redis_mod.Redis, "mget", _no_get)
    monkeypatch.setattr(redis_mod.Redis, "mset", _no_set)
    monkeypatch.setattr(redis_mod.Redis, "dump", _no_dump)
    monkeypatch.setattr(redis_mod.Redis, "restore", _no_restore)
    try:
        result = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("redis_read") == "copy"
        assert _dest_count(dest) == 80
    finally:
        monkeypatch.setattr(redis_mod.Redis, "get", orig_get)
        monkeypatch.setattr(redis_mod.Redis, "set", orig_set)
        monkeypatch.setattr(redis_mod.Redis, "mget", orig_mget)
        monkeypatch.setattr(redis_mod.Redis, "mset", orig_mset)
        monkeypatch.setattr(redis_mod.Redis, "dump", orig_dump)
        monkeypatch.setattr(redis_mod.Redis, "restore", orig_restore)
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_empty_string_and_null_preserved():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        client.set(f"{src}:1", json.dumps({"id": 1, "label": None}, separators=(",", ":")))
        client.set(f"{src}:2", json.dumps({"id": 2, "label": ""}, separators=(",", ":")))
        client.set(f"{src}:3", json.dumps({"id": 3, "label": "x"}, separators=(",", ":")))
        result = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        assert client.get(f"{dest}:1") == client.get(f"{src}:1")
        assert client.get(f"{dest}:2") == client.get(f"{src}:2")
        assert client.get(f"{dest}:3") == client.get(f"{src}:3")
        assert json.loads(client.get(f"{dest}:2"))["label"] == ""
        assert json.loads(client.get(f"{dest}:1"))["label"] is None
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_skip_when_dest_count_matches():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        _seed(client, src, 800)
        _delete_prefix(client, dest)
        first = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_occupied_mismatch_declines():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        _seed(client, src, 800)
        _seed(client, dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Redis dest"):
            copy_redis_to_redis(
                source_cfg=_redis_cfg(src),
                source_table=src,
                dest_cfg=_redis_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                redis_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_overwrite_replaces_dest():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        _seed(client, src, 800)
        _seed(client, dest, 1)
        result = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("redis_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()


def test_live_redis_redis_dest_count_is_prefix_not_dbsize():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    other = f"df_roth_{tag}"
    try:
        _seed(client, src, 80)
        _seed(client, other, 50)
        _delete_prefix(client, dest)
        dbsize = int(client.dbsize() or 0)
        assert dbsize >= 130
        result = copy_redis_to_redis(
            source_cfg=_redis_cfg(src),
            source_table=src,
            dest_cfg=_redis_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            redis_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
        assert _dest_count(dest) != dbsize
        assert _dest_count(other) == 50
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        _delete_prefix(client, other)
        client.close()


def test_live_redis_redis_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_REDIS_REDIS_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"df_rsrc_{tag}"
    dest = f"df_rdst_{tag}"
    try:
        _seed(client, src, 800)
        _delete_prefix(client, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"redis-redis-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_redis_cfg(src), "format": "redis"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_redis_cfg(dest), "format": "redis"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_redis_redis"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Redis" in line or "COPY" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _delete_prefix(client, src)
        _delete_prefix(client, dest)
        client.close()
