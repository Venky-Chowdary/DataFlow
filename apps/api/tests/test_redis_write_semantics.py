"""Redis upsert merges and keeps TTL; append never overwrites; keys never collide.

Defects (live local Redis, throwaway ``ws_`` prefixes):

* upsert ``SET`` the mapped fields as the whole JSON value: every field the
  mapping does not write was dropped (contract G15) and the key's TTL was
  cleared. A hash key was replaced by a JSON string.
* append (``write_mode=insert``) ``SET`` over a stored key.
* the legacy key layout ``sanitize_identifier(id)`` maps distinct ids onto one
  key (``ann@acme.com`` / ``ann.acme@com`` → ``ann_acme_com``, ids sharing
  their first 63 characters) and the second write silently replaced the
  first id's document.

Fixed: upsert merges onto the stored document (string: read-merge-write with
a compare-and-set that re-applies the TTL; hash: HSET of the mapped fields);
append is ``SET NX`` and a stored key is a rejected row; a key that holds
another id's document, or two ids that map to one key in a batch, refuse the
row with a named reason. New connectors default to ``key_format=raw`` (the
id text is the key); existing connectors keep ``legacy``.
"""

from __future__ import annotations

import json
import uuid

import pytest

redis = pytest.importorskip("redis")


def _client():
    return redis.Redis(host="127.0.0.1", port=6379, db=0)


def _reachable() -> bool:
    try:
        return bool(_client().ping())
    except redis.RedisError:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="Redis not reachable")

_MAP = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


@pytest.fixture
def prefix():
    p = f"ws_rw_{uuid.uuid4().hex[:8]}"
    c = _client()
    assert not c.keys(f"{p}*")
    yield p
    for k in c.keys(f"{p}*"):
        c.delete(k)
    c.close()


def _write(prefix, rows, **kw):
    from connectors.redis_writer import write_mapped_rows

    return write_mapped_rows(
        host="127.0.0.1",
        port=6379,
        database="0",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name=prefix,
        headers=["id", "name"],
        data_rows=rows,
        mappings=_MAP,
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        # Studio's live field types: a prefix of hashes/lists has no JSON
        # sample, and the writer refuses to bind without one.
        destination_column_types={"id": "VARCHAR", "name": "VARCHAR"},
        **kw,
    )


def _key(prefix, ident, key_format="legacy"):
    from connectors.redis_reader import redis_key_for

    if key_format == "legacy":  # the layout every existing connector keeps
        return redis_key_for(prefix, ident)
    return redis_key_for(prefix, ident, key_format=key_format)


def test_upsert_merges_unmapped_fields_and_keeps_ttl(prefix):
    c = _client()
    k = _key(prefix, "1")
    c.set(k, json.dumps({"id": "1", "name": "old", "loyalty_tier": "gold"}), ex=3600)
    r = _write(prefix, [["1", "new"], ["2", "two"]], write_mode="upsert")
    assert r.ok, r.error
    assert r.rows_written == 2
    assert json.loads(c.get(k)) == {"id": "1", "name": "new", "loyalty_tier": "gold"}
    assert 3500 < c.ttl(k) <= 3600
    assert json.loads(c.get(_key(prefix, "2"))) == {"id": "2", "name": "two"}
    assert c.ttl(_key(prefix, "2")) == -1


def test_upsert_onto_hash_sets_mapped_fields_only(prefix):
    c = _client()
    k = _key(prefix, "1")
    c.hset(k, mapping={"id": "1", "name": "old", "app_meta": "keep"})
    c.expire(k, 3600)
    r = _write(prefix, [["1", "new"]], write_mode="upsert")
    assert r.ok, r.error
    assert c.type(k) == b"hash"
    assert c.hgetall(k) == {b"id": b"1", b"name": b"new", b"app_meta": b"keep"}
    assert c.ttl(k) > 3500


def test_upsert_refuses_to_replace_a_non_document_key(prefix):
    c = _client()
    k = _key(prefix, "1")
    c.rpush(k, "a", "b")
    r = _write(prefix, [["1", "new"]], write_mode="upsert")
    assert c.lrange(k, 0, -1) == [b"a", b"b"]
    assert r.rows_written == 0
    assert r.rejected_rows == 1
    assert "list" in r.rejected_details[0]["reason"]


def test_append_never_overwrites_a_stored_key(prefix):
    c = _client()
    k = _key(prefix, "1")
    c.set(k, json.dumps({"id": "1", "name": "EXISTING"}))
    r = _write(prefix, [["1", "INCOMING"], ["3", "three"]], write_mode="insert")
    assert json.loads(c.get(k))["name"] == "EXISTING"
    assert json.loads(c.get(_key(prefix, "3")))["name"] == "three"
    assert r.ok, r.error
    assert r.rows_written == 1
    assert r.rejected_rows == 1
    detail = r.rejected_details[0]
    assert detail["row"] == 1
    assert "already" in detail["reason"].lower()


def test_append_conflict_fails_under_policy_fail(prefix):
    c = _client()
    c.set(_key(prefix, "1"), json.dumps({"id": "1", "name": "EXISTING"}))
    r = _write(prefix, [["1", "INCOMING"]], write_mode="insert", error_policy="fail")
    assert not r.ok
    assert json.loads(c.get(_key(prefix, "1")))["name"] == "EXISTING"


@pytest.mark.parametrize("write_mode", ["insert", "upsert"])
def test_legacy_key_collision_across_runs_is_refused(prefix, write_mode):
    c = _client()
    r1 = _write(prefix, [["ann@acme.com", "Ann"]], write_mode=write_mode)
    assert r1.ok and r1.rows_written == 1
    r2 = _write(prefix, [["ann.acme@com", "Bob"]], write_mode=write_mode)
    keys = c.keys(f"{prefix}:*")
    assert len(keys) == 1
    assert json.loads(c.get(keys[0])) == {"id": "ann@acme.com", "name": "Ann"}
    assert r2.rows_written == 0
    assert r2.rejected_rows == 1
    reason = r2.rejected_details[0]["reason"]
    assert "ann@acme.com" in reason and "ann.acme@com" in reason
    assert "collision" in reason.lower()


def test_legacy_key_collision_in_one_batch_is_refused(prefix):
    c = _client()
    long_a = "x" * 63 + "A"
    long_b = "x" * 63 + "B"
    r = _write(
        prefix,
        [["ann@acme.com", "Ann"], ["ann.acme@com", "Bob"], [long_a, "a"], [long_b, "b"]],
        write_mode="upsert",
    )
    assert r.rows_written == 2
    assert r.rejected_rows == 2
    assert sorted(d["row"] for d in r.rejected_details) == [2, 4]
    docs = sorted(json.loads(c.get(k))["name"] for k in c.keys(f"{prefix}:*"))
    assert docs == ["Ann", "a"]


def test_legacy_collision_overwrite_mode_is_refused(prefix):
    c = _client()
    r = _write(
        prefix,
        [["ann@acme.com", "Ann"], ["ann.acme@com", "Bob"]],
        write_mode="overwrite",
        sync_mode="full_refresh_overwrite",
    )
    assert r.rows_written == 1
    assert r.rejected_rows == 1
    assert [json.loads(c.get(k))["name"] for k in c.keys(f"{prefix}:*")] == ["Ann"]


def test_raw_key_format_uses_the_id_text(prefix):
    c = _client()
    r = _write(
        prefix,
        [["1", "one"], ["ann@acme.com", "Ann"], ["ann.acme@com", "Bob"]],
        write_mode="upsert",
        key_format="raw",
    )
    assert r.ok, r.error
    assert r.rows_written == 3
    keys = sorted(k.decode() for k in c.keys(f"{prefix}:*"))
    assert keys == sorted([f"{prefix}:1", f"{prefix}:ann@acme.com", f"{prefix}:ann.acme@com"])
    assert _key(prefix, "1", "raw") == f"{prefix}:1"
    assert _key(prefix, "1") == f"{prefix}:col_1"


def test_raw_composite_identity_collision_is_refused(prefix):
    """("a|b","c") and ("a","b|c") join to the same raw key text."""
    from connectors.redis_writer import write_mapped_rows

    common = dict(
        host="127.0.0.1", port=6379, database="0", username="", password="", schema="",
        connection_string="", ssl=False, table_name=prefix, headers=["k1", "k2", "v"],
        mappings=[{"source": c, "target": c} for c in ("k1", "k2", "v")],
        column_types={"k1": "VARCHAR", "k2": "VARCHAR", "v": "VARCHAR"},
        conflict_columns=["k1", "k2"], key_format="raw", write_mode="upsert",
    )
    r1 = write_mapped_rows(data_rows=[["a|b", "c", "first"]], **common)
    assert r1.ok and r1.rows_written == 1
    r2 = write_mapped_rows(data_rows=[["a", "b|c", "second"]], **common)
    assert r2.rows_written == 0 and r2.rejected_rows == 1
    assert [json.loads(_client().get(k))["v"] for k in _client().keys(f"{prefix}:*")] == ["first"]


def test_key_format_reaches_writer_and_readback(prefix):
    from connectors.redis_reader import redis_key_format
    from services.reconciliation_readback import verify_redis_prefix
    from src.transfer.connector_dispatch import writer_extra_kwargs

    assert redis_key_format({}) == "legacy"
    assert redis_key_format({"extra": {"key_format": "raw"}}) == "raw"
    assert redis_key_format({"key_format": "RAW"}) == "raw"
    assert writer_extra_kwargs("redis", cfg={"extra": {"key_format": "raw"}})["key_format"] == "raw"
    assert writer_extra_kwargs("redis", cfg={})["key_format"] == "legacy"

    r = _write(prefix, [["ann@acme.com", "Ann"]], write_mode="upsert", key_format="raw")
    assert r.ok, r.error
    count, digest = verify_redis_prefix(
        host="127.0.0.1", port=6379, database="0", username="", password="",
        connection_string="", ssl=False, prefix=prefix, target_columns=["id", "name"],
        written_ids=["ann@acme.com"], pk_column="id", key_format="raw",
    )
    assert count == 1 and digest


def test_new_redis_connector_defaults_to_raw_existing_stays_legacy(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "ws_connectors.json"))
    import services.connector_store as store

    monkeypatch.setattr(store, "_use_mongo", lambda: False)
    base = {"type": "redis", "role": "destination", "host": "127.0.0.1", "port": 6379}
    new = store.create_connector({**base, "name": "ws_redis_new"})
    assert new.extra.get("key_format") == "raw"
    root = store.create_connector({**base, "name": "ws_redis_root", "key_format": "legacy"})
    assert root.extra.get("key_format") == "legacy"
    pg = store.create_connector({"type": "postgresql", "role": "destination", "name": "ws_pg"})
    assert "key_format" not in pg.extra
    # A connector saved before key_format existed has no option (legacy); a
    # same-named re-save must not silently move its keys to the raw layout.
    old = store.SavedConnector(
        id=str(uuid.uuid4()), name="ws_redis_old", type="redis", role="destination",
        host="127.0.0.1", port=6379,
    )
    with store._file_store_mutation():
        saved = store._load_all()
        saved.append(old)
        store._save_all(saved)
    resaved = store.create_connector({**base, "name": "ws_redis_old"})
    assert resaved.id == old.id
    assert "key_format" not in resaved.extra


@pytest.mark.parametrize("mode", ["upsert", "insert"])
def test_raw_write_onto_a_legacy_layout_prefix_is_refused(prefix, mode):
    """Review M3: a new (raw) connector replacing a legacy one on the same prefix.

    Raw writes ``prefix:7`` while the legacy document is ``prefix:col_7``: two
    documents for one id, and precount / Gate-8 (raw) never see the legacy one.
    """
    c = _client()
    legacy = _key(prefix, "7")
    assert legacy != _key(prefix, "7", key_format="raw")
    c.set(legacy, json.dumps({"id": "7", "name": "old"}))
    r = _write(prefix, [["7", "new"], ["8", "eight"]], write_mode=mode, key_format="raw")
    assert not r.ok
    assert "legacy" in (r.error or "") and "key_format" in (r.error or "")
    assert r.rows_written == 0
    assert c.get(_key(prefix, "7", key_format="raw")) is None
    assert c.get(_key(prefix, "8", key_format="raw")) is None
    assert json.loads(c.get(legacy)) == {"id": "7", "name": "old"}


def test_raw_write_onto_a_raw_prefix_is_not_refused(prefix):
    c = _client()
    c.set(_key(prefix, "7", key_format="raw"), json.dumps({"id": "7", "name": "old"}))
    r = _write(prefix, [["7", "new"]], write_mode="upsert", key_format="raw")
    assert r.ok, r.error
    assert json.loads(c.get(_key(prefix, "7", key_format="raw")))["name"] == "new"


@pytest.mark.parametrize(
    "ids", [["A_1", "A-1"], ["col_7", "7"], ["ann@acme.com", "ann_acme_com"]],
    ids=["underscore_dash", "col_prefix", "email"],
)
def test_raw_ids_whose_legacy_spelling_is_another_raw_id_keep_working(prefix, ids):
    """Re-review 1: the legacy spelling of one raw id is another raw id's key.

    Only a document carrying *this* row's identity at the legacy key is a
    legacy-layout document; another id living there is not.
    """
    c = _client()
    for run in range(3):
        r = _write(prefix, [[i, f"v{run}"] for i in ids], write_mode="upsert", key_format="raw")
        assert r.ok, (run, r.error)
        assert r.rows_written == 2
    for i in ids:
        assert json.loads(c.get(_key(prefix, i, key_format="raw"))) == {"id": i, "name": "v2"}


def test_legacy_document_of_the_same_id_is_still_refused_beside_a_raw_neighbour(prefix):
    c = _client()
    c.set(_key(prefix, "7"), json.dumps({"id": "7", "name": "legacy"}))  # prefix:col_7
    c.set(_key(prefix, "A_1", key_format="raw"), json.dumps({"id": "A_1", "name": "raw"}))
    r = _write(prefix, [["A-1", "new"]], write_mode="upsert", key_format="raw")
    assert r.ok, r.error  # prefix:A_1 holds id A_1, not A-1
    r = _write(prefix, [["7", "new"]], write_mode="upsert", key_format="raw")
    assert not r.ok and "legacy" in (r.error or "")


# ------------------------------------- append: an identical stored row is present
def test_append_of_an_identical_stored_json_document_is_already_present(prefix):
    """A second append run of an unchanged source into a key-addressed store
    (the sync-mode matrix contract): no write, rows_skipped + Gate-8 marker."""
    c = _client()
    r1 = _write(prefix, [["1", "one"], ["2", "two"]], write_mode="insert")
    assert r1.ok and r1.rows_written == 2
    r2 = _write(prefix, [["1", "one"], ["2", "two"]], write_mode="insert")
    assert r2.ok, r2.error
    assert (r2.rows_written, r2.rows_skipped, r2.rejected_rows) == (0, 2, 0), r2
    assert sorted(r2.meta.get("skipped_identity_keys") or []) == ["1", "2"]
    assert json.loads(c.get(_key(prefix, "1"))) == {"id": "1", "name": "one"}


def test_append_of_a_changed_stored_json_document_is_still_rejected(prefix):
    c = _client()
    c.set(_key(prefix, "1"), json.dumps({"id": "1", "name": "one"}))
    r = _write(prefix, [["1", "CHANGED"]], write_mode="insert")
    assert r.ok, r.error
    assert (r.rows_written, r.rejected_rows) == (0, 1)
    assert json.loads(c.get(_key(prefix, "1")))["name"] == "one"


def test_append_compares_parsed_json_values_not_bytes(prefix):
    c = _client()
    # Same values, different JSON spelling (key order, whitespace) plus an
    # unmapped field the mapping does not write.
    c.set(_key(prefix, "1"), '{ "name" : "one", "extra": 5, "id": "1" }')
    r = _write(prefix, [["1", "one"]], write_mode="insert")
    assert (r.ok, r.rows_written, r.rows_skipped, r.rejected_rows) == (True, 0, 1, 0), r


def test_append_onto_an_identical_hash_is_already_present_and_a_changed_one_rejected(prefix):
    c = _client()
    c.hset(_key(prefix, "1"), mapping={"id": "1", "name": "one", "app_meta": "keep"})
    c.hset(_key(prefix, "2"), mapping={"id": "2", "name": "old"})
    r = _write(prefix, [["1", "one"], ["2", "two"]], write_mode="insert")
    assert r.ok, r.error
    assert (r.rows_written, r.rows_skipped, r.rejected_rows) == (0, 1, 1), r
    assert r.meta.get("skipped_identity_keys") == ["1"]
    assert c.hgetall(_key(prefix, "2"))[b"name"] == b"old"
