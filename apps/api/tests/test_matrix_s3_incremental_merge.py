"""S3 incremental append/deduped must not replace the object with only the new rows."""

from __future__ import annotations

import json

import pytest

from connectors.object_store_incremental import (
    ObjectMergeRefused,
    incremental_merge_strategy,
    merge_object_bytes,
)
from connectors.object_store_common import serialize_object_store_export


def _client(store: dict[str, bytes]):
    class _Body:
        def __init__(self, data: bytes) -> None:
            self._data = data

        def read(self) -> bytes:
            return self._data

    class FakeClient:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            store[Key] = Body if isinstance(Body, bytes) else bytes(Body)

        def head_object(self, Bucket, Key):  # noqa: N803
            if Key not in store:
                raise KeyError(Key)
            return {"ContentLength": len(store[Key])}

        def get_object(self, Bucket, Key):  # noqa: N803
            if Key not in store:
                raise KeyError(Key)
            return {"Body": _Body(store[Key])}

        def copy_object(self, **kwargs):
            source = kwargs["CopySource"]["Key"]
            store[kwargs["Key"]] = store[source]

        def delete_object(self, Bucket, Key):  # noqa: N803
            store.pop(Key, None)

    return FakeClient()


def _write(monkeypatch, store, **overrides):
    from connectors import s3_writer

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: _client(store))
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    kwargs = dict(
        host="",
        port=0,
        database="bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders/export.json",
        headers=["id", "name"],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        column_types={"id": "string", "name": "string"},
        create_table=True,
        sync_mode="incremental_append",
    )
    kwargs.update(overrides)
    return s3_writer.write_mapped_rows(**kwargs)


def _rows(store, key="orders/export.json"):
    return json.loads(store[key].decode())


def test_incremental_append_keeps_prior_rows(monkeypatch):
    store: dict[str, bytes] = {}
    first = [[str(i), f"n{i}"] for i in range(100)]
    res = _write(monkeypatch, store, data_rows=first)
    assert res.ok, res.error
    assert res.rows_written == 100
    assert len(_rows(store)) == 100

    delta = [[str(i), f"new{i}"] for i in range(100, 115)]
    res = _write(monkeypatch, store, data_rows=delta)
    assert res.ok, res.error
    assert res.rows_written == 15
    body = _rows(store)
    assert len(body) == 115
    assert body[0]["id"] == "0"
    assert body[-1]["id"] == "114"
    assert store  # live key, not a run-token part
    assert "orders/export.json" in store


def test_incremental_deduped_merges_by_key_and_keeps_other_rows(monkeypatch):
    store: dict[str, bytes] = {}
    first = [[str(i), f"n{i}"] for i in range(100)]
    res = _write(
        monkeypatch,
        store,
        data_rows=first,
        sync_mode="incremental_deduped",
        conflict_columns=["id"],
    )
    assert res.ok, res.error

    delta = [[str(i), f"upd{i}"] for i in range(90, 105)]
    res = _write(
        monkeypatch,
        store,
        data_rows=delta,
        sync_mode="incremental_deduped",
        conflict_columns=["id"],
    )
    assert res.ok, res.error
    assert res.rows_written == 15
    body = _rows(store)
    assert len(body) == 105
    by_id = {row["id"]: row["name"] for row in body}
    assert by_id["0"] == "n0"
    assert by_id["89"] == "n89"
    assert by_id["90"] == "upd90"
    assert by_id["104"] == "upd104"


def test_deduped_without_key_does_not_replace(monkeypatch):
    store: dict[str, bytes] = {}
    res = _write(
        monkeypatch,
        store,
        data_rows=[["1", "a"]],
        sync_mode="incremental_deduped",
        conflict_columns=["id"],
    )
    assert res.ok, res.error
    before = store["orders/export.json"]
    res = _write(
        monkeypatch,
        store,
        data_rows=[["2", "b"]],
        sync_mode="incremental_deduped",
    )
    assert res.ok is False
    assert "primary key" in (res.error or "")
    assert store["orders/export.json"] == before


def test_unparseable_object_is_not_replaced(monkeypatch):
    store = {"orders/export.json": b"this is not json"}
    res = _write(monkeypatch, store, data_rows=[["1", "a"]])
    assert res.ok is False
    assert "not replaced" in (res.error or "")
    assert store["orders/export.json"] == b"this is not json"


def test_overwrite_still_replaces(monkeypatch):
    store: dict[str, bytes] = {}
    res = _write(
        monkeypatch,
        store,
        data_rows=[[str(i), "a"] for i in range(100)],
        sync_mode="full_refresh_overwrite",
    )
    assert res.ok, res.error
    res = _write(
        monkeypatch,
        store,
        data_rows=[[str(i), "b"] for i in range(15)],
        sync_mode="full_refresh_overwrite",
    )
    assert res.ok, res.error
    assert len(_rows(store)) == 15


def test_strategy_skips_multichunk_and_overwrite():
    assert incremental_merge_strategy("incremental_append", total_chunks=1) == "concat"
    assert incremental_merge_strategy("full_refresh_append", total_chunks=1) == "concat"
    assert incremental_merge_strategy("incremental_deduped", total_chunks=1) == "key"
    assert incremental_merge_strategy("incremental_append", total_chunks=3) is None
    assert incremental_merge_strategy("full_refresh_overwrite", total_chunks=1) is None
    assert incremental_merge_strategy("incremental_append", protect_landed=True) is None


def test_csv_concat_and_key_merge_share_the_object_algorithm():
    prior = serialize_object_store_export(
        key="orders.csv",
        mapped_rows=[("1", "a"), ("2", "b")],
        target_cols=["id", "name"],
        dest_types={"id": "string", "name": "string"},
    )
    incoming = serialize_object_store_export(
        key="orders.csv",
        mapped_rows=[("2", "updated"), ("3", "c")],
        target_cols=["id", "name"],
        dest_types={"id": "string", "name": "string"},
    )
    try:
        appended = merge_object_bytes(
            key="orders.csv",
            existing=prior.read_all(),
            new_export=incoming,
            target_cols=["id", "name"],
            dest_types={"id": "string", "name": "string"},
            conflict_columns=["id"],
            strategy="concat",
            spill_max_size=1024 * 1024,
        )
        text = appended.read_all().decode()
        assert text.count("\n") >= 4
        assert "1" in text and "3" in text
        appended.close()
        incoming.rewind()
        merged = merge_object_bytes(
            key="orders.csv",
            existing=prior.read_all(),
            new_export=incoming,
            target_cols=["id", "name"],
            dest_types={"id": "string", "name": "string"},
            conflict_columns=["id"],
            strategy="key",
            spill_max_size=1024 * 1024,
        )
        body = merged.read_all().decode()
        assert "updated" in body
        assert "\n3," in body or body.rstrip().endswith(",c") or ",c" in body
        # key 1 survives; key 2 is the new value; no second copy of key 2
        assert body.count("updated") == 1
        merged.close()
    finally:
        prior.close()
        incoming.close()

    with pytest.raises(ObjectMergeRefused, match="primary key"):
        export = serialize_object_store_export(
            key="orders.json",
            mapped_rows=[("1",)],
            target_cols=["id"],
            dest_types={"id": "string"},
        )
        try:
            merge_object_bytes(
                key="orders.json",
                existing=b'[\n  {\n    "id": "9"\n  }\n]',
                new_export=export,
                target_cols=["id"],
                dest_types={"id": "string"},
                conflict_columns=[],
                strategy="key",
                spill_max_size=1024,
            )
        finally:
            export.close()
