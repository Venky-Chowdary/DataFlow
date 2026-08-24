"""Wave 100 L1: object-store multi-chunk writes must not overwrite prior parts."""

from __future__ import annotations

import pytest

from connectors.object_store_common import (
    normalize_object_base_key,
    object_store_read_keys,
    purge_object_store_parts,
    resolve_object_write_key,
    resolve_object_write_layout,
)


def test_single_chunk_keeps_exact_key():
    base = normalize_object_base_key("exports/data.json")
    key, prefix = resolve_object_write_key(base, file_batch_idx=1, total_chunks=1)
    assert key == "exports/data.json"
    assert prefix == ""


def test_multi_chunk_emits_distinct_part_keys():
    base = normalize_object_base_key("orders/export.json")
    k1, p1 = resolve_object_write_key(base, file_batch_idx=1, total_chunks=3)
    k2, p2 = resolve_object_write_key(base, file_batch_idx=2, total_chunks=3)
    k3, p3 = resolve_object_write_key(base, file_batch_idx=3, total_chunks=3)
    assert k1 != k2 != k3
    assert k1.endswith("part-00001.json")
    assert k2.endswith("part-00002.json")
    assert k3.endswith("part-00003.json")
    assert p1 == p2 == p3 == "orders/export/"


def test_object_store_read_keys_prefers_parts():
    from connectors.object_store_common import object_store_read_keys

    keys = object_store_read_keys(
        "orders/export.json",
        [
            "orders/export/part-00002.json",
            "orders/export/part-00001.json",
            "orders/export/readme.txt",
        ],
    )
    assert keys == [
        "orders/export/part-00001.json",
        "orders/export/part-00002.json",
    ]


def test_object_store_read_keys_falls_back_to_base():
    from connectors.object_store_common import object_store_read_keys

    assert object_store_read_keys("orders/export.json", []) == ["orders/export.json"]


def test_purge_removes_only_part_objects():
    store = {
        "orders/export/part-00001.json": b"a",
        "orders/export/part-00002.json": b"b",
        "orders/export/readme.txt": b"keep",
        "orders/other.json": b"keep",
    }

    def list_keys(prefix: str) -> list[str]:
        return [k for k in store if k.startswith(prefix)]

    def delete_key(key: str) -> None:
        store.pop(key, None)

    removed = purge_object_store_parts(
        list_keys=list_keys,
        delete_key=delete_key,
        parts_prefix="orders/export/",
        legacy_base_key="orders/export.json",
    )
    assert "orders/export/part-00001.json" in removed
    assert "orders/export/part-00002.json" in removed
    assert "orders/export/readme.txt" in store
    assert "orders/other.json" in store


def test_s3_writer_two_chunks_use_distinct_keys(monkeypatch):
    from connectors import s3_writer

    puts: list[str] = []

    class FakeClient:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append(Key)

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    monkeypatch.setattr(
        "connectors.s3_reader.list_objects", lambda *a, **k: []
    )

    common = dict(
        host="",
        port=0,
        database="bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders/export.json",
        headers=["id"],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "string"},
        create_table=True,
        sync_mode="full_refresh_overwrite",
        total_chunks=2,
    )
    r1 = s3_writer.write_mapped_rows(
        **common, data_rows=[["1"]], file_batch_idx=1
    )
    r2 = s3_writer.write_mapped_rows(
        **common, data_rows=[["2"]], file_batch_idx=2
    )
    assert r1.ok and r2.ok
    # Each chunk: staging put + live put (purge-after-commit honesty).
    live_puts = [k for k in puts if not k.endswith(".__df_staging__")]
    assert len(live_puts) == 2
    assert live_puts[0] != live_puts[1]
    assert "part-00001" in live_puts[0]
    assert "part-00002" in live_puts[1]
    assert any(k.endswith(".__df_staging__") for k in puts)


# --- Wave 102: append runs must not interleave with a previous run's parts ---


def test_overwrite_layout_shares_one_part_set_and_purges_last_chunk():
    first = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="full_refresh_overwrite",
        file_batch_idx=1,
        total_chunks=3,
        job_id="job-a",
    )
    second = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="full_refresh_overwrite",
        file_batch_idx=2,
        total_chunks=3,
        job_id="job-a",
    )
    last = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="full_refresh_overwrite",
        file_batch_idx=3,
        total_chunks=3,
        job_id="job-a",
    )
    # Overwrite reuses a stable part set (no run token) and clears once *after*
    # the last successful promote — never before the first upload.
    assert first.write_key == "orders/export/part-00001.json"
    assert second.write_key == "orders/export/part-00002.json"
    assert first.should_purge is False
    assert second.should_purge is False
    assert last.should_purge is True
    assert last.keep_part_count == 3
    assert first.purge_prefix == "orders/export/"
    assert first.purge_legacy_key == "orders/export.json"


def test_purge_keeps_current_run_parts_after_overwrite():
    from connectors.object_store_common import purge_object_store_parts

    store = {
        "orders/export/part-00001.json": b"new1",
        "orders/export/part-00002.json": b"new2",
        "orders/export/part-00003.json": b"stale",
        "orders/export.json": b"legacy",
    }

    def list_keys(prefix: str) -> list[str]:
        return [k for k in store if k.startswith(prefix)]

    def delete_key(key: str) -> None:
        store.pop(key, None)

    removed = purge_object_store_parts(
        list_keys=list_keys,
        delete_key=delete_key,
        parts_prefix="orders/export/",
        legacy_base_key="orders/export.json",
        keep_part_count=2,
        keep_keys=["orders/export/part-00001.json", "orders/export/part-00002.json"],
    )
    assert "orders/export/part-00003.json" in removed
    assert "orders/export.json" in removed
    assert "orders/export/part-00001.json" in store
    assert "orders/export/part-00002.json" in store


def test_append_reruns_never_collide_on_part_keys():
    def keys_for(job_id: str, chunks: int) -> list[str]:
        return [
            resolve_object_write_layout(
                table_name="orders/export.json",
                sync_mode="incremental_append",
                file_batch_idx=i,
                total_chunks=chunks,
                job_id=job_id,
            ).write_key
            for i in range(1, chunks + 1)
        ]

    run_a = keys_for("job-a", 3)
    run_b = keys_for("job-b", 2)

    # The bug this guards: a 2-chunk rerun rewriting part-00001/2 while run A's
    # part-00003 survives, leaving one export mixing two generations of rows.
    assert not set(run_a) & set(run_b)
    assert all("/run-job-a/" in k for k in run_a)
    assert all("/run-job-b/" in k for k in run_b)


def test_append_never_purges_previous_runs():
    layout = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="incremental_append",
        file_batch_idx=1,
        total_chunks=3,
        job_id="job-a",
    )
    assert layout.should_purge is False


def test_single_chunk_append_keeps_exact_key_without_run_token():
    layout = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="incremental_append",
        file_batch_idx=1,
        total_chunks=1,
        job_id="",
    )
    assert layout.write_key == "orders/export.json"
    assert layout.parts_prefix == ""


def test_multi_chunk_append_without_job_id_fails_closed():
    with pytest.raises(ValueError, match="requires a job id"):
        resolve_object_write_layout(
            table_name="orders/export.json",
            sync_mode="incremental_append",
            file_batch_idx=1,
            total_chunks=3,
            job_id="",
        )


def test_run_token_sanitizes_unsafe_job_ids():
    layout = resolve_object_write_layout(
        table_name="orders/export.json",
        sync_mode="incremental_append",
        file_batch_idx=1,
        total_chunks=2,
        job_id="../../etc/passwd?x=1",
    )
    assert layout.write_key == "orders/export/run-etcpasswdx1/part-00001.json"
    assert ".." not in layout.write_key


def test_gate8_read_keys_find_append_run_parts():
    # Gate-8 must still aggregate parts nested one level under a run token,
    # otherwise read-back looks empty and falls back to writer-ack (false pass).
    keys = object_store_read_keys(
        "orders/export.json",
        [
            "orders/export/run-job-a/part-00002.json",
            "orders/export/run-job-a/part-00001.json",
        ],
    )
    assert keys == [
        "orders/export/run-job-a/part-00001.json",
        "orders/export/run-job-a/part-00002.json",
    ]


def test_s3_append_rerun_writes_isolated_keys(monkeypatch):
    from connectors import s3_writer

    store: dict[str, bytes] = {}

    class FakeClient:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            store[Key] = Body if isinstance(Body, bytes) else str(Body).encode()

        def delete_object(self, Bucket, Key):  # noqa: N803
            store.pop(Key, None)

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    monkeypatch.setattr(
        "connectors.s3_reader.list_objects",
        lambda cfg, bucket, prefix: [k for k in store if k.startswith(prefix)],
    )

    common = dict(
        host="",
        port=0,
        database="bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders/export.json",
        headers=["id"],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "string"},
        create_table=True,
        sync_mode="incremental_append",
    )

    # Run A: three chunks.
    for idx in range(1, 4):
        res = s3_writer.write_mapped_rows(
            **common, data_rows=[[f"a{idx}"]], file_batch_idx=idx,
            total_chunks=3, job_id="job-a",
        )
        assert res.ok, res.error
    # Run B: only two chunks — must not clobber run A's first two parts.
    for idx in range(1, 3):
        res = s3_writer.write_mapped_rows(
            **common, data_rows=[[f"b{idx}"]], file_batch_idx=idx,
            total_chunks=2, job_id="job-b",
        )
        assert res.ok, res.error

    assert len(store) == 5
    run_a = {k for k in store if "run-job-a" in k}
    run_b = {k for k in store if "run-job-b" in k}
    assert len(run_a) == 3
    assert len(run_b) == 2
    # Run A's rows survive untouched.
    assert all(b"a" in store[k] for k in run_a)


def test_s3_multi_chunk_append_without_job_id_returns_error(monkeypatch):
    from connectors import s3_writer

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: None)
    res = s3_writer.write_mapped_rows(
        host="", port=0, database="bucket", username="", password="", schema="",
        connection_string="", ssl=False, table_name="orders/export.json",
        headers=["id"], data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "string"}, create_table=True,
        sync_mode="incremental_append", file_batch_idx=1, total_chunks=3,
    )
    assert res.ok is False
    assert "job id" in (res.error or "")
    assert res.rows_written == 0
