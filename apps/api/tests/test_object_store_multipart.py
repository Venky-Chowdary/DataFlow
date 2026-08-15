"""Object-store multipart — S3 abort, GCS compose, ADLS blocks (no cloud creds)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.object_store_multipart import (  # noqa: E402
    GCS_COMPOSE_LIMIT,
    iter_object_store_parts,
    should_use_object_store_multipart,
    upload_object_store_bytes,
)


def test_multipart_requires_two_parts_at_threshold():
    assert should_use_object_store_multipart(b"x" * 8, threshold=8, part_size=8) is False
    assert should_use_object_store_multipart(b"x" * 9, threshold=8, part_size=8) is True
    assert should_use_object_store_multipart(b"x" * 100, threshold=1000, part_size=8) is False


def test_iter_parts_last_may_be_short():
    parts = list(iter_object_store_parts(b"abcdefghij", part_size=4))
    assert [n for n, _ in parts] == [1, 2, 3]
    assert [c for _, c in parts] == [b"abcd", b"efgh", b"ij"]


def test_s3_small_body_uses_put_object():
    puts: list[tuple[str, bytes]] = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append((Key, Body))

        def create_multipart_upload(self, **_k):
            raise AssertionError("small body must not multipart")

    meta = upload_object_store_bytes(
        "s3",
        client=FakeS3(),
        bucket="landing",
        key="exports/a.json",
        body=b'{"ok":true}',
        content_type="application/json",
        threshold=64,
        part_size=32,
    )
    assert meta["method"] == "put_object"
    assert puts == [("exports/a.json", b'{"ok":true}')]


def test_s3_multipart_completes_and_reassembles():
    store: dict[str, bytes] = {}
    parts_in: dict[int, bytes] = {}

    class FakeS3:
        def create_multipart_upload(self, Bucket, Key, ContentType):  # noqa: N803
            return {"UploadId": "up-1"}

        def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):  # noqa: N803
            parts_in[int(PartNumber)] = bytes(Body)
            return {"ETag": f'"etag-{PartNumber}"'}

        def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):  # noqa: N803
            ordered = sorted(MultipartUpload["Parts"], key=lambda p: p["PartNumber"])
            store[Key] = b"".join(parts_in[p["PartNumber"]] for p in ordered)

        def abort_multipart_upload(self, **_k):
            raise AssertionError("successful upload must not abort")

        def put_object(self, **_k):
            raise AssertionError("large body must not PutObject")

    body = b"ABCDEFGHIJ"  # 10 bytes → 3 parts at size 4
    meta = upload_object_store_bytes(
        "s3",
        client=FakeS3(),
        bucket="landing",
        key="exports/big.json",
        body=body,
        content_type="application/json",
        threshold=8,
        part_size=4,
    )
    assert meta["method"] == "multipart"
    assert meta["parts"] == 3
    assert store["exports/big.json"] == body


def test_s3_multipart_aborts_when_a_part_fails():
    aborted: list[str] = []

    class FakeS3:
        def create_multipart_upload(self, **_k):
            return {"UploadId": "up-fail"}

        def upload_part(self, PartNumber, **_k):  # noqa: N803
            if int(PartNumber) >= 2:
                raise RuntimeError("part 2 failed")
            return {"ETag": '"etag-1"'}

        def complete_multipart_upload(self, **_k):
            raise AssertionError("failed upload must not complete")

        def abort_multipart_upload(self, UploadId, **_k):  # noqa: N803
            aborted.append(UploadId)

        def put_object(self, **_k):
            raise AssertionError("large body must not PutObject")

    with pytest.raises(RuntimeError, match="part 2 failed"):
        upload_object_store_bytes(
            "s3",
            client=FakeS3(),
            bucket="landing",
            key="exports/big.json",
            body=b"ABCDEFGHIJ",
            content_type="application/json",
            threshold=8,
            part_size=4,
        )
    assert aborted == ["up-fail"]


class _FakeGcsBlob:
    def __init__(self, name: str, store: dict[str, bytes]):
        self.name = name
        self.store = store
        self.content_type = ""

    def upload_from_string(self, body, content_type=None):
        self.store[self.name] = bytes(body)
        if content_type:
            self.content_type = content_type

    def delete(self):
        self.store.pop(self.name, None)

    def compose(self, sources):
        self.store[self.name] = b"".join(self.store[s.name] for s in sources)


class _FakeGcsBucket:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def blob(self, name: str) -> _FakeGcsBlob:
        return _FakeGcsBlob(name, self.store)


def test_gcs_compose_reassembles_and_deletes_components():
    bucket = _FakeGcsBucket()
    body = b"ABCDEFGHIJ"
    meta = upload_object_store_bytes(
        "gcs",
        bucket_obj=bucket,
        key="exports/big.json",
        body=body,
        content_type="application/json",
        threshold=8,
        part_size=4,
    )
    assert meta["method"] == "compose"
    assert meta["parts"] == 3
    assert bucket.store["exports/big.json"] == body
    leftovers = [k for k in bucket.store if ".__df_mp__" in k]
    assert leftovers == []


def test_gcs_compose_failure_deletes_components():
    class BoomBlob(_FakeGcsBlob):
        def compose(self, sources):
            if ".__df_mp__" not in self.name:
                raise RuntimeError("compose denied")
            super().compose(sources)

    class BoomBucket(_FakeGcsBucket):
        def blob(self, name: str) -> _FakeGcsBlob:
            return BoomBlob(name, self.store)

    bucket = BoomBucket()
    with pytest.raises(RuntimeError, match="compose denied"):
        upload_object_store_bytes(
            "gcs",
            bucket_obj=bucket,
            key="exports/big.json",
            body=b"ABCDEFGHIJ",
            content_type="application/json",
            threshold=8,
            part_size=4,
        )
    leftovers = [k for k in bucket.store if ".__df_mp__" in k]
    assert leftovers == []
    assert "exports/big.json" not in bucket.store


def test_gcs_tree_compose_above_32_sources():
    bucket = _FakeGcsBucket()
    body = b"x" * (GCS_COMPOSE_LIMIT + 1)
    meta = upload_object_store_bytes(
        "gcs",
        bucket_obj=bucket,
        key="exports/wide.json",
        body=body,
        content_type="application/json",
        threshold=2,
        part_size=1,
    )
    assert meta["parts"] == GCS_COMPOSE_LIMIT + 1
    assert bucket.store["exports/wide.json"] == body
    assert not any(".__df_mp__" in k for k in bucket.store)


class _FakeAdlsBlob:
    def __init__(self):
        self.blocks: dict[str, bytes] = {}
        self.committed: bytes | None = None
        self.single: bytes | None = None

    def upload_blob(self, body, overwrite=True, content_type=None):
        self.single = bytes(body)

    def stage_block(self, block_id, data):
        self.blocks[block_id] = bytes(data)

    def commit_block_list(self, block_ids, content_settings=None):
        self.committed = b"".join(self.blocks[i] for i in block_ids)


def test_adls_blocks_reassemble():
    blob = _FakeAdlsBlob()
    body = b"ABCDEFGHIJ"
    meta = upload_object_store_bytes(
        "adls",
        blob_client_factory=lambda _k: blob,
        key="exports/big.json",
        body=body,
        content_type="application/json",
        threshold=8,
        part_size=4,
    )
    assert meta["method"] == "block_list"
    assert meta["parts"] == 3
    assert blob.committed == body
    assert blob.single is None


def test_adls_failed_stage_does_not_commit():
    class Boom(_FakeAdlsBlob):
        def stage_block(self, block_id, data):
            if len(self.blocks) >= 1:
                raise RuntimeError("stage denied")
            super().stage_block(block_id, data)

        def commit_block_list(self, *a, **k):
            raise AssertionError("failed stage must not commit")

    blob = Boom()
    with pytest.raises(RuntimeError, match="stage denied"):
        upload_object_store_bytes(
            "adls",
            blob_client_factory=lambda _k: blob,
            key="exports/big.json",
            body=b"ABCDEFGHIJ",
            content_type="application/json",
            threshold=8,
            part_size=4,
        )
    assert blob.committed is None


def test_s3_writer_large_export_uses_multipart(monkeypatch):
    from connectors import s3_writer

    completed: list[str] = []
    puts: list[str] = []

    class FakeS3:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append(Key)

        def create_multipart_upload(self, Bucket, Key, ContentType):  # noqa: N803
            return {"UploadId": f"up-{Key}"}

        def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):  # noqa: N803
            return {"ETag": f'"e{PartNumber}"'}

        def complete_multipart_upload(self, Bucket, Key, **_k):  # noqa: N803
            completed.append(Key)

        def abort_multipart_upload(self, **_k):
            raise AssertionError("writer success must not abort")

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: FakeS3())
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    monkeypatch.setattr("connectors.s3_reader.list_objects", lambda *a, **k: [])

    rows = [[str(i), "x" * 8] for i in range(20)]
    result = s3_writer.write_mapped_rows(
        host="",
        port=0,
        database="landing",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="exports/orders.json",
        headers=["id", "note"],
        data_rows=rows,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "note"},
        ],
        column_types={"id": "string", "note": "string"},
        create_table=True,
        dest_extra={"multipart_threshold": 40, "multipart_part_size": 30},
    )
    assert result.ok, result.error
    assert completed
    assert any(k == "exports/orders.json" for k in completed)
    assert not puts  # large path must not single-PUT


def test_capability_mentions_multipart_honesty():
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    s3 = " ".join(CAPABILITY_REGISTRY["s3"]["common_issues"])
    assert "multipart" in s3.lower()
    assert "in memory" in s3.lower() or "serialized in memory" in s3.lower()
    assert "at-least-once" in s3.lower()
    gcs = " ".join(CAPABILITY_REGISTRY["gcs"]["common_issues"])
    assert "compose" in gcs.lower()
    adls = " ".join(CAPABILITY_REGISTRY["adls"]["common_issues"])
    assert "stage_block" in adls.lower()
