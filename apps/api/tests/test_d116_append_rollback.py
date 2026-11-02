"""D116: object-store append read-back must compare the rows just appended,
and a failed append must not leave rows behind.

Prod job 6adfd056: an incremental append to an S3 CSV held the old copy of
id=1 plus the appended copy; the keyed read-back truncated at the limit in
file order, so the appended copy was dropped from the sample and the new
source row was compared against the *older* copy — job FAILED while the
batch stayed in the file, meaning a retry would duplicate it.
"""

from __future__ import annotations

import pytest

from connectors.object_store_incremental import (
    ObjectMergeRefused,
    prepare_incremental_export,
)
from connectors.object_store_common import (
    serialize_object_store_export,
)


PRIOR_CSV = b"id,name\n1,old-name\n2,keeper\n"
NEW_CSV = b"id,name\n1,new-name\n3,added\n"


# --- read-back compares the appended copies, not the stale ones -------------


def _export(body: bytes, key: str = "orders/export.csv"):
    return serialize_object_store_export(
        key=key,
        mapped_rows=[("1", "new-name"), ("3", "added")],
        target_cols=["id", "name"],
        dest_types=None,
        spill_max_size=1 << 20,
    )


def test_merge_snapshots_prior_object():
    snapped: list[bytes] = []
    export = _export(NEW_CSV)
    merged = prepare_incremental_export(
        key="orders/export.csv",
        export=export,
        fetch=lambda: PRIOR_CSV,
        sync_mode="incremental_append",
        total_chunks=1,
        protect_landed=False,
        created_here=False,
        target_cols=["id", "name"],
        dest_types=None,
        conflict_columns=None,
        spill_max_size=1 << 20,
        backup_put=snapped.append,
    )
    try:
        assert snapped == [PRIOR_CSV]
    finally:
        merged.close()


def test_merge_refuses_when_snapshot_fails():
    def _boom(body: bytes) -> None:
        raise RuntimeError("backup store down")

    with pytest.raises(ObjectMergeRefused):
        prepare_incremental_export(
            key="orders/export.csv",
            export=_export(NEW_CSV),
            fetch=lambda: PRIOR_CSV,
            sync_mode="incremental_append",
            total_chunks=1,
            protect_landed=False,
            created_here=False,
            target_cols=["id", "name"],
            dest_types=None,
            conflict_columns=None,
            spill_max_size=1 << 20,
            backup_put=_boom,
        )


# --- s3 writer stores the snapshot; undo restores it -------------------------


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(self, store: dict[str, bytes]) -> None:
        self.store = store

    def head_bucket(self, Bucket):  # noqa: N803
        return {}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)

    def _missing(self, Key):
        from botocore.exceptions import ClientError

        raise ClientError(
            {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
            "GetObject",
        )

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.store:
            self._missing(Key)
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.store:
            self._missing(Key)
        return {"Body": _Body(self.store[Key])}

    def copy_object(self, **kwargs):
        self.store[kwargs["Key"]] = self.store[kwargs["CopySource"]["Key"]]

    def copy(self, **kwargs):
        self.copy_object(**kwargs)

    def delete_object(self, Bucket, Key):  # noqa: N803
        self.store.pop(Key, None)


def _s3_write(monkeypatch, store, **overrides):
    from connectors import s3_writer

    fake = _FakeS3(store)
    monkeypatch.setattr(s3_writer, "boto3_client", lambda *a, **k: fake)
    monkeypatch.setattr(s3_writer, "_ensure_bucket", lambda *a, **k: None)
    # The backup PUT/DELETE goes through object_store_leftover's own client.
    import connectors.aws_common as aws_common

    monkeypatch.setattr(aws_common, "boto3_client", lambda *a, **k: fake)
    kwargs = dict(
        host="",
        port=0,
        database="bucket",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders/export.csv",
        headers=["id", "name"],
        data_rows=[["1", "new-name"], ["3", "added"]],
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


def test_s3_append_stores_preappend_snapshot(monkeypatch):
    store = {"orders/export.csv": PRIOR_CSV}
    result = _s3_write(monkeypatch, store)
    assert result.ok, result.error
    assert store["orders/export.csv.__df_append_backup__"] == PRIOR_CSV
    merged = store["orders/export.csv"].decode()
    assert "old-name" in merged and "new-name" in merged and "added" in merged


def test_s3_overwrite_drops_stale_snapshot(monkeypatch):
    backup = "orders/export.csv.__df_append_backup__"
    store = {"orders/export.csv": PRIOR_CSV, backup: b"id,name\n9,ancient\n"}
    result = _s3_write(monkeypatch, store, sync_mode="full_refresh")
    assert result.ok, result.error
    assert backup not in store


def test_failed_append_undo_restores_prior_image(monkeypatch):
    from src.transfer.models import EndpointConfig
    from services.batch_undo import undo_failed_batch_if_dest_was_empty
    from services.dest_precount import PRECOUNT_KEY
    import connectors.aws_common as aws_common
    import services.object_streaming as object_streaming

    backup = "orders/export.csv.__df_append_backup__"
    store = {
        "orders/export.csv": PRIOR_CSV.rstrip() + b"\n" + NEW_CSV.split(b"\n", 1)[1],
        backup: PRIOR_CSV,
    }
    fake = _FakeS3(store)
    monkeypatch.setattr(aws_common, "boto3_client", lambda *a, **k: fake)

    summary = {
        PRECOUNT_KEY: 2,
        "table": "orders/export.csv",
        "sync_mode": "incremental_append",
    }
    note = undo_failed_batch_if_dest_was_empty(
        destination=EndpointConfig(
            kind="object_store",
            format="s3",
            database="bucket",
            table="orders/export.csv",
        ),
        dest_summary=summary,
        sync_mode="incremental_append",
    )
    assert summary["partial_batch_undo"] == "restored"
    assert summary["partial_batch_removed"] is True
    assert store["orders/export.csv"] == PRIOR_CSV
    assert backup not in store
    assert "restored" in note


def test_failed_append_undo_retains_when_no_snapshot(monkeypatch):
    from src.transfer.models import EndpointConfig
    from services.batch_undo import undo_failed_batch_if_dest_was_empty
    from services.dest_precount import PRECOUNT_KEY
    import connectors.aws_common as aws_common

    merged = PRIOR_CSV.rstrip() + b"\n" + NEW_CSV.split(b"\n", 1)[1]
    store = {"orders/export.csv": merged}
    fake = _FakeS3(store)
    monkeypatch.setattr(aws_common, "boto3_client", lambda *a, **k: fake)

    summary = {
        PRECOUNT_KEY: 2,
        "table": "orders/export.csv",
        "sync_mode": "incremental_append",
    }
    note = undo_failed_batch_if_dest_was_empty(
        destination=EndpointConfig(
            kind="object_store",
            format="s3",
            database="bucket",
            table="orders/export.csv",
        ),
        dest_summary=summary,
        sync_mode="incremental_append",
    )
    assert summary["partial_batch_undo"] == "retained"
    assert not summary.get("partial_batch_removed")
    assert store["orders/export.csv"] == merged
    assert "pre-append" in note
