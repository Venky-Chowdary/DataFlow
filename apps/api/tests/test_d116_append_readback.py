"""D116 (read-back): keyed samples of an appended object must return the
rows this run appended, not the older copy of the same key left earlier in
the file.

Prod job 6adfd056: an incremental append to an S3 CSV held the old copy of
id=1 plus the appended copy; the keyed read-back truncated at the limit in
file order, so the appended copy was dropped from the sample and the new
source row was compared against the *older* copy — job FAILED while the
batch stayed in the file.
"""

from __future__ import annotations

import io

from services.dest_precount import sample_artifact_records, sample_object_store


PRIOR_CSV = b"id,name\n1,old-name\n2,keeper\n"
NEW_CSV = b"id,name\n1,new-name\n3,added\n"


def _csv_stream(body: bytes) -> io.BytesIO:
    return io.BytesIO(body)


def test_keyed_sample_keeps_last_copy_per_key():
    """The appended copy at the tail is the row this run wrote."""
    rows = sample_artifact_records(
        _csv_stream(PRIOR_CSV.rstrip() + b"\n" + NEW_CSV.split(b"\n", 1)[1]),
        name="orders/export.csv",
        limit=1,
        sort_key="id",
        keys=[1],
    )
    assert rows == [{"id": "1", "name": "new-name"}]


def test_keyed_sample_returns_newest_limited():
    rows = sample_artifact_records(
        _csv_stream(PRIOR_CSV.rstrip() + b"\n" + NEW_CSV.split(b"\n", 1)[1]),
        name="orders/export.csv",
        limit=50,
        sort_key="id",
        keys=[1, 2, 3],
    )
    assert len(rows) == 3
    by_id = {r["id"]: r["name"] for r in rows}
    assert by_id == {"1": "new-name", "2": "keeper", "3": "added"}


def test_sample_object_store_keeps_last_copy_across_parts(monkeypatch):
    from services import dest_precount
    from services import object_streaming

    monkeypatch.setattr(
        dest_precount,
        "_object_store_list_keys",
        lambda kind, cfg, bucket, key: ["orders/part-00001.csv", "orders/part-00002.csv"],
    )
    bodies = {
        "orders/part-00001.csv": b"id,name\n1,old-name\n",
        "orders/part-00002.csv": b"id,name\n1,new-name\n",
    }

    def _open(kind, cfg, bucket, obj_key):
        if obj_key not in bodies:
            return False
        stream = io.BytesIO(bodies[obj_key])
        return stream, None

    monkeypatch.setattr(object_streaming, "open_object_store_binary", _open)
    rows = sample_object_store(
        "s3",
        {"database": "bucket"},
        table_name="orders/export.csv",
        sort_key="id",
        keys=[1],
    )
    assert rows == [{"id": "1", "name": "new-name"}]
