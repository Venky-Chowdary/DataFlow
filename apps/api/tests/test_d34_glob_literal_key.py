"""D34 — a file pattern is never read as an object key.

A pickup source such as ``in/*.csv`` that matched one file failed with
"Object store has no object 'in/*.csv'": the stream peek handed patterns
matching fewer than two files back to the single-object reader, and every
preflight reader (sample, nullability, population, duplicate-key probe)
issued GET on the pattern itself — unread evidence, then a failed or
mis-contracted load. One match now reads that object, no match is "file not
arrived", and several matches refuse a single-object read.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

import src.transfer.engine as engine_mod
from src.transfer.models import EndpointConfig
from tests.test_cr_d125_file_quoted_empty_fidelity import (
    assert_verified,
    put_s3_object,
    run_transfer,
    s3_source,
)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.file_load_ledger as ledger
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(ledger, "STORE_PATH", tmp_path / "file_load_ledger.json")


def test_pattern_matching_one_file_loads_it_and_never_gets_the_pattern(tmp_path, monkeypatch):
    import moto

    import services.object_streaming as object_streaming

    requested: list[str] = []
    real_download = object_streaming.download_s3_object

    def _download(path, cfg, bucket, key):
        requested.append(key)
        return real_download(path, cfg, bucket, key)

    monkeypatch.setattr(object_streaming, "download_s3_object", _download)
    bucket = f"d34-{uuid.uuid4().hex[:8]}"
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="t"
    )
    with moto.mock_aws():
        put_s3_object(bucket, "in/orders_1.csv", "id,name\n1,a\n2,b\n")
        result = run_transfer(source=s3_source(bucket, "in/*.csv"), destination=destination)
    assert_verified(result)
    conn = sqlite3.connect(str(tmp_path / "dst.db"))
    try:
        assert conn.execute("SELECT id, name FROM t ORDER BY id").fetchall() == [
            (1, "a"),
            (2, "b"),
        ]
    finally:
        conn.close()
    assert requested and all("*" not in key for key in requested), requested


def test_single_object_read_of_a_pattern_resolves_or_refuses():
    import moto

    from connectors.object_store_common import materialize_store_spill
    from services.file_pickup import FileNotArrived

    bucket = f"d34-{uuid.uuid4().hex[:8]}"
    cfg = {
        "host": "s3.amazonaws.com", "port": 443, "database": bucket,
        "username": "testing", "password": "testing", "region": "us-east-1",
    }
    with moto.mock_aws():
        put_s3_object(bucket, "in/a_1.csv", "id\n1\n")
        path = materialize_store_spill("s3", cfg, bucket, "in/*.csv")
        assert open(path, encoding="utf-8").read() == "id\n1\n"
        with pytest.raises(FileNotArrived):
            materialize_store_spill("s3", cfg, bucket, "missing/*.csv")
        put_s3_object(bucket, "in/b_2.csv", "id\n2\n")
        with pytest.raises(ValueError, match="matches 2 objects"):
            materialize_store_spill("s3", cfg, bucket, "in/*.csv")
