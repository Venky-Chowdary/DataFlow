"""G7-F3 review H2: ``tsv_null: "\\N"`` reaches the S3 writer on the real engine paths.

Before: the stream writer's S3/GCS/ADLS kwargs carried no ``dest_extra`` (only
SFTP merged it) and the adapter path read only the saved connector's nested
``extra``, while Gate-8 read ``tsv_null`` from the resolved destination config
— so the object was written with NULL as an empty cell and Gate-8 read it back
in the ``\\N`` convention: red on any NULL. After: one helper
(``connector_dispatch.file_writer_dest_extra``) hands every file-like writer
the saved + inline extra, the same view Gate-8 reads back with.

Bucket faked with moto (in requirements.txt).
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

moto = pytest.importorskip("moto")

ROWS = [(1, ""), (2, None), (3, "x"), (4, "\\N")]
EXPECTED_BODY = b'id\tnote\n1\t\n2\t\\N\n3\tx\n4\t"\\N"\n'


def _s3_dest(key: str, extra: dict) -> object:
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="s3", host="s3.amazonaws.com", port=443, database="g7f3bucket",
        username="mock", password="mock", table=key, extra=dict(extra),
    )


def _get(key: str) -> bytes:
    import boto3

    return boto3.client("s3", region_name="us-east-1").get_object(Bucket="g7f3bucket", Key=key)["Body"].read()


def _engine(monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    return UniversalTransferEngine()


@pytest.fixture()
def bucket(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="g7f3bucket")
        yield


def test_database_source_to_s3_tsv_stream_path(monkeypatch, tmp_path, bucket):
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, note TEXT)")
    conn.executemany("INSERT INTO notes VALUES (?, ?)", ROWS)
    conn.commit()
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(db), table="notes"),
        destination=_s3_dest("out/notes.tsv", {"tsv_null": "\\N"}),
        sync_mode="full_refresh_overwrite",
        mappings=[{"source": c, "target": c} for c in ("id", "note")],
        skip_preflight=True,
    )
    result = _engine(monkeypatch).execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert _get("out/notes.tsv") == EXPECTED_BODY
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation


def test_file_source_to_s3_tsv(monkeypatch, bucket):
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="notes.csv",
        source_content=b'id,note\n1,""\n2,\n3,x\n4,"\\N"\n',
        destination=_s3_dest("out/file_notes.tsv", {"tsv_null": "\\N"}),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    result = _engine(monkeypatch).execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert _get("out/file_notes.tsv") == EXPECTED_BODY
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation


def test_file_writer_dest_extra_merges_saved_inline_and_flattened():
    from src.transfer.connector_dispatch import file_writer_dest_extra

    class _Dest:
        extra = {"tsv_null": "\\N"}

    assert file_writer_dest_extra({"extra": {"a": 1}}, _Dest()) == {"a": 1, "tsv_null": "\\N"}
    # resolve_connector_config flattens the inline extra onto cfg: still seen.
    assert file_writer_dest_extra({"tsv_null": "\\N", "file_format": "tsv"}, None) == {
        "tsv_null": "\\N", "file_format": "tsv",
    }
    assert file_writer_dest_extra({"extra": {"tsv_null": "empty"}}, _Dest())["tsv_null"] == "\\N"


class _Stop(Exception):
    pass


@pytest.mark.parametrize("engine, module", [("gcs", "connectors.gcs_writer"), ("adls", "connectors.adls_writer")])
def test_gcs_and_adls_writers_receive_tsv_null_on_the_stream_path(monkeypatch, engine, module):
    """The stream batch writer hands GCS / ADLS the dest_extra Gate-8 reads (fake: the
    writer's encoder call is captured; no cloud client is built)."""
    import importlib

    from src.transfer.models import EndpointConfig
    from src.transfer.stream_write import _write_batch

    writer = importlib.import_module(module)
    seen: list[dict] = []

    def capture(**kwargs):
        seen.append(kwargs)
        raise _Stop("captured")

    monkeypatch.setattr(writer, "materialize_object_store_export", capture)
    dest = EndpointConfig(
        kind="database", format=engine, database="g7f3container", table="out/notes.tsv",
        extra={"tsv_null": "\\N"},
    )
    # The resolved cfg flattens the inline extra (resolve_connector_config) — no nested copy.
    cfg = {"host": "", "port": 0, "database": "g7f3container", "tsv_null": "\\N", "extra": {}}
    with pytest.raises(Exception):
        _write_batch(
            engine, dest, cfg, "out/notes.tsv", ["id", "note"], [["1", ""], ["2", None]],
            [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
            {"id": "TEXT", "note": "TEXT"}, True, None, 0, 1, 0,
            sync_mode="full_refresh_overwrite",
        )
    assert seen, "the writer's encoder was reached"
    assert seen[0].get("tsv_null") == "\\N"


def test_adapter_path_hands_the_s3_writer_tsv_null(monkeypatch, bucket):
    """adapters._write_destination_database (the non-stream path) with an inline extra."""
    from src.transfer.adapters import _write_destination_database

    written, _ddl, _summary = _write_destination_database(
        _s3_dest("out/adapter_notes.tsv", {"tsv_null": "\\N"}),
        [{"id": i, "note": n} for i, n in ROWS],
        ["id", "note"],
        {"id": "INTEGER", "note": "TEXT"},
        [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
    )
    assert written == len(ROWS)
    assert _get("out/adapter_notes.tsv") == EXPECTED_BODY
