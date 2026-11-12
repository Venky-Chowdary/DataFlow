"""Transfer Studio file exports through a private export bucket, with signed downloads.

On the hosted cloud the API, the worker and the scheduler have separate disks,
so a file export written to the worker's exports folder could not be downloaded
from the API (D276 refused it). With ``DATAWRAP_EXPORT_BUCKET`` set the job puts
the export in that private bucket under ``exports/<workspace>/<job>/<file>``;
``GET /api/v1/jobs/{job_id}/export/download`` gives a signed-in member of the
job's workspace a presigned URL of at most 15 minutes (audited, never logging
the URL); the scheduler's sweeper deletes objects past the 24–72 h retention
and the route then answers 410. Gate-8 binds the stored object (key, size,
rows, SHA-256) to the bytes it read back. Self-hosted installs keep the exports
folder; the hosted cloud without a bucket keeps the refusal.

S3 is moto (in-process); the route tests run the real app on the local MongoDB.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

moto = pytest.importorskip("moto")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

from r8_ent_harness import ent  # noqa: E402,F401

BUCKET = "dw-export-test"
LOCAL = "DATAWRAP_LOCAL_FILE_DESTINATIONS=on keeps it"
_RAILWAY_MARKERS = ("RAILWAY_ENVIRONMENT_NAME", "RAILWAY_DEPLOYMENT_ID", "RAILWAY_REPLICA_ID")
_ENV_CLEAR = (
    *_RAILWAY_MARKERS,
    "DATAWRAP_SQLITE_DESTINATION",
    "DATAFLOW_SQLITE_DESTINATION",
    "DATAWRAP_LOCAL_FILE_DESTINATIONS",
    "DATAFLOW_LOCAL_FILE_DESTINATIONS",
    "DATAWRAP_EXPORT_BUCKET",
    "DATAFLOW_EXPORT_BUCKET",
    "DATAWRAP_EXPORT_PREFIX",
    "DATAWRAP_EXPORT_S3_ENDPOINT",
    "DATAWRAP_EXPORT_S3_REGION",
    "DATAWRAP_EXPORT_S3_ACCESS_KEY",
    "DATAWRAP_EXPORT_S3_SECRET_KEY",
    "DATAWRAP_EXPORT_RETENTION_HOURS",
    "DATAWRAP_EXPORT_URL_TTL_SECONDS",
    "DATAWRAP_EXPORT_PART_SIZE_MB",
    "DATAWRAP_S3_ENDPOINT",
    "DATAFLOW_S3_ENDPOINT",
    "DATAWRAP_S3_ACCESS_KEY",
    "DATAFLOW_S3_ACCESS_KEY",
    "DATAWRAP_S3_SECRET_KEY",
    "DATAFLOW_S3_SECRET_KEY",
    "AWS_ENDPOINT_URL",
    "AWS_PROFILE",
)


def _not_hosted(monkeypatch) -> None:
    for key in _ENV_CLEAR:
        monkeypatch.delenv(key, raising=False)


def _hosted(monkeypatch) -> None:
    for key in (*_RAILWAY_MARKERS, "DATAWRAP_LOCAL_FILE_DESTINATIONS", "DATAFLOW_LOCAL_FILE_DESTINATIONS",
                "DATAWRAP_SQLITE_DESTINATION", "DATAFLOW_SQLITE_DESTINATION"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "export-bucket-deploy")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_NAME", "production")


@pytest.fixture()
def s3(monkeypatch):
    """A moto S3 with the private export bucket, configured through the env."""
    _not_hosted(monkeypatch)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        monkeypatch.setenv("DATAWRAP_EXPORT_BUCKET", BUCKET)
        yield client


def _keys(client) -> list[str]:
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket=BUCKET).get("Contents") or [])


def _files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file()] if root.exists() else []


def _seed(path: Path, rows: int = 31) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
            conn.executemany("INSERT INTO customers VALUES (?, ?)", [(i, f"c{i}") for i in range(1, rows + 1)])
    finally:
        conn.close()


# --------------------------------------------------------------------------- engine


@pytest.fixture()
def fake_store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(dict(fields or {}))
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    return fake


def _run(fake, destination: EndpointConfig, src: Path, workspace_id: str = "ws-acme"):
    job_id = "fxo" + uuid.uuid4().hex[:17]
    fake.update_job_status(job_id, "pending", transfer_request={}, workspace_id=workspace_id)
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        workspace_id=workspace_id,
    )
    return job_id, UniversalTransferEngine().execute_tracked(request, job_id)


def test_hosted_export_goes_to_the_scoped_key_and_the_proof_binds_it(tmp_path, fake_store, s3, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    job_id, result = _run(fake_store, EndpointConfig(kind="file_export", format="csv", table="customers"), src)
    assert result.success, result.error
    assert int(result.records_transferred or 0) == 31

    ds = result.destination_summary
    key = f"exports/ws-acme/{job_id}/export_{job_id}.csv"
    assert ds["storage"] == "object_store" and ds["object_key"] == key
    assert _keys(s3) == [key]
    assert _files(tmp_path / "exports") == []  # the local exports folder is untouched

    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert hashlib.sha256(body).hexdigest() == ds["sha256"]
    assert ds["size_bytes"] == len(body)
    assert len(list(csv.DictReader(io.StringIO(body.decode())))) == 31
    assert ds["download_url"] == f"/api/v1/jobs/{job_id}/export/download"

    # Gate-8 read back the very bytes that were hashed and uploaded …
    proof = result.reconciliation["export_object"]
    assert proof == {
        "storage": "object_store", "object_key": key, "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "rows": 31, "writer_rows": 31,
        "expires_at": ds["expires_at"],
    }
    assert result.reconciliation.get("artifact_row_count") == 31
    assert result.reconciliation.get("passed") is True
    # … and the job carries the same proof; the staged local copy is gone.
    job = fake_store.jobs[job_id]
    assert job["reconciliation"]["export_object"]["sha256"] == ds["sha256"]
    assert job["destination_summary"]["object_key"] == key
    assert not Path(ds["path"]).exists()
    expires = datetime.fromisoformat(ds["expires_at"].replace("Z", "+00:00"))
    assert timedelta(hours=47) < expires - datetime.now(timezone.utc) <= timedelta(hours=48)


def test_named_output_keeps_a_safe_name_under_the_job(tmp_path, fake_store, s3, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    job_id, result = _run(
        fake_store,
        EndpointConfig(kind="file_export", format="json", output_path="../../nightly/Orders 2026.json",
                       table="customers"),
        src,
        workspace_id="ws/../evil",
    )
    assert result.success, result.error
    assert _keys(s3) == [f"exports/ws_.._evil/{job_id}/Orders_2026.json"]
    assert _files(tmp_path / "exports") == []


def test_hosted_without_a_bucket_keeps_the_refusal(tmp_path, fake_store, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _not_hosted(monkeypatch)
    _hosted(monkeypatch)
    job_id, result = _run(fake_store, EndpointConfig(kind="file_export", format="csv", table="customers"), src)
    assert not result.success and LOCAL in str(result.error)
    assert int(result.records_transferred or 0) == 0
    assert _files(tmp_path / "exports") == []

    from services.local_file_policy import endpoint_local_destination_refusal
    from src.transfer.registry import get_capabilities, validate_transfer

    assert LOCAL in str(endpoint_local_destination_refusal({"kind": "file_export", "format": "csv"}))
    assert not validate_transfer("database", "postgresql", "file_export", "csv")[0]
    caps = get_capabilities()
    assert caps["destination_file_formats"] == []
    assert {f["id"] for f in caps["destination_file_formats_unavailable"]} >= {"csv", "json"}


def test_hosted_with_a_bucket_offers_file_formats(s3, monkeypatch):
    from services.catalog_service import get_connector_by_id
    from services.local_file_policy import endpoint_local_destination_refusal, local_destination_refusal
    from src.transfer.registry import get_capabilities, validate_transfer

    _hosted(monkeypatch)
    assert endpoint_local_destination_refusal({"kind": "file_export", "format": "csv"}) is None
    assert validate_transfer("database", "postgresql", "file_export", "csv")[0]
    caps = get_capabilities()
    assert "csv" in caps["destination_file_formats"]
    assert caps["destination_file_formats_unavailable"] == []
    assert [c for c in caps["live_combinations"] if c["dest_kind"] == "file_export"]
    assert (get_connector_by_id("csv") or {}).get("dest_ready") is True
    # Only file exports move to the bucket: local DuckDB / SQLite stay refused.
    assert "DuckDB" in str(local_destination_refusal({"type": "duckdb", "database": "/srv/x.duckdb"}))
    assert local_destination_refusal("sqlite")


def test_self_hosted_keeps_the_exports_folder_even_with_a_bucket(tmp_path, fake_store, s3, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _not_hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_EXPORT_BUCKET", BUCKET)
    monkeypatch.setenv("ENV", "production")
    job_id, result = _run(
        fake_store, EndpointConfig(kind="file_export", format="csv", output_path="self.csv", table="customers"), src
    )
    assert result.success, result.error
    ds = result.destination_summary
    assert "storage" not in ds and "object_key" not in ds
    assert ds["download_url"] == "/api/v1/transfer/download/self.csv"
    # In the job's workspace folder (audit H-2), not the shared flat folder.
    assert (tmp_path / "exports" / "_workspaces" / "ws-acme" / "self.csv").exists()
    assert not (tmp_path / "exports" / "self.csv").exists()
    assert "export_object" not in result.reconciliation
    assert _keys(s3) == []


def test_upload_failure_fails_the_job_and_leaves_nothing(tmp_path, fake_store, s3, monkeypatch):
    src = tmp_path / "src.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_EXPORT_BUCKET", "dw-export-missing")
    job_id, result = _run(fake_store, EndpointConfig(kind="file_export", format="csv", table="customers"), src)
    assert not result.success
    assert "export bucket" in str(result.error)
    assert _files(tmp_path / "exports") == []


# --------------------------------------------------------------------------- streaming


class _Reads(io.BytesIO):
    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:  # type: ignore[override]
        assert 0 < size, "the uploader must never read the whole stream at once"
        self.sizes.append(size)
        return super().read(size)


def test_large_export_is_a_streamed_multipart_upload(s3, monkeypatch):
    import moto.s3.models as moto_s3

    from services.export_object_store import upload_export_stream

    monkeypatch.setattr(moto_s3, "S3_UPLOAD_PART_MIN_SIZE", 1024)
    part = 64 * 1024
    data = os.urandom(5 * part + 123)
    stream, tee = _Reads(data), io.BytesIO()
    stored = upload_export_stream(stream, "exports/ws/job/big.csv", part_size=part, tee=tee)

    assert stored.parts == 6 and stored.size_bytes == len(data)
    assert set(stream.sizes) == {part}
    assert stored.sha256 == hashlib.sha256(data).hexdigest()
    assert tee.getvalue() == data
    head = s3.head_object(Bucket=BUCKET, Key="exports/ws/job/big.csv")
    assert head["ETag"].strip('"').endswith("-6")  # a multipart object
    assert s3.get_object(Bucket=BUCKET, Key="exports/ws/job/big.csv")["Body"].read() == data

    # A failing upload aborts: no object, no dangling multipart upload.
    class _Broken(io.BytesIO):
        calls = 0

        def read(self, size: int = -1) -> bytes:  # type: ignore[override]
            _Broken.calls += 1
            if _Broken.calls == 4:
                raise OSError("source went away")
            return super().read(size)

    with pytest.raises(OSError):
        upload_export_stream(_Broken(data), "exports/ws/job/broken.csv", part_size=part)
    assert "exports/ws/job/broken.csv" not in _keys(s3)
    assert not s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads")


def test_settings_are_clamped_and_the_endpoint_is_configurable(s3, monkeypatch):
    from services import export_object_store as eos

    assert eos.export_retention_hours() == 48 and eos.export_url_ttl_seconds() == 900
    for raw, hours in (("1", 24), ("500", 72), ("36", 36), ("nonsense", 48)):
        monkeypatch.setenv("DATAWRAP_EXPORT_RETENTION_HOURS", raw)
        assert eos.export_retention_hours() == hours
    monkeypatch.setenv("DATAWRAP_EXPORT_URL_TTL_SECONDS", "86400")
    assert eos.export_url_ttl_seconds() == 900
    monkeypatch.setenv("DATAWRAP_EXPORT_URL_TTL_SECONDS", "5")
    assert eos.export_url_ttl_seconds() == 60
    monkeypatch.setenv("DATAWRAP_EXPORT_PART_SIZE_MB", "1")
    assert eos.export_part_size() == 5 * 1024 * 1024
    # An S3-compatible endpoint (R2-style), with its own region.
    monkeypatch.setenv("DATAWRAP_EXPORT_S3_ENDPOINT", "https://objects.example.test")
    monkeypatch.setenv("DATAWRAP_EXPORT_S3_REGION", "auto")
    client = eos.export_client()
    assert client.meta.endpoint_url == "https://objects.example.test"
    assert client.meta.region_name == "auto"


# --------------------------------------------------------------------------- download route

def _stored_job(ent, s3, workspace_id: str, *, expires_in: timedelta = timedelta(hours=48)) -> tuple[str, str, bytes]:
    from services.mongodb_service import get_mongodb_service

    body = b"id,name\n1,a\n2,b\n"
    job_id = get_mongodb_service().create_transfer_job({
        "name": "export job", "workspace_id": workspace_id, "status": "completed",
        "source": {"type": "sqlite"}, "destination": {"type": "csv"},
    })
    key = f"exports/{workspace_id}/{job_id}/export_{job_id}.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    get_mongodb_service().update_job_fields(job_id, {"destination_summary": {
        "storage": "object_store", "object_key": key, "filename": f"export_{job_id}.csv",
        "size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "rows": 2,
        "expires_at": (datetime.now(timezone.utc) + expires_in).isoformat(),
    }})
    return job_id, key, body


def _fetch(url: str) -> bytes:
    import requests

    resp = requests.get(url, timeout=10)
    assert resp.status_code == 200, resp.text
    return resp.content


def _all_stored_text(ent) -> str:
    """Every document in the deployment's Mongo database and the audit file."""
    chunks = []
    for name in ent.db.list_collection_names():
        chunks.extend(json.dumps(doc, default=str) for doc in ent.db[name].find({}))
    audit_file = ent.tmp / "audit.jsonl"
    if audit_file.exists():
        chunks.append(audit_file.read_text(encoding="utf-8"))
    return "\n".join(chunks)


def test_member_gets_a_short_lived_audited_url(ent, s3, monkeypatch):
    _hosted(monkeypatch)
    monkeypatch.setenv("DATAWRAP_EXPORT_URL_TTL_SECONDS", "86400")  # capped at 15 minutes
    ws = ent.workspace("Export downloads")
    job_id, key, body = _stored_job(ent, s3, ws)
    viewer = ent.member(email="export-viewer@example.com", workspace_id=ws, role="viewer")

    before = datetime.now(timezone.utc)
    res = viewer.get(f"/api/v1/jobs/{job_id}/export/download")
    assert res.status_code == 200, res.text
    assert res.headers.get("cache-control") == "no-store"
    got = res.json()
    assert got["sha256"] == hashlib.sha256(body).hexdigest() and got["object_key"] == key
    assert got["expires_in"] <= 900
    expires = datetime.fromisoformat(got["expires_at"].replace("Z", "+00:00"))
    assert expires - before <= timedelta(minutes=15, seconds=5)
    query = parse_qs(urlparse(got["url"]).query)
    assert int(query["X-Amz-Expires"][0]) <= 900
    assert "attachment" in query["response-content-disposition"][0]
    fetched = _fetch(got["url"])
    assert hashlib.sha256(fetched).hexdigest() == got["sha256"]

    issued = [e for e in ent.audit(ws, limit=500) if e.get("action") == "job.export.download_url.issued"]
    assert len(issued) == 1, issued
    event = issued[0]
    assert event["actor"] == "export-viewer@example.com"
    assert event.get("workspace_id") == ws
    assert event["details"]["job_id"] == job_id and event["details"]["object_key"] == key
    signature = query["X-Amz-Signature"][0]
    stored = _all_stored_text(ent)
    assert signature not in stored and got["url"] not in stored  # never the signed URL


def test_non_member_and_anonymous_learn_nothing(ent, s3, monkeypatch):
    _hosted(monkeypatch)
    acme = ent.workspace("Acme exports")
    other = ent.workspace("Other exports")
    job_id, _key, _body = _stored_job(ent, s3, acme)
    stranger = ent.member(email="export-stranger@example.com", workspace_id=other, role="admin")
    missing = "0123456789abcdef01234567"

    foreign = stranger.get(f"/api/v1/jobs/{job_id}/export/download")
    absent = stranger.get(f"/api/v1/jobs/{missing}/export/download")
    assert foreign.status_code == absent.status_code == 404
    assert foreign.json() == absent.json() == {"detail": "Job not found"}

    anon = ent.anonymous()
    a1 = anon.get(f"/api/v1/jobs/{job_id}/export/download")
    a2 = anon.get(f"/api/v1/jobs/{missing}/export/download")
    assert a1.status_code == a2.status_code == 401
    assert a1.json() == a2.json()
    issued = [e for e in ent.audit(limit=500) if e.get("action") == "job.export.download_url.issued"]
    assert issued == []


def test_sweeper_deletes_expired_exports_and_the_route_answers_410(ent, s3, monkeypatch):
    from services.export_object_store import sweep_expired_exports
    from services.mongodb_service import get_mongodb_service

    _hosted(monkeypatch)
    ws = ent.workspace("Expiring exports")
    old_job, old_key, _ = _stored_job(ent, s3, ws)
    owner = ent.member(email="export-owner@example.com", workspace_id=ws, role="admin")
    assert owner.get(f"/api/v1/jobs/{old_job}/export/download").status_code == 200

    # 49 hours later, with the default 48 h retention.
    later = datetime.now(timezone.utc) + timedelta(hours=49)
    fresh_key = f"exports/{ws}/fresh/export_fresh.csv"
    report = sweep_expired_exports(now=later, force=True)
    assert report and report["deleted"] == 1
    assert old_key not in _keys(s3)
    assert get_mongodb_service().get_job(old_job).get("export_expired_at")
    res = owner.get(f"/api/v1/jobs/{old_job}/export/download")
    assert res.status_code == 410 and res.json() == {"detail": "export expired"}
    expired = [e for e in ent.audit(ws, limit=500) if e.get("action") == "job.export.expired"]
    assert expired and expired[0]["details"]["object_key"] == old_key

    # An export younger than the retention is kept.
    s3.put_object(Bucket=BUCKET, Key=fresh_key, Body=b"x")
    assert sweep_expired_exports(force=True)["deleted"] == 0
    assert fresh_key in _keys(s3)

    # Past its recorded expiry the route says 410 before the sweeper ran, and an
    # object a bucket lifecycle rule already removed is 410 too, not a dead URL.
    stale_job, _k, _b = _stored_job(ent, s3, ws, expires_in=timedelta(seconds=-1))
    assert owner.get(f"/api/v1/jobs/{stale_job}/export/download").status_code == 410
    gone_job, gone_key, _b = _stored_job(ent, s3, ws)
    s3.delete_object(Bucket=BUCKET, Key=gone_key)
    assert owner.get(f"/api/v1/jobs/{gone_job}/export/download").status_code == 410
    assert get_mongodb_service().get_job(gone_job).get("export_expired_at")


def test_without_a_bucket_the_sweeper_is_a_no_op(monkeypatch):
    from services.export_object_store import sweep_expired_exports

    _not_hosted(monkeypatch)
    assert sweep_expired_exports(force=True) is None
