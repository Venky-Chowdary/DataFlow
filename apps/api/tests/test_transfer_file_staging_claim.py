"""Claim-queue file staging — Excel/CSV must not fail with re-upload on fresh submit."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_stored_upload_file_id_hydrates_without_reupload(tmp_path, monkeypatch):
    """Validate scanned this upload; Execute must stream the same bytes."""
    from services import file_parser as fp
    from src.transfer.models import EndpointConfig, TransferRequest
    from services.transfer_file_staging import (
        file_source_bytes_available,
        hydrate_file_source,
        requires_file_reupload,
    )

    monkeypatch.setattr(fp, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(fp, "REGISTRY_PATH", tmp_path / "reg.json")
    record = fp.store_upload("flights.csv", b"DEP_TIME\n12.345678\n7.9166665\n")

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra={"file_id": record["file_id"]}),
        destination=EndpointConfig(kind="database", format="snowflake", table="TREE"),
        source_filename="",
        source_content=b"",
    )
    assert requires_file_reupload(req) is False
    hydrate_file_source(req)
    assert file_source_bytes_available(req) is True
    assert Path(req.source_path).is_file()
    assert req.source_filename == "flights.csv"
    assert Path(req.source_path).read_bytes().endswith(b"7.9166665\n")


def test_content_only_marks_reupload_until_persisted(tmp_path, monkeypatch):
    from services import platform_config
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )
    from services.transfer_file_staging import persist_file_source

    uploads = tmp_path / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(platform_config, "upload_dir", lambda: uploads)

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="xlsx"),
        destination=EndpointConfig(kind="database", format="postgresql", table="excel"),
        source_filename="fsi-2019.xlsx",
        source_content=b"PK\x03\x04fake-xlsx",
    )
    assert transfer_request_to_dict(req)["requires_file_reupload"] is True

    persist_file_source(req, job_token="jobabc")
    payload = transfer_request_to_dict(req)
    assert payload["requires_file_reupload"] is False
    assert payload["source_path"]
    assert Path(payload["source_path"]).is_file()

    restored = transfer_request_from_dict(payload)
    assert restored.source_content == b""
    assert restored.source_path == payload["source_path"]
    from services.transfer_file_staging import file_source_bytes_available, hydrate_file_source

    hydrate_file_source(restored)
    assert file_source_bytes_available(restored) is True


def test_run_fleet_job_executes_when_path_staged(tmp_path, monkeypatch):
    """Regression: claim worker must not fail-closed when path was staged."""
    from services import platform_config
    from services.transfer_file_staging import persist_file_source
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    monkeypatch.setattr(platform_config, "upload_dir", lambda: tmp_path / "uploads")

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="t"),
        source_filename="tiny.csv",
        source_content=b"id,name\n1,a\n",
    )
    persist_file_source(req, job_token="fleet1")
    payload = transfer_request_to_dict(req)
    assert payload["requires_file_reupload"] is False

    calls: list[str] = []

    class _Mongo:
        def get_job(self, job_id):
            return {
                "_id": job_id,
                "status": "pending",
                "transfer_request": payload,
            }

        def update_job_status(self, job_id, status, **kwargs):
            calls.append(status)
            return True

    monkeypatch.setattr(
        "src.transfer.background.get_mongodb_service",
        lambda: _Mongo(),
    )

    def _fake_run(job_id, request, resume=False, resume_from_job_id=None):
        calls.append("ran")
        assert request.source_path
        assert Path(request.source_path).is_file()

    monkeypatch.setattr("src.transfer.background._run_transfer", _fake_run)

    from src.transfer.background import run_fleet_job

    run_fleet_job("job-fleet-1")
    assert "ran" in calls
    assert "failed" not in calls


def test_run_fleet_job_fails_when_bytes_unreachable(monkeypatch):
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        source_filename="gone.csv",
        source_content=b"id\n1\n",
    )
    # Serialize without persist — legacy / broken path.
    payload = transfer_request_to_dict(req)
    assert payload["requires_file_reupload"] is True
    # Strip path if any
    payload["source_path"] = ""
    payload["source_object_uri"] = ""

    statuses: list[str] = []

    class _Mongo:
        def get_job(self, job_id):
            return {"_id": job_id, "status": "pending", "transfer_request": payload}

        def update_job_status(self, job_id, status, **kwargs):
            statuses.append(status)
            return True

    monkeypatch.setattr(
        "src.transfer.background.get_mongodb_service",
        lambda: _Mongo(),
    )
    monkeypatch.setattr(
        "src.transfer.background._run_transfer",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    from src.transfer.background import run_fleet_job

    run_fleet_job("job-missing")
    assert statuses == ["failed"]
