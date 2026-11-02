"""M2 — job source_type is the connector format, not the endpoint kind.

Repro: an S3 or REST source job shows ``source_type="database"`` — the job
doc stamped ``request.source.kind``, which is the routing kind and defaults
to "database" even for file/object/API sources.
"""

from __future__ import annotations


def _pending_job_doc(monkeypatch, source_format: str, source_kind: str) -> dict:
    from src.transfer import engine as engine_module
    from src.transfer.job_submission import create_pending_job
    from src.transfer.models import EndpointConfig, TransferRequest

    captured: dict = {}

    class FakeEngine:
        def _resolve_saved_connectors(self, request):
            pass

        def _claim_idempotency(self, request, job_doc):
            return None

        def _claim_destination(self, request, job_id):
            return None

    class FakeMongo:
        def create_transfer_job(self, job_doc):
            captured.update(job_doc)
            return "job-1"

    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: FakeMongo())
    monkeypatch.setattr(
        engine_module, "_mapping_proof_for_request", lambda r: None
    )
    monkeypatch.setattr(
        engine_module.lifecycle_events, "job_requested_fields", lambda r: {}
    )

    request = TransferRequest(
        source=EndpointConfig(kind=source_kind, format=source_format, table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", table="d"),
    )
    create_pending_job(FakeEngine(), request)
    return captured


def test_s3_source_reports_s3_not_database(monkeypatch):
    doc = _pending_job_doc(monkeypatch, "s3", "database")
    assert doc["source_type"] == "s3"
    assert doc["source_kind"] == "database"


def test_rest_source_reports_rest_api(monkeypatch):
    doc = _pending_job_doc(monkeypatch, "rest_api", "database")
    assert doc["source_type"] == "rest_api"


def test_database_source_unchanged(monkeypatch):
    doc = _pending_job_doc(monkeypatch, "postgresql", "database")
    assert doc["source_type"] == "postgresql"
