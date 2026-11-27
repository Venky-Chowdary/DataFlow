"""D374 edge: a partial source object on POST /api/v1/transfer/run is a 422 and creates no job.

{} and a destination-only body were refused on build 70cb (test_d374_incomplete_run_creates_no_job).
A source that names only a table or a database, with no file, no stored upload, no
saved connector and no connection, still passed the "has a source" check and ran on
the form's invented defaults: source kind ``file`` with no file, which created a job
that then failed "File wasn't found" (the QA symptom). Driven through the real app;
the engine's job creation and the job store are both counted.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

DEST = {"dest_format": "postgresql", "dest_host": "db.example.com", "dest_table": "t"}


@pytest.fixture
def no_job(monkeypatch):
    from src.transfer.engine import UniversalTransferEngine

    created: list[object] = []

    def _refuse(self, request):  # noqa: ANN001
        created.append(request)
        raise AssertionError("a job was created for an incomplete request")

    monkeypatch.setattr(UniversalTransferEngine, "_create_pending_job", _refuse)
    return created


def _jobs() -> int:
    from services.mongodb_service import get_mongodb_service

    counts = get_mongodb_service().count_jobs() or {}
    return int(counts.get("total") or 0)


@pytest.mark.parametrize(
    "source",
    [
        {"source_table": "orders"},
        {"source_collection": "orders"},
        {"source_kind": "file", "source_format": "csv", "source_table": "orders"},
        {"source_kind": "database", "source_format": "postgresql", "source_table": "orders"},
        {"source_kind": "database", "source_format": "mysql", "source_schema": "app", "source_table": "orders"},
    ],
    ids=["table-only", "collection-only", "file-kind-no-file", "db-kind-no-connection", "db-kind-schema-table"],
)
def test_partial_source_is_422_names_what_is_missing_and_no_job_exists(no_job, source):
    from src.main import app

    before = _jobs()
    res = TestClient(app).post("/api/v1/transfer/run", data={**source, **DEST})
    assert res.status_code == 422, res.text
    detail = res.json()["detail"]
    assert "source" in detail and "no job was started" in detail, detail
    assert res.json()["error"]["reference"]
    assert no_job == []
    assert _jobs() == before


def test_a_sqlite_source_path_is_a_complete_source(no_job):
    """Control: a file-backed database named by its path is a source, not a partial one."""
    from src.routers.transfer_router import incomplete_source_detail

    assert incomplete_source_detail(
        kind="database", fmt="sqlite", has_file=False, has_connection=False, database="/data/orders.db",
    ) == ""
    assert incomplete_source_detail(
        kind="file", fmt="csv", has_file=True, has_connection=False, database="",
    ) == ""
    assert incomplete_source_detail(
        kind="database", fmt="postgresql", has_file=False, has_connection=True, database="",
    ) == ""
