"""D374: an empty or incomplete POST /api/v1/transfer/run answers 422 and creates no job.

QA: body ``{}`` created job 6afb9afe5eafee38493e13ad on the form defaults
(source file "database", destination MongoDB ``test_db``). 8d2b01127 refused
``{}``; 1455a8a66 refused a destination with no source. The review gap was
that no test proved "no job row": these do, through the real app, for the
QA's exact JSON body, an empty form, and a destination-only form, by
counting both the engine's job creation and the job store.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


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
    ("kwargs", "names"),
    [
        ({"json": {}}, ("source", "destination")),
        ({"data": {}}, ("source", "destination")),
        ({"data": {"dest_format": "postgresql", "dest_host": "db.example", "dest_table": "t"}}, ("source",)),
    ],
    ids=["qa-json-body", "empty-form", "destination-only"],
)
def test_incomplete_run_is_422_and_no_job_exists(no_job, kwargs, names):
    from src.main import app

    before = _jobs()
    res = TestClient(app).post("/api/v1/transfer/run", **kwargs)
    assert res.status_code == 422, res.text
    detail = res.json()["detail"]
    for side in names:
        assert side in detail
    assert "no job was started" in detail
    assert res.json()["error"]["reference"]
    assert no_job == []
    assert _jobs() == before
