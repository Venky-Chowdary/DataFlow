"""GET /jobs/{id} must carry the measured cutover window, behind access control."""

from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi import HTTPException  # noqa: E402

# ``src.routers`` re-exports an APIRouter under the same name, so import the
# module explicitly rather than through the package attribute.
cr = importlib.import_module("src.routers.connectors_router")

T0 = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)


def _job_doc(**kw: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "_id": "job_1",
        "source_type": "postgresql",
        "destination_type": "mysql",
        "status": "running",
        "records_processed": 400_000,
        "total_rows": 1_000_000,
        "throughput_marks": [
            {"rows": 0, "at": T0.isoformat()},
            {"rows": 200_000, "at": (T0 + timedelta(seconds=100)).isoformat()},
            {"rows": 400_000, "at": (T0 + timedelta(seconds=200)).isoformat()},
        ],
    }
    doc.update(kw)
    return doc


def _request() -> Any:
    req = MagicMock()
    req.state = MagicMock()
    req.state.user = {"email": "ops@example.com"}
    return req


def _serve(monkeypatch: pytest.MonkeyPatch, doc: dict[str, Any] | None) -> Any:
    mongo = MagicMock()
    mongo.get_job.return_value = doc
    monkeypatch.setattr(cr, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(cr, "_can_access_job", lambda *_a, **_k: doc is not None)
    return asyncio.run(cr.get_transfer_job("job_1", _request()))


def test_running_job_reports_a_measured_window(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _serve(monkeypatch, _job_doc())
    est = body["runtime_estimate"]
    assert est["available"] is True
    assert est["basis"] == "observed_checkpoints"
    assert est["rows_per_second_p50"] == 2000.0
    assert est["rows_remaining"] == 600_000


def test_job_without_throughput_evidence_says_why(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _serve(monkeypatch, _job_doc(throughput_marks=[]))
    est = body["runtime_estimate"]
    assert est["available"] is False
    assert est["reason"]
    assert est["remaining_seconds_p50"] is None


def test_unknown_population_never_projects_a_finish_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _serve(monkeypatch, _job_doc(total_rows=0))
    est = body["runtime_estimate"]
    assert est["available"] is False
    assert est["finishes_at_p50"] == ""


def test_inaccessible_job_leaks_no_estimate(monkeypatch: pytest.MonkeyPatch) -> None:
    mongo = MagicMock()
    mongo.get_job.return_value = _job_doc()
    monkeypatch.setattr(cr, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(cr, "_can_access_job", lambda *_a, **_k: False)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(cr.get_transfer_job("job_1", _request()))
    assert exc.value.status_code == 404
