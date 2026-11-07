"""QA MX3-15 — replay_quarantine must refuse a transform_overrides it cannot apply.

QA: replay_quarantine(transform_overrides={"flt": "null"}) staged "with the
staged cell edits"; the child replay then re-ran the original cast
(``Invalid decimal: 'not-a-number'``, rows_written=0) — the override was
silently ignored.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_mx3_14_cast_and_continue_distinct as _mx314  # noqa: E402
from test_mx3_14_cast_and_continue_distinct import _pg_reachable, _run  # noqa: E402
from src.ai.copilot import lifecycle_tools as lt  # noqa: E402


def _job():
    return {
        "id": "job-mx315abcdef",
        "status": "completed_with_quarantine",
        "rejected_rows": 1,
        "transfer_request": {
            "source": {"connector_name": "Src", "table": "pay"},
            "destination": {"connector_name": "Dst"},
            "mappings": [{"source": "id", "target": "id"}, {"source": "flt", "target": "flt"}],
        },
    }


@pytest.fixture
def staged(monkeypatch):
    acks: list[dict] = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            acks.append({"kind": kind, "payload": payload})
            return f"ack-{len(acks)}"

    import src.ai.copilot.ack_ledger as ledger_mod

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job(), ""))
    return acks


def test_unknown_override_transform_is_refused_at_staging(staged):
    tr = lt._job_tool("replay_quarantine", "job-mx315abcdef", "", transform_overrides={"flt": "null"})
    assert not tr.success
    assert "'null' is not a transform" in tr.error
    assert "cell set to null" in tr.error
    assert staged == []


def test_override_on_an_unmapped_column_is_refused_at_staging(staged):
    tr = lt._job_tool("replay_quarantine", "job-mx315abcdef", "", transform_overrides={"amount": "text"})
    assert not tr.success
    assert "does not map" in tr.error and "'amount'" in tr.error
    assert staged == []


def test_known_override_and_ui_alias_still_stage(staged):
    assert lt._job_tool("replay_quarantine", "job-mx315abcdef", "", transform_overrides={"flt": "none"}).success
    assert lt._job_tool("replay_quarantine", "job-mx315abcdef", "", transform_overrides={"FLT": "cast_number"}).success
    assert [a["payload"]["transform_overrides"] for a in staged] == [{"flt": "none"}, {"FLT": "cast_number"}]


@pytest.fixture
def replay_route():
    yield from _mx314.cast_route.__wrapped__()


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_rest_replay_refuses_unknown_override_and_writes_nothing(replay_route):
    from fastapi import HTTPException

    from services.mongodb_service import get_mongodb_service
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine

    job, _rows = _run(replay_route, {"execution_policy": "QUARANTINE_ROW"}, "o")
    assert job["status"] == "completed_with_quarantine", job
    before = {j.get("_id") for j in get_mongodb_service().list_jobs(limit=500)}

    class _State:
        pass

    class _Req:
        state = _State()
        headers: dict[str, str] = {}

    with pytest.raises(HTTPException) as exc:
        asyncio.run(replay_job_quarantine(
            job["id"], QuarantineReplayRequest(transform_overrides={"flt": "null"}), _Req(),
        ))
    assert exc.value.status_code == 400
    assert "'null' is not a transform" in str(exc.value.detail)
    after = {j.get("_id") for j in get_mongodb_service().list_jobs(limit=500)}
    assert after == before, "a refused replay must not create a child job"
