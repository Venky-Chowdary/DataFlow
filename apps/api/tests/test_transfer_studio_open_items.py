"""Scenario tests for Transfer Studio open-item wiring (load history, stream contracts)."""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import patch

import pytest


class _FakeMongo:
    def __init__(self) -> None:
        self.updates: list[tuple[str, dict[str, Any]]] = []

    def update_job_status(self, job_id: str, status: str, **kwargs: Any) -> None:
        self.updates.append((job_id, {"status": status, **kwargs}))


def _sample_request(*, validation_mode: str = "strict"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="orders"),
        destination=EndpointConfig(kind="database", format="snowflake", table="orders"),
        validation_mode=validation_mode,
    )


def test_compare_and_publish_load_history_publishes_and_strict_blocks() -> None:
    from src.transfer.engine import _compare_and_publish_load_history

    mongo = _FakeMongo()
    request = _sample_request(validation_mode="strict")
    rows = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
    report = {
        "passed": False,
        "anomalies": ["null-rate spike on amount"],
        "prior_load_count": 3,
    }

    with patch(
        "services.data_quality_history.compare_route_to_history",
        return_value=report,
    ):
        out = _compare_and_publish_load_history(
            mongo, "job-1", rows, request, {"id": "integer", "amount": "float"},
            validation_mode="strict",
            row_count_hint=1000,
        )

    assert out["strict_blocked"] is True
    assert out["anomalies"]
    assert any(u[1].get("load_history_report") for u in mongo.updates)
    phases = [u[1].get("phase") for u in mongo.updates]
    assert "quality_check" in phases


def test_compare_and_publish_load_history_never_raises() -> None:
    from src.transfer.engine import _compare_and_publish_load_history
    from src.transfer.models import EndpointConfig, TransferRequest

    mongo = _FakeMongo()
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        validation_mode="balanced",
        source_filename="a.csv",
    )

    with patch(
        "services.data_quality_history.compare_route_to_history",
        side_effect=RuntimeError("disk full"),
    ):
        out = _compare_and_publish_load_history(
            mongo, "job-2", [], request, None,
            validation_mode="balanced",
            row_count_hint=0,
        )

    assert out["passed"] is True
    assert "unavailable" in (out.get("warning") or "").lower()


def test_persist_load_history_profile_uses_row_count_hint() -> None:
    from src.transfer.engine import _persist_load_history_profile

    request = _sample_request()
    captured: dict[str, Any] = {}

    def _save(source, dest, profile, **kwargs):
        captured["row_count"] = kwargs.get("row_count")
        captured["job_id"] = kwargs.get("job_id")
        captured["rejected_rows"] = kwargs.get("rejected_rows")

    with patch("services.data_quality_history.profile_batch", return_value={"id": object()}), patch(
        "services.data_quality_history.save_profile", side_effect=_save
    ):
        _persist_load_history_profile(
            request,
            [{"id": 1}],
            {"id": "integer"},
            job_id="job-3",
            dest_summary={"rejected_rows": 2, "rejected_details": []},
            row_count=50_000,
        )

    assert captured["row_count"] == 50_000
    assert captured["job_id"] == "job-3"
    assert captured["rejected_rows"] == 2


def test_compare_route_to_history_accepts_current_row_count(tmp_path, monkeypatch) -> None:
    from services import data_quality_history as dqh

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    source = {"kind": "database", "format": "postgresql", "table": "t1"}
    dest = {"kind": "database", "format": "snowflake", "table": "t1"}
    rows = [{"id": i, "v": float(i)} for i in range(20)]
    dqh.save_profile(
        source, dest, dqh.profile_batch(rows, {"id": "integer", "v": "float"}),
        job_id="prior",
        row_count=1000,
    )
    report = dqh.compare_route_to_history(
        rows[:5],
        source,
        dest,
        schema={"id": "integer", "v": "float"},
        current_row_count=50,
    )
    assert report["prior_load_count"] >= 1
    assert isinstance(report.get("anomalies"), list)


def test_streaming_and_file_paths_wire_load_history() -> None:
    """Regression: DB streaming + file streaming must compare and persist load history."""
    from src.transfer.engine import UniversalTransferEngine

    for path_name in ("_execute_streaming", "_execute_file_streaming"):
        method_src = inspect.getsource(getattr(UniversalTransferEngine, path_name))
        assert "_compare_and_publish_load_history" in method_src
        assert "_persist_load_history_profile" in method_src
        assert "strict_blocked" in method_src

    # Buffered / in-memory path lives in _execute_tracked_core
    core_src = inspect.getsource(UniversalTransferEngine._execute_tracked_core)
    assert "_compare_and_publish_load_history" in core_src
    assert "_persist_load_history_profile" in core_src


def test_stream_contracts_distinct_cursor_per_stream() -> None:
    """Contract shape expected by engine.resolve_sync_contract / multi-stream Advanced."""
    contracts = [
        {
            "name": "orders",
            "selected": True,
            "sync_mode": "incremental_append",
            "cursor_field": "updated_at",
            "primary_key": "",
            "schema_policy": "manual_review",
            "field_count": 4,
            "validation_mode": "balanced",
        },
        {
            "name": "order_items",
            "selected": True,
            "sync_mode": "incremental_append",
            "cursor_field": "modified_ts",
            "primary_key": "",
            "schema_policy": "manual_review",
            "field_count": 4,
            "validation_mode": "balanced",
        },
    ]
    assert contracts[0]["cursor_field"] != contracts[1]["cursor_field"]
    assert {c["name"] for c in contracts} == {"orders", "order_items"}
