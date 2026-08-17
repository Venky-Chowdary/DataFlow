"""A schedule must not replay a verdict that cannot change.

Reported from the field: a scheduled Snowflake→MySQL job stopped at "Validating
mapping and schema…" with a lossy type path and one mapping below the
confidence floor, wrote 0 rows, and was retried three times — each attempt
re-deciding the same configuration against the same catalogs and failing
identically, which spends the retry budget and buries the corrective action.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.failure_retry_policy import (  # noqa: E402
    DETERMINISTIC,
    TRANSIENT,
    UNKNOWN,
    classify_failure,
    classify_job_failure,
)
from services.schedule_runner import _retry_decision, _should_retry  # noqa: E402


def _validating_job() -> dict:
    return {
        "status": "failed",
        "phase": "validating",
        "records_processed": 0,
        "error": (
            "Lossy / fidelity collapse across type path: Fidelity risk across type "
            "path — impacts 1 gate check(s); open Map for column detail; Mapping "
            "confidence below floor: 1 mapping(s) below the Map confidence floor"
        ),
    }


def test_mapping_and_fidelity_blockers_are_deterministic():
    result = classify_job_failure(_validating_job())
    assert result.kind == DETERMINISTIC
    assert result.retryable is False
    assert "Map" in result.corrective_action


def test_gate_phase_without_a_written_row_is_deterministic():
    result = classify_failure(
        error="Job stopped before completion", phase="validating", rows_written=0
    )
    assert result.kind == DETERMINISTIC
    assert "Validate" in result.corrective_action


def test_operational_faults_stay_retryable():
    for message in (
        "Connection reset by peer while reading from source",
        "MySQL server has gone away: lock wait timeout exceeded",
        "503 Service Unavailable — please retry",
        "Worker lost during load phase",
    ):
        result = classify_failure(error=message, phase="loading", rows_written=0)
        assert result.kind == TRANSIENT, message
        assert result.retryable is True


def test_unrecognised_failure_is_retried_rather_than_assumed_fatal():
    result = classify_failure(error="Something odd happened", phase="loading")
    assert result.kind == UNKNOWN
    assert result.retryable is True


def test_a_load_phase_failure_after_writes_is_not_called_deterministic():
    result = classify_failure(error="", phase="loading", rows_written=1200)
    assert result.retryable is True


def test_schedule_refuses_to_retry_the_blocked_preflight():
    decision = _retry_decision(
        "failed", 0, 3, sync_mode="full_refresh_append", job_doc=_validating_job()
    )
    assert decision["retry"] is False
    assert decision["failure_class"]["kind"] == DETERMINISTIC
    assert "confidence floor" in decision["reason"] or "Map" in decision["reason"]
    assert (
        _should_retry("failed", 0, 3, sync_mode="overwrite", job_doc=_validating_job())
        is False
    )


def test_schedule_still_retries_a_dropped_connection():
    job = {
        "status": "failed",
        "phase": "loading",
        "records_processed": 0,
        "error": "Connection refused connecting to mysql:3306",
    }
    decision = _retry_decision("failed", 0, 3, sync_mode="overwrite", job_doc=job)
    assert decision["retry"] is True
    assert decision["failure_class"]["kind"] == TRANSIENT


def test_missing_job_document_does_not_block_the_retry():
    decision = _retry_decision("failed", 0, 3, sync_mode="overwrite", job_doc=None)
    assert decision["retry"] is True
