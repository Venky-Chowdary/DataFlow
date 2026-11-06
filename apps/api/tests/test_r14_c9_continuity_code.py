"""QA C9 review: a continuity refusal is classified by its stable code, not its wording.

03b263d9d matched the refusal text twice (a failure_retry_policy regex and
schedule_recovery._CONTINUITY_MARKERS), so rewording the message would have
silently turned a parked, recoverable schedule back into a retried "unknown".
The refusal carries error_code DESTINATION_CONTINUITY (error_details and the
job), and both the retry policy and the schedule park read that.
"""

from __future__ import annotations

import pytest

from services.append_continuity import CONTINUITY_ERROR_CODE, compare
from services.failure_retry_policy import DETERMINISTIC, classify_job_failure


def _job(mismatch: str, error: str = "anything at all") -> dict:
    return {"status": "failed", "error": error, "error_code": CONTINUITY_ERROR_CODE,
            "error_details": {"error_code": CONTINUITY_ERROR_CODE, "mismatch": mismatch}}


def test_every_compare_refusal_carries_the_code():
    base = {"rows": 3, "checksum": "a" * 16, "algo": "engine_v1", "job_id": "j"}
    missing = compare("t", base, None, dest_missing=True)
    changed = compare("t", base, {"rows": 4, "checksum": "b" * 16, "algo": "engine_v1"})
    unreadable = compare("t", base, None, dest_missing=None)
    for violation in (missing, changed, unreadable):
        assert violation is not None and violation.details["error_code"] == CONTINUITY_ERROR_CODE
    assert unreadable.details["mismatch"] == "unreadable"


@pytest.mark.parametrize("mismatch", ["destination_missing", "row_count", "checksum"])
def test_a_drop_or_change_is_deterministic_whatever_the_wording(mismatch):
    verdict = classify_job_failure(_job(mismatch, error="Reworded: the target went away."))
    assert verdict.kind == DETERMINISTIC
    assert "recover_schedule_destination" in verdict.corrective_action


def test_an_unreadable_catalog_is_still_retried():
    assert classify_job_failure(_job("unreadable")).kind != DETERMINISTIC


def test_the_old_wording_without_the_code_is_not_matched():
    doc = {"status": "failed", "error": "Verification failed for table 't': the table no "
           "longer exists, but the last verified run (j) left 3 rows in it."}
    assert classify_job_failure(doc).corrective_action.find("recover_schedule_destination") == -1
