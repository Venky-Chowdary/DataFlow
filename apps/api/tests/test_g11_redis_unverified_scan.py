"""Security check m1: an unverified Redis SCAN is not a decision.

A Redis SCAN that found no key within its budget was taken as a new prefix
and stamped "exact" (note_checked), so the old-field probe never ran again
for that prefix. An exhausted, unverified budget now stamps nothing.
"""

from __future__ import annotations

from unittest.mock import patch

from tests.test_g11_naming_history_scope import _redis, _run, jobs  # noqa: F401


def test_an_exhausted_redis_scan_stamps_nothing_and_the_next_run_checks(jobs):  # noqa: F811
    """The reviewer's m1 probe."""
    from services.target_name_legacy import NoKeysWithinBudget

    with patch("services.target_name_legacy.redis_fields_present",
               side_effect=NoKeysWithinBudget("no customers:* key within budget")):
        refusal, job = _run(jobs, _redis())
    assert refusal == "" and not job.get("naming"), job.get("naming")
    with patch("services.target_name_legacy.redis_fields_present",
               return_value={"full_name"}) as probe:
        refusal, _ = _run(jobs, _redis())
    assert probe.call_count == 1 and "full_name" in refusal
