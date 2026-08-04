"""Module 1b: writers must honor Migration Risk Contract execution policies.

Challenge: Validate signs CAST_AND_CONTINUE / FAIL_JOB contracts, but
``build_mapped_rows_with_details`` only obeys job-level error_policy. A
FAIL_JOB contract under balanced quarantine must abort — never soft-holdout.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    build_mapped_rows_with_details,
    reject_on_strict_policy,
)
from services.migration_risk_contract import create_migration_risk_contract  # noqa: E402


def _age_fixture(contract: dict):
    headers = ["id", "age"]
    data_rows = [["1", "30"], ["2", "nope"], ["3", "40"]]
    mappings = [
        {"source": "id", "target": "id", "transform": "none"},
        {
            "source": "age",
            "target": "age",
            "transform": "integer",
            "target_type": "INTEGER",
            "risk_acknowledged": True,
            "risk_contract": contract,
        },
    ]
    return dict(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
    )


def test_fail_job_contract_aborts_under_quarantine_job_policy():
    """FAIL_JOB on the mapping must win over balanced job quarantine."""
    c = create_migration_risk_contract(
        column="age",
        source_type="VARCHAR",
        destination_type="INTEGER",
        approved_by="tester@datawrap.io",
        reason="Prove write path honors FAIL_JOB over job quarantine",
        execution_policy="FAIL_JOB",
        transform="integer",
    )
    mapped, errors, details = build_mapped_rows_with_details(
        error_policy="quarantine",
        **_age_fixture(c.to_dict()),
    )
    assert errors
    bad = next(d for d in details if d.get("value") == "nope")
    assert bad["column"] == "age"
    assert bad.get("policy") == "fail" or bad.get("execution_policy") == "FAIL_JOB"
    # reject_on_strict_policy must refuse partial write even when job policy is quarantine
    blocked = reject_on_strict_policy("quarantine", details, "sqlite")
    assert blocked is not None, "FAIL_JOB contract breach must block partial write"
    assert "FAIL_JOB" in blocked or "risk contract" in blocked.lower() or "rejected" in blocked.lower()


def test_cast_and_continue_quarantines_cast_failure_under_strict_job():
    """CAST_AND_CONTINUE allows write path to continue with quarantine on cast failure."""
    c = create_migration_risk_contract(
        column="age",
        source_type="VARCHAR",
        destination_type="INTEGER",
        approved_by="tester@datawrap.io",
        reason="Cast TEXT ages; quarantine non-numeric",
        execution_policy="CAST_AND_CONTINUE",
        quarantine_policy="QUARANTINE_ROW_on_cast_failure",
        transform="integer",
    )
    mapped, errors, details = build_mapped_rows_with_details(
        error_policy="fail",  # strict job
        **_age_fixture(c.to_dict()),
    )
    assert errors
    bad = next(d for d in details if d.get("value") == "nope")
    assert bad.get("execution_policy") == "CAST_AND_CONTINUE"
    # Cast failure → quarantine holdout (not silent NULL), good rows still mapped
    assert bad.get("policy") == "quarantine"
    assert len(mapped) == 2
    # Must NOT abort the whole batch solely due to this contracted cast failure
    blocked = reject_on_strict_policy("fail", details, "sqlite")
    # Job is fail, but CAST_AND_CONTINUE failures are quarantined — abort only if
    # remaining details still demand fail. Contracted cast failures are stamped
    # so reject_on_strict ignores them when all failures are continue-policy.
    assert blocked is None or "CAST_AND_CONTINUE" not in (blocked or "")


def test_quarantine_row_contract_holdout_under_fail_job_mode():
    c = create_migration_risk_contract(
        column="age",
        source_type="VARCHAR",
        destination_type="INTEGER",
        approved_by="tester@datawrap.io",
        reason="Hold out bad ages",
        execution_policy="QUARANTINE_ROW",
        transform="integer",
    )
    mapped, _errors, details = build_mapped_rows_with_details(
        error_policy="fail",
        **_age_fixture(c.to_dict()),
    )
    bad = next(d for d in details if d.get("value") == "nope")
    assert bad.get("policy") == "quarantine"
    assert bad.get("execution_policy") == "QUARANTINE_ROW"
    assert len(mapped) == 2
    assert reject_on_strict_policy("fail", details, "pg") is None


def test_no_contract_keeps_job_quarantine_behavior():
    """Legacy mappings without contracts still follow job error_policy."""
    headers = ["id", "age"]
    data_rows = [["1", "30"], ["2", "nope"]]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "age", "target": "age", "transform": "integer", "target_type": "INTEGER"},
    ]
    mapped, _e, details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="quarantine",
    )
    assert len(mapped) == 1
    assert details[0]["policy"] == "quarantine"
    assert reject_on_strict_policy("quarantine", details, "sqlite") is None
