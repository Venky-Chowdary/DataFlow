"""One policy decides a bad cell, at Validate and at the write.

Production defect this pins: a 1M-row CSV → Snowflake load whose ``arr_time``
column carried a *signed continue-policy* Migration Risk Contract
(``DECIMAL(12,9) → NUMBER(11,8)``). Validate forecast "27 rows will be held out
in quarantine" — the contract says continue — and then the write aborted with
"strict error policy blocks partial write", committing nothing. Two different
answers for the same 27 cells.

The cause was that the write-time type matrix stamped its holdouts with
``policy="write_quarantine"`` and no ``execution_policy``, so the column's
contract never reached ``reject_on_strict_policy``: the job-level posture decided
alone. The contract is the authority in both directions — it may hold rows out
under a strict posture, and it must abort under a lenient one.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    apply_write_quarantine_matrix,
    reject_on_strict_policy,
)

TARGET_COLS = ["id", "arr_time"]
TARGET_TYPES = ["NUMBER(9,0)", "NUMBER(11,8)"]
# 12 fractional digits cannot fit NUMBER(11,8) — the production row class.
ROWS: list[tuple[Any, ...]] = [
    (1, "1.5"),
    (2, "12.123456789012"),
    (3, "2.25"),
]


def _signed_contract(execution_policy: str) -> dict[str, Any]:
    from services.migration_risk_contract import sign_risk_contract

    body: dict[str, Any] = {
        "risk_id": "fidelity_collapse",
        "severity": "high",
        "root_cause": "fidelity_collapse",
        "column": "arr_time",
        "source_type": "DECIMAL(12,9)",
        "destination_type": "NUMBER(11,8)",
        "transform": None,
        "rows_sampled": 25,
        "estimated_rows": 1000000,
        "expected_failure_pct": 0.003,
        "expected_precision_loss": True,
        "expected_truncation": False,
        "expected_nulls": False,
        "execution_policy": execution_policy,
        "quarantine_policy": "DLQ",
        "retry_policy": "NONE",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "admin@dataflow.app",
        "approved_at": "2026-08-17T00:00:00Z",
        "reason": "Declared fidelity collapse accepted for this load",
        "target": "arr_time",
    }
    body["signature"] = sign_risk_contract(body)
    return body


def _mappings(contract: dict[str, Any] | None) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = [
        {"source": "id", "target": "id", "target_type": "NUMBER(9,0)"},
        {"source": "arr_time", "target": "arr_time", "target_type": "NUMBER(11,8)"},
    ]
    if contract is not None:
        mapped[1]["risk_contract"] = contract
    return mapped


def _write(policy: str, contract: dict[str, Any] | None):
    """Run the writer sequence: type matrix, then the strict-policy decision."""
    rejected: list[dict[str, Any]] = []
    kept = apply_write_quarantine_matrix(
        list(ROWS),
        TARGET_COLS,
        TARGET_TYPES,
        rejected,
        policy,
        dialect_label="Snowflake",
        mappings=_mappings(contract),
        dest_db="snowflake",
    )
    abort = reject_on_strict_policy(policy, rejected, "Snowflake")
    return kept, rejected, abort


def test_signed_continue_contract_holds_rows_out_under_a_strict_posture() -> None:
    """The production case: Validate promised a holdout, so the write must hold out."""
    kept, rejected, abort = _write("fail", _signed_contract("QUARANTINE_ROW"))

    assert abort is None, f"a signed continue contract must not abort the load: {abort}"
    assert [r[0] for r in kept] == [1, 3], "only the unfit row is held out"
    assert len(rejected) == 1
    assert rejected[0]["execution_policy"] == "QUARANTINE_ROW"
    assert rejected[0]["risk_id"] == "fidelity_collapse"


def test_cast_and_continue_is_the_same_authority() -> None:
    _kept, rejected, abort = _write("fail", _signed_contract("CAST_AND_CONTINUE"))
    assert abort is None
    assert rejected[0]["execution_policy"] == "CAST_AND_CONTINUE"


def test_no_contract_under_a_strict_posture_still_aborts() -> None:
    """Nothing here weakens the fail-closed default — an unfit cell nobody signed
    for still stops the load, which is what the Validate gate now blocks on."""
    kept, rejected, abort = _write("fail", None)
    assert abort and "blocks partial write" in abort
    assert len(rejected) == 1
    assert [r[0] for r in kept] == [1, 3]


def test_a_fail_job_contract_aborts_even_under_a_lenient_posture() -> None:
    """The other direction of the same incoherence: a signed FAIL_JOB contract
    must not be softened into a silent holdout by a lenient job posture."""
    _kept, rejected, abort = _write("quarantine", _signed_contract("FAIL_JOB"))
    assert abort and "abort policy blocks partial write" in abort
    assert rejected[0]["execution_policy"] == "FAIL_JOB"


def test_a_tampered_contract_cannot_grant_a_holdout() -> None:
    contract = dict(_signed_contract("FAIL_JOB"))
    contract["execution_policy"] = "QUARANTINE_ROW"  # body changed after signing
    _kept, _rejected, abort = _write("fail", contract)
    assert abort, "an unverifiable contract must fail closed"


#: One narrowing bounded carrier per destination family, with a value that cannot
#: fit it and two that can. The incoherence was never Snowflake-specific — every
#: typed writer runs the same matrix — so every family is asserted on the same
#: contract. The fitting pair is carrier-shaped: a fractional value is not an
#: integer, so an integer carrier's good rows are integral.
_CARRIERS: list[tuple[str, str, str, str, tuple[str, str]]] = [
    ("snowflake", "Snowflake", "NUMBER(11,8)", "12.123456789012", ("1.5", "2.25")),
    ("postgresql", "Postgres", "NUMERIC(6,2)", "123456.78", ("1.5", "2.25")),
    ("mysql", "MySQL", "DECIMAL(6,2)", "9999999.99", ("1.5", "2.25")),
    ("sqlserver", "SQL Server", "DECIMAL(5,2)", "12345.67", ("1.5", "2.25")),
    ("oracle", "Oracle", "NUMBER(6,2)", "1234567.89", ("1.5", "2.25")),
    ("bigquery", "BigQuery", "NUMERIC(6,2)", "1234567.89", ("1.5", "2.25")),
    ("redshift", "Redshift", "NUMERIC(6,2)", "1234567.89", ("1.5", "2.25")),
    ("postgresql", "Postgres", "VARCHAR(4)", "far-too-wide", ("1.5", "2.25")),
    ("mysql", "MySQL", "VARCHAR(4)", "far-too-wide", ("1.5", "2.25")),
    ("snowflake", "Snowflake", "VARCHAR(4)", "far-too-wide", ("1.5", "2.25")),
    ("postgresql", "Postgres", "SMALLINT", "99999", ("15", "225")),
    ("mysql", "MySQL", "TINYINT", "9999", ("15", "25")),
    ("sqlserver", "SQL Server", "SMALLINT", "99999", ("15", "225")),
]


@pytest.mark.parametrize(("dest_db", "label", "target_type", "bad", "good"), _CARRIERS)
def test_every_family_reads_the_same_contract(
    dest_db: str, label: str, target_type: str, bad: str, good: tuple[str, str]
) -> None:
    """One bad cell, one contract, one answer — per destination family."""
    contract = _signed_contract("QUARANTINE_ROW")
    contract["destination_type"] = target_type
    from services.migration_risk_contract import sign_risk_contract

    contract.pop("signature", None)
    contract["signature"] = sign_risk_contract(contract)
    mappings = _mappings(contract)
    mappings[1]["target_type"] = target_type
    rows: list[tuple[Any, ...]] = [(1, good[0]), (2, bad), (3, good[1])]

    def _run(policy: str, maps: list[dict[str, Any]] | None):
        rejected: list[dict[str, Any]] = []
        kept = apply_write_quarantine_matrix(
            list(rows),
            TARGET_COLS,
            ["NUMBER(9,0)" if dest_db in {"snowflake", "oracle"} else "INTEGER", target_type],
            rejected,
            policy,
            dialect_label=label,
            mappings=maps,
            dest_db=dest_db,
        )
        return kept, rejected, reject_on_strict_policy(policy, rejected, label)

    # Unsigned: the strict posture still refuses — the Validate gate blocks first.
    _kept, rejected, abort = _run("fail", _mappings(None))
    assert rejected, f"{label} {target_type} must catch {bad!r} before the driver"
    assert abort, f"{label} {target_type} must refuse an unsigned overflow"

    # Signed continue policy: the row is held out and the load proceeds.
    kept, rejected, abort = _run("fail", mappings)
    assert abort is None, f"{label} {target_type} aborted a contracted holdout: {abort}"
    assert [r[0] for r in kept] == [1, 3]
    assert rejected[0]["execution_policy"] == "QUARANTINE_ROW"


def test_a_write_names_its_map_for_bind_and_salvage_holdouts() -> None:
    """Writers that stamp holdouts outside the matrix inherit the same contract.

    ``active_quarantine_mappings`` is opened once around a write (adapters), so a
    bind refusal or SAVEPOINT salvage inside a connector resolves the column
    contract instead of deciding on the job posture alone.
    """
    from connectors.writer_common import (
        active_quarantine_mappings,
        append_write_quarantine_detail,
    )

    rejected: list[dict[str, Any]] = []
    with active_quarantine_mappings(_mappings(_signed_contract("QUARANTINE_ROW"))):
        append_write_quarantine_detail(
            rejected,
            {
                "row": 431,
                "column": "arr_time",
                "target": "arr_time",
                "value": "12.123456789012",
                "reason": "Snowflake bind refused the value",
                "policy": "write_quarantine",
            },
            mapped_row=(431, "12.123456789012"),
            target_cols=TARGET_COLS,
        )
    assert rejected[0]["execution_policy"] == "QUARANTINE_ROW"
    assert reject_on_strict_policy("fail", rejected, "Snowflake") is None


def test_the_named_map_does_not_outlive_the_write() -> None:
    from connectors.writer_common import (
        active_quarantine_mappings,
        append_write_quarantine_detail,
    )

    with active_quarantine_mappings(_mappings(_signed_contract("QUARANTINE_ROW"))):
        pass
    rejected: list[dict[str, Any]] = []
    append_write_quarantine_detail(
        rejected,
        {
            "row": 1,
            "column": "arr_time",
            "target": "arr_time",
            "value": "12.123456789012",
            "reason": "bind refused",
            "policy": "write_quarantine",
        },
        mapped_row=(1, "12.123456789012"),
        target_cols=TARGET_COLS,
    )
    assert not rejected[0].get("execution_policy")


def test_an_unmapped_column_keeps_the_job_posture() -> None:
    """A holdout with no mapping to consult is still decided by the job posture."""
    rejected: list[dict[str, Any]] = []
    apply_write_quarantine_matrix(
        list(ROWS),
        TARGET_COLS,
        TARGET_TYPES,
        rejected,
        "fail",
        dialect_label="Snowflake",
        dest_db="snowflake",
    )
    assert reject_on_strict_policy("fail", rejected, "Snowflake")
    assert not rejected[0].get("execution_policy")
