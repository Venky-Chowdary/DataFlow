"""A refused write unit names its rollbacks instead of inflating quarantine.

The MySQL ``ARR_TIME`` run showed "5,000 rows quarantined / 0 findings": the
writers count ``source - kept`` and an aborted batch keeps nothing, so every
uncommitted row was reported as a reject even though only half the rows carried
a bad cell — and the durable findings could not be hydrated at all because the
raw Mongo document identifies itself with ``_id``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
for path in (str(API_ROOT), str(API_ROOT / "src")):
    if path not in sys.path:
        sys.path.insert(0, path)

from services.quarantine_from_preflight import merge_job_quarantine  # noqa: E402
from src.transfer.adapters import (  # noqa: E402
    WriteBatchBlocked,
    raise_writer_failure,
)
from src.transfer.job_quarantine import (  # noqa: E402
    rows_with_findings,
    split_refused_unit,
)
from src.transfer.stream import _raise_write_failure  # noqa: E402


class _Result:
    """Minimal writer result: an aborted unit, half of it actually bad."""

    def __init__(self, rejected_rows: int, details: list[dict[str, Any]]) -> None:
        self.ok = False
        self.error = "MySQL rejected cell finding(s); strict error policy blocks write"
        self.rows_written = 0
        self.rejected_rows = rejected_rows
        self.rejected_details = details
        self.coerced_null_rows = 0
        self.rows_skipped = 0
        self.checksum = ""
        self.driver = "pymysql"
        self.table_name = "onw"
        self.target_schema = "railway"


def _details(n: int) -> list[dict[str, Any]]:
    return [
        {
            "row": i + 1,
            "column": "ARR_TIME",
            "value": "22.433332",
            "reason": "value does not fit MySQL INTEGER(INT) — quarantined",
            "policy": "write_quarantine",
        }
        for i in range(n)
    ]


def test_rows_with_findings_counts_distinct_source_rows() -> None:
    details = _details(3) + [{"row": 3, "column": "DEP_TIME", "value": "x"}]
    assert rows_with_findings(details) == 3
    assert rows_with_findings([{"column": "a"}]) == 0


def test_a_refused_unit_reports_findings_and_names_the_rollback() -> None:
    summary: dict[str, Any] = {"rejected_rows": 5000}
    assert split_refused_unit(_details(2500), 5000, summary) == 2500
    assert summary["rejected_rows"] == 2500
    assert summary["rows_rolled_back"] == 2500
    assert summary["rows_refused_unit"] == 5000


def test_a_unit_where_every_row_is_bad_keeps_its_total() -> None:
    summary: dict[str, Any] = {"rejected_rows": 2500}
    assert split_refused_unit(_details(2500), 2500, summary) == 2500
    assert "rows_rolled_back" not in summary
    assert "rows_refused_unit" not in summary


def test_a_unit_with_no_row_numbered_findings_is_left_alone() -> None:
    summary: dict[str, Any] = {"rejected_rows": 5000}
    details = [{"column": "ARR_TIME", "reason": "bad"}]
    assert split_refused_unit(details, 5000, summary) == 5000
    assert "rows_rolled_back" not in summary


@pytest.mark.parametrize("raise_fn", [raise_writer_failure, _raise_write_failure])
def test_both_writer_choke_points_agree_on_the_refused_unit(raise_fn) -> None:
    result = _Result(5000, _details(2500))
    with pytest.raises(WriteBatchBlocked) as exc:
        raise_fn(result, "MySQL batch write failed")
    blocked = exc.value
    assert blocked.rejected_rows == 2500
    assert len(blocked.rejected_details) == 2500
    assert blocked.dest_summary["rows_rolled_back"] == 2500
    assert blocked.dest_summary["rows_refused_unit"] == 5000


def test_the_write_time_refusal_names_the_remediation() -> None:
    """``Invalid integer: '22.433332'`` names the value but not the fix."""
    from services.transform_engine import apply_transform

    value, err = apply_transform("22.433332", "integer")
    assert value is None
    assert err is not None
    assert err.startswith("Invalid integer:")
    assert "fractional" in err
    assert "DECIMAL" in err

    _v, plain = apply_transform("not-a-number", "integer")
    assert plain == "Invalid integer: 'not-a-number'"


def test_a_running_checkpoint_carries_the_same_split() -> None:
    """The last checkpoint must not restore the refused-unit total on the job."""
    from src.transfer.job_quarantine import checkpoint_quarantine_summary

    details = _details(2500)
    checkpoint = {"checksum": "abc", "rejected_rows": 5000}
    summary = checkpoint_quarantine_summary(
        checkpoint, details, details[:40], len(details), True
    )
    assert summary["rejected_rows"] == 2500
    assert summary["rows_rolled_back"] == 2500
    assert summary["rows_refused_unit"] == 5000
    assert summary["rejected_details_total"] == 2500
    assert summary["quarantine_checkpoint_durable"] is True


def test_a_raw_mongo_job_hydrates_its_durable_findings(monkeypatch) -> None:
    """``_id`` is how a raw job document names itself — Inspect must follow it."""
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(
        dlq, "quarantine_details_from_dlq", lambda job_id: _details(2500)
    )
    job = {
        "_id": "6a8a0ac7-8b00",
        "rejected_details": _details(40),
        "rejected_details_total": 2500,
        "rejected_details_truncated": True,
        "destination_summary": {
            "rejected_rows": 2500,
            "rows_rolled_back": 2500,
            "rejected_details_total": 2500,
        },
    }
    merged = merge_job_quarantine(job)
    assert len(merged) == 2500


def test_a_small_run_keeps_the_offending_value_on_the_job() -> None:
    """Under the hydration threshold the job copy *is* the evidence.

    ``slim_rejected_detail`` whitelisted row/column/reason but not ``value``, so
    a run with a handful of findings showed a reason for a cell it could not
    name, and Export CSV had an empty value column.
    """
    from services.job_document_budget import trim_job_update_payload

    trimmed = trim_job_update_payload(
        {"rejected_details": _details(5), "rejected_rows": 5}
    )
    kept = trimmed["rejected_details"]
    assert len(kept) == 5
    assert [d.get("value") for d in kept] == ["22.433332"] * 5


@pytest.mark.parametrize(
    "dest_type",
    ["mysql", "postgresql", "oracle", "mssql", "sqlite", "redshift"],
)
def test_the_dlq_table_declares_a_carrier_the_dialect_accepts(dest_type: str) -> None:
    """A bare ``VARCHAR`` is a MySQL 1064 — the DLQ table could never be created."""
    from services.type_system import materialize_dest_ddl

    carrier = materialize_dest_ddl(dest_type, "string").strip()
    assert carrier
    assert carrier.upper() != "VARCHAR"


def test_an_unmeasured_read_is_not_an_unbalanced_ledger() -> None:
    """No source count means no equation to fail, so no loss may be implied."""
    from services.row_conservation import account_population

    ledger = account_population(
        rows_read=None,
        dest_count=0,
        dest_count_source="unmeasured",
        dest_count_before=None,
        rejected_rows=250,
        coerced_null_rows=0,
        rows_skipped=0,
        writer_ack=0,
        sync_mode="insert",
    )
    assert ledger.conservation_kind == "unmeasured"
    assert ledger.rows_read is None
    assert ledger.unaccounted is None
