"""D255: a gate-level preflight refusal holds out no source rows."""

from __future__ import annotations

from typing import Any

from src.transfer.job_failure import _fail_job_preflight


class _Mongo:
    def __init__(self) -> None:
        self.fields: dict[str, Any] = {}

    def update_job_status(self, job_id: str, status: str, **fields: Any) -> None:
        self.fields = {"status": status, **fields}

    def get_job(self, job_id: str) -> dict[str, Any]:
        return {}


class _Lineage:
    def emit_preflight_completed(self, **_: Any) -> None:
        pass

    def emit_run_failed(self, **_: Any) -> None:
        pass


def _fail(pf: dict[str, Any]) -> dict[str, Any]:
    mongo = _Mongo()
    _fail_job_preflight(mongo, "job-d255", pf, lineage=_Lineage(), rows_read=0)
    return mongo.fields


def test_unmapped_columns_refusal_reports_zero_rejected_rows():
    message = (
        "2 source column(s) are neither mapped nor declared omitted: payload_x, "
        "payload_n — Datawrap will not drop them silently."
    )
    fields = _fail(
        {
            "passed": False,
            "gates": [{"id": "g13_source_coverage", "status": "block", "message": message}],
            "blockers": [{"id": "g13_source_coverage", "message": message}],
        }
    )
    assert fields["status"] == "failed"
    assert fields["error"]
    assert fields["rejected_rows"] == 0
    assert fields["error_details"]["quarantine_row_count"] == 0
    assert fields["row_accounting"]["rows_quarantined"] == 0


def test_cell_findings_still_count_their_rows():
    fields = _fail(
        {
            "passed": False,
            "gates": [],
            "blockers": [
                {
                    "id": "g8_reconciliation",
                    "message": "Dry-run reconciliation failed",
                    "details": {
                        "issues": [
                            "row 1 phone→phone: Empty value cannot coerce to integer",
                            "row 4 phone→phone: Empty value cannot coerce to integer",
                        ]
                    },
                }
            ],
        }
    )
    assert fields["rejected_rows"] == 2
    assert fields["error_details"]["quarantine_row_count"] == 2
