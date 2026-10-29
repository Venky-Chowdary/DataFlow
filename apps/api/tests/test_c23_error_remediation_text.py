"""C23: error and remediation text names the true cause.

- X-44: a failed tamper leads with the localized culprit, not "Row count
  verified …" — apply_fidelity_veto must PREPEND loc to base.
- X-131: a binary column's JSON NULL read-back renders "NULL", not the
  base64-decode of the literal "null" (0x9ee965).
- "Fix bad data" is offered only for row-level value failures, never for
  dropped tables, schema drift, pagination or array/struct gaps.
- A job failed at Validate shows the failure reason in `message`, not the
  stale "Validating mapping and schema…" progress copy.
- SFTP/file tamper leads with read-back wording, not transform-proof text.
- generic_sql destination: "not a supported destination", not
  "probe not implemented".
- cast_integer on '3O7' headlines the declared transform, not the carrier.
"""

import pytest


# ── X-44: loc leads the failed-tamper message ──────────────────────────────

def test_veto_prepends_localized_culprit_before_verified_text():
    from services.signed_proof_pack import apply_fidelity_veto

    recon = {
        "passed": True,
        "message": "Row count verified by engine copy: 60 row(s)",
        "verification_ladder": {
            "passed": False,
            "localization_summary": (
                "L4/L5 localized: row 'id'=10 column 'amt' "
                "source='10.25' target='999.99'"
            ),
            "layers": {"L4": {"passed": False}},
        },
    }
    out = apply_fidelity_veto(dict(recon))
    assert out["passed"] is False
    # The culprit leads; the verified fact trails as context.
    assert out["message"].startswith("L4/L5 localized"), out["message"]
    assert "Row count verified" in out["message"]


# ── X-131: NULL literal on a binary column renders NULL ────────────────────

def test_binary_cell_json_null_literal_renders_null():
    from services.value_serializer import operator_binary_cell_text

    # "null" is valid base64 → \x9e\xe9\x65; it used to display 0x9ee965.
    assert operator_binary_cell_text("null", "BYTEA") == "NULL"
    assert operator_binary_cell_text("  null  ", "VARBINARY") == "NULL"
    # Literal "null" was the bug; "__SQLNULL__"-style sentinels too.
    from services.value_serializer import SQL_NULL_SENTINEL
    assert operator_binary_cell_text(SQL_NULL_SENTINEL, "BYTEA") == "NULL"
    # Real base64 stays hex-rendered.
    assert operator_binary_cell_text("aGVsbG8=", "BYTEA") != "NULL"


# ── Fix-bad-data CTA filtered by failure class ──────────────────────────────

def _cta_kinds(job):
    from src.ai.copilot.tools import DataPilotTools
    return DataPilotTools._offers_bad_data_fix(
        job,
        job.get("_quarantine") or [],
        int(job.get("rejected_rows") or 0),
    )


def test_fix_bad_data_offered_on_value_failures():
    assert _cta_kinds(
        {"error": "Invalid integer: '3O7' in column code"}
    ) is True
    assert _cta_kinds({"rejected_rows": 3}) is True
    assert _cta_kinds({"_quarantine": [{"row": 1}]}) is True


@pytest.mark.parametrize(
    "err",
    [
        "Destination table hq0800_up2 does not exist (dropped since last run)",
        "schema_drift: destination has unaccounted columns",
        "REST pagination stopped at page 3 of 9 — next page token missing",
        "source column tags is an array; destination carrier cannot hold it",
        "authentication failed for user etl_user",
        "could not translate host name pg.example.internal",
        "'genericsql' is not a supported destination",
    ],
)
def test_fix_bad_data_not_offered_on_structural_failures(err):
    assert _cta_kinds({"error": err}) is False


# ── Failed-at-Validate message names the reason ────────────────────────────

def test_failed_job_message_carries_error_not_progress_text():
    from services.mongodb_service import _terminal_message_over_progress

    fields = {"error": "Validate gate g9 blocked: dest DDL incompatible"}
    _terminal_message_over_progress("failed", fields)
    assert fields["message"] == fields["error"]

    # An explicit caller message is not clobbered.
    fields = {"error": "e", "message": "custom"}
    _terminal_message_over_progress("failed", fields)
    assert fields["message"] == "custom"

    # Non-failure statuses are untouched.
    fields = {"error": "e"}
    _terminal_message_over_progress("completed", fields)
    assert "message" not in fields


def test_memory_store_failed_update_stamps_message():
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService()
    svc.create_transfer_job(
        {"_id": "j1", "status": "running", "message": "Validating mapping and schema…"}
    )
    svc.update_job_status("j1", "failed", error="gate g9 blocked", phase="failed")
    rec = svc.get_job("j1")
    assert rec["error"] == "gate g9 blocked"
    assert "g9" in rec["message"]
    assert "Validating" not in rec["message"]


# ── generic_sql destination names the real gap ──────────────────────────────

def test_genericsql_dest_says_not_supported():
    from services.destination_privilege_probe import probe_destination_privileges

    res = probe_destination_privileges(
        db_type="generic_sql",
        host="db.internal",
        port=5432,
        username="u",
        password="p",
        database="d",
        table="t",
    )
    assert res.status == "unavailable"
    assert "not a supported destination" in res.detail
    assert "probe not implemented" not in res.detail


# ── cast_integer: headline names the transform, not the carrier ─────────────

def test_transform_failure_headline_names_declared_transform():
    from services.population_fit_scan import (
        BoundedTarget,
        CARRIER_TYPED,
        build_population_fit_gate,
        scan_rows,
    )

    target = BoundedTarget(
        source="code",
        target="code",
        target_type="TEXT",
        carrier=CARRIER_TYPED,
        transform="cast_integer",
    )
    report = scan_rows(
        [{"code": "1"}, {"code": "3O7"}],
        [target],
        rows_total=2,
        rows_are_population=True,
    )
    gate = build_population_fit_gate(report)
    assert gate["status"] == "block"
    assert "cast_integer" in gate["message"]
    assert "3O7" in gate["message"] or "Invalid integer" in gate["message"]
    assert "cannot fit the destination carrier" not in gate["message"]


# ── SFTP/file tamper leads with read-back wording ───────────────────────────

def test_file_tamper_message_not_transform_proof_wording():
    import inspect

    src = inspect.getsource(__import__("src.transfer.reconcile_step", fromlist=["x"]))
    # The failed-proof branch must check artifact_cell_compare before writing
    # transform-proof wording as the headline.
    idx_failed = src.find('proof.get("status") == "failed"')
    idx_head = src.find("Read-back verification failed: the written file changed")
    assert 0 < idx_failed < idx_head
