"""Job-level Gate-8 on a sequential multi-table run is the last table's digest.

The last table can match. That match is not proof of the other tables, and it
must not earn full_checksum or migration_proven for the job.
"""

from __future__ import annotations

from services.decision_kernel.proof import build_migration_proof_pack
from services.job_trust import compute_job_trust, has_full_checksum_proof
from services.reconcile_coverage import (
    LAST_STREAM_CHECKSUM,
    qualify_multi_stream_reconciliation,
)
from services.signed_proof_pack import classify_post_write_assurance


def _two_stream_summary(*, orders_balanced: bool = True) -> dict:
    return {
        "multi_stream": True,
        "table": "orders",
        "streams": [
            {
                "name": "customers",
                "row_accounting": {"dest_count": 2, "balanced": True},
            },
            {
                "name": "orders",
                "row_accounting": {"dest_count": 2, "balanced": orders_balanced},
            },
        ],
    }


def _last_table_full_checksum() -> dict:
    return {
        "passed": True,
        "assurance_level": "full_checksum",
        "coverage": "full_checksum",
        "source_rows": 2,
        "target_rows": 2,
        "source_checksum": "abc",
        "target_checksum": "abc",
        "checksum_match": True,
        "source_checksum_provenance": "independent_source_reread",
        "migration_proven": True,
        "message": "Row fidelity verified — source and target checksums match (2 rows)",
    }


def test_last_table_checksum_is_not_the_job_digest() -> None:
    out = qualify_multi_stream_reconciliation(
        _last_table_full_checksum(),
        _two_stream_summary(),
    )
    assert out["passed"] is True
    assert out["source_rows"] == 2
    assert out["checksum_scope"] == LAST_STREAM_CHECKSUM
    assert out["assurance_level"] == "per_stream_checksum"
    assert out["coverage"] == "per_stream_checksum"
    assert out["migration_proven"] is False
    assert out["population_proof"] is False
    assert out["job_dest_count"] == 4
    assert "orders" in out["message"]
    assert "4" in out["message"]
    assert "not the whole job" in out["message"]
    assert "Row fidelity verified" not in out["message"]
    assert not has_full_checksum_proof(out)
    # Matching hashes plus a leftover full_checksum label still must not pass.
    assert not has_full_checksum_proof({**_last_table_full_checksum(), "checksum_scope": LAST_STREAM_CHECKSUM})


def test_unbalanced_stream_ledger_fails_the_job_report() -> None:
    out = qualify_multi_stream_reconciliation(
        _last_table_full_checksum(),
        _two_stream_summary(orders_balanced=False),
    )
    assert out["passed"] is False
    assert out["checksum_scope"] == LAST_STREAM_CHECKSUM
    assert "orders" in out["message"]
    assert "unbalanced" in out["message"].lower()


def test_failed_last_stream_checksum_stays_failed() -> None:
    report = _last_table_full_checksum()
    report["passed"] = False
    report["message"] = "Checksum mismatch"
    out = qualify_multi_stream_reconciliation(report, _two_stream_summary())
    assert out["passed"] is False
    assert "Checksum mismatch" in out["message"]
    assert "orders" in out["message"]
    assert out["assurance_level"] == "per_stream_checksum"


def test_single_table_full_checksum_is_unchanged() -> None:
    report = _last_table_full_checksum()
    assert qualify_multi_stream_reconciliation(report, {"multi_stream": False}) is report
    assert qualify_multi_stream_reconciliation(
        report,
        {"multi_stream": True, "streams": [{"name": "orders"}]},
    ) is report


def test_narrow_scope_is_not_replaced() -> None:
    report = {
        "passed": True,
        "checksum_scope": "whole_table_not_comparable",
        "assurance_level": "row_count",
        "coverage": "row_count",
        "message": "Append delta verified",
    }
    assert qualify_multi_stream_reconciliation(report, _two_stream_summary()) is report


def test_writer_ack_keeps_its_message_and_loses_job_proof() -> None:
    report = {
        "passed": True,
        "assurance_level": "writer_ack",
        "coverage": "writer_ack",
        "source_checksum": "abc",
        "target_checksum": "abc",
        "message": "Writer acknowledgment for 2 row(s)",
    }
    out = qualify_multi_stream_reconciliation(report, _two_stream_summary())
    assert out["checksum_scope"] == LAST_STREAM_CHECKSUM
    assert out["message"] == report["message"]
    assert out["assurance_level"] == "writer_ack"
    assert not has_full_checksum_proof(out)


def test_bool_dest_count_is_not_a_population() -> None:
    summary = _two_stream_summary()
    summary["streams"][0]["row_accounting"]["dest_count"] = True
    summary["streams"][1]["row_accounting"]["dest_count"] = True
    out = qualify_multi_stream_reconciliation(_last_table_full_checksum(), summary)
    assert "job_dest_count" not in out
    assert "not counted" in out["message"]


def test_proof_and_trust_refuse_last_stream_as_migration_proven() -> None:
    recon = qualify_multi_stream_reconciliation(
        _last_table_full_checksum(),
        _two_stream_summary(),
    )
    assurance = classify_post_write_assurance(recon)
    assert assurance["migration_proven"] is False
    assert assurance["claim_level"] == "per_stream_checksum"

    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 4,
        "rejected_rows": 0,
        "reconciliation": recon,
    })
    assert trust["grade"] != "A"
    assert trust["score"] <= 89
    factor = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert factor["score"] == 70
    assert "last stream" in factor["note"].lower()
    assert trust["next_action"]["code"] == "last_stream_checksum"

    pack = build_migration_proof_pack(
        decision_artifact={"content_hash": "a" * 64, "ddl": {"ddl_identity_hash": "b" * 64}},
        reconciliation={
            **recon,
            "final_checksum": "c" * 64,
            "matched": True,
            "identity_hash_aligned": True,
        },
        validation_summary={"decision_artifact_present": True, "blocked_classes": []},
        connector_versions={
            "source": "postgresql 16.2",
            "destination": "postgresql 16.2",
        },
        job_success=True,
    )
    assert pack["migration_proven"] is False
    assert "checksum_scope_last_stream" in pack["assurance"]["reasons"]
