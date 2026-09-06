"""A signed document must verify as the bytes the operator actually downloads.

The pack is signed in the API process and verified again after a full HTTP
round trip, so anything the response serializer rewrites on the way out breaks
the signature. That is not a theoretical asymmetry: a control total is a
``Decimal`` and a measurement stamp is a ``datetime``, and both were rewritten,
so every freshly exported pack failed the product's own verify control.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from fastapi.encoders import jsonable_encoder

from services.field_reduction_ledger import sign_field_reduction_ledger
from services.migration_certificate import build_migration_certificate, verify_migration_certificate
from services.signed_proof_pack import (
    build_signed_proof_pack,
    export_proof_pack_for_job,
    verify_signed_proof_pack,
)


def over_the_wire(payload: dict) -> dict:
    """What the client parses: the response serializer's output, re-parsed."""
    return json.loads(json.dumps(jsonable_encoder(payload)))


def proven_reconciliation() -> dict:
    return {
        "phase": "post_write",
        "coverage": "full_checksum",
        "passed": True,
        "source_rows": 6,
        "target_rows": 6,
        "source_checksum": "c0ffee",
        "target_checksum": "c0ffee",
        "measured_at": datetime(2026, 9, 6, 12, 30, tzinfo=timezone.utc),
        "control_totals": {
            "declared": True,
            "evidence": "exact",
            "columns": [
                {
                    "source": "amount",
                    "target": "amount",
                    "source_sum": Decimal("618.75"),
                    "dest_sum": Decimal("618.75"),
                    "matched": True,
                    "proven": True,
                }
            ],
        },
    }


def test_pack_with_decimal_and_datetime_verifies_after_the_wire():
    pack = build_signed_proof_pack(
        job_id="job-wire-1",
        reconciliation=proven_reconciliation(),
        connector_versions={"source": "postgresql 16.2", "destination": "mysql 8.4.2"},
        ddl_hash="ddl-1",
        mapping_hash="map-1",
        job_success=True,
        anchor_in_chain=True,
    )
    assert verify_signed_proof_pack(pack)["ok"] is True

    shipped = over_the_wire(pack)
    result = verify_signed_proof_pack(shipped)
    assert result["ok"] is True, result["errors"]
    assert result["content_sha256"] == pack["content_sha256"]


def test_anchor_digest_survives_the_wire_and_still_finds_its_record():
    """The chain says "a pack with this digest was sealed here" — after the wire too."""
    from services.evidence_chain import find_anchor
    from services.signed_proof_pack import pack_body_digest_excluding_anchor

    pack = build_signed_proof_pack(
        job_id="job-wire-2",
        reconciliation=proven_reconciliation(),
        job_success=True,
        anchor_in_chain=True,
    )
    anchor = pack["chain_anchor"]
    assert anchor["anchored"] is True

    shipped = over_the_wire(pack)
    digest = pack_body_digest_excluding_anchor(shipped)
    assert digest == anchor["evidence_sha256"]
    assert find_anchor(digest) is not None


def test_money_crosses_the_wire_as_exact_text_not_binary64():
    """0.1 + 0.2 is why a control total may never become a JSON number."""
    recon = proven_reconciliation()
    recon["control_totals"]["columns"][0]["source_sum"] = Decimal("1234567890123.45")
    recon["control_totals"]["columns"][0]["dest_sum"] = Decimal("1234567890123.45")
    pack = build_signed_proof_pack(
        job_id="job-wire-3", reconciliation=recon, job_success=True
    )
    column = over_the_wire(pack)["gate8"]["control_totals"]["columns"][0]
    assert column["source_sum"] == "1234567890123.45"
    assert column["dest_sum"] == "1234567890123.45"
    assert not isinstance(column["source_sum"], float)


def test_tampering_after_the_wire_is_still_caught():
    """Normalizing before signing must not soften what a signature is for."""
    pack = over_the_wire(
        build_signed_proof_pack(
            job_id="job-wire-4",
            reconciliation=proven_reconciliation(),
            job_success=True,
        )
    )
    pack["gate8"]["control_totals"]["columns"][0]["dest_sum"] = "618.76"
    result = verify_signed_proof_pack(pack)
    assert result["ok"] is False
    assert "content_sha256 mismatch" in result["errors"]


def test_exported_job_pack_verifies_after_the_wire():
    job = {
        "_id": "job-wire-5",
        "status": "completed",
        "reconciliation": proven_reconciliation(),
        "mapping_proof": {"mapping_hash": "map-5", "mappings": []},
        "connector_versions": {"source": "postgresql 16.2"},
        "preflight": {"passed": True, "decision": "allow", "total_gates": 9},
    }
    pack = export_proof_pack_for_job(job, actor="ops@example.com")
    assert verify_signed_proof_pack(over_the_wire(pack))["ok"] is True


def test_migration_certificate_verifies_after_the_wire():
    job = {
        "_id": "job-wire-6",
        "id": "job-wire-6",
        "status": "completed",
        "reconciliation": proven_reconciliation(),
        "rows_written": 6,
        "started_at": datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc),
        "completed_at": datetime(2026, 9, 6, 12, 30, tzinfo=timezone.utc),
    }
    cert = build_migration_certificate(job, actor="ops@example.com")
    assert verify_migration_certificate(over_the_wire(cert))["ok"] is True


def test_signed_field_reduction_ledger_verifies_after_the_wire():
    from services.field_reduction_ledger import (
        LEDGER_SCHEMA,
        verify_field_reduction_ledger,
    )

    ledger = sign_field_reduction_ledger(
        {
            "schema": LEDGER_SCHEMA,
            "job_id": "job-wire-7",
            "signed_at": datetime(2026, 9, 6, 12, 30, tzinfo=timezone.utc),
            "fields": [
                {
                    "source": "legacy_fee",
                    "disposition": "deliberately_dropped",
                    "reason": "superseded_by_fee_schedule",
                    "approver": "cfo@example.com",
                }
            ],
        },
        job_id="job-wire-7",
    )
    shipped = over_the_wire(ledger)
    assert verify_field_reduction_ledger(shipped, job_id="job-wire-7")["ok"] is True
