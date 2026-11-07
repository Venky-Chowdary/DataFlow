"""Governance operations on the migration certificate (mask / hash / redact)."""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from services.governance_ops import (  # noqa: E402
    collect_governance_operations,
    harvest_governance_operations,
)
from services.migration_certificate import (  # noqa: E402
    build_migration_certificate,
    render_certificate_markdown,
)
from services.transform_engine import REDACTED_PLACEHOLDER, apply_transform  # noqa: E402


JOB_ID = "b" * 24

SSN = "123-45-6789"
EMAIL = "alice@example.com"
NAME = "Alice Patient"


def _job(**overrides: Any) -> dict[str, Any]:
    job: dict[str, Any] = {
        "id": JOB_ID,
        "status": "completed",
        "records_processed": 2,
        "sync_mode": "overwrite",
        "source": {"format": "sqlite"},
        "destination": {"format": "sqlite"},
        "reconciliation": {
            "passed": True,
            "phase": "post_write_verified",
            "assurance_level": "full_checksum",
            "checksum_match": True,
            "source_rows": 2,
            "target_rows": 2,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "message": "Verified",
        },
        "destination_summary": {"rejected_details": []},
    }
    job.update(overrides)
    return job


def test_harvest_lists_mask_and_hash_and_omits_identity() -> None:
    ledger = harvest_governance_operations(
        {
            "mappings": [
                {"source": "id", "target": "id", "transform": "none"},
                {"source": "ssn", "target": "tax_id", "transform": "mask_pii"},
                {"source": "email", "target": "email", "transform": "hash_pii"},
            ]
        }
    )
    assert ledger["count"] == 2
    ops = {(row["source"], row["operation"], row["transform"]) for row in ledger["applied"]}
    assert ops == {
        ("ssn", "mask", "mask_pii"),
        ("email", "hash", "hash_pii"),
    }
    assert all(row["reversible"] is False for row in ledger["applied"])
    assert all(row["write_path_applied"] is True for row in ledger["applied"])
    assert "original values" in ledger["note"]


def test_harvest_records_declared_alias_as_not_applied_on_write() -> None:
    ledger = harvest_governance_operations(
        {"mappings": [{"source": "secret", "target": "secret", "transform": "encrypt"}]}
    )
    assert ledger["count"] == 1
    row = ledger["applied"][0]
    assert row["operation"] == "redact"
    assert row["transform"] == "encrypt"
    assert row["write_path_applied"] is False


def test_harvest_from_transfer_request_when_job_mappings_stripped() -> None:
    ledger = harvest_governance_operations(
        {
            "mappings": [],
            "transfer_request": {
                "mappings": [
                    {"source": "ssn", "target": "ssn", "transform": "redact"},
                ]
            },
        }
    )
    assert ledger["count"] == 1
    assert ledger["applied"][0]["operation"] == "redact"
    assert ledger["applied"][0]["write_path_applied"] is True
    ledger = harvest_governance_operations({"mappings": []})
    assert ledger["count"] == 0
    assert ledger["applied"] == []
    assert "not proof" in ledger["note"].lower()


def test_stamp_wins_over_later_mapping_harvest() -> None:
    stamped = harvest_governance_operations(
        {"mappings": [{"source": "ssn", "target": "ssn", "transform": "mask_pii"}]}
    )
    job = {
        "governance_operations": stamped,
        "mappings": [{"source": "email", "target": "email", "transform": "hash_pii"}],
    }
    ledger = collect_governance_operations(job)
    assert ledger["count"] == 1
    assert ledger["applied"][0]["source"] == "ssn"


def test_certificate_includes_governance_and_markdown() -> None:
    job = _job(
        mappings=[
            {"source": "id", "target": "id", "transform": "identity"},
            {"source": "ssn", "target": "ssn", "transform": "mask_pii"},
            {"source": "email", "target": "email", "transform": "hash_pii"},
        ]
    )
    cert = build_migration_certificate(job)
    gov = cert["governance_operations"]
    assert gov["count"] == 2
    families = {row["operation"] for row in gov["applied"]}
    assert families == {"mask", "hash"}
    md = render_certificate_markdown(cert)
    assert "## Governance operations" in md
    assert "mask" in md
    assert "`ssn`" in md
    assert SSN not in md
    assert EMAIL not in md
    assert "source PII is unrecoverable" in md.lower() or "HMAC" in md


def test_certificate_without_governance_transforms_says_none_declared() -> None:
    cert = build_migration_certificate(
        _job(mappings=[{"source": "id", "target": "id", "transform": "none"}])
    )
    assert cert["governance_operations"]["count"] == 0
    md = render_certificate_markdown(cert)
    assert "No governance transform" in md


def test_redact_write_path_replaces_the_cell() -> None:
    val, err = apply_transform(NAME, "redact")
    assert err is None
    assert val == REDACTED_PLACEHOLDER
    empty, err2 = apply_transform("", "redact")
    assert err2 is None
    assert empty == REDACTED_PLACEHOLDER
    missing, err3 = apply_transform(None, "redact")
    assert err3 is None
    assert missing is None


class _FakeMongo:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs: Any) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = str(job_data.get("_id") or job_data.get("id") or uuid.uuid4().hex[:24])
        self.jobs[job_id] = {**job_data, "id": job_id}
        return job_id


def test_live_sqlite_mask_hash_redact_land_and_certificate_lists_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two-row sqlite→sqlite: originals absent on dest; certificate lists ops."""
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "governance-ops-live-key")
    mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mongo)

    src_path = tmp_path / "src.db"
    dst_path = tmp_path / "dst.db"
    conn = sqlite3.connect(src_path)
    conn.execute(
        "CREATE TABLE patients (id INTEGER PRIMARY KEY, ssn TEXT, email TEXT, name TEXT)"
    )
    conn.executemany(
        "INSERT INTO patients (id, ssn, email, name) VALUES (?, ?, ?, ?)",
        [
            (1, SSN, EMAIL, NAME),
            (2, "987-65-4321", "bob@example.com", "Bob Patient"),
        ],
    )
    conn.commit()
    conn.close()

    dest = sqlite3.connect(dst_path)
    dest.execute(
        "CREATE TABLE patients (id INTEGER PRIMARY KEY, ssn TEXT, email TEXT, name TEXT)"
    )
    dest.commit()
    dest.close()

    mappings = [
        {"source": "id", "target": "id", "transform": "none"},
        {"source": "ssn", "target": "ssn", "transform": "mask_pii"},
        {"source": "email", "target": "email", "transform": "hash_pii"},
        {"source": "name", "target": "name", "transform": "redact"},
    ]
    job_id = uuid.uuid4().hex[:24]
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src_path),
            table="patients",
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(dst_path),
            table="patients",
        ),
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    assert result.records_transferred == 2

    independent = sqlite3.connect(dst_path)
    rows = independent.execute(
        "SELECT id, ssn, email, name FROM patients ORDER BY id"
    ).fetchall()
    count = independent.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
    independent.close()
    assert count == 2
    assert len(rows) == 2
    for _id, ssn, email, name in rows:
        assert SSN not in (ssn or "")
        assert EMAIL not in (email or "")
        assert NAME not in (name or "")
        assert "987-65-4321" not in (ssn or "")
        assert "bob@example.com" not in (email or "")
        assert "Bob Patient" not in (name or "")
        assert "*" in (ssn or "")
        assert email and len(email) == 32
        assert name == REDACTED_PLACEHOLDER

    stamped = (mongo.get_job(job_id) or {}).get("governance_operations")
    assert isinstance(stamped, dict)
    assert stamped.get("count") == 3

    job = _job(
        id=job_id,
        mappings=mappings,
        governance_operations=stamped,
        reconciliation={
            **_job()["reconciliation"],
            **(result.reconciliation or {}),
            "source_rows": 2,
            "target_rows": 2,
            "rejected_rows": 0,
        },
        destination_summary=result.destination_summary or {},
        records_processed=result.records_transferred,
    )
    cert = build_migration_certificate(job)
    families = {row["operation"] for row in cert["governance_operations"]["applied"]}
    assert families == {"mask", "hash", "redact"}
    assert cert["governance_operations"]["count"] == 3
    md = render_certificate_markdown(cert)
    assert "## Governance operations" in md
    assert SSN not in md
    assert EMAIL not in md
    assert NAME not in md
    blob = str(result.destination_summary) + str(result.explanation)
    assert SSN not in blob
    assert EMAIL not in blob
