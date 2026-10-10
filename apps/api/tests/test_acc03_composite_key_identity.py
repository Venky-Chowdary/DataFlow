"""QA ACC-03 — composite keys flagged as duplicates on every route.

A ``(region, id)`` key legitimately repeats ``id`` across regions. Validate's
G9 was made composite-aware on the branch, but Execute's per-batch audit
(``services.data_quality.run_integrity_audit``) still deduped the first key
component alone and hard-blocked every composite upsert in strict mode.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.data_integrity import run_integrity_audit as validate_g9_audit  # noqa: E402
from services.data_quality import run_integrity_audit as batch_audit  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

HEADERS = ["region", "id", "amount"]
ROWS = [[r, str(i), f"{i}.00"] for r in ("eu", "us", "ap") for i in range(1, 11)]


def test_batch_audit_composite_key_repeats_component_not_tuple():
    report = batch_audit(
        headers=HEADERS,
        rows=ROWS,
        mappings=[{"source": h, "target": h} for h in HEADERS],
        primary_key="region",
        primary_key_columns=["region", "id"],
        validation_mode="strict",
        dest_kind="postgresql",
        sync_mode="upsert",
    )
    assert report.passed, report.issues
    assert report.stats["primary_key_columns"] == ["region", "id"]


def test_batch_audit_still_blocks_a_real_composite_duplicate():
    report = batch_audit(
        headers=HEADERS,
        rows=ROWS + [["eu", "3", "9.99"]],
        mappings=[{"source": h, "target": h} for h in HEADERS],
        primary_key_columns=["region", "id"],
        validation_mode="strict",
        dest_kind="postgresql",
        sync_mode="upsert",
    )
    assert not report.passed
    assert "(eu, 3)" in " ".join(report.issues)


def test_batch_audit_absent_component_is_not_a_key():
    rows = [["eu", "1", "1"], [None, "1", "2"], [None, "1", "3"]]
    report = batch_audit(
        headers=HEADERS,
        rows=rows,
        mappings=[{"source": h, "target": h} for h in HEADERS],
        primary_key_columns=["region", "id"],
        validation_mode="strict",
        dest_kind="postgresql",
        sync_mode="upsert",
    )
    assert report.passed, report.issues


def test_validate_g9_composite_contract_passes():
    out = validate_g9_audit(
        mappings=[{"source": h, "target": h, "confidence": 1.0} for h in HEADERS],
        source_columns=HEADERS,
        target_columns=HEADERS,
        sample_rows=[dict(zip(HEADERS, r)) for r in ROWS],
        destination_db_type="postgresql",
        sync_mode="upsert",
        validation_mode="strict",
        stream_contracts=[{"name": "t", "primary_key": ["region", "id"], "selected": True}],
        destination_pk_columns=["region", "id"],
    )
    dup = next(c for c in out["checks"] if c["check"] == "duplicate_keys")
    assert dup["passed"], dup


def _sqlite(path: Path, rows) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE \"Orders_Mixed\" (region TEXT NOT NULL, id INTEGER NOT NULL, "
            "amount TEXT, PRIMARY KEY (region, id))"
        )
        conn.executemany('INSERT INTO "Orders_Mixed" VALUES (?, ?, ?)', rows)
    conn.close()


def test_composite_upsert_runs_twice_end_to_end_strict(tmp_path: Path, monkeypatch):
    """Two strict-mode upsert runs on a composite, mixed-case table (MX3-06 shape)."""
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sqlite(src, [tuple(r) for r in ROWS])

    def _req():
        return TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="Orders_Mixed"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="Orders_Mixed"),
            sync_mode="upsert",
            stream_contracts=[{
                "name": "Orders_Mixed", "primary_key": ["region", "id"],
                "sync_mode": "upsert", "selected": True,
            }],
            skip_preflight=True,
            validation_mode="strict",
        )

    for run in (1, 2):
        job_id = f"acc03r{run}" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(_req(), job_id)
        assert result.success, f"run {run}: {result.error}"
        assert int((result.destination_summary or {}).get("rejected_rows") or 0) == 0

    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute('SELECT count(*) FROM "Orders_Mixed"').fetchone()[0] == len(ROWS)
        assert conn.execute(
            'SELECT count(*) FROM (SELECT DISTINCT region, id FROM "Orders_Mixed")'
        ).fetchone()[0] == len(ROWS)
    finally:
        conn.close()
