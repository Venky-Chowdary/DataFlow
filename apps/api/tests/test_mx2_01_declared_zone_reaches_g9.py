"""MX2-01 — ``source_timezone`` must reach every gate, not only G3.

QA: upload sample_schema_types.csv -> SQLite. Without a zone, G3 correctly asks
for one. With ``source_timezone=UTC`` the run was still refused:
``rc-fidelity-collapse … impacts 3 gate check(s)`` because G9
(``_check_coercion_safety``) read the introspected ``updated_epoch_ms
(TIMESTAMP) → (TIMESTAMPTZ)`` while G3 read the declared type through
``declared_source_column_types``. G9 now projects the same declaration.
"""

from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import tests.test_mcp_sqlite_file_transfer as _mcp_harness  # noqa: E402
from services.data_integrity import _check_coercion_safety  # noqa: E402
from tests.test_mcp_sqlite_file_transfer import _call, _mcp, _wait_job  # noqa: E402

mcp_client = _mcp_harness.mcp_client


def _epoch_mapping(transform: str) -> list[dict]:
    return [{
        "source": "updated_epoch_ms", "target": "updated_epoch_ms", "confidence": 1.0,
        "source_type": "TIMESTAMP", "target_type": "TIMESTAMPTZ", "create_new": True,
        "transform": transform,
    }]


def test_g9_reads_the_declared_zone_like_g3():
    out = _check_coercion_safety(
        _epoch_mapping("assume_timezone:UTC"),
        {"updated_epoch_ms": "TIMESTAMP"},
        {"updated_epoch_ms": "TIMESTAMPTZ"},
        dest_kind="sqlite", validation_mode="balanced", dest_table_exists=False,
    )
    assert out["blocks_transfer"] is False, out["issues"]


def test_g9_still_blocks_a_zoneless_ntz_to_tz_without_a_declaration():
    out = _check_coercion_safety(
        _epoch_mapping(""),
        {"updated_epoch_ms": "TIMESTAMP"},
        {"updated_epoch_ms": "TIMESTAMPTZ"},
        dest_kind="sqlite", validation_mode="balanced", dest_table_exists=False,
    )
    assert out["blocks_transfer"] is True


def _stage(client, db_path, tmp_path, *, keep: tuple[str, ...] = ()):
    fixture = Path(_mcp_harness.__file__).parent / "fixtures" / "sample_schema_types.csv"
    name = "sample_schema_types" if not keep else "schema_types_instants"
    with fixture.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    fields = [c for c in rows[0] if not keep or c in keep]
    with (tmp_path / "uploads" / f"{name}.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    staged = _mcp(client, "create_connector", {
        "name": "qe-mx201", "type": "sqlite", "database": str(db_path), "test_first": True,
    })
    assert _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})["ok"]
    args = {
        "dataset_name": name, "dest_connector_name": "qe-mx201",
        "dest_table": "QA_E2E_RT_schema_types", "sync_mode": "full_refresh_overwrite",
    }
    return args, len(rows)


def test_qa_fixture_with_source_timezone_is_no_longer_a_fidelity_collapse(mcp_client, tmp_path):
    client, db_path = mcp_client
    args, _ = _stage(client, db_path, tmp_path)
    failed, refused = _call(client, "start_dataset_transfer", dict(args))
    assert failed and "source_timezone" in str(refused)  # no zone: asks, never guesses
    failed, out = _call(client, "start_dataset_transfer", {**args, "source_timezone": "UTC"})
    # customer_email still stops at PII review (T5, separate defect) — but the
    # zone the operator declared is no longer refused as a fidelity collapse.
    text = str(out)
    assert "fidelity-collapse" not in text and "updated_epoch_ms" not in text, text
    assert "TIMESTAMPTZ" not in text, text


def test_upload_with_source_timezone_lands_every_row_as_an_instant(mcp_client, tmp_path):
    client, db_path = mcp_client
    # Only non-PII columns: birth_date / epoch digits trip the PII heuristics,
    # and PII review has no acknowledge path on this tool (T5, group B).
    args, expected = _stage(client, db_path, tmp_path, keep=("row_id", "amount", "created_at"))
    transfer = _mcp(client, "start_dataset_transfer", {**args, "source_timezone": "UTC"})
    assert transfer.get("requires_confirm") is True, transfer
    confirmed = _mcp(client, "confirm_action", {"ack_id": transfer["ack_id"], "reason": "qe"})
    job = _wait_job(confirmed["job_id"])
    assert job.get("status") == "completed", job.get("error")
    with sqlite3.connect(db_path) as conn:
        got = {r[0]: (r[1], r[2]) for r in conn.execute(
            'SELECT "row_id", "created_at", typeof("created_at") FROM "QA_E2E_RT_schema_types"'
        )}
    assert len(got) == expected
    assert got[1] == ("2024-01-15T10:30:00+00:00", "text")
    assert got[3][0] == "2024-03-10T08:15:00+00:00"  # already-offset value kept


def test_population_fit_screens_with_the_declared_zone_transform():
    """g3f forecast '4 row(s) will be held out … created_at → TIMESTAMPTZ'
    by screening raw wall-clock values; the write applies the zone first."""
    from services.population_fit_scan import _scannable_transform

    mapping = {
        "source": "created_at", "target": "created_at", "source_type": "TIMESTAMP",
        "target_type": "TIMESTAMPTZ", "transform": "assume_timezone:Asia/Kolkata",
    }
    got = _scannable_transform(
        mapping, source_types={"created_at": "TIMESTAMP"}, dest_types={"created_at": "TIMESTAMPTZ"},
    )
    assert got == "assume_timezone:Asia/Kolkata"  # IANA case kept for ZoneInfo
