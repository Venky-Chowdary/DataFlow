"""Exit rehearsal end to end, in process: SQLite tables -> file exports on the real engine.

The runner starts one extract job per declared table on the existing
scheduler (``run_transfer_async``), then verifies, seals and times them.
Pins the plan's guarantees: "proven" only when rows, key sets, every value
and control totals match over the whole population; missing or changed rows
block it; a table the rehearsal could not re-read is "not proven", never
proven; a failed extract fails the rehearsal; durations come from the job
clocks; the Merkle root covers every part.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


def _db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC(12,2), note TEXT)")
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?)",
                         [(i, f"{i * 1.25:.2f}", f"order {i}" if i % 7 else None) for i in range(1, 61)])
        conn.execute("CREATE TABLE events (kind TEXT, qty INTEGER)")
        conn.executemany("INSERT INTO events VALUES (?, ?)", [("a", 1), ("b", 2), ("a", 1)])
    conn.close()


@pytest.fixture()
def world(tmp_path, monkeypatch):
    from services import exit_rehearsal, exit_store, schedule_runner

    monkeypatch.setenv("DATAWRAP_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.setattr(exit_store, "STORE_PATH", tmp_path / "exit.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    src = tmp_path / "ledger.sqlite"
    _db(src)
    conn = {"name": "ledger", "type": "sqlite", "database": str(src)}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _cid, _sched: dict(conn))
    # Settle explicitly (the completion callback would race the tamper tests).
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda _rid: None)
    audits: list[tuple[str, dict]] = []
    from services import exit_inventory

    real_audit = exit_inventory.audit
    monkeypatch.setattr(exit_rehearsal, "audit", lambda action, **kw: (audits.append((action, kw["details"])),
                                                                       real_audit(action, **kw)))
    return {"src": src, "audits": audits, "tmp": tmp_path}


def _system(**over):
    from services.exit_inventory import declare

    body = {"name": "Core ledger", "criticality": "critical", "source": {"connector_id": "c1"},
            "tables": [{"name": "orders"}, {"name": "events"}], "exit_window_hours": 720}
    body.update(over)
    return declare(body, workspace_id="ws1", actor="owner@example.com")


def _wait(rehearsal: dict, timeout: float = 120.0) -> None:
    from services.parity_state import job_store

    ids = [t["job_id"] for t in rehearsal["tables"] if t.get("job_id")]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = [job_store().get_job(j) or {} for j in ids]
        if all(str(j.get("status")) in {"completed", "failed", "cancelled"} for j in jobs):
            return
        time.sleep(0.2)
    raise AssertionError(f"extract jobs did not finish: {[j.get('status') for j in jobs]}")


def _run(system, *, tamper=None):
    from services.exit_rehearsal import public, settle, start

    started = start(system, actor="owner@example.com")
    assert started["verdict"] == "running"
    _wait(started)
    if tamper:
        tamper(started)
    return public(settle(started["id"]))


def _part_path(world, rehearsal, table):
    from services.parity_state import job_store

    job_id = next(t["job_id"] for t in rehearsal["tables"] if t["name"] == table)
    return Path(job_store().get_job(job_id)["destination_summary"]["path"])


def test_a_clean_rehearsal_is_proven_sealed_and_measured(world):
    from services.exit_completeness import merkle_root

    system = _system()
    out = _run(system)
    assert out["verdict"] == "proven", (out["verdict_reason"], [(t["name"], t["reason"]) for t in out["tables"]])
    tables = {t["name"]: t for t in out["tables"]}
    orders, events = tables["orders"], tables["events"]
    assert orders["rows_source"] == orders["rows_extracted"] == 60
    assert orders["key_columns"] == ["id"] and orders["key_set"]["match"] is True
    assert orders["row_set"]["match"] is True
    assert orders["value_parity"]["method"] == "population_digest"
    assert orders["value_parity"]["report"].endswith("/parity-report")
    totals = {c["column"]: c for c in orders["control_totals"]}
    assert totals["id"]["source"] == {"sum": "1830", "min": "1", "max": "60"} and totals["id"]["match"] is True
    assert Decimal(totals["amount"]["source"]["sum"]) == Decimal("2287.50") and totals["amount"]["match"]
    # A keyless table is compared by its row digest (duplicates included); no key-set check applies.
    assert events["status"] == "proven" and events["key_set"]["source_sha256"] is None
    assert events["rows_extracted"] == 3

    seal = out["seal"]
    assert seal["parts_count"] == 2 and seal["storage"] == "local_file"
    assert seal["merkle_root"] == merkle_root(seal["parts"])
    assert all(p["verified_on_read"] and len(p["sha256"]) == 64 for p in seal["parts"])
    assert seal["total_bytes"] == sum(p["size_bytes"] for p in seal["parts"]) > 0

    duration = out["duration"]
    assert duration["basis"] == "measured" and isinstance(duration["measured_seconds"], int)
    assert duration["window_seconds"] == 720 * 3600 and duration["fits_window"] is True
    assert "measured, not projected" in duration["statement"]
    assert [s["stage"] for s in out["timeline"]] == ["inventory", "extract", "verify", "seal", "sign"]
    assert {s["stage"]: s["status"] for s in out["timeline"]}["sign"] == "pending"
    assert out["completeness"]["tables_proven"] == 2
    assert out["signoff"]["status"] == "open"
    assert out["trend"]["points"][-1]["rehearsal_id"] == out["id"]
    assert out["trend"]["projection"] is None  # one rehearsal projects nothing
    actions = [a for a, _ in world["audits"]]
    assert actions == ["exit.rehearsal.started", "exit.rehearsal.completed"]
    assert world["audits"][-1][1]["merkle_root"] == seal["merkle_root"]

    # The system's readiness follows; a second rehearsal is numbered and trended.
    from services.exit_inventory import get_system, readiness

    ready = readiness(get_system(system["id"]))
    assert ready["state"] == "proven" and ready["last_proven_rehearsal"]["id"] == out["id"]
    second = _run(get_system(system["id"]))
    assert second["n"] == 2 and len(second["trend"]["points"]) == 2
    assert second["trend"]["growth_pct_rows"] == 0.0
    assert second["trend"]["projection"]["kind"] == "projection"


def test_missing_and_changed_rows_block_proven(world):
    system = _system(tables=[{"name": "orders"}])

    def drop_a_row(rehearsal):
        path = _part_path(world, rehearsal, "orders")
        lines = path.read_text().splitlines()
        path.write_text("\n".join(lines[:-1]) + "\n")

    out = _run(system, tamper=drop_a_row)
    table = out["tables"][0]
    assert out["verdict"] == "incomplete" and table["status"] == "incomplete"
    kinds = {d["kind"] for d in table["differences"]}
    assert "missing_rows" in kinds
    missing = next(d for d in table["differences"] if d["kind"] == "missing_rows")
    assert missing["rows"] == 1 and missing["rows_at_least"] is True
    assert out["signoff"]["status"] == "blocked" and "Only a proven rehearsal" in out["signoff"]["blocked_reason"]

    def change_a_value(rehearsal):
        path = _part_path(world, rehearsal, "orders")
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        rows[3]["note"] = "tampered"
        path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    changed = _run(_system(name="Copy", tables=[{"name": "orders"}]), tamper=change_a_value)
    diffs = changed["tables"][0]["differences"]
    assert changed["verdict"] == "incomplete"
    assert [d["kind"] for d in diffs] == ["changed_rows"] and diffs[0]["rows_at_least"] is True


def test_a_source_that_cannot_be_reread_is_not_proven(world, monkeypatch):
    from services import exit_completeness

    def unreadable(_endpoint, **_kw):
        raise RuntimeError("no SQL dialect for this source")

    monkeypatch.setattr(exit_completeness, "source_columns_and_rows", unreadable)
    out = _run(_system(tables=[{"name": "orders"}]))
    table = out["tables"][0]
    assert out["verdict"] == "not_proven" and table["status"] == "not_proven"
    assert "could not be re-read" in table["reason"]
    assert table["value_parity"]["method"] in {"sampled", "count_only", "none"}
    assert table["rows_source"] is None
    assert out["signoff"]["status"] == "blocked"


def test_a_failed_extract_fails_the_rehearsal(world):
    out = _run(_system(tables=[{"name": "orders"}, {"name": "no_such_table"}]))
    assert out["verdict"] == "failed"
    failed = next(t for t in out["tables"] if t["name"] == "no_such_table")
    assert failed["status"] == "failed" and failed["reason"]
    assert out["seal"]["parts_count"] == 1  # the extracted part is still sealed
    assert [a for a, _ in world["audits"]][-1] == "exit.rehearsal.failed"


def test_one_rehearsal_at_a_time(world):
    from services.exit_inventory import ExitConflict, get_system
    from services.exit_rehearsal import settle, start

    system = _system(tables=[{"name": "events"}])
    first = start(system, actor="a@example.com")
    from services.exit_rehearsal import public

    running = public(first)
    if running["verdict"] == "running":  # a running rehearsal has the full shape, nothing measured
        assert running["completeness"]["tables_declared"] == 1 and running["seal"]["merkle_root"] is None
        assert running["duration"]["measured_seconds"] is None and running["signoff"]["status"] == "blocked"
    with pytest.raises(ExitConflict):
        start(get_system(system["id"]), actor="b@example.com")
    _wait(first)
    settle(first["id"])
    assert get_system(system["id"])["running_rehearsal_id"] is None


@pytest.fixture()
def bucket(world, monkeypatch):
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    for name in ("DATAFLOW_S3_ENDPOINT", "DATAWRAP_S3_ENDPOINT", "AWS_ENDPOINT_URL",
                 "DATAWRAP_EXPORT_S3_ENDPOINT", "DATAFLOW_EXPORT_S3_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAWRAP_EXPORT_BUCKET", "exit-exports")
    monkeypatch.setenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", "off")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="exit-exports")
        yield s3


def test_extracts_in_the_export_bucket_are_verified_on_read(world, bucket):
    from services.exit_completeness import merkle_root

    out = _run(_system(tables=[{"name": "orders"}]))
    part = out["seal"]["parts"][0]
    assert out["seal"]["storage"] == "object_store" and part["storage"] == "object_store"
    assert part["object_key"].startswith("exports/ws1/") and part["verified_on_read"] is True
    body = bucket.get_object(Bucket="exit-exports", Key=part["object_key"])["Body"].read()
    import hashlib

    assert hashlib.sha256(body).hexdigest() == part["sha256"] and len(body) == part["size_bytes"]
    assert out["verdict"] == "proven", [t["reason"] for t in out["tables"]]
    assert out["seal"]["merkle_root"] == merkle_root([part])


def test_a_part_altered_in_the_bucket_breaks_the_seal(world, bucket):
    def overwrite(rehearsal):
        from services.parity_state import job_store

        job = job_store().get_job(rehearsal["tables"][0]["job_id"])
        key = job["destination_summary"]["object_key"]
        body = bucket.get_object(Bucket="exit-exports", Key=key)["Body"].read()
        bucket.put_object(Bucket="exit-exports", Key=key, Body=body.replace(b"order 1\"", b"order X\""))

    out = _run(_system(tables=[{"name": "orders"}]), tamper=overwrite)
    table = out["tables"][0]
    assert out["verdict"] == "incomplete"
    assert "seal" in {d["kind"] for d in table["differences"]}
    assert table["part"]["verified_on_read"] is False
