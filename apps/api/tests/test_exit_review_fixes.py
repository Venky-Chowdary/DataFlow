"""Exit rehearsal: regressions from the adversarial review of the Reversibility Proof.

Each test names the finding it pins (C4 population digests that folded real
differences, H1 row values in reasons, H2 long verifications, ...).
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.exit_completeness import judge_table, profile_rows  # noqa: E402
from tests.test_exit_rehearsal_e2e import _part_path, _run, _system, world  # noqa: E402,F401

PARITY = {"check": "count_only", "verdict": "unverifiable", "unexplained": 0}


def _digest(rows, types, engine="postgresql", columns=None):
    columns = columns or list(types)
    return profile_rows(rows, columns, [], [], types=types, engine=engine)


# ------------------------------- C4: the population digest must not fold real differences


@pytest.mark.parametrize("before,after", [
    ("007", "7"), ("  padded  ", "padded"), ("abc\n", "abc"), ("abc ", "abc"),
    ("true", "1"), ("TRUE", "true"), ("t", "1"), ("1.50", "1.5"), ("$5", "5"), ("-0", "0"), ("+5", "5"),
    ("2024-01-01T10:00:00+02:00", "2024-01-01T08:00:00Z"), ("2024-01-01 10:00:00", "2024-01-01T10:00:00"),
    ("2024-01-01", "2024-01-01T00:00:00"), ("10:00:00.500", "10:00:00.5"),
    ('{"b":1, "a":2}', '{"a":2,"b":1}'), ('{"a":1,"a":2}', '{"a":2}'), ('{"a":1.10}', '{"a":1.1}'),
    ("__DF_SQL_NULL__", None), ("", None),
])
@pytest.mark.parametrize("ddl", ["TEXT", "VARCHAR(40)"])
def test_c4_a_text_column_is_compared_exactly(before, after, ddl):
    types = {"c": ddl}
    assert _digest([{"c": before}], types).row_set_sha256 != _digest([{"c": after}], types).row_set_sha256


@pytest.mark.parametrize("ddl,source,extract", [
    ("BIGINT", 7, "7"),
    ("DECIMAL(12,4)", Decimal("1.5000"), "1.5000"),
    ("DECIMAL(12,4)", Decimal("1.5000"), Decimal("1.5000")),
    ("BOOLEAN", True, True),
    ("TIMESTAMPTZ", datetime(2026, 1, 1, 8, tzinfo=timezone.utc), "2026-01-01 08:00:00+00:00"),
    ("TIMESTAMP_NTZ", datetime(2026, 1, 1, 10, 0, 0, 500000), "2026-01-01 10:00:00.500000"),  # noqa: DTZ001
    ("DATE", date(2024, 1, 1), "2024-01-01"),
    ("DOUBLE PRECISION", 0.1, 0.1),
    ("BINARY", b"\x00\xff", "AP8="),
    ("UUID", uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    ("JSONB", {"b": 1, "a": 3}, {"b": 1, "a": 3}),
    ("TEXT", "  spaced  ", "  spaced  "),
])
def test_c4_a_typed_value_and_its_extract_rendering_still_match(ddl, source, extract):
    types = {"c": ddl}
    assert _digest([{"c": source}], types).row_set_sha256 == _digest([{"c": extract}], types).row_set_sha256


def test_c4_a_json_column_and_an_undeclared_column_are_named_as_not_compared():
    prof = _digest([{"id": 1, "doc": {"a": 1}, "x": 5}], {"id": "BIGINT", "doc": "JSON"},
                   columns=["id", "doc", "x"])
    assert set(prof.uncompared) == {"doc", "x"}
    assert "json" in prof.uncompared["doc"] and "declared type" in prof.uncompared["x"]
    # jsonb (and MySQL JSON) are compared as documents, by their own equality.
    assert not _digest([{"d": {"a": 1}}], {"d": "JSONB"}).uncompared
    assert not _digest([{"d": {"a": 1}}], {"d": "JSON"}, engine="mysql").uncompared


def test_c4_a_table_with_an_uncompared_column_is_not_proven():
    types = {"id": "BIGINT", "doc": "JSON"}
    src = profile_rows([{"id": 1, "doc": {"a": 1}}], ["id", "doc"], ["id"], None, types=types, engine="postgresql")
    ext = profile_rows([{"id": 1, "doc": {"a": 1}}], ["id", "doc"], ["id"], ["id"], types=types, engine="postgresql")
    out = judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=True, parity=PARITY,
                      not_measured=[])
    assert out["status"] == "not_proven" and "doc" in out["reason"] and "json" in out["reason"]


def test_c4_differing_row_digests_name_the_columns_that_differ():
    types = {"id": "BIGINT", "code": "TEXT", "note": "TEXT"}
    rows = [{"id": 1, "code": "007", "note": "a"}, {"id": 2, "code": "8", "note": "b"}]
    src = profile_rows(rows, list(types), ["id"], None, types=types, engine="sqlite")
    changed = [dict(rows[0], code="7"), rows[1]]
    ext = profile_rows(changed, list(types), ["id"], ["id"], types=types, engine="sqlite")
    out = judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=True, parity=PARITY,
                      not_measured=[])
    assert out["status"] == "incomplete"
    detail = next(d["detail"] for d in out["differences"] if d["kind"] == "changed_rows")
    assert "code" in detail and "note" not in detail


@pytest.fixture()
def acct(world):  # noqa: F811
    conn = sqlite3.connect(str(world["src"]))
    with conn:
        conn.execute("CREATE TABLE acct (id INTEGER PRIMARY KEY, code TEXT, note TEXT, flag TEXT, amt TEXT)")
        conn.executemany("INSERT INTO acct VALUES (?,?,?,?,?)", [
            (1, "007", "  padded  ", "yes", "1.50"), (2, "0100", "plain", "TRUE", "2.000"), (3, "42", "x", "no", "3")])
    conn.close()
    return world


def test_c4_a_source_changed_after_the_extract_is_not_proven_e2e(acct):
    def tamper(_rehearsal):
        conn = sqlite3.connect(str(acct["src"]))
        with conn:
            conn.execute("UPDATE acct SET note='padded', code='7' WHERE id=1")
        conn.close()

    baseline = _run(_system(tables=[{"name": "acct"}]))
    assert baseline["verdict"] == "proven", baseline["tables"][0]["reason"]
    out = _run(_system(name="Ledger 2", tables=[{"name": "acct"}]), tamper=tamper)
    t = out["tables"][0]
    assert out["verdict"] == "incomplete" and t["status"] == "incomplete", t["reason"]
    assert "code" in t["reason"] and "note" in t["reason"]


def test_c4_extract_text_rewritten_is_not_proven_e2e(acct):
    def tamper(rehearsal):
        path = _part_path(acct, rehearsal, "acct")
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        for row in rows:
            row["flag"] = {"yes": "yes", "TRUE": "true", "no": "no"}[row["flag"]]
        path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    out = _run(_system(tables=[{"name": "acct"}]), tamper=tamper)
    t = out["tables"][0]
    # The local part has no upload SHA here, so only the value digest can see the change.
    assert t["row_set"]["match"] is False and t["status"] == "incomplete", t["reason"]


def test_c4_live_pg_json_is_not_proven_and_exact_text_survives(world, monkeypatch):  # noqa: F811
    psycopg2 = pytest.importorskip("psycopg2")
    from services import schedule_runner
    from services.exit_inventory import declare
    from tests.test_parity_live_pg import _pg_cfg

    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    conn.autocommit = True
    table = f"exit_c4_{uuid.uuid4().hex[:10]}"
    saved = {"name": "pg", "type": "postgresql", "host": cfg["host"], "port": cfg["port"],
             "database": cfg["database"], "username": cfg["username"], "password": cfg["password"],
             "schema": "public"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _c, _s: dict(saved))
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE {table} (id BIGINT PRIMARY KEY, code TEXT, amount NUMERIC(12,4),
                        at TIMESTAMPTZ, f DOUBLE PRECISION, flag TEXT, docb JSONB, b BYTEA)""")  # nosec B608
        cur.execute(f"""INSERT INTO {table} VALUES
            (1, '007', 1.5000, '2026-01-01 10:00:00+02', 0.1, 'yes', '{{"n": 1}}', '\\x00ff'),
            (2, '  spaced  ', 2.0000, '2026-06-01 00:00:00.123456+00', 1e-7, 'TRUE', '{{"m": [1, 2]}}', NULL)""")  # nosec B608
    try:
        def system(name):
            return declare({"name": name, "source": {"connector_id": "pg"}, "tables": [{"name": table}],
                            "exit_window_hours": 24}, workspace_id="ws1", actor="owner@example.com")

        out = _run(system("PG typed"))
        t = out["tables"][0]
        assert out["verdict"] == "proven", (t["reason"], t["differences"])
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN doc JSON")  # nosec B608
            cur.execute(f"""UPDATE {table} SET doc = '{{"b": 1, "a": 2, "a": 3}}'""")  # nosec B608
        out = _run(system("PG json"))
        t = out["tables"][0]
        assert out["verdict"] == "not_proven" and t["status"] == "not_proven", t["reason"]
        assert "doc" in t["reason"] and "json" in t["reason"]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        conn.close()



# ----------------------------------------- H1: no row value in a failed extract's reason

_LEAKY = ("Read-back verification failed: row 1 column email→email: source='alice@example.com' dest='' "
          "and source='__DF_SQL_NULL__'")


def test_h1_a_failed_extract_job_error_is_allowlisted():
    from services.exit_rehearsal import _failed_reason

    reason = _failed_reason({"status": "failed", "error": _LEAKY})
    assert "alice" not in reason and "example.com" not in reason and "__DF_SQL_NULL__" not in reason
    assert "source=" not in reason and reason.startswith("The extract job")
    assert "see the server log" in reason


def test_h1_no_row_value_reaches_the_view_the_export_or_pilot_e2e(world):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import render_markdown
    from services.parity_state import job_store
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    def fail_job(rehearsal):
        job_id = rehearsal["tables"][0]["job_id"]
        job_store().update_job_fields(job_id, {"status": "failed", "error": _LEAKY})

    out = _run(_system(tables=[{"name": "orders"}]), tamper=fail_job)
    assert out["verdict"] == "failed"
    stored = exit_store.get(exit_store.REHEARSALS, out["id"])
    with bound_workspace_filter(WorkspaceFilter.only("ws1")):
        pilot = DataPilotTools().execute("explain_exit_rehearsal", {"rehearsal_id": out["id"]})
    assert pilot.success, pilot.error
    for text in (json.dumps(out), render_markdown(stored), json.dumps(pilot.output)):
        assert "alice@example.com" not in text and "__DF_SQL_NULL__" not in text


# ------------------------------- H2: a verification longer than the stale window finishes once


def test_h2_a_long_verification_holds_its_claim_and_closes_exactly_once(world, monkeypatch):  # noqa: F811
    import time
    from datetime import timedelta

    from services import exit_rehearsal
    from services.exit_store import REHEARSALS, get
    from tests.test_exit_rehearsal_e2e import _wait

    real_verify = exit_rehearsal.verify
    runs = []

    def slow_verify(doc, jobs):
        runs.append(time.monotonic())
        time.sleep(1.5)  # stands for a full-population re-read longer than the stale window
        return real_verify(doc, jobs)

    monkeypatch.setattr(exit_rehearsal, "VERIFY_STALE", timedelta(seconds=1))
    monkeypatch.setattr(exit_rehearsal, "verify", slow_verify)
    monkeypatch.setattr(exit_rehearsal, "settle_quietly", lambda rid: exit_rehearsal.settle(rid))
    first = exit_rehearsal.start(_system(tables=[{"name": "events"}]), actor="a@example.com")
    _wait(first)
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline and get(REHEARSALS, first["id"])["verdict"] == "running":
        exit_rehearsal.settle(first["id"], inline=False)  # page poll / scheduler beat
        time.sleep(0.2)
    doc = get(REHEARSALS, first["id"])
    assert doc["verdict"] == "proven", doc["verdict_reason"]
    assert len(runs) == 1, f"{len(runs)} verifications started"
    assert doc["_verify"] is None


# --------------------------- H3: readiness follows the system definition the rehearsal proved


def _proven_events_system():
    system = _system(tables=[{"name": "events"}])
    out = _run(system)
    assert out["verdict"] == "proven", out["verdict_reason"]
    return system, out


@pytest.mark.parametrize("change", [
    {"tables": [{"name": "events"}, {"name": "orders"}, {"name": "never_extracted"}]},
    {"tables": [{"name": "events", "key_columns": ["kind"]}]},
    {"exit_window_hours": 1},
    {"source": {"connector_id": "other-db"}},
])
def test_h3_a_changed_system_is_no_longer_proven_by_an_older_rehearsal(world, change):  # noqa: F811
    from services.exit_inventory import board, get_system, readiness, update

    system, out = _proven_events_system()
    assert out["definition_sha256"]
    before = board([get_system(system["id"])])
    assert before["kpis"]["systems_proven"] == 1 and before["systems"][0]["completeness_label"] == "Proven, 1 table"
    update(system["id"], change, actor="x", expected_version=None)
    current = get_system(system["id"])
    ready = readiness(current)
    assert ready["state"] == "not_proven" and ready["stale"] is True and ready["stale_reason"]
    assert ready["last_rehearsal"]["current"] is False
    row = board([current])
    assert row["kpis"]["systems_proven"] == 0
    assert row["systems"][0]["state"] == "not_proven" and row["systems"][0]["stale"] is True
    assert not row["systems"][0]["completeness_label"].startswith("Proven")
    assert row["systems"][0]["fits_window"] is None


def test_h3_an_unrelated_edit_keeps_the_proof_and_the_label_counts_the_rehearsal(world):  # noqa: F811
    from services.exit_inventory import board, get_system, readiness, update

    system, _out = _proven_events_system()
    update(system["id"], {"description": "renamed", "criticality": "standard", "rehearsal_every_days": 30},
           actor="x", expected_version=None)
    current = get_system(system["id"])
    ready = readiness(current)
    assert ready["state"] == "proven" and ready["stale"] is False and ready["last_rehearsal"]["current"] is True
    row = board([current])["systems"][0]
    assert row["completeness_label"] == "Proven, 1 table" and row["fits_window"] is True
    assert row["window_seconds"] == ready["last_rehearsal"]["window_seconds"] == 720 * 3600


# --------------------------- H4: only a proven rehearsal is compared with the window


@pytest.mark.parametrize("verdict", ["failed", "incomplete", "not_proven"])
def test_h4_a_rehearsal_that_is_not_proven_has_no_window_verdict(verdict):
    from services.exit_rehearsal import duration_block

    out = duration_block(0.0, 0.0, 3600, 10, verdict=verdict)
    assert out["measured_seconds"] == 0 and out["fits_window"] is None and out["margin_pct"] is None
    assert "not proven" in out["statement"] and "fits the window" not in out["statement"]
    proven = duration_block(10.0, 0.0, 3600, 10, verdict="proven")
    assert proven["fits_window"] is True and proven["margin_pct"] is not None


def test_h4_a_failed_rehearsal_reads_no_fits_window_e2e(world):  # noqa: F811
    from services.exit_inventory import board, get_system, readiness

    system = _system(tables=[{"name": "events"}, {"name": "no_such_table"}])
    out = _run(system)
    assert out["verdict"] == "failed"
    assert out["duration"]["fits_window"] is None and out["duration"]["margin_pct"] is None
    ready = readiness(get_system(system["id"]))
    assert ready["last_rehearsal"]["fits_window"] is None
    assert board([get_system(system["id"])])["kpis"]["longest_measured_seconds"] is None


def test_h4_the_longest_measured_kpi_counts_proven_rehearsals_only(tmp_path, monkeypatch):
    from services import exit_store
    from services.exit_inventory import board, definition_sha256

    monkeypatch.setattr(exit_store, "STORE_PATH", tmp_path / "exit.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    systems = [{"id": f"exs_{i}", "workspace_id": "ws", "name": f"s{i}", "source": {"connector_id": "c"},
                "tables": [{"name": "t"}], "exit_window_hours": 1, "created_at": "2026-11-01T00:00:00Z"}
               for i in (1, 2)]
    for system, verdict, seconds in ((systems[0], "proven", 100), (systems[1], "failed", 5000)):
        exit_store.insert(exit_store.REHEARSALS, {
            "id": f"exr_{system['id']}", "system_id": system["id"], "workspace_id": "ws", "n": 1, "verdict": verdict,
            "started_at": "2026-11-02T00:00:00Z", "finished_at": "2026-11-02T00:10:00Z",
            "definition_sha256": definition_sha256(system),
            "duration": {"measured_seconds": seconds, "window_seconds": 3600, "fits_window": seconds <= 3600}})
    kpis = board(systems)["kpis"]
    assert kpis["longest_measured_seconds"] == 100 and kpis["longest_measured_system_id"] == "exs_1"
    rows = {r["id"]: r for r in board(systems)["systems"]}
    assert rows["exs_2"]["fits_window"] is None  # a stored figure from a failed run is not shown as fitting


# ------------------------------------- H6: tests for the mutants the suite did not catch


def test_h6_an_approval_is_bound_to_the_content_it_attested(world):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import content_sha256, sign_off, signoff_block

    out = _run(_system(tables=[{"name": "orders"}]))
    assert out["verdict"] == "proven", out["verdict_reason"]
    doc = exit_store.get(exit_store.REHEARSALS, out["id"])
    changed = json.loads(json.dumps(doc))
    changed["tables"][0]["rows_source"] = int(changed["tables"][0]["rows_source"]) + 1
    assert content_sha256(changed) != content_sha256(doc)

    sign_off(out["id"], role="engineering", actor="eng@example.com", signer_role="operator")

    def alter(current):
        current["tables"][0]["rows_source"] = int(current["tables"][0]["rows_source"]) + 1

    exit_store.update(exit_store.REHEARSALS, out["id"], alter)
    view = signoff_block(exit_store.get(exit_store.REHEARSALS, out["id"]))
    assert [a["role"] for a in view["invalidated_approvals"]] == ["engineering"] and view["approvals"] == []
    sign_off(out["id"], role="data_owner", actor="owner@example.com", signer_role="editor")
    after = sign_off(out["id"], role="compliance", actor="audit@example.com", signer_role="admin")
    assert not after.get("signed"), "a stale approval completed the sign-off"
    assert signoff_block(after)["status"] == "open"


def test_h6_a_source_read_error_reaches_the_reason_allowlisted(world, monkeypatch):  # noqa: F811
    from services import exit_completeness

    def unreadable(_endpoint, **_kw):
        raise RuntimeError("invalid input syntax for type integer: \"alice@example.com\" password=hunter2")

    monkeypatch.setattr(exit_completeness, "source_columns_and_rows", unreadable)
    out = _run(_system(tables=[{"name": "orders"}]))
    reason = out["tables"][0]["reason"]
    assert out["tables"][0]["status"] == "not_proven" and "could not be re-read" in reason
    assert "alice" not in reason and "hunter2" not in reason and "22P02" in reason


# ------------------------------------------------- M1 / M2: anchor read-back on verify


@pytest.fixture()
def lock_bucket(monkeypatch, tmp_path):
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    for name in ("DATAFLOW_S3_ENDPOINT", "DATAWRAP_S3_ENDPOINT", "AWS_ENDPOINT_URL",
                 "DATAFLOW_AUDIT_ANCHOR_S3_ENDPOINT", "DATAWRAP_AUDIT_ANCHOR_S3_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "exit-anchors")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="exit-anchors", ObjectLockEnabledForBucket=True)
        yield s3


def test_m1_a_receipt_that_failed_reads_back_failed(lock_bucket, monkeypatch, tmp_path):
    from services import audit_anchor

    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    lock_bucket.create_bucket(Bucket="no-lock")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "no-lock")
    digest = "cd" * 32
    receipt = audit_anchor.anchor_digest(digest, kind="exit_evidence", subject="exit_evidence:s:r")
    assert receipt["status"] == "failed" and receipt["location"] is None
    checked = audit_anchor.verify_receipt(audit_anchor.find_receipt(digest, kind="exit_evidence"), digest=digest)
    assert checked["status"] == "failed" and "Object Lock" in checked["reason"]


def _signed_rehearsal(world):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import sign_off, signed_pack

    out = _run(_system(tables=[{"name": "orders"}], required_roles=["engineering"]))
    assert out["verdict"] == "proven", out["verdict_reason"]
    sign_off(out["id"], role="engineering", actor="eng@example.com", signer_role="operator")
    return out, signed_pack(exit_store.get(exit_store.REHEARSALS, out["id"]))


def test_m1_a_pack_whose_anchor_failed_at_signing_does_not_verify(world, monkeypatch):  # noqa: F811
    from services.exit_evidence import verify_pack

    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "")
    _out, pack = _signed_rehearsal(world)
    result = verify_pack(pack)
    assert result["anchor"]["status"] == "failed" and result["ok"] is False


def test_m2_another_replica_without_the_local_index_still_finds_the_anchor(world, lock_bucket, monkeypatch):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import verify_pack

    out, pack = _signed_rehearsal(world)
    assert verify_pack(pack)["anchor"]["status"] == "anchored"
    # Another replica: its local receipt index is empty.
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(world["tmp"] / "other_replica.jsonl"))
    assert verify_pack(pack)["anchor"]["status"] == "anchored"
    # ... and even without the rehearsal record, the bucket itself is listed.
    monkeypatch.setattr(exit_store, "get", lambda *_a, **_k: None)
    found = verify_pack(pack)
    assert found["anchor"]["status"] == "anchored" and found["ok"] is True


# ---------------------------------- M3: a stored extract changed after it was written


def test_m3_a_local_part_rewritten_after_the_extract_breaks_the_seal(world):  # noqa: F811
    def rewrite(rehearsal):
        path = _part_path(world, rehearsal, "events")
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        # Same rows, different bytes: only the SHA-256 recorded at write time can tell.
        path.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n")

    out = _run(_system(tables=[{"name": "events"}]), tamper=rewrite)
    t = out["tables"][0]
    assert t["status"] == "incomplete" and t["part"]["verified_on_read"] is False
    assert any(d["kind"] == "seal" for d in t["differences"])


def test_m3_a_clean_local_part_is_verified_against_the_sha_recorded_at_write(world):  # noqa: F811
    from services.parity_state import job_store

    out = _run(_system(tables=[{"name": "events"}]))
    t = out["tables"][0]
    ds = job_store().get_job(t["job_id"])["destination_summary"]
    assert ds["sha256"] and ds["size_bytes"] > 0
    assert t["part"]["sha256"] == ds["sha256"] and t["part"]["verified_on_read"] is True
    assert out["verdict"] == "proven"


def test_m3_pack_verify_says_the_parts_are_not_re_read(world):  # noqa: F811
    from services.exit_evidence import verify_pack

    _out, pack = _signed_rehearsal(world)
    result = verify_pack(pack)
    assert result["parts_reread"] is False and "not re-read" in result["note"]


# ------------------------------------------- M5: control totals never show a PII value


def test_m5_control_totals_of_a_pii_column_are_compared_but_not_shown():
    types = {"id": "BIGINT", "ssn": "BIGINT", "ref": "BIGINT", "amount": "DECIMAL(12,2)"}
    rows = [{"id": 1, "ssn": 123456789, "ref": 4111111111111111, "amount": Decimal("2.50")},
            {"id": 2, "ssn": 987654321, "ref": 7, "amount": Decimal("1.25")}]
    src = profile_rows(rows, list(types), ["id"], None, types=types, engine="postgresql")
    ext = profile_rows([{k: str(v) for k, v in r.items()} for r in rows], list(types), ["id"], sorted(src.totals),
                       types=types, engine="postgresql")
    out = judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=True, parity=PARITY,
                      not_measured=[])
    totals = {c["column"]: c for c in out["control_totals"]}
    for col in ("ssn", "ref"):  # a sensitive name, and a card number found in the values
        assert totals[col]["masked"] is True and totals[col]["match"] is True
        assert totals[col]["source"] == totals[col]["extract"] == {"sum": None, "min": None, "max": None}
    assert totals["amount"]["masked"] is False and totals["amount"]["source"]["max"] == "2.50"
    assert "123456789" not in json.dumps(out) and "4111111111111111" not in json.dumps(out)
    changed = profile_rows([dict(r, ssn=r["ssn"] + 1) for r in rows], list(types), ["id"], sorted(src.totals),
                           types=types, engine="postgresql")
    differs = judge_table(name="t", source=src, extract=changed, key_columns=["id"], seal_ok=True, parity=PARITY,
                          not_measured=[])
    assert {c["column"]: c for c in differs["control_totals"]}["ssn"]["match"] is False


# ------------------------------- M6: the reason claims a key-set check only when one ran


def test_m6_a_declared_key_resolves_case_insensitively_e2e(world):  # noqa: F811
    out = _run(_system(tables=[{"name": "orders", "key_columns": ["ID"]}]))
    t = out["tables"][0]
    assert t["key_set"]["source_sha256"] and t["key_set"]["match"] is True
    assert out["verdict"] == "proven" and "key set" in t["reason"]


def test_m6_declared_keys_resolve_to_the_table_columns_and_a_missing_one_is_named():
    from services.exit_completeness import resolve_keys

    assert resolve_keys(["ID"], ["id", "note"]) == (["id"], [])
    assert resolve_keys(["id", "order_ref"], ["id", "note"]) == (["id"], ["order_ref"])


def test_m6_judge_does_not_claim_a_key_set_it_did_not_measure():
    types = {"id": "BIGINT"}
    src = profile_rows([{"id": 1}], ["id"], [], None, types=types, engine="sqlite")
    ext = profile_rows([{"id": 1}], ["id"], [], ["id"], types=types, engine="sqlite")
    out = judge_table(name="t", source=src, extract=ext, key_columns=["ID"], seal_ok=True, parity=PARITY,
                      not_measured=[])
    assert out["status"] == "proven" and "key set" not in out["reason"]


# --------------------------------------------- API gaps found by the web build (W1-W4)

_TABLE_KEYS = {"name", "schema", "job_id", "status", "reason", "key_columns", "rows_source", "rows_extracted",
               "key_set", "row_set", "control_totals", "value_parity", "differences", "extract_seconds", "part"}


def test_w1_a_running_rehearsal_has_the_full_table_shape(world):  # noqa: F811
    from services.exit_rehearsal import public, start
    from tests.test_exit_rehearsal_e2e import _wait

    started = start(_system(tables=[{"name": "events"}]), actor="a@example.com")
    view = public(started)
    for table in view["tables"]:
        assert _TABLE_KEYS <= set(table), sorted(_TABLE_KEYS - set(table))
        if table["status"] in {"pending", "running"}:
            assert table["rows_source"] is None and table["part"] is None and table["differences"] == []
            assert table["key_set"] == {"source_sha256": None, "extract_sha256": None, "match": None}
            assert table["value_parity"]["method"] is None and table["control_totals"] == []
    _wait(started)


def test_w2_the_board_names_the_running_rehearsal(world):  # noqa: F811
    from services.exit_inventory import board, get_system, readiness
    from services.exit_rehearsal import settle, start
    from tests.test_exit_rehearsal_e2e import _wait

    system = _system(tables=[{"name": "events"}])
    first = _run(system)
    second = start(get_system(system["id"]), actor="a@example.com")
    row = board([get_system(system["id"])])["systems"][0]
    assert row["running_rehearsal_id"] == second["id"] and row["last_rehearsal_id"] == first["id"]
    assert readiness(get_system(system["id"]))["running_rehearsal_id"] == second["id"]
    _wait(second)
    settle(second["id"])
    after = board([get_system(system["id"])])["systems"][0]
    assert after["running_rehearsal_id"] is None and after["last_rehearsal_id"] == second["id"]


def test_w3_reasons_join_sentences_cleanly():
    types = {"id": "BIGINT", "amount": "DECIMAL(12,2)"}
    rows = [{"id": 1, "amount": Decimal("1.00")}, {"id": 2, "amount": Decimal("2.00")}]
    src = profile_rows(rows, list(types), ["id"], None, types=types, engine="sqlite")
    ext = profile_rows([rows[0], dict(rows[1], amount=Decimal("3.00"))], list(types), ["id"], ["id", "amount"],
                       types=types, engine="sqlite")
    out = judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=False, parity=PARITY,
                      not_measured=[])
    assert ".;" not in out["reason"] and ".." not in out["reason"] and out["reason"].endswith(".")
    gapped = judge_table(name="t", source=src, extract=src, key_columns=["id"], seal_ok=True, parity=PARITY,
                         not_measured=["the source could not be re-read (The read failed: see the server log.)",
                                       "doc is json."])
    assert ".;" not in gapped["reason"] and ".," not in gapped["reason"] and ".." not in gapped["reason"]


def test_w4_a_failed_extract_reason_has_no_driver_text_e2e(world):  # noqa: F811
    out = _run(_system(tables=[{"name": "orders"}, {"name": "no_such_table"}]))
    failed = next(t for t in out["tables"] if t["name"] == "no_such_table")
    assert failed["status"] == "failed"
    assert "no such table" not in failed["reason"].lower() and "driver" not in failed["reason"].lower()
    assert failed["reason"].startswith("The extract job failed")
