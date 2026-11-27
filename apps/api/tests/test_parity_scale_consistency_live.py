"""Parity at Scale phase 3: "proven" needs each side read from one snapshot (review defect 3, design §4.2).

The scan pins each side to one snapshot for its whole length (PostgreSQL:
one REPEATABLE READ transaction with a savepoint per statement; MySQL/MariaDB
InnoDB: START TRANSACTION WITH CONSISTENT SNAPSHOT) and records exactly what
it established. Only ``pinned`` may become "proven" or a full population
proof; otherwise the verdict is "no difference found" with the plain
statement that writes during the scan were not ruled out.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _env, _job, _report, _sql, tables  # noqa: E402,F401


def _svc():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _scan_with_a_write_midway(job_id: str, *, pin: bool, write) -> dict[str, Any]:
    """Run the job's scan in slices; after the first units, ``write`` changes the source."""
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore

    spec, _why = verification.table_spec(_svc().get_job(job_id))
    scan = ParityScan.start(verification.plan_for(spec), store=MemoryScanStore(),
                            open_side=verification.side_opener(spec),
                            config=ScanConfig(segment_rows=300, pin_snapshot=pin, mode="R"))
    while scan.state["items_done"] < 2:
        scan.run(max_seconds=0.01)
    write()
    while not scan.done:
        scan.run(max_seconds=5)
    return scan.result()


pg = pytest.mark.skipif(not parity_engines.available("postgresql"),
                        reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


@pg
def test_a_pinned_scan_reads_one_snapshot_while_the_source_moves(tables) -> None:
    src, dst = tables
    job_id = _job(src, dst)
    # A write to the last rows while the first units are being compared.
    moved = _scan_with_a_write_midway(job_id, pin=True,
                                      write=lambda: _sql(f'DELETE FROM "{src}" WHERE id > 2990'))
    assert moved["consistency"]["kind"] == "pinned", moved["consistency"]
    assert moved["consistency"]["source"]["method"] == "repeatable_read_transaction"
    assert moved["outcome"] == "match"  # the destination equals the source as of the source snapshot


@pg
def test_without_a_snapshot_the_same_write_shows_and_nothing_is_established(tables) -> None:
    src, dst = tables
    job_id = _job(src, dst)
    moved = _scan_with_a_write_midway(job_id, pin=False,
                                      write=lambda: _sql(f'DELETE FROM "{src}" WHERE id > 2990'))
    assert moved["consistency"]["kind"] == "not_established"
    assert moved["outcome"] == "differences_found"  # the control: the write really lands mid-scan


@pg
def test_verification_pins_and_only_a_pinned_match_is_proven(tables, monkeypatch) -> None:
    from services.parity_scale import verification
    from services.parity_scale.dialects.base import Side
    from services.signed_proof_pack import classify_post_write_assurance

    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    recon = _svc().get_job(job_id)["reconciliation"]
    assert recon["parity_at_scale"]["consistency"]["kind"] == "pinned"
    assert _report(job_id)["verdict"] == "proven"
    assert classify_post_write_assurance(recon)["migration_proven"] is True

    # The same scan where an engine cannot pin: matched at scan time, nothing more.
    from services.parity_scale.dialects.postgresql import PostgresSide

    monkeypatch.setattr(PostgresSide, "pin", Side.pin)
    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    recon = _svc().get_job(job_id)["reconciliation"]
    assert recon["parity_at_scale"]["consistency"]["kind"] == "not_established"
    report = _report(job_id)
    assert report["verdict"] == "no_difference_found"
    assert "writes during the scan were not ruled out" in report["verdict_reason"]
    assert report["tables"][0]["consistency"] == "not_established"
    assessed = classify_post_write_assurance(recon)
    assert assessed["migration_proven"] is False and assessed.get("source_checksum_provenance") != "engine_segment_digest"


@pg
def test_a_resumed_scan_is_not_pinned(tables) -> None:
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore

    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    store = MemoryScanStore()
    plan = verification.plan_for(spec)
    scan = ParityScan.start(plan, store=store, open_side=verification.side_opener(spec),
                            config=ScanConfig(segment_rows=300, pin_snapshot=True, mode="R"))
    while scan.state["items_done"] < 3:
        scan.run(max_seconds=0.01)
    scan.close()
    resumed = ParityScan.resume(scan.state["scan_id"], store=store, open_side=verification.side_opener(spec),
                                plan=plan)
    while not resumed.done:
        resumed.run(max_seconds=5)
    evidence = resumed.result()
    assert evidence["outcome"] == "match"
    assert evidence["consistency"]["kind"] == "not_established"
    assert "resumed" in " ".join(evidence["consistency"]["reasons"])


def _mysql_usable() -> bool:
    if not parity_engines.available("mysql"):
        return False
    try:
        parity_engines.connect("mysql").close()
        return True
    except Exception:  # noqa: BLE001 — a reachable port with other credentials is not usable here
        return False


@pytest.mark.skipif(not _mysql_usable(), reason="MySQL not usable (DATAFLOW_TEST_MYSQL_*)")
def test_mysql_innodb_pins_a_consistent_snapshot() -> None:
    from services.parity_scale.dialects.mysql import MySQLSide
    from services.parity_scale.plan import build_plan

    table = f"psd1_pin_{uuid.uuid4().hex[:8]}"
    conn = parity_engines.connect("mysql")
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, v INT) ENGINE=InnoDB")
        cur.execute(f"INSERT INTO `{table}` VALUES (1, 1), (2, 2)")
    conn.commit()
    try:
        plan = build_plan([{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
                          source_types={"id": "bigint", "v": "int"}, dest_types={"id": "bigint", "v": "int"},
                          source_engine="mysql", dest_engine="mysql", key_columns=["id"])
        side = MySQLSide(role="source", plan=plan, table=table, connection=parity_engines.connect("mysql"))
        info = side.pin()
        assert info["kind"] == "pinned"
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{table}` WHERE id = 2")
        conn.commit()
        assert int(side.execute(f"SELECT COUNT(*) FROM `{table}`", [])[0][0]) == 2  # the pinned snapshot
        side.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
        conn.close()


@pg
def test_proven_says_each_side_was_read_from_its_own_snapshot(tables) -> None:
    """Review 2, N8: the claim is worded to what was established."""
    from services.parity_scale import verification

    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    consistency = _svc().get_job(job_id)["reconciliation"]["parity_at_scale"]["consistency"]
    assert isinstance(consistency["snapshots_ms_apart"], int)
    reason = _report(job_id)["verdict_reason"]
    assert "each side was read from one snapshot" in reason and "ms apart" in reason
    assert "after its snapshot is not part of this result" in reason


@pg
def test_a_failed_stream_keeps_the_pinned_snapshot_usable(tables) -> None:
    """Review 2, N8: a pinned PostgreSQL stream that errors must not leave the transaction aborted."""
    from services.parity_scale import verification

    src, _dst = tables
    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    plan = verification.plan_for(spec)
    side = verification.side_opener(spec)("source", plan)
    try:
        assert side.pin()["kind"] == "pinned"
        before = side.execute(f'SELECT count(*) FROM "{src}"', [])[0][0]
        with pytest.raises(Exception):
            list(side.stream("SELECT no_such_column FROM pg_class", [], 10))
        _sql(f'DELETE FROM "{src}" WHERE id <= 10')
        assert side.execute(f'SELECT count(*) FROM "{src}"', [])[0][0] == before  # same snapshot, still usable
    finally:
        side.close()


@pg
def test_a_hashed_key_is_not_eligible_and_never_errors(tables) -> None:
    """Review 2, N8: a hash_pii key made every scan error; it is now not eligible, with the reason."""
    from services.parity_scale import verification

    job_id = _job(*tables)
    job = _svc().get_job(job_id)
    job["transfer_request"]["mappings"][0]["transform"] = "hash_pii"
    _svc().update_job_fields(job_id, {"transfer_request": job["transfer_request"]})
    block = verification.start_for_job(job_id, background=False)
    assert block["state"] == "not_eligible", block
    assert "key is a protected column" in block["reason"]


class _OldServer:
    """A connection to a PostgreSQL 13 server (only what prepare_session reads)."""

    def cursor(self):
        rows = iter([("UTF8",), ("130011",)])

        class _Cur:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params=None):
                return None

            def fetchone(self):
                return next(rows)

        return _Cur()

    def rollback(self):
        return None


def test_a_server_below_the_version_floor_is_not_verified_and_says_why(tables, monkeypatch) -> None:
    """Review 3, item 9: the PostgreSQL 10-12 snapshot path was dead (the dialect refuses < 14); the
    floor is stated instead, and a run on an older server reads "not verified at full scale"."""
    from services.parity_scale import verification
    from services.parity_scale.dialects.postgresql import PostgresSide
    from services.parity_scale.plan import PairNotCertified

    with pytest.raises(PairNotCertified, match="needs PostgreSQL 14 or later; this server is PostgreSQL 13"):
        PostgresSide.prepare_session(type("S", (), {"conn": _OldServer()})())
    real = PostgresSide.prepare_session

    def old(self):
        real(self)
        self.conn = _OldServer()
        real(self)

    monkeypatch.setattr(PostgresSide, "prepare_session", old)
    job_id = _job(*tables)
    block = verification.start_for_job(job_id, background=False)
    # Before: "error" / "The comparison stopped (ValueError). Re-verify the table to try again."
    assert block["state"] == "not_eligible"
    assert block["reason"].startswith("the comparison inside the database needs PostgreSQL 14")
    report = _report(job_id)
    # Gate-8's own result stands: the verification neither runs nor blocks sign-off.
    assert report["verdict"] != "verifying"
    assert "Verification" not in str(report["signoff"].get("blocked_reason") or "")


@pg
def test_a_pin_that_fails_falls_back_and_never_errors_the_scan(tables, monkeypatch) -> None:
    """Review 2, N3: a failed pin left the transaction aborted and the scan ended in error."""
    from services.parity_scale import verification
    from services.parity_scale.dialects.postgresql import PostgresSide

    def broken_pin(self, max_hold_s=None) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            try:
                cur.execute("SELECT no_such_snapshot_function()")  # leaves the transaction aborted
            except Exception:  # noqa: BLE001 — the point of the test
                pass
        raise RuntimeError("could not pin")

    monkeypatch.setattr(PostgresSide, "pin", broken_pin)
    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    consistency = _svc().get_job(job_id)["reconciliation"]["parity_at_scale"]["consistency"]
    assert consistency["kind"] == "not_established"
    assert _report(job_id)["verdict"] == "no_difference_found"


@pg
def test_a_hold_past_its_maximum_lets_go_and_says_why(tables, monkeypatch) -> None:
    """Review 2, N2: the hold is capped; past it the scan goes on unpinned and the verdict says why."""
    from services.parity_scale import verification

    monkeypatch.setattr(verification, "snapshot_policy",
                        lambda job: {"enabled": True, "max_hold_s": 0.0, "source": "workspace"})
    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    report = _report(job_id)
    assert report["verdict"] == "no_difference_found"
    assert "held for its maximum" in report["verdict_reason"]
    seen = report["verification"]["tables"][0]["consistency"]
    assert seen["kind"] == "not_established" and seen["source"]["held_s"] is not None


@pg
def test_the_snapshot_setting_turns_pinning_off_and_a_schedule_overrides_it(tables, monkeypatch) -> None:
    """Review 2, N2: "Consistent snapshot during verification" is a deployment, workspace and schedule option."""
    import services.workspace_store as workspace_store

    from services.parity_scale import verification

    monkeypatch.setattr(workspace_store, "get_workspace_settings",
                        lambda: {"verification_snapshot": False, "verification_snapshot_max_minutes": 30})
    job_id = _job(*tables)
    assert verification.snapshot_policy(_svc().get_job(job_id)) == {
        "enabled": False, "max_hold_s": 1800.0, "source": "deployment"}
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    report = _report(job_id)
    assert report["verdict"] == "no_difference_found"
    assert "turned off (deployment setting)" in report["verdict_reason"]

    class Sched:
        verification_snapshot = True
        verification_snapshot_max_minutes = 5

    monkeypatch.setattr(verification, "_schedule_of", lambda job: Sched())
    # Review 3b, item 6: the deployment's "off" is a ceiling; a schedule may only shorten the hold.
    assert verification.snapshot_policy({}) == {"enabled": False, "max_hold_s": 300.0, "source": "deployment"}
    monkeypatch.setattr(workspace_store, "get_workspace_settings",
                        lambda: {"verification_snapshot": True, "verification_snapshot_max_minutes": 30})
    assert verification.snapshot_policy({}) == {"enabled": True, "max_hold_s": 300.0, "source": "schedule"}


@pg
def test_a_session_timeout_shorter_than_the_hold_is_not_pinned(tables) -> None:
    """Review 2, N2: idle_in_transaction_session_timeout below the hold would kill it: fall back, say why."""
    from services.parity_scale import verification

    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    side = verification.side_opener(spec)("source", verification.plan_for(spec))
    try:
        side.conn.autocommit = True
        with side.conn.cursor() as cur:
            cur.execute("SET idle_in_transaction_session_timeout = '10s'")
        side.conn.autocommit = False
        info = side.pin(max_hold_s=1800)
        assert info["kind"] == "not_established"
        assert "idle_in_transaction_session_timeout" in info["reason"]
        assert side.execute("SELECT 1", [])[0][0] == 1  # still usable, statement by statement
    finally:
        side.close()


def test_a_hot_standby_is_never_pinned() -> None:
    """Review 2, N2: pg_is_in_recovery() true → not pinned (recovery conflicts cancel held snapshots)."""
    from services.parity_scale.dialects.postgresql import PostgresSide

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def execute(self, sql: str, *args: Any) -> None:
            return None

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return []

    class Conn:
        def cursor(self):
            return Cursor()

        def rollback(self) -> None:
            return None

    side = object.__new__(PostgresSide)
    side.conn = Conn()
    side.pinned = False
    assert "hot standby" in side._pin_refusal(1800)
