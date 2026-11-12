"""Audit C1 / R1 — an incremental read must not leave late commits behind.

A sequence value and ``now()`` are assigned before COMMIT. Transaction A takes
id 11 and stays open; transaction B inserts 12, 13 and commits; the run reads up
to 13 and stores the watermark; A commits. Its row sorts below the watermark
and no later ``cursor > watermark`` read returns it — while every run reports
green. Measured on the pre-fix engine with this fixture: ``source=13 dest=12
missing=[(11, 'SLOW-TXN')]`` for the sequence cursor and the timestamp cursor.

The commit-safe window bounds each read by a point in *commit* order
(PostgreSQL: the snapshot xmin horizon recorded with the watermark) and re-reads
from the lowest late-committed cursor value, de-duplicated on the row identity.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services import commit_safe_window as csw  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

SEP = "\x1f"


# --------------------------------------------------------------------------
# Deterministic window computation
# --------------------------------------------------------------------------


def test_overlap_seed_shifts_a_timestamp_by_seconds_in_its_own_shape():
    assert csw.overlap_seed("2026-03-01T10:00:00", "TIMESTAMP", seconds=300, values=10) == (
        "2026-03-01T09:55:00"
    )
    assert csw.overlap_seed("2026-03-01 10:00:00.250000", "DATETIME", seconds=60, values=0) == (
        "2026-03-01 09:59:00.250000"
    )
    aware = csw.overlap_seed("2026-03-01T10:00:00+00:00", "TIMESTAMPTZ", seconds=300, values=0)
    assert aware == "2026-03-01T09:55:00+00:00"


def test_overlap_seed_shifts_an_integer_by_values_and_drops_the_tiebreak():
    assert csw.overlap_seed("5000", "BIGINT", seconds=300, values=1000) == "4000"
    # A composite watermark is (cursor, tie-break): the seed is the cursor part
    # alone, because the whole tie group at the seed is re-read.
    assert csw.overlap_seed("5000" + SEP + "77", "INTEGER", seconds=300, values=1000) == "4000"
    assert csw.overlap_seed("12", "SERIAL", seconds=300, values=1000) == "-988"


def test_overlap_seed_refuses_what_it_cannot_shift():
    # Text has no distance: no overlap can be stated, so none is invented.
    assert csw.overlap_seed("order-000123", "TEXT", seconds=300, values=1000) is None
    assert csw.overlap_seed("2026-03-01", "DATE", seconds=0, values=1000) is None
    assert csw.overlap_seed("5000", "BIGINT", seconds=300, values=0) is None


def _plan(**kw):
    base = dict(
        source_type="postgresql",
        sync_mode="incremental_append",
        cursor_column="id",
        cursor_type="BIGINT",
        stored_watermark="13",
        metadata={"commit_horizon": {"strategy": csw.STRATEGY_PG, "xmin": "900"}},
        can_replay=True,
        contract={},
        capture_bound=lambda: {"strategy": csw.STRATEGY_PG, "xmin": "950", "xmax": "955"},
        probe_late=lambda **_: csw.LateRows(count=1, min_cursor="11"),
    )
    base.update(kw)
    return csw.plan_commit_safe_window(**base)


def test_pg_window_reseeds_at_the_lowest_late_commit_and_carries_the_new_bound():
    window = _plan()
    assert window.strategy == csw.STRATEGY_PG
    assert window.seed == "11"
    assert window.replay_through == "13"
    assert window.replays
    assert window.late_rows_detected == 1
    # The bound this run read under is what the next run probes from.
    assert window.metadata() == {
        "commit_horizon": {"strategy": csw.STRATEGY_PG, "xmin": "950", "xmax": "955"}
    }
    evidence = window.evidence()
    assert evidence["bound"]["xmin"] == "950"
    assert evidence["previous_bound"]["xmin"] == "900"
    assert evidence["replay_seed"] == "11" and evidence["stored_watermark"] == "13"
    assert evidence["guarantee"] == csw.GUARANTEE_COMMIT_SAFE


def test_pg_window_without_late_rows_reads_from_the_watermark():
    window = _plan(probe_late=lambda **_: csw.LateRows(count=0, min_cursor=None))
    assert window.seed == "13" and not window.replays and window.replay_through == ""
    assert window.late_rows_detected == 0


def test_rows_edited_in_place_are_not_a_late_commit():
    # Reported behind the watermark, and the destination already holds each of
    # those row versions: no re-read, and the bound moves on.
    window = _plan(
        probe_late=lambda **_: csw.LateRows(
            count=2, min_cursor=None, candidates=(("5", "5"), ("7", "7")), undelivered=0
        )
    )
    assert not window.replays and window.seed == "13"
    assert window.late_rows_detected == 2 and window.late_rows_undelivered == 0
    assert window.metadata()["commit_horizon"]["xmin"] == "950"
    assert "edited in place" in window.reason
    # One of them is not at the destination: the re-read starts there.
    mixed = _plan(
        probe_late=lambda **_: csw.LateRows(
            count=2, min_cursor="11", candidates=(("5", "5"), ("11", "11")), undelivered=1
        )
    )
    assert mixed.replays and mixed.seed == "11" and mixed.late_rows_undelivered == 1


def test_pg_window_probes_from_the_bound_stored_with_the_watermark():
    seen = {}

    def probe(**kw):
        seen.update(kw)
        return csw.LateRows(count=0, min_cursor=None)

    _plan(stored_watermark="13" + SEP + "4", probe_late=probe)
    assert seen["since_xmin"] == "900"
    assert seen["below"] == "13"  # cursor part only: the tie group is re-read anyway


def test_first_bounded_run_after_upgrade_records_a_bound_and_does_not_guess():
    probed = []
    window = _plan(metadata={}, probe_late=lambda **kw: probed.append(kw))
    # No bound was stored with this watermark, so nothing can be probed yet.
    assert probed == []
    assert window.seed == "13" and not window.replays
    assert window.metadata()["commit_horizon"]["xmin"] == "950"
    assert window.guarantee == csw.GUARANTEE_FROM_NEXT_RUN


def test_keyless_append_is_never_called_commit_safe():
    window = _plan(
        source_type="mysql", can_replay=False, cursor_type="BIGINT",
        metadata={}, capture_bound=None, probe_late=None,
    )
    assert window.strategy == csw.STRATEGY_NONE
    assert window.seed == "13" and not window.replays
    assert window.guarantee == csw.GUARANTEE_NONE
    assert "cannot guarantee no skipped rows" in window.reason
    assert window.census_recommended


def test_fallback_overlap_rereads_a_bounded_window_on_a_keyed_route():
    window = _plan(
        source_type="mysql", metadata={}, capture_bound=None, probe_late=None,
        stored_watermark="50000",
    )
    assert window.strategy == csw.STRATEGY_OVERLAP
    assert window.seed == str(50000 - csw.DEFAULT_OVERLAP_VALUES)
    assert window.replay_through == "50000"
    assert window.overlap == {"unit": "values", "size": csw.DEFAULT_OVERLAP_VALUES}
    assert window.guarantee == csw.GUARANTEE_WITHIN_WINDOW
    assert window.census_recommended


def test_declared_overlap_overrides_the_default_and_zero_disables_it():
    wide = _plan(
        source_type="mysql", metadata={}, capture_bound=None, probe_late=None,
        cursor_column="updated_at", cursor_type="DATETIME",
        stored_watermark="2026-03-01 10:00:00", contract={"cursor_overlap_seconds": 3600},
    )
    assert wide.seed == "2026-03-01 09:00:00"
    assert wide.overlap == {"unit": "seconds", "size": 3600}
    off = _plan(
        source_type="mysql", metadata={}, capture_bound=None, probe_late=None,
        contract={"cursor_overlap_values": 0},
    )
    assert off.strategy == csw.STRATEGY_NONE and off.seed == "13"
    assert off.guarantee == csw.GUARANTEE_NONE


def test_single_writer_engine_needs_no_window():
    window = _plan(source_type="sqlite", metadata={}, capture_bound=None, probe_late=None)
    assert window.strategy == csw.STRATEGY_SERIAL
    assert window.seed == "13" and not window.replays
    assert window.guarantee == csw.GUARANTEE_COMMIT_SAFE


def test_first_run_has_no_seed_and_stores_the_bound():
    window = _plan(stored_watermark=None, metadata={})
    assert window.seed is None and not window.replays
    assert window.metadata()["commit_horizon"]["xmin"] == "950"


def test_postgres_compatible_source_without_snapshot_functions_falls_back_to_overlap():
    window = _plan(capture_bound=lambda: {}, stored_watermark="50000")
    assert window.strategy == csw.STRATEGY_OVERLAP
    assert window.seed == str(50000 - csw.DEFAULT_OVERLAP_VALUES)
    assert window.metadata() == {}


def test_a_failed_probe_keeps_the_stored_bound_and_claims_nothing():
    def boom(**_):
        raise RuntimeError("connection reset")

    window = _plan(probe_late=boom)
    assert window.guarantee == csw.GUARANTEE_NONE and not window.replays
    # The next run must ask from the same bound again.
    assert window.metadata()["commit_horizon"]["xmin"] == "900"


def test_replay_seed_is_the_seed_text_and_carries_the_stored_mark():
    window = _plan()
    assert isinstance(window.seed, str) and window.seed == "11"
    assert window.seed.replay_through == "13"
    # Everything that formats or compares the seed sees plain text.
    assert f"{window.seed}" == "11" and window.seed.strip() == "11"


def test_scope_reads_through_the_window_and_the_stored_mark_never_moves():
    import dataclasses

    from services.sync_cursor import IncrementalReadScope

    scope = IncrementalReadScope(cursor_column="id", watermark="13", cursor_key="k")
    assert scope.read_watermark == "13"
    bound = dataclasses.replace(scope, commit_safe=_plan())
    assert bound.read_watermark == "11" and bound.watermark == "13"
    quiet = dataclasses.replace(
        scope, commit_safe=_plan(probe_late=lambda **_: csw.LateRows(count=0))
    )
    assert quiet.read_watermark == "13"


def test_static_commit_safety_states_what_each_route_may_claim():
    pg_keyed = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        has_identity=True,
    )
    assert pg_keyed["guarantee"] == csw.GUARANTEE_COMMIT_SAFE
    keyless = csw.static_commit_safety(
        source_type="mysql", sync_mode="incremental_append", cursor_column="id",
        has_identity=False,
    )
    assert keyless["guarantee"] == csw.GUARANTEE_NONE
    assert keyless["statement"].startswith("Cannot guarantee no skipped rows")
    assert "key_census_every" in keyless["statement"] and keyless["census_recommended"]
    windowed = csw.static_commit_safety(
        source_type="mysql", sync_mode="incremental_deduped", cursor_column="updated_at",
        cursor_type="DATETIME", has_identity=True,
    )
    assert windowed["guarantee"] == csw.GUARANTEE_WITHIN_WINDOW
    assert windowed["overlap"] == {"unit": "seconds", "size": csw.DEFAULT_OVERLAP_SECONDS}
    assert csw.static_commit_safety(
        source_type="mysql", sync_mode="full_refresh_overwrite", cursor_column="", has_identity=True
    ) == {}


def test_cursor_semantics_no_longer_claims_nothing_is_skipped():
    from services.cursor_semantics import evaluate_cursor_semantics
    from services.preflight_cursor_gate import build_sync_contract_gate

    safety = csw.static_commit_safety(
        source_type="mysql", sync_mode="incremental_append", cursor_column="id",
        has_identity=False,
    )
    verdict = evaluate_cursor_semantics(
        sync_mode="incremental_append", cursor_field="id", declared="monotonic_sequence",
        commit_safety=safety,
    )
    assert verdict.status == "ok" and verdict.monotonic_on_insert
    assert "Cannot guarantee no skipped rows" in verdict.reason
    assert verdict.to_dict()["commit_safety"]["guarantee"] == csw.GUARANTEE_NONE

    contract = {"name": "ev", "cursor_field": "id", "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_append"}
    gate = build_sync_contract_gate(
        [contract], sync="incremental_append", validation="strict", dest="postgresql",
        src="mysql", kind="database", source_columns=["id", "v"],
        pass_status="pass", block_status="block",
    )
    assert gate["status"] == "pass"
    assert "Cannot guarantee no skipped rows" in gate["message"]
    assert gate["details"]["commit_safety_warnings"]
    keyed = build_sync_contract_gate(
        [contract], sync="incremental_append", validation="strict", dest="postgresql",
        src="postgresql", kind="database", source_columns=["id", "v"],
        pass_status="pass", block_status="block", catalog_primary_key_columns=["id"],
    )
    assert keyed["status"] == "pass" and "Cannot guarantee" not in keyed["message"]
    assert keyed["details"]["commit_safety_warnings"] is None


def test_non_incremental_modes_get_no_window():
    assert csw.plan_commit_safe_window(
        source_type="postgresql", sync_mode="full_refresh_overwrite", cursor_column="id",
        cursor_type="BIGINT", stored_watermark=None, metadata={}, can_replay=True, contract={},
    ) is None


# --------------------------------------------------------------------------
# The race, on two real PostgreSQL connections
# --------------------------------------------------------------------------

SCHEMA = "df_commit_safe"


def _connect(autocommit: bool = True):
    import psycopg2

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], user=c["username"],
        password=c["password"], dbname=c["database"],
    )
    conn.autocommit = autocommit
    return conn


def _sql(statement: str, fetch: bool = False):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _endpoint(table: str) -> EndpointConfig:
    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], schema=SCHEMA, table=table,
        username=c["username"], password=c["password"],
    )


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def events():
    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    _sql(
        f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, "
        "updated_at timestamptz NOT NULL DEFAULT now(), v text)"
    )
    _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'base' || g FROM generate_series(1, 10) g")
    yield "ev"
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, dst: str, *, cursor: str, mode: str, semantics: str = "", pk: str = "",
         skip_preflight: bool = True):
    contract = {"name": "ev", "cursor_field": cursor, "sync_mode": mode, "selected": True}
    if semantics:
        contract["cursor_semantics"] = semantics
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=_endpoint("ev"), destination=_endpoint(dst), sync_mode=mode,
        stream_contracts=[contract], skip_preflight=skip_preflight, validation_mode="strict",
    )
    job_id = "csw" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _missing(dst: str) -> list[tuple]:
    return _sql(
        f"SELECT s.id, s.v FROM {SCHEMA}.ev s LEFT JOIN {SCHEMA}.{dst} d ON d.id = s.id "
        "WHERE d.id IS NULL ORDER BY 1",
        fetch=True,
    )


def _duplicates(dst: str) -> list[tuple]:
    return _sql(
        f"SELECT id, count(*) FROM {SCHEMA}.{dst} GROUP BY id HAVING count(*) > 1", fetch=True
    )


CASES = [
    pytest.param("id", "incremental_append", "", "", True, id="sequence-append-keyless-contract"),
    pytest.param("updated_at", "incremental_append", "", "", True, id="timestamp-append"),
    pytest.param("updated_at", "incremental_deduped", "", "id", True, id="timestamp-deduped"),
    pytest.param(
        "id", "incremental_append", "monotonic_sequence", "id", False,
        id="sequence-append-preflight-on",
    ),
]


@pytest.mark.parametrize("cursor,mode,semantics,pk,skip_preflight", CASES)
def test_incremental_inflight_transaction_is_not_skipped(
    isolated, events, cursor, mode, semantics, pk, skip_preflight
):
    dst = "ev_out_" + uuid.uuid4().hex[:8]  # the append-continuity ledger is per table
    kw = dict(cursor=cursor, mode=mode, semantics=semantics, pk=pk, skip_preflight=skip_preflight)

    first = _run(isolated, dst, **kw)
    assert first.success, first.error

    # T1 takes sequence value 11 and now() = T1, and does not commit.
    slow = _connect(autocommit=False)
    try:
        slow.cursor().execute(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('SLOW-TXN')")
        # T2 inserts 12, 13 with a later now() and commits first.
        _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('fast-1'), ('fast-2')")

        second = _run(isolated, dst, **kw)
        assert second.success, second.error
        assert int(second.records_transferred) == 2
        slow.commit()
    finally:
        slow.close()

    third = _run(isolated, dst, **kw)
    assert third.success, third.error
    assert _missing(dst) == [], "the late commit stayed behind the watermark"
    assert _duplicates(dst) == []
    assert int(third.records_transferred) == 1

    # The first read already recorded the bound the second one was probed from.
    first_bound = (first.destination_summary.get("commit_safe_window") or {}).get("bound") or {}
    assert first_bound.get("xmin"), first.destination_summary.get("commit_safe_window")
    evidence = third.destination_summary.get("commit_safe_window") or {}
    assert evidence.get("strategy") == csw.STRATEGY_PG, evidence
    assert evidence.get("previous_bound", {}).get("xmin"), evidence
    assert evidence.get("bound", {}).get("xmin"), evidence
    assert int(evidence.get("late_rows_detected") or 0) >= 1, evidence
    assert evidence.get("replay_seed") not in (None, ""), evidence
    assert int(evidence.get("rows_recovered") or 0) == 1, evidence

    # The job's own cursor evidence carries the same record.
    job = next(j for j in isolated.jobs.values() if j.get("incremental_watermark"))
    assert any(
        (j.get("incremental_watermark") or {}).get("commit_safe_window", {}).get("strategy")
        == csw.STRATEGY_PG
        for j in isolated.jobs.values()
    ), job

    # Idempotent: quiet polls neither duplicate nor lose, and re-read nothing.
    fourth = _run(isolated, dst, **kw)
    assert fourth.success, fourth.error
    assert int(fourth.records_transferred) == 0
    assert _missing(dst) == [] and _duplicates(dst) == []
    total = _sql(f"SELECT count(*) FROM {SCHEMA}.{dst}", fetch=True)[0][0]
    assert total == 13


def test_savepoint_rows_of_an_open_transaction_are_not_skipped(isolated, events):
    """A row inserted under a SAVEPOINT carries a subtransaction xid, which a
    snapshot's in-progress list never names — the horizon bound still covers it."""
    dst = "ev_sub_" + uuid.uuid4().hex[:8]
    kw = dict(cursor="id", mode="incremental_append")
    assert _run(isolated, dst, **kw).success
    slow = _connect(autocommit=False)
    try:
        cur = slow.cursor()
        cur.execute(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('outer')")
        cur.execute("SAVEPOINT s1")
        cur.execute(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('inner-subxact')")
        _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('fast')")
        assert _run(isolated, dst, **kw).success
        slow.commit()
    finally:
        slow.close()
    third = _run(isolated, dst, **kw)
    assert third.success, third.error
    assert _missing(dst) == [] and _duplicates(dst) == []


# --------------------------------------------------------------------------
# Fallback: overlap re-read on a source without a commit-order bound (MySQL)
# --------------------------------------------------------------------------


def _mysql(autocommit: bool = True):
    import pymysql

    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    return pymysql.connect(
        host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
        database=c["database"], autocommit=autocommit,
    )


def _mysql_endpoint(table: str) -> EndpointConfig:
    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    return EndpointConfig(
        kind="database", format="mysql", host=c["host"], port=int(c["port"]),
        database=c["database"], table=table, username=c["username"], password=c["password"],
    )


@pytest.mark.parametrize(
    "mode,pk",
    [("incremental_append", ""), ("incremental_deduped", "id")],
    ids=["append-catalog-key", "deduped"],
)
def test_overlap_reread_recovers_a_late_commit_on_mysql(isolated, mode, pk):
    from tests.helpers.live_env import mysql_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL / PostgreSQL not reachable")
    src = "csw_src_" + uuid.uuid4().hex[:8]
    dst = "csw_dst_" + uuid.uuid4().hex[:8]
    _sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {src} (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32))"
            )
            cur.executemany(f"INSERT INTO {src} (v) VALUES (%s)", [(f"base{i}",) for i in range(1, 11)])

        def run():
            contract = {"name": src, "cursor_field": "id", "sync_mode": mode, "selected": True}
            if pk:
                contract["primary_key"] = pk
            request = TransferRequest(
                source=_mysql_endpoint(src), destination=_endpoint(dst), sync_mode=mode,
                stream_contracts=[contract], skip_preflight=True, validation_mode="strict",
            )
            job_id = "csw" + uuid.uuid4().hex[:16]
            isolated.update_job_status(job_id, "pending", transfer_request={})
            return UniversalTransferEngine().execute_tracked(request, job_id)

        first = run()
        assert first.success, first.error
        slow = _mysql(autocommit=False)
        try:
            slow.cursor().execute(f"INSERT INTO {src} (v) VALUES ('SLOW-TXN')")  # id 11, open
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO {src} (v) VALUES ('fast-1'), ('fast-2')")  # 12, 13
            second = run()
            assert second.success, second.error
            slow.commit()
        finally:
            slow.close()
        third = run()
        assert third.success, third.error
        got = _sql(f"SELECT id, count(*) FROM {SCHEMA}.{dst} GROUP BY id ORDER BY id", fetch=True)
        assert [r[0] for r in got] == list(range(1, 14)), got
        assert all(r[1] == 1 for r in got), got

        evidence = third.destination_summary.get("commit_safe_window") or {}
        assert evidence.get("strategy") == csw.STRATEGY_OVERLAP, evidence
        assert evidence.get("overlap") == {"unit": "values", "size": csw.DEFAULT_OVERLAP_VALUES}
        assert evidence.get("guarantee") == csw.GUARANTEE_WITHIN_WINDOW
        # Bounded re-read: the 12 rows below the mark (ids 1-12), 11 of them
        # already delivered and one — the late commit — recovered.
        assert int(evidence.get("rows_replayed") or 0) == 12, evidence
        assert int(evidence.get("rows_already_landed") or 0) == 11, evidence
        assert int(evidence.get("rows_recovered") or 0) == 1, evidence

        fourth = run()  # quiet poll: re-reads the window, recovers nothing
        assert fourth.success, fourth.error
        # An upsert re-merges the tie row at the watermark, as it did before the
        # window existed (D124); nothing below the mark is written again.
        assert int(fourth.records_transferred) <= (1 if pk else 0)
        quiet = fourth.destination_summary.get("commit_safe_window") or {}
        assert int(quiet.get("rows_replayed") or 0) == 12 and int(quiet.get("rows_recovered") or 0) == 0
        assert _sql(f"SELECT count(*) FROM {SCHEMA}.{dst}", fetch=True)[0][0] == 13
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
        conn.close()
        _sql(f'DROP TABLE IF EXISTS {SCHEMA}."{dst}"')


# --------------------------------------------------------------------------
# R1b — periodic key census
# --------------------------------------------------------------------------


def test_census_schedule_counts_runs():
    from services.incremental_census import census_schedule

    assert census_schedule(0, 7) == (False, 0)  # off by default
    assert census_schedule(1, 0) == (True, 0)  # every run
    assert census_schedule(3, 0) == (False, 1)
    assert census_schedule(3, 1) == (False, 2)
    assert census_schedule(3, 2) == (True, 0)


def _sqlite_exec(path, sql, rows=None):
    import sqlite3

    conn = sqlite3.connect(str(path))
    try:
        with conn:
            return (conn.execute(sql) if rows is None else conn.executemany(sql, rows)).fetchall()
    finally:
        conn.close()


def _sqlite_run(fake, src, dst, **contract_extra):
    contract = {
        "name": "t", "cursor_field": "seq", "sync_mode": "incremental_append",
        "selected": True, **contract_extra,
    }
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append", stream_contracts=[contract],
        skip_preflight=True, validation_mode="strict",
    )
    job_id = "csw" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_key_census_reports_a_row_behind_the_watermark_and_is_never_green(tmp_path, isolated):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _sqlite_exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    _sqlite_exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in range(1, 21)])

    first = _sqlite_run(isolated, src, dst, key_census_every=2)
    assert first.success, first.error
    assert "incremental_key_census" not in (first.reconciliation or {})  # run 1 of 2

    # A row lands behind the watermark (seq 5 < 20): no cursor read returns it.
    _sqlite_exec(src, "INSERT INTO t VALUES (500, 5, 'behind')")
    _sqlite_exec(src, "INSERT INTO t VALUES (21, 21, 'new')")

    second = _sqlite_run(isolated, src, dst, key_census_every=2)  # run 2 of 2: census due
    census = (second.reconciliation or {}).get("incremental_key_census") or {}
    assert census.get("ran") is True, second.reconciliation
    assert census.get("missing_behind_watermark") == 1 and census.get("missing_keys") == ["500"]
    assert not second.success
    assert "Key census" in (second.error or "") and "500" in second.error
    drift = (second.reconciliation or {}).get("out_of_window_drift") or {}
    assert drift.get("behind_watermark") == 1
    # The delta itself was delivered; only the verdict is not green.
    assert _sqlite_exec(dst, "SELECT count(*) FROM t WHERE id = 21")[0][0] == 1


def test_key_census_is_off_by_default(tmp_path, isolated):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _sqlite_exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    _sqlite_exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in range(1, 21)])
    assert _sqlite_run(isolated, src, dst).success
    _sqlite_exec(src, "INSERT INTO t VALUES (500, 5, 'behind')")
    second = _sqlite_run(isolated, src, dst)
    assert second.success, second.error
    assert "incremental_key_census" not in (second.reconciliation or {})


def test_clean_key_census_passes(tmp_path, isolated):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _sqlite_exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    _sqlite_exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in range(1, 21)])
    assert _sqlite_run(isolated, src, dst, key_census_every=1).success
    _sqlite_exec(src, "INSERT INTO t VALUES (21, 21, 'new')")
    second = _sqlite_run(isolated, src, dst, key_census_every=1)
    assert second.success, second.error
    census = (second.reconciliation or {}).get("incremental_key_census") or {}
    assert census.get("ran") is True and census.get("missing_behind_watermark") == 0, census


def test_keyless_append_does_not_replay_and_says_so(isolated):
    """No key, no unique cursor: a re-read row cannot be told from a new one.

    The route is not re-read (no duplicates), is never called commit-safe, and
    the late commit the source reports is a warning on the Gate-8 report.
    """
    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    try:
        _sql(f"CREATE TABLE {SCHEMA}.ev (seq bigint NOT NULL, v text)")  # no key at all
        _sql(f"INSERT INTO {SCHEMA}.ev SELECT g, 'base' || g FROM generate_series(1, 10) g")
        dst = "ev_keyless_" + uuid.uuid4().hex[:8]
        kw = dict(cursor="seq", mode="incremental_append")
        assert _run(isolated, dst, **kw).success
        slow = _connect(autocommit=False)
        try:
            slow.cursor().execute(f"INSERT INTO {SCHEMA}.ev VALUES (11, 'SLOW-TXN')")
            _sql(f"INSERT INTO {SCHEMA}.ev VALUES (12, 'fast-1'), (13, 'fast-2')")
            assert _run(isolated, dst, **kw).success
            slow.commit()
        finally:
            slow.close()
        third = _run(isolated, dst, **kw)
        assert third.success, third.error
        evidence = third.destination_summary.get("commit_safe_window") or {}
        assert evidence.get("guarantee") == csw.GUARANTEE_NONE, evidence
        assert int(evidence.get("late_rows_detected") or 0) >= 1
        assert "cannot guarantee no skipped rows" in evidence.get("reason", "")
        assert evidence.get("census_recommended") is True
        warnings = " ".join((third.reconciliation or {}).get("warnings") or [])
        assert "committed behind the watermark" in warnings, third.reconciliation
        rows = _sql(f"SELECT seq, count(*) FROM {SCHEMA}.{dst} GROUP BY seq", fetch=True)
        assert all(n == 1 for _seq, n in rows), rows  # nothing duplicated
        assert sorted(s for s, _n in rows) == [*range(1, 11), 12, 13]  # the honest limit
    finally:
        _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


@pytest.mark.parametrize("mode,pk", [("incremental_append", ""), ("incremental_deduped", "id")])
def test_a_row_edited_in_place_is_not_reread_and_stays_drift(isolated, events, mode, pk):
    """An UPDATE that leaves the cursor alone writes a new row version behind the
    watermark, exactly like a late commit. The destination already holds that
    (cursor, id): nothing is re-read, nothing is duplicated, the run is green."""
    dst = "ev_edit_" + uuid.uuid4().hex[:8]
    kw = dict(cursor="id", mode=mode, pk=pk)
    assert _run(isolated, dst, **kw).success
    _sql(f"UPDATE {SCHEMA}.ev SET v = 'edited in place' WHERE id = 3")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('new-11')")
    second = _run(isolated, dst, **kw)
    assert second.success, second.error
    assert int(second.records_transferred) == 1
    evidence = second.destination_summary.get("commit_safe_window") or {}
    assert int(evidence.get("late_rows_detected") or 0) >= 1, evidence
    assert evidence.get("late_rows_undelivered") == 0, evidence
    assert evidence.get("replay_seed") is None and "rows_replayed" not in evidence, evidence
    assert _duplicates(dst) == [] and _missing(dst) == []
    kept = _sql(f"SELECT v FROM {SCHEMA}.{dst} WHERE id = 3", fetch=True)
    assert kept == [("base3",)]  # the cursor did not select the edit (D188 drift)
