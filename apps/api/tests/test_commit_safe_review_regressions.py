"""PR 407 review — regressions of the commit-safe incremental window, on live engines.

Each test reproduces a review finding that lost rows silently or claimed more
than the run did. The live cases need a reachable PostgreSQL (and MySQL where
named); they skip, never pass, when it is not there.
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

SCHEMA = "df_csw_review"


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
def pg():
    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    yield SCHEMA
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, src: str, dst: str, *, cursor: str, mode: str, pk: str = "",
         skip_preflight: bool = True, source: EndpointConfig | None = None, **extra):
    contract = {"name": src, "cursor_field": cursor, "sync_mode": mode, "selected": True, **extra}
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=source or _endpoint(src), destination=_endpoint(dst), sync_mode=mode,
        stream_contracts=[contract], skip_preflight=skip_preflight, validation_mode="strict",
    )
    job_id = "csr" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _window(result) -> dict:
    return (result.destination_summary or {}).get("commit_safe_window") or {}


# --------------------------------------------------------------------------
# C1 — a composite key with a coarse cursor
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode,pk",
    [
        ("incremental_deduped", "order_id,line_id"),
        ("incremental_append", "order_id,line_id"),
        ("incremental_append", ""),  # the catalog primary key is the identity
    ],
    ids=["deduped", "append", "append-catalog-key"],
)
def test_late_row_sharing_cursor_and_first_key_column_is_not_skipped(isolated, pg, mode, pk):
    """PK (order_id, line_id), cursor a DATE. The late row (7, 2) shares the
    cursor value and order_id with the delivered (7, 1): judging delivery on
    (cursor, first key column) called it an in-place edit and skipped it."""
    _sql(
        f"CREATE TABLE {SCHEMA}.lines (order_id int, line_id int, d date NOT NULL, v text, "
        "PRIMARY KEY (order_id, line_id))"
    )
    _sql(f"INSERT INTO {SCHEMA}.lines VALUES (7, 1, '2026-11-10', 'a'), (6, 1, '2026-11-09', 'b')")
    dst = "lines_out_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="d", mode=mode, pk=pk)
    assert _run(isolated, "lines", dst, **kw).success
    slow = _connect(autocommit=False)
    try:
        slow.cursor().execute(f"INSERT INTO {SCHEMA}.lines VALUES (7, 2, '2026-11-10', 'LATE')")
        _sql(f"INSERT INTO {SCHEMA}.lines VALUES (8, 1, '2026-11-11', 'fast')")
        assert _run(isolated, "lines", dst, **kw).success
        slow.commit()
    finally:
        slow.close()
    _sql(f"INSERT INTO {SCHEMA}.lines VALUES (9, 1, '2026-11-12', 'next')")
    third = _run(isolated, "lines", dst, **kw)
    assert third.success, third.error
    evidence = _window(third)
    assert evidence.get("strategy") == csw.STRATEGY_PG, evidence
    assert int(evidence.get("late_rows_undelivered") or 0) == 1, evidence
    assert "edited in place" not in str(evidence.get("reason")), evidence
    assert _run(isolated, "lines", dst, **kw).success
    missing = _sql(
        f"SELECT s.order_id, s.line_id, s.v FROM {SCHEMA}.lines s LEFT JOIN {SCHEMA}.{dst} d "
        "ON d.order_id = s.order_id AND d.line_id = s.line_id WHERE d.order_id IS NULL",
        fetch=True,
    )
    assert missing == [], "the late commit stayed behind the watermark"
    dupes = _sql(
        f"SELECT order_id, line_id FROM {SCHEMA}.{dst} GROUP BY 1, 2 HAVING count(*) > 1",
        fetch=True,
    )
    assert dupes == []


def test_replay_key_is_the_whole_unique_key_or_says_it_is_not():
    from types import SimpleNamespace

    from src.transfer.stream_commit_window import route_replay_key

    def cfg(**kw):
        base = dict(
            cursor_source_col="d", effective_sync="incremental_append", pk_source_cols=[],
            pk_target_cols=[], cursor_pk_source="",
        )
        base.update(kw)
        return SimpleNamespace(**base)

    composite = cfg(pk_source_cols=["order_id", "line_id"], pk_target_cols=["order_id", "line_id"],
                    cursor_pk_source="order_id")
    assert route_replay_key(composite, None) == (("order_id", "line_id"), True)
    upsert = cfg(effective_sync="incremental_deduped", pk_source_cols=["a", "b"],
                 pk_target_cols=["a", "b"])
    assert route_replay_key(upsert, None) == (("a", "b"), True)
    # Catalog primary key: the whole of it, not the tie-break column alone.
    catalog = ({}, {"a": False, "b": False}, {"primary_key_columns": ["a", "b"]})
    assert route_replay_key(cfg(cursor_pk_source="a"), catalog) == (("a", "b"), True)
    # A non-null unique key the tie-break comes from.
    uk = ({}, {"d": False, "x": False}, {"unique_keys": [{"columns": ["d", "x"]}]})
    assert route_replay_key(cfg(cursor_pk_source="x"), uk) == (("d", "x"), True)
    # A cursor the catalog proves unique.
    alone = ({}, {"d": False}, {"primary_key_columns": ["d"]})
    assert route_replay_key(cfg(), alone) == (("d",), True)
    # A tie-break column with no key evidence: usable to look rows up, but not
    # a unique key — every reported row then counts as undelivered.
    assert route_replay_key(cfg(cursor_pk_source="x"), None) == (("x",), False)
    assert route_replay_key(cfg(), None) == ((), False)


# --------------------------------------------------------------------------
# H1 — a text cursor whose type name contains INT or LONG
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared,unit",
    [
        ("LONGTEXT", ""),
        ("LONG VARCHAR", ""),
        ("LONG", ""),  # Oracle LONG is character data
        ("POINT", ""),
        ("MULTIPOINT", ""),
        ("INTERVAL", ""),
        ("INTERVAL DAY TO SECOND", ""),
        ("VARCHAR(20)", ""),
        ("TEXT", ""),
        ("NUMBER", ""),  # no scale: not an integer
        ("NUMBER(10,2)", ""),
        ("DECIMAL(18,4)", ""),
        ("FLOAT", ""),
        ("DATE", ""),
        ("INT", "values"),
        ("int unsigned", "values"),
        ("INTEGER", "values"),
        ("BIGINT", "values"),
        ("BIGINT UNSIGNED", "values"),
        ("SMALLINT", "values"),
        ("TINYINT", "values"),
        ("MEDIUMINT", "values"),
        ("SERIAL", "values"),
        ("BIGSERIAL", "values"),
        ("INT2", "values"),
        ("INT4", "values"),
        ("INT8", "values"),
        ("INT64", "values"),
        ("NUMBER(10)", "values"),
        ("NUMBER(10,0)", "values"),
        ("NUMERIC(12, 0)", "values"),
        ("DECIMAL(18)", "values"),
        ("TIMESTAMP", "seconds"),
        ("TIMESTAMP(6) WITH TIME ZONE", "seconds"),
        ("TIMESTAMPTZ", "seconds"),
        ("DATETIME", "seconds"),
        ("DATETIME2", "seconds"),
        ("SMALLDATETIME", "seconds"),
    ],
)
def test_cursor_distance_is_decided_by_whole_type_words(declared, unit):
    assert csw.cursor_distance_unit(declared) == unit


def test_zero_padded_text_never_gets_an_integer_seed():
    # The seed would lose its padding: '4000' sorts above '005001' as text.
    assert csw.overlap_seed("005000", "LONGTEXT", seconds=300, values=1000) is None
    assert csw.overlap_seed("005000", "", seconds=300, values=1000) is None
    assert csw.overlap_seed("005000", "BIGINT", seconds=300, values=1000) is None
    assert csw.overlap_seed("+5000", "BIGINT", seconds=300, values=1000) is None
    assert csw.overlap_seed("5000", "BIGINT", seconds=300, values=1000) == "4000"
    assert csw.overlap_seed("-5", "INT", seconds=300, values=10) == "-15"


def test_text_cursor_route_is_not_guaranteed_and_reads_from_the_watermark():
    window = csw.plan_commit_safe_window(
        source_type="mysql", sync_mode="incremental_append", cursor_column="code",
        cursor_type="LONGTEXT", stored_watermark="005000", metadata={}, can_replay=True,
        contract={},
    )
    assert window.strategy == csw.STRATEGY_NONE and window.guarantee == csw.GUARANTEE_NONE
    assert window.seed == "005000" and not window.replays
    assert window.census_recommended
    static = csw.static_commit_safety(
        source_type="mysql", sync_mode="incremental_append", cursor_column="code",
        cursor_type="LONGTEXT", has_identity=True,
    )
    assert static["guarantee"] == csw.GUARANTEE_NONE, static
    assert static["statement"].startswith("Cannot guarantee no skipped rows"), static
    assert static["census_recommended"]


def test_mysql_longtext_cursor_delivers_every_new_row(isolated, pg):
    """Live MySQL → PostgreSQL. A LONGTEXT cursor of zero-padded numbers: the
    integer re-read seed '4000' sat above every new value as text, so runs 2
    and 3 read nothing and reported commit_safe_within_window."""
    from tests.helpers.live_env import mysql_up
    from tests.test_incremental_commit_safe_window import _mysql, _mysql_endpoint

    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    src = "csr_lt_" + uuid.uuid4().hex[:6]
    dst = "csr_ltd_" + uuid.uuid4().hex[:6]
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, code LONGTEXT NOT NULL, v VARCHAR(20))")
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s, %s, %s)",
                [(i, f"{i:06d}", f"v{i}") for i in range(4995, 5001)],
            )
        kw = dict(cursor="code", mode="incremental_append", source=_mysql_endpoint(src))
        assert _run(isolated, src, dst, **kw).success
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s, %s, %s)",
                [(i, f"{i:06d}", f"new{i}") for i in range(5001, 5004)],
            )
        second = _run(isolated, src, dst, **kw)
        assert second.success, second.error
        assert int(second.records_transferred) == 3
        evidence = _window(second)
        assert evidence.get("guarantee") == csw.GUARANTEE_NONE, evidence
        assert evidence.get("replay_seed") is None, evidence
        assert _run(isolated, src, dst, **kw).success
        got = sorted(r[0] for r in _sql(f"SELECT id FROM {SCHEMA}.{dst}", fetch=True))
        assert got == list(range(4995, 5004))
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
        conn.close()


# --------------------------------------------------------------------------
# H2 — a view (or any relation that is not a table) as the source
# --------------------------------------------------------------------------


def test_only_tables_get_the_snapshot_horizon():
    def plan(kind):
        return csw.plan_commit_safe_window(
            source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
            cursor_type="BIGINT", stored_watermark="13",
            metadata={"commit_horizon": {"strategy": csw.STRATEGY_PG, "xmin": "900"}},
            can_replay=True, contract={},
            capture_bound=lambda: {"strategy": csw.STRATEGY_PG, "xmin": "950", "xmax": "955"},
            probe_late=lambda **_: csw.LateRows(count=0),
            relation_kind=kind,
        )

    assert plan("r").strategy == csw.STRATEGY_PG
    assert plan("p").strategy == csw.STRATEGY_PG
    for kind in ("v", "m", "f", ""):
        window = plan(kind)
        assert window.strategy == csw.STRATEGY_OVERLAP, (kind, window)
        assert window.guarantee == csw.GUARANTEE_WITHIN_WINDOW
        assert window.seed == str(13 - csw.DEFAULT_OVERLAP_VALUES)
        assert window.bound == {}  # no horizon is stored for a relation without one
    assert "view" in plan("v").reason
    # Unknown kind: the lookup failed — said on the window, never silent.
    assert plan("").probe_error
    # No distance on the cursor: not guaranteed, with the census.
    text = csw.plan_commit_safe_window(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="code",
        cursor_type="TEXT", stored_watermark="a", metadata={}, can_replay=True, contract={},
        capture_bound=lambda: {"strategy": csw.STRATEGY_PG, "xmin": "950"},
        probe_late=lambda **_: csw.LateRows(count=0), relation_kind="v",
    )
    assert text.guarantee == csw.GUARANTEE_NONE and text.census_recommended


def test_static_claim_for_a_view_is_never_covered():
    view = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        cursor_type="BIGINT", has_identity=True, relation_kind="v",
    )
    assert view["strategy"] == csw.STRATEGY_OVERLAP and view["guarantee"] == csw.GUARANTEE_WITHIN_WINDOW
    assert "view" in view["statement"]
    assert "Late commits are covered: each run records" not in view["statement"]
    unknown = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="v",
        cursor_type="TEXT", has_identity=True, relation_kind="",
    )
    assert unknown["guarantee"] == csw.GUARANTEE_NONE, unknown


def _view_source():
    _sql(f"CREATE TABLE {SCHEMA}.base (id bigserial PRIMARY KEY, v text)")
    _sql(f"INSERT INTO {SCHEMA}.base (v) SELECT 'b' || g FROM generate_series(1, 10) g")
    _sql(f"CREATE VIEW {SCHEMA}.v_base AS SELECT id, v FROM {SCHEMA}.base")


def test_preflight_names_a_view_source_and_does_not_claim_the_horizon(pg):
    from services.preflight_transfer_policy import run_transfer_policy_gates

    _view_source()
    c = pg_creds()
    contract = {"name": "v_base", "cursor_field": "id", "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_append", "primary_key": "id"}
    gates = run_transfer_policy_gates(
        sync_mode="incremental_append", validation_mode="strict", stream_contracts=[contract],
        source_columns=["id", "v"], dest_type="postgresql", source_type="postgresql",
        source_kind="database", source_table="v_base",
        source_config={"type": "postgresql", "host": c["host"], "port": int(c["port"]),
                       "database": c["database"], "username": c["username"],
                       "password": c["password"], "schema": SCHEMA},
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    safety = (g9["details"]["cursor_semantics"] or [{}])[0].get("commit_safety") or {}
    assert safety.get("strategy") == csw.STRATEGY_OVERLAP, safety
    assert "Late commits are covered: each run records" not in str(safety.get("statement"))
    assert "view" in str(safety.get("statement"))


def test_view_source_recovers_a_late_commit_within_the_window(isolated, pg):
    """Every horizon probe on a view failed (a view has no xmin), the runs stayed
    green and the late row was skipped with no warning on the report."""
    _view_source()
    dst = "vout_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append", pk="id")
    first = _run(isolated, "v_base", dst, **kw)
    assert first.success, first.error
    assert _window(first).get("strategy") == csw.STRATEGY_OVERLAP, _window(first)
    slow = _connect(autocommit=False)
    try:
        slow.cursor().execute(f"INSERT INTO {SCHEMA}.base (v) VALUES ('SLOW')")
        _sql(f"INSERT INTO {SCHEMA}.base (v) VALUES ('fast1'), ('fast2')")
        assert _run(isolated, "v_base", dst, **kw).success
        slow.commit()
    finally:
        slow.close()
    third = _run(isolated, "v_base", dst, **kw)
    # The run's only write is the recovered row, outside the cursor window the
    # ladder grades: graded on Gate-8's count and digests, never as an empty
    # (failed) digest — which left the route stuck on the continuity check.
    assert third.success, third.error
    assert (third.reconciliation or {}).get("ladder_declined_reason") == (
        "only_rows_recovered_behind_the_watermark"
    ), third.reconciliation
    evidence = _window(third)
    assert evidence.get("strategy") == csw.STRATEGY_OVERLAP, evidence
    assert evidence.get("guarantee") == csw.GUARANTEE_WITHIN_WINDOW, evidence
    assert int(evidence.get("rows_recovered") or 0) == 1, evidence
    missing = _sql(
        f"SELECT s.id, s.v FROM {SCHEMA}.base s LEFT JOIN {SCHEMA}.{dst} d ON d.id = s.id "
        "WHERE d.id IS NULL", fetch=True,
    )
    assert missing == []
    fourth = _run(isolated, "v_base", dst, **kw)
    assert fourth.success, fourth.error
    assert int(fourth.records_transferred) == 0
    dupes = _sql(f"SELECT id FROM {SCHEMA}.{dst} GROUP BY id HAVING count(*) > 1", fetch=True)
    assert dupes == []


def test_a_failed_late_commit_probe_is_a_gate8_warning(isolated, pg, monkeypatch):
    import src.transfer.stream_commit_window as scw

    _sql(f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, v text)")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'b' || g FROM generate_series(1, 5) g")
    dst = "pfail_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append")
    assert _run(isolated, "ev", dst, **kw).success

    def boom(*_a, **_k):
        raise RuntimeError("probe connection reset")

    monkeypatch.setattr(scw, "probe_pg_late_rows", boom)
    _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('new')")
    second = _run(isolated, "ev", dst, **kw)
    evidence = _window(second)
    assert evidence.get("guarantee") == csw.GUARANTEE_NONE, evidence
    assert "RuntimeError" in str(evidence.get("probe_error")), evidence
    warnings = " ".join((second.reconciliation or {}).get("warnings") or [])
    assert "not guaranteed" in warnings.lower() and "RuntimeError" in warnings, second.reconciliation


def test_a_failed_window_plan_is_a_gate8_warning(isolated, pg, monkeypatch):
    import src.transfer.stream_commit_window as scw

    _sql(f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, v text)")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'b' || g FROM generate_series(1, 5) g")
    dst = "plfail_" + uuid.uuid4().hex[:6]

    def boom(*_a, **_k):
        raise RuntimeError("catalog unreadable")

    monkeypatch.setattr(scw, "plan_commit_safe_window", boom)
    first = _run(isolated, "ev", dst, cursor="id", mode="incremental_append")
    assert first.success, first.error
    evidence = _window(first)
    assert evidence.get("guarantee") == csw.GUARANTEE_NONE, evidence
    assert "RuntimeError" in str(evidence.get("probe_error")), evidence
    warnings = " ".join((first.reconciliation or {}).get("warnings") or [])
    assert "RuntimeError" in warnings, first.reconciliation


# --------------------------------------------------------------------------
# H3 — rows that are not late commits must not trigger a re-read
# --------------------------------------------------------------------------


def test_late_row_rule_narrows_only_on_a_complete_subtransaction_free_snapshot():
    full = {"xmin": "100", "xmax": "110", "xip": ["100", "104"], "xip_complete": True,
            "open_subxacts": 0}
    assert csw.late_row_rule(full) == ("in_progress", 110, (100, 104))
    # An open subtransaction at capture: its xid is never in xip, so only the
    # horizon is safe.
    assert csw.late_row_rule({**full, "open_subxacts": 1}) == ("horizon", 100, ())
    assert csw.late_row_rule({**full, "open_subxacts": None}) == ("horizon", 100, ())
    # The in-progress list was too large to store.
    assert csw.late_row_rule({**full, "xip": [], "xip_complete": False}) == ("horizon", 100, ())
    # A bound stored before this change carries no list.
    assert csw.late_row_rule({"xmin": "100", "xmax": "110"}) == ("horizon", 100, ())


def test_re_read_evidence_is_worded_neutrally():
    window = csw.plan_commit_safe_window(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        cursor_type="BIGINT", stored_watermark="13",
        metadata={"commit_horizon": {"strategy": csw.STRATEGY_PG, "xmin": "900"}},
        can_replay=True, contract={},
        capture_bound=lambda: {"strategy": csw.STRATEGY_PG, "xmin": "950", "xmax": "955"},
        probe_late=lambda **_: csw.LateRows(count=3, min_cursor="11", undelivered=1, checked=3),
    )
    assert window.replays and window.seed == "11"
    assert "changed or committed behind the watermark" in window.reason
    assert "not finished at the last read" not in window.reason
    assert window.evidence()["late_rows_checked"] == 3


def _wide_table(n: int) -> None:
    _sql(f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, v text)")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'r' || g FROM generate_series(1, {n}) g")
    _sql(f"ANALYZE {SCHEMA}.ev")


def test_many_in_place_updates_do_not_re_read_the_table(isolated, pg):
    """More in-place updates than the old 5,000-candidate cap: every candidate
    was then counted undelivered and the run re-read the whole table."""
    _wide_table(12_000)
    dst = "upd_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append")
    assert _run(isolated, "ev", dst, **kw).success
    _sql(f"UPDATE {SCHEMA}.ev SET v = v || '!' WHERE id <= 6000")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('new')")
    second = _run(isolated, "ev", dst, **kw)
    assert second.success, second.error
    assert int(second.records_transferred) == 1
    evidence = _window(second)
    assert int(evidence.get("late_rows_detected") or 0) == 6000, evidence
    assert evidence.get("late_rows_undelivered") == 0, evidence
    assert evidence.get("replay_seed") is None and "rows_replayed" not in evidence, evidence
    assert "changed or committed behind the watermark" in str(evidence.get("reason"))
    total = _sql(f"SELECT count(*), count(DISTINCT id) FROM {SCHEMA}.{dst}", fetch=True)[0]
    assert total == (12_001, 12_001)


def test_idle_writer_on_another_table_does_not_grow_the_re_read(isolated, pg):
    """An idle-in-transaction session holding an xid pins the snapshot xmin.
    Judged against xmin alone, every row delivered since it opened was reported
    again each run — 2,999 → 5,999 → 8,999 — and re-read past the cap."""
    _wide_table(1_000)
    _sql(f"CREATE TABLE {SCHEMA}.other (x int)")
    dst = "idle_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append")
    assert _run(isolated, "ev", dst, **kw).success
    idle = _connect(autocommit=False)
    try:
        idle.cursor().execute(f"INSERT INTO {SCHEMA}.other VALUES (1)")  # holds an xid
        judged = []
        for _ in range(4):
            _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'n' || g FROM generate_series(1, 2000) g")
            result = _run(isolated, "ev", dst, **kw)
            assert result.success, result.error
            assert int(result.records_transferred) == 2000
            evidence = _window(result)
            # Never a re-read: rows delivered since the idle writer opened are
            # either not reported (judged on the in-progress list) or reported
            # and found at the destination (the horizon rule, when a session on
            # this shared server holds a savepoint at capture).
            assert evidence.get("replay_seed") is None, evidence
            assert "rows_replayed" not in evidence, evidence
            judged.append((evidence.get("late_row_rule"), int(evidence.get("late_rows_detected") or 0)))
        assert all(n == 0 for rule, n in judged if rule == "in_progress"), judged
        assert any(rule == "in_progress" for rule, _n in judged), judged
    finally:
        idle.rollback()
        idle.close()
    total = _sql(f"SELECT count(*), count(DISTINCT id) FROM {SCHEMA}.{dst}", fetch=True)[0]
    assert total == (9_000, 9_000)


def test_idle_writer_does_not_hide_a_real_late_commit(isolated, pg):
    """The narrowed rule still finds the race the window exists for."""
    _wide_table(10)
    _sql(f"CREATE TABLE {SCHEMA}.other (x int)")
    dst = "idlerace_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append")
    idle = _connect(autocommit=False)
    try:
        idle.cursor().execute(f"INSERT INTO {SCHEMA}.other VALUES (1)")
        assert _run(isolated, "ev", dst, **kw).success
        slow = _connect(autocommit=False)
        try:
            slow.cursor().execute(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('SLOW')")
            _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('fast1'), ('fast2')")
            assert _run(isolated, "ev", dst, **kw).success
            slow.commit()
        finally:
            slow.close()
        third = _run(isolated, "ev", dst, **kw)
        assert third.success, third.error
        assert int(_window(third).get("rows_recovered") or 0) == 1, _window(third)
    finally:
        idle.rollback()
        idle.close()
    missing = _sql(
        f"SELECT s.id FROM {SCHEMA}.ev s LEFT JOIN {SCHEMA}.{dst} d ON d.id = s.id "
        "WHERE d.id IS NULL", fetch=True,
    )
    assert missing == []


# --------------------------------------------------------------------------
# M2 — wall-clock text in more than one layout, on live PostgreSQL
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "batches",
    [
        (["2024-01-05T12:00:00"], ["2024-01-05 13:00:00"], ["2024-01-06T08:00:00"]),
        (["31/12/2023"], ["13/01/2024"], ["14/01/2024"]),
    ],
    ids=["iso-T-vs-space", "dd-mm-yyyy"],
)
def test_text_cursor_in_a_non_iso_or_mixed_layout_is_refused_before_any_write(
    isolated, pg, batches
):
    """Text order is not time order for these: they were accepted, a later run
    then failed after the read ("Append refused before insert") and kept failing
    — or skipped a row while reporting success."""
    _sql(f"CREATE TABLE {SCHEMA}.tt (id serial PRIMARY KEY, c text NOT NULL)")
    dst = "tt_" + uuid.uuid4().hex[:6]
    conn = _connect()
    refused = []
    try:
        for batch in batches:
            with conn.cursor() as cur:
                for value in batch:
                    cur.execute(f"INSERT INTO {SCHEMA}.tt (c) VALUES (%s)", (value,))
            result = _run(isolated, "tt", dst, cursor="c", mode="incremental_append")
            error = str(result.error or "")
            assert "Append refused before insert" not in error, error
            if result.success:
                missing = _sql(
                    f"SELECT s.id FROM {SCHEMA}.tt s LEFT JOIN {SCHEMA}.{dst} d ON d.id = s.id "
                    "WHERE d.id IS NULL", fetch=True,
                )
                assert missing == [], "a green run left a row behind the watermark"
            else:
                assert "Cursor 'c' is a text column" in error, error
                assert int(result.records_transferred or 0) == 0
                refused.append(error)
    finally:
        conn.close()
    assert refused, "the layout was never refused"


def test_preflight_refuses_a_day_month_text_cursor(pg):
    from services.null_cursor_probe import build_cursor_value_gates
    from services.sync_cursor import IncrementalReadScope

    _sql(f"CREATE TABLE {SCHEMA}.tt (id serial PRIMARY KEY, c text NOT NULL)")
    _sql(f"INSERT INTO {SCHEMA}.tt (c) VALUES ('31/12/2023'), ('13/01/2024')")
    c = pg_creds()
    gate, blockers = build_cursor_value_gates(
        read_scope=IncrementalReadScope(cursor_column="c", watermark=None, cursor_key="k"),
        sync_mode="incremental_append", validation_mode="strict",
        column_nullability={"c": False}, column_types={"c": "text"},
        source_connector_id="",
        source_config={"type": "postgresql", "host": c["host"], "port": int(c["port"]),
                       "database": c["database"], "username": c["username"],
                       "password": c["password"], "schema": SCHEMA},
        source_table="tt",
    )
    assert gate and gate["id"] == "g_cursor_order" and gate["status"] == "block", gate
    assert "day" in gate["message"], gate["message"]


# --------------------------------------------------------------------------
# M5 — cost: never-analysed tables, statement timeouts, the NOT LIKE scan
# --------------------------------------------------------------------------


def test_a_never_analysed_table_is_probed_with_the_floor(isolated, pg):
    """reltuples is -1 until a table is first analysed (and on a partitioned
    parent): it was read as "small" and the probe scanned without a floor."""
    _sql(
        f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, v text) "
        "WITH (autovacuum_enabled = false)"
    )
    _sql(f"INSERT INTO {SCHEMA}.ev (v) SELECT 'r' || g FROM generate_series(1, 20) g")
    assert _sql(
        f"SELECT reltuples FROM pg_class WHERE oid = '{SCHEMA}.ev'::regclass", fetch=True
    )[0][0] < 0
    dst = "unan_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="id", mode="incremental_append")
    assert _run(isolated, "ev", dst, **kw).success
    _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('new')")
    second = _run(isolated, "ev", dst, **kw)
    assert second.success, second.error
    evidence = _window(second)
    assert "probe looks back to cursor" in str(evidence.get("reason")), evidence
    assert evidence.get("guarantee") == csw.GUARANTEE_WITHIN_WINDOW, evidence


def test_every_probe_statement_runs_under_a_timeout(pg, monkeypatch):
    """A probe blocked by a lock (or a slow scan) gives up after the configured
    timeout instead of holding the run; the caller records it as a failure."""
    import threading
    import time

    import psycopg2

    _sql(f"CREATE TABLE {SCHEMA}.ev (id bigserial PRIMARY KEY, v text)")
    _sql(f"INSERT INTO {SCHEMA}.ev (v) VALUES ('a'), ('b')")
    monkeypatch.setenv(csw.PROBE_TIMEOUT_ENV, "400")
    holder = _connect(autocommit=False)
    holder.cursor().execute(f"LOCK TABLE {SCHEMA}.ev IN ACCESS EXCLUSIVE MODE")
    release = threading.Timer(6.0, holder.rollback)
    release.start()
    c = pg_creds()
    src = {"host": c["host"], "port": int(c["port"]), "database": c["database"],
           "username": c["username"], "password": c["password"]}
    started = time.monotonic()
    try:
        with pytest.raises(psycopg2.errors.QueryCanceled):
            csw.probe_pg_late_rows(
                src, schema=SCHEMA, table="ev", cursor_column="id", since_xmin="3",
                below="2", cursor_type="BIGINT",
            )
    finally:
        release.cancel()
        holder.rollback()
        holder.close()
    assert time.monotonic() - started < 4.0


def _not_like_spy():
    """``(statements seen, listener)`` for every NOT LIKE statement any engine runs."""
    import sqlalchemy as sa

    seen: list[str] = []

    def spy(conn, cursor, statement, params, context, executemany):
        # The cursor-shape scan, not the engine's own catalog reflection.
        if "NOT LIKE" in statement.upper() and "sqlite_master" not in statement:
            seen.append(statement)

    sa.event.listen(sa.engine.Engine, "before_cursor_execute", spy)
    return seen, spy


def test_text_shape_scan_runs_on_first_run_then_on_interval(isolated, tmp_path, monkeypatch):
    import sqlite3

    import sqlalchemy as sa

    from services import cursor_order

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT NOT NULL)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"{i:06d}") for i in range(1, 31)])

    def run():
        contract = {"name": "t", "cursor_field": "code", "sync_mode": "incremental_append",
                    "selected": True}
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="incremental_append", stream_contracts=[contract],
            skip_preflight=True, validation_mode="strict",
        )
        job_id = "csr" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        return result

    seen, spy = _not_like_spy()
    try:
        run()
        assert len(seen) == 1, "the first run of a route confirms the shape on every row"
        with sqlite3.connect(src) as conn:
            conn.execute("INSERT INTO t VALUES (31, '000031')")
        run()
        run()
        assert len(seen) == 1, "quiet runs reuse the confirmed shape"
        monkeypatch.setenv(cursor_order.SHAPE_CHECK_INTERVAL_ENV, "0")
        run()
        assert len(seen) == 2, "a due interval confirms it again"
    finally:
        sa.event.remove(sa.engine.Engine, "before_cursor_execute", spy)
