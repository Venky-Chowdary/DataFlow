"""Final re-review follow-ups (1-5) of the Oracle ``''`` gate."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pytest

from services import oracle_empty_not_null as oe

_MAPS = [{"source": c, "target": c, "confidence": 0.99, "target_type": "TEXT"} for c in ("id", "note")]


@pytest.fixture(autouse=True)
def _fresh_measurements(monkeypatch):
    import uuid as _uuid

    monkeypatch.setattr(oe, "_MEASURED", {})
    monkeypatch.setattr(oe, "_COOLDOWN_SCOPE", _uuid.uuid4().hex)


@pytest.fixture
def sqlite_source(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, note TEXT NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", "x"), ("b", "")])
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


def _ctx(cfg, **kw):
    ctx = dict(
        destination_db_type="oracle", write_maps=[dict(m) for m in _MAPS],
        column_types={"id": "TEXT", "note": "TEXT"}, column_nullability={"id": False, "note": False},
        dest_table_exists=False, destination_column_nullability={}, sample_rows=[],
        source_kind="database", source_format="sqlite", source_config=cfg, source_table="t",
        destination_table="DEST", sync_mode="full_refresh_overwrite",
    )
    ctx.update(kw)
    return ctx


def _ids(blockers):
    return [a["id"] for b in blockers for a in b.get("suggested_actions") or []]


# ------------------------------------------------------------------------ 1

def test_1_only_types_that_provably_cannot_be_empty_are_answered_unseen():
    import sqlalchemy as sa
    from sqlalchemy.dialects import mssql, mysql, postgresql

    for never in (sa.Integer(), sa.Numeric(10, 2), sa.Float(), sa.Date(), sa.DateTime(), sa.Time(),
                  sa.Boolean(), postgresql.UUID(), sa.Interval()):
        assert oe.provably_never_empty(never), never
    for maybe in (postgresql.ENUM("", "ok", name="mood"), postgresql.TSVECTOR(), mssql.SQL_VARIANT(),
                  mysql.ENUM("", "a"), mysql.SET("a", "b"), postgresql.CITEXT(), sa.CHAR(5),
                  postgresql.JSONB(), sa.LargeBinary(), sa.types.NullType()):
        assert not oe.provably_never_empty(maybe), maybe


def test_1_an_oracle_source_clob_is_walked_because_empty_clob_reads_as_empty_text():
    import sqlalchemy as sa

    assert oe.oracle_source_never_empty(sa.VARCHAR(30)) and oe.oracle_source_never_empty(sa.CHAR(3))
    assert not oe.oracle_source_never_empty(sa.CLOB()) and not oe.oracle_source_never_empty(sa.Text())


def test_1_postgresql_enum_label_and_tsvector_can_be_empty_and_are_caught_live():
    import uuid

    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    import psycopg2

    c = pg_creds()
    cfg = {"type": "postgresql", "host": c["host"], "port": c["port"], "database": c["database"],
           "username": c["username"], "password": c["password"]}
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
                            password=c["password"])
    conn.autocommit = True
    t = f"r3e_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TYPE {t}_m AS ENUM ('', 'ok')")
        cur.execute(f'CREATE TABLE "{t}" (m {t}_m NOT NULL, tv tsvector NOT NULL, note text NOT NULL)')
        cur.execute(f"""INSERT INTO "{t}" VALUES ('', ''::tsvector, 'x'), ('ok', 'a b'::tsvector, 'y')""")
    try:
        maps = [{"source": c_, "target": c_.upper(), "target_type": "TEXT"} for c_ in ("m", "tv", "note")]
        gate, _ = oe.preflight_hook({
            "destination_db_type": "oracle", "write_maps": maps,
            "column_types": {"m": "USER-DEFINED", "tv": "tsvector", "note": "text"},
            "column_nullability": {"m": False, "tv": False, "note": False}, "dest_table_exists": False,
            "source_kind": "database", "source_format": "postgresql", "source_config": cfg,
            "source_table": t, "destination_table": "D", "sync_mode": "full_refresh_overwrite",
        })
        by = {col["source"]: col for col in gate["details"]["columns"]}
        assert set(by) == {"m", "tv"}, gate["message"]
        assert by["m"]["count"] == 1 and by["tv"]["count"] == 1 and by["m"]["exact"]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{t}"')
            cur.execute(f"DROP TYPE IF EXISTS {t}_m")
        conn.close()


def test_1_a_file_column_inferred_as_a_number_with_empty_cells_is_caught_by_the_existing_gates():
    from services.preflight_service import run_file_preflight

    rows = [{"id": "1", "amount": "10"}, {"id": "2", "amount": ""}]
    maps = [{"source": "id", "target": "ID", "confidence": 0.99, "target_type": "NUMBER"},
            {"source": "amount", "target": "AMOUNT", "confidence": 0.99, "target_type": "NUMBER"}]
    common = dict(columns=["id", "amount"], column_types={"id": "integer", "amount": "integer"}, row_count=2,
                  mappings=maps, destination_connected=True, sample_rows=rows, population_rows=rows,
                  rows_are_population=True, destination_can_create=True, destination_db_type="oracle",
                  source_kind="file", source_format="csv", sync_mode="append")
    existing = run_file_preflight(destination_table_exists=True,
                                  destination_column_types={"ID": "NUMBER", "AMOUNT": "NUMBER"},
                                  destination_column_nullability={"ID": False, "AMOUNT": False}, **common)
    assert existing["passed"] is False
    assert any("amount" in str(b.get("message")) for b in existing["blockers"])
    # A new table from a file carries no NOT NULL (no source catalog), so the
    # empty cell lands as NULL in a NULL-able column: nothing to refuse.
    assert oe.candidates(destination_db_type="oracle", mappings=[{"source": "amount", "target": "A",
                         "target_type": "TEXT"}], column_nullability={}, destination_table_exists=False,
                         destination_column_nullability={}, column_types={"amount": "integer"}) == []
    # And an inferred number is never trusted to be non-text.
    assert oe.candidates(destination_db_type="oracle", mappings=[{"source": "amount", "target": "A",
                         "target_type": "TEXT"}], column_nullability={"amount": False},
                         destination_table_exists=False, destination_column_nullability={},
                         column_types={"amount": "integer"})


# ------------------------------------------------------------------------ 2

def test_2_one_total_budget_per_check_and_what_it_does_not_reach_blocks_with_check_now():
    maps = [{"source": f"c{i}", "target": f"C{i}", "target_type": "TEXT"} for i in range(10)]
    targets = oe.candidates(destination_db_type="oracle", mappings=maps,
                            column_nullability={f"c{i}": False for i in range(10)},
                            destination_table_exists=False, destination_column_nullability={})
    asked = []

    def slow(col, now, seconds, count):
        asked.append((col, round(seconds, 2)))
        time.sleep(0.05)
        return {"state": "zero", "count": 0}

    # c7 is empty in the preview, so it is asked first.
    measured = oe.measure(targets, probe=slow, whole_scope=True, budget_seconds=0.12,
                          sample_rows=[{"c7": ""}], walk=lambda cols: pytest.fail("walked past the budget"))
    assert asked[0][0] == "c7"
    assert all(s <= 0.12 for _c, s in asked)  # each statement gets what is left, never a fresh bound
    late = [t for t in measured if t.get("why") == "budget"]
    assert late and len(asked) + len(late) == 10
    gate, blockers = oe.build_gate(measured, mappings=maps)
    assert "budget ran out before this column was reached" in gate["message"]
    check = next(a for b in blockers for a in b["suggested_actions"] if a["id"] == oe.CHECK_OPTION)
    assert check["budget_seconds"] == oe.CHECK_NOW_SECONDS and "5 minutes for this check" in check["label"]


# ------------------------------------------------------------------------ 3

def test_3_execute_reuse_is_bound_to_the_destination_and_the_workspace(sqlite_source, monkeypatch):
    calls = []
    real = oe.sql_empty_probe

    def counting(**kw):
        calls.append(kw["column"])
        return real(**kw)

    monkeypatch.setattr(oe, "sql_empty_probe", counting)
    oe.preflight_hook(_ctx(sqlite_source))
    n = len(calls)
    oe.preflight_hook(_ctx(sqlite_source, skip_population_fit=True, destination_table="OTHER"))
    assert len(calls) == 2 * n  # another destination table: measured again
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-b")
    oe.preflight_hook(_ctx(sqlite_source, skip_population_fit=True))
    assert len(calls) == 3 * n  # another workspace: measured again


def test_3_the_kept_measurements_are_bounded():
    monkeypatch_max = 5
    old = oe._MEASURED_MAX
    oe._MEASURED_MAX = monkeypatch_max
    try:
        for i in range(20):
            oe._remember(f"k{i}", {"count": 0, "exact": True}, checked_now=False)
        assert len(oe._MEASURED) == monkeypatch_max and "k19" in oe._MEASURED and "k0" not in oe._MEASURED
    finally:
        oe._MEASURED_MAX = old


# ------------------------------------------------------------------------ 4

def test_4_check_now_records_the_signed_in_user_and_refuses_without_one(monkeypatch):
    from services import workspace_filter
    from src.services import auth_service

    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "alice@example.com")
    out = oe.stamp_check_now([dict(m) for m in _MAPS], {"option": oe.CHECK_OPTION, "columns": ["id"],
                                                        "approved_by": "anyone"})
    assert out[0][oe.CHECK_KEY]["requested_by"] == "alice@example.com"
    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "")
    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    with pytest.raises(ValueError, match="needs a signed-in user"):
        oe.stamp_check_now([dict(m) for m in _MAPS], {"option": oe.CHECK_OPTION, "columns": ["id"]})


def test_4_an_expired_check_now_is_not_honoured():
    m = oe.stamp_check_now([dict(_MAPS[0])], {"option": oe.CHECK_OPTION, "columns": ["id"]})[0]
    assert oe._check_now_asked(m)
    stale = dict(m)
    stale[oe.CHECK_KEY] = {**m[oe.CHECK_KEY],
                           "expires_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}
    assert not oe._check_now_asked(stale)


def test_4_a_billed_warehouse_is_scanned_once_on_request_and_its_result_reused(monkeypatch):
    from services import preflight_population

    walks = []

    def walk(**kw):
        walks.append(kw["source_columns"])
        return iter([{"id": "a", "note": "x"}, {"id": "b", "note": "y"}])

    monkeypatch.setattr(preflight_population, "_iter_table_population_for_preflight", walk)
    cfg = {"type": "bigquery", "project": "p", "dataset": "d"}
    unasked, _ = oe.preflight_hook(_ctx(cfg, source_format="bigquery"))
    assert unasked["status"] == "block" and walks == []
    maps = oe.stamp_check_now([dict(m) for m in _MAPS], {"option": oe.CHECK_OPTION, "columns": ["id", "note"]},
                              table="DEST")
    asked, _ = oe.preflight_hook(_ctx(cfg, source_format="bigquery", write_maps=maps))
    assert asked is None and len(walks) == 1  # read once, nothing empty
    # A later Validate (a schedule beat) without the request reuses that result: no
    # second scan, and the gate says so (round 4: never a silent reuse).
    later, later_blockers = oe.preflight_hook(_ctx(cfg, source_format="bigquery"))
    assert later["status"] == "warn" and later_blockers == [] and len(walks) == 1
    assert "being reused" in later["message"]


# ------------------------------------------------------------------------ 5

def test_5_no_action_that_does_nothing_is_offered():
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=[dict(_MAPS[1])], column_nullability={},
        destination_table_exists=True, destination_column_nullability={"note": False},
        column_types={}, key_columns=[], sample_rows=[], walk=lambda cols: iter([{"note": ""}]),
    )
    labels = " ".join(a["label"] for b in blockers for a in b["suggested_actions"])
    assert "DEFAULT on the Oracle table" not in labels and "oracle_not_null_fill" not in _ids(blockers)


def test_5_an_unreadable_catalog_offers_fixing_the_connection_not_check_now():
    ctx = _ctx({"type": "postgresql", "host": "127.0.0.1", "port": 1, "database": "x", "username": "u",
                "password": "p"}, source_format="postgresql", column_nullability={})
    gate, blockers = oe.preflight_hook(ctx)
    ids = _ids(blockers)
    assert oe.CHECK_OPTION not in ids and "oracle_fix_connection" in ids
    fix = next(a for b in blockers for a in b["suggested_actions"] if a["id"] == "oracle_fix_connection")
    assert "Connection refused" in fix["label"] or "refused" in fix["label"].lower()
