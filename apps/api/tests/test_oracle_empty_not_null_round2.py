"""Re-review (REVIEW_ORACLE.md "Round 2", A-H) of the Oracle ``''`` gate.

Principle (owner): the gate never dead-ends a legitimate run. Every block
names the column, says what is known (an exact count, "at least one", or why
it could not be counted) and offers at least one action that works on that
source. SQLite stands in for a SQL source where a live engine is not needed;
the PostgreSQL and MySQL cases need those engines and skip without them.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

import pytest

from services import oracle_empty_not_null as oe

_MAPS = [{"source": c, "target": c, "confidence": 0.99, "target_type": "TEXT"} for c in ("id", "note")]


@pytest.fixture
def sqlite_source(tmp_path, monkeypatch):
    """``t``: text key ``id`` ('' once) and ``note TEXT NOT NULL`` ('' twice)."""
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, note TEXT NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", ""), ("b", "x"), ("", ""), ("c", "  ")])
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


def _ctx(cfg, **kw):
    ctx = dict(
        destination_db_type="oracle", write_maps=[dict(m) for m in _MAPS],
        column_types={"id": "TEXT", "note": "TEXT"}, column_nullability={"id": False, "note": False},
        dest_table_exists=False, destination_column_nullability={}, sample_rows=[{"id": "b", "note": "x"}],
        source_kind="database", source_format="sqlite", source_config=cfg, source_table="t",
        destination_table="DEST", sync_mode="full_refresh_overwrite",
    )
    ctx.update(kw)
    return ctx


def _ids(blockers):
    return [a["id"] for b in blockers for a in b.get("suggested_actions") or []]


# ------------------------------------------------------------------------ A

def test_a_a_key_column_with_empty_strings_has_working_actions(sqlite_source):
    gate, blockers = oe.preflight_hook(_ctx(sqlite_source))
    by = {c["target"]: c for c in gate["details"]["columns"]}
    assert by["id"]["approvable"] is False and by["id"]["count"] == 1
    actions = [a for a in blockers[0]["suggested_actions"] if a.get("column") == "id"]
    assert {a["id"] for a in actions} >= {"oracle_default_transform", "oracle_fix_source_values"}


def test_a_the_default_transform_clears_a_key_column_without_reading_the_source(sqlite_source, monkeypatch):
    maps = [dict(m) for m in _MAPS]
    maps[0]["transform"] = "default:UNKNOWN"
    monkeypatch.setattr(oe, "sql_empty_probe", lambda **kw: pytest.fail("asked the source")
                        if kw["column"] == "id" else {"state": "zero", "count": 0})
    gate, blockers = oe.preflight_hook(_ctx(sqlite_source, write_maps=maps))
    assert gate is None and blockers == []
    assert oe.written_value_is_never_empty("default:UNKNOWN")
    assert not oe.written_value_is_never_empty("default:") and not oe.written_value_is_never_empty("trim")


def test_a_a_key_column_that_could_not_be_checked_offers_check_now_and_it_works(monkeypatch):
    monkeypatch.setattr(oe, "SCAN_MAX_ROWS", 2)
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": ""}]
    args = dict(destination_db_type="oracle", column_nullability={"id": False},
                destination_table_exists=False, destination_column_nullability={}, column_types={},
                key_columns=["id"], sample_rows=[], walk=lambda cols: iter(rows))
    maps = [dict(_MAPS[0])]
    gate, blockers = oe.preflight_gate(mappings=maps, **args)
    assert "could not rule out empty strings in id, because the scan stopped after 2 rows" in gate["message"]
    assert oe.CHECK_OPTION in _ids(blockers) and "oracle_default_transform" in _ids(blockers)
    checked = oe.stamp_check_now(maps, {"option": oe.CHECK_OPTION, "columns": ["id"]})
    gate, _ = oe.preflight_gate(mappings=checked, check_now=["id"], **args)
    (col,) = gate["details"]["columns"]
    assert col["exact"] and col["count"] == 1  # the asked-for scan reads to the end


# ------------------------------------------------------------------------ B

def test_b_a_column_that_cannot_hold_text_is_not_a_candidate():
    maps = [{"source": c, "target": c, "target_type": "TEXT"} for c in ("u", "j", "a", "d", "note")]
    found = oe.candidates(
        destination_db_type="oracle", mappings=maps,
        column_nullability={c: False for c in ("u", "j", "a", "d", "note")},
        destination_table_exists=False, destination_column_nullability={},
        column_types={"u": "uuid", "j": "integer", "a": "boolean", "d": "date", "note": "text"},
        native_source_types=True,
    )
    assert [t["source"] for t in found] == ["note"]


def test_b_one_column_the_source_cannot_answer_for_does_not_disable_the_others():
    targets = oe.candidates(
        destination_db_type="oracle", mappings=[dict(m) for m in _MAPS],
        column_nullability={"id": False, "note": False}, destination_table_exists=False,
        destination_column_nullability={},
    )
    answers = {"id": None, "note": {"state": "found", "count": 3}}
    measured = oe.measure(targets, probe=lambda col, now, secs, count: answers[col], whole_scope=True,
                          walk=lambda cols: iter([{"id": "x"}]) if cols == ["id"] else pytest.fail(cols))
    by = {t["source"]: t for t in measured}
    assert by["note"]["how"] == "pushdown" and by["note"]["count"] == 3
    assert by["id"]["how"] == "population" and by["id"]["exact"]


@pytest.fixture
def pg_conn():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    import psycopg2

    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
                            password=c["password"])
    conn.autocommit = True
    yield conn, {"type": "postgresql", "host": c["host"], "port": c["port"], "database": c["database"],
                 "username": c["username"], "password": c["password"]}
    conn.close()


def test_b_c_postgresql_enum_uuid_json_array_date_and_char_padding_live(pg_conn):
    import uuid

    conn, cfg = pg_conn
    t = f"r2b_{uuid.uuid4().hex[:8]}"
    mood = f"{t}_mood"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TYPE {mood} AS ENUM ('ok', 'sad')")
        cur.execute(
            f'CREATE TABLE "{t}" (m {mood} NOT NULL, u uuid NOT NULL, j jsonb NOT NULL, '
            f"a text[] NOT NULL, d date NOT NULL, c char(5) NOT NULL, note text NOT NULL)"
        )
        cur.execute(f"""INSERT INTO "{t}" VALUES ('ok', gen_random_uuid(), '""', '{{}}', now(), '', ''),
                        ('sad', gen_random_uuid(), '{{}}', '{{""}}', now(), '   ', 'x')""")
    try:
        got = {
            col: (oe.sql_empty_probe(source_connector_id="", source_config=cfg, source_table=t, column=col)
                  or {}).get("state")
            for col in ("m", "u", "j", "a", "d", "c", "note")
        }
        # uuid and date cannot hold ''; a blank-padded char(5) is read as spaces,
        # never ''; enum, jsonb and arrays are not answered "zero" unseen: the
        # predicate does not apply to them, so the bounded walk decides (None).
        assert got == {"m": None, "u": "zero", "j": None, "a": None, "d": "zero",
                       "c": "zero", "note": "found"}, got
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{t}"')
            cur.execute(f"DROP TYPE IF EXISTS {mood}")


# ------------------------------------------------------------------------ C

def test_c_mysql_pad_space_does_not_count_trailing_blanks_live():
    import uuid

    from tests.helpers.live_env import mysql_creds, mysql_up

    if not mysql_up():
        pytest.skip("MySQL not reachable")
    import pymysql

    c = mysql_creds()
    cfg = {"type": "mysql", "host": c["host"], "port": int(c["port"]), "database": c["database"],
           "username": c["username"], "password": c["password"]}
    conn = pymysql.connect(host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
                           database=c["database"], autocommit=True)
    t = f"r2c_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{t}` (v VARCHAR(10) NOT NULL, w VARCHAR(10) NOT NULL)")
            cur.execute(f"INSERT INTO `{t}` VALUES ('  ', ''), ('x', 'y')")
        probe = lambda col: oe.sql_empty_probe(source_connector_id="", source_config=cfg,  # noqa: E731
                                              source_table=t, column=col)
        assert probe("v") == {"state": "zero", "count": 0}  # '  ' = '' under PAD SPACE, not empty
        assert probe("w") == {"state": "found", "count": None}  # exists
        assert oe.sql_empty_probe(source_connector_id="", source_config=cfg, source_table=t, column="w",
                                  count=True) == {"state": "found", "count": 1}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        conn.close()


def test_c_the_sql_server_predicate_counts_trailing_blanks_and_an_oracle_source_holds_no_empty_string():
    import sqlalchemy as sa
    from sqlalchemy.dialects import mssql, postgresql

    col = sa.column("v")
    assert "datalength(v) = " in str(oe._empty_predicate(col, "mssql").compile(dialect=mssql.dialect()))
    assert "octet_length(v) = " in str(oe._empty_predicate(col, "postgresql").compile(dialect=postgresql.dialect()))
    # An Oracle VARCHAR2 cannot be read as '' (zero-length is NULL); a CLOB can
    # (EMPTY_CLOB() is an empty LOB, not NULL), so it is walked.
    assert oe.oracle_source_never_empty(sa.VARCHAR(20))
    assert not oe.oracle_source_never_empty(sa.CLOB())


# ------------------------------------------------------------------------ D

def test_d_no_server_side_timeout_means_no_push_down(sqlite_source, monkeypatch):
    from services import null_marker_probe

    @contextmanager
    def unenforced(conn, db_type, seconds):
        yield False

    monkeypatch.setattr(null_marker_probe, "statement_timeout", unenforced)
    assert oe.sql_empty_probe(source_connector_id="", source_config=sqlite_source,
                              source_table="t", column="note") is None


def test_d_a_billed_warehouse_is_never_scanned_unasked(monkeypatch):
    from services import preflight_population

    monkeypatch.setattr(preflight_population, "_iter_table_population_for_preflight",
                        lambda **kw: pytest.fail("walked a billed warehouse"))
    ctx = _ctx({"type": "bigquery", "project": "p", "dataset": "d"}, source_format="bigquery")
    gate, blockers = oe.preflight_hook(ctx)
    assert gate["status"] == "block"
    assert "which bigquery bills; Datawrap does not do that unasked" in gate["message"]
    check = next(a for a in blockers[0]["suggested_actions"] if a["id"] == oe.CHECK_OPTION)
    assert "the warehouse bills the scan" in check["label"] and check["budget_seconds"] == oe.CHECK_NOW_SECONDS


def test_d_execute_reuses_validates_measurement(sqlite_source, monkeypatch):
    calls = []
    real = oe.sql_empty_probe

    def counting(**kw):
        calls.append(kw["column"])
        return real(**kw)

    monkeypatch.setattr(oe, "sql_empty_probe", counting)
    monkeypatch.setattr(oe, "_MEASURED", {})
    first, _ = oe.preflight_hook(_ctx(sqlite_source))
    asked = len(calls)
    assert sorted(set(calls)) == ["id", "note"]
    again, _ = oe.preflight_hook(_ctx(sqlite_source, skip_population_fit=True))
    assert len(calls) == asked  # Execute confirming that Validate: not asked twice
    assert again["message"] == first["message"]
    oe.preflight_hook(_ctx(sqlite_source))  # a fresh Validate asks again
    assert len(calls) == 2 * asked


# ------------------------------------------------------------------------ E

def test_e_an_unreadable_source_catalog_is_could_not_check(monkeypatch):
    ctx = _ctx({"type": "postgresql", "host": "127.0.0.1", "port": 1, "database": "x",
                "username": "u", "password": "p"}, source_format="postgresql", column_nullability={})
    gate, blockers = oe.preflight_hook(ctx)
    assert gate["status"] == "block"
    assert "the source catalog could not be read" in gate["message"]
    assert {c["target"] for c in gate["details"]["columns"]} == {"id", "note"}
    assert oe.OPTION in _ids(blockers)


def test_e_the_callers_nullability_is_reused(sqlite_source, monkeypatch):
    monkeypatch.setattr(oe, "source_catalog_facts", lambda ctx: pytest.fail("read the catalog again"))
    conn = sqlite3.connect(sqlite_source["database"])
    conn.execute("DELETE FROM t WHERE id = '' OR note = ''")
    conn.commit()
    conn.close()
    assert oe.preflight_hook(_ctx(sqlite_source)) == (None, [])


# ------------------------------------------------------- finding 4 (existing table)

def test_existing_not_null_column_that_could_not_be_checked_blocks_with_actions(monkeypatch):
    monkeypatch.setattr(oe, "SCAN_MAX_ROWS", 1)
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=[dict(_MAPS[1])], column_nullability={},
        destination_table_exists=True, destination_column_nullability={"note": False},
        column_types={}, key_columns=[], sample_rows=[], walk=lambda cols: iter([{"note": "x"}] * 3),
    )
    assert gate["status"] == "block"
    assert {"oracle_empty_check_now", "oracle_default_transform"} <= set(_ids(blockers))


# ------------------------------------------------------------------------ H

def test_h_a_malformed_saved_filter_is_could_not_check_not_a_crash(sqlite_source):
    cfg = {**sqlite_source, "source_filter": "{not json"}
    gate, blockers = oe.preflight_hook(_ctx(cfg))
    assert gate["status"] == "block" and blockers
    assert "the row filter is not valid JSON" in gate["message"]


def test_h_any_unexpected_error_is_reported_not_raised(sqlite_source, monkeypatch):
    monkeypatch.setattr(oe, "candidates", lambda **kw: 1 / 0)
    gate, blockers = oe.preflight_hook(_ctx(sqlite_source))
    assert gate["status"] == "block" and "could not check" in gate["message"]
    assert "ZeroDivisionError" in gate["message"]


def test_a_check_now_is_recorded_through_the_route_signing_entry_point():
    from services.risk_acceptance import validate_risk_acceptance
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    acceptance = {"option": oe.CHECK_OPTION, "columns": ["id"], "approved_by": "qa"}
    assert validate_risk_acceptance(acceptance) == acceptance
    out = _sign_required_risk_contracts([dict(m) for m in _MAPS], acceptance, table="T")
    by = {m["target"]: m for m in out}
    assert by["id"][oe.CHECK_KEY]["requested_by"].startswith("local operator") and oe.CHECK_KEY not in by["note"]
    with pytest.raises(ValueError, match="Nothing was recorded"):
        _sign_required_risk_contracts([dict(m) for m in _MAPS], {"option": oe.CHECK_OPTION}, table="T")
