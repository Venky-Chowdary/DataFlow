"""Round-4 review findings of the Oracle ``''`` gate (reproduced on live PostgreSQL 16).

1. A type that cannot hold ``''`` is ruled out from the catalog (no scan), so a
   large table no longer blocks it as "could not rule out"; a type that can
   hold ``''`` is never ruled out by type.
2. The budget is spent fairly: the requested columns first, then the columns
   the preview shows empty, then an equal share each with the leftover rolled
   forward.
3. A Check-now stamp is honoured only when this server issued it (HMAC), for
   the pair, table, workspace and user it names, within the stated window.
4. A billed warehouse's reused Check-now result is stated, with when and how
   to check again.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services import oracle_empty_not_null as oe


@pytest.fixture(autouse=True)
def _fresh_measurements(monkeypatch):
    import uuid as _uuid

    monkeypatch.setattr(oe, "_MEASURED", {})
    monkeypatch.setattr(oe, "_COOLDOWN_SCOPE", _uuid.uuid4().hex)


@pytest.fixture
def pg():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    import psycopg2

    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
                            password=c["password"])
    conn.autocommit = True
    cfg = {"type": "postgresql", "host": c["host"], "port": c["port"], "database": c["database"],
           "username": c["username"], "password": c["password"]}
    dropped: list[str] = []
    yield conn, cfg, dropped
    with conn.cursor() as cur:
        for stmt in reversed(dropped):
            cur.execute(stmt)
    conn.close()


def _hook_ctx(cfg, table, maps, **kw):
    ctx = {
        "destination_db_type": "oracle", "write_maps": maps,
        "column_types": {m["source"]: "text" for m in maps},
        "column_nullability": {m["source"]: False for m in maps}, "dest_table_exists": False,
        "source_kind": "database", "source_format": str(cfg.get("type")), "source_config": cfg,
        "source_table": table, "destination_table": "D", "sync_mode": "full_refresh_overwrite",
    }
    ctx.update(kw)
    return ctx


# ------------------------------------------------------------------------ 1

def test_1_types_that_cannot_hold_empty_are_ruled_out_from_the_catalog_on_a_large_table_live(pg, monkeypatch):
    """Enum without a '' label, jsonb, json, arrays, inet/cidr/macaddr: no scan, no "could not rule out".

    The walk is scaled down (1,000 rows, table 3,000 rows) the way the 1M-row
    bound applies to a large table. Reverse safety on the same table: an enum
    WITH a '' label and an xml column holding '' are still found.
    """
    conn, cfg, dropped = pg
    t = f"r4a_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TYPE {t}_m AS ENUM ('ok', 'sad')")
        cur.execute(f"CREATE TYPE {t}_e AS ENUM ('', 'ok')")
        dropped += [f"DROP TYPE IF EXISTS {t}_m", f"DROP TYPE IF EXISTS {t}_e", f'DROP TABLE IF EXISTS "{t}"']
        cur.execute(
            f'CREATE TABLE "{t}" (m {t}_m NOT NULL, j jsonb NOT NULL, js json NOT NULL, a text[] NOT NULL, '
            f"ia int[] NOT NULL, ip inet NOT NULL, ci cidr NOT NULL, mac macaddr NOT NULL, "
            f"note text NOT NULL, e {t}_e NOT NULL, x xml NOT NULL, tv tsvector NOT NULL)"
        )
        cur.execute(
            f"""INSERT INTO "{t}" SELECT 'ok', '""', '""', '{{}}', '{{}}', '10.0.0.1', '10.0.0.0/8', '08:00:2b:01:02:03', 'n' || i,
                CASE WHEN i = 3000 THEN ''::{t}_e ELSE 'ok'::{t}_e END,
                CASE WHEN i = 1 THEN ''::xml ELSE '<a/>'::xml END,
                CASE WHEN i = 2999 THEN ''::tsvector ELSE 'a b'::tsvector END
                FROM generate_series(1, 3000) i"""
        )
        import psycopg2

        for ruled_out in (f"{t}_m", "jsonb", "json", "inet", "cidr", "macaddr"):  # '' is not valid input
            with pytest.raises(psycopg2.DataError):
                cur.execute(f"SELECT ''::{ruled_out}")
    monkeypatch.setattr(oe, "SCAN_MAX_ROWS", 1000)
    walked: list[list[str]] = []
    real_walk = oe._walk_rows

    def spy(ctx, cols, *a, **k):
        walked.append(list(cols))
        return real_walk(ctx, cols, *a, **k)

    monkeypatch.setattr(oe, "_walk_rows", spy)
    cols = ("m", "j", "js", "a", "ia", "ip", "ci", "mac", "note")
    maps = [{"source": c, "target": c.upper(), "target_type": "TEXT"} for c in cols]
    gate, blockers = oe.preflight_hook(_hook_ctx(cfg, t, maps))
    assert gate is None and blockers == [], gate and gate["message"]
    assert walked == []  # answered from the catalog and the bounded question, never walked

    maps = [{"source": c, "target": c.upper(), "target_type": "TEXT"} for c in ("m", "e", "x", "tv")]
    gate, _ = oe.preflight_hook(_hook_ctx(cfg, t, maps))
    by = {c["source"]: c for c in gate["details"]["columns"]}
    assert set(by) == {"e", "x", "tv"}, gate["message"]
    for c in ("e", "tv"):  # an enum with a '' label and tsvector: asked exactly, past the walk bound
        assert by[c]["found"] and by[c]["count"] == 1 and by[c]["exact"] and by[c]["how"] == "pushdown", by[c]
    # xml holds '' and has no '' predicate: it is walked and never passed (this
    # walker cannot ORDER BY xml, so it blocks as "could not check").
    assert by["x"]["found"] or not by["x"]["exact"], repr(by["x"])


def test_1_a_type_that_can_hold_empty_is_never_ruled_out_by_type():
    import sqlalchemy as sa
    from sqlalchemy.dialects import mssql, mysql, postgresql as pgd

    for col_type, dialect in (
        (pgd.ENUM("ok", "sad", name="m"), "postgresql"), (pgd.JSONB(), "postgresql"),
        (pgd.JSON(), "postgresql"), (pgd.ARRAY(sa.Text()), "postgresql"), (pgd.INET(), "postgresql"),
        (pgd.CIDR(), "postgresql"), (pgd.MACADDR(), "postgresql"), (mysql.JSON(), "mysql"),
        (sa.Integer(), "postgresql"), (sa.Date(), "mysql"), (pgd.UUID(), "postgresql"),
    ):
        assert oe.catalog_rules_out_empty(col_type, dialect), (col_type, dialect)
    for col_type, dialect, kw in (
        (pgd.ENUM("", "ok", name="e"), "postgresql", {}), (pgd.ENUM(name="unknown_labels"), "postgresql", {}),
        (pgd.TSVECTOR(), "postgresql", {}), (pgd.CITEXT(), "postgresql", {}), (sa.Text(), "postgresql", {}),
        (sa.CHAR(5), "postgresql", {}), (pgd.HSTORE(), "postgresql", {}), (pgd.BYTEA(), "postgresql", {}),
        (pgd.BIT(varying=True), "postgresql", {}), (pgd.DOMAIN("d", sa.Text()), "postgresql", {}),
        (sa.types.NullType(), "postgresql", {}),  # xml and other unrecognized types
        (mysql.ENUM("a", "b"), "mysql", {}),  # non-strict mode stores '' for an invalid value
        (mysql.SET("a", "b"), "mysql", {}), (mysql.JSON(), "mysql", {"mariadb": True}),
        (mssql.SQL_VARIANT(), "mssql", {}), (mssql.XML(), "mssql", {}),
        (sa.Integer(), "sqlite", {}), (sa.Date(), "sqlite", {}),  # SQLite types are affinities
    ):
        assert oe.catalog_rules_out_empty(col_type, dialect, **kw) == "", (col_type, dialect)


def test_1_a_sqlite_integer_column_holding_empty_text_is_not_ruled_out_by_its_declared_type(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "aff.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (n INTEGER NOT NULL, d DATE NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "2026-01-01"), ("", "")])
    conn.commit()
    assert conn.execute("SELECT typeof(n) FROM t WHERE n = ''").fetchone() == ("text",)
    conn.close()
    cfg = {"type": "sqlite", "database": str(db)}
    maps = [{"source": c, "target": c.upper(), "target_type": "TEXT"} for c in ("n", "d")]
    gate, _ = oe.preflight_hook(_hook_ctx(cfg, "t", maps, column_types={"n": "INTEGER", "d": "DATE"}))
    assert gate is not None and gate["status"] == "block"
    assert {c["source"] for c in gate["details"]["columns"] if c["found"]} == {"n", "d"}, gate["message"]


def test_1_mysql_json_is_ruled_out_and_mariadb_json_is_still_asked_live():
    """MySQL JSON refuses '' and is read as JSON text; MariaDB's JSON is LONGTEXT, asked as text."""
    from tests.helpers.live_env import mysql_creds, mysql_up

    if not mysql_up():
        pytest.skip("MySQL not reachable")
    import pymysql

    c = mysql_creds()
    cfg = {"type": "mysql", "host": c["host"], "port": int(c["port"]), "database": c["database"],
           "username": c["username"], "password": c["password"]}
    conn = pymysql.connect(host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
                           database=c["database"], autocommit=True)
    t = f"r4j_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            mariadb = "mariadb" in str(cur.fetchone()[0]).lower()
            cur.execute(f"CREATE TABLE `{t}` (j JSON NOT NULL, e ENUM('a', 'b') NOT NULL)")
            cur.execute(f"INSERT INTO `{t}` VALUES ('{{}}', 'a')")
            with pytest.raises(pymysql.MySQLError):
                cur.execute(f"INSERT INTO `{t}` VALUES ('', 'a')")
        res = oe.sql_empty_probe(source_connector_id="", source_config=cfg, source_table=t, column="j")
        assert res["state"] == "zero"
        # MySQL: from the catalog; MariaDB (JSON is LONGTEXT with a check): asked, no type note.
        assert ("never reads as ''" in str(res.get("note") or "")) is (not mariadb), (mariadb, res)
        enum = oe.sql_empty_probe(source_connector_id="", source_config=cfg, source_table=t, column="e")
        assert not enum.get("note"), enum  # a MySQL ENUM is never ruled out by its labels
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        conn.close()


# ------------------------------------------------------------------------ 2

def _twelve():
    maps = [{"source": f"c{i}", "target": f"C{i}", "target_type": "TEXT"} for i in range(12)]
    targets = oe.candidates(destination_db_type="oracle", mappings=maps,
                            column_nullability={f"c{i}": False for i in range(12)},
                            destination_table_exists=False, destination_column_nullability={})
    return maps, targets


def test_2_one_slow_column_does_not_starve_the_rest():
    maps, targets = _twelve()
    given: dict[str, float] = {}

    def probe(col, now, seconds, count):
        given[col] = seconds
        if col == "c0":  # a statement that always runs to its timeout
            time.sleep(seconds)
            return {"state": "unknown", "why": "timeout", "seconds": seconds}
        time.sleep(0.01)
        return {"state": "zero", "count": 0}

    measured = oe.measure(targets, probe=probe, whole_scope=True, budget_seconds=1.2, walk_allowed=False)
    by = {t["source"]: t for t in measured}
    assert given["c0"] <= 1.2 / 12 + 0.01  # a fair share, never the whole budget
    assert all(by[f"c{i}"]["exact"] for i in range(1, 12)), [(k, v.get("why")) for k, v in by.items()]
    assert by["c0"]["why"] == "timeout" and not by["c0"]["exact"]
    gate, blockers = oe.build_gate(measured, mappings=maps)
    assert [c["source"] for c in gate["details"]["columns"]] == ["c0"]
    ids = {a["id"] for b in blockers for a in b["suggested_actions"]}
    assert {oe.CHECK_OPTION, "oracle_default_transform", oe.OPTION} <= ids


def test_2_the_requested_column_is_checked_first_then_the_preview_empty_ones():
    maps, targets = _twelve()
    order: list[str] = []

    def probe(col, now, seconds, count):
        order.append(col)
        time.sleep(0.03)
        return {"state": "zero", "count": 0}

    measured = oe.measure(targets, probe=probe, whole_scope=True, budget_seconds=0.1,
                          sample_rows=[{"c5": ""}], check_now=["C11"])
    assert order[:2] == ["c11", "c5"]
    by = {t["source"]: t for t in measured}
    assert by["c11"]["exact"]
    assert all(t["exact"] or t.get("why") == "budget" for t in measured)


def test_2_twelve_columns_on_a_large_unindexed_table_under_a_scaled_down_budget_live(pg, monkeypatch):
    """The reviewer's shape: Check now on the last of 12 columns of a large unindexed table.

    The budget is scaled to the measured cost of one full-column question: it
    holds about four of the twelve. The requested column (its only '' is the
    last row) is always checked; every other column is checked or says why not.
    """
    conn, cfg, dropped = pg
    t = f"r4b_{uuid.uuid4().hex[:8]}"
    cols = [f"c{i}" for i in range(12)]
    with conn.cursor() as cur:
        dropped.append(f'DROP TABLE IF EXISTS "{t}"')
        cur.execute(f'CREATE TABLE "{t}" (' + ", ".join(f"{c} text NOT NULL" for c in cols) + ")")
        cur.execute(
            f'INSERT INTO "{t}" SELECT ' + ", ".join(
                "CASE WHEN i = 400000 THEN '' ELSE md5(i::text) END" if c == "c11" else "md5(i::text)"
                for c in cols) + " FROM generate_series(1, 400000) i"
        )
        cur.execute(f'ANALYZE "{t}"')
        start = time.monotonic()
        cur.execute(f"""SELECT 1 FROM "{t}" WHERE c0 = '' AND octet_length(c0) = 0 LIMIT 1""")
        one = time.monotonic() - start
    budget = max(0.4, 4 * one)
    monkeypatch.setattr(oe, "CHECK_NOW_SECONDS", budget)
    monkeypatch.setattr(oe, "VALIDATE_BUDGET_SECONDS", budget)
    maps = [{"source": c, "target": c.upper(), "target_type": "TEXT"} for c in cols]
    maps = oe.stamp_check_now(maps, {"option": oe.CHECK_OPTION, "columns": ["C11"]}, table="D")
    start = time.monotonic()
    gate, blockers = oe.preflight_hook(_hook_ctx(cfg, t, maps))
    took = time.monotonic() - start
    by = {c["source"]: c for c in gate["details"]["columns"]}
    assert by["c11"]["found"], gate["message"]  # the requested column, always checked
    unchecked = [c for c in cols[:11] if c in by]
    for c in unchecked:  # explicitly out of budget, never silently passed
        assert by[c].get("why") in {"budget", "time", "timeout", "rows"}, by[c]
        assert f"{c.upper()} (NOT NULL from the source): Datawrap could not rule out" in gate["message"]
    assert unchecked, "the scaled budget should not hold all twelve questions"
    assert took < budget + 5.0
    ids = {a["id"] for b in blockers for a in b["suggested_actions"]}
    assert "oracle_default_transform" in ids and oe.CHECK_OPTION in ids


# ------------------------------------------------------------------------ 3

_MAP = {"source": "note", "target": "NOTE", "target_type": "TEXT"}


def _stamped(**kw):
    return oe.stamp_check_now([dict(_MAP)], {"option": oe.CHECK_OPTION, "columns": ["NOTE"]},
                              table=kw.pop("table", "D"))[0]


def test_3_a_hand_written_stamp_is_never_honoured():
    forged = {**_MAP, oe.CHECK_KEY: {
        "requested_by": "mallory", "at": datetime.now(timezone.utc).isoformat(),
        "expires_at": "2099-01-01T00:00:00+00:00", "workspace_id": oe.current_workspace(),
        "budget_seconds": 300, "source": "note", "target": "NOTE", "table": "D",
    }}
    assert not oe._check_now_asked(forged, table="D")
    real = _stamped()
    assert oe._check_now_asked(real, table="D")
    for field, value in (("expires_at", "2099-01-01T00:00:00+00:00"), ("table", "OTHER"),
                         ("requested_by", "mallory"), ("signature", "0" * 64), ("key_id", "ak_forged")):
        tampered = {**real, oe.CHECK_KEY: {**real[oe.CHECK_KEY], field: value}}
        assert not oe._check_now_asked(tampered, table="D"), field


def test_3_a_stamp_is_bound_to_its_pair_table_workspace_and_user(monkeypatch):
    from services import workspace_filter

    real = _stamped()
    assert not oe._check_now_asked(real, table="OTHER")
    assert not oe._check_now_asked({**real, "source": "other"}, table="D")
    monkeypatch.setattr(oe, "current_workspace", lambda: "ws-elsewhere")
    assert not oe._check_now_asked(real, table="D")
    monkeypatch.setattr(oe, "current_workspace", lambda: "")
    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "bob@example.com")
    assert not oe._check_now_asked(real, table="D")  # issued to the local operator, not bob


def test_3_a_signed_stamp_never_lasts_longer_than_the_documented_window():
    from services.audit_chain_keys import signing_key

    key_id, key, _src = signing_key()
    issued = datetime.now(timezone.utc) - timedelta(seconds=oe.CHECK_NOW_VALID_SECONDS + 60)
    stamp = {"requested_by": "local operator (authentication is off)", "at": issued.isoformat(),
             "expires_at": "2099-01-01T00:00:00+00:00", "workspace_id": oe.current_workspace(),
             "budget_seconds": 300, "source": "note", "target": "NOTE", "table": "D", "key_id": key_id}
    signed = {**_MAP, oe.CHECK_KEY: {**stamp, "signature": oe._stamp_mac(stamp, key)}}
    assert not oe._check_now_asked(signed, table="D")
    fresh = {**stamp, "at": datetime.now(timezone.utc).isoformat()}
    assert oe._check_now_asked({**_MAP, oe.CHECK_KEY: {**fresh, "signature": oe._stamp_mac(fresh, key)}},
                               table="D")


def test_3_a_forged_stamp_in_the_request_does_not_trigger_the_billed_scan(monkeypatch):
    from services import preflight_population

    walks = []
    monkeypatch.setattr(preflight_population, "_iter_table_population_for_preflight",
                        lambda **kw: walks.append(kw) or iter([{"note": "x"}]))
    cfg = {"type": "bigquery", "project": "p", "dataset": "d"}
    forged = {**_MAP, oe.CHECK_KEY: {"requested_by": "mallory", "at": "2026-01-01T00:00:00+00:00",
                                     "expires_at": "2099-01-01T00:00:00+00:00", "workspace_id": ""}}
    gate, blockers = oe.preflight_hook(_hook_ctx(cfg, "t", [forged], source_format="bigquery"))
    assert gate["status"] == "block" and walks == []
    assert any(a["id"] == oe.CHECK_OPTION for b in blockers for a in b["suggested_actions"])
    gate, _ = oe.preflight_hook(_hook_ctx(cfg, "t", [_stamped()], source_format="bigquery"))
    assert gate is None and len(walks) == 1  # the server-issued stamp does


# ------------------------------------------------------------------------ 4

def test_4_a_reused_billed_check_now_result_is_stated_with_the_way_to_check_again_live(pg, monkeypatch):
    """PostgreSQL stands in for a billed warehouse, as the reviewer did."""
    conn, cfg, dropped = pg
    t = f"r4d_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        dropped.append(f'DROP TABLE IF EXISTS "{t}"')
        cur.execute(f'CREATE TABLE "{t}" (note text NOT NULL)')
        cur.execute(f"""INSERT INTO "{t}" VALUES ('a'), ('b')""")
    monkeypatch.setattr(oe, "BILLED_SCAN_ENGINES", oe.BILLED_SCAN_ENGINES | {"postgresql"})
    calls = []
    real = oe.sql_empty_probe

    def counting(**kw):
        calls.append(kw["check_now"])
        return real(**kw)

    monkeypatch.setattr(oe, "sql_empty_probe", counting)
    unasked, _ = oe.preflight_hook(_hook_ctx(cfg, t, [dict(_MAP)]))
    assert unasked["status"] == "block" and "bills" in unasked["message"]
    asked, _ = oe.preflight_hook(_hook_ctx(cfg, t, [_stamped()]))
    assert asked is None and True in calls
    before = len(calls)
    later, later_blockers = oe.preflight_hook(_hook_ctx(cfg, t, [dict(_MAP)]))
    assert len(calls) == before  # not billed again
    assert later["status"] == "warn" and later_blockers == []
    msg = later["message"]
    today = datetime.now(timezone.utc).date().isoformat()
    assert f"NOTE was checked for empty strings by Check now at {today}" in msg and "being reused until" in msg
    assert "no empty strings" in msg
    # Within the 10-minute pause after a Check now, the gate says when it can be
    # asked again instead of offering a request the server would refuse.
    wait = later["details"]["suggested_actions"][0]
    assert wait["kind"] == "oracle_empty_check_again_later" and "again after the 10-minute pause" in msg
    with pytest.raises(ValueError, match="at most once every 10 minutes"):
        oe.stamp_check_now([dict(_MAP)], {"option": oe.CHECK_OPTION, "columns": ["NOTE"]}, table="D")
    # After the pause (cleared here) the action is offered, and it works: a fresh, billed check.
    oe._check_now_cooldown({"workspace_id": oe.current_workspace(), "source": "note", "target": "NOTE",
                            "destination": {"connector": "", "schema": "", "table": "d"}}, clear=True)
    later, _ = oe.preflight_hook(_hook_ctx(cfg, t, [dict(_MAP)]))
    again = later["details"]["suggested_actions"][0]
    assert again["kind"] == oe.CHECK_OPTION and again["risk_acceptance"]["columns"] == ["NOTE"]
    assert "To check again" in later["message"]
    maps = oe.stamp_check_now([dict(_MAP)], again["risk_acceptance"], table="D")
    oe.preflight_hook(_hook_ctx(cfg, t, maps))
    assert len(calls) > before and calls[-1] is True
