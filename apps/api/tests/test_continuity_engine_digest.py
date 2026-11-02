"""X-97 follow-up: continuity baseline digested inside the destination engine.

Verdict equivalence with the Gate-8 Python digest (not byte equality),
duplicate rows, pinned session rendering, schema drift, algorithm upgrade.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from bson.decimal128 import Decimal128

from services import append_continuity as ac
from services import continuity_digest as cd
from tests.test_x97_append_continuity import (  # noqa: F401 — fixture re-export
    ENGINES,
    _add_source_rows,
    _Dest,
    _key,
    _runner,
    _source,
    fake_store,
)

TS = "2026-01-02 03:04:05.123456"
ROWS = [
    (1, "Aa", "héllo 🌍", "150.25", TS, None),
    (2, "Bb", "日本語", "0.10", TS, ""),
    (3, "Cc", "", "-1.00", TS, "x"),
]
DDL = {
    "postgresql": 'CREATE TABLE "{t}" (id int, "MixedCase" text, uni text, amt numeric(12,2), '
    'ts timestamp(6), tz timestamptz, note text)',
    "mysql": "CREATE TABLE `{t}` (id int, `MixedCase` varchar(64), uni varchar(64) CHARACTER SET utf8mb4, "
    "amt decimal(12,2), ts datetime(6), tz timestamp(6) NULL, note varchar(64) NULL, j json NULL)",
    "sqlite": 'CREATE TABLE "{t}" (id INTEGER, "MixedCase" TEXT, uni TEXT, amt NUMERIC, ts TEXT, note TEXT)',
    "duckdb": 'CREATE TABLE "{t}" (id INTEGER, "MixedCase" VARCHAR, uni VARCHAR, amt DECIMAL(12,2), '
    "ts TIMESTAMP, tz TIMESTAMPTZ, note VARCHAR)",
}


def _q(engine: str, name: str) -> str:
    return f"`{name}`" if engine == "mysql" else f'"{name}"'


def _ph(engine: str) -> str:
    return "?" if engine in {"sqlite", "duckdb"} else "%s"


def _seed(dest: _Dest) -> None:
    e, t = dest.engine, dest.table
    if e == "mongodb":
        dest._mongo.insert_many([
            {"_id": i, "MixedCase": m, "uni": u, "amt": Decimal128(a), "ts": dt.datetime(2026, 1, 2, 3, 4, 5, 123000, tzinfo=dt.timezone.utc),
             "note": n, "nested": {"b": 1, "a": [1, {"z": None, "y": "é"}]}}
            for i, m, u, a, _ts, n in ROWS
        ])
        return
    dest._sql(DDL[e].format(t=t))
    p = _ph(e)
    for i, m, u, a, ts, n in ROWS:
        if e == "sqlite":
            dest._sql(f'INSERT INTO "{t}" VALUES ({p},{p},{p},{p},{p},{p})', (i, m, u, a, ts, n))
        elif e in {"postgresql", "duckdb"}:
            dest._sql(f'INSERT INTO "{t}" VALUES ({p},{p},{p},{p},{p},{p},{p})', (i, m, u, a, ts, ts + "+00", n))
        else:
            dest._sql(
                f"INSERT INTO `{t}` VALUES ({p},{p},{p},{p},{p},{p},{p},{p})",
                (i, m, u, a, ts, ts, n, '{"b": 1, "a": [1, null]}'),
            )


def _mutate(dest: _Dest, kind: str) -> None:
    e, t = dest.engine, dest.table
    if e == "mongodb":
        c = dest._mongo
        if kind == "changed_cell":
            c.update_one({"_id": 1}, {"$set": {"uni": "hallo 🌍"}})
        elif kind == "deleted_row":
            c.delete_one({"_id": 2})
        elif kind == "inserted_row":
            c.insert_one({"_id": 4, "MixedCase": "Dd", "uni": "new"})
        elif kind == "duplicate_row":
            doc = c.find_one({"_id": 1})
            doc["_id"] = 99  # _id is unique; identical content under a new _id
            c.insert_one(doc)
        elif kind == "null_to_empty":
            c.update_one({"_id": 1}, {"$set": {"note": ""}})
        return
    T, mc = _q(e, t), _q(e, "MixedCase")
    sql = {
        "changed_cell": f"UPDATE {T} SET uni = 'hallo 🌍' WHERE id = 1",
        "deleted_row": f"DELETE FROM {T} WHERE id = 2",
        "inserted_row": f"INSERT INTO {T} (id, {mc}, uni) VALUES (4, 'Dd', 'new')",
        "duplicate_row": f"INSERT INTO {T} SELECT * FROM {T} WHERE id = 1",
        "null_to_empty": f"UPDATE {T} SET note = '' WHERE id = 1",
    }[kind]
    dest._sql(sql)


def _target(dest: _Dest) -> ac.ContinuityTarget:
    t = ac.resolve_target(dest.endpoint())
    assert t is not None
    return t


MUTATIONS = ["changed_cell", "deleted_row", "inserted_row", "duplicate_row", "null_to_empty"]


@pytest.mark.parametrize("kind", MUTATIONS)
@pytest.mark.parametrize("engine", ENGINES)
def test_engine_digest_verdict_matches_python_digest(engine, kind, tmp_path):
    dest = _Dest(engine, tmp_path)
    try:
        _seed(dest)
        tgt = _target(dest)
        e0, p0 = ac.fingerprint(tgt, cd.ALGO), ac.fingerprint(tgt, ac.LEGACY_ALGO)
        assert e0 and e0["algo"] == cd.ALGO and e0["rows"] == 3
        assert p0 and p0["rows"] == 3
        # Unchanged data: both stable.
        assert ac.fingerprint(tgt, cd.ALGO) == e0
        assert ac.fingerprint(tgt, ac.LEGACY_ALGO)["checksum"] == p0["checksum"]
        _mutate(dest, kind)
        e1, p1 = ac.fingerprint(tgt, cd.ALGO), ac.fingerprint(tgt, ac.LEGACY_ALGO)
        engine_caught = (e1["rows"], e1["checksum"]) != (e0["rows"], e0["checksum"])
        python_caught = (p1["rows"], p1["checksum"]) != (p0["rows"], p0["checksum"])
        assert engine_caught, f"engine digest missed {kind}"
        assert engine_caught == python_caught
    finally:
        dest.drop()


@pytest.mark.parametrize("engine", ENGINES)
def test_exact_duplicate_row_fails_the_next_append(engine, tmp_path, fake_store):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=False)
        run = _runner(fake_store, src, dest, pk=False)
        assert run().success
        if engine == "mongodb":
            doc = dest._mongo.find_one({"id": 1})
            doc.pop("_id")
            dest._mongo.insert_one(doc)
        else:
            T = _q(engine, dest.table)
            dest._sql(f"INSERT INTO {T} SELECT * FROM {T} WHERE id = 1")
        before = dest.snapshot()
        _add_source_rows(src, 11, 13)
        r2 = run()
        assert not r2.success
        assert r2.error_details["mismatch"] == "row_count"
        assert r2.error_details["found_rows"] == 11
        assert dest.snapshot() == before
    finally:
        dest.drop()


def test_sum_aggregate_does_not_cancel_pairs():
    one = cd._sum_rows(["r"])
    two = cd._sum_rows(["r", "r"])
    assert one[1:] != (0, 0) and two[1:] == (2 * one[1], 2 * one[2])
    assert cd._sum_rows([]) == (0, 0, 0)


def test_mongo_canonical_form_sorts_keys_at_every_level():
    a = {"x": 1, "n": {"b": 2, "a": [1, {"d": 1, "c": None}]}}
    b = {"n": {"a": [1, {"c": None, "d": 1}], "b": 2}, "x": 1}
    assert cd.canonical(a) == cd.canonical(b)
    assert cd.canonical({"a": [1, 2]}) != cd.canonical({"a": [2, 1]})
    assert cd.canonical(None) != cd.canonical("") != cd.canonical("n")
    assert cd.canonical(1) != cd.canonical("1") != cd.canonical(1.0)


def test_pg_session_settings_do_not_change_the_digest(tmp_path):
    dest = _Dest("postgresql", tmp_path)
    try:
        _seed(dest)
        tgt = _target(dest)
        e0 = ac.fingerprint(tgt, cd.ALGO)
        raw0 = dest._sql(f'SELECT tz::text FROM "{dest.table}" WHERE id = 1')
        dest._sql(
            "DO $$ BEGIN EXECUTE format('ALTER ROLE %%I SET TimeZone = %%L', current_user, 'Pacific/Chatham'); "
            "EXECUTE format('ALTER ROLE %%I SET DateStyle = %%L', current_user, 'SQL, DMY'); "
            "EXECUTE format('ALTER ROLE %%I SET extra_float_digits = %%L', current_user, '0'); END $$"
        )
        try:
            raw1 = dest._sql(f'SELECT tz::text FROM "{dest.table}" WHERE id = 1')
            assert raw1 != raw0, "precondition: the session setting must change ::text"
            assert ac.fingerprint(tgt, cd.ALGO) == e0
        finally:
            dest._sql("DO $$ BEGIN EXECUTE format('ALTER ROLE %%I RESET ALL', current_user); END $$")
    finally:
        dest.drop()


def _raw_mysql(session_tz: str):
    import pymysql

    from tests.test_x97_append_continuity import MYSQL

    conn = pymysql.connect(
        host=MYSQL["host"], port=int(MYSQL["port"]), user=MYSQL["username"],
        password=MYSQL["password"], database=MYSQL["database"], autocommit=True,
    )
    conn.cursor().execute(f"SET SESSION time_zone = '{session_tz}'")
    return conn


def test_mysql_session_time_zone_does_not_change_the_digest(tmp_path, monkeypatch):
    """Raw connections (no connector +00:00 default) under different session zones."""
    dest = _Dest("mysql", tmp_path)
    db = _target(dest).cfg.get("database", "")
    try:
        _seed(dest)
        utc, nepal = _raw_mysql("+00:00"), _raw_mysql("+05:45")
        try:
            pinned_utc = cd.mysql_digest_on(utc, db, dest.table)
            pinned_nepal = cd.mysql_digest_on(nepal, db, dest.table)
            assert pinned_utc == pinned_nepal
            monkeypatch.setattr(cd, "MYSQL_SESSION_PINS", ())
            utc.cursor().execute("SET SESSION time_zone = '+00:00'")
            nepal.cursor().execute("SET SESSION time_zone = '+05:45'")
            unpinned_utc = cd.mysql_digest_on(utc, db, dest.table)
            unpinned_nepal = cd.mysql_digest_on(nepal, db, dest.table)
            # Without the pin the same stored TIMESTAMP renders differently: the
            # pin is what keeps unchanged data from looking tampered.
            assert unpinned_utc != unpinned_nepal
        finally:
            utc.close()
            nepal.close()
    finally:
        dest.drop()


@pytest.mark.parametrize("engine", ["sqlite", "postgresql", "mysql"])
def test_added_column_is_reported_as_schema_change(engine, tmp_path, fake_store):
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        dest._sql(f"ALTER TABLE {_q(engine, dest.table)} ADD COLUMN extra INTEGER")
        before = dest.snapshot()
        _add_source_rows(src, 11, 13)
        r2 = run()
        assert not r2.success
        d = r2.error_details
        assert d["mismatch"] == "schema" and d["reason"] == "destination_schema_changed"
        assert d["columns_added"] == ["extra"] and d["columns_dropped"] == []
        assert "schema change, not a data mismatch" in r2.error
        assert dest.snapshot() == before
    finally:
        dest.drop()


def test_compare_reports_drop_rename_and_retype():
    base = {"rows": 1, "checksum": "a", "algo": cd.ALGO, "job_id": "j",
            "columns": [["a", "integer"], ["b", "text"], ["c", "text"]]}
    cur = {"rows": 1, "checksum": "a", "algo": cd.ALGO,
           "columns": [["a", "bigint"], ["b2", "text"], ["c", "text"]]}
    v = ac.compare("t", base, cur)
    assert v is not None and v.details["mismatch"] == "schema"
    assert v.details["columns_added"] == ["b2"]
    assert v.details["columns_dropped"] == ["b"]
    assert v.details["columns_retyped"] == ["a"]


def test_compare_never_passes_across_algorithms():
    base = {"rows": 3, "checksum": "same", "algo": ac.LEGACY_ALGO}
    cur = {"rows": 3, "checksum": "same", "algo": cd.ALGO}
    v = ac.compare("t", base, cur)
    assert v is not None and v.details["mismatch"] == "unreadable"
    assert ac.compare("t", {"rows": 3, "checksum": "same"}, cur) is not None


def _legacy_baseline(dest: _Dest) -> None:
    tgt = _target(dest)
    fp = ac.fingerprint(tgt, ac.LEGACY_ALGO)
    ac.save_baseline(tgt.key, {"rows": fp["rows"], "checksum": fp["checksum"]}, "legacy-job")
    assert ac.load_baseline(tgt.key)["algo"] == ac.LEGACY_ALGO


def _audit_spy(monkeypatch) -> list[dict[str, Any]]:
    import services.audit_log as audit_log

    events: list[dict[str, Any]] = []
    real = audit_log.append_audit_event

    def spy(**kw):
        if str(kw.get("action", "")).startswith("append_continuity."):
            events.append(kw)
            return kw
        return real(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", spy)
    return events


@pytest.mark.parametrize("engine", ENGINES)
def test_legacy_baseline_verified_then_upgraded(engine, tmp_path, fake_store, monkeypatch):
    events = _audit_spy(monkeypatch)
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        _legacy_baseline(dest)
        _add_source_rows(src, 11, 13)
        r2 = run()
        assert r2.success, r2.error
        base = ac.load_baseline(_key(dest))
        assert base["algo"] == cd.ALGO and base["rows"] == 12
        up = [e for e in events if e["action"] == "append_continuity.algo_upgraded"]
        assert len(up) == 1 and up[0]["details"]["from_algo"] == ac.LEGACY_ALGO
    finally:
        dest.drop()


@pytest.mark.parametrize("engine", ENGINES)
def test_legacy_baseline_on_tampered_destination_still_fails(engine, tmp_path, fake_store, monkeypatch):
    events = _audit_spy(monkeypatch)
    dest = _Dest(engine, tmp_path)
    try:
        src = _source(tmp_path, pk=True)
        run = _runner(fake_store, src, dest, pk=True)
        assert run().success
        _legacy_baseline(dest)
        dest.tamper()
        before = dest.snapshot()
        _add_source_rows(src, 11, 13)
        r2 = run()
        assert not r2.success
        assert r2.error_details["mismatch"] == "checksum"
        assert ac.load_baseline(_key(dest))["algo"] == ac.LEGACY_ALGO
        assert not events
        assert dest.snapshot() == before
    finally:
        dest.drop()
