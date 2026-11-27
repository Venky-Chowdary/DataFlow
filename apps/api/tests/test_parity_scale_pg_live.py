"""Parity at Scale on live PostgreSQL→PostgreSQL (design §5.1–§5.5, phase 1).

* Cross-implementation equality: every CE1 field the PostgreSQL dialect renders
  in SQL equals the Python reference byte for byte, for Hypothesis-generated
  values written to typed columns (the SQL side returns hex, so no driver
  fold can hide a difference).
* The fault-injection table: single-cell change, missing row, extra row, NULL
  vs '', trailing space, decimal scale zeros, timestamp precision and a zone
  shift, an approved instant narrowing, an approved lossy decimal cast written
  through the real PostgreSQL writer (and the row it should have held out),
  duplicate rows in a keyless table — each found with the exact key and count,
  in mode R and mode H.
* Statement timeouts (``not_compared``), resume with fresh connections, and
  ``engine_checksum``'s CE1 whole-table digest behind the flag.

Connection: ``DATAFLOW_PG_TEST_DSN`` (CI's PostgreSQL 16 service).
"""

from __future__ import annotations

import socket
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

hypothesis = pytest.importorskip("hypothesis")
psycopg2 = pytest.importorskip("psycopg2")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from services.parity_scale import ce1  # noqa: E402
from services.parity_scale.dialects.postgresql import PostgresSide, field_sql  # noqa: E402
from services.parity_scale.driver import ParityScan, ScanConfig  # noqa: E402
from services.parity_scale.plan import ColumnPlan, build_plan  # noqa: E402
from services.parity_scale.store import MemoryScanStore  # noqa: E402


def _cfg() -> dict[str, Any]:
    from conftest import pg_test_cfg

    return pg_test_cfg()


def _reachable() -> bool:
    cfg = _cfg()
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


def connect():
    cfg = _cfg()
    return psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                            user=cfg["username"], password=cfg["password"])


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")


@pytest.fixture(scope="module")
def admin():
    conn = connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    tables: list[str] = []
    yield conn, tables
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.close()


def _name(admin, stem: str) -> str:
    name = f"psd1_{stem}_{uuid.uuid4().hex[:8]}"
    admin[1].append(name)
    return name


def run_sql(admin, *statements: str, params: Any = None) -> None:
    with admin[0].cursor() as cur:
        for stmt in statements:
            cur.execute(stmt, params)


# ── cross-implementation equality: SQL rendering == Python reference ─────────

SAFE_TEXT = st.text(alphabet=st.characters(codec="utf-8", exclude_characters="\x00"), max_size=30)
SAFE_DT = st.datetimes(min_value=datetime(1, 1, 2), max_value=datetime(9999, 12, 30))
OFFSETS = st.integers(-14 * 60, 14 * 60).map(lambda m: timezone(timedelta(minutes=m)))

KIND_CASES = {
    "bigint": ("bigint", "integer", 6, "inline", st.integers(-(2**63), 2**63 - 1)),
    "boolean": ("boolean", "boolean", 6, "inline", st.booleans()),
    "numeric": ("numeric", "decimal", 6, "inline", st.one_of(
        st.decimals(allow_nan=False, allow_infinity=False, places=None, min_value=-(10**30), max_value=10**30),
        st.just(Decimal("NaN")), st.just(Decimal("-0.000")))),
    "numeric(12,4)": ("numeric(12,4)", "decimal", 6, "inline",
                      st.decimals(allow_nan=False, allow_infinity=False, places=4, min_value=-(10**7), max_value=10**7)),
    "varchar(20)": ("varchar(30)", "text", 6, "inline", SAFE_TEXT),
    "text": ("text", "text", 6, "digest", SAFE_TEXT),
    "char(5)": ("char(5)", "char", 6, "inline", SAFE_TEXT.map(lambda s: s[:5])),
    "date": ("date", "date", 6, "inline", st.dates(min_value=datetime(1, 1, 2).date())),
    "timestamp(6)": ("timestamp(6)", "timestamp", 6, "inline", SAFE_DT),
    "timestamptz": ("timestamptz", "timestamptz", 6, "inline",
                    st.datetimes(min_value=datetime(1, 1, 3), max_value=datetime(9999, 12, 29), timezones=OFFSETS)),
    "time(6)": ("time(6)", "time", 6, "inline", st.times()),
    "uuid": ("uuid", "uuid", 6, "inline", st.uuids().map(str)),
    "bytea": ("bytea", "binary", 6, "digest", st.binary(max_size=64).map(psycopg2.Binary)),
}


def _compare_rendering(admin, table: str, ddl: str, col: ColumnPlan, values: list[Any]) -> None:
    run_sql(admin, f'TRUNCATE "{table}"')
    with admin[0].cursor() as cur:
        cur.executemany(f'INSERT INTO "{table}" (id, x) VALUES (%s, %s)', list(enumerate(values)))
    dest_sql = field_sql(col, column="x", role="destination", column_type=ddl)
    src_sql = field_sql(col, column="x", role="source", column_type=ddl)
    conn = connect()
    try:
        side = PostgresSide(role="destination", plan=build_plan(
            [{"source": "x", "target": "x"}], source_types={"x": ddl}, dest_types={"x": ddl},
            source_engine="postgresql", dest_engine="postgresql"), table=table, connection=conn)
        rows = side._run(
            f"SELECT id, x, encode(convert_to({dest_sql}, 'UTF8'), 'hex'), "
            f"encode(convert_to({src_sql}, 'UTF8'), 'hex') FROM \"{table}\" ORDER BY id"
        )
    finally:
        conn.close()
    spec = col.field_spec("postgresql")
    for _id, value, dest_hex, src_hex in rows:
        expect_dest = ce1.encode_value(value, spec)
        assert bytes.fromhex(dest_hex) == expect_dest, (ddl, value, bytes.fromhex(dest_hex), expect_dest)
        expect_src = ce1.encode_source_value(value, spec)
        if expect_src is ce1.HELD:
            assert src_hex is None, (ddl, value, src_hex)
        else:
            assert src_hex is not None and bytes.fromhex(src_hex) == expect_src, (ddl, value, src_hex, expect_src)


@pytest.mark.parametrize("case", sorted(KIND_CASES))
def test_sql_rendering_equals_the_python_reference(admin, case) -> None:
    ddl, kind, digits, width, values = KIND_CASES[case]
    table = _name(admin, "ce")
    run_sql(admin, f'CREATE TABLE "{table}" (id int PRIMARY KEY, x {ddl})')
    col = ColumnPlan(source="x", target="x", cls="engine", kind=kind, width=width, digits=digits,
                     source_type=ddl, dest_type=ddl)

    @settings(max_examples=40, derandomize=True, deadline=None,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
    @given(batch=st.lists(st.one_of(values, st.none()), min_size=1, max_size=25))
    def check(batch):
        _compare_rendering(admin, table, ddl, col, batch)

    check()


@pytest.mark.parametrize("target,digits", [("timestamp(3)", 3), ("timestamp(0)", 0), ("timestamptz(3)", 3)])
def test_sql_instant_projection_replays_carrier_instant(admin, target, digits) -> None:
    aware = target.startswith("timestamptz")
    source = "timestamptz(6)" if aware else "timestamp(6)"
    table = _name(admin, "inst")
    run_sql(admin, f'CREATE TABLE "{table}" (id int PRIMARY KEY, x {source})')
    col = ColumnPlan(source="x", target="x", cls="engine", kind="timestamptz" if aware else "timestamp",
                     digits=digits, source_type=source, dest_type=target, projection=f"instant({digits})")
    # The whole range, halves included: PostgreSQL rounds half away from zero
    # around 2000-01-01, and carrier_instant (so Gate-8 and CE1) models it.
    halves = st.builds(lambda d, m: d.replace(microsecond=(m * 1000 + 500) % 1_000_000),
                       st.datetimes(min_value=datetime(1, 1, 3), max_value=datetime(9999, 12, 29)),
                       st.integers(0, 999))
    pool = st.one_of(st.datetimes(min_value=datetime(1, 1, 3), max_value=datetime(9999, 12, 29)), halves)
    if aware:
        pool = pool.map(lambda d: d.replace(tzinfo=timezone.utc))

    @settings(max_examples=40, derandomize=True, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(batch=st.lists(pool, min_size=1, max_size=25))
    def check(batch):
        _compare_rendering(admin, table, source, col, batch)

    check()
    # And the stored, narrowed value encodes like the projected source.
    run_sql(admin, f'TRUNCATE "{table}"', f"INSERT INTO \"{table}\" VALUES (1, '2024-01-01 00:00:00.1235')")
    with admin[0].cursor() as cur:
        cur.execute(f'SELECT x, x::{target} FROM "{table}"')
        raw, stored = cur.fetchone()
    spec = col.field_spec("postgresql")
    assert ce1.encode_source_value(raw, spec) == ce1.encode_value(stored, spec)


def test_sql_decimal_fit_classes_replay_the_python_projection(admin) -> None:
    table = _name(admin, "fit")
    run_sql(admin, f'CREATE TABLE "{table}" (id int PRIMARY KEY, x numeric(20,6))')
    for p, s in ((6, 2), (4, 4), (5, 0)):
        for actions in ({"scale": "null", "int": "hold", "both": "hold", "nan": "null", "inf": "hold"},
                        {"scale": "hold", "int": "null", "both": "null", "nan": "keep", "inf": "hold"}):
            fit = {"precision": p, "scale": s, "dest_db": "postgresql", "actions": actions}
            col = ColumnPlan(source="x", target="x", cls="engine", kind="decimal", source_type="numeric(20,6)",
                             dest_type=f"numeric({p},{s})", decimal_fit=fit)
            values = [Decimal(v) for v in ("0", "0.5", "0.9999", "1", "12.34", "12.345", "9999.99", "10000",
                                           "-0.0001", "99.999999", "123456.123456", "NaN", "-12.300000")]
            _compare_rendering(admin, table, "numeric(20,6)", col, values)


# ── the fault-injection table ────────────────────────────────────────────────

N_ROWS = 20_000
NARROW = ("id bigint PRIMARY KEY, i integer, amt numeric(18,4), name varchar(64), "
          "ts timestamp(6), ok boolean, d date, u uuid, note text")
TYPES = {"id": "bigint", "i": "integer", "amt": "numeric(18,4)", "name": "varchar(64)", "ts": "timestamp(6)",
         "ok": "boolean", "d": "date", "u": "uuid", "note": "text"}
FILL = (
    "SELECT g, (g * 7) % 1000, round((g * 1.37)::numeric, 4), 'name ' || g, "
    "timestamp '2024-01-01' + g * interval '1.234567 second', g % 2 = 0, date '2020-01-01' + g % 2000, "
    "md5(g::text)::uuid, CASE WHEN g % 10 = 0 THEN NULL ELSE repeat('n', g % 40) END "
    f"FROM generate_series(1, {N_ROWS}) g"
)
CFG = ScanConfig(segment_rows=2_000, leaf_threshold=200, bisection_factor=8)


def pair(admin, stem: str, ddl: str = NARROW, fill: str = FILL, *, key: bool = True) -> tuple[str, str]:
    src, dst = _name(admin, f"{stem}_s"), _name(admin, f"{stem}_d")
    cols = ddl if key else ddl.replace(" PRIMARY KEY", "")
    run_sql(admin, f'CREATE TABLE "{src}" ({cols})', f'INSERT INTO "{src}" {fill}',
            f'CREATE TABLE "{dst}" ({cols})', f'INSERT INTO "{dst}" SELECT * FROM "{src}"')
    return src, dst


def scan_pg(src: str, dst: str, plan, *, mode: str, config: ScanConfig = CFG, store=None, fault=None):
    def open_side(role, plan_):
        return PostgresSide(role=role, plan=plan_, table=src if role == "source" else dst, connection=connect())

    cfg = ScanConfig(**{**config.__dict__, "mode": mode})
    run = ParityScan.start(plan, store=store or MemoryScanStore(), open_side=open_side, config=cfg, fault=fault)
    run.run()
    return run.result()


def plan_of(types=TYPES, key=("id",), mappings=None, dest_types=None, **kw):
    maps = mappings or [{"source": c, "target": c} for c in types]
    return build_plan(maps, source_types=types, dest_types=dest_types or types, source_engine="postgresql",
                      dest_engine="postgresql", key_columns=list(key), **kw)


def keys(diff: dict, name: str) -> list[int]:
    return sorted(int(k[0]["$i"]) for k in diff[name])


FAULTS = {
    "single cell": (["UPDATE {d} SET i = i + 1 WHERE id = 12345"],
                    {"changed": [12345], "cells": {"i": 1}}),
    "missing row": (["DELETE FROM {d} WHERE id = 777"], {"missing": [777]}),
    "extra row": (["INSERT INTO {d} SELECT id + 100000, i, amt, name, ts, ok, d, u, note FROM {d} WHERE id = 5"],
                  {"extra": [100005]}),
    "NULL vs empty": (["UPDATE {s} SET note = NULL WHERE id = 300", "UPDATE {d} SET note = '' WHERE id = 300",
                       "UPDATE {s} SET note = '' WHERE id = 301", "UPDATE {d} SET note = NULL WHERE id = 301"],
                      {"changed": [300, 301], "cells": {"note": 2}, "cause": ("null_vs_empty", 2)}),
    "trailing space": (["UPDATE {d} SET name = name || ' ' WHERE id = 4242"],
                       {"changed": [4242], "cells": {"name": 1}, "cause": ("whitespace", 1)}),
    "timestamp microsecond": (["UPDATE {d} SET ts = ts + interval '1 microsecond' WHERE id = 9000"],
                              {"changed": [9000], "cells": {"ts": 1}}),
    "timestamp zone shift": (["UPDATE {d} SET ts = ts + interval '1 hour' WHERE id = 9001"],
                             {"changed": [9001], "cells": {"ts": 1}}),
    "decimal last digit": (["UPDATE {d} SET amt = amt + 0.0001 WHERE id = 19999"],
                           {"changed": [19999], "cells": {"amt": 1}}),
}


@pytest.mark.parametrize("mode", ["R", "H"])
@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_fault_injection_table(admin, fault, mode) -> None:
    src, dst = pair(admin, "f")
    statements, want = FAULTS[fault]
    run_sql(admin, *[s.format(s=f'"{src}"', d=f'"{dst}"') for s in statements])
    ev = scan_pg(src, dst, plan_of(), mode=mode)
    diff = ev["differences"]
    assert ev["coverage"]["complete"], ev["coverage"]
    assert ev["outcome"] == "differences_found"
    assert keys(diff, "changed_keys") == want.get("changed", [])
    assert keys(diff, "missing_keys") == want.get("missing", [])
    assert keys(diff, "extra_keys") == want.get("extra", [])
    assert diff["changed_rows"] == len(want.get("changed", []))
    assert diff["changed_cells_by_column"] == want.get("cells", {})
    if "cause" in want:
        code, count = want["cause"]
        assert diff["causes"].get(code) == count, diff["causes"]
    assert diff["at_least"] is False
    assert ev["population"]["rows_source"] == N_ROWS


@pytest.mark.parametrize("mode", ["R", "H"])
def test_clean_copy_matches(admin, mode) -> None:
    src, dst = pair(admin, "clean")
    ev = scan_pg(src, dst, plan_of(), mode=mode)
    assert ev["outcome"] == "match" and ev["coverage"]["complete"]
    assert ev["population"]["rows_source"] == ev["population"]["rows_destination"] == N_ROWS


def test_decimal_scale_zeros_are_not_a_difference(admin) -> None:
    ddl = "id bigint PRIMARY KEY, amt numeric"
    src, dst = pair(admin, "scale", ddl, "SELECT g, (g || '.3400')::numeric FROM generate_series(1, 3000) g")
    run_sql(admin, f'UPDATE "{dst}" SET amt = trim_scale(amt)')
    ev = scan_pg(src, dst, plan_of({"id": "bigint", "amt": "numeric"}), mode="R")
    assert ev["outcome"] == "match"
    assert ev["differences"]["ce_stricter_than_fp"] == 0


def test_approved_instant_narrowing_is_equal_and_a_real_change_is_not(admin) -> None:
    src, dst = _name(admin, "nar_s"), _name(admin, "nar_d")
    run_sql(admin,
            f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, ts timestamp(6))',
            f"INSERT INTO \"{src}\" SELECT g, timestamp '2024-01-01' + g * interval '1.234567 second' "
            "FROM generate_series(1, 5000) g",
            f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, ts timestamp(3))',
            f'INSERT INTO "{dst}" SELECT id, ts FROM "{src}"')
    plan = plan_of({"id": "bigint", "ts": "timestamp(6)"}, dest_types={"id": "bigint", "ts": "timestamp(3)"})
    assert {c.source: c.projection for c in plan.columns}["ts"] == "instant(3)"
    for mode in ("R", "H"):
        assert scan_pg(src, dst, plan, mode=mode)["outcome"] == "match"
    run_sql(admin, f"UPDATE \"{dst}\" SET ts = ts + interval '1 millisecond' WHERE id = 2500")
    ev = scan_pg(src, dst, plan, mode="R")
    assert keys(ev["differences"], "changed_keys") == [2500]


@pytest.mark.parametrize("mode", ["R", "H"])
def test_keyless_duplicate_and_missing_rows(admin, mode) -> None:
    src, dst = pair(admin, "kl", key=False)
    run_sql(admin, f'INSERT INTO "{dst}" SELECT * FROM "{dst}" WHERE id = 42',
            f'DELETE FROM "{dst}" WHERE id = 43')
    ev = scan_pg(src, dst, plan_of(key=()), mode=mode)
    diff = ev["differences"]
    assert ev["check_kind"] == "multiset_digest" and ev["mode"] == "H"
    assert (diff["missing_in_destination"], diff["extra_in_destination"]) == (1, 1)
    assert diff["key_attribution"] == "cannot_attribute_to_key"
    assert diff["missing_content_examples"][0][0] == "43"
    assert diff["extra_content_examples"][0][0] == "42"


@pytest.mark.parametrize("signed", [True, False], ids=["coerce_null_contract", "no_contract"])
def test_approved_lossy_cast_written_by_the_real_writer(admin, signed) -> None:
    """DECIMAL(12,4) → NUMERIC(10,2), written by the PostgreSQL writer.

    Under a signed CAST_AND_CONTINUE / COERCE_NULL contract every value that
    does not fit is written as NULL; without one it is held out. Either way
    the scan's projection replays what the writer did and the copy matches.
    """
    from connectors.postgresql_writer import write_mapped_rows
    from services.migration_risk_contract import create_migration_risk_contract
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    src, dst = _name(admin, "lossy_s"), _name(admin, "lossy_d")
    run_sql(admin, f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, amt numeric(12,4))',
            f"INSERT INTO \"{src}\" SELECT g, CASE WHEN g % 50 = 0 THEN g + 0.1234 "  # does not fit (scale)
            "WHEN g % 77 = 0 THEN 99999999.5 "                                        # the widest value that fits
            "ELSE round(g * 1.25, 2) END FROM generate_series(1, 4000) g",
            f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, amt numeric(10,2))')
    amt_map: dict[str, Any] = {"source": "amt", "target": "amt"}
    if signed:
        amt_map["risk_contract"] = create_migration_risk_contract(
            column="amt", source_type="numeric(12,4)", destination_type="numeric(10,2)",
            approved_by="qa@example.com", reason="accepted narrowing", execution_policy="CAST_AND_CONTINUE",
            quarantine_policy="COERCE_NULL",
        ).to_dict()
    mappings = [{"source": "id", "target": "id"}, amt_map]
    with admin[0].cursor() as cur:
        cur.execute(f'SELECT id, amt FROM "{src}" ORDER BY id')
        rows = [list(r) for r in cur.fetchall()]
        cur.execute(f'SELECT count(*) FROM "{src}" WHERE id % 50 = 0')
        unfit = cur.fetchone()[0]
    cfg = _cfg()
    token = set_active_number_locale("WIRE")
    try:
        result = write_mapped_rows(
            host=cfg["host"], port=cfg["port"], database=cfg["database"], username=cfg["username"],
            password=cfg["password"], schema="public", connection_string="", ssl=False, table_name=dst,
            headers=["id", "amt"], data_rows=rows, mappings=mappings,
            column_types={"id": "bigint", "amt": "numeric(10,2)"}, create_table=False, error_policy="quarantine",
        )
    finally:
        reset_active_number_locale(token)
    assert result.ok, result.error
    with admin[0].cursor() as cur:
        cur.execute(f'SELECT count(*), count(amt) FROM "{dst}"')
        landed, non_null = cur.fetchone()
    if signed:
        assert (landed, non_null) == (4000, 4000 - unfit)
    else:
        assert landed == 4000 - unfit
    plan = plan_of({"id": "bigint", "amt": "numeric(12,4)"}, mappings=mappings,
                   dest_types={"id": "bigint", "amt": "numeric(10,2)"})
    amt = {c.source: c for c in plan.columns}["amt"]
    assert amt.cls == "engine" and amt.projection.startswith("fit(10,2)")
    for mode in ("R", "H"):
        ev = scan_pg(src, dst, plan, mode=mode)
        assert ev["outcome"] == "match", ev["differences"]
        assert ev["population"]["held_out"] == {"fit_excluded": 0 if signed else unfit}
        assert ev["population"]["rows_source"] == landed
    # The value the contract said to NULL / hold, written rounded instead.
    if signed:
        run_sql(admin, f'UPDATE "{dst}" SET amt = 50.12 WHERE id = 50')
        ev = scan_pg(src, dst, plan, mode="R")
        assert keys(ev["differences"], "changed_keys") == [50]
    else:
        run_sql(admin, f'INSERT INTO "{dst}" VALUES (50, 50.12)')
        ev = scan_pg(src, dst, plan, mode="R")
        assert keys(ev["differences"], "extra_keys") == [50]


def test_float_residue_is_judged_equal_by_the_fingerprint(admin) -> None:
    ddl = "id bigint PRIMARY KEY, f float8, j jsonb"
    src, dst = pair(admin, "flt", ddl, "SELECT g, g / 3.0, jsonb_build_object('a', g) FROM generate_series(1, 2000) g")
    run_sql(admin, f'UPDATE "{dst}" SET f = f + 1e-13 WHERE id = 1000')
    types = {"id": "bigint", "f": "double precision", "j": "jsonb"}
    ev = scan_pg(src, dst, plan_of(types), mode="R")
    assert ev["outcome"] == "match"
    assert ev["differences"]["ce_stricter_than_fp"] == 1
    # Streamed instead of natively rendered: the same verdict.
    ev = scan_pg(src, dst, plan_of(types, allow_native=False), mode="R")
    assert ev["outcome"] == "match" and ev["coverage"]["columns_streamed"] == 2


def test_statement_timeout_leaves_units_not_compared(admin) -> None:
    src, dst = pair(admin, "to")
    cfg = ScanConfig(segment_rows=20_000, statement_timeout_s=0.001, max_retries=1)
    ev = scan_pg(src, dst, plan_of(), mode="R", config=cfg)
    assert ev["outcome"] == "not_fully_compared"
    assert ev["coverage"]["units_not_compared"] >= 1
    assert "statement timeout" in ev["coverage"]["not_compared"][0]["reason"]


def test_resume_with_fresh_connections_gives_the_same_result(admin) -> None:
    src, dst = pair(admin, "res")
    run_sql(admin, f'UPDATE "{dst}" SET name = \'x\' WHERE id = 15000', f'DELETE FROM "{dst}" WHERE id = 3')
    plan = plan_of()

    def open_side(role, plan_):
        return PostgresSide(role=role, plan=plan_, table=src if role == "source" else dst, connection=connect())

    store = MemoryScanStore()
    first = ParityScan.start(plan, store=store, open_side=open_side, config=ScanConfig(**{**CFG.__dict__, "mode": "R"}))
    first.run(max_seconds=0.05)
    first.close()  # the worker dies; its connections go with it
    resumed = ParityScan.resume(first.state["scan_id"], store=store, open_side=open_side, plan=plan)
    resumed.run()
    ev = resumed.result()
    assert keys(ev["differences"], "changed_keys") == [15000]
    assert keys(ev["differences"], "missing_keys") == [3]
    assert ev["resumed"] >= 1


def test_engine_checksum_ce1_population_digest(admin, monkeypatch) -> None:
    from services.engine_checksum import postgresql_ce1_population_digest, postgresql_engine_checksum

    src, dst = pair(admin, "ec")
    plan = plan_of()
    a = postgresql_ce1_population_digest(connect(), plan, role="source", table=src)
    b = postgresql_ce1_population_digest(connect(), plan, role="destination", table=dst)
    assert a is not None and a == b._replace(scope=a.scope) and a.row_count == N_ROWS
    assert a.checksum.startswith("psd1:")
    run_sql(admin, f'UPDATE "{dst}" SET ok = NOT ok WHERE id = 10')
    c = postgresql_ce1_population_digest(connect(), plan, role="destination", table=dst)
    assert c.checksum != a.checksum
    # The legacy md5 digest Gate-8 uses is unchanged and still not comparable.
    with admin[0].cursor() as cur:
        legacy = postgresql_engine_checksum(cur, f'"{src}"', list(TYPES))
    assert legacy is not None and not legacy.checksum.startswith("psd1:")
    monkeypatch.delenv("DATAWRAP_PARITY_SCALE")
    assert postgresql_ce1_population_digest(connect(), plan, role="source", table=src) is None


@pytest.mark.parametrize("strategy", ["linear", "sample", "quantile"])
def test_large_table_boundaries_still_find_every_fault(admin, strategy) -> None:
    """Linear integer split and TABLESAMPLE boundaries (forced on a small table)."""
    src, dst = pair(admin, f"bd_{strategy}")
    run_sql(admin, f'ANALYZE "{src}"',
            f'UPDATE "{dst}" SET i = i + 1 WHERE id IN (3, 9999, 19998)',
            f'DELETE FROM "{dst}" WHERE id = 12000',
            f'INSERT INTO "{dst}" SELECT 50000, i, amt, name, ts, ok, d, u, note FROM "{dst}" WHERE id = 1')
    cfg = ScanConfig(segment_rows=2_000, leaf_threshold=200, bisection_factor=8, boundary_strategy=strategy)
    ev = scan_pg(src, dst, plan_of(), mode="R", config=cfg)
    diff = ev["differences"]
    assert ev["boundary_strategy"] == strategy
    assert ev["coverage"]["complete"]
    assert keys(diff, "changed_keys") == [3, 9999, 19998]
    assert keys(diff, "missing_keys") == [12000]
    assert keys(diff, "extra_keys") == [50000]
