"""The CE matrix (design §5.2–§5.3): certify CE1 per engine, kind and pair.

Two properties, on every engine the job starts (PostgreSQL, MySQL 8, MariaDB
as its own leg, SQL Server 2022):

1. **Cross-implementation equality.** For each kind, Hypothesis-generated
   values are written to a typed column; the dialect renders the CE1 field in
   SQL (returned as hex of its UTF-8 bytes, so no driver can fold it) and it
   must equal the Python reference on the value the driver reads back — for
   the destination rendering and for the source projections (instant
   rounding, decimal fit classes).
2. **Pairs.** For each engine pair of the first wave, a table holding every
   kind both ends carry in-engine is copied by the product's **real writer**,
   the scan must find the copy equal (mode R and mode H), and injected faults
   (a changed cell, a missing row, an extra row) must be found with their
   exact keys.

Every passing case is recorded; ``scripts/gen_parity_ce_certification.py``
turns the record into ``apps/api/data/parity_ce_certification.json``. Set
``DATAFLOW_PARITY_CE_RECORD`` to the record path. ``DATAFLOW_PARITY_CE_REQUIRE``
(``postgresql,mysql,mariadb,sqlserver`` in the ``parity-ce-matrix`` job) makes
a missing leg a failure instead of an uncollected case.
"""

from __future__ import annotations

import json
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

import parity_engines  # noqa: E402
from services.parity_scale import ce1  # noqa: E402
from services.parity_scale.plan import ColumnPlan, build_plan  # noqa: E402

ENGINES = ("postgresql", "mysql", "mariadb", "sqlserver")
LIVE = [e for e in ENGINES if parity_engines.available(e)]
EXAMPLES = int(os.environ.get("DATAFLOW_PARITY_CE_LIVE_EXAMPLES") or 30)
RECORD = os.environ.get("DATAFLOW_PARITY_CE_RECORD")


@pytest.fixture(autouse=True)
def _flag_on(monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")


def _server(engine: str) -> str:
    """The server a leg really talked to (a 'mysql' leg may be MariaDB)."""
    conn = parity_engines.connect(engine)
    try:
        with conn.cursor() as cur:
            if engine == "sqlserver":
                cur.execute("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)")
                return f"sqlserver{cur.fetchone()[0]}"
            if engine == "postgresql":
                cur.execute("SHOW server_version_num")
                return f"postgresql{int(cur.fetchone()[0]) // 10000}"
            cur.execute("SELECT VERSION()")
            version = str(cur.fetchone()[0])
            flavour = "mariadb" if "mariadb" in version.lower() else "mysql"
            return f"{flavour}{'.'.join(version.split('-')[0].split('.')[:2])}"
    finally:
        conn.close()


def record(kind: str, **fields: Any) -> None:
    if not RECORD:
        return
    with open(RECORD, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"kind": kind, **fields}, sort_keys=True, default=str) + "\n")


def test_matrix_job_has_every_leg() -> None:
    required = [e.strip() for e in (os.environ.get("DATAFLOW_PARITY_CE_REQUIRE") or "").split(",") if e.strip()]
    missing = [e for e in required if e not in LIVE]
    assert not missing, f"CE matrix legs not reachable: {missing}"
    for engine in required:
        server = _server(engine)
        if engine == "mariadb":
            assert server.startswith("mariadb"), server
        if engine == "mysql":
            assert server.startswith("mysql"), server


# ── 1. cross-implementation equality per engine and kind ────────────────────

SAFE_TEXT = st.text(alphabet=st.characters(codec="utf-8", exclude_characters="\x00"), max_size=24)
LATIN_TEXT = st.text(alphabet=st.characters(min_codepoint=32, max_codepoint=0xFFFF, codec="utf-8",
                                            exclude_categories=("Cs",)), max_size=24)
DT = st.datetimes(min_value=datetime(1000, 1, 2), max_value=datetime(9999, 12, 30))
HALVES = st.builds(lambda d, m: d.replace(microsecond=(m * 1000 + 500) % 1_000_000), DT, st.integers(0, 999))
DEC = st.decimals(allow_nan=False, allow_infinity=False, places=4, min_value=-(10**13), max_value=10**13)

#: (kind, width, digits, {engine: ddl}, strategy, python->bind adapter)
KINDS: dict[str, tuple[str, str, int, dict[str, str], Any]] = {
    "integer": ("integer", "inline", 6, {"postgresql": "bigint", "mysql": "BIGINT", "mariadb": "BIGINT",
                                         "sqlserver": "bigint"}, st.integers(-(2**63), 2**63 - 1)),
    "boolean": ("boolean", "inline", 6, {"postgresql": "boolean", "mysql": "TINYINT(1)", "mariadb": "TINYINT(1)",
                                         "sqlserver": "bit"}, st.booleans()),
    "decimal": ("decimal", "inline", 6, {"postgresql": "numeric(18,4)", "mysql": "DECIMAL(18,4)",
                                         "mariadb": "DECIMAL(18,4)", "sqlserver": "DECIMAL(18,4)"}, DEC),
    "text": ("text", "inline", 6, {"postgresql": "varchar(40)", "mysql": "VARCHAR(40)", "mariadb": "VARCHAR(40)",
                                   "sqlserver": "NVARCHAR(40)"}, SAFE_TEXT),
    "text_digest": ("text", "digest", 6, {"postgresql": "text", "mysql": "TEXT", "mariadb": "TEXT",
                                          "sqlserver": "NVARCHAR(MAX)"}, SAFE_TEXT),
    "char": ("char", "inline", 6, {"postgresql": "char(8)", "mysql": "CHAR(8)", "mariadb": "CHAR(8)",
                                   "sqlserver": "NCHAR(8)"}, LATIN_TEXT.map(lambda s: s[:8])),
    "date": ("date", "inline", 6, {"postgresql": "date", "mysql": "DATE", "mariadb": "DATE", "sqlserver": "date"},
             st.dates(min_value=date(1000, 1, 1), max_value=date(9999, 12, 31))),
    "timestamp": ("timestamp", "inline", 6, {"postgresql": "timestamp(6)", "mysql": "DATETIME(6)",
                                             "mariadb": "DATETIME(6)", "sqlserver": "DATETIME2(6)"},
                  st.one_of(DT, HALVES)),
    "timestamp7": ("timestamp", "inline", 7, {"sqlserver": "DATETIME2(7)"}, DT),
    "timestamptz": ("timestamptz", "inline", 6, {"postgresql": "timestamptz", "sqlserver": "DATETIMEOFFSET(6)"},
                    DT.map(lambda d: d.replace(tzinfo=timezone.utc))),
    "uuid": ("uuid", "inline", 6, {"postgresql": "uuid", "sqlserver": "uniqueidentifier"}, st.uuids().map(str)),
    "binary": ("binary", "digest", 6, {"postgresql": "bytea", "mysql": "VARBINARY(64)", "mariadb": "VARBINARY(64)",
                                       "sqlserver": "VARBINARY(64)"}, st.binary(max_size=64)),
}
#: Source projections checked in SQL against carrier_instant / the fit classes.
PROJECTIONS = {
    "instant3": ("timestamp", 3, {"postgresql": ("timestamp(6)", "timestamp(3)"),
                                  "mysql": ("DATETIME(6)", "DATETIME(3)"),
                                  "mariadb": ("DATETIME(6)", "DATETIME(3)"),
                                  "sqlserver": ("DATETIME2(7)", "DATETIME2(3)")}),
    "instant0": ("timestamp", 0, {"postgresql": ("timestamp(6)", "timestamp(0)"),
                                  "mysql": ("DATETIME(6)", "DATETIME(0)"),
                                  "mariadb": ("DATETIME(6)", "DATETIME(0)"),
                                  "sqlserver": ("DATETIME2(7)", "DATETIME2(0)")}),
}
CASES = [(e, k) for e in LIVE for k, spec in KINDS.items() if e in spec[3]]


def _bind(engine: str, value: Any) -> Any:
    if value is None:
        return None
    if engine == "postgresql" and isinstance(value, (bytes, bytearray)):
        import psycopg2

        return psycopg2.Binary(value)
    if engine == "sqlserver" and isinstance(value, datetime) and value.tzinfo is not None:
        return value.isoformat(sep=" ")
    if engine == "sqlserver" and isinstance(value, bool):
        return int(value)
    return value


def _side(engine: str, conn: Any, col: ColumnPlan, table: str, dest_engine: str):
    from services.parity_scale.dialects import open_side

    product = "mysql" if engine == "mariadb" else engine
    plan = build_plan([{"source": "x", "target": "x"}], source_types={"x": col.source_type},
                      dest_types={"x": col.dest_type}, source_engine=product, dest_engine=dest_engine)
    return open_side(product, role="destination", plan=plan, table=table, connection=conn,
                     **({"schema": "dbo"} if engine == "sqlserver" else {}))


def _render(engine: str, table: str, col: ColumnPlan, values: list[Any], dest_engine: str) -> list[tuple]:
    conn = parity_engines.connect(engine)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table}")
            for i, value in enumerate(values):
                cur.execute(f"INSERT INTO {table} (id, x) VALUES (%s, %s)", (i, _bind(engine, value)))
        conn.commit()
        side = _side(engine, conn, col, table, dest_engine)
        q = side.quote("x")
        from services.parity_scale.dialects import mysql as my
        from services.parity_scale.dialects import postgresql as pg
        from services.parity_scale.dialects import sqlserver as ms

        module = {"postgresql": pg, "mysql": my, "mariadb": my, "sqlserver": ms}[engine]
        kwargs = {"column_type": ""} if engine == "postgresql" else {}
        dest_f = module.field_sql(col, column="x", role="destination", dest_engine=dest_engine, **kwargs)
        src_f = module.field_sql(col, column="x", role="source", dest_engine=dest_engine, **kwargs)
        sql = (f"SELECT id, {q}, {side.utf8_hex(dest_f)}, {side.utf8_hex(src_f)} "
               f"FROM {side.table_ref} ORDER BY id")
        return side.execute(sql, [])
    finally:
        conn.close()


def _check(rows: list[tuple], col: ColumnPlan, dest_engine: str, engine: str, case: str) -> None:
    spec = col.field_spec(dest_engine)
    for _id, value, dest_hex, src_hex in rows:
        if engine == "postgresql" and isinstance(value, memoryview):
            value = bytes(value)
        want = ce1.encode_value(value, spec)
        assert bytes.fromhex(dest_hex or "") == want, (engine, case, value, dest_hex, want)
        want_src = ce1.encode_source_value(value, spec)
        if want_src is ce1.HELD:
            assert src_hex is None, (engine, case, value, src_hex)
        else:
            assert bytes.fromhex(src_hex or "") == want_src, (engine, case, value, src_hex, want_src)


@pytest.mark.parametrize(("engine", "case"), CASES, ids=[f"{e}-{k}" for e, k in CASES])
def test_sql_rendering_equals_the_python_reference(engine, case) -> None:
    kind, width, digits, ddls, strategy = KINDS[case]
    ddl = ddls[engine]
    product = "mysql" if engine == "mariadb" else engine
    table = f"psd1_ce_{uuid.uuid4().hex[:8]}"
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, x {ddl})")
    conn.commit()
    conn.close()
    col = ColumnPlan(source="x", target="x", cls="engine", kind=kind, width=width, digits=digits,
                     source_type=ddl, dest_type=ddl)
    seen = {"n": 0}

    @settings(max_examples=EXAMPLES, derandomize=True, deadline=None,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
    @given(batch=st.lists(st.one_of(strategy, st.none()), min_size=1, max_size=20))
    def check(batch):
        rows = _render(engine, table, col, batch, product)
        _check(rows, col, product, engine, case)
        seen["n"] += len(batch)

    try:
        check()
    finally:
        conn = parity_engines.connect(engine)
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {table}")
        conn.commit()
        conn.close()
    record("kind", engine=engine, server=_server(engine), case=case, ce_kind=kind, ddl=ddl, values=seen["n"])


def _projection_property(engine: str, table: str, dcol: ColumnPlan, dest_engine: str):
    @settings(max_examples=EXAMPLES, derandomize=True, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(batch=st.lists(st.one_of(DT, HALVES), min_size=1, max_size=20))
    def check(batch):
        rows = _render(engine, table, dcol, batch, dest_engine)
        spec = dcol.field_spec(dest_engine)
        for _id, value, _dest_hex, src_hex in rows:
            assert bytes.fromhex(src_hex) == ce1.encode_source_value(value, spec), (engine, dest_engine, value)

    return check


PROJECTION_CASES = [(e, p) for e in LIVE for p, spec in PROJECTIONS.items() if e in spec[2]]


@pytest.mark.parametrize(("engine", "case"), PROJECTION_CASES, ids=[f"{e}-{p}" for e, p in PROJECTION_CASES])
def test_sql_instant_projection_replays_carrier_instant(engine, case) -> None:
    kind, digits, ddls = PROJECTIONS[case]
    src_ddl, dst_ddl = ddls[engine]
    product = "mysql" if engine == "mariadb" else engine
    table = f"psd1_pj_{uuid.uuid4().hex[:8]}"
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, x {src_ddl})")
    conn.commit()
    conn.close()
    col = ColumnPlan(source="x", target="x", cls="engine", kind=kind, digits=digits,
                     source_type=src_ddl, dest_type=dst_ddl, projection=f"instant({digits})")
    try:
        # Both destination rules (PostgreSQL's 2000 axis and half up) from every source engine.
        for dest_engine in ("postgresql", "mysql", "sqlserver"):
            dcol = ColumnPlan(**{**col.__dict__, "dest_type": {"postgresql": f"timestamp({digits})",
                                                                "mysql": f"DATETIME({digits})",
                                                                "sqlserver": f"DATETIME2({digits})"}[dest_engine]})

            _projection_property(engine, table, dcol, dest_engine)()
    finally:
        conn = parity_engines.connect(engine)
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {table}")
        conn.commit()
        conn.close()
    record("projection", engine=engine, server=_server(engine), case=case)
    assert product


@pytest.mark.parametrize("engine", LIVE)
def test_sql_decimal_fit_classes_replay_the_python_projection(engine) -> None:
    ddl = {"postgresql": "numeric(20,6)", "sqlserver": "DECIMAL(20,6)"}.get(engine, "DECIMAL(20,6)")
    table = f"psd1_fit_{uuid.uuid4().hex[:8]}"
    product = "mysql" if engine == "mariadb" else engine
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, x {ddl})")
    conn.commit()
    conn.close()
    values = [Decimal(v) for v in ("0", "0.5", "0.9999", "1", "12.34", "12.345", "9999.99", "10000",
                                   "-0.0001", "99.999999", "123456.123456", "-12.300000")]
    try:
        for p, s in ((6, 2), (4, 4), (5, 0)):
            for actions in ({"scale": "null", "int": "hold", "both": "hold"},
                            {"scale": "hold", "int": "null", "both": "null"}):
                fit = {"precision": p, "scale": s, "dest_db": product, "actions": actions}
                col = ColumnPlan(source="x", target="x", cls="engine", kind="decimal", source_type=ddl,
                                 dest_type=f"DECIMAL({p},{s})", decimal_fit=fit)
                _check(_render(engine, table, col, values, product), col, product, engine, f"fit({p},{s})")
    finally:
        conn = parity_engines.connect(engine)
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {table}")
        conn.commit()
        conn.close()
    record("projection", engine=engine, server=_server(engine), case="decimal_fit")


# ── 2. pairs: the real writer copies, the scan finds the copy equal ─────────

PAIR_KINDS = ["integer", "boolean", "decimal", "text", "text_digest", "char", "date", "timestamp",
              "timestamptz", "uuid", "binary"]
WAVE1 = [("postgresql", "postgresql"), ("postgresql", "sqlserver"), ("sqlserver", "postgresql"),
         ("mysql", "postgresql"), ("postgresql", "mysql"), ("sqlserver", "sqlserver"), ("mysql", "mysql"),
         ("mariadb", "mariadb"), ("mariadb", "postgresql"), ("postgresql", "mariadb")]
PAIRS = [(a, b) for a, b in WAVE1 if a in LIVE and b in LIVE]
N_ROWS = 400


def _row_values(rng: random.Random, i: int) -> dict[str, Any]:
    base = datetime(1999, 12, 31, 23, 59, 50) + timedelta(seconds=rng.randint(-10**8, 10**8),
                                                            microseconds=rng.randint(0, 999_999))
    return {
        "integer": rng.randint(-(2**62), 2**62),
        "boolean": rng.random() < 0.5,
        "decimal": Decimal(rng.randint(-(10**13), 10**13)) / Decimal(10000),
        "text": rng.choice(["", " ", "abc", "Ünïcödé", "trailing  ", "日本語", "emoji 😀", "a'b\"c"]) + str(i),
        "text_digest": "long text " * rng.randint(0, 30) + str(i),
        "char": rng.choice(["a", "ab ", "xyz", ""]) + str(i % 10),
        "date": date(1990, 1, 1) + timedelta(days=rng.randint(-20000, 20000)),
        "timestamp": base,
        "timestamptz": base.replace(tzinfo=timezone.utc),
        "uuid": str(uuid.UUID(int=rng.getrandbits(128))),
        "binary": rng.randbytes(rng.randint(0, 40)),
    }


def _pair_kinds(src: str, dst: str) -> list[str]:
    return [k for k in PAIR_KINDS if src in KINDS[k][3] and dst in KINDS[k][3]]


def _create(engine: str, table: str, kinds: list[str]) -> None:
    cols = ", ".join(f"c_{k} {KINDS[k][3][engine]}" for k in kinds)
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, {cols})")
    conn.commit()
    conn.close()


def _drop(engine: str, table: str) -> None:
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE {table}")
    conn.commit()
    conn.close()


def _exec(engine: str, sql: str, params: tuple = ()) -> None:
    conn = parity_engines.connect(engine)
    with conn.cursor() as cur:
        cur.execute(sql, params) if params else cur.execute(sql)
    conn.commit()
    conn.close()


@pytest.mark.parametrize(("src", "dst"), PAIRS, ids=[f"{a}-{b}" for a, b in PAIRS])
def test_pair_copy_by_the_real_writer_is_found_equal_and_faults_exactly(src, dst) -> None:
    from services.parity_scale.dialects import open_side
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    kinds = _pair_kinds(src, dst)
    rng = random.Random(f"{src}->{dst}")
    s_table, d_table = f"psd1_ps_{uuid.uuid4().hex[:8]}", f"psd1_pd_{uuid.uuid4().hex[:8]}"
    _create(src, s_table, kinds)
    _create(dst, d_table, kinds)
    try:
        conn = parity_engines.connect(src)
        with conn.cursor() as cur:
            for i in range(1, N_ROWS + 1):
                vals = _row_values(rng, i)
                cols = ", ".join(f"c_{k}" for k in kinds)
                marks = ", ".join("%s" for _ in range(len(kinds) + 1))
                cur.execute(f"INSERT INTO {s_table} (id, {cols}) VALUES ({marks})",
                            (i, *[_bind(src, vals[k]) for k in kinds]))
            cur.execute(f"SELECT id, {', '.join(f'c_{k}' for k in kinds)} FROM {s_table} ORDER BY id")
            rows = [[bytes(v) if isinstance(v, memoryview) else v for v in r] for r in cur.fetchall()]
        conn.commit()
        conn.close()
        headers = ["id", *[f"c_{k}" for k in kinds]]
        mappings = [{"source": h, "target": h} for h in headers]
        dest_types = {"id": "INT", **{f"c_{k}": KINDS[k][3][dst] for k in kinds}}
        token = set_active_number_locale("WIRE")
        try:
            result = parity_engines.writer(dst)(
                **parity_engines.writer_kwargs(dst, d_table), headers=headers, data_rows=rows,
                mappings=mappings, column_types=dest_types, create_table=False, error_policy="quarantine",
            )
        finally:
            reset_active_number_locale(token)
        assert result.ok, result.error
        src_types = {"id": "INT", **{f"c_{k}": KINDS[k][3][src] for k in kinds}}
        plan = build_plan(mappings, source_types=src_types, dest_types=dest_types, source_engine=src,
                          dest_engine=dst, key_columns=["id"], allow_native=False)
        streamed = [c.source for c in plan.columns if c.cls != "engine"]
        assert not streamed, (src, dst, [(c.source, c.reason) for c in plan.columns if c.cls != "engine"])

        def open_(role, plan_):
            engine = src if role == "source" else dst
            return open_side("mysql" if engine == "mariadb" else engine, role=role, plan=plan_,
                             table=s_table if role == "source" else d_table, connection=parity_engines.connect(engine),
                             **({"schema": "dbo"} if engine == "sqlserver" else {}))

        def run(mode: str) -> dict[str, Any]:
            cfg = ScanConfig(mode=mode, segment_rows=50, leaf_threshold=20, bisection_factor=4)
            scan = ParityScan.start(plan, store=MemoryScanStore(), open_side=open_, config=cfg)
            scan.run()
            return scan.result()

        for mode in ("R", "H"):
            ev = run(mode)
            assert ev["outcome"] == "match", (src, dst, mode, ev["differences"])
            assert ev["differences"]["ce_stricter_than_fp"] == 0, (src, dst, mode, ev["differences"])
        q = {"postgresql": '"{}"', "sqlserver": "[{}]"}.get(dst, "`{}`")
        _exec(dst, f"UPDATE {d_table} SET {q.format('c_integer')} = {q.format('c_integer')} + 1 WHERE id = 7")
        _exec(dst, f"DELETE FROM {d_table} WHERE id = 300")
        for mode in ("R", "H"):
            ev = run(mode)
            diff = ev["differences"]
            assert [k[0] for k in diff["changed_keys"]] == [{"$i": "7"}], (src, dst, mode, diff)
            assert [k[0] for k in diff["missing_keys"]] == [{"$i": "300"}], (src, dst, mode, diff)
            assert diff["changed_cells_by_column"] == {"c_integer": 1}
    finally:
        _drop(src, s_table)
        _drop(dst, d_table)
    record("pair", source=src, destination=dst, source_server=_server(src), destination_server=_server(dst),
           kinds=sorted({KINDS[k][0] for k in kinds}), rows=N_ROWS)
