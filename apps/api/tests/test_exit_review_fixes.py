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

