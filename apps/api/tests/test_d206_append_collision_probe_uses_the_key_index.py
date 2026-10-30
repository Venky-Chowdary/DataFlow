"""D206: the append collision probe answers from the key's index, and its
answer is the engine's own key comparison (NULL, '', whitespace, unicode).

``CAST(key AS TEXT) IN (...)`` could not use any index, so every append plan
read the whole destination. These run the probe itself against a live
PostgreSQL table and SQLite, and read PostgreSQL's plan for the statement.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from services.destination_key_collision_probe import (
    probe_destination_key_collisions,
    probe_destination_tuple_collisions,
)


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn, cfg


@pytest.fixture
def pg_tables():
    conn, cfg = _pg()
    sfx = uuid.uuid4().hex[:8]
    ints, texts, pair = f"d206_int_{sfx}", f"d206_txt_{sfx}", f"d206_pair_{sfx}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE public.{ints} (id BIGINT PRIMARY KEY, v TEXT)")
        cur.execute(f"INSERT INTO public.{ints} SELECT g, 'x' FROM generate_series(1, 50000) g")
        cur.execute(f"CREATE TABLE public.{texts} (code TEXT PRIMARY KEY)")
        cur.execute(
            f"INSERT INTO public.{texts} VALUES (''), (' '), ('a '), ('Zoë 東京 ✓'), ('007')"
        )
        cur.execute(f"INSERT INTO public.{texts} SELECT 'k' || g FROM generate_series(1, 50000) g")
        cur.execute(f"CREATE TABLE public.{pair} (a BIGINT, b TEXT, PRIMARY KEY (a, b))")
        cur.execute(f"INSERT INTO public.{pair} SELECT g, 'b' || g FROM generate_series(1, 50000) g")
        for t in (ints, texts, pair):
            cur.execute(f"ANALYZE public.{t}")
    statements: list[str] = []
    try:
        yield {"cfg": {**cfg, "type": "postgresql", "schema": "public"}, "conn": conn,
               "ints": ints, "texts": texts, "pair": pair, "statements": statements}
    finally:
        with conn.cursor() as cur:
            for t in (ints, texts, pair):
                cur.execute(f"DROP TABLE IF EXISTS public.{t}")
        conn.close()


@pytest.fixture
def seen():
    """The probe's SQL as the server receives it (every engine, this test only)."""
    import sqlalchemy as sa

    out: list[tuple[str, dict]] = []

    def hook(conn, cursor, statement, parameters, context, executemany):
        out.append((statement, parameters))

    sa.event.listen(sa.engine.Engine, "before_cursor_execute", hook)
    try:
        yield out
    finally:
        sa.event.remove(sa.engine.Engine, "before_cursor_execute", hook)


def _plans(conn, seen, table: str) -> list[str]:
    out = []
    with conn.cursor() as cur:
        for stmt, params in seen:
            if table in stmt and stmt.lstrip().upper().startswith("SELECT") and "pg_catalog" not in stmt:
                cur.execute("EXPLAIN " + stmt, params)
                out.append("\n".join(r[0] for r in cur.fetchall()))
    return out


def test_integer_key_probe_uses_the_index_and_matches_the_stored_value(pg_tables, seen):
    result = probe_destination_key_collisions(
        destination_config=pg_tables["cfg"], destination_db_type="postgresql",
        destination_table=pg_tables["ints"], key_column="id",
        # '007' and ' 7' land in a BIGINT key as 7; 'abc' and '' cannot be stored keys.
        values=["007", " 12 ", "49999.00", "abc", "", None, "999999"],
    )
    assert result.status == "ran", result.message
    assert sorted(f["value"] for f in result.findings) == ["12", "49999", "7"]
    plans = _plans(pg_tables["conn"], seen, pg_tables["ints"])
    assert plans and all("Seq Scan" not in p for p in plans), plans


def test_text_key_probe_keeps_empty_whitespace_and_unicode_exact(pg_tables, seen):
    result = probe_destination_key_collisions(
        destination_config=pg_tables["cfg"], destination_db_type="postgresql",
        destination_table=pg_tables["texts"], key_column="code",
        values=["", " ", "a", "a ", "Zoë 東京 ✓", "7", "007", None],
    )
    assert result.status == "ran", result.message
    assert sorted(f["value"] for f in result.findings) == sorted(["", " ", "a ", "Zoë 東京 ✓", "007"])
    plans = _plans(pg_tables["conn"], seen, pg_tables["texts"])
    assert plans and all("Seq Scan" not in p for p in plans), plans


def test_composite_key_probe_uses_the_index(pg_tables, seen):
    result = probe_destination_tuple_collisions(
        destination_config=pg_tables["cfg"], destination_db_type="postgresql",
        destination_table=pg_tables["pair"], columns=["a", "b"],
        tuples=[("5", "b5"), ("6", "nope"), ("70000", "b70000"), ("x", "b1")],
    )
    assert result.status == "ran", result.message
    assert [f["value"] for f in result.findings] == ["5 | b5"]
    plans = _plans(pg_tables["conn"], seen, pg_tables["pair"])
    assert plans and all("Seq Scan" not in p for p in plans), plans


def test_sqlite_keys_compare_natively(tmp_path):
    path = tmp_path / "d206.db"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
    db.executemany("INSERT INTO t VALUES (?, ?)", [(1, ""), (2, " "), (3, "ü")])
    db.commit()
    db.close()
    cfg = {"type": "sqlite", "database": str(path), "path": str(path)}
    ids = probe_destination_key_collisions(
        destination_config=cfg, destination_db_type="sqlite", destination_table="t",
        key_column="id", values=["01", "4", "x"],
    )
    assert ids.status == "ran", ids.message
    assert [f["value"] for f in ids.findings] == ["1"]
    codes = probe_destination_key_collisions(
        destination_config=cfg, destination_db_type="sqlite", destination_table="t",
        key_column="code", values=["", "  ", "ü", "u"],
    )
    assert codes.status == "ran", codes.message
    assert sorted(f["value"] for f in codes.findings) == ["", "ü"]


def test_types_without_an_exact_python_twin_keep_the_text_comparison():
    import sqlalchemy as sa

    from services.destination_key_collision_probe import native_key_values

    assert native_key_values(sa.Numeric(), "postgresql", ["1.50"]) is None
    assert native_key_values(sa.Boolean(), "postgresql", ["true"]) is None
    assert native_key_values(sa.types.NullType(), "sqlite", ["1"]) is None
    assert native_key_values(sa.Text(), "oracle", ["a"]) is None
    assert native_key_values(sa.String(20), "oracle", ["a"]) == ["a"]
    assert native_key_values(sa.BigInteger(), "mysql", ["-3", "+4", "5.0", "5.5"]) == [-3, 4, 5]
