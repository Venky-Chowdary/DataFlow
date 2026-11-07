"""QA round 11 (build 9c75ada4): "current transaction is aborted" into PostgreSQL.

QA: MariaDB ``family`` -> PG, MySQL query mode -> PG, PG -> pgvector-type upsert
and mirror soft under load each failed with "current transaction is aborted,
commands ignored until end of transaction block" and no other reason.

Root A: the PostgreSQL writer ran the schema-fidelity ``CREATE TABLE`` in a
try block and, when PostgreSQL refused it, ran the types-only fallback on the
same transaction with no savepoint, so the fallback (and every statement after
it) failed with "aborted" while the real error went only to a log line. Two
triggers made the fidelity CREATE fail:

* MySQL/MariaDB ``tinyint(1) DEFAULT '0'`` became ``boolean DEFAULT 0``
  ("column is of type boolean but default expression is of type integer");
* a typed pgvector table's ``vector(n)`` column in a database where the
  ``vector`` extension was never created ("type vector does not exist").

Root B: the mirror census COUNT swallowed its own failure on the write
connection, so the soft-delete UPDATE that followed failed with "aborted".

Every test drives MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job
into a real PostgreSQL, with NULL, '', whitespace and unicode cells, and a
second run into the table the first run created.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r

pytestmark = pytest.mark.timeout(900)

NOTES = {1: "Zoë ✓", 2: None, 3: "", 4: "   ", 5: "日本語"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    # Deterministic local embedder for pgvector: no model download, real vectors.
    monkeypatch.setenv("DATAFLOW_EMBEDDING_MODEL", "hash/32")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------
# MySQL / MariaDB tinyint(1) DEFAULT 0 -> PostgreSQL BOOLEAN
# --------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["mariadb", "mysql"])
def test_mysql_family_tinyint_default_lands_as_boolean_default(client, kind):
    cfg = r.mysql_family(kind)
    token = uuid.uuid4().hex[:8]
    src, dst = f"r11ab_{token}", f"r11ab_{kind}_{token}"
    r.mysql_execute(
        kind,
        f"DROP TABLE IF EXISTS `{src}`",
        f"CREATE TABLE `{src}` (id int NOT NULL PRIMARY KEY, note varchar(40) DEFAULT NULL, "
        "flag tinyint(1) NOT NULL DEFAULT '0', on_flag tinyint(1) DEFAULT '1', "
        "qty int DEFAULT '0', label varchar(20) NOT NULL DEFAULT '') "
        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    rows = [
        (1, NOTES[1], 0, 1, 5, "a"),
        (2, NOTES[2], 1, None, None, ""),
        (3, NOTES[3], 0, 0, 0, " "),
        (4, NOTES[4], 1, 1, 2, "ü"),
        (5, NOTES[5], 0, None, 7, "z"),
    ]
    for row in rows:
        r.mysql_execute(kind, f"INSERT INTO `{src}` VALUES (%s, %s, %s, %s, %s, %s)", params=row)
    source = r.connector("source", cfg)
    dest = r.connector("destination", r.pg_cfg())
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest, dest_table=dst)
    try:
        job = r.run(client, {**args, "sync_mode": "full_refresh_overwrite"})
        assert "aborted" not in r.job_text(job)
        cols = r.columns(dst)
        # The fidelity CREATE ran (no types-only fallback): the defaults carried,
        # spelled as PostgreSQL booleans.
        assert cols["flag"][0] == "boolean" and cols["flag"][1] == "false", cols
        assert cols["on_flag"][0] == "boolean" and cols["on_flag"][1] == "true", cols
        assert str(cols["qty"][1]) == "0", cols
        want = [
            (1, "Zoë ✓", False, True, 5, "a"),
            (2, None, True, None, None, ""),
            (3, "", False, False, 0, " "),
            (4, "   ", True, True, 2, "ü"),
            (5, "日本語", False, None, 7, "z"),
        ]
        got = r.pg(f'SELECT id, note, flag, on_flag, qty, label FROM public."{dst}" ORDER BY id')
        assert got == want

        # Run 2 into the table run 1 created: upsert, NULL / '' / unicode changes.
        r.mysql_execute(
            kind,
            f"UPDATE `{src}` SET note = NULL, flag = 0 WHERE id = 1",
            f"UPDATE `{src}` SET note = 'ñ2', on_flag = 0 WHERE id = 2",
            f"INSERT INTO `{src}` (id, note) VALUES (6, '')",
        )
        job2 = r.run(client, {**args, "sync_mode": "upsert", "primary_key": "id"})
        assert "aborted" not in r.job_text(job2)
        got2 = r.pg(f'SELECT id, note, flag, on_flag, qty, label FROM public."{dst}" ORDER BY id')
        assert got2[:2] == [(1, None, False, True, 5, "a"), (2, "ñ2", True, False, None, "")]
        # Row 6 took the source's own defaults (flag 0, on_flag 1, qty 0, label '').
        assert got2[5] == (6, "", False, True, 0, "")
        assert got2[2:5] == want[2:5]
    finally:
        r.mysql_execute(kind, f"DROP TABLE IF EXISTS `{src}`")
        h.drop_table("postgresql", dst)


# --------------------------------------------------------------------------
# PostgreSQL -> pgvector typed table
# --------------------------------------------------------------------------


def _pgvector_source(token: str) -> str:
    src = f"r11pv_s_{token}"
    r.pg(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name text, '
        "amount numeric(10,2), active boolean DEFAULT true, note varchar(40))"
    )
    for i, note in NOTES.items():
        r.pg(
            f'INSERT INTO public."{src}" (id, name, amount, note) VALUES (%s, %s, %s, %s)',
            (i, f"n{i}", round(i * 1.37, 2), note),
        )
    return src


def _assert_typed_vector_table(dst: str, src: str, *, database: str | None) -> None:
    cols = r.columns(dst, database=database)
    assert cols["id"][0] in {"integer", "bigint"} and cols["amount"][0] == "numeric(10,2)", cols
    assert cols["active"] == ("boolean", "true"), cols
    assert cols["embedding"][0] == "vector(32)", cols
    got = r.pg(f'SELECT id, name, amount, active, note FROM public."{dst}" ORDER BY id', database=database)
    assert got == r.pg(f'SELECT id, name, amount, active, note FROM public."{src}" ORDER BY id')
    assert not r.pg(f'SELECT id FROM public."{dst}" WHERE embedding IS NULL', database=database)


@pytest.mark.parametrize("fresh_database", [True, False], ids=["no_extension_yet", "extension_present"])
def test_pg_into_pgvector_typed_upsert_creates_the_extension_it_needs(client, fresh_database):
    if not r.pg("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"):
        pytest.skip("pgvector is not installed on this PostgreSQL")
    token = uuid.uuid4().hex[:8]
    src, dst = _pgvector_source(token), f"r11pv_d_{token}"
    try:
        if fresh_database:
            ctx = r.scratch_database()
        else:
            from contextlib import nullcontext

            r.pg("CREATE EXTENSION IF NOT EXISTS vector")
            ctx = nullcontext(None)
        with ctx as database:
            if database:
                assert not r.pg("SELECT 1 FROM pg_extension WHERE extname = 'vector'", database=database)
            source = r.connector("source", r.pg_cfg())
            dest = r.connector("destination", r.pg_cfg(type="pgvector", **({"database": database} if database else {})))
            args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest,
                        dest_table=dst, sync_mode="upsert", primary_key="id")
            job = r.run(client, args)
            assert "aborted" not in r.job_text(job)
            assert r.pg("SELECT 1 FROM pg_extension WHERE extname = 'vector'", database=database)
            _assert_typed_vector_table(dst, src, database=database)

            # Run 2 into the table run 1 created.
            r.pg(f'UPDATE public."{src}" SET note = NULL, name = \'ñ\' WHERE id = 1')
            r.pg(f'UPDATE public."{src}" SET note = \'\', active = false WHERE id = 2')
            r.pg(f'INSERT INTO public."{src}" (id, name, amount, note) VALUES (6, \'   \', 0.01, \'ⓢix\')')
            job2 = r.run(client, args)
            assert "aborted" not in r.job_text(job2)
            _assert_typed_vector_table(dst, src, database=database)
            if not database:
                h.drop_table("postgresql", dst)
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_pgvector_without_extension_and_no_privilege_is_refused_at_preflight(client):
    if not r.pg("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"):
        pytest.skip("pgvector is not installed on this PostgreSQL")
    token = uuid.uuid4().hex[:8]
    src, dst = _pgvector_source(token), f"r11pv_d_{token}"
    try:
        with r.plain_role() as (role, secret), r.scratch_database(owner=role) as database:
            source = r.connector("source", r.pg_cfg())
            dest = r.connector(
                "destination",
                r.pg_cfg(type="pgvector", database=database, username=role, password=secret),
            )
            ok, staged = r.stage(client, dict(
                source_connector_id=source, source_table=src, dest_connector_id=dest,
                dest_table=dst, sync_mode="upsert", primary_key="id",
            ))
            text = f"{staged}" if not ok else " ".join(h.blockers_of(staged))
            assert (not ok) or h.blockers_of(staged), "preflight passed a pgvector table it cannot create"
            assert "pgvector" in text and "vector" in text.lower(), text
            assert "aborted" not in text
            assert not r.table_exists(dst, database=database)
    finally:
        h.drop_table("postgresql", src)


# --------------------------------------------------------------------------
# Mirror soft: a census COUNT that fails must not abort the soft-delete
# --------------------------------------------------------------------------


def test_mirror_soft_completes_when_a_census_count_fails(client, monkeypatch):
    import services.mirror_engine as mirror_engine

    token = uuid.uuid4().hex[:8]
    src, dst = f"r11mi_s_{token}", f"r11mi_d_{token}"
    r.pg(f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, note text)')
    for i, note in NOTES.items():
        r.pg(f'INSERT INTO public."{src}" VALUES (%s, %s)', (i, note))
    source = r.connector("source", r.pg_cfg())
    dest = r.connector("destination", r.pg_cfg())
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest,
                dest_table=dst, sync_mode="full_refresh_mirror", primary_key="id")
    try:
        r.run(client, args)
        assert r.pg(f'SELECT count(*) FROM public."{dst}"') == [(5,)]

        r.pg(f'DELETE FROM public."{src}" WHERE id IN (2, 3)')
        r.pg(f'UPDATE public."{src}" SET note = \'ñ4\' WHERE id = 4')
        r.pg(f'INSERT INTO public."{src}" VALUES (6, \'  \')')

        # Force the first transition COUNT to fail *in PostgreSQL* (a COUNT
        # cancelled under load), on the mirror's own connection.
        original = mirror_engine._dest_engine_count
        forced: list[str] = []

        def failing_count(conn, sql):
            if not forced and "EXISTS" in sql:
                forced.append(sql)
                sql = "SELECT COUNT(*) FROM pg_class WHERE 1 / 0 = 1"
            return original(conn, sql)

        monkeypatch.setattr(mirror_engine, "_dest_engine_count", failing_count)
        job = r.run(client, args)
        assert forced, "the census COUNT was never reached"
        assert "aborted" not in r.job_text(job)
        got = r.pg(f'SELECT id, note, _deleted FROM public."{dst}" ORDER BY id')
        assert got == [
            (1, "Zoë ✓", False), (2, None, True), (3, "", True),
            (4, "ñ4", False), (5, "日本語", False), (6, "  ", False),
        ]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
