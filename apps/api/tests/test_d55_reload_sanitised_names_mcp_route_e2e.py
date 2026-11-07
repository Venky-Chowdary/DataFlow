"""D55 — reload into a table DataFlow created, column names with spaces/dashes/accents.

QA (production): run 1 creates the destination table; run 2 into that table is
blocked or loses columns when the source column names contain spaces, dashes or
accents. The earlier fix was proven only through ``execute_tracked`` with a
SQLite source into PostgreSQL. This drives QA's route on every SQL engine here:

  MCP ``start_transfer`` (plan + preflight) -> ``/api/v1/copilot/confirm`` -> job

twice, PostgreSQL source, PostgreSQL / MySQL 8 / SQL Server 2022 destination.
Run 2 needs no override of any kind, and every value (NULL, "", whitespace,
unicode) reads back in the column it was written to.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

# Live engines: a hung run fails instead of holding a session (pytest-timeout).
pytestmark = pytest.mark.timeout(300)

# Source column names QA named: spaces, dashes, accents (and a mix).
_COLS = ["id", "First Name", "last-name", "Café", "Straße Nr", "émoji-ñ col", "updated-at"]
_ROWS = [
    (1, "Ada", "Love-lace", "crème", "Hauptstraße 1", "🎉", 1),
    (2, None, "", "   ", "\t", "山田", 2),
    (3, "Łukasz", None, "", None, "   ", 3),
]
_RUN2 = [(4, "Zoë", "O'Neil", None, "Nº 5", "", 4)]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


_TRANSLITERATED = {
    "id", "first_name", "last_name", "cafe", "strasse_nr", "emoji_n_col", "updated_at",
}
# What run 1 created before transliteration: Café -> Caf_, Straße -> Stra_e.
_LEGACY = ["id", "First_Name", "last_name", "Caf_", "Stra_e_Nr", "_moji___col", "updated_at"]


def _dest_columns(kind: str, table: str) -> list[str]:
    return [
        str(c[0]) for c in h.query(
            kind,
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{table}' ORDER BY ordinal_position",
        )
    ]


def _dest_values(kind: str, table: str) -> dict[int, tuple]:
    """id -> the five text cells, read back by ordinal so no name is assumed."""
    cols = h.query(
        kind,
        (
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_name = '{table}' ORDER BY ordinal_position"
        ),
    )
    names = [str(c[0]) for c in cols]
    assert len(names) == len(_COLS), names
    sel = ", ".join(h.quote(kind, n) for n in names)
    rows = h.query(kind, f"SELECT {sel} FROM {h.quote(kind, table)}")  # nosec B608
    return {int(r[0]): tuple(r[1:]) for r in rows}


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver"])
@pytest.mark.parametrize("mode", ["full_refresh_append", "full_refresh_overwrite", "upsert"])
def test_second_run_into_created_table_with_spaced_dashed_accented_names(client, kind, mode):
    from services.connector_store import create_connector

    dest_cfg = h.dest_config(kind)
    pg = h.pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"d55_{token}"
    dest_table = f"d55_{kind[:5]}_{token}_out"
    cur = pg.cursor()
    col_ddl = ", ".join(
        f'"{c}" BIGINT PRIMARY KEY' if c == "id"
        else f'"{c}" BIGINT' if c == "updated-at"
        else f'"{c}" TEXT'
        for c in _COLS
    )
    try:
        cur.execute(f"CREATE TABLE {table} ({col_ddl})")  # nosec B608
        ins = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(_COLS))})"  # nosec B608
        cur.executemany(ins, _ROWS)
        src = create_connector(
            {"name": f"d55-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d55-{kind}-{token}", "role": "destination", "workspace_id": "",
             "ssl": False, **dest_cfg}
        )
        args = {
            "source_connector_id": src.id,
            "source_table": table,
            "dest_connector_id": dst.id,
            "dest_table": dest_table,
            "validation_mode": "strict",
            "sync_mode": mode,
        }
        if mode == "upsert":
            args.update(upsert_key="id", cursor_column="updated-at",
                        cursor_semantics="monotonic_sequence")
        # Run 1 creates the table.
        h.stage_and_run(client, "start_transfer", args)
        first = _dest_values(kind, dest_table)
        assert first == {r[0]: tuple(r[1:]) for r in _ROWS}
        # Accented letters are transliterated, not replaced: Café -> Cafe.
        assert {c.lower() for c in _dest_columns(kind, dest_table)} == _TRANSLITERATED

        cur.executemany(ins, _RUN2)
        cur.execute(
            f'UPDATE {table} SET "First Name" = %s, "updated-at" = 10 WHERE id = 1',  # nosec B608
            ("Ада",),
        )
        # Run 2 into the table DataFlow created: no override, no acknowledgement.
        h.stage_and_run(client, "start_transfer", args)
        second = _dest_values(kind, dest_table)
        changed = [(1, "Ада", *_ROWS[0][2:-1], 10)]
        expected = {r[0]: tuple(r[1:]) for r in _ROWS + _RUN2 + changed}
        if mode == "full_refresh_append":
            # Append re-sends every row; each value still lands in its own column.
            count = h.query(kind, f"SELECT COUNT(*) FROM {h.quote(kind, dest_table)}")[0][0]
            assert int(count) == 2 * len(_ROWS) + len(_RUN2)
            got = h.query(
                kind,
                f"SELECT COUNT(*) FROM {h.quote(kind, dest_table)} WHERE id = 1",  # nosec B608
            )[0][0]
            assert int(got) == 2
            expected[1] = second[1]  # which of the two id=1 rows the dict kept
            assert second[1] in {tuple(_ROWS[0][1:]), tuple(changed[0][1:])}
        assert second == expected
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        h.drop_table(kind, dest_table)


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver"])
def test_run_two_into_a_table_created_under_the_old_sanitised_names(client, kind):
    """A table run 1 created as ``Caf_`` / ``Stra_e_Nr`` still takes run 2."""
    from services.connector_store import create_connector

    dest_cfg = h.dest_config(kind)
    pg = h.pg_conn()
    token = uuid.uuid4().hex[:8]
    table = f"d55l_{token}"
    dest_table = f"d55l_{kind[:5]}_{token}_out"
    cur = pg.cursor()
    text = {"postgresql": "TEXT", "mysql": "TEXT", "sqlserver": "NVARCHAR(MAX)"}[kind]
    col_ddl = ", ".join(
        f'"{c}" BIGINT PRIMARY KEY' if c == "id" else f'"{c}" BIGINT' if c == "updated-at"
        else f'"{c}" TEXT'
        for c in _COLS
    )
    legacy_ddl = ", ".join(
        f"{h.quote(kind, c)} BIGINT" if c in {"id", "updated_at"} else f"{h.quote(kind, c)} {text}"
        for c in _LEGACY
    )
    try:
        cur.execute(f"CREATE TABLE {table} ({col_ddl})")  # nosec B608
        ins = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(_COLS))})"  # nosec B608
        cur.executemany(ins, _ROWS)
        h.execute(kind, f"CREATE TABLE {h.quote(kind, dest_table)} ({legacy_ddl})")  # nosec B608
        src = create_connector(
            {"name": f"d55l-pg-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d55l-{kind}-{token}", "role": "destination", "workspace_id": "",
             "ssl": False, **dest_cfg}
        )
        h.stage_and_run(client, "start_transfer", {
            "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "validation_mode": "strict", "sync_mode": "full_refresh_append",
        })
        assert _dest_columns(kind, dest_table) == _LEGACY  # no column added
        assert _dest_values(kind, dest_table) == {r[0]: tuple(r[1:]) for r in _ROWS}
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        h.drop_table(kind, dest_table)
