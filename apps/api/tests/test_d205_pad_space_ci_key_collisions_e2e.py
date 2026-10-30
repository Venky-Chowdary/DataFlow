"""Provisional D205 — distinct source keys that the destination compares as equal.

MySQL, MariaDB and SQL Server compare ``VARCHAR`` under PAD SPACE (``''`` =
``' '``, ``'a'`` = ``'a '``), and a ``_ci`` / ``CI_AS`` collation also equates
``'a'`` and ``'A'``. On the base, when the colliding keys sat beyond the
Validate sample, the upsert ran: MySQL / MariaDB failed closed on the row-count
proof and rolled back; SQL Server died on a raw ``IntegrityError`` after 2,048
rows were committed.

The decided principle: never silently merge distinct source keys. Preflight now
walks the key population under the destination's own comparison and refuses,
naming the colliding keys and the remedy. ``DATAFLOW_KEY_COLLATION_NO_PAD=1``
(off by default) makes a table DataFlow creates on MySQL / MariaDB carry the
NO PAD binary collation, so all five keys load; run 2 into a table created
before the flag still works.

QA's route: PostgreSQL source -> MCP ``start_transfer`` ->
``/api/v1/copilot/confirm`` -> job, on live MySQL 8, MariaDB 11, SQL Server 2022.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_r4_upsert_identity_e2e import (  # noqa: F401 - fixtures
    _confirm,
    _connectors,
    _drop,
    _exec,
    _fakesnow_path,
    _keyed,
    _pg_conn,
    _q,
    _select,
    _stage,
    client,
)

KEYS = ["", " ", "a", "A", "a "]
FILL = 3000  # the colliding keys sit beyond every Validate sample


def _seed(pg, table: str, keys: list[str], fill: int = FILL) -> None:
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id TEXT PRIMARY KEY, note TEXT, "  # nosec B608
                "updated_at BIGINT NOT NULL)")
    rows = [(f"k{i:05d}", f"f{i}", i) for i in range(fill)]
    rows += [(k, f"n{i}", fill + i) for i, k in enumerate(keys)]
    cur.executemany(f"INSERT INTO {table} VALUES (%s, %s, %s)", rows)  # nosec B608


def _named(kind: str, dest_table: str) -> dict[str, str]:
    rows = _select(kind, f"SELECT id, note FROM {_q(kind, dest_table)}")  # nosec B608
    return {r[0]: r[1] for r in rows if not str(r[0]).startswith("k0")}


def _key_collation(kind: str, dest_table: str) -> str:
    if kind == "sqlserver":
        rows = _select(kind, "SELECT collation_name FROM sys.columns WHERE name = 'id' AND "
                             f"object_id = OBJECT_ID('dbo.{dest_table}')")
    else:
        rows = _select(kind, "SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE "
                             f"TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{dest_table}' "
                             "AND COLUMN_NAME = 'id'")
    return str(rows[0][0]) if rows else ""


def _exists(kind: str, dest_table: str) -> bool:
    if kind == "sqlserver":
        return bool(_select(kind, f"SELECT 1 FROM sys.tables WHERE name = '{dest_table}'"))
    return bool(_select(kind, "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE "
                              f"TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{dest_table}'"))


def _same_group(text: str, a: str, b: str) -> bool:
    return f"{a!r} = {b!r}" in text or f"{b!r} = {a!r}" in text


def _refusal(staged, blockers) -> str:
    assert not staged.success, "a merge of distinct source keys was staged"
    return f"{staged.error} {' '.join(blockers)}"


@pytest.mark.parametrize("kind", ["mysql", "mariadb", "sqlserver"])
def test_new_table_refuses_pad_space_collisions_by_name(client, kind, monkeypatch):
    monkeypatch.delenv("DATAFLOW_KEY_COLLATION_NO_PAD", raising=False)
    pg, token = _pg_conn(), uuid.uuid4().hex[:8]
    table, dest_table = f"d205_s_{token}", f"d205_{kind[:4]}_{token}"
    try:
        _seed(pg, table, KEYS)
        src, dst = _connectors(token, kind)
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **_keyed("incremental_upsert"))
        text = _refusal(staged, blockers)
        assert "equal under the destination's comparison" in text, text
        assert _same_group(text, "", " ") and _same_group(text, "a", "a "), text
        # The table DataFlow creates is binary: case stays distinct.
        assert not _same_group(text, "a", "A"), text
        assert "PAD SPACE" in text
        assert ("BIN2" in text) if kind == "sqlserver" else ("NO PAD" in text), text
        assert not _exists(kind, dest_table), "nothing is created or written"
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


_CI_DDL = {
    "mysql": "CREATE TABLE `{t}` (id VARCHAR(100) COLLATE utf8mb4_general_ci NOT NULL "
             "PRIMARY KEY, note TEXT, updated_at BIGINT)",
    "mariadb": "CREATE TABLE `{t}` (id VARCHAR(100) COLLATE utf8mb4_general_ci NOT NULL "
               "PRIMARY KEY, note TEXT, updated_at BIGINT)",
    "sqlserver": "CREATE TABLE [{t}] (id NVARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS "
                 "NOT NULL PRIMARY KEY, note NVARCHAR(100), updated_at BIGINT)",
}


@pytest.mark.parametrize("kind", ["mysql", "mariadb", "sqlserver"])
def test_existing_ci_table_refuses_case_and_pad_collisions_by_name(client, kind):
    pg, token = _pg_conn(), uuid.uuid4().hex[:8]
    table, dest_table = f"d205_s_{token}", f"d205_{kind[:4]}_{token}"
    try:
        _seed(pg, table, KEYS)
        _exec(kind, _CI_DDL[kind].format(t=dest_table))
        src, dst = _connectors(token, kind)
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **_keyed("incremental_upsert"))
        text = _refusal(staged, blockers)
        assert "case is ignored" in text, text
        assert _same_group(text, "", " "), text
        assert "'A'" in text and "'a'" in text, text
        assert _named(kind, dest_table) == {}, "nothing written"
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


@pytest.mark.parametrize("kind", ["mysql", "mariadb"])
def test_no_pad_flag_creates_a_key_that_keeps_all_five_keys(client, kind, monkeypatch):
    monkeypatch.setenv("DATAFLOW_KEY_COLLATION_NO_PAD", "1")
    pg, token = _pg_conn(), uuid.uuid4().hex[:8]
    table, dest_table = f"d205_s_{token}", f"d205_{kind[:4]}_{token}"
    try:
        _seed(pg, table, KEYS)
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        assert staged.success and not blockers, (staged.error, blockers)
        job = _confirm(client, staged)
        assert job.get("status") == "completed", job.get("error")
        # Both are NO PAD binaries; MariaDB 11.4+ also knows MySQL's 0900 name.
        assert _key_collation(kind, dest_table) in (
            {"utf8mb4_0900_bin"} if kind == "mysql"
            else {"utf8mb4_0900_bin", "utf8mb4_nopad_bin"})
        assert _named(kind, dest_table) == {k: f"n{i}" for i, k in enumerate(KEYS)}

        # Run 2 into the table DataFlow created: each key updates only itself.
        pg.cursor().execute(f"UPDATE {table} SET note = 'v2:' || length(id), "  # nosec B608
                            "updated_at = updated_at + 10000 WHERE id IN ('', 'a ', 'A')")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        assert staged.success and not blockers, (staged.error, blockers)
        assert _confirm(client, staged).get("status") == "completed"
        assert _named(kind, dest_table) == {"": "v2:0", " ": "n1", "a": "n2", "A": "v2:1",
                                            "a ": "v2:2"}
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)


_CREATED_BEFORE = {"mysql": "utf8mb4_bin", "mariadb": "utf8mb4_bin",
                   "sqlserver": "Latin1_General_BIN"}


@pytest.mark.parametrize("kind", ["mysql", "mariadb", "sqlserver"])
def test_run2_into_a_table_created_before_the_flag_still_works(client, kind, monkeypatch):
    """A table created with the binary collation (flag off) keeps loading with
    the flag on, and run 2 updating 'A' beside 'a' passes Gate-8: the keyed
    read-back used the session's _ci collation and fetched both rows."""
    monkeypatch.delenv("DATAFLOW_KEY_COLLATION_NO_PAD", raising=False)
    pg, token = _pg_conn(), uuid.uuid4().hex[:8]
    table, dest_table = f"d205_s_{token}", f"d205_{kind[:4]}_{token}"
    keys = ["a", "A", "b", "Łukasz 山田 🎉"]
    try:
        _seed(pg, table, keys, fill=50)
        src, dst = _connectors(token, kind)
        keyed = _keyed("incremental_upsert")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        assert staged.success and not blockers, (staged.error, blockers)
        assert _confirm(client, staged).get("status") == "completed"
        assert _key_collation(kind, dest_table) == _CREATED_BEFORE[kind]

        monkeypatch.setenv("DATAFLOW_KEY_COLLATION_NO_PAD", "1")
        pg.cursor().execute(f"UPDATE {table} SET note = 'v2', updated_at = updated_at + 1000 "  # nosec B608
                            "WHERE id IN ('A', 'Łukasz 山田 🎉')")
        staged, _plan, blockers = _stage(src.id, dst.id, table, dest_table,
                                         "incremental_upsert", **keyed)
        assert staged.success and not blockers, (staged.error, blockers)
        job = _confirm(client, staged)
        assert job.get("status") == "completed", job.get("error")
        assert _named(kind, dest_table) == {"a": "n0", "A": "v2", "b": "n2",
                                            "Łukasz 山田 🎉": "v2"}
        assert _key_collation(kind, dest_table) == _CREATED_BEFORE[kind], "never re-collated"
    finally:
        pg.cursor().execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()
        _drop(kind, dest_table)
