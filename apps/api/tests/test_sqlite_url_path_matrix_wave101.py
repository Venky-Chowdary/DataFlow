"""Wave 101: SQLite destinations configured by URL must resolve to a real path.

`table_manager` used to pass the raw ``sqlite:///...`` connection string to
``sqlite3.connect``, so every URL-configured SQLite destination failed its
full-refresh DROP, CDC delete, and LSN read-back with "unable to open database
file". The DROP failure was fail-closed (the transfer errored), but the delete
paths returned ``0`` — an idempotent-success lie that advanced the CDC cursor
past tombstones that were never applied.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.cdc_snapshot_window import _PK_SEP

from connectors.table_manager import (
    DestinationDeleteError,
    _fetch_composite_pk_lsn_map,
    delete_by_primary_keys,
    drop_table,
)


def _seed(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE "users" (id TEXT PRIMARY KEY, region TEXT, _df_lsn TEXT)')
    conn.executemany(
        'INSERT INTO "users" (id, region, _df_lsn) VALUES (?, ?, ?)',
        [("1", "us", "0/100"), ("2", "eu", "0/200"), ("3", "us", "0/300")],
    )
    conn.commit()
    conn.close()


def _table_exists(path: str, table: str) -> bool:
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "dest.db")
    _seed(path)
    return path


def test_drop_table_resolves_sqlite_url(db_path):
    cfg = {"connection_string": f"sqlite:///{db_path}"}
    assert drop_table("sqlite", cfg, "users") is True
    assert _table_exists(db_path, "users") is False


def test_drop_table_resolves_plain_path(db_path):
    cfg = {"database": db_path}
    assert drop_table("sqlite", cfg, "users") is True
    assert _table_exists(db_path, "users") is False


def test_delete_by_primary_keys_resolves_sqlite_url(db_path):
    cfg = {"connection_string": f"sqlite:///{db_path}"}
    deleted = delete_by_primary_keys("sqlite", cfg, "users", "id", ["1", "2"])
    assert deleted == 2
    conn = sqlite3.connect(db_path)
    remaining = [r[0] for r in conn.execute('SELECT id FROM "users" ORDER BY id')]
    conn.close()
    assert remaining == ["3"]


def test_composite_delete_resolves_sqlite_url(db_path):
    cfg = {"connection_string": f"sqlite:///{db_path}"}
    deleted = delete_by_primary_keys(
        "sqlite",
        cfg,
        "users",
        ["id", "region"],
        [_PK_SEP.join(("1", "us")), _PK_SEP.join(("2", "eu"))],
    )
    assert deleted == 2
    conn = sqlite3.connect(db_path)
    remaining = [r[0] for r in conn.execute('SELECT id FROM "users" ORDER BY id')]
    conn.close()
    assert remaining == ["3"]


def test_composite_lsn_map_resolves_sqlite_url(db_path):
    cfg = {"connection_string": f"sqlite:///{db_path}"}
    k1 = _PK_SEP.join(("1", "us"))
    k3 = _PK_SEP.join(("3", "us"))
    lsn_map = _fetch_composite_pk_lsn_map(
        "sqlite",
        cfg,
        "users",
        ["id", "region"],
        [k1, k3],
        None,
        lsn_column="_df_lsn",
    )
    assert lsn_map.get(k1) == "0/100"
    assert lsn_map.get(k3) == "0/300"


def test_unresolvable_sqlite_delete_raises_instead_of_reporting_zero():
    # No database/connection_string at all: a 0 here would read as "those keys
    # were already absent" and let the CDC cursor advance past real tombstones.
    with pytest.raises(DestinationDeleteError):
        delete_by_primary_keys("sqlite", {}, "users", "id", ["1"])


def test_unresolvable_sqlite_composite_delete_raises():
    with pytest.raises(DestinationDeleteError):
        delete_by_primary_keys(
            "sqlite", {}, "users", ["id", "region"], [_PK_SEP.join(("1", "us"))]
        )


def test_unresolvable_sqlite_lsn_map_raises_instead_of_empty():
    # An empty LSN map means "no existing rows", which disables the stale-write
    # guard and lets an out-of-order change overwrite newer destination values.
    with pytest.raises(RuntimeError, match="could not be resolved"):
        _fetch_composite_pk_lsn_map(
            "sqlite",
            {},
            "users",
            ["id", "region"],
            [_PK_SEP.join(("1", "us"))],
            None,
            lsn_column="_df_lsn",
        )
