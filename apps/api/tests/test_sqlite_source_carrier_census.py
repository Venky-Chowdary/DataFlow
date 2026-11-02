"""An identity COPY out of SQLite must prove every stored cell fits the carrier.

SQLite never enforces a column's declared type, so a ``NUMERIC`` column can
hold ``'not-a-number'`` and an ``INTEGER`` one can hold ``'abc'``. Before the
census those bytes moved verbatim through ``INSERT … SELECT`` into a declared
DECIMAL / INTEGER destination with ``rejected_rows == 0`` and every validation
mode green. The census runs on the engine, over the exact population the COPY
would move, and declines so the row path quarantines or fails the row.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from services.copy_fast_path import FastPathUnavailable
from services.copy_sqlite_common import sqlite_source_carrier_census
from services.copy_sqlite_sqlite import copy_sqlite_to_sqlite


def _db(tmp_path: Path, rows: list[tuple], ddl: str = "id INTEGER, v") -> Path:
    path = tmp_path / "src.db"
    conn = sqlite3.connect(path)
    conn.execute(f"CREATE TABLE t ({ddl})")
    conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return path


def _census(path: Path, ddl: str, where: str = "") -> None:
    conn = sqlite3.connect(path)
    try:
        sqlite_source_carrier_census(conn, "t", ["v"], [ddl], where)
    finally:
        conn.close()


@pytest.mark.parametrize("ddl", ["INTEGER", "BIGINT", "SMALLINT"])
def test_integer_carrier_declines_text_and_real_cells(tmp_path, ddl):
    src = _db(tmp_path, [(1, 1), (2, "abc"), (3, 2.5), (4, None)])
    with pytest.raises(FastPathUnavailable) as exc:
        _census(src, ddl)
    assert "'v'" in str(exc.value)
    assert "2 cell(s)" in str(exc.value)


def test_integer_carrier_accepts_integers_and_nulls(tmp_path):
    src = _db(tmp_path, [(1, 1), (2, -7), (3, None)])
    _census(src, "INTEGER")


def test_real_carrier_accepts_numbers(tmp_path):
    _census(_db(tmp_path, [(1, 1), (2, 2.5), (3, None)]), "REAL")


def test_real_carrier_declines_text(tmp_path):
    with pytest.raises(FastPathUnavailable):
        _census(_db(tmp_path, [(1, "1.5x")]), "REAL")


@pytest.mark.parametrize("cell", ["10.50", "-0.001", 3, 2.5, None])
def test_decimal_carrier_accepts_canonical_numbers(tmp_path, cell):
    _census(_db(tmp_path, [(1, cell)]), "DECIMAL(12,3)")


@pytest.mark.parametrize("cell", ["not-a-number", "1,234", "$10", "", " 1.5 x"])
def test_decimal_carrier_declines_non_canonical_text(tmp_path, cell):
    with pytest.raises(FastPathUnavailable) as exc:
        _census(_db(tmp_path, [(1, "10.50"), (2, cell)]), "NUMERIC")
    assert "1 cell(s)" in str(exc.value)


@pytest.mark.parametrize("cell", [0, 1, "0", "1", None])
def test_boolean_carrier_accepts_binary_cells(tmp_path, cell):
    _census(_db(tmp_path, [(1, cell)]), "BOOLEAN")


@pytest.mark.parametrize("cell", [2, "true", "yes", 1.0])
def test_boolean_carrier_declines_anything_else(tmp_path, cell):
    with pytest.raises(FastPathUnavailable):
        _census(_db(tmp_path, [(1, cell)]), "BOOLEAN")


def test_text_carrier_never_censuses(tmp_path):
    _census(_db(tmp_path, [(1, "anything"), (2, 5), (3, 1.5)]), "TEXT")


def test_census_is_scoped_to_the_incremental_predicate(tmp_path):
    src = _db(tmp_path, [(1, "bad"), (2, 5), (3, 6)])
    _census(src, "INTEGER", " WHERE id > 1")
    with pytest.raises(FastPathUnavailable):
        _census(src, "INTEGER", " WHERE id >= 1")


def test_identity_copy_declines_before_creating_the_destination(tmp_path):
    src = _db(tmp_path, [(1, "10.50"), (2, "not-a-number"), (3, "20.00")], "id INTEGER, v TEXT")
    dst = tmp_path / "dst.db"
    with pytest.raises(FastPathUnavailable, match="not-a-number"):
        copy_sqlite_to_sqlite(
            source_cfg={"database": str(src)},
            source_table="t",
            dest_cfg={"database": str(dst)},
            dest_table="out",
            pairs=[("id", "id"), ("v", "v")],
            sqlite_ddls=["INTEGER", "NUMERIC"],
            replace_destination=False,
        )
    if dst.exists():
        conn = sqlite3.connect(dst)
        try:
            names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            conn.close()
        assert "out" not in names


def test_identity_copy_still_moves_a_clean_decimal_population(tmp_path):
    src = _db(tmp_path, [(1, "10.50"), (2, "20.00")], "id INTEGER, v TEXT")
    dst = tmp_path / "dst.db"
    result = copy_sqlite_to_sqlite(
        source_cfg={"database": str(src)},
        source_table="t",
        dest_cfg={"database": str(dst)},
        dest_table="out",
        pairs=[("id", "id"), ("v", "v")],
        sqlite_ddls=["INTEGER", "NUMERIC"],
        replace_destination=False,
    )
    assert result.rows_copied == 2
    assert result.target_rows == 2
