"""P0: MySQL->SQL Server norecipe clean run truncation + partial commit.

Two defects from the live report (`('String data, right truncation: length 78
buffer 76', 'HY000')` leaving 3 committed rows, regression from 65cb097a):

(a) pyodbc fast_executemany sizes the parameter buffer from the described
    decimal precision — a DECIMAL(38,10) column gets a 76-byte buffer that
    truncates the 39-char fixed-point text a >28-digit bind sends. The insert
    placeholder for wide-decimal dest columns wraps in ``CAST(%s AS
    DECIMAL(p,s))`` and the param position is declared SQL_VARCHAR via
    ``setinputsizes`` so the text binds whole.

(b) A failed run whose dest table was created by that run (or was empty
    before) must leave 0 committed rows — matching the row-path X-9 undo.
    ``preserve_dest_on_failure`` (PK-shard resume) only applies to a
    pre-existing *occupied* dest.

The dest-end state is simulated with a fake SQL Server connection: a
mid-stream executemany error after committed batches proves the run leaves
the destination empty (drop for created-here, truncate for existing-empty,
preserve for occupied-pk-resume).
"""

from __future__ import annotations

import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services import copy_mysql_sqlserver as cms  # noqa: E402
import services.copy_pg_sqlserver as cps  # noqa: E402

WIDE = "9999999999999999999999999999.9999999999"


# ---------------------------------------------------------------------------
# Fix 1a — placeholder CAST + setinputsizes for wide-decimal dest columns
# ---------------------------------------------------------------------------


def test_placeholders_cast_only_wide_decimal_columns():
    sql = cps.sqlserver_insert_placeholders(
        ["INT", "DECIMAL(38,10)", "NUMERIC(10,2)", "VARCHAR(50)", "DECIMAL(38)"]
    )
    assert sql == (
        "%s, CAST(%s AS DECIMAL(38,10)), %s, %s, CAST(%s AS DECIMAL(38,0))"
    )


def test_placeholders_leave_narrow_and_nondecimal_untouched():
    sql = cps.sqlserver_insert_placeholders(["DECIMAL(28,4)", "DATETIME2(7)", "DECIMAL"])
    assert sql == "%s, %s, %s"


class _PyodbcCursor:
    __module__ = "pyodbc"

    def __init__(self):
        self.input_sizes = None

    def setinputsizes(self, *args):
        # Real pyodbc takes exactly one argument — the sizes sequence.
        if len(args) != 1:
            raise TypeError(
                "Cursor.setinputsizes() takes exactly one argument "
                f"({len(args)} given)"
            )
        (self.input_sizes,) = args


def _fake_pyodbc() -> types.SimpleNamespace:
    # Constants the column-descriptor builder reads for every position —
    # msodbcsql 18 drops declarations at the first ``None`` slot, so the
    # fix gives every position a real type entry, not a placeholder.
    return types.SimpleNamespace(
        SQL_VARCHAR=-9,
        SQL_WVARCHAR=-10,
        SQL_DECIMAL=3,
        SQL_INTEGER=4,
        SQL_BIGINT=-5,
        SQL_SMALLINT=5,
        SQL_TINYINT=-6,
    )


def test_setinputsizes_declares_varchar_for_wide_decimal_positions():
    fake_pyodbc = _fake_pyodbc()
    monkey = pytest.MonkeyPatch()
    monkey.setitem(sys.modules, "pyodbc", fake_pyodbc)
    try:
        cur = _PyodbcCursor()
        cps.set_wide_decimal_inputsizes(
            cur, ["INT", "DECIMAL(38,10)", "DECIMAL(10,2)", "DECIMAL(38)"]
        )
        assert list(cur.input_sizes) == [
            4,  # SQL_INTEGER — real descriptor, never a None placeholder
            (-9, 41),  # 38 digits + sign + point
            (3, 10, 2),  # SQL_DECIMAL(10,2)
            (-9, 41),
        ]
    finally:
        monkey.undo()


def test_setinputsizes_noop_without_wide_decimal():
    fake_pyodbc = _fake_pyodbc()
    monkey = pytest.MonkeyPatch()
    monkey.setitem(sys.modules, "pyodbc", fake_pyodbc)
    try:
        cur = _PyodbcCursor()
        cps.set_wide_decimal_inputsizes(cur, ["DECIMAL(10,2)", "INT"])
        assert cur.input_sizes is None
    finally:
        monkey.undo()


def test_setinputsizes_noop_for_pymssql_cursor():
    class _PymssqlCursor:
        __module__ = "pymssql"

    cur = _PymssqlCursor()
    cps.set_wide_decimal_inputsizes(cur, ["DECIMAL(38,10)"])
    assert not hasattr(cur, "input_sizes")


# ---------------------------------------------------------------------------
# Fix 1b — failed run leaves the destination byte-identical (0 committed rows)
# ---------------------------------------------------------------------------


class _FakeSrcCursor:
    def __init__(self, count):
        self.count = count

    def execute(self, sql, *args):
        return self

    def fetchone(self):
        return (self.count,)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeSrcConn:
    def cursor(self, *args, **kwargs):
        return _FakeSrcCursor(6)

    def commit(self):
        pass

    def close(self):
        pass


class _FakeDstCursor:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        upper = str(sql).upper()
        if "DROP TABLE" in upper:
            self._conn.dropped = True
            self._conn.rows.clear()
        if "TRUNCATE TABLE" in upper:
            self._conn.rows.clear()
        self._conn.statements.append(str(sql))
        return self

    def fetchone(self):
        return (0,)

    def close(self):
        pass


class _FakeDstConn:
    """Models a SQL Server dest table whose committed rows are observable."""

    def __init__(self):
        self.rows = []
        self.statements = []
        self.dropped = False

    def cursor(self):
        return _FakeDstCursor(self)

    def commit(self):
        pass

    def close(self):
        pass


def _patch_common(monkeypatch, dst_conn, *, table_exists, dest_rows):
    """Wire the copy's driver seams to fakes; only the copy stream is real."""
    pairs = [("id", "id"), ("name", "name"), ("amount", "amount")]
    ddls = ["INT", "VARCHAR(20)", "DECIMAL(38,10)"]

    monkeypatch.setattr(cms, "_mysql_connect", lambda cfg: _FakeSrcConn())
    monkeypatch.setattr(cms, "_ss_connect", lambda cfg: dst_conn)
    monkeypatch.setattr(
        cms, "_mysql_table_pk_and_types",
        lambda cur, table, cols: (["id"], {"id": "int", "name": "varchar(20)", "amount": "decimal(38,10)"}),
    )
    monkeypatch.setattr(cms, "mysql_type_is_copy_safe", lambda ddl: True)
    monkeypatch.setattr(cms, "mapped_single_pk", lambda pk, prs: ("id", "id"))
    monkeypatch.setattr(cms, "_ss_table_exists", lambda *a, **k: table_exists)
    monkeypatch.setattr(cms, "_ss_count", lambda *a, **k: dest_rows)
    monkeypatch.setattr(cms, "_ss_create_sql", lambda *a, **k: "CREATE TABLE t")
    monkeypatch.setattr(cms, "_ss_drop_sql", lambda ref: f"DROP TABLE {ref}")
    monkeypatch.setattr(cms, "_has_identity", lambda *a, **k: False)
    monkeypatch.setattr(cms, "settle_fast_path_create_on", lambda *a, **k: None)
    monkeypatch.setattr(cms, "pg_mysql_copy_workers", lambda n: 1)
    monkeypatch.setattr(cms, "pg_mysql_copy_partitions", lambda n, w: 1)
    monkeypatch.setattr(
        cms, "_plan_pk_partitions",
        lambda *a, **k: [{"lo": None, "hi": None, "null_shard": False, "source_count": 6}],
    )
    monkeypatch.setattr(cms, "_ss_range_count", lambda *a, **k: 0)
    monkeypatch.setattr(cms, "occupied_pk_range_action", lambda *a, **k: None)
    monkeypatch.setattr(cms, "_ss_delete_range", lambda *a, **k: None)
    monkeypatch.setattr(cms, "_close_ss", lambda c: c.close())

    def _midstream_failure(src_conn, dst_cur, **kwargs):
        # The live error arrives after earlier batches already committed.
        dst_cur._conn.rows.extend([1, 2, 3])
        raise RuntimeError(
            "('String data, right truncation: length 78 buffer 76', 'HY000')"
        )

    monkeypatch.setattr(cms, "_select_into_sqlserver", _midstream_failure)
    return pairs, ddls


def _run_copy(dst_conn, monkeypatch, *, table_exists, dest_rows):
    pairs, ddls = _patch_common(
        monkeypatch, dst_conn, table_exists=table_exists, dest_rows=dest_rows
    )
    with pytest.raises(RuntimeError, match="right truncation"):
        cms.copy_mysql_to_sqlserver(
            source_cfg={"host": "localhost"},
            source_table="src_t",
            dest_cfg={"host": "localhost"},
            dest_table="dst_t",
            pairs=pairs,
            sqlserver_ddls=ddls,
            replace_destination=False,
        )


def test_failure_drops_table_created_by_this_run(monkeypatch):
    """pk_map sets preserve_dest_on_failure BEFORE occupancy — on the buggy
    ordering the preserve early-raise skipped the created_here drop and the
    live run left 3 committed rows."""
    dst = _FakeDstConn()
    _run_copy(dst, monkeypatch, table_exists=False, dest_rows=0)
    assert dst.rows == []
    assert dst.dropped
    assert any("DROP TABLE" in s.upper() for s in dst.statements)


def test_failure_truncates_existing_empty_dest(monkeypatch):
    """An existing-but-empty dest must end byte-identical: 0 rows, table kept."""
    dst = _FakeDstConn()
    _run_copy(dst, monkeypatch, table_exists=True, dest_rows=0)
    assert dst.rows == []
    assert not dst.dropped
    assert any("TRUNCATE TABLE" in s.upper() for s in dst.statements)


def test_failure_preserves_occupied_dest_for_pk_resume(monkeypatch):
    """A pre-existing occupied dest keeps committed rows — the rerun's
    pk-range skip depends on them."""
    dst = _FakeDstConn()
    _run_copy(dst, monkeypatch, table_exists=True, dest_rows=5)
    assert dst.rows == [1, 2, 3]
    assert not any(
        tok in s.upper() for s in dst.statements for tok in ("DROP TABLE", "TRUNCATE TABLE")
    )
