"""X-40 + undo-208 residual + engine-copy message ordering regressions.

X-40: ``set_wide_decimal_inputsizes`` called pyodbc's ``setinputsizes``
with N positional args (``setter(*sizes)``); the driver takes exactly ONE
sequence argument, so every precision>28 bind crashed with a raw
``TypeError: Cursor.setinputsizes() takes exactly one argument (N given)``.
Now the sizes list is passed as the single argument and a driver quirk
raises ``FastPathUnavailable`` — the COPY fast path declines to the row
path where every bind failure is a typed finding and read-back verifies
exact precision. A driver call must never escape as a raw error, and a
failed size declaration must never silently degrade precision.

undo-208: on the streaming FAIL_JOB path the undo entry point
``_clear`` issued ``DELETE FROM [dbo].[x_t4ms_…]`` against a table the run
never created — logged as 'Failed-batch undo did not run … Invalid object
name (208)'. A destination that probes to 0 rows (missing == empty) has
nothing to delete; the DELETE must not be issued at all.
"""

from __future__ import annotations

import sys
import types


def _fake_pyodbc_module() -> types.ModuleType:
    mod = types.ModuleType("pyodbc")
    # Real constant values where the test asserts on them; unique sentinels
    # for the rest so a wrong-but-set slot can't pass as None.
    mod.SQL_VARCHAR = -9
    for name in (
        "SQL_WVARCHAR",
        "SQL_DECIMAL",
        "SQL_BIGINT",
        "SQL_INTEGER",
        "SQL_SMALLINT",
        "SQL_TINYINT",
        "SQL_BIT",
        "SQL_FLOAT",
        "SQL_REAL",
        "SQL_TYPE_DATE",
        "SQL_TYPE_TIME",
        "SQL_TYPE_TIMESTAMP",
        "SQL_SS_TIMESTAMPOFFSET",
        "SQL_VARBINARY",
        "SQL_LONGVARBINARY",
        "SQL_WLONGVARCHAR",
        "SQL_LONGVARCHAR",
    ):
        setattr(mod, name, name)
    return mod


class _PyodbcCursor:
    __module__ = "pyodbc"

    def __init__(self):
        self.calls: list[tuple] = []

    def setinputsizes(self, *args):
        self.calls.append(args)
        # Mirror the real driver: exactly one argument accepted.
        if len(args) != 1:
            raise TypeError(
                f"Cursor.setinputsizes() takes exactly one argument ({len(args)} given)"
            )


def test_x40_inputsizes_called_with_single_sequence(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyodbc", _fake_pyodbc_module())
    from services.copy_pg_sqlserver import set_wide_decimal_inputsizes

    cur = _PyodbcCursor()
    set_wide_decimal_inputsizes(
        cur, ["INT", "DECIMAL(38,10)", "DECIMAL(30,5)", "VARCHAR(50)"]
    )
    assert len(cur.calls) == 1, "setinputsizes must be called exactly once"
    (sizes,) = cur.calls[0]
    assert isinstance(sizes, list) and len(sizes) == 4
    # X-40 live (HY090): NO slot may be None — msodbcsql 18 maps a None
    # element to a zero-length parameter and executemany dies with
    # 'Invalid string or buffer length (0)'. Every column gets a real
    # descriptor; only the >28-precision ones declare the varchar text
    # bind that the CAST placeholder converts.
    assert all(s is not None for s in sizes), sizes
    assert sizes[1] == (-9, 41)  # SQL_VARCHAR, p+3 — DECIMAL(38,10)
    assert sizes[2] == (-9, 33)  # DECIMAL(30,5) is also >28


def test_x40_driver_quirk_declines_to_row_path(monkeypatch):
    """A setinputsizes that rejects the wide-decimal declarations must
    decline the fast path (FastPathUnavailable -> row path with per-cell
    typed findings), not swallow a warning and risk a truncated bind, and
    not leak the raw driver error."""
    import pytest

    monkeypatch.setitem(sys.modules, "pyodbc", _fake_pyodbc_module())
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_sqlserver import set_wide_decimal_inputsizes

    class _AngryCursor:
        __module__ = "pyodbc"

        def setinputsizes(self, *_a):
            raise RuntimeError("driver exploded")

    with pytest.raises(FastPathUnavailable, match="setinputsizes rejected"):
        set_wide_decimal_inputsizes(_AngryCursor(), ["DECIMAL(38,10)"])


def test_x40_narrow_precision_skips_setter(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyodbc", _fake_pyodbc_module())
    from services.copy_pg_sqlserver import set_wide_decimal_inputsizes

    cur = _PyodbcCursor()
    set_wide_decimal_inputsizes(cur, ["INT", "DECIMAL(28,10)", "VARCHAR(5)"])
    assert cur.calls == [], "no wide decimals — setter must not be called"


def test_undo208_no_delete_on_never_created_table(monkeypatch):
    """The streaming FAIL_JOB path: dest_summary lacks
    ``dest_table_existed_before=False`` so _clear used to fall straight to
    ``_delete_all_rows`` → 'Invalid object name' 208. With the probe the
    DELETE is never issued."""
    import services.batch_undo as bu

    issued: list[str] = []
    monkeypatch.setattr(bu, "precount_table", lambda *a, **k: 0)

    def _record_delete(*a, **k):
        issued.append(str(a) + str(k))
        return True

    monkeypatch.setattr(bu, "_delete_all_rows", _record_delete)

    # The probe itself must also not blow up for the object-store short-circuit:
    # use a sqlserver-shaped cfg so _object_store_kind returns None.
    dest_summary = {
        "table": "dbo.x_t4ms_never_created",
        "job_id": "t" * 24,
        # intentionally NO dest_table_existed_before — the streaming path
        # doesn't stamp it.
    }
    destination = types.SimpleNamespace(
        format="sqlserver",
        config={"type": "sqlserver", "host": "h", "database": "d"},
        extra={},
    )

    def _resolve_cfg(_d):
        return destination.config

    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config", _resolve_cfg, raising=False
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_dest_table", lambda *a, **k: "x_t4ms_never_created",
        raising=False,
    )

    bu._clear(destination, dest_summary)
    assert issued == [], (
        f"no DELETE may be issued against a never-created table, got {issued}"
    )
    assert dest_summary.get("partial_batch_removed") is True


def test_undo208_existing_table_with_rows_still_deletes(monkeypatch):
    """Rollback correctness: a table that exists with rows MUST be deleted —
    the probe must not neuter real undo."""
    import services.batch_undo as bu

    counts = {"n": 0}
    issued: list[str] = []

    def _precount(*a, **k):
        counts["n"] += 1
        return 5 if counts["n"] == 1 else 0  # before: 5 rows, after: gone

    monkeypatch.setattr(bu, "precount_table", _precount)
    monkeypatch.setattr(
        bu, "_delete_all_rows", lambda *a, **k: issued.append("del") or True
    )

    dest_summary = {"table": "dbo.real_table", "job_id": "t" * 24}
    destination = types.SimpleNamespace(
        format="sqlserver",
        config={"type": "sqlserver", "host": "h", "database": "d"},
        extra={},
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config",
        lambda _d: destination.config,
        raising=False,
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_dest_table",
        lambda *a, **k: "real_table",
        raising=False,
    )

    bu._clear(destination, dest_summary)
    assert issued == ["del"], "an occupied pre-existing table must be undone"


def test_x48_bulk_executemany_uses_wrapped_cursor_for_qmark():
    """X-48: ``bulk_executemany`` must call the cursor's own ``executemany`` —
    ``_MssqlQmarkCursor`` is the layer that rewrites pymssql-style ``%s``
    markers to pyodbc ``?``. Unwrapping ``cur._cur`` skipped the rewrite and
    pyodbc reported "Expected 0 parameters, supplied N", silently declining
    every MySQL/Oracle/Mongo/Iceberg→MSSQL fast path to the row route."""
    from connectors.generic_sql import _MssqlQmarkCursor
    from services.copy_pg_sqlserver import bulk_executemany

    received: list[str] = []

    class _Inner:
        __module__ = "pyodbc"

    class _RecordingQmark(_MssqlQmarkCursor):
        def executemany(self, sql, rows):
            received.append(sql)

    cur = _RecordingQmark(_Inner())
    bulk_executemany(cur, "INSERT INTO t (a, b) VALUES (%s, %s)", [(1, "x")])
    # The wrapper's executemany saw the pymssql-style SQL — proving the call
    # went through it (where %s -> ? adaptation lives), not the raw inner.
    assert received == ["INSERT INTO t (a, b) VALUES (%s, %s)"], received


def test_x48_bulk_executemany_decline_is_typed_not_raw():
    """Any residual driver crash still folds into FastPathUnavailable —
    never the raw driver text on the fast path."""
    import pytest

    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_sqlserver import bulk_executemany

    class _Boom:
        def executemany(self, _sql, _rows):
            raise RuntimeError("HY104 Invalid precision value")

    with pytest.raises(FastPathUnavailable, match="bulk bind failed"):
        bulk_executemany(_Boom(), "INSERT INTO t VALUES (%s)", [(1,)])
