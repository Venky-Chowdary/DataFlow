"""Query CDC (cursor poll) — run 2 into the table DataFlow created.

Found while fixing D188: SQL Server -> MySQL and SQL Server -> PostgreSQL CDC on
a table without change tracking falls back to polling a declared
modification-timestamp cursor. Run 1 created the destination; run 2 failed:

* MySQL: "physical DDL missing for mapped column(s) _df_lsn"
* PostgreSQL: "Destination table ... is missing column(s) _df_lsn"

Root cause: after the poll, the log-CDC "image repair" ran on query CDC too
(its cursor watermark passes as an opaque resume token). It upserted a row
edited *outside* the poll window, stamping the cursor text as ``_df_lsn`` — a
bookkeeping column a poll never creates. That row is out-of-window drift
(D188): reported by Gate-8, not written by the run.

The same route also wrote every SQL NULL the poll read as the literal text
``__DF_SQL_NULL__`` (reader wire cells encoded twice), and Gate-8 read that text
back as NULL on both sides, so it passed. Both are pinned here cell by cell.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_d196_sqlserver_change_tracking_cdc_live import (  # noqa: F401
    _DESTS,
    _MS,
    _dest_conn,
    _exec,
    _fetch,
    _ms,
    _up,
    isolated,
)

pytestmark = pytest.mark.skipif(not _up(_MS), reason="live SQL Server not reachable")


def _q(fmt: str, table: str) -> str:
    return f'"{table}"' if fmt == "postgresql" else f"`{table}`"


_SEED = [
    (1, "alpha", "2026-01-01 00:00:01"),
    (2, None, "2026-01-01 00:00:02"),
    (3, "", "2026-01-01 00:00:03"),
    (4, "  ", "2026-01-01 00:00:04"),
    (5, "naïve 中文 ✓", "2026-01-01 00:00:05"),
    (6, "__DF_SQL_NULL__", "2026-01-01 00:00:06"),
    (7, "seven", "2026-01-01 00:00:07"),
]


def _request(table: str, fmt: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlserver", schema="dbo", table=table,
            extra={"trust_server_certificate": True}, **_MS,
        ),
        destination=EndpointConfig(kind="database", format=fmt, table=table, **_DESTS[fmt]),
        sync_mode="cdc",
        stream_contracts=[
            {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": "cdc",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp"}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


def _run(fake, table: str, fmt: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "qcdc" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table, fmt), job_id)


def _rows(fmt: str, table: str) -> dict[int, tuple]:
    if fmt == "sqlserver":
        conn, sql = _ms(), f"SELECT id, name, updated_at FROM dbo.[{table}]"  # nosec B608
    else:
        conn = _dest_conn(fmt)
        sql = f"SELECT id, name, updated_at FROM {_q(fmt, table)}"  # nosec B608
    try:
        return {int(r[0]): (int(r[0]), r[1], r[2]) for r in _fetch(conn, sql)}
    finally:
        conn.close()


def _dest_columns(fmt: str, table: str) -> set[str]:
    conn = _dest_conn(fmt)
    try:
        if fmt == "postgresql":
            sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"  # nosec B608
        else:
            sql = (
                "SELECT column_name FROM information_schema.columns "  # nosec B608
                f"WHERE table_schema = DATABASE() AND table_name = '{table}'"
            )
        return {str(r[0]).lower() for r in _fetch(conn, sql)}
    finally:
        conn.close()


@pytest.mark.parametrize("fmt", ["mysql", "postgresql"])
def test_query_cdc_second_run_into_the_created_table(isolated, fmt):
    if not _up(_DESTS[fmt]):
        pytest.skip(f"live {fmt} not reachable")
    table = "qcdc_" + uuid.uuid4().hex[:10]
    conn = _ms()
    try:
        _exec(
            conn,
            f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, "
            "name NVARCHAR(80) NULL, updated_at DATETIME2(0) NOT NULL)",
        )
        _exec(conn, f"INSERT INTO dbo.[{table}] VALUES (?, ?, ?)", _SEED)  # nosec B608
    finally:
        conn.close()
    try:
        first = _run(isolated, table, fmt)
        assert first.success, (first.error, first.reconciliation)
        assert "_df_lsn" not in _dest_columns(fmt, table)
        assert _rows(fmt, table) == _rows("sqlserver", table)
        got = _rows(fmt, table)
        assert got[2][1] is None, "SQL NULL must land as NULL, not sentinel text"
        assert got[3][1] == "" and got[4][1] == "  "
        assert got[6][1] == "__DF_SQL_NULL__", "a literal sentinel spelling is text"

        conn = _ms()
        try:
            # Moves the cursor: inside run 2's window.
            _exec(conn, f"UPDATE dbo.[{table}] SET name = NULL, "
                        f"updated_at = '2026-01-02 00:00:01' WHERE id = 1")
            _exec(conn, f"INSERT INTO dbo.[{table}] VALUES (8, N'eight ✓', '2026-01-02 00:00:02')")
            # Does not move the cursor: outside the window, drift not failure.
            _exec(conn, f"UPDATE dbo.[{table}] SET name = N'hand-fixed' WHERE id = 7")
        finally:
            conn.close()

        second = _run(isolated, table, fmt)
        assert second.success, (second.error, second.reconciliation)
        assert "_df_lsn" not in _dest_columns(fmt, table)
        got, src = _rows(fmt, table), _rows("sqlserver", table)
        assert set(got) == set(src)
        for key in (1, 2, 3, 4, 5, 6, 8):
            assert got[key] == src[key], key
        assert got[1][1] is None and got[8][1] == "eight ✓"
        assert got[7][1] == "seven", "row 7 is outside run 2's window"
        drift = (second.reconciliation or {}).get("out_of_window_drift") or {}
        assert drift.get("sample_keys") == ["7"], second.reconciliation

        quiet = _run(isolated, table, fmt)
        assert quiet.success, (quiet.error, quiet.reconciliation)
    finally:
        conn = _ms()
        try:
            _exec(conn, f"DROP TABLE IF EXISTS dbo.[{table}]")
        finally:
            conn.close()
        dconn = _dest_conn(fmt)
        try:
            _exec(dconn, f"DROP TABLE IF EXISTS {_q(fmt, table)}")
        finally:
            dconn.close()


@pytest.mark.parametrize("fmt", ["postgresql"])
def test_query_cdc_real_loss_inside_the_window_still_fails(isolated, monkeypatch, fmt):
    """Scoping the proof to the poll window must not hide an in-window loss."""
    import src.transfer.engine as engine_mod

    if not _up(_DESTS[fmt]):
        pytest.skip(f"live {fmt} not reachable")
    table = "qcdc_" + uuid.uuid4().hex[:10]
    conn = _ms()
    try:
        _exec(conn, f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, "
                    "name NVARCHAR(80) NULL, updated_at DATETIME2(0) NOT NULL)")
        _exec(conn, f"INSERT INTO dbo.[{table}] VALUES (?, ?, ?)", _SEED)  # nosec B608
    finally:
        conn.close()
    try:
        assert _run(isolated, table, fmt).success
        conn = _ms()
        try:
            _exec(conn, f"INSERT INTO dbo.[{table}] VALUES (9, N'nine', '2026-01-03 00:00:00')")
        finally:
            conn.close()
        real = engine_mod.run_reconciliation

        def _lose_then_verify(*args, **kwargs):
            dconn = _dest_conn(fmt)
            try:
                _exec(dconn, f"UPDATE {_q(fmt, table)} SET name = 'corrupt' WHERE id = 9")
            finally:
                dconn.close()
            return real(*args, **kwargs)

        monkeypatch.setattr(engine_mod, "run_reconciliation", _lose_then_verify)
        bad = _run(isolated, table, fmt)
        assert not bad.success, bad.reconciliation
        assert "9" in str(bad.error), bad.error
    finally:
        conn = _ms()
        try:
            _exec(conn, f"DROP TABLE IF EXISTS dbo.[{table}]")
        finally:
            conn.close()
        dconn = _dest_conn(fmt)
        try:
            _exec(dconn, f"DROP TABLE IF EXISTS {_q(fmt, table)}")
        finally:
            dconn.close()
