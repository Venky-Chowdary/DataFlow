"""QA D224: MySQL DATETIME(6) into a NEW SQLite table lost its microseconds,
midnight values became date-only text, and the job still showed green.

Two defects, one family:

* the carrier: every SQL -> SQLite engine copy (MySQL, SQL Server, Oracle,
  Iceberg) bound a naive ``datetime`` with ``isoformat(timespec="seconds")``
  and wrote a midnight value as a calendar day. SQLite has no temporal storage
  class, so that text IS the stored value: the fraction was gone and a
  DATETIME column held DATE text;
* the verification: Gate-8 fingerprints a source instant at the precision the
  destination carrier keeps, and read a SQLite ``DATETIME`` column as FSP 0.
  On such a column a copy that dropped the microseconds fingerprinted equal.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, on real
MySQL / SQL Server / PostgreSQL / Oracle sources, then a second run into the
table DataFlow created.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    # Gate-8 grades an engine copy (MySQL -> SQLite here) "verified" only on a
    # two-sided population digest, and that digest runs only with
    # DATAFLOW_ENGINE_DIGEST=1; without it the same correct run is "partial" by
    # design (X-41b, tests/test_x41b_count_only_verdict.py). This test asserts
    # the digest verdict, so it states the flag instead of inheriting it from
    # the shell: CI does not export it, and the run there read "partial".
    monkeypatch.delenv("DATAWRAP_ENGINE_DIGEST", raising=False)  # read first by getenv_brand
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    sqlite_root = tmp_path / "sqlite"
    sqlite_root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(sqlite_root))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        c.sqlite_db = str(sqlite_root / "d224.db")  # type: ignore[attr-defined]
        yield c


# id, timestamp text as the source stores it (None = NULL)
_ROWS = [
    (1, "2024-01-02 03:04:05.123456"),
    (2, "2024-01-02 00:00:00"),  # midnight: a DATETIME, not a DATE
    (3, None),
    (4, "2024-01-02 00:00:00.000001"),
    (5, "1999-12-31 23:59:59.999999"),
    (6, "2024-06-30 12:00:00.5"),
]
_RUN2 = {2: "2024-01-03 00:00:00", 7: "2025-02-03 04:05:06.000007"}


def _expected(rows) -> dict[int, str | None]:
    """The exact SQLite text: full six-digit fraction when there is one,
    the clock always present."""
    out: dict[int, str | None] = {}
    for rid, text in rows:
        out[rid] = None if text is None else datetime.fromisoformat(text).isoformat(sep=" ")
    return out


def _oracle():
    from tests import test_r8_d210_oracle_empty_string_e2e as o

    return o


def _src_cfg(kind: str) -> dict[str, Any]:
    if kind == "oracle":
        return _oracle()._oracle_cfg()
    return h.dest_config(kind)


def _src_exec(kind: str, sql: str) -> None:
    if kind == "oracle":
        _oracle()._ora(sql)
    else:
        h.execute(kind, sql)


def _qt(kind: str, table: str) -> str:
    # Oracle: an unquoted name, stored upper case like a user's DDL.
    return table if kind == "oracle" else h.quote(kind, table)


def _create_source(kind: str, table: str, rows) -> None:
    ts_type = {
        "mysql": "DATETIME(6)",
        "postgresql": "TIMESTAMP(6)",
        # DATETIME2(7)'s seventh digit cannot ride a Python datetime; that
        # carrier is reported separately (see the D224 report), not here.
        "sqlserver": "DATETIME2(6)",
        "oracle": "TIMESTAMP(6)",
    }[kind]
    _src_exec(kind, f"CREATE TABLE {_qt(kind, table)} (id INT PRIMARY KEY, ts {ts_type} NULL)")
    _insert(kind, table, rows)


def _insert(kind: str, table: str, rows) -> None:
    for rid, text in rows:
        if text is None:
            lit = "NULL"
        elif kind == "oracle":
            lit = f"TO_TIMESTAMP('{text}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        else:
            lit = f"'{text}'"
        _src_exec(kind, f"INSERT INTO {_qt(kind, table)} (id, ts) VALUES ({rid}, {lit})")


def _drop_source(kind: str, table: str) -> None:
    if kind == "oracle":
        _oracle()._drop_ora(table)
    else:
        h.drop_table(kind, table)


def _connectors(client, kind: str, token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"d224-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **_src_cfg(kind)})
    dst = create_connector({"name": f"d224-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, "type": "sqlite",
                            "database": client.sqlite_db})
    return src, dst


def _values(rows: dict[int, tuple[str | None, str]]) -> dict[int, str | None]:
    """Stored text, separator-neutral: the engine copy binds SQLite's
    ``YYYY-MM-DD HH:MM:SS`` form and the row writer ISO ``T``; both are the
    full value. Precision and the clock are compared exactly."""
    return {k: (v if v is None else v.replace("T", " ", 1)) for k, (v, _t) in rows.items()}


def _sqlite_rows(client, table: str) -> dict[int, tuple[str | None, str]]:
    with sqlite3.connect(client.sqlite_db) as conn:
        names = [str(r[0]) for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)", (table,))]
        assert names, f"SQLite table {table} missing"
        cols = {str(r[1]).lower(): str(r[1]) for r in conn.execute(f'PRAGMA table_info("{names[0]}")')}
        rows = conn.execute(
            f'SELECT "{cols["id"]}", "{cols["ts"]}", typeof("{cols["ts"]}") FROM "{names[0]}"'
        ).fetchall()
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _assert_verified(job: dict) -> None:
    assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
    rec = job.get("reconciliation") or {}
    assert rec.get("verification_verdict") in (None, "verified"), rec


@pytest.mark.parametrize("kind", ["mysql", "sqlserver", "postgresql", "oracle"])
def test_fractional_timestamps_land_exactly_in_a_new_sqlite_table(client, kind):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d224_s_{token}", f"d224_d_{token}"
    try:
        _create_source(kind, src_t, _ROWS)
        src, dst = _connectors(client, kind, token)
        args = {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite",
        }
        job = h.stage_and_run(client, "start_transfer", args)
        _assert_verified(job)
        got = _sqlite_rows(client, dst_t)
        assert _values(got) == _expected(_ROWS), got
        assert all(t in ("text", "null") for _v, t in got.values()), got

        # Second run into the table DataFlow created: a changed midnight value
        # and a new row with a one-microsecond fraction.
        _src_exec(kind, f"DELETE FROM {_qt(kind, src_t)} WHERE id = 2")
        _insert(kind, src_t, list(_RUN2.items()))
        job2 = h.stage_and_run(client, "start_transfer", args)
        _assert_verified(job2)
        rows2 = [r for r in _ROWS if r[0] != 2] + list(_RUN2.items())
        got2 = _sqlite_rows(client, dst_t)
        assert _values(got2) == _expected(rows2), got2
    finally:
        _drop_source(kind, src_t)


def test_gate8_fingerprints_a_sqlite_datetime_column_at_full_precision():
    """Verification half of D224: SQLite DATETIME is text and keeps every digit;
    a copy that dropped the microseconds must not fingerprint equal."""
    from services.reconciliation import fingerprint_for_reconcile

    src = datetime(2024, 1, 2, 3, 4, 5, 123456)  # noqa: DTZ001 - naive wall clock
    for ddl in ("DATETIME", "datetime", "TIMESTAMP", "DATETIME(6)"):
        source_fp = fingerprint_for_reconcile(src, ddl_type=ddl, engine="sqlite")
        lossy_fp = fingerprint_for_reconcile("2024-01-02 03:04:05", ddl_type=ddl, engine="sqlite")
        exact_fp = fingerprint_for_reconcile(
            "2024-01-02 03:04:05.123456", ddl_type=ddl, engine="sqlite"
        )
        assert source_fp != lossy_fp, ddl
        assert source_fp == exact_fp, ddl


def test_time_fraction_spellings_compare_as_one_clock():
    """MySQL TIME(6) read back as '03:04:05.250000' and fingerprinted from the
    driver as '03:04:05.25' are the same clock; a different clock is not."""
    from datetime import timedelta

    from services.reconciliation import normalize_cell

    driver = timedelta(hours=3, minutes=4, seconds=5, microseconds=250000)
    for text in ("03:04:05.250000", "03:04:05.25"):
        assert normalize_cell(text) == normalize_cell(driver)
        assert normalize_cell(text, ddl_type="TEXT", engine="sqlite") == normalize_cell(driver)
    assert normalize_cell("03:04:05.250001") != normalize_cell(driver)
    assert normalize_cell("03:04:05") != normalize_cell(driver)
