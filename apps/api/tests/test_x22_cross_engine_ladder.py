"""X-22: the cell-value ladder must cover MySQL/SQL Server ends — a norecipe
tamper must fail closed, and a read failure must not stamp count-only green.

Production defect: the ladder's population readers covered sqlite and
PostgreSQL only. A MySQL/SQL Server destination exited at the dest-type gate
(or took the PG reader and threw), a MySQL/SQL Server source could never be
re-read, and a thrown dest read returned the report unchanged — so a run whose
dest held tampered rows still reported ``verification_verdict == "verified"``
on row-count evidence alone.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.reconcile_step import _maybe_attach_verification_ladder  # noqa: E402

LADDER = "services.verification_ladder"


def _sqlite_dest(tmp_path: Path, rows: list[tuple]) -> EndpointConfig:
    path = str(tmp_path / "dest.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount TEXT, name TEXT)")
    conn.executemany("INSERT INTO t (id, amount, name) VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return EndpointConfig(
        kind="database", format="sqlite", database=path, table="t"
    )


def _run(monkeypatch, *, dest, source=None, records, dest_reader=None, src_reader=None):
    import services.verification_ladder as vl

    real_sqlite = vl.read_sqlite_rows

    def explode(**kw):
        raise AssertionError("wrong reader used for this route")

    for name in ("read_sqlite_rows", "read_postgres_rows", "read_mysql_rows", "read_sqlserver_rows"):
        monkeypatch.setattr(f"{LADDER}.{name}", explode)
    if dest_reader is not None:
        monkeypatch.setattr(f"{LADDER}.{dest_reader[0]}", dest_reader[1])
    if src_reader is not None:
        monkeypatch.setattr(f"{LADDER}.{src_reader[0]}", src_reader[1])
    # read_sqlite_rows serves real file dests — keep it when the dest is sqlite.
    if dest.format == "sqlite":
        monkeypatch.setattr(f"{LADDER}.read_sqlite_rows", real_sqlite)

    report = {
        "passed": True,
        "source_rows": len(records) if records else 2,
        "target_rows": 2,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "rows_skipped": 0,
    }
    return _maybe_attach_verification_ladder(
        report,
        endpoint=dest,
        source_endpoint=source,
        records=records,
        columns=["id", "amount", "name"],
        dest_summary={"sync_mode": "overwrite"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "name", "target": "name"},
        ],
        validation_mode="strict",
    )


CLEAN = [{"id": "1", "amount": "10.5", "name": "alpha"}, {"id": "2", "amount": "20.0", "name": "beta"}]
TAMPERED = [{"id": "1", "amount": "10.5000000001", "name": "alpha"}, {"id": "2", "amount": "20.0", "name": "beta"}]


def test_mysql_dest_tamper_fails_closed(monkeypatch):
    """PG→MySQL norecipe tamper used to stay verified on row_count."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_mysql_rows", lambda **kw: TAMPERED),
    )
    assert out.get("passed") is False


def test_sqlserver_dest_tamper_fails_closed(monkeypatch):
    dest = EndpointConfig(kind="database", format="sqlserver", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_sqlserver_rows", lambda **kw: TAMPERED),
    )
    assert out.get("passed") is False


def test_mysql_source_tamper_fails_closed(tmp_path, monkeypatch):
    """MySQL→PG-family norecipe: source re-read must run and catch the tamper."""
    dest = _sqlite_dest(tmp_path, [(1, "10.5", "alpha"), (2, "20.0", "beta")])
    source = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        source=source,
        records=[],
        src_reader=("read_mysql_rows", lambda **kw: TAMPERED),
    )
    assert out.get("passed") is False


def test_sqlserver_source_tamper_fails_closed(tmp_path, monkeypatch):
    dest = _sqlite_dest(tmp_path, [(1, "10.5", "alpha"), (2, "20.0", "beta")])
    source = EndpointConfig(kind="database", format="sqlserver", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        source=source,
        records=[],
        src_reader=("read_sqlserver_rows", lambda **kw: TAMPERED),
    )
    assert out.get("passed") is False


def test_dest_read_failure_marks_unproven(monkeypatch):
    """A covered dest whose population cannot be read has no value proof —
    the verdict must not stay 'verified' on COUNT-only."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")

    def refused(**kw):
        raise RuntimeError("connection refused")

    out = _run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_mysql_rows", refused),
    )
    assert out.get("unproven") is True


def test_clean_mysql_dest_still_passes(monkeypatch):
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_mysql_rows", lambda **kw: CLEAN),
    )
    assert out.get("passed") is not False


def test_postgres_to_sqlite_dest_tamper_fails_closed(tmp_path, monkeypatch):
    """PG→SQLite norecipe: tampered sqlite dest must fail closed through the
    real sqlite reader — proves the ladder is reached for a DB→sqlite route."""
    dest = _sqlite_dest(
        tmp_path, [(1, "10.5000000001", "alpha"), (2, "20.0", "beta")]
    )
    source = EndpointConfig(kind="database", format="postgresql", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        source=source,
        records=[],
        src_reader=("read_postgres_rows", lambda **kw: CLEAN),
    )
    assert out.get("passed") is False


def test_sqlserver_to_sqlite_dest_tamper_fails_closed(tmp_path, monkeypatch):
    """SQL Server→SQLite norecipe: same coverage through the real sqlite read."""
    dest = _sqlite_dest(
        tmp_path, [(1, "10.5", "alpha"), (2, "20.0", "tampered")]
    )
    source = EndpointConfig(kind="database", format="sqlserver", table="t", host="h")
    out = _run(
        monkeypatch,
        dest=dest,
        source=source,
        records=[],
        src_reader=("read_sqlserver_rows", lambda **kw: CLEAN),
    )
    assert out.get("passed") is False


def test_sqlite_dest_read_failure_marks_unproven(tmp_path, monkeypatch):
    """A sqlite dest whose population cannot be read must not stay verified
    on COUNT-only — the read failure is unproven, not silently skipped."""
    dest = _sqlite_dest(tmp_path, [(1, "10.5", "alpha")])
    # Point the endpoint at a sqlite file that exists but has no table t.
    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    conn.close()
    dest = EndpointConfig(
        kind="database", format="sqlite",
        database=str(tmp_path / "empty.db"), table="t",
    )
    out = _run(monkeypatch, dest=dest, records=CLEAN)
    assert out.get("unproven") is True


def _count_proof_run(monkeypatch, *, dest, source=None, records, dest_reader=None, src_reader=None, report_extra=None):
    """Same as _run but the report carries an engine count token — the live
    X-22 shape (dest_count:N, assurance=row_count)."""
    import services.verification_ladder as vl

    real_sqlite = vl.read_sqlite_rows

    def explode(**kw):
        raise AssertionError("wrong reader used for this route")

    for name in ("read_sqlite_rows", "read_postgres_rows", "read_mysql_rows", "read_sqlserver_rows"):
        monkeypatch.setattr(f"{LADDER}.{name}", explode)
    if dest_reader is not None:
        monkeypatch.setattr(f"{LADDER}.{dest_reader[0]}", dest_reader[1])
    if src_reader is not None:
        monkeypatch.setattr(f"{LADDER}.{src_reader[0]}", src_reader[1])
    if dest.format == "sqlite":
        monkeypatch.setattr(f"{LADDER}.read_sqlite_rows", real_sqlite)

    report = {
        "passed": True,
        "source_rows": len(records) if records else 2,
        "target_rows": 2,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "rows_skipped": 0,
        "checksum": "dest_count:2",
        "proof_scope": "dest_count_equals_source_snapshot",
        "assurance_level": "row_count",
    }
    report.update(report_extra or {})
    return _maybe_attach_verification_ladder(
        report,
        endpoint=dest,
        source_endpoint=source,
        records=records,
        columns=["id", "amount", "name"],
        dest_summary={"sync_mode": "overwrite"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "name", "target": "name"},
        ],
        validation_mode="strict",
    )


def test_count_proof_uncovered_dest_never_stays_verified(monkeypatch):
    """X-22 live: a dest engine outside the ladder exits early — a count-only
    report must then read unproven, never verified."""
    dest = EndpointConfig(kind="database", format="oracle", table="t", host="h")
    out = _count_proof_run(monkeypatch, dest=dest, records=CLEAN)
    assert out.get("unproven") is True
    assert out.get("ladder_declined_reason") == "destination_engine_not_in_ladder"


def test_count_proof_no_targets_never_stays_verified(monkeypatch):
    """X-22 live: empty mapped columns exits early — count-only green banned."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    import services.verification_ladder as vl

    def explode(**kw):
        raise AssertionError("must not reach readers")

    for name in ("read_mysql_rows", "read_sqlserver_rows"):
        monkeypatch.setattr(f"{LADDER}.{name}", explode)
    report = {
        "passed": True,
        "source_rows": 2,
        "target_rows": 2,
        "checksum": "dest_count:2",
        "proof_scope": "dest_count_equals_source_snapshot",
    }
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=dest,
        source_endpoint=None,
        records=[],
        columns=[],
        dest_summary={"sync_mode": "overwrite"},
        mappings=[],
        validation_mode="strict",
    )
    assert out.get("unproven") is True
    assert out.get("ladder_declined_reason") == "no_mapped_target_columns"


def test_count_proof_tamper_still_fails_closed(monkeypatch):
    """X-22 live: the count-proof report reaching the ladder still catches a
    tamper — the guard only covers exits that never ran the ladder."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    out = _count_proof_run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_mysql_rows", lambda **kw: TAMPERED),
    )
    assert out.get("passed") is False


def test_count_proof_clean_passes_when_ladder_ran(monkeypatch):
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    out = _count_proof_run(
        monkeypatch,
        dest=dest,
        records=CLEAN,
        dest_reader=("read_mysql_rows", lambda **kw: CLEAN),
    )
    assert out.get("unproven") is not True
    assert out.get("passed") is not False


def test_non_count_proof_early_exit_unchanged(monkeypatch):
    """Non-count reports keep the old quiet decline — no new unproven stamp."""
    dest = EndpointConfig(kind="database", format="oracle", table="t", host="h")
    out = _run(monkeypatch, dest=dest, records=CLEAN)
    assert out.get("unproven") is not True
    assert out.get("ladder_declined_reason") == "destination_engine_not_in_ladder"

def test_comparable_count_proof_scope_runs_l4_l5_and_fails(monkeypatch):
    """X-22 live: a dest_count_equals_source_snapshot report used to stamp
    checksum_scope=whole_table_not_comparable — L4/L5 never ran and a failed
    L2 rode out as verified on row_count assurance."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    report = {
        "passed": True,
        "source_rows": 2,
        "target_rows": 2,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "rows_skipped": 0,
        "checksum": "dest_count:2",
        "checksum_scope": "dest_count_equals_source_snapshot",
        "assurance_level": "row_count",
    }
    monkeypatch.setattr(
        f"{LADDER}.read_mysql_rows", lambda **kw: TAMPERED
    )
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=dest,
        source_endpoint=None,
        records=CLEAN,
        columns=["id", "amount", "name"],
        dest_summary={"sync_mode": "overwrite"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "name", "target": "name"},
        ],
        validation_mode="strict",
    )
    layers = (out.get("verification_ladder") or {}).get("layers") or {}
    assert "L4" in layers and "L5" in layers
    assert out.get("passed") is False


def test_append_count_scope_stays_incomparable(monkeypatch):
    """dest_count_equals_before_plus_staging is stamped
    whole_table_not_comparable in the production report — the dest
    legitimately holds pre-existing rows, so L4/L5 stay off."""
    dest = EndpointConfig(kind="database", format="mysql", table="t", host="h")
    report = {
        "passed": True,
        "source_rows": 2,
        "target_rows": 4,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "rows_skipped": 0,
        "checksum": "dest_count:4",
        "checksum_scope": "whole_table_not_comparable",
        "assurance_level": "row_count",
    }
    monkeypatch.setattr(f"{LADDER}.read_mysql_rows", lambda **kw: TAMPERED)
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=dest,
        source_endpoint=None,
        records=CLEAN,
        columns=["id", "amount", "name"],
        dest_summary={"sync_mode": "append"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "name", "target": "name"},
        ],
        validation_mode="strict",
    )
    layers = (out.get("verification_ladder") or {}).get("layers") or {}
    assert "L4" not in layers
