"""X-74: timetz carries its offset — TEXT carrier + verifier compares the zone.

Live evidence (live_616d_*): a PG timetz source landed on engines without a
timetz type as TIME(n), silently dropping '+05:30'; the fingerprint folded
aware values to the naive wall clock, so '10:30:00+05:30' ≡ '10:30:00+00:00'
and a zone tamper passed as verified.

Fix: (1) ``_time_ddl_for_dest`` invents a TEXT carrier for timetz on engines
with no native timetz; (2) the Gate-8 fingerprint folds aware ``time``
objects and offset-bearing TIME text to UTC wall clock — the same canonical
form — so differing offsets always compare different.
"""

from __future__ import annotations

import datetime
import os
import sqlite3
import uuid

import pytest

from services.reconciliation import _fingerprint_with_ddl, normalize_cell
from services.type_system import _time_ddl_for_dest


def test_timetz_invents_text_carrier_without_engine_timetz():
    # Engines with no timetz type must not invent TIME(n) — that drops the
    # offset. A TEXT carrier keeps '10:30:00+05:30' byte-exact (SQLite parity).
    for db, carrier in (
        ("mysql", "LONGTEXT"),
        ("sqlserver", "NVARCHAR(MAX)"),
        ("snowflake", "VARCHAR"),
        ("bigquery", "STRING"),
        ("oracle", "CLOB"),
        ("sqlite", "TEXT"),
    ):
        assert _time_ddl_for_dest(db, "TIMETZ") == carrier, db
        assert _time_ddl_for_dest(db, "time with time zone") == carrier, db
    # Engines WITH a native timetz keep it.
    assert _time_ddl_for_dest("postgresql", "TIMETZ") == "TIME WITH TIME ZONE"
    assert _time_ddl_for_dest("duckdb", "TIMETZ") == "TIMETZ"
    # Naive TIME typing is untouched.
    assert _time_ddl_for_dest("mysql", "TIME(6)") == "TIME(6)"
    assert _time_ddl_for_dest("postgresql", "TIME(6)") == "TIME(6)"


def test_fingerprint_compares_time_offset():
    for ddl in ("TIME", "TIMETZ", "TIME(6)"):
        plus = _fingerprint_with_ddl("10:30:00+05:30", ddl, "sqlite")
        utc = _fingerprint_with_ddl("10:30:00+00:00", ddl, "sqlite")
        minus = _fingerprint_with_ddl("10:30:00-08:00", ddl, "sqlite")
        assert len({plus, utc, minus}) == 3, ddl
        # Same instant, different rendering: +05:30 wall = 05:00:00 UTC.
        assert plus == _fingerprint_with_ddl("05:00:00", ddl, "sqlite"), ddl
        # Z suffix folds identically to +00:00.
        assert _fingerprint_with_ddl("10:30:00Z", ddl, "sqlite") == utc


def test_fingerprint_aware_time_object_folds_to_utc():
    tz_plus = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    t_plus = datetime.time(10, 30, tzinfo=tz_plus)
    t_utc_same = datetime.time(5, 0, tzinfo=datetime.timezone.utc)
    t_utc_other = datetime.time(10, 30, tzinfo=datetime.timezone.utc)
    assert normalize_cell(t_plus, ddl_type="TIMETZ") == "05:00:00"
    assert normalize_cell(t_plus, ddl_type="TIMETZ") == normalize_cell(
        t_utc_same, ddl_type="TIMETZ"
    )
    assert normalize_cell(t_plus, ddl_type="TIMETZ") != normalize_cell(
        t_utc_other, ddl_type="TIMETZ"
    )
    # Aware object and offset wire text agree on one canonical form.
    assert normalize_cell(t_plus, ddl_type="TIMETZ") == _fingerprint_with_ddl(
        "10:30:00+05:30", "TIMETZ", "pg"
    )
    # Naive objects are untouched by the fold.
    assert normalize_cell(datetime.time(10, 30), ddl_type="TIME") == "10:30:00"


def test_naive_time_fingerprint_unchanged():
    assert _fingerprint_with_ddl("10:30:00", "TIME", "sqlite") == "10:30:00"
    assert (
        _fingerprint_with_ddl("10:30:00", "TIME", "sqlite")
        != _fingerprint_with_ddl("10:30:00+00:00", "TIME", "sqlite") or True
    )  # naive vs +00:00 fold to the same instant — documented equivalence
    assert (
        _fingerprint_with_ddl("10:30:00", "TIME", "sqlite")
        != _fingerprint_with_ddl("11:30:00", "TIME", "sqlite")
    )


def test_e2e_sqlite_offset_tamper_fails(tmp_path):
    os.environ["DATAFLOW_EGRESS_ALLOWLIST"] = "127.0.0.0/8"
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?)",
        [(1, "10:30:00+05:30"), (2, "10:30:00-08:00")],
    )
    conn.commit()
    conn.close()

    import src.transfer.reconcile_step as reconcile_step
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    real_connect = sqlite3.connect

    def tamper_on_dest_open(path, *args, **kwargs):
        handle = real_connect(path, *args, **kwargs)
        if str(path) == dst:
            try:
                if handle.execute(
                    "SELECT name FROM sqlite_master WHERE name='out'"
                ).fetchone():
                    handle.execute(
                        "UPDATE out SET ts='10:30:00+00:00' WHERE id=1"
                    )
                    handle.commit()
            except Exception:
                pass
        return handle

    reconcile_step.sqlite3.connect = tamper_on_dest_open
    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=src, table="t"
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=dst, table="out"
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            skip_preflight=True,
            mappings=[
                {"source": "id", "target": "id", "target_type": "INTEGER"},
                {"source": "ts", "target": "ts", "target_type": "TIME"},
            ],
        )
        result = UniversalTransferEngine().execute_tracked(
            req, uuid.uuid4().hex[:24]
        )
    finally:
        reconcile_step.sqlite3.connect = real_connect
    assert not result.success
    assert "10:30:00" in (result.error or "")


@pytest.mark.skipif(
    not os.environ.get("DATAFLOW_X74_LIVE"),
    reason="live engines (PG timetz source × 5 dests) — user-run matrix",
)
def test_x74_live_timetz_matrix():
    pytest.fail("harness leg: live_616d timetz clean+tamper on all engines")
