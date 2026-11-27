"""D335 review round 3: no permanent stall, structural vs transient, BigQuery, replay.

* 60,000 stored rows inside the old ±120 s / ±2 day window: the keyless lookup
  now matches the exact stored instant and streams the answer without a cap,
  so run 2 lands its new row (ab19cbeac held it back on every run);
* a structural failure (the destination cannot be asked) fails with the
  remedy in every mode instead of "the next run will retry them" forever; an
  audited per-route acknowledgement appends and records who accepted it;
* a clean run's "Append delta verified" is left alone;
* BigQuery's writer cuts sub-microsecond digits, so the carrier instant does too;
* the commit-safe replay re-read fails closed on an append.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request):
    job_id = "d335t" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _pg(sql, params=None, fetch=False):
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _req(src: Path, dest: EndpointConfig, keys=(), mode="strict") -> TransferRequest:
    contract = {"name": "t", "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
                "sync_mode": "incremental_append", "selected": True}
    if keys:
        contract["primary_key"] = list(keys)
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=dest, sync_mode="incremental_append", stream_contracts=[contract],
        validation_mode=mode,
    )


@pytest.mark.parametrize("cursor_type", ["TIMESTAMP(6)", "DATE"])
def test_keyless_append_with_60k_rows_at_the_watermark_lands_run_2(tmp_path, isolated, cursor_type):
    from tests import typed_fidelity_helpers as tf

    src = tmp_path / "s.sqlite"
    dst = f"d335q_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, k2 BIGINT, note TEXT, updated_at {cursor_type})')

    def ts(i):
        if cursor_type == "DATE":
            return f"2026-11-{1 + i // 30000:02d}"
        return f"2026-11-24 10:00:{(i // 1000) % 60:02d}.{i % 1000:06d}"

    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, k2 INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?,?)", [(i, 1, "n", ts(i)) for i in range(1, 60001)])
        assert _run(isolated, _req(src, tf.pg_endpoint(dst))).success
        with sqlite3.connect(src) as c:
            nxt = "2026-11-28" if cursor_type == "DATE" else "2026-11-24 11:00:00.000001"
            c.execute("INSERT INTO t VALUES (60001, 1, 'n', ?)", (nxt,))
        second = _run(isolated, _req(src, tf.pg_endpoint(dst)))
        assert second.success, second.error
        assert _pg(f'SELECT count(*) FROM public."{dst}"', fetch=True)[0][0] == 60001
        third = _run(isolated, _req(src, tf.pg_endpoint(dst)))
        assert third.success, third.error
        assert _pg(f'SELECT count(*) FROM public."{dst}"', fetch=True)[0][0] == 60001
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def _sqlite_pair(tmp_path):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    with sqlite3.connect(src) as c:
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO t VALUES (?,?,?)",
                      [(i, f"n{i}", f"2026-11-24 10:{i:02d}:00.5") for i in range(1, 11)])
    with sqlite3.connect(dst) as c:
        c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
    return src, dst


@pytest.mark.parametrize("mode", ["strict", "balanced"])
def test_a_structural_boundary_failure_names_the_remedy_instead_of_retrying(tmp_path, isolated, monkeypatch, mode):
    from tests import typed_fidelity_helpers as tf
    import src.transfer.stream_boundary_keyed as sbk
    from src.transfer.stream_boundary_read import BoundaryStructural

    src = tmp_path / "s.sqlite"
    dst = f"d335s_{uuid.uuid4().hex[:8]}"
    _pg(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
    try:
        with sqlite3.connect(src) as c:
            c.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
            c.executemany("INSERT INTO t VALUES (?,?,?)", [(i, "n", f"2026-11-24 10:{i:02d}:00") for i in range(1, 11)])
        assert _run(isolated, _req(src, tf.pg_endpoint(dst), mode=mode)).success

        def _never(**_k):
            raise BoundaryStructural("the cursor column's type cannot be compared with the watermark")

        monkeypatch.setattr(sbk, "read_candidates", _never)
        with sqlite3.connect(src) as c:
            c.execute("INSERT INTO t VALUES (11, 'n', '2026-11-24 10:11:00')")
        refused = _run(isolated, _req(src, tf.pg_endpoint(dst), mode=mode))
        assert not refused.success
        text = str(refused.error or "")
        assert "Declare a primary key or dedupe key" in text, text
        assert "the next run will retry" not in text, text
        assert isolated.jobs[refused.job_id]["status"] == "failed"

        acked = tf.pg_endpoint(dst)
        acked.extra = {"accept_boundary_duplicates": {"actor": "ops-lead", "reason": "append log, dupes ok"}}
        accepted = _run(isolated, _req(src, acked, mode=mode))
        assert accepted.success, accepted.error
        recon = accepted.reconciliation or {}
        msg = str(recon.get("message") or "")
        # Never green: explained, completed with warnings, the count named, every run.
        assert "Boundary duplicates accepted by ops-lead: 1 this run" in msg, msg
        assert "Append delta verified" not in msg, msg
        assert recon.get("verification_verdict") == "explained", recon.get("verification_verdict")
        job = isolated.jobs[accepted.job_id]
        assert job["status"] == "completed_with_warnings", job["status"]
        from services.job_trust import compute_job_trust

        trust = json.dumps(compute_job_trust(job))
        assert "boundary duplicates accepted by ops-lead: 1 this run" in trust, trust[:2000]
    finally:
        _pg(f'DROP TABLE IF EXISTS public."{dst}"')


def test_a_clean_run_keeps_its_append_delta_sentence():
    from src.transfer.stream_boundary_keyed import _no_green_count_sentence

    clean = {"passed": True, "verification_verdict": "partial",
             "message": "Append delta verified (2 row(s) appended: 10 → 12)."}
    assert _no_green_count_sentence(dict(clean))["message"] == clean["message"]
    unmeasured = {**clean, "boundary_check": {"status": "unmeasured"}}
    assert "Append delta verified" not in _no_green_count_sentence(unmeasured)["message"]


@pytest.mark.parametrize(
    ("src", "dest", "ddl", "same"),
    [
        ("2026-11-24 10:10:00.9999996", datetime(2026, 11, 24, 10, 10, 0, 999999), "DATETIME", True),
        ("2026-11-24 10:10:00.9999996", datetime(2026, 11, 24, 10, 10, 1), "DATETIME", False),
        ("2026-11-24T10:10:00.1234569+00:00", "2026-11-24T10:10:00.123456+00:00", "TIMESTAMP", True),
        ("2026-11-24T10:10:00.1234569+00:00", "2026-11-24T10:10:00.123457+00:00", "TIMESTAMP", False),
    ],
)
def test_bigquery_carrier_cuts_sub_microsecond_digits(src, dest, ddl, same):
    """The BigQuery writer binds through a Python datetime: past the microsecond
    it cuts (warehouse_temporal.format_bigquery_bind), it never rounds up."""
    from connectors.warehouse_temporal import format_bigquery_bind
    from src.transfer.stream_boundary_read import carrier_values, instant, same_cursor

    assert same_cursor(src, dest, ddl=ddl, dialect="bigquery") is same
    written = format_bigquery_bind(src, ddl)
    assert instant(written)[0] in carrier_values(instant(src)[0], ddl, "bigquery")
    assert carrier_values(Decimal("0.9999996"), "DATETIME", "bigquery") == [Decimal("0.999999")]


def test_replay_re_read_fails_closed_on_an_append(monkeypatch):
    sync = "incremental_append"
    import services.target_sample as ts
    from src.transfer.stream_boundary_keyed import BoundaryHeldBack
    from src.transfer.stream_commit_window import replay_rows_already_landed

    def _boom(*_a, **_k):
        raise RuntimeError("connection reset (fake)")

    monkeypatch.setattr(ts, "read_target_sample", _boom)
    cfg = SimpleNamespace(
        replay_through="2026-11-24 10:05:00", replay_identity="id", cursor_source_col="updated_at",
        target_cols=["id", "updated_at"], mappings=[{"source": "id", "target": "id"},
                                                   {"source": "updated_at", "target": "updated_at"}],
        dest_bind_dialect="postgresql", dest_type="postgresql", dest_cfg={"schema": "public"},
        dest_table="t", effective_sync=sync, pk_target_cols=["id"], replay_key=(), replay_key_complete=True,
        column_types={}, stream_error_policy="quarantine", digest_dest_types={}, fingerprint_dest_types={},
    )
    run = SimpleNamespace(cfg=cfg, cur=SimpleNamespace(_boundary_fps_state={"fps": None}),
                          tally=SimpleNamespace(dest_summary={}))
    rows = [["1", "2026-11-24 10:01:00"], ["2", "2026-11-24 10:05:00"]]
    with pytest.raises(BoundaryHeldBack, match="held back; the next run will retry them"):
        replay_rows_already_landed(run, ["id", "updated_at"], rows)
