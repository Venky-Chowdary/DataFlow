"""A dropped Postgres slot must not resume its last LSN.

The next run snapshots current source keys and creates the new slot before
that snapshot. ``never`` stays fail-closed. A slot that still exists with
``wal_status=lost`` is not treated as permission to skip the recycled window.
"""

from __future__ import annotations

import sqlite3

import pytest

from connectors.cdc_eos_sql import (
    WATERMARK_TABLE,
    blank_sqlite_eos_resume,
    dest_watermark_view,
)
from services.cdc_catchup import release_finished_cdc_slot
from services.cdc_exactly_once import DestWmView, plan_open_session
from services.cdc_value_digest import (
    CdcValueScanIncomplete,
    prove_cdc_values,
)
from services.cdc_slot_resume import (
    prepare_resume_for_missing_slot,
    preflight_slot_gate,
    retire_slot_lsn,
    slot_lsn_retired,
    without_retired_slot_resume,
)
from services.cdc_snapshot_mode import (
    KIND_BLOCKING,
    KIND_REFUSE,
    build_snapshot_mode_preflight_gate,
    resolve_cdc_snapshot_plan,
)


class _Probe:
    def __init__(self, status: str, retained: str, *, exists: bool) -> None:
        self.status = status
        self.retained = retained
        self.dialect = "postgresql"
        self.resume = "0/11744550"
        self.cursor_key = "pg:qa:orders"
        self.details = {"slot_exists": exists}


class _Reader:
    def __init__(self) -> None:
        self.consistent_point_lsn = "0/11744550"
        self._resume_expected = True
        self._resume_snapshot = False
        self.resume_token = "lsn=0/11744550|slot=df_orders"
        self.snapshot_last_pk = "9"
        self.snapshot_table = "orders"
        self.slot_name = "df_orders"


@pytest.fixture
def retired_store(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "services.cdc_slot_resume.STORE_PATH", tmp_path / "retired.json"
    )
    monkeypatch.setattr("services.cdc_slot_resume._mongo_retired", lambda: None)


def test_missing_slot_makes_initial_snapshot_current_keys(retired_store) -> None:
    reader = _Reader()
    watermark, note = prepare_resume_for_missing_slot(
        "lsn=0/11744550|slot=df_orders",
        _Probe("gap", "slot_missing", exists=False),
        reader,
        mode="initial",
    )
    assert watermark is None
    assert reader._resume_expected is False
    assert reader.consistent_point_lsn is None
    assert "current source keys" in note
    plan = resolve_cdc_snapshot_plan(
        "initial",
        watermark=watermark,
        retention=_Probe("gap", "slot_missing", exists=False),
    )
    assert plan["kind"] == KIND_BLOCKING
    assert plan["run_snapshot"] is True
    assert plan["kind"] != KIND_REFUSE


def test_never_refuses_a_missing_slot_instead_of_creating_one(retired_store) -> None:
    reader = _Reader()
    watermark, note = prepare_resume_for_missing_slot(
        "0/11744550",
        _Probe("gap", "slot_missing", exists=False),
        reader,
        mode="never",
    )
    assert watermark == "0/11744550"
    assert reader._resume_expected is True
    assert note == ""
    with pytest.raises(Exception) as exc:
        resolve_cdc_snapshot_plan(
            "never",
            watermark=watermark,
            retention=_Probe("gap", "slot_missing", exists=False),
        )
    assert "never" in str(exc.value)


def test_lost_wal_does_not_void_an_initial_resume(retired_store) -> None:
    reader = _Reader()
    watermark, note = prepare_resume_for_missing_slot(
        "0/11744550",
        _Probe("gap", "lost", exists=True),
        reader,
        mode="initial",
    )
    assert watermark == "0/11744550"
    assert reader.consistent_point_lsn == "0/11744550"
    assert note == ""
    with pytest.raises(Exception):
        resolve_cdc_snapshot_plan(
            "initial",
            watermark=watermark,
            retention=_Probe("gap", "lost", exists=True),
        )


def test_preflight_missing_slot_snapshots_and_never_blocks(retired_store) -> None:
    missing = _Probe("gap", "slot_missing", exists=False)
    initial = preflight_slot_gate("initial", "0/11744550", missing, cursor_key="ck")
    assert initial is not None
    assert initial["status"] == "pass"
    assert initial["details"]["run_snapshot"] is True
    assert "current source keys" in initial["message"]
    assert "stream only" not in initial["message"]
    never = preflight_slot_gate("never", "0/11744550", missing, cursor_key="ck")
    assert never is not None
    assert never["status"] == "block"
    assert never["details"]["primary_action"] == "open_advanced"
    lost = preflight_slot_gate(
        "initial", "0/11744550", _Probe("gap", "lost", exists=True), cursor_key="ck"
    )
    assert lost is not None
    assert lost["status"] == "block"


def test_retired_lsn_is_scoped_to_the_cursor(retired_store) -> None:
    retire_slot_lsn("0/11744550", cursor_key="pg:qa:orders", slot_name="df_orders")
    assert slot_lsn_retired("0/11744550", cursor_key="pg:qa:orders") is True
    assert slot_lsn_retired("0/11744550", cursor_key="pg:other:orders") is False
    gate = build_snapshot_mode_preflight_gate(
        sync_mode="cdc",
        stream_contracts=[
            {
                "name": "orders",
                "selected": True,
                "primary_key": "id",
                "snapshot_mode": "initial",
            }
        ],
        watermark="0/11744550",
        cursor_key="pg:qa:orders",
    )
    assert gate is not None
    assert gate["status"] == "pass"
    assert gate["details"]["run_snapshot"] is True
    assert gate["details"]["resume_retired"] is True


def test_open_ignores_a_retired_dest_lsn_and_keeps_the_fence(retired_store) -> None:
    retire_slot_lsn("0/11744550", cursor_key="pg:qa:orders", slot_name="df_orders")
    view = DestWmView(
        committed_lsn="0/11744550",
        fence_epoch=4,
        resume_blob="lsn=0/11744550|slot=df_orders",
        apply_seq=2,
    )
    stripped, job = without_retired_slot_resume(
        view,
        "lsn=0/11744550|slot=df_orders",
        cursor_key="pg:qa:orders",
    )
    assert stripped.committed_lsn is None
    assert stripped.fence_epoch == 4
    assert stripped.apply_seq == 2
    assert job is None
    opened = plan_open_session(dest=stripped, incoming_fence=0, job_resume=job)
    assert opened.fence_epoch == 4
    assert opened.resume in {None, ""}


def test_slot_drop_retires_the_prior_lsn(retired_store, monkeypatch) -> None:
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": "dropped", "slot_name": "df_orders"},
    )
    monkeypatch.setattr(
        "services.sync_cursor.clear_watermark",
        lambda _key: {"cleared": True, "prior_watermark": "lsn=0/11744550|slot=df_orders"},
    )
    out = release_finished_cdc_slot(
        {
            "cdc_slot_name": "df_orders",
            "cursor_key": "pg:qa:orders",
        },
        reason="completed",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-c004",
    )
    assert out["released"] is True
    assert out["retired_lsns"] == ["0/11744550"]
    assert slot_lsn_retired("0/11744550", cursor_key="pg:qa:orders") is True


def test_incomplete_value_scan_is_not_a_row_count_pass(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.cdc_value_digest._scan_table", lambda *_a, **_k: None
    )
    with pytest.raises(CdcValueScanIncomplete):
        prove_cdc_values(
            source_type="postgresql",
            source_cfg={},
            source_table="orders",
            dest_type="mysql",
            dest_cfg={},
            dest_table="orders",
            mappings=[{"source": "id", "target": "id"}, {"source": "qty", "target": "qty"}],
        )


def test_corrupted_dest_cell_is_not_a_source_fingerprint(monkeypatch) -> None:
    def _scan(_db, _cfg, table, _columns):
        if table == "src_orders":
            return [{"id": 1, "qty": 3}]
        return [{"id": 1, "qty": 9}]

    monkeypatch.setattr("services.cdc_value_digest._scan_table", _scan)
    proof = prove_cdc_values(
        source_type="postgresql",
        source_cfg={},
        source_table="src_orders",
        dest_type="mysql",
        dest_cfg={},
        dest_table="dst_orders",
        mappings=[{"source": "id", "target": "id"}, {"source": "qty", "target": "qty"}],
    )
    assert proof is not None
    assert proof.missing == 1
    assert proof.matched is False
    from services.reconciliation import reconcile
    from services.reconcile_coverage import CDC_SOURCE_IMAGE_VALUES

    report = reconcile(
        source_rows=1,
        target_rows=1,
        source_checksum=proof.source_digest,
        target_checksum=proof.dest_digest,
        checksum_scope=CDC_SOURCE_IMAGE_VALUES,
    )
    assert report.passed is False
    assert "missing" in report.message


def test_sqlite_blank_clears_only_the_dropped_lsn(tmp_path) -> None:
    path = tmp_path / "eos.db"
    cfg = {"database": str(path)}
    conn = sqlite3.connect(path)
    conn.execute(
        f"CREATE TABLE {WATERMARK_TABLE} ("
        "stream_key TEXT PRIMARY KEY, committed_lsn TEXT NOT NULL, "
        "batch_id TEXT NOT NULL, committed_at TEXT NOT NULL, "
        "epoch INTEGER NOT NULL DEFAULT 1, "
        "fence_epoch INTEGER NOT NULL DEFAULT 0, phase TEXT, "
        "apply_checksum TEXT, resume_blob TEXT, apply_seq INTEGER NOT NULL DEFAULT 0, "
        "window_id TEXT, snapshot_signal_id TEXT, window_hi_pk TEXT)"
    )
    conn.execute(
        f"INSERT INTO {WATERMARK_TABLE} "
        "(stream_key, committed_lsn, batch_id, committed_at, fence_epoch, resume_blob) "
        "VALUES ('pg:qa:orders', '0/11744550', 'b', 't', 4, 'lsn=0/11744550')"
    )
    conn.commit()
    conn.close()
    assert blank_sqlite_eos_resume(cfg, "pg:qa:orders", lsn="0/999") is False
    assert blank_sqlite_eos_resume(cfg, "pg:qa:orders", lsn="0/11744550") is True
    view = dest_watermark_view(cfg, "pg:qa:orders")
    assert view.committed_lsn in {None, ""}
    assert view.fence_epoch == 4
