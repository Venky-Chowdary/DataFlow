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


def test_pre_retire_slot_resume_persists_and_reads_back_all_tokens(
    retired_store, monkeypatch
) -> None:
    from services import cdc_slot_resume

    monkeypatch.setattr(
        "services.sync_cursor.get_watermark", lambda _key: "lsn=0/11744550"
    )
    monkeypatch.setattr(
        "connectors.cdc_eos_sql.read_route_dest_lsn",
        lambda *_args, **_kwargs: "0/11744560",
    )
    pre_retire = getattr(cdc_slot_resume, "pre_retire_slot_resume", None)
    assert callable(pre_retire)

    tokens, new_entries = pre_retire(
        slot_name="df_orders",
        cursor_keys=["pg:qa:orders"],
        job_tokens=["lsn=0/11744550|slot=df_orders", "0/11744570"],
        dest_type="generic_sql",
        dest_cfg={"type": "sqlite", "database": "unused.db"},
    )

    assert tokens == ["0/11744550", "0/11744570", "0/11744560"]
    assert new_entries == [
        ("0/11744550", "pg:qa:orders", "df_orders"),
        ("0/11744570", "pg:qa:orders", "df_orders"),
        ("0/11744560", "pg:qa:orders", "df_orders"),
    ]
    assert all(
        slot_lsn_retired(token, cursor_key="pg:qa:orders", slot_name="df_orders")
        for token in tokens
    )


@pytest.mark.parametrize("slot_state", ["active", "kept"])
def test_kept_active_slot_unretires_only_new_markers_and_next_open_resumes(
    retired_store, monkeypatch, tmp_path, slot_state
) -> None:
    from connectors.cdc_eos_sql import open_eos_session

    cursor_key = "pg:qa:current"
    slot = "df_current"
    token = "0/11744550"
    resume = f"lsn={token}|slot={slot}"
    retire_slot_lsn(
        token, cursor_key="pg:qa:other", slot_name="df_other"
    )
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "services.sync_cursor.cursor_keys_for_job", lambda *_a, **_k: []
    )
    monkeypatch.setattr("services.sync_cursor.get_watermark", lambda _key: None)
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": slot_state, "slot_name": slot},
    )

    result = release_finished_cdc_slot(
        {
            "cdc_slot_name": slot,
            "cursor_key": cursor_key,
            "cursor_value": resume,
        },
        reason="failed",
        source_cfg={"type": "postgresql"},
        job_id="job-active-slot",
        worker_closed=True,
    )

    assert result["released"] is False
    assert result["reason"] == slot_state
    assert slot_lsn_retired(token, cursor_key="pg:qa:other", slot_name="df_other")
    assert not slot_lsn_retired(token, cursor_key=cursor_key, slot_name=slot)
    opened = open_eos_session(
        dest_type="sqlite",
        dest_cfg={"type": "sqlite", "database": str(tmp_path / "next-open.db")},
        stream_key=cursor_key,
        incoming_fence=0,
        job_resume=resume,
    )
    assert opened.resume == resume


def test_unretire_slot_lsns_removes_only_exact_entries_idempotently(
    retired_store,
) -> None:
    from services.cdc_slot_resume import unretire_slot_lsns

    retire_slot_lsn("0/20", cursor_key="key-a", slot_name="slot-a")
    retire_slot_lsn("0/20", cursor_key="key-b", slot_name="slot-b")
    retire_slot_lsn("0/21", cursor_key="key-a", slot_name="slot-a")

    unretire_slot_lsns([("0/20", "key-a", "slot-a")])
    unretire_slot_lsns([("0/20", "key-a", "slot-a")])

    assert not slot_lsn_retired("0/20", cursor_key="key-a", slot_name="slot-a")
    assert slot_lsn_retired("0/20", cursor_key="key-b", slot_name="slot-b")
    assert slot_lsn_retired("0/21", cursor_key="key-a", slot_name="slot-a")


def test_unknown_slot_release_keeps_retirement_markers(
    retired_store, monkeypatch, caplog
) -> None:
    cursor_key = "pg:qa:unknown"
    slot = "df_unknown"
    token = "0/11744560"
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "services.sync_cursor.cursor_keys_for_job", lambda *_a, **_k: []
    )
    monkeypatch.setattr("services.sync_cursor.get_watermark", lambda _key: None)
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("source unavailable")),
    )

    with caplog.at_level("ERROR"):
        result = release_finished_cdc_slot(
            {
                "cdc_slot_name": slot,
                "cursor_key": cursor_key,
                "cursor_value": f"lsn={token}|slot={slot}",
            },
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-unknown-slot",
            worker_closed=True,
        )

    assert result["released"] is False
    assert result["reason"] == "source_unreachable"
    assert result["retired_markers_left"] is True
    assert slot_lsn_retired(token, cursor_key=cursor_key, slot_name=slot)
    assert (
        "next start will not resume from the old position if the slot survived"
        in caplog.text
    )


def test_unretire_failure_reports_markers_left(
    retired_store, monkeypatch, caplog
) -> None:
    from services import cdc_slot_resume

    cursor_key = "pg:qa:active-unretire-failure"
    slot = "df_active_unretire_failure"
    token = "0/11744570"
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "services.sync_cursor.cursor_keys_for_job", lambda *_a, **_k: []
    )
    monkeypatch.setattr("services.sync_cursor.get_watermark", lambda _key: None)
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": "active", "slot_name": slot},
    )
    monkeypatch.setattr(
        cdc_slot_resume,
        "unretire_slot_lsns",
        lambda _entries: (_ for _ in ()).throw(OSError("retirement store unavailable")),
        raising=False,
    )

    with caplog.at_level("ERROR"):
        result = release_finished_cdc_slot(
            {
                "cdc_slot_name": slot,
                "cursor_key": cursor_key,
                "cursor_value": f"lsn={token}|slot={slot}",
            },
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-unretire-failure",
            worker_closed=True,
        )

    assert result["released"] is False
    assert result["retired_markers_left"] is True
    assert slot_lsn_retired(token, cursor_key=cursor_key, slot_name=slot)
    assert slot in caplog.text
    assert "OSError" in caplog.text


@pytest.mark.parametrize("slot_state", ["absent", "missing"])
def test_absent_slot_keeps_retirement_markers(
    retired_store, monkeypatch, slot_state
) -> None:
    cursor_key = "pg:qa:absent"
    slot = "df_absent"
    token = "0/11744580"
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "services.sync_cursor.cursor_keys_for_job", lambda *_a, **_k: []
    )
    monkeypatch.setattr("services.sync_cursor.get_watermark", lambda _key: None)
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": slot_state, "slot_name": slot},
    )

    result = release_finished_cdc_slot(
        {
            "cdc_slot_name": slot,
            "cursor_key": cursor_key,
            "cursor_value": f"lsn={token}|slot={slot}",
        },
        reason="failed",
        source_cfg={"type": "postgresql"},
        job_id="job-absent-slot",
        worker_closed=True,
    )

    assert result["released"] is False
    assert result["reason"] == slot_state
    assert slot_lsn_retired(token, cursor_key=cursor_key, slot_name=slot)


def test_pre_retire_slot_resume_refuses_failed_readback(retired_store, monkeypatch) -> None:
    from services import cdc_slot_resume

    monkeypatch.setattr(
        "services.sync_cursor.get_watermark", lambda _key: "0/11744550"
    )
    monkeypatch.setattr(cdc_slot_resume, "slot_lsn_retired", lambda *_a, **_k: False)
    pre_retire = getattr(cdc_slot_resume, "pre_retire_slot_resume", None)
    assert callable(pre_retire)
    error_type = getattr(cdc_slot_resume, "CdcSlotRetirementError", RuntimeError)

    with pytest.raises(error_type, match="df_orders"):
        pre_retire(
            slot_name="df_orders",
            cursor_keys=["pg:qa:orders"],
            job_tokens=[],
        )


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
    kept = release_finished_cdc_slot(
        {
            "cdc_slot_name": "df_orders",
            "cursor_key": "pg:qa:orders",
        },
        reason="completed",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-c004",
    )
    assert kept["released"] is False
    assert kept["reason"] == "resume_keeps_slot"
    assert slot_lsn_retired("0/11744550", cursor_key="pg:qa:orders") is False
    out = release_finished_cdc_slot(
        {
            "cdc_slot_name": "df_orders",
            "cursor_key": "pg:qa:orders",
        },
        reason="failed",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-c004",
        worker_closed=True,
    )
    assert out["released"] is True
    assert out["retired_lsns"] == ["0/11744550"]
    assert slot_lsn_retired("0/11744550", cursor_key="pg:qa:orders") is True


def test_mongo_retirement_write_failure_refuses_slot_drop(
    retired_store, monkeypatch
) -> None:
    from services import cdc_slot_resume

    class _Mongo:
        def find(self, *_args, **_kwargs):
            return []

        def delete_many(self, *_args, **_kwargs):
            return None

        def insert_many(self, *_args, **_kwargs):
            raise OSError("store credentials must not appear in the error")

    dropped: list[str] = []
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: _Mongo())
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: dropped.append("df_orders") or {"slot": "dropped"},
    )
    error_type = getattr(cdc_slot_resume, "CdcSlotRetirementError", RuntimeError)

    with pytest.raises(error_type, match="df_orders") as raised:
        release_finished_cdc_slot(
            {"cdc_slot_name": "df_orders", "cursor_key": "pg:qa:orders",
             "cursor_value": "lsn=0/11744550|slot=df_orders"},
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-mongo-retire",
            worker_closed=True,
        )

    assert dropped == []
    assert "store credentials must not appear" not in str(raised.value)


def test_destination_lsn_read_failure_refuses_slot_drop(
    retired_store, monkeypatch
) -> None:
    from services import cdc_slot_resume

    dropped: list[str] = []
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: dropped.append("df_orders") or {"slot": "dropped"},
    )
    monkeypatch.setattr(
        "connectors.cdc_eos_sql.read_route_dest_lsn",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("secret=do-not-log")),
    )
    error_type = getattr(cdc_slot_resume, "CdcSlotRetirementError", RuntimeError)

    with pytest.raises(error_type, match="df_orders") as raised:
        release_finished_cdc_slot(
            {"cdc_slot_name": "df_orders", "cursor_key": "pg:qa:orders"},
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-dest-read",
            worker_closed=True,
            dest_type="generic_sql",
            dest_cfg={"type": "sqlite", "database": "unused.db"},
        )

    assert dropped == []
    assert "do-not-log" not in str(raised.value)


def test_cursor_key_enumeration_failure_refuses_slot_drop(
    retired_store, monkeypatch
) -> None:
    from services import cdc_slot_resume

    class _Mongo:
        def find(self, *_args, **_kwargs):
            raise OSError("cursor store unavailable")

    dropped: list[str] = []
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "services.sync_cursor._mongo_cursors", lambda: _Mongo()
    )
    monkeypatch.setattr(
        "services.sync_cursor._load", lambda: {"cursors": []}
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: dropped.append("df_orders") or {"slot": "dropped"},
    )
    error_type = getattr(cdc_slot_resume, "CdcSlotRetirementError", RuntimeError)

    with pytest.raises(error_type, match="df_orders"):
        release_finished_cdc_slot(
            {"cdc_slot_name": "df_orders"},
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-keys",
            worker_closed=True,
        )

    assert dropped == []


def test_cursor_keys_for_job_default_keeps_file_keys_after_mongo_read_error(
    monkeypatch,
) -> None:
    from services import sync_cursor

    class _Mongo:
        def find(self, *_args, **_kwargs):
            raise OSError("cursor store unavailable")

    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: _Mongo())
    monkeypatch.setattr(
        sync_cursor,
        "_load",
        lambda: {
            "cursors": [
                {
                    "key": "pg:qa:partial",
                    "metadata": {"job_id": "job-partial"},
                }
            ]
        },
    )

    assert sync_cursor.cursor_keys_for_job("job-partial") == ["pg:qa:partial"]


@pytest.mark.parametrize("read_source", ["mongo", "file"])
def test_cursor_keys_for_job_strict_reraises_read_errors(
    monkeypatch, read_source
) -> None:
    from services import sync_cursor

    if read_source == "mongo":
        class _Mongo:
            def find(self, *_args, **_kwargs):
                raise OSError("cursor store unavailable")

        monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: _Mongo())
        monkeypatch.setattr(sync_cursor, "_load", lambda: {"cursors": []})
        message = "cursor store unavailable"
    else:
        monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

        def _read_file():
            raise OSError("cursor file unavailable")

        monkeypatch.setattr(sync_cursor, "_load", _read_file)
        message = "cursor file unavailable"

    with pytest.raises(OSError, match=message):
        sync_cursor.cursor_keys_for_job("job-strict", strict=True)


def test_evicted_resume_is_retired_by_file_horizon(retired_store, monkeypatch) -> None:
    import json

    from services import cdc_slot_resume
    from services.cdc_slot_resume import resume_token_retired

    slot = "df_orders"
    key = "pg:qa:orders"
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 2)
    for token in ("0/10", "0/20", "0/30"):
        retire_slot_lsn(token, cursor_key=key, slot_name=slot)

    payload = json.loads(cdc_slot_resume.STORE_PATH.read_text(encoding="utf-8"))
    assert payload["horizons"] == [
        {
            "lsn": "0/10",
            "cursor_key": key,
            "slot_name": slot,
            "family": "pg_wal",
        }
    ]
    assert slot_lsn_retired("0/10", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired("0/11", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired("1", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired(
        "0/10", cursor_key="pg:qa:other", slot_name=slot
    )
    assert not slot_lsn_retired("0/10", cursor_key=key, slot_name="df_other")
    assert resume_token_retired(
        "lsn=0/10|slot=df_orders", cursor_key=key
    )

    view = DestWmView(
        committed_lsn="0/10",
        resume_blob="lsn=0/10|slot=df_orders",
    )
    stripped, job_resume = without_retired_slot_resume(
        view, "lsn=0/10|slot=df_orders", cursor_key=key
    )
    assert stripped.committed_lsn is None
    assert stripped.resume_blob == ""
    assert job_resume is None

    above_horizon = DestWmView(
        committed_lsn="0/11",
        resume_blob="lsn=0/11|slot=df_orders",
    )
    unchanged, job_resume = without_retired_slot_resume(
        above_horizon, "lsn=0/11|slot=df_orders", cursor_key=key
    )
    assert unchanged.committed_lsn == "0/11"
    assert unchanged.resume_blob == "lsn=0/11|slot=df_orders"
    assert job_resume == "lsn=0/11|slot=df_orders"


def test_retirement_horizons_are_kept_per_lsn_family(
    retired_store, monkeypatch
) -> None:
    import json

    from services import cdc_slot_resume

    slot = "df_orders"
    key = "pg:qa:orders"
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 1)
    for token in ("0/10", "1", "0/20", "2", "0/15"):
        retire_slot_lsn(token, cursor_key=key, slot_name=slot)

    payload = json.loads(cdc_slot_resume.STORE_PATH.read_text(encoding="utf-8"))
    assert {
        (row["family"], row["lsn"]) for row in payload["horizons"]
    } == {("pg_wal", "0/20"), ("numeric_version", "2")}
    assert slot_lsn_retired("0/10", cursor_key=key, slot_name=slot)
    assert slot_lsn_retired("1", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired("0/30", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired("3", cursor_key=key, slot_name=slot)


def test_opaque_eviction_uses_exact_bounded_hashes(
    retired_store, monkeypatch
) -> None:
    import hashlib
    import json

    from services import cdc_slot_resume

    slot = "reused_slot"
    key = "pg:job:stream"
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 1)
    cdc_slot_resume.retire_slot_lsn(
        "opaque-token-old", cursor_key=key, slot_name=slot
    )
    cdc_slot_resume.retire_slot_lsn(
        "opaque-token-current", cursor_key=key, slot_name=slot
    )

    payload = json.loads(cdc_slot_resume.STORE_PATH.read_text(encoding="utf-8"))
    opaque_horizon = next(
        row for row in payload["horizons"] if row["family"] == "opaque"
    )
    assert opaque_horizon["opaque_hashes"] == [
        hashlib.sha256(b"opaque-token-old").hexdigest()
    ]
    assert "opaque-token-old" not in json.dumps(opaque_horizon)
    assert slot_lsn_retired("opaque-token-old", cursor_key=key, slot_name=slot)
    assert slot_lsn_retired("opaque-token-current", cursor_key=key, slot_name=slot)
    assert not slot_lsn_retired("opaque-token-fresh", cursor_key=key, slot_name=slot)


class _FakeRetiredMongo:
    def __init__(self):
        self.docs = []

    def find(self, query=None, *_args, **_kwargs):
        query = query or {}
        return [
            dict(doc)
            for doc in self.docs
            if all(doc.get(key) == value for key, value in query.items())
        ]

    def replace_one(self, selector, replacement, *, upsert=False):
        for index, doc in enumerate(self.docs):
            if all(
                doc.get(key) != value["$ne"]
                if isinstance(value, dict) and "$ne" in value
                else doc.get(key) == value
                for key, value in selector.items()
            ):
                self.docs[index] = dict(replacement)
                return
        if upsert:
            self.docs.append(dict(replacement))

    def delete_many(self, query):
        selectors = query.get("$nor", [])
        self.docs = [
            doc
            for doc in self.docs
            if doc.get("kind") == "horizon"
            or any(
                all(doc.get(key) == value for key, value in selector.items())
                for selector in selectors
            )
        ]

    def insert_many(self, docs):
        self.docs.extend(dict(doc) for doc in docs)


def _opaque_overflow_rows(
    slot: str, key: str, token_count: int = 503
) -> tuple[list[dict[str, str]], str, str]:
    import hashlib
    import random

    tokens = sorted(
        (f"opaque-overflow-{index}" for index in range(token_count)),
        key=lambda token: hashlib.sha256(token.encode("utf-8")).hexdigest(),
    )
    first = tokens[0]
    middle = random.Random(17).choice(tokens[1:-1])
    rows = [
        {"lsn": token, "cursor_key": key, "slot_name": slot}
        for token in tokens
    ]
    return rows, first, middle


@pytest.mark.parametrize("store", ["file", "mongo"])
def test_opaque_overflow_is_fail_closed_and_persisted_for_each_store(
    store, retired_store, monkeypatch
) -> None:
    import json

    from services import cdc_slot_resume
    from services.cdc_cursor_gap import CdcSlotGapError

    slot = "reused_slot"
    key = "pg:job:stream"
    rows, first, middle = _opaque_overflow_rows(
        slot, key, cdc_slot_resume._MAX_OPAQUE_HASHES + 1
    )
    mongo = None
    if store == "file":
        monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: None)
    else:
        mongo = _FakeRetiredMongo()
        monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: mongo)
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 0)
    cdc_slot_resume._save(rows)

    payload = json.loads(cdc_slot_resume.STORE_PATH.read_text(encoding="utf-8"))
    if store == "file":
        horizon = next(
            row
            for row in payload["horizons"]
            if row["slot_name"] == slot
            and row["cursor_key"] == key
            and row["family"] == "opaque"
        )
    else:
        assert mongo is not None
        horizon = next(
            row
            for row in mongo.docs
            if row.get("kind") == "horizon"
            and row["slot_name"] == slot
            and row["cursor_key"] == key
            and row["family"] == "opaque"
        )
        payload["horizons"] = []
        cdc_slot_resume.STORE_PATH.write_text(
            json.dumps(payload), encoding="utf-8"
        )

    assert horizon["opaque_overflow"] is True
    assert len(horizon["opaque_hashes"]) == cdc_slot_resume._MAX_OPAQUE_HASHES
    reloaded = cdc_slot_resume._load_horizons()
    assert any(
        row["slot_name"] == slot
        and row["cursor_key"] == key
        and row["family"] == "opaque"
        and row["opaque_overflow"] is True
        for row in reloaded
    )

    for token in (first, middle):
        resume = f"lsn={token}|slot={slot}"
        with pytest.raises(CdcSlotGapError) as raised:
            without_retired_slot_resume(
                DestWmView(committed_lsn=token, resume_blob=resume),
                resume,
                cursor_key=key,
            )
        assert raised.value.slot_name == slot
        assert "fresh snapshot is required" in str(raised.value).lower()

    for fresh_slot, fresh_key in (
        ("another_slot", key),
        (slot, "pg:job:another-stream"),
    ):
        token = f"opaque-never-retired-{store}-{fresh_slot}-{fresh_key}"
        resume = f"lsn={token}|slot={fresh_slot}"
        fresh_view = DestWmView(committed_lsn=token, resume_blob=resume)
        assert not slot_lsn_retired(
            token, cursor_key=fresh_key, slot_name=fresh_slot
        )
        unchanged, job_resume = without_retired_slot_resume(
            fresh_view, resume, cursor_key=fresh_key
        )
        assert unchanged.committed_lsn == token
        assert unchanged.resume_blob == resume
        assert job_resume == resume


def test_opaque_overflow_union_uses_or_semantics(retired_store, monkeypatch) -> None:
    import json

    from services import cdc_slot_resume

    mongo = _FakeRetiredMongo()
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: mongo)
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 0)
    slot = "reused_slot"
    key = "pg:job:stream"
    rows, _first, _middle = _opaque_overflow_rows(
        slot, key, cdc_slot_resume._MAX_OPAQUE_HASHES + 1
    )
    cdc_slot_resume._save(rows)
    mongo_horizon = next(
        doc for doc in mongo.docs if doc.get("kind") == "horizon"
    )
    assert mongo_horizon["opaque_overflow"] is True

    payload = json.loads(cdc_slot_resume.STORE_PATH.read_text(encoding="utf-8"))
    payload["horizons"] = [{**mongo_horizon, "opaque_overflow": False}]
    cdc_slot_resume.STORE_PATH.write_text(json.dumps(payload), encoding="utf-8")
    assert cdc_slot_resume._load_horizons()[0]["opaque_overflow"] is True


def test_retirement_horizon_survives_mongo_reload(retired_store, monkeypatch) -> None:
    import json

    from services import cdc_slot_resume

    class _Mongo:
        def __init__(self):
            self.docs = []

        def find(self, query=None, *_args, **_kwargs):
            query = query or {}
            return [
                dict(doc)
                for doc in self.docs
                if all(doc.get(key) == value for key, value in query.items())
            ]

        def replace_one(self, selector, replacement, *, upsert=False):
            for index, doc in enumerate(self.docs):
                if all(
                    doc.get(key) != value["$ne"]
                    if isinstance(value, dict) and "$ne" in value
                    else doc.get(key) == value
                    for key, value in selector.items()
                ):
                    self.docs[index] = dict(replacement)
                    return
            if upsert:
                self.docs.append(dict(replacement))

        def delete_many(self, query):
            selectors = query.get("$nor", [])
            self.docs = [
                doc
                for doc in self.docs
                if doc.get("kind") == "horizon"
                or any(
                    all(doc.get(key) == value for key, value in selector.items())
                    for selector in selectors
                )
            ]

        def insert_many(self, docs):
            self.docs.extend(dict(doc) for doc in docs)

    mongo = _Mongo()
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: mongo)
    monkeypatch.setattr(cdc_slot_resume, "_MAX_RECORDS", 2)
    slot = "df_orders"
    key = "pg:qa:orders"
    for token in ("0/10", "0/20", "0/30"):
        retire_slot_lsn(token, cursor_key=key, slot_name=slot)

    assert any(doc.get("kind") == "horizon" for doc in mongo.docs)
    assert all(
        doc.get("kind") != "horizon" for doc in cdc_slot_resume._load()
    )
    cdc_slot_resume.STORE_PATH.write_text(
        json.dumps({"resumes": [], "horizons": []}), encoding="utf-8"
    )
    assert slot_lsn_retired("0/10", cursor_key=key, slot_name=slot)


def test_retirement_load_unions_mongo_and_file_rows(retired_store, monkeypatch) -> None:
    import json

    from services import cdc_slot_resume

    class _Mongo:
        def find(self, *_args, **_kwargs):
            return [{"lsn": "0/10", "cursor_key": "mongo-key", "slot_name": "slot"}]

    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: _Mongo())
    cdc_slot_resume.STORE_PATH.write_text(
        json.dumps(
            {
                "resumes": [
                    {"lsn": "0/20", "cursor_key": "file-key", "slot_name": "slot"}
                ],
                "horizons": [],
            }
        ),
        encoding="utf-8",
    )

    assert {
        (row["lsn"], row["cursor_key"], row["slot_name"])
        for row in cdc_slot_resume._load()
    } == {("0/10", "mongo-key", "slot"), ("0/20", "file-key", "slot")}


def test_retirement_survives_old_delete_insert_crash_window(
    retired_store, monkeypatch
) -> None:
    import json

    from services import cdc_slot_resume

    class _Mongo:
        def __init__(self):
            self.docs = []

        def find(self, *_args, **_kwargs):
            return [dict(doc) for doc in self.docs]

        def delete_many(self, _query):
            self.docs.clear()

    mongo = _Mongo()
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: mongo)
    row = {"lsn": "0/20", "cursor_key": "key", "slot_name": "slot"}
    cdc_slot_resume.STORE_PATH.write_text(
        json.dumps({"resumes": [row], "horizons": []}), encoding="utf-8"
    )
    mongo.delete_many({"kind": {"$ne": "horizon"}})

    assert slot_lsn_retired("0/20", cursor_key="key", slot_name="slot")


def test_retirement_upserts_before_stale_mongo_delete(
    retired_store, monkeypatch
) -> None:
    from services import cdc_slot_resume

    class _Mongo:
        def __init__(self):
            self.docs = []
            self.calls = []

        def find(self, *_args, **_kwargs):
            return [dict(doc) for doc in self.docs]

        def replace_one(self, selector, replacement, *, upsert=False):
            self.calls.append("upsert")
            self.docs.append(dict(replacement))

        def delete_many(self, _query):
            self.calls.append("delete")
            raise OSError("crash after upsert")

    mongo = _Mongo()
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: mongo)
    row = {"lsn": "0/20", "cursor_key": "key", "slot_name": "slot"}

    with pytest.raises(OSError, match="crash after upsert"):
        cdc_slot_resume._save([row])

    assert mongo.calls == ["upsert", "delete"]
    assert slot_lsn_retired("0/20", cursor_key="key", slot_name="slot")


def test_post_drop_retirement_failure_is_reported(retired_store, monkeypatch, caplog) -> None:
    from services import cdc_slot_resume

    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *_a, **_k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": "dropped", "slot_name": "df_orders"},
    )
    monkeypatch.setattr(
        "services.sync_cursor.clear_watermark", lambda _key: {"cleared": True}
    )
    monkeypatch.setattr(
        cdc_slot_resume,
        "retire_after_slot_drop",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("post-drop write failed")),
        raising=False,
    )

    with caplog.at_level("ERROR"):
        result = release_finished_cdc_slot(
            {"cdc_slot_name": "df_orders", "cursor_key": "pg:qa:orders"},
            reason="failed",
            source_cfg={"type": "postgresql"},
            job_id="job-post-drop",
            worker_closed=True,
        )

    assert result["released"] is True
    assert result["dest_resume_blank_failed"] is True
    assert result["dest_resume_blank_error"] == "OSError"
    assert "df_orders" in caplog.text
    assert "retired_lsns" not in result


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
