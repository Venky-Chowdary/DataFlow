"""CDC dest-owned watermark exactly-once — algorithm + sqlite proofs.

Named fixture: tests/fixtures/cdc_exactly_once_matrix.json
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.cdc_eos_sql import (  # noqa: E402
    apply_change_batch_exactly_once,
    apply_eos_bundle,
    dest_engine_count,
    dest_watermark_lsn,
    dest_watermark_view,
    open_eos_session,
)
from connectors.lsn_guards import DF_LSN_COL  # noqa: E402
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    ALGORITHM,
    PLATFORM_EXACTLY_ONCE_CLAIMED,
    PROTOCOL,
    WATERMARK_TABLE,
    REASON_APPEND,
    REASON_BUNDLE_LSN,
    REASON_CHECKSUM,
    REASON_DEST_NOT_TXN,
    REASON_NOT_CDC,
    REASON_OK,
    EosBundleStream,
    EosCrash,
    ExactlyOnceRouteError,
    REASON_STALE_FENCE,
    already_committed,
    assert_requested_cdc_delivery,
    assert_writer_fence,
    clamp_job_resume_to_dest,
    combine_change_batch,
    chaos_crash_after_commit_redelivery,
    chaos_crash_before_commit_then_retry,
    classify_exactly_once_route,
    assert_bundle_members_reached,
    dest_owned_stream_wins,
    decide_eos_apply,
    eos_stream_key,
)
from services.execution_engine_contract import (  # noqa: E402
    DeliveryGuaranteeError,
    assert_delivery_guarantee_allowed,
    execution_contract_dict,
)

MATRIX_PATH = _API_ROOT / "tests" / "fixtures" / "cdc_exactly_once_matrix.json"


def _batch(lsn: str, *, inserts=None, updates=None, deletes=None) -> ChangeBatch:
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=list(updates or []),
        deletes=list(deletes or []),
        resume_token={"lsn": lsn},
    )


def test_platform_never_claims_all_cdc_is_exactly_once() -> None:
    assert PLATFORM_EXACTLY_ONCE_CLAIMED is False
    blob = execution_contract_dict()
    assert blob["delivery_default"] == "at_least_once"
    assert blob["never_claim_exactly_once"] is True
    assert blob["capabilities"]["exactly_once"]["available"] is True
    assert blob["capabilities"]["exactly_once"]["platform_claimed"] is False
    assert "exactly_once" in blob["selectable_delivery"]
    assert assert_delivery_guarantee_allowed("exactly_once") == "exactly_once"
    with pytest.raises(DeliveryGuaranteeError):
        assert_delivery_guarantee_allowed("at_most_once")


def test_classify_fail_closed_ineligible_routes() -> None:
    csv = classify_exactly_once_route(
        dest_type="csv", sync_mode="cdc", has_primary_key=True
    )
    assert csv.eligible is False
    assert csv.reason == REASON_DEST_NOT_TXN

    pg = classify_exactly_once_route(
        dest_type="postgresql", sync_mode="cdc", has_primary_key=True
    )
    assert pg.eligible is True
    assert pg.reason == REASON_OK
    assert pg.wired is True
    assert pg.algorithm == ALGORITHM

    append = classify_exactly_once_route(
        dest_type="sqlite",
        sync_mode="cdc",
        has_primary_key=True,
        allow_append_only=True,
    )
    assert append.reason == REASON_APPEND

    refresh = classify_exactly_once_route(
        dest_type="sqlite", sync_mode="full_refresh_overwrite", has_primary_key=True
    )
    assert refresh.reason == REASON_NOT_CDC

    ok = classify_exactly_once_route(
        dest_type="sqlite", sync_mode="cdc", has_primary_key=True
    )
    assert ok.eligible is True
    assert ok.reason == REASON_OK
    assert ok.wired is True

    azure = classify_exactly_once_route(
        dest_type="azure_sql_database", sync_mode="cdc", has_primary_key=True
    )
    assert azure.eligible is True
    assert azure.wired is True

    duck = classify_exactly_once_route(
        dest_type="duckdb", sync_mode="cdc", has_primary_key=True
    )
    assert duck.eligible is True
    assert duck.wired is True


def test_assert_requested_refuses_ineligible_exactly_once() -> None:
    assert (
        assert_requested_cdc_delivery("at_least_once", sync_mode="cdc", dest_type="csv")
        == "at_least_once"
    )
    with pytest.raises(ExactlyOnceRouteError) as exc:
        assert_requested_cdc_delivery(
            "exactly_once",
            sync_mode="cdc",
            dest_type="csv",
            has_primary_key=True,
        )
    assert exc.value.reason == REASON_DEST_NOT_TXN
    assert (
        assert_requested_cdc_delivery(
            "exactly_once",
            sync_mode="cdc",
            dest_type="sqlite",
            has_primary_key=True,
        )
        == "exactly_once"
    )


def test_already_committed_compare() -> None:
    assert already_committed("0/100", "0/200") is True
    assert already_committed("0/200", "0/200") is True
    assert already_committed("0/300", "0/200") is False
    assert already_committed("0/100", None) is False


def test_dest_authoritative_resume_rewinds_job_ahead() -> None:
    """Honoring a job cursor ahead of dest would skip uncommitted LSNs."""
    resume, proof = clamp_job_resume_to_dest({"lsn": "0/500"}, "0/200")
    assert proof["clamped"] is True
    assert proof["reason"] == "job_ahead_rewound_to_dest"
    assert resume["lsn"] == "0/200"


def test_dest_authoritative_resume_fast_forwards_job_behind() -> None:
    resume, proof = clamp_job_resume_to_dest("0/100", "0/300")
    assert proof["clamped"] is True
    assert proof["reason"] == "job_behind_fast_forward_to_dest"
    assert resume == "0/300"


def test_stale_writer_fence_refuses_zombie() -> None:
    assert_writer_fence(5, 5)
    assert_writer_fence(6, 5)
    assert_writer_fence(0, 0)
    with pytest.raises(ExactlyOnceRouteError) as exc:
        assert_writer_fence(3, 5)
    assert exc.value.reason == REASON_STALE_FENCE


def test_decide_handoff_skips_checksum() -> None:
    action, fence = decide_eos_apply(
        incoming_lsn="0/10",
        dest_lsn="0/10",
        incoming_phase="streaming",
        dest_phase="snapshot",
        incoming_checksum="aaa",
        dest_checksum="bbb",
        incoming_fence=1,
        dest_fence=1,
    )
    assert action == "handoff_phase"
    assert fence == 1


def test_dest_owned_stream_wins_skips_incremental_after_stream() -> None:
    assert dest_owned_stream_wins(
        incoming_phase="snapshot",
        dest_phase="streaming",
        incoming_lsn="0/10",
        dest_lsn="0/10",
        incremental_snapshot=True,
    )
    action, _fence = decide_eos_apply(
        incoming_lsn="0/10",
        dest_lsn="0/10",
        incoming_phase="snapshot",
        dest_phase="streaming",
        incremental_snapshot=True,
        incoming_checksum="snap",
        dest_checksum="stream",
    )
    assert action == "stream_wins_skip"


def test_bundle_coordinator_refuses_member_behind() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        assert_bundle_members_reached(["0/10", "0/05"], "0/10")
    assert exc.value.reason == REASON_BUNDLE_LSN
    assert_bundle_members_reached(["0/10", "0/20"], "0/10")


def test_decide_same_lsn_payload_mismatch_refuses() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        decide_eos_apply(
            incoming_lsn="0/10",
            dest_lsn="0/10",
            incoming_phase="streaming",
            dest_phase="streaming",
            incoming_checksum="aaa",
            dest_checksum="bbb",
        )
    assert exc.value.reason == REASON_CHECKSUM


def test_combine_batch_last_op_per_pk_wins() -> None:
    combined = combine_change_batch(
        _batch(
            "0/9",
            inserts=[{"id": "1", "v": "a"}, {"id": "2", "v": "keep"}],
            updates=[{"id": "1", "v": "b"}],
            deletes=["1"],
        ),
        pk_cols=["id"],
    )
    assert combined.deletes == ["1"]
    assert [r["id"] for r in combined.updates] == ["2"]
    assert combined.inserts == []


def test_sqlite_eos_stale_fence_does_not_commit() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_fence.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=_batch("0/10", inserts=[{"id": "1", "v": "first"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="fence|orders",
            writer_fence=4,
        )
        with pytest.raises(ExactlyOnceRouteError) as exc:
            apply_change_batch_exactly_once(
                dest_type="sqlite",
                dest_cfg=dest_cfg,
                dest_table="orders",
                change=_batch("0/20", inserts=[{"id": "1", "v": "zombie"}]),
                mappings=mappings,
                column_types=types,
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key="fence|orders",
                writer_fence=2,
            )
        assert exc.value.reason == REASON_STALE_FENCE
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert dest_watermark_lsn(dest_cfg, "fence|orders") == "0/10"


def test_chaos_crash_before_commit_retries_once() -> None:
    store = chaos_crash_before_commit_then_retry()
    assert store.rollback_calls == 1
    assert store.commit_calls == 1
    assert store.rows["1"]["v"] == "first"
    assert store.watermarks["s|db|t"].committed_lsn == "0/100"
    assert store.watermarks["s|db|t"].epoch == 1


def test_chaos_crash_after_commit_redelivery_is_noop() -> None:
    store = chaos_crash_after_commit_redelivery()
    assert store.commit_calls == 1
    assert store.rows["1"]["v"] == "new"
    assert store.rows["1"][DF_LSN_COL] == "0/200"
    wm = store.watermarks["s|db|t"]
    assert wm.committed_lsn == "0/200"
    assert wm.epoch == 1


def test_sqlite_eos_apply_and_redelivery_count_once() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        first = _batch(
            "0/100",
            inserts=[{"id": "1", "v": "first"}],
        )
        rows, _ck, summary, deleted = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=first,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="sqlite|eos|orders",
        )
        assert summary["exactly_once_active"] is True
        assert summary["exactly_once_claimed_platform"] is False
        assert summary["delivery_semantics"] == "exactly_once_dest_owned_watermark_txn"
        assert rows == 1
        assert deleted == 0
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert dest_watermark_lsn(dest_cfg, "sqlite|eos|orders") == "0/100"

        redelivery = _batch(
            "0/100",
            inserts=[{"id": "1", "v": "first"}],
        )
        rows2, _ck2, summary2, _del2 = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=redelivery,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="sqlite|eos|orders",
        )
        assert summary2["eos_already_committed"] is True
        assert rows2 == 0
        assert dest_engine_count(dest_cfg, "orders") == 1
        conn = sqlite3.connect(path)
        try:
            v, lsn = conn.execute(
                f'SELECT v, "{DF_LSN_COL}" FROM orders WHERE id = ?', ("1",)
            ).fetchone()
        finally:
            conn.close()
        assert v == "first"
        assert str(lsn) == "0/100"


def test_sqlite_eos_crash_before_watermark_then_retry() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        change = _batch("0/150", inserts=[{"id": "9", "v": "x"}])
        with pytest.raises(EosCrash):
            apply_change_batch_exactly_once(
                dest_type="sqlite",
                dest_cfg=dest_cfg,
                dest_table="t",
                change=change,
                mappings=mappings,
                column_types=types,
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key="k",
                crash_after="after_apply_before_watermark",
            )
        assert dest_engine_count(dest_cfg, "t") == 0
        assert dest_watermark_lsn(dest_cfg, "k") is None
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="t",
            change=change,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="k",
        )
        assert dest_engine_count(dest_cfg, "t") == 1
        assert dest_watermark_lsn(dest_cfg, "k") == "0/150"


def test_eos_refuses_unwired_file_dest() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        apply_change_batch_exactly_once(
            dest_type="csv",
            dest_cfg={},
            dest_table="t",
            change=_batch("0/1", inserts=[{"id": "1"}]),
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "string"},
            headers=["id"],
            pk_target_cols=["id"],
        )
    assert exc.value.reason == REASON_DEST_NOT_TXN


def test_sqlalchemy_sqlite_eos_apply_and_redelivery() -> None:
    """Prove the portable SQLAlchemy coordinator on a sqlite file (no network)."""
    from connectors.cdc_eos_sa import sa_dest_engine_count, sa_dest_watermark_lsn

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_sa.db")
        dest_cfg = {"type": "sqlite", "database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        first = _batch("0/300", inserts=[{"id": "2", "v": "sa"}])
        rows, _ck, summary, _del = apply_change_batch_exactly_once(
            dest_type="generic_sql",
            dest_cfg=dest_cfg,
            dest_table="lines",
            change=first,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="sa|eos|lines",
        )
        assert summary["exactly_once_active"] is True
        assert rows == 1
        assert sa_dest_engine_count(dest_cfg, "lines", "generic_sql") == 1
        assert sa_dest_watermark_lsn(dest_cfg, "sa|eos|lines", "generic_sql") == "0/300"
        rows2, _ck2, summary2, _del2 = apply_change_batch_exactly_once(
            dest_type="generic_sql",
            dest_cfg=dest_cfg,
            dest_table="lines",
            change=_batch("0/300", inserts=[{"id": "2", "v": "sa"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="sa|eos|lines",
        )
        assert summary2["eos_already_committed"] is True
        assert rows2 == 0
        assert sa_dest_engine_count(dest_cfg, "lines", "generic_sql") == 1


def test_sqlalchemy_eos_crash_before_watermark_then_retry() -> None:
    """SA dest txn must roll back apply when watermark is not committed."""
    from connectors.cdc_eos_sa import sa_dest_engine_count, sa_dest_watermark_lsn

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_sa_crash.db")
        dest_cfg = {"type": "sqlite", "database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        change = _batch("0/350", inserts=[{"id": "3", "v": "x"}])
        with pytest.raises(EosCrash):
            apply_change_batch_exactly_once(
                dest_type="generic_sql",
                dest_cfg=dest_cfg,
                dest_table="crash",
                change=change,
                mappings=mappings,
                column_types=types,
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key="sa|eos|crash",
                crash_after="after_apply_before_watermark",
            )
        assert sa_dest_engine_count(dest_cfg, "crash", "generic_sql") == 0
        assert sa_dest_watermark_lsn(dest_cfg, "sa|eos|crash", "generic_sql") is None
        apply_change_batch_exactly_once(
            dest_type="generic_sql",
            dest_cfg=dest_cfg,
            dest_table="crash",
            change=change,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="sa|eos|crash",
        )
        assert sa_dest_engine_count(dest_cfg, "crash", "generic_sql") == 1
        assert sa_dest_watermark_lsn(dest_cfg, "sa|eos|crash", "generic_sql") == "0/350"


def test_sqlite_eos_checksum_mismatch_refuses_overwrite() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_ck.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=_batch("0/80", inserts=[{"id": "1", "v": "first"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="ck|orders",
        )
        with pytest.raises(ExactlyOnceRouteError) as exc:
            apply_change_batch_exactly_once(
                dest_type="sqlite",
                dest_cfg=dest_cfg,
                dest_table="orders",
                change=_batch("0/80", inserts=[{"id": "1", "v": "other"}]),
                mappings=mappings,
                column_types=types,
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key="ck|orders",
            )
        assert exc.value.reason == REASON_CHECKSUM
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert exc.value.quarantine
        assert exc.value.quarantine[0]["failure_reason"] == REASON_CHECKSUM
        conn = sqlite3.connect(path)
        try:
            v = conn.execute("SELECT v FROM orders WHERE id = ?", ("1",)).fetchone()[0]
        finally:
            conn.close()
        assert v == "first"


def test_sqlite_eos_snapshot_stream_handoff_no_double_write() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_ho.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        snap = ChangeBatch(
            inserts=[{"id": "1", "v": "snap"}],
            resume_token={"lsn": "0/90", "phase": "snapshot"},
        )
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=snap,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="ho|orders",
        )
        assert dest_watermark_view(dest_cfg, "ho|orders").phase == "snapshot"
        stream = ChangeBatch(
            resume_token={"lsn": "0/90", "phase": "streaming"},
        )
        rows, _ck, summary, _del = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=stream,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="ho|orders",
        )
        assert summary["eos_status"] == "handoff_phase"
        assert rows == 0
        assert dest_engine_count(dest_cfg, "orders") == 1
        view = dest_watermark_view(dest_cfg, "ho|orders")
        assert view.phase == "streaming"
        assert view.committed_lsn == "0/90"


def _bundle_stream(table: str, key: str, change: ChangeBatch) -> EosBundleStream:
    return EosBundleStream(
        dest_table=table,
        change=change,
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ],
        column_types={"id": "string", "v": "string"},
        pk_target_cols=["id"],
        stream_key=key,
        headers=["id", "v"],
    )


def test_sqlite_eos_bundle_crash_rolls_back_both_then_retry() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_bundle.db")
        dest_cfg = {"database": path}
        streams = [
            _bundle_stream(
                "orders",
                "b|orders",
                _batch("0/500", inserts=[{"id": "1", "v": "o"}]),
            ),
            _bundle_stream(
                "users",
                "b|users",
                _batch("0/500", inserts=[{"id": "u1", "v": "u"}]),
            ),
        ]
        with pytest.raises(EosCrash):
            apply_eos_bundle(
                dest_type="sqlite",
                dest_cfg=dest_cfg,
                streams=streams,
                incoming_lsn="0/500",
                bundle_key="b|shared",
                crash_after="after_watermark_before_commit",
            )
        assert dest_engine_count(dest_cfg, "orders") == 0
        assert dest_engine_count(dest_cfg, "users") == 0
        assert dest_watermark_lsn(dest_cfg, "b|orders") is None
        assert dest_watermark_lsn(dest_cfg, "b|shared") is None
        result = apply_eos_bundle(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            streams=streams,
            incoming_lsn="0/500",
            bundle_key="b|shared",
        )
        assert result.rows_written == 2
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert dest_engine_count(dest_cfg, "users") == 1
        assert dest_watermark_lsn(dest_cfg, "b|orders") == "0/500"
        assert dest_watermark_lsn(dest_cfg, "b|users") == "0/500"
        assert dest_watermark_lsn(dest_cfg, "b|shared") == "0/500"
        again = apply_eos_bundle(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            streams=streams,
            incoming_lsn="0/500",
            bundle_key="b|shared",
        )
        assert again.already_committed is True
        assert again.rows_written == 0
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert dest_engine_count(dest_cfg, "users") == 1


def test_sqlalchemy_eos_bundle_crash_then_retry() -> None:
    from connectors.cdc_eos_sa import sa_dest_engine_count, sa_dest_watermark_lsn

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_sa_bundle.db")
        dest_cfg = {"type": "sqlite", "database": path}
        streams = [
            _bundle_stream(
                "orders",
                "sa|b|orders",
                _batch("0/600", inserts=[{"id": "1", "v": "o"}]),
            ),
            _bundle_stream(
                "users",
                "sa|b|users",
                _batch("0/600", inserts=[{"id": "u1", "v": "u"}]),
            ),
        ]
        with pytest.raises(EosCrash):
            apply_eos_bundle(
                dest_type="generic_sql",
                dest_cfg=dest_cfg,
                streams=streams,
                incoming_lsn="0/600",
                bundle_key="sa|b|shared",
                crash_after="after_watermark_before_commit",
            )
        assert sa_dest_engine_count(dest_cfg, "orders", "generic_sql") == 0
        assert sa_dest_watermark_lsn(dest_cfg, "sa|b|orders", "generic_sql") is None
        result = apply_eos_bundle(
            dest_type="generic_sql",
            dest_cfg=dest_cfg,
            streams=streams,
            incoming_lsn="0/600",
            bundle_key="sa|b|shared",
        )
        assert result.rows_written == 2
        assert sa_dest_engine_count(dest_cfg, "orders", "generic_sql") == 1
        assert sa_dest_engine_count(dest_cfg, "users", "generic_sql") == 1
        assert sa_dest_watermark_lsn(dest_cfg, "sa|b|shared", "generic_sql") == "0/600"


def test_shared_transfer_eos_bundle_two_tables_one_dest_txn(tmp_path, monkeypatch) -> None:
    """Shared-log EOS: N tables land in one dest txn; redelivery does not double-write."""
    from unittest.mock import patch

    from services.sync_cursor import SyncContract
    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig

    class FakeCdc:
        def __init__(self, *a, **k):
            self._polled = False

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(
                inserts=[{"id": "1", "v": "o"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="orders",
                ack_barrier=False,
            )
            yield ChangeBatch(
                inserts=[{"id": "u1", "v": "u"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="users",
                ack_barrier=False,
            )
            yield ChangeBatch(
                resume_token="slot=s|phase=streaming|lsn=0/1",
                ack_barrier=True,
            )

        def poll(self):
            if self._polled:
                return
                yield  # pragma: no cover
            self._polled = True
            yield ChangeBatch(
                updates=[{"id": "1", "v": "o2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="orders",
                ack_barrier=False,
            )
            yield ChangeBatch(
                inserts=[{"id": "u2", "v": "u2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="users",
                ack_barrier=True,
            )

        def ack(self, token=None):
            self.acked = token

        def close(self):
            pass

    dest_path = str(tmp_path / "eos_shared.db")
    dest_cfg = {"database": dest_path}
    job_id = f"job-eos-bundle-{uuid.uuid4().hex[:10]}"
    source = EndpointConfig(
        kind="database",
        format="postgresql",
        database=f"app_{job_id}",
        table="orders",
        schema="public",
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=dest_path, table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    kwargs = dict(
        sync_mode="cdc",
        stream_contracts=[
            {"name": "orders", "selected": True, "primary_key": "id", "mappings": mappings},
            {"name": "users", "selected": True, "primary_key": "id", "mappings": mappings},
        ],
        selected=selected,
        job_id=job_id,
        checkpoint=None,
        checkpoint_service=None,
        backfill_new_fields=False,
        validation_mode="strict",
        limit=0,
        delivery_guarantee="exactly_once",
    )
    monkeypatch.setenv("DATAFLOW_CDC_MAX_IDLE_POLLS", "1")
    monkeypatch.setenv("DATAFLOW_CDC_MAX_POLL_ROUNDS", "2")
    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc):
        rows, ddl, summary, _ = _run_cdc_shared_multi_table(
            source, destination, mappings, {"id": "string", "v": "string"}, None, **kwargs
        )
    assert any("exactly_once" in line for line in ddl)
    assert summary.get("exactly_once_active") is True
    assert summary.get("exactly_once_claimed_platform") is False
    assert summary.get("exactly_once_protocol") == PROTOCOL
    assert summary.get("eos_bundle") is True
    assert dest_engine_count(dest_cfg, "orders") == 1
    assert dest_engine_count(dest_cfg, "users") == 2
    from services.cdc_multi_table import shared_route_cursor_key

    shared_key = shared_route_cursor_key(
        engine="postgresql",
        database=f"app_{job_id}",
        tables=["orders", "users"],
        job_id=job_id,
    )
    assert dest_watermark_lsn(dest_cfg, shared_key) == "0/2"
    first_orders = dest_engine_count(dest_cfg, "orders")
    first_users = dest_engine_count(dest_cfg, "users")
    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc):
        _run_cdc_shared_multi_table(
            source, destination, mappings, {"id": "string", "v": "string"}, None, **kwargs
        )
    assert dest_engine_count(dest_cfg, "orders") == first_orders
    assert dest_engine_count(dest_cfg, "users") == first_users
    assert rows >= 3


def test_sqlite_eos_open_raises_fence_without_data() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_open.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        token = {"lsn": "0/70", "slot": "df_open", "phase": "streaming"}
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=ChangeBatch(inserts=[{"id": "1", "v": "keep"}], resume_token=token),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="open|orders",
            writer_fence=2,
        )
        opened = open_eos_session(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            stream_key="open|orders",
            incoming_fence=5,
            job_resume=None,
        )
        assert opened.fence_raised is True
        assert opened.fence_epoch == 5
        assert dest_engine_count(dest_cfg, "orders") == 1
        assert dest_watermark_view(dest_cfg, "open|orders").fence_epoch == 5
        assert opened.resume["lsn"] == "0/70"
        assert opened.resume.get("slot") == "df_open"
        with pytest.raises(ExactlyOnceRouteError) as exc:
            apply_change_batch_exactly_once(
                dest_type="sqlite",
                dest_cfg=dest_cfg,
                dest_table="orders",
                change=_batch("0/80", inserts=[{"id": "1", "v": "zombie"}]),
                mappings=mappings,
                column_types=types,
                headers=["id", "v"],
                pk_target_cols=["id"],
                cursor_key="open|orders",
                writer_fence=3,
            )
        assert exc.value.reason == REASON_STALE_FENCE
        assert dest_engine_count(dest_cfg, "orders") == 1


def test_sqlite_eos_incremental_snapshot_stream_wins() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "eos_ddd3.db")
        dest_cfg = {"database": path}
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "v", "target": "v", "confidence": 1.0},
        ]
        types = {"id": "string", "v": "string"}
        apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=_batch("0/40", inserts=[{"id": "1", "v": "stream"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="ddd3|orders",
        )
        snap = ChangeBatch(
            inserts=[{"id": "1", "v": "stale-read"}],
            resume_token={
                "lsn": "0/40",
                "phase": "snapshot",
                "incremental_snapshot": True,
            },
        )
        rows, _ck, summary, _del = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=snap,
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="ddd3|orders",
        )
        assert summary["eos_status"] == "stream_wins_skip"
        assert rows == 0
        assert dest_engine_count(dest_cfg, "orders") == 1
        conn = sqlite3.connect(path)
        try:
            v = conn.execute("SELECT v FROM orders WHERE id = ?", ("1",)).fetchone()[0]
        finally:
            conn.close()
        assert v == "stream"


def test_named_matrix_artifact_matches_measured() -> None:
    """Write / verify the named fixture. Floor is 1.0 on this matrix only."""
    cases = [
        {
            "id": "sqlite_cdc_pk_upsert",
            "dest": "sqlite",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "postgres_cdc_wired",
            "dest": "postgresql",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "mysql_cdc_wired",
            "dest": "mysql",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "sqlserver_cdc_wired",
            "dest": "sqlserver",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "generic_sql_cdc_wired",
            "dest": "generic_sql",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "duckdb_cdc_wired",
            "dest": "duckdb",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "oracle_cdc_wired",
            "dest": "oracle",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "snowflake_cdc_wired",
            "dest": "snowflake",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "azure_sql_alias_wired",
            "dest": "azure_sql_database",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": True,
        },
        {
            "id": "sqlite_callable_refused",
            "dest": "sqlite",
            "sync_mode": "cdc",
            "has_pk": True,
            "callable_source": True,
            "expect_eligible": False,
            "expect_reason": "exactly_once_refuses_callable_source",
        },
        {
            "id": "csv_append_not_txn",
            "dest": "csv",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_DEST_NOT_TXN,
        },
        {
            "id": "sqlite_full_refresh_refused",
            "dest": "sqlite",
            "sync_mode": "full_refresh_overwrite",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_NOT_CDC,
        },
        {
            "id": "sqlite_append_only_refused",
            "dest": "sqlite",
            "sync_mode": "cdc",
            "has_pk": True,
            "allow_append_only": True,
            "expect_eligible": False,
            "expect_reason": REASON_APPEND,
        },
        {
            "id": "iceberg_not_txn",
            "dest": "iceberg",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_DEST_NOT_TXN,
        },
        {
            "id": "kafka_not_txn",
            "dest": "kafka",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_DEST_NOT_TXN,
        },
    ]
    results = []
    for case in cases:
        elig = classify_exactly_once_route(
            dest_type=case["dest"],
            sync_mode=case["sync_mode"],
            has_primary_key=case["has_pk"],
            allow_append_only=bool(case.get("allow_append_only")),
            callable_source=bool(case.get("callable_source")),
        )
        assert elig.eligible is case["expect_eligible"], case["id"]
        if "expect_reason" in case:
            assert elig.reason == case["expect_reason"], case["id"]
        results.append(
            {
                "id": case["id"],
                "dest": case["dest"],
                "sync_mode": case["sync_mode"],
                "eligible": elig.eligible,
                "reason": elig.reason,
                "wired": elig.wired,
                "pass": True,
            }
        )
    payload = {
        "name": "cdc_exactly_once_matrix",
        "algorithm": ALGORITHM,
        "protocol": PROTOCOL,
        "platform_exactly_once_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
        "delivery_default": "at_least_once",
        "measured_floor": 1.0,
        "pass": len(results),
        "fail": 0,
        "skip": 0,
        "cases": results,
        "notes": [
            "100% means this named fixture only — not live warehouse CDC.",
            "Protocol dest_authoritative_open_bundle: dest SSOT + Open fence "
            "(no data) + dest resume blob + last-op-per-PK + shared-log N-table "
            "dest txn + dest-owned DDD-3 stream-wins + checksum quarantine + "
            "bundle min-LSN.",
            "Wired dests: sqlite (native) plus SQLAlchemy dest-txn for "
            "postgresql/mysql/sqlserver/duckdb/generic_sql/oracle/snowflake.",
            "File/Iceberg/Kafka dests stay fail-closed.",
        ],
    }
    MATRIX_PATH.parent.mkdir(parents=True, exist_ok=True)
    MATRIX_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    loaded = json.loads(MATRIX_PATH.read_text())
    assert loaded["pass"] == 15
    assert loaded["fail"] == 0
    assert loaded["measured_floor"] == 1.0
    assert loaded["platform_exactly_once_claimed"] is False


def test_eos_stream_key_prefers_cursor() -> None:
    assert (
        eos_stream_key(
            dest_type="sqlite",
            dest_database="db",
            dest_object="t",
            cursor_key="job-cursor",
        )
        == "job-cursor"
    )


def _pg_ready() -> bool:
    import socket

    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        from connectors.postgresql_conn import get_connection

        with get_connection(
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            connection_string="",
            ssl=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return bool(cur.fetchone())
    except Exception:
        return False


@pytest.mark.skipif(not _pg_ready(), reason="PostgreSQL not reachable on localhost:5432")
def test_postgres_eos_apply_and_redelivery_live() -> None:
    import uuid as _uuid

    from connectors.cdc_eos_sa import sa_dest_engine_count, sa_dest_watermark_lsn

    table = f"eos_pg_{_uuid.uuid4().hex[:8]}"
    dest_cfg = {
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    types = {"id": "string", "v": "string"}
    key = f"pg|{table}"
    try:
        rows, _ck, summary, _del = apply_change_batch_exactly_once(
            dest_type="postgresql",
            dest_cfg=dest_cfg,
            dest_table=table,
            change=_batch("0/400", inserts=[{"id": "1", "v": "pg"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key=key,
        )
        assert summary["exactly_once_active"] is True
        assert rows == 1
        assert sa_dest_engine_count(dest_cfg, table, "postgresql") == 1
        rows2, _ck2, summary2, _del2 = apply_change_batch_exactly_once(
            dest_type="postgresql",
            dest_cfg=dest_cfg,
            dest_table=table,
            change=_batch("0/400", inserts=[{"id": "1", "v": "pg"}]),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key=key,
        )
        assert summary2["eos_already_committed"] is True
        assert rows2 == 0
        assert sa_dest_watermark_lsn(dest_cfg, key, "postgresql") == "0/400"
    finally:
        try:
            from connectors.postgresql_conn import get_connection
            from psycopg2 import sql

            with get_connection(
                host="localhost",
                port=5432,
                database="dataflow",
                username="dataflow",
                password="dataflow",
                connection_string="",
                ssl=False,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
                    cur.execute(f"DELETE FROM {WATERMARK_TABLE} WHERE stream_key = %s", (key,))
                conn.commit()
        except Exception:
            pass
