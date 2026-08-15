"""CDC dest-owned watermark exactly-once — algorithm + sqlite proofs.

Named fixture: tests/fixtures/cdc_exactly_once_matrix.json
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.cdc_eos_sql import (  # noqa: E402
    apply_change_batch_exactly_once,
    dest_engine_count,
    dest_watermark_lsn,
)
from connectors.lsn_guards import DF_LSN_COL  # noqa: E402
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    ALGORITHM,
    PLATFORM_EXACTLY_ONCE_CLAIMED,
    REASON_APPEND,
    REASON_DEST_NOT_TXN,
    REASON_DEST_NOT_WIRED,
    REASON_NOT_CDC,
    REASON_OK,
    EosCrash,
    ExactlyOnceRouteError,
    already_committed,
    assert_requested_cdc_delivery,
    chaos_crash_after_commit_redelivery,
    chaos_crash_before_commit_then_retry,
    classify_exactly_once_route,
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
    assert pg.eligible is False
    assert pg.reason == REASON_DEST_NOT_WIRED
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


def test_assert_requested_refuses_ineligible_exactly_once() -> None:
    assert (
        assert_requested_cdc_delivery("at_least_once", sync_mode="cdc", dest_type="csv")
        == "at_least_once"
    )
    with pytest.raises(ExactlyOnceRouteError) as exc:
        assert_requested_cdc_delivery(
            "exactly_once",
            sync_mode="cdc",
            dest_type="postgresql",
            has_primary_key=True,
        )
    assert exc.value.reason == REASON_DEST_NOT_WIRED
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
            inserts=[{"id": "1", "v": "dup"}],
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


def test_sqlite_eos_refuses_unwired_dest() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        apply_change_batch_exactly_once(
            dest_type="postgresql",
            dest_cfg={},
            dest_table="t",
            change=_batch("0/1", inserts=[{"id": "1"}]),
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "string"},
            headers=["id"],
            pk_target_cols=["id"],
        )
    assert exc.value.reason == "exactly_once_dest_txn_not_wired"


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
            "id": "postgres_cdc_not_wired",
            "dest": "postgresql",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_DEST_NOT_WIRED,
        },
        {
            "id": "mysql_cdc_not_wired",
            "dest": "mysql",
            "sync_mode": "cdc",
            "has_pk": True,
            "expect_eligible": False,
            "expect_reason": REASON_DEST_NOT_WIRED,
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
        "platform_exactly_once_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
        "delivery_default": "at_least_once",
        "measured_floor": 1.0,
        "pass": len(results),
        "fail": 0,
        "skip": 0,
        "cases": results,
        "notes": [
            "100% means this named fixture only — not live warehouse CDC.",
            "sqlite dest-owned watermark txn is the wired EOS path.",
            "postgresql/mysql/sqlserver remain fail-closed until shared-conn apply is wired.",
        ],
    }
    MATRIX_PATH.parent.mkdir(parents=True, exist_ok=True)
    MATRIX_PATH.write_text(json.dumps(payload, indent=2) + "\n")
    loaded = json.loads(MATRIX_PATH.read_text())
    assert loaded["pass"] == 8
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
