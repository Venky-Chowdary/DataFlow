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
    WATERMARK_TABLE,
    REASON_APPEND,
    REASON_DEST_NOT_TXN,
    REASON_NOT_CDC,
    REASON_OK,
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
            change=_batch("0/300", inserts=[{"id": "2", "v": "dup"}]),
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
        "platform_exactly_once_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
        "delivery_default": "at_least_once",
        "measured_floor": 1.0,
        "pass": len(results),
        "fail": 0,
        "skip": 0,
        "cases": results,
        "notes": [
            "100% means this named fixture only — not live warehouse CDC.",
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
            change=_batch("0/400", inserts=[{"id": "1", "v": "dup"}]),
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
