"""Transactional dest-offset exactly-once — fail-closed gaps.

Covers the contract the dest-owned watermark (``_df_cdc_eos_watermarks``)
must hold for transactional sinks:

* apply + offset commit atomically; redelivery at/below the offset is a no-op
* crash before commit leaves nothing; crash after commit (before the
  control-plane watermark/ack) does not duplicate on resume
* cross-family LSN comparisons fail closed (never "equal" → silent skip)
* the offset write is compare-and-set, so two writers cannot both commit
* ``require_exactly_once`` fails closed on non-transactional sinks
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.cdc_eos_sa as eos_sa  # noqa: E402
import connectors.cdc_eos_sql as eos_sql  # noqa: E402
from connectors.cdc_eos_sql import (  # noqa: E402
    apply_change_batch_exactly_once,
    dest_engine_count,
    dest_watermark_lsn,
    dest_watermark_view,
)
from services.cdc_effectively_once import (  # noqa: E402
    EXACTLY_ONCE_CLAIMED,
    gate_cdc_destination,
    honesty_dict,
)
from services.cdc_engine import ChangeBatch  # noqa: E402
from services.cdc_exactly_once import (  # noqa: E402
    DELIVERY_CLASS_EXACTLY_ONCE,
    PLATFORM_EXACTLY_ONCE_CLAIMED,
    REASON_CONCURRENT_COMMIT,
    REASON_LSN_FAMILY,
    REASON_NO_PK,
    DestWmView,
    EosCrash,
    ExactlyOnceRouteError,
    apply_require_exactly_once,
    classify_sink_exactly_once,
    decide_eos_apply,
)

MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "v", "target": "v", "confidence": 1.0},
]
TYPES = {"id": "string", "v": "string"}


def _batch(lsn: str, *rows: dict) -> ChangeBatch:
    return ChangeBatch(inserts=list(rows), updates=[], deletes=[], resume_token={"lsn": lsn})


def _apply(dest_type: str, dest_cfg: dict, change: ChangeBatch, *, key: str, table: str = "orders", **kw):
    return apply_change_batch_exactly_once(
        dest_type=dest_type,
        dest_cfg=dest_cfg,
        dest_table=table,
        change=change,
        mappings=MAPPINGS,
        column_types=TYPES,
        headers=["id", "v"],
        pk_target_cols=["id"],
        cursor_key=key,
        **kw,
    )


@pytest.fixture()
def sqlite_cfg():
    with tempfile.TemporaryDirectory() as tmp:
        yield {"database": str(Path(tmp) / "eos_offset.db")}


@pytest.fixture()
def sa_cfg():
    with tempfile.TemporaryDirectory() as tmp:
        yield {"type": "sqlite", "database": str(Path(tmp) / "eos_offset_sa.db")}


def _sqlite_rows(path: str, table: str = "orders") -> list[tuple]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f'SELECT id, v FROM "{table}" ORDER BY id').fetchall()
    finally:
        conn.close()


# --- token comparison -------------------------------------------------------


def test_cross_family_lsn_fails_closed_instead_of_skipping() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        decide_eos_apply(incoming_lsn="0/200", dest_lsn="mysql-bin.000001:100")
    assert exc.value.reason == REASON_LSN_FAMILY
    assert "incomparable" in str(exc.value)


@pytest.mark.parametrize(
    ("incoming", "dest", "expected"),
    [
        ("0/100", "0/200", "already_committed"),  # stale
        ("0/200", "0/200", "already_committed"),  # equal (idle redelivery)
        ("0/300", "0/200", "apply"),  # newer
        ("0/300", None, "apply"),  # first commit
    ],
)
def test_same_family_stale_equal_newer(incoming: str, dest: str | None, expected: str) -> None:
    action, _fence = decide_eos_apply(incoming_lsn=incoming, dest_lsn=dest)
    assert action == expected


def test_sqlite_cross_family_redelivery_never_silently_drops(sqlite_cfg) -> None:
    key = "sqlite|xf|orders"
    _apply("sqlite", sqlite_cfg, _batch("mysql-bin.000001:100", {"id": "1", "v": "a"}), key=key)
    with pytest.raises(ExactlyOnceRouteError) as exc:
        _apply("sqlite", sqlite_cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    assert exc.value.reason == REASON_LSN_FAMILY
    assert dest_engine_count(sqlite_cfg, "orders") == 1
    assert dest_watermark_lsn(sqlite_cfg, key) == "mysql-bin.000001:100"


# --- atomic apply + offset, idempotent redelivery ---------------------------


@pytest.mark.parametrize("dest_type", ["sqlite", "generic_sql"])
def test_atomic_apply_offset_and_redelivery_is_noop(dest_type, sqlite_cfg, sa_cfg, caplog) -> None:
    cfg = sqlite_cfg if dest_type == "sqlite" else sa_cfg
    key = f"{dest_type}|atomic|orders"
    caplog.set_level(logging.DEBUG)
    rows, _ck, summary, _d = _apply(
        dest_type, cfg, _batch("0/200", {"id": "1", "v": "a"}, {"id": "2", "v": "b"}), key=key
    )
    assert rows == 2
    assert summary["exactly_once_active"] is True
    assert any("cdc_eos: committed" in r.message and r.levelno == logging.INFO for r in caplog.records)
    caplog.clear()
    for lsn in ("0/200", "0/150"):  # equal then stale redelivery
        rows2, _ck2, summary2, _d2 = _apply(
            dest_type, cfg, _batch(lsn, {"id": "1", "v": "a"}, {"id": "2", "v": "b"}), key=key
        )
        assert rows2 == 0
        assert summary2["eos_already_committed"] is True
    assert any("cdc_eos: skip" in r.message and r.levelno == logging.DEBUG for r in caplog.records)
    assert _sqlite_rows(cfg["database"]) == [("1", "a"), ("2", "b")]
    assert dest_watermark_view(cfg, key).committed_lsn == "0/200"
    assert dest_watermark_view(cfg, key).apply_seq == 1


@pytest.mark.parametrize("dest_type", ["sqlite", "generic_sql"])
def test_crash_before_commit_leaves_no_partial_apply(dest_type, sqlite_cfg, sa_cfg) -> None:
    cfg = sqlite_cfg if dest_type == "sqlite" else sa_cfg
    key = f"{dest_type}|crash|orders"
    _apply(dest_type, cfg, _batch("0/100", {"id": "1", "v": "a"}), key=key)
    for point in ("after_apply_before_watermark", "after_watermark_before_commit"):
        with pytest.raises(EosCrash):
            _apply(dest_type, cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key, crash_after=point)
        assert _sqlite_rows(cfg["database"]) == [("1", "a")]
        assert dest_watermark_view(cfg, key).committed_lsn == "0/100"
    rows, *_ = _apply(dest_type, cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    assert rows == 1
    assert _sqlite_rows(cfg["database"]) == [("1", "a"), ("2", "b")]


def test_crash_after_commit_before_control_plane_watermark_no_duplicate(sa_cfg) -> None:
    key = "sa|after-commit|orders"
    with pytest.raises(EosCrash):
        _apply(
            "generic_sql",
            sa_cfg,
            _batch("0/200", {"id": "1", "v": "a"}),
            key=key,
            crash_after="after_commit_before_ack",
        )
    # Dest committed; the runner never advanced its cursor or acked the source.
    assert dest_watermark_view(sa_cfg, key).committed_lsn == "0/200"
    rows, _ck, summary, _d = _apply("generic_sql", sa_cfg, _batch("0/200", {"id": "1", "v": "a"}), key=key)
    assert rows == 0
    assert summary["eos_already_committed"] is True
    assert _sqlite_rows(sa_cfg["database"]) == [("1", "a")]


# --- concurrency: compare-and-set offset write ------------------------------


def test_sqlite_concurrent_first_commit_cannot_double_commit(sqlite_cfg, monkeypatch) -> None:
    key = "sqlite|race|orders"
    _apply("sqlite", sqlite_cfg, _batch("0/100", {"id": "1", "v": "a"}), key=key)
    # Second writer read "no offset yet" before the first committed.
    monkeypatch.setattr(eos_sql, "_read_watermark", lambda cur, k: DestWmView())
    with pytest.raises(ExactlyOnceRouteError) as exc:
        _apply("sqlite", sqlite_cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    assert exc.value.reason == REASON_CONCURRENT_COMMIT
    monkeypatch.undo()
    assert _sqlite_rows(sqlite_cfg["database"]) == [("1", "a")]
    assert dest_watermark_lsn(sqlite_cfg, key) == "0/100"


def test_sqlite_lost_update_on_offset_is_refused(sqlite_cfg, monkeypatch) -> None:
    key = "sqlite|cas|orders"
    _apply("sqlite", sqlite_cfg, _batch("0/100", {"id": "1", "v": "a"}), key=key)
    stale = dest_watermark_view(sqlite_cfg, key)
    _apply("sqlite", sqlite_cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    # A slower writer still holds the epoch-1 view and tries to commit 0/300.
    monkeypatch.setattr(eos_sql, "_read_watermark", lambda cur, k: stale)
    with pytest.raises(ExactlyOnceRouteError) as exc:
        _apply("sqlite", sqlite_cfg, _batch("0/300", {"id": "3", "v": "c"}), key=key)
    assert exc.value.reason == REASON_CONCURRENT_COMMIT
    monkeypatch.undo()
    assert _sqlite_rows(sqlite_cfg["database"]) == [("1", "a"), ("2", "b")]
    assert dest_watermark_lsn(sqlite_cfg, key) == "0/200"


def test_sqlalchemy_lost_update_on_offset_is_refused(sa_cfg, monkeypatch) -> None:
    key = "sa|cas|orders"
    _apply("generic_sql", sa_cfg, _batch("0/100", {"id": "1", "v": "a"}), key=key)
    stale = eos_sa.sa_dest_watermark_view(sa_cfg, key, "generic_sql")
    assert stale.exists is True
    _apply("generic_sql", sa_cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    monkeypatch.setattr(eos_sa, "_lock_watermark", lambda conn, d, k: stale)
    with pytest.raises(ExactlyOnceRouteError) as exc:
        _apply("generic_sql", sa_cfg, _batch("0/300", {"id": "3", "v": "c"}), key=key)
    assert exc.value.reason == REASON_CONCURRENT_COMMIT
    monkeypatch.undo()
    assert _sqlite_rows(sa_cfg["database"]) == [("1", "a"), ("2", "b")]
    assert eos_sa.sa_dest_watermark_lsn(sa_cfg, key, "generic_sql") == "0/200"


def test_sqlalchemy_concurrent_first_commit_cannot_double_commit(sa_cfg, monkeypatch) -> None:
    key = "sa|race|orders"
    _apply("generic_sql", sa_cfg, _batch("0/100", {"id": "1", "v": "a"}), key=key)
    monkeypatch.setattr(eos_sa, "_lock_watermark", lambda conn, d, k: DestWmView())
    with pytest.raises(ExactlyOnceRouteError) as exc:
        _apply("generic_sql", sa_cfg, _batch("0/200", {"id": "2", "v": "b"}), key=key)
    assert exc.value.reason == REASON_CONCURRENT_COMMIT
    monkeypatch.undo()
    assert _sqlite_rows(sa_cfg["database"]) == [("1", "a")]


def test_unreadable_dest_offset_fails_closed_not_none(monkeypatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("dest unreachable")

    monkeypatch.setattr(eos_sa, "sa_dest_watermark_lsn", boom)
    with pytest.raises(RuntimeError):
        eos_sql.read_route_dest_lsn("postgresql", {"host": "x"}, "pg|k")


# --- per-sink classification and the require_exactly_once gate -------------


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "sqlserver", "oracle", "snowflake", "sqlite"])
def test_transactional_sinks_classify_exactly_once(dest: str) -> None:
    posture = classify_sink_exactly_once(dest_type=dest, has_primary_key=True)
    assert posture["delivery_class"] == DELIVERY_CLASS_EXACTLY_ONCE
    assert posture["exactly_once"] is True
    assert posture["exactly_once_scope"] == "destination"
    assert posture["platform_exactly_once_claimed"] is False
    gated = gate_cdc_destination(
        dest_type=dest, has_primary_key=True, has_lsn_column=True, require_exactly_once=True
    )
    assert gated["delivery_class"] == DELIVERY_CLASS_EXACTLY_ONCE


@pytest.mark.parametrize(
    "dest", ["clickhouse", "athena", "hive", "impala", "s3", "gcs", "csv", "parquet", "kafka", "bigquery"]
)
def test_non_transactional_sinks_stay_at_least_once_and_gate_fails_closed(dest: str) -> None:
    posture = classify_sink_exactly_once(dest_type=dest, has_primary_key=True)
    assert posture["exactly_once"] is False
    assert posture["delivery_class"] == "at_least_once"
    assert posture["exactly_once_refusal"]
    with pytest.raises(ExactlyOnceRouteError) as exc:
        gate_cdc_destination(
            dest_type=dest,
            has_primary_key=True,
            allow_append_only=True,
            require_exactly_once=True,
        )
    msg = str(exc.value)
    assert f"'{dest}'" in msg and "require_exactly_once=true" in msg and "at-least-once" in msg


def test_gate_requires_primary_key_for_exactly_once() -> None:
    with pytest.raises(ExactlyOnceRouteError) as exc:
        gate_cdc_destination(dest_type="postgresql", has_primary_key=False, require_exactly_once=True)
    assert exc.value.reason == REASON_NO_PK


def test_gate_refuses_append_only_even_on_transactional_engine() -> None:
    with pytest.raises(ExactlyOnceRouteError):
        gate_cdc_destination(
            dest_type="postgresql",
            has_primary_key=True,
            write_mode="append",
            allow_append_only=True,
            require_exactly_once=True,
        )


def test_runner_gate_reads_require_exactly_once_from_dest_config() -> None:
    from src.transfer.cdc_transfer import _gate_cdc_sink

    with pytest.raises(ExactlyOnceRouteError):
        _gate_cdc_sink(
            dest_type="clickhouse",
            dest_cfg={"require_exactly_once": "true", "allow_append_only": True},
            has_primary_key=True,
        )
    posture = _gate_cdc_sink(
        dest_type="postgresql", dest_cfg={"require_exactly_once": True}, has_primary_key=True
    )
    assert posture["delivery_class"] == DELIVERY_CLASS_EXACTLY_ONCE


def test_require_exactly_once_promotes_delivery_and_rejects_conflicting_pin() -> None:
    assert apply_require_exactly_once("at_least_once", required=True) == "exactly_once"
    assert apply_require_exactly_once("auto", required=True) == "exactly_once"
    assert apply_require_exactly_once("at_least_once", required=False) == "at_least_once"
    with pytest.raises(ExactlyOnceRouteError):
        apply_require_exactly_once("at_least_once", required=True, pinned=True)


def test_honesty_is_per_destination_never_platform_wide() -> None:
    blob = honesty_dict()
    assert EXACTLY_ONCE_CLAIMED is False
    assert PLATFORM_EXACTLY_ONCE_CLAIMED is False
    assert blob["exactly_once_claimed"] is False
    assert blob["delivery_default"] == "at-least-once"
    assert blob["exactly_once_scope"] == "per_destination_transactional_sinks_only"
    assert "postgresql" in blob["exactly_once_transactional_sinks"]
    for never in ("clickhouse", "athena", "hive", "impala", "s3"):
        assert never in blob["exactly_once_never_for"]
        assert never not in blob["exactly_once_transactional_sinks"]
