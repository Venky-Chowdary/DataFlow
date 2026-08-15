"""CDC exactly-once — dest-owned watermark transaction (Flink / Estuary).

Honesty
-------
Platform-wide CDC remains **at-least-once upsert** until a route opts in
*and* the dest can commit apply + watermark in one transaction.

This is not Kafka transactional-id EOS and not XA across heterogeneous
sinks. It is the Estuary materialization / Flink two-phase sink pattern:

1. BEGIN dest transaction (SELECT … FOR UPDATE on the watermark row).
2. Drop events with LSN ``<=`` dest-committed watermark (crash after
   dest commit, before source ack).
3. Apply upserts/deletes with ``_df_lsn`` guards on the same connection.
4. UPSERT ``_df_cdc_eos_watermarks`` in the same transaction.
5. COMMIT dest.
6. Persist job watermark and ack the source **after** dest commit.

Crash before dest COMMIT → dest rolls back; source not acked; retry.
Crash after dest COMMIT → dest watermark wins; redelivery is a no-op.

``EXACTLY_ONCE_CLAIMED`` (platform) stays False. Route-scoped
``exactly_once_active`` is the only honest green.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.lsn_guards import DF_LSN_COL, compare_lsn, extract_cdc_lsn
from services.cdc_effectively_once import (
    DELIVERY_CLASS_AT_LEAST_ONCE,
    DELIVERY_CLASS_AT_MOST_ONCE,
    DELIVERY_CLASS_EXACTLY_ONCE,
    classify_sink_delivery,
)

# Platform-wide claim — never flip. Route opt-in is separate.
PLATFORM_EXACTLY_ONCE_CLAIMED = False
ALGORITHM = "dest_owned_watermark_txn"
WATERMARK_TABLE = "_df_cdc_eos_watermarks"
DELIVERY_SEMANTICS_EOS = "exactly_once_dest_owned_watermark_txn"
DELIVERY_SEMANTICS_ALO = "at_least_once_idempotent_apply"

# Destinations that can host a transactional watermark table in principle.
# Aliases stay listed so classify never invents a miss on catalog ids.
EOS_TRANSACTIONAL_DESTS = frozenset({
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
    "oracle",
    "oracle_db",
    "oracle_autonomous_warehouse",
    "duckdb",
    "generic_sql",
    "snowflake",
})

# Destinations whose writer shares one dest transaction (native sqlite or SQLAlchemy).
EOS_TXN_WIRED_DESTS = frozenset({
    "sqlite",
    "postgresql",
    "postgres",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
    "duckdb",
    "generic_sql",
    "oracle",
    "oracle_db",
    "oracle_autonomous_warehouse",
    "snowflake",
})

REASON_NOT_CDC = "exactly_once_requires_cdc_sync"
REASON_APPEND = "exactly_once_refuses_append_only"
REASON_NO_PK = "exactly_once_requires_primary_key"
REASON_DEST_NOT_TXN = "exactly_once_dest_not_transactional"
REASON_DEST_NOT_WIRED = "exactly_once_dest_txn_not_wired"
REASON_NO_LSN = "exactly_once_requires_durable_lsn"
REASON_CALLABLE = "exactly_once_refuses_callable_source"
REASON_AT_MOST = "at_most_once_not_offered"
REASON_OK = "dest_owned_watermark_txn"


class ExactlyOnceRouteError(ValueError):
    """Fail-closed: operator asked for exactly-once on an ineligible route."""

    def __init__(self, message: str, *, reason: str = "") -> None:
        super().__init__(message)
        self.reason = reason or "exactly_once_ineligible"


class EosCrash(RuntimeError):
    """Injected crash for chaos proofs — never used in production paths."""

    def __init__(self, phase: str) -> None:
        super().__init__(f"injected EOS crash at {phase}")
        self.phase = phase


@dataclass(frozen=True)
class EosEligibility:
    eligible: bool
    reason: str
    dest_type: str
    algorithm: str | None
    wired: bool
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "reason": self.reason,
            "dest_type": self.dest_type,
            "algorithm": self.algorithm,
            "wired": self.wired,
            "notes": list(self.notes),
            "platform_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
        }


@dataclass
class EosWatermark:
    stream_key: str
    committed_lsn: str
    batch_id: str
    committed_at: str
    epoch: int = 1


@dataclass
class EosApplyResult:
    status: str  # applied | already_committed | empty
    rows_written: int = 0
    deleted: int = 0
    committed_lsn: str | None = None
    batch_id: str = ""
    epoch: int = 0
    delivery_semantics: str = DELIVERY_SEMANTICS_EOS
    already_committed: bool = False

    def to_dest_summary(self) -> dict[str, Any]:
        return {
            "ok": True,
            "delivery_semantics": self.delivery_semantics,
            "cdc_delivery": "exactly_once",
            "exactly_once_algorithm": ALGORITHM,
            "exactly_once_active": True,
            "exactly_once_claimed_platform": PLATFORM_EXACTLY_ONCE_CLAIMED,
            "eos_status": self.status,
            "eos_already_committed": self.already_committed,
            "eos_committed_lsn": self.committed_lsn,
            "eos_batch_id": self.batch_id,
            "eos_epoch": self.epoch,
            "rows_written": self.rows_written,
        }


def normalize_delivery_guarantee(requested: str | None) -> str:
    raw = (requested or "at_least_once").strip().lower().replace("-", "_")
    if raw in {"eos", "exactlyonce"}:
        return DELIVERY_CLASS_EXACTLY_ONCE
    return raw or "at_least_once"


def eos_stream_key(
    *,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    cursor_key: str = "",
    stream_name: str = "",
) -> str:
    """Stable dest-owned watermark identity. Prefer the job cursor key."""
    if (cursor_key or "").strip():
        return cursor_key.strip()
    parts = [
        (dest_type or "").strip().lower() or "dest",
        (dest_database or "").strip() or "_",
        (dest_object or stream_name or "").strip() or "stream",
    ]
    return "|".join(parts)


def batch_lsn(resume_token: Any) -> str | None:
    """Durable LSN for EOS coordination — fail-closed when missing."""
    return extract_cdc_lsn(resume_token)


def already_committed(incoming_lsn: str | None, dest_lsn: str | None) -> bool:
    """True when dest already committed this LSN or a newer one."""
    if not incoming_lsn or not str(incoming_lsn).strip():
        return False
    if not dest_lsn or not str(dest_lsn).strip():
        return False
    return compare_lsn(incoming_lsn, dest_lsn) <= 0


def classify_exactly_once_route(
    *,
    dest_type: str,
    sync_mode: str = "cdc",
    has_primary_key: bool = False,
    write_mode: str = "upsert",
    allow_append_only: bool = False,
    has_lsn_column: bool | None = True,
    callable_source: bool = False,
    source_type: str = "",
) -> EosEligibility:
    """Fail-closed eligibility. Default CDC path does not call this for ALO."""
    dest = (dest_type or "").strip().lower().replace("-", "_")
    if dest == "postgres":
        dest = "postgresql"
    _SINK_ALIASES = {
        "mariadb": "mysql",
        "mssql": "sqlserver",
        "azure_sql": "sqlserver",
        "azure_sql_database": "sqlserver",
        "amazon_rds_sql_server": "sqlserver",
        "oracle_db": "oracle",
        "oracle_autonomous_warehouse": "oracle",
    }
    sink_dest = _SINK_ALIASES.get(dest, dest)
    mode = (sync_mode or "").strip().lower().replace("-", "_")
    notes: list[str] = [
        "Algorithm: dest-owned watermark in the same dest transaction as apply.",
        "Platform never claims all CDC is exactly-once.",
        "At-most-once is not offered (silent loss).",
    ]
    if callable_source:
        return EosEligibility(
            False, REASON_CALLABLE, dest, None, False, tuple(notes)
        )
    if mode not in {"cdc", "cdc_incremental"}:
        return EosEligibility(
            False, REASON_NOT_CDC, dest, None, False, tuple(notes)
        )
    if allow_append_only:
        return EosEligibility(
            False, REASON_APPEND, dest, None, False, tuple(notes)
        )
    if not has_primary_key:
        return EosEligibility(
            False, REASON_NO_PK, dest, None, False, tuple(notes)
        )
    if dest not in EOS_TRANSACTIONAL_DESTS:
        return EosEligibility(
            False, REASON_DEST_NOT_TXN, dest, None, False, tuple(notes)
        )
    sink = classify_sink_delivery(
        dest_type=sink_dest,
        has_primary_key=has_primary_key,
        write_mode=write_mode or "upsert",
        has_lsn_column=has_lsn_column,
    )
    if sink.get("class") != "effectively_once_eligible":
        return EosEligibility(
            False, REASON_APPEND, dest, None, False, tuple(notes)
        )
    wired = dest in EOS_TXN_WIRED_DESTS
    if not wired:
        return EosEligibility(
            False,
            REASON_DEST_NOT_WIRED,
            dest,
            ALGORITHM,
            False,
            tuple(
                notes
                + [
                    f"Destination '{dest}' can host a transactional watermark "
                    "but the shared-connection apply path is not wired — refuse.",
                ]
            ),
        )
    _ = source_type  # reserved for log-plugin gates; LSN is checked per batch
    return EosEligibility(
        True,
        REASON_OK,
        dest,
        ALGORITHM,
        True,
        tuple(notes),
    )


def assert_requested_cdc_delivery(
    requested: str | None,
    *,
    sync_mode: str = "",
    dest_type: str = "",
    source_type: str = "",
    has_primary_key: bool = False,
    write_mode: str = "upsert",
    allow_append_only: bool = False,
    callable_source: bool = False,
    has_lsn_column: bool | None = True,
) -> str:
    """Normalize delivery. Exactly-once is opt-in and fail-closed on the route."""
    from services.execution_engine_contract import DeliveryGuaranteeError

    raw = normalize_delivery_guarantee(requested)
    if raw == DELIVERY_CLASS_AT_MOST_ONCE:
        raise DeliveryGuaranteeError(
            "at_most_once is not a selectable product guarantee — "
            "silent loss is incompatible with migration assurance."
        )
    if raw not in {DELIVERY_CLASS_AT_LEAST_ONCE, DELIVERY_CLASS_EXACTLY_ONCE}:
        raise DeliveryGuaranteeError(
            f"Unknown delivery guarantee {requested!r} — allowed: "
            f"at_least_once, exactly_once"
        )
    if raw == DELIVERY_CLASS_AT_LEAST_ONCE:
        return raw
    eligibility = classify_exactly_once_route(
        dest_type=dest_type,
        sync_mode=sync_mode,
        has_primary_key=has_primary_key,
        write_mode=write_mode,
        allow_append_only=allow_append_only,
        has_lsn_column=has_lsn_column,
        callable_source=callable_source,
        source_type=source_type,
    )
    if not eligibility.eligible:
        raise ExactlyOnceRouteError(
            "exactly_once is opt-in dest-owned watermark delivery and this "
            f"route is ineligible ({eligibility.reason}). "
            "Default remains at_least_once upsert. "
            + " ".join(eligibility.notes[:2]),
            reason=eligibility.reason,
        )
    return raw


def require_batch_lsn(resume_token: Any) -> str:
    lsn = batch_lsn(resume_token)
    if not lsn:
        raise ExactlyOnceRouteError(
            "exactly_once requires a durable LSN/GTID/SCN/resume token on "
            "every batch — refuse to commit a watermark without a position.",
            reason=REASON_NO_LSN,
        )
    return lsn


@dataclass
class InMemoryEosStore:
    """Crash-injectable dest watermark + PK sink for algorithm proofs."""

    rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    watermarks: dict[str, EosWatermark] = field(default_factory=dict)
    apply_calls: int = 0
    commit_calls: int = 0
    rollback_calls: int = 0

    def read(self, stream_key: str) -> EosWatermark | None:
        return self.watermarks.get(stream_key)

    def commit_atomic(
        self,
        *,
        stream_key: str,
        incoming_lsn: str,
        batch_id: str,
        apply_fn: Callable[[], None],
        crash_after: str | None = None,
    ) -> EosApplyResult:
        """BEGIN → filter → apply → watermark → COMMIT, with optional crash."""
        current = self.watermarks.get(stream_key)
        dest_lsn = current.committed_lsn if current else None
        if already_committed(incoming_lsn, dest_lsn):
            return EosApplyResult(
                status="already_committed",
                committed_lsn=dest_lsn,
                batch_id=current.batch_id if current else batch_id,
                epoch=current.epoch if current else 0,
                already_committed=True,
            )
        staged_rows: dict[str, dict[str, Any]] | None = None
        try:
            staged_rows = {k: dict(v) for k, v in self.rows.items()}
            apply_fn()
            self.apply_calls += 1
            if crash_after == "after_apply_before_watermark":
                raise EosCrash(crash_after)
            prior_epoch = current.epoch if current else 0
            watermark = EosWatermark(
                stream_key=stream_key,
                committed_lsn=incoming_lsn,
                batch_id=batch_id,
                committed_at=datetime.now(timezone.utc).isoformat(),
                epoch=prior_epoch + 1,
            )
            if crash_after == "after_watermark_before_commit":
                raise EosCrash(crash_after)
            self.watermarks[stream_key] = watermark
            self.commit_calls += 1
            if crash_after == "after_commit_before_ack":
                raise EosCrash(crash_after)
            return EosApplyResult(
                status="applied",
                committed_lsn=incoming_lsn,
                batch_id=batch_id,
                epoch=watermark.epoch,
            )
        except EosCrash:
            if crash_after in {
                "after_apply_before_watermark",
                "after_watermark_before_commit",
            }:
                if staged_rows is not None:
                    self.rows = staged_rows
                self.rollback_calls += 1
            raise

    def upsert_row(self, pk: str, row: dict[str, Any]) -> None:
        from services.cdc_effectively_once import should_apply_pk_row

        existing = self.rows.get(pk)
        prior = existing.get(DF_LSN_COL) if existing else None
        decision = should_apply_pk_row(
            existing_lsn=prior, incoming_lsn=row.get(DF_LSN_COL)
        )
        if decision.applied:
            self.rows[pk] = dict(row)


def chaos_crash_before_commit_then_retry(
    *,
    pk: str = "1",
    stream_key: str = "s|db|t",
) -> InMemoryEosStore:
    """Apply once, crash before dest commit, retry — row and watermark land once."""
    store = InMemoryEosStore()

    def apply_v(value: str, lsn: str) -> Callable[[], None]:
        def _fn() -> None:
            store.upsert_row(pk, {"id": pk, "v": value, DF_LSN_COL: lsn})

        return _fn

    try:
        store.commit_atomic(
            stream_key=stream_key,
            incoming_lsn="0/100",
            batch_id="b1",
            apply_fn=apply_v("first", "0/100"),
            crash_after="after_apply_before_watermark",
        )
    except EosCrash:
        pass
    store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn="0/100",
        batch_id="b1-retry",
        apply_fn=apply_v("first", "0/100"),
    )
    return store


def chaos_crash_after_commit_redelivery(
    *,
    pk: str = "1",
    stream_key: str = "s|db|t",
) -> InMemoryEosStore:
    """Dest commit succeeds, source ack fails, redelivery is already_committed."""
    store = InMemoryEosStore()

    def apply_v(value: str, lsn: str) -> Callable[[], None]:
        def _fn() -> None:
            store.upsert_row(pk, {"id": pk, "v": value, DF_LSN_COL: lsn})

        return _fn

    try:
        store.commit_atomic(
            stream_key=stream_key,
            incoming_lsn="0/200",
            batch_id="b2",
            apply_fn=apply_v("new", "0/200"),
            crash_after="after_commit_before_ack",
        )
    except EosCrash:
        pass
    store.commit_atomic(
        stream_key=stream_key,
        incoming_lsn="0/200",
        batch_id="b2-redeliver",
        apply_fn=apply_v("dup", "0/200"),
    )
    return store


def route_has_cdc_pk(stream_contracts: list[Any] | None, primary_key: str = "") -> bool:
    if (primary_key or "").strip():
        return True
    for raw in stream_contracts or []:
        if not isinstance(raw, dict):
            pk = getattr(raw, "primary_key", "") or ""
            selected = getattr(raw, "selected", True)
        else:
            pk = raw.get("primary_key") or ""
            selected = raw.get("selected", True)
        if selected is False:
            continue
        if str(pk).strip():
            return True
    return False


def dest_allow_append_only(destination: Any) -> bool:
    extra = getattr(destination, "extra", None)
    if isinstance(extra, dict) and extra.get("allow_append_only"):
        return True
    cfg = getattr(destination, "config", None)
    if isinstance(cfg, dict) and cfg.get("allow_append_only"):
        return True
    return False


def preflight_delivery_gate(
    *,
    sync_mode: str,
    dest_type: str,
    delivery_guarantee: str | None = None,
    has_primary_key: bool = False,
    allow_append_only: bool = False,
    callable_source: bool = False,
    source_type: str = "",
) -> dict[str, Any] | None:
    """Validate-time EOS gate. Absent when the route is not CDC and did not opt in."""
    requested = normalize_delivery_guarantee(delivery_guarantee)
    mode = (sync_mode or "").strip().lower().replace("-", "_")
    is_cdc = mode in {"cdc", "cdc_incremental"}
    if requested != DELIVERY_CLASS_EXACTLY_ONCE and not is_cdc:
        return None
    if requested != DELIVERY_CLASS_EXACTLY_ONCE:
        return {
            "id": "g16_cdc_delivery",
            "status": "pass",
            "message": (
                "CDC delivery default is at-least-once upsert — "
                "exactly-once is opt-in dest-owned watermark, not platform-wide"
            ),
            "duration_ms": 0,
            "details": {
                "delivery_guarantee": DELIVERY_CLASS_AT_LEAST_ONCE,
                "platform_claimed": PLATFORM_EXACTLY_ONCE_CLAIMED,
                "algorithm": ALGORITHM,
            },
        }
    eligibility = classify_exactly_once_route(
        dest_type=dest_type,
        sync_mode=sync_mode,
        has_primary_key=has_primary_key,
        allow_append_only=allow_append_only,
        callable_source=callable_source,
        source_type=source_type,
    )
    details = eligibility.to_dict()
    details["delivery_guarantee"] = DELIVERY_CLASS_EXACTLY_ONCE
    if eligibility.eligible:
        return {
            "id": "g16_cdc_delivery",
            "status": "pass",
            "message": (
                "exactly_once dest-owned watermark is eligible on this route "
                "(not platform-wide)"
            ),
            "duration_ms": 0,
            "details": details,
        }
    return {
        "id": "g16_cdc_delivery",
        "status": "block",
        "message": (
            f"exactly_once refused ({eligibility.reason}) — "
            "default remains at-least-once upsert"
        ),
        "duration_ms": 0,
        "details": details,
    }
