"""CDC exactly-once — dest-owned watermark transaction (Flink / Estuary).

Honesty
-------
Platform-wide CDC remains **at-least-once upsert** until a route opts in
*and* the dest can commit apply + watermark in one transaction.

This is not Kafka transactional-id EOS and not XA across heterogeneous
sinks. It is dest-authoritative fenced materialization — Estuary Open
fence + remote-store-authoritative checkpoint, plus Flink idempotent
commit-on-restore, plus Debezium-class snapshot/stream LSN compare:

1. Open: dest watermark is SSOT. Job cursor ahead of dest is rewound
   (honoring job would skip uncommitted LSNs — silent loss). Job behind
   dest fast-forwards (redelivery is already_committed).
2. BEGIN dest transaction (SELECT … FOR UPDATE on the watermark row).
3. Fence: incoming lease generation must be ``>=`` dest ``fence_epoch``.
   A stolen-lease zombie cannot commit (Estuary Open fence, persisted
   in the apply transaction so the lease check cannot race).
4. Reduce: last op per PK in the batch wins (Estuary load-reduce-store).
5. Drop events with LSN ``<=`` dest-committed watermark.
6. Apply upserts/deletes with ``_df_lsn`` guards on the same connection.
7. UPSERT ``_df_cdc_eos_watermarks`` (LSN + fence + epoch + phase +
   apply checksum) in the same txn.
8. Shared-log multi-stream: N tables + one LSN in **one** dest BEGIN/COMMIT
   (Estuary multi-binding). Crash before COMMIT rolls back every stream;
   source is acked once after dest commit.
9. Snapshot→stream handoff is dest-owned: same LSN, dest phase=snapshot,
   incoming=streaming → phase-only update, no double-write.
10. Redelivery checksum: same LSN + different payload is refused
    (not a silent overwrite). Handoff skips this check. Conflict rows
    are quarantined — never dropped, never overwritten.
11. Open (Estuary): raise dest fence in a dest txn with **no data apply**,
    and return the dest-stored resume blob. Job store wipe cannot invent
    a cursor ahead of dest.
12. Dest-owned DDD-3 stream-wins: incremental snapshot READ cannot
    overwrite a dest row whose ``_df_lsn`` / watermark is already
    streaming at the same or newer LSN. Debezium does this in an
    in-memory Kafka buffer (lost on crash).
13. Bundle coordinator LSN cannot advance past a member that did not
    reach it (Estuary multi-binding checkpoint is all-or-nothing).
14. Dest load-reduce-store: SELECT dest rows for batch PKs **in the apply
    txn** (Estuary Load), merge incoming into dest documents (partial CDC
    updates keep dest columns), then Store. Estuary Load can be a separate
    pipelined phase; ours cannot race dest between Load and Store.
15. apply_seq is dest-monotonic (Iceberg-style sequence). Never write a
    lower seq than dest already committed.
16. Post-commit dest verify: re-read dest watermark after COMMIT. If dest
    does not show the LSN/seq/fence, refuse success so the source is not
    acked (stronger than Flink heuristic recoverAndCommit).
17. Dest-owned incremental-snapshot ``window_id``: a closed window is
    persisted on dest. Crash re-emit of the same Debezium DDD-3 window
    is ``window_closed_skip`` — Debezium's buffer is in-memory and lost.
18. Quarantine replay is dest-LSN gated. Dest already at or past the
    quarantined LSN refuses overwrite (checksum conflict is inspect-only).
19. COMMIT dest. Persist job watermark and ack the source **after** verify.

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
REASON_STALE_FENCE = "exactly_once_stale_writer_fence"
REASON_CHECKSUM = "exactly_once_checksum_mismatch"
REASON_BUNDLE_LSN = "exactly_once_bundle_lsn_divergence"
REASON_UNVERIFIED = "exactly_once_dest_commit_unverified"
REASON_STALE_SEQ = "exactly_once_stale_apply_seq"
REASON_STALE_REPLAY = "exactly_once_quarantine_replay_stale_dest"
REASON_OK = "dest_owned_watermark_txn"
PROTOCOL = "dest_authoritative_windowed_bundle"


class ExactlyOnceRouteError(ValueError):
    """Fail-closed: operator asked for exactly-once on an ineligible route."""

    def __init__(
        self,
        message: str,
        *,
        reason: str = "",
        quarantine: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason or "exactly_once_ineligible"
        self.quarantine = list(quarantine or [])


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
            "protocol": PROTOCOL,
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
    fence_epoch: int = 0
    prev_lsn: str | None = None
    phase: str = "streaming"


@dataclass
class EosApplyResult:
    status: str  # applied | already_committed | empty | handoff_phase
    rows_written: int = 0
    deleted: int = 0
    committed_lsn: str | None = None
    batch_id: str = ""
    epoch: int = 0
    delivery_semantics: str = DELIVERY_SEMANTICS_EOS
    already_committed: bool = False
    fence_epoch: int = 0
    dest_authoritative: bool = True
    protocol: str = PROTOCOL
    phase: str = "streaming"
    apply_checksum: str = ""
    apply_seq: int = 0
    window_id: str = ""

    def to_dest_summary(self) -> dict[str, Any]:
        return {
            "ok": True,
            "delivery_semantics": self.delivery_semantics,
            "cdc_delivery": "exactly_once",
            "exactly_once_algorithm": ALGORITHM,
            "exactly_once_protocol": self.protocol,
            "exactly_once_active": True,
            "exactly_once_claimed_platform": PLATFORM_EXACTLY_ONCE_CLAIMED,
            "eos_status": self.status,
            "eos_already_committed": self.already_committed,
            "eos_committed_lsn": self.committed_lsn,
            "eos_batch_id": self.batch_id,
            "eos_epoch": self.epoch,
            "eos_fence_epoch": self.fence_epoch,
            "eos_dest_authoritative": self.dest_authoritative,
            "eos_phase": self.phase,
            "eos_apply_checksum": self.apply_checksum,
            "eos_apply_seq": self.apply_seq,
            "eos_window_id": self.window_id,
            "rows_written": self.rows_written,
        }


@dataclass
class EosBundleResult:
    """N streams committed in one dest transaction (shared-log bundle)."""

    members: list[EosApplyResult] = field(default_factory=list)
    committed_lsn: str | None = None
    already_committed: bool = False
    rows_written: int = 0
    deleted: int = 0
    fence_epoch: int = 0
    protocol: str = PROTOCOL
    dest_authoritative: bool = True
    bundle_key: str = ""

    def to_dest_summary(self) -> dict[str, Any]:
        first = self.members[0] if self.members else None
        blob = (first.to_dest_summary() if first else EosApplyResult(status="empty").to_dest_summary())
        blob.update(
            {
                "eos_bundle": True,
                "eos_bundle_streams": len(self.members),
                "eos_committed_lsn": self.committed_lsn,
                "eos_already_committed": self.already_committed,
                "eos_fence_epoch": self.fence_epoch,
                "eos_dest_authoritative": self.dest_authoritative,
                "exactly_once_protocol": self.protocol,
                "rows_written": self.rows_written,
                "eos_bundle_key": self.bundle_key,
            }
        )
        return blob


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


def assert_writer_fence(incoming_fence: int, dest_fence: int) -> None:
    """Estuary Open fence — a stolen-lease zombie cannot commit dest EOS.

    Fence 0 means unleased / single-writer tests. A positive dest fence
    refuses a lower incoming generation.
    """
    incoming = int(incoming_fence or 0)
    dest = int(dest_fence or 0)
    if dest > 0 and incoming < dest:
        raise ExactlyOnceRouteError(
            "exactly_once dest fence refused a stale writer "
            f"(incoming={incoming} dest={dest}). "
            "Zombie after lease steal cannot commit.",
            reason=REASON_STALE_FENCE,
        )


def next_dest_fence(incoming_fence: int, dest_fence: int) -> int:
    return max(int(incoming_fence or 0), int(dest_fence or 0))


def encode_resume_blob(resume_token: Any) -> str:
    import json

    if resume_token is None:
        return ""
    if isinstance(resume_token, (dict, list)):
        return json.dumps(resume_token, sort_keys=True, default=str)
    return str(resume_token)


def decode_resume_blob(blob: str | None) -> Any:
    import json

    raw = (blob or "").strip()
    if not raw:
        return None
    if raw.startswith("{") or raw.startswith("["):
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return raw


def clamp_job_resume_to_dest(
    job_resume: Any,
    dest_lsn: str | None,
    dest_resume: Any = None,
) -> tuple[Any, dict[str, Any]]:
    """Dest watermark is SSOT (Estuary remote-store-authoritative).

    Job cursor ahead of dest would skip uncommitted LSNs — rewind.
    Job cursor behind dest is already committed — fast-forward.
    When dest stored a resume blob (Estuary Opened checkpoint), that blob
    wins over a job-only LSN rewrite so slot/GTID/file:pos is not invented.
    """
    dest_blob = decode_resume_blob(dest_resume) if isinstance(dest_resume, str) else dest_resume
    proof: dict[str, Any] = {
        "dest_authoritative": True,
        "protocol": PROTOCOL,
        "clamped": False,
        "reason": "no_dest_watermark",
        "dest_lsn": dest_lsn,
        "job_lsn": extract_cdc_lsn(job_resume),
        "dest_resume_blob": bool(dest_blob),
    }
    if not dest_lsn or not str(dest_lsn).strip():
        return job_resume, proof
    job_lsn = proof["job_lsn"]
    if dest_blob is not None:
        blob_lsn = extract_cdc_lsn(dest_blob)
        if blob_lsn and compare_lsn(blob_lsn, dest_lsn) == 0:
            if not job_lsn or compare_lsn(job_lsn, dest_lsn) != 0:
                proof["clamped"] = True
                proof["reason"] = (
                    "job_resume_missing_dest_blob"
                    if not job_lsn
                    else (
                        "job_ahead_rewound_to_dest_blob"
                        if compare_lsn(job_lsn, dest_lsn) > 0
                        else "job_behind_fast_forward_to_dest_blob"
                    )
                )
                return dest_blob, proof
            proof["reason"] = "dest_resume_blob_authoritative"
            return dest_blob, proof
    if not job_lsn:
        proof["clamped"] = True
        proof["reason"] = "job_resume_missing"
        return _resume_with_lsn(job_resume, dest_lsn), proof
    cmp = compare_lsn(job_lsn, dest_lsn)
    if cmp == 0:
        proof["reason"] = "aligned"
        return job_resume, proof
    if cmp > 0:
        proof["clamped"] = True
        proof["reason"] = "job_ahead_rewound_to_dest"
        return _resume_with_lsn(job_resume, dest_lsn), proof
    proof["clamped"] = True
    proof["reason"] = "job_behind_fast_forward_to_dest"
    return _resume_with_lsn(job_resume, dest_lsn), proof


def _resume_with_lsn(job_resume: Any, dest_lsn: str) -> Any:
    if isinstance(job_resume, dict):
        out = dict(job_resume)
        out["lsn"] = dest_lsn
        return out
    return dest_lsn


@dataclass
class EosOpenResult:
    """Estuary Open: dest fence raised, dest resume returned, no data apply."""

    resume: Any
    dest_lsn: str | None
    fence_epoch: int
    apply_seq: int = 0
    opened: bool = True
    dest_authoritative: bool = True
    protocol: str = PROTOCOL
    fence_raised: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "opened": self.opened,
            "dest_lsn": self.dest_lsn,
            "fence_epoch": self.fence_epoch,
            "apply_seq": self.apply_seq,
            "fence_raised": self.fence_raised,
            "dest_authoritative": self.dest_authoritative,
            "exactly_once_protocol": self.protocol,
        }


def plan_open_session(
    *,
    dest: DestWmView,
    incoming_fence: int,
    job_resume: Any,
) -> EosOpenResult:
    """Decide Open fence + dest-authoritative resume (no data)."""
    assert_writer_fence(incoming_fence, dest.fence_epoch)
    fence = next_dest_fence(incoming_fence, dest.fence_epoch)
    resume, _proof = clamp_job_resume_to_dest(
        job_resume, dest.committed_lsn, dest.resume_blob or None
    )
    return EosOpenResult(
        resume=resume,
        dest_lsn=dest.committed_lsn,
        fence_epoch=fence,
        apply_seq=dest.apply_seq,
        fence_raised=fence > int(dest.fence_epoch or 0),
    )


def combine_change_batch(change: Any, *, pk_cols: list[str]) -> Any:
    """Estuary load-reduce-store: last op per PK in this batch wins.

    Unkeyed rows are kept (never silently dropped). Deletes after upserts
    on the same PK tombstone the row for this LSN.
    """
    from services.cdc_engine import ChangeBatch
    from services.cdc_snapshot_window import _pk_value

    if not pk_cols:
        return change
    last: dict[str, tuple[str, dict[str, Any] | None]] = {}
    orphans: list[dict[str, Any]] = []
    for rec in list(getattr(change, "inserts", None) or []) + list(
        getattr(change, "updates", None) or []
    ):
        if not isinstance(rec, dict):
            continue
        key = _pk_value(rec, pk_cols)
        if not key:
            orphans.append(rec)
            continue
        last[str(key)] = ("u", rec)
    for raw_key in list(getattr(change, "deletes", None) or []):
        last[str(raw_key)] = ("d", None)
    updates: list[dict[str, Any]] = []
    deletes: list[str] = []
    for key, (op, rec) in last.items():
        if op == "d":
            deletes.append(key)
        elif rec is not None:
            updates.append(rec)
    return ChangeBatch(
        inserts=orphans,
        updates=updates,
        deletes=deletes,
        unchanged=int(getattr(change, "unchanged", 0) or 0),
        resume_token=getattr(change, "resume_token", None),
        table=getattr(change, "table", None),
        ack_barrier=bool(getattr(change, "ack_barrier", True)),
    )


def next_apply_seq(dest_seq: int) -> int:
    return int(dest_seq or 0) + 1


def assert_apply_seq_monotonic(incoming_seq: int, dest_seq: int) -> None:
    """Iceberg-style dest sequence — never commit a lower apply_seq."""
    incoming = int(incoming_seq or 0)
    dest = int(dest_seq or 0)
    if dest > 0 and incoming <= dest:
        raise ExactlyOnceRouteError(
            "exactly_once dest apply_seq refused a stale writer "
            f"(incoming={incoming} dest={dest}).",
            reason=REASON_STALE_SEQ,
        )


def planned_apply_seq(dest_seq: int) -> int:
    """Next dest apply_seq, fail-closed if it would regress."""
    seq = next_apply_seq(dest_seq)
    assert_apply_seq_monotonic(seq, dest_seq)
    return seq


def incoming_pk_keys(rows: list[dict[str, Any]], pk_cols: list[str]) -> list[str]:
    """PK identities for Estuary Load — unkeyed rows are omitted here, kept in reduce."""
    from services.cdc_snapshot_window import _pk_value

    keys: list[str] = []
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        key = _pk_value(rec, pk_cols) if pk_cols else None
        if key:
            keys.append(str(key))
    return keys


def load_reduce_into_dest(
    *,
    incoming_rows: list[dict[str, Any]],
    dest_rows: dict[str, dict[str, Any]],
    pk_cols: list[str],
    incoming_lsn: str,
) -> list[dict[str, Any]]:
    """Estuary Load + Reduce, in the dest apply transaction.

    Incoming keys overwrite dest columns. Dest columns absent from the CDC
    event are kept (partial update). A dest row whose LSN is already
    ``>=`` incoming is omitted — never regress dest.
    Unkeyed rows are kept (never silently dropped).
    """
    from services.cdc_effectively_once import should_apply_pk_row
    from services.cdc_snapshot_window import _pk_value

    out: list[dict[str, Any]] = []
    for rec in incoming_rows:
        if not isinstance(rec, dict):
            continue
        key = _pk_value(rec, pk_cols) if pk_cols else None
        if not key:
            stamped = dict(rec)
            stamped[DF_LSN_COL] = incoming_lsn
            out.append(stamped)
            continue
        dest = dest_rows.get(str(key))
        if dest:
            if not should_apply_pk_row(
                existing_lsn=dest.get(DF_LSN_COL), incoming_lsn=incoming_lsn
            ).applied:
                continue
            merged = dict(dest)
            merged.update(rec)
            merged[DF_LSN_COL] = incoming_lsn
            out.append(merged)
        else:
            stamped = dict(rec)
            stamped[DF_LSN_COL] = incoming_lsn
            out.append(stamped)
    return out


def verify_dest_commit(
    *,
    dest: DestWmView,
    expected_lsn: str,
    expected_fence: int = 0,
    expected_seq: int = 0,
) -> None:
    """Re-read dest after COMMIT. Refuse success if dest did not persist.

    Flink recoverAndCommit is heuristic — if it never succeeds, that is
    data loss. We do not ack the source unless dest shows the LSN.
    """
    if not expected_lsn:
        return
    if not dest.committed_lsn or compare_lsn(dest.committed_lsn, expected_lsn) < 0:
        raise ExactlyOnceRouteError(
            "exactly_once dest commit unverified — watermark LSN "
            f"{dest.committed_lsn!r} is behind expected {expected_lsn!r}. "
            "Refuse source ack.",
            reason=REASON_UNVERIFIED,
        )
    if expected_fence and int(dest.fence_epoch or 0) < int(expected_fence):
        raise ExactlyOnceRouteError(
            "exactly_once dest commit unverified — fence "
            f"{dest.fence_epoch} is behind expected {expected_fence}. "
            "Refuse source ack.",
            reason=REASON_UNVERIFIED,
        )
    if expected_seq and int(dest.apply_seq or 0) < int(expected_seq):
        raise ExactlyOnceRouteError(
            "exactly_once dest commit unverified — apply_seq "
            f"{dest.apply_seq} is behind expected {expected_seq}. "
            "Refuse source ack.",
            reason=REASON_UNVERIFIED,
        )


def dest_view_from_job_summary(job: dict[str, Any] | None) -> DestWmView:
    """Dest watermark fields from a job summary — never invent a dest LSN."""
    summary = job.get("destination_summary") if isinstance(job, dict) else {}
    if not isinstance(summary, dict):
        summary = {}
    lsn = str(summary.get("eos_committed_lsn") or "").strip()
    return DestWmView(
        committed_lsn=lsn or None,
        apply_checksum=str(summary.get("eos_apply_checksum") or ""),
        apply_seq=int(summary.get("eos_apply_seq") or 0),
        window_id=str(summary.get("eos_window_id") or ""),
        fence_epoch=int(summary.get("eos_fence_epoch") or 0),
        phase=str(summary.get("eos_phase") or ""),
    )


def quarantined_lsn_from_row(row: dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    raw = str(row.get("lsn") or "").strip()
    if raw:
        return raw
    rec = row.get("source_record") or row.get("original_value") or {}
    if isinstance(rec, dict):
        rec_lsn = extract_cdc_lsn(rec) or str(rec.get("lsn") or "").strip()
        if rec_lsn:
            return rec_lsn
    err = str(row.get("error") or "")
    if err.startswith("LSN "):
        parts = err.split()
        if len(parts) >= 2:
            return parts[1]
    return ""


def is_cdc_eos_checksum_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    reason = str(row.get("failure_reason") or row.get("reason") or "")
    stage = str(row.get("stage") or "")
    return reason == REASON_CHECKSUM or stage == "cdc_exactly_once"


def assert_quarantine_replay_allowed(
    *,
    dest: DestWmView,
    quarantined_lsn: str,
) -> None:
    """Replay only when dest has not committed this LSN or a newer one.

    Dest equal or ahead → refuse overwrite (checksum conflict is inspect-only).
    Dest behind or empty → dest never committed; replay may land.
    """
    if not quarantined_lsn:
        raise ExactlyOnceRouteError(
            "exactly_once quarantine replay refused — row has no LSN. "
            "Refuse inventing dest overwrite.",
            reason=REASON_STALE_REPLAY,
        )
    dest_lsn = dest.committed_lsn
    if not dest_lsn:
        return
    if compare_lsn(dest_lsn, quarantined_lsn) >= 0:
        raise ExactlyOnceRouteError(
            "exactly_once quarantine replay refused — dest LSN "
            f"{dest_lsn} is at or past quarantined {quarantined_lsn}. "
            "Inspect dest; do not overwrite.",
            reason=REASON_STALE_REPLAY,
        )


def assert_cdc_eos_quarantine_replay(
    *,
    details: list[dict[str, Any]],
    dest: DestWmView,
) -> None:
    """Fail-closed gate for CDC EOS checksum quarantine replay."""
    rows = [d for d in details if is_cdc_eos_checksum_row(d)]
    if not rows:
        return
    if not dest.committed_lsn:
        raise ExactlyOnceRouteError(
            "exactly_once quarantine replay refused — dest LSN is unknown. "
            "Refuse inventing overwrite of a dest-owned watermark.",
            reason=REASON_STALE_REPLAY,
        )
    for row in rows:
        assert_quarantine_replay_allowed(
            dest=dest,
            quarantined_lsn=quarantined_lsn_from_row(row),
        )


def extract_cdc_phase(resume_token: Any) -> str:
    """Snapshot vs streaming phase — Debezium handoff token, dest-owned."""
    if isinstance(resume_token, dict):
        nested = resume_token.get("token")
        if isinstance(nested, (dict, str)) and nested:
            phase = extract_cdc_phase(nested)
            if phase:
                return phase
        if resume_token.get("incremental_snapshot") or resume_token.get("snapshot_window"):
            return "snapshot"
        raw = str(resume_token.get("phase") or "").strip().lower()
        if raw in {"snapshot", "streaming"}:
            return raw
        return "streaming"
    text = str(resume_token or "").lower()
    if "incremental_snapshot" in text or "phase=snapshot" in text:
        return "snapshot"
    if "phase=streaming" in text:
        return "streaming"
    return "streaming"


def is_incremental_snapshot_token(resume_token: Any) -> bool:
    if isinstance(resume_token, dict):
        if resume_token.get("incremental_snapshot") or resume_token.get("snapshot_window"):
            return True
        nested = resume_token.get("token")
        if isinstance(nested, (dict, str)) and nested:
            return is_incremental_snapshot_token(nested)
        return False
    return "incremental_snapshot" in str(resume_token or "").lower()


def extract_snapshot_window_id(resume_token: Any) -> str:
    """Debezium DDD-3 window id from the incremental-snapshot resume token."""
    if isinstance(resume_token, dict):
        nested = resume_token.get("snapshot_window")
        if isinstance(nested, dict) and nested.get("window_id"):
            return str(nested.get("window_id") or "").strip()
        if resume_token.get("window_id"):
            return str(resume_token.get("window_id") or "").strip()
        token = resume_token.get("token")
        if isinstance(token, (dict, str)) and token:
            return extract_snapshot_window_id(token)
        return ""
    text = str(resume_token or "")
    if "window_id=" in text.lower():
        for part in text.replace("|", " ").split():
            if part.lower().startswith("window_id="):
                return part.split("=", 1)[-1].strip()
    return ""


def dest_owned_window_closed(
    *,
    incoming_window_id: str,
    dest_window_id: str,
) -> bool:
    """True when dest already closed this incremental-snapshot window.

    Debezium stream-wins lives in an in-memory buffer (lost on crash;
    the same window_id is re-emitted). Dest-owned window_id makes
    redelivery a no-op after dest COMMIT.
    """
    incoming = (incoming_window_id or "").strip()
    dest = (dest_window_id or "").strip()
    return bool(incoming and dest and incoming == dest)


def next_dest_window_id(incoming_window_id: str, dest_window_id: str) -> str:
    return (incoming_window_id or "").strip() or (dest_window_id or "").strip()


def dest_owned_stream_wins(
    *,
    incoming_phase: str,
    dest_phase: str,
    incoming_lsn: str,
    dest_lsn: str | None,
    incremental_snapshot: bool = False,
) -> bool:
    """Dest-owned DDD-3: snapshot READ cannot overwrite streaming dest.

    Debezium stream-wins lives in an in-memory window before Kafka
    (at-least-once; crash re-emits the READ). We decide on the dest
    watermark so a committed stream row cannot be clobbered.
    """
    incoming_snap = (incoming_phase or "") == "snapshot" or incremental_snapshot
    if not incoming_snap:
        return False
    if (dest_phase or "").strip().lower() != "streaming":
        return False
    return already_committed(incoming_lsn, dest_lsn)


def assert_bundle_members_reached(
    member_lsns: list[str | None],
    coordinator_lsn: str,
) -> None:
    """Coordinator cannot ack a shared LSN a member did not commit.

    Estuary multi-binding writes one checkpoint for every binding. A
    member behind that LSN would be silently skipped on dest-authoritative
    resume.
    """
    if not coordinator_lsn or not member_lsns:
        return
    for lsn in member_lsns:
        if not lsn or compare_lsn(lsn, coordinator_lsn) < 0:
            raise ExactlyOnceRouteError(
                "exactly_once bundle coordinator LSN "
                f"{coordinator_lsn} is ahead of a member ({lsn!r}). "
                "Refuse dest-authoritative skip.",
                reason=REASON_BUNDLE_LSN,
            )


def checksum_conflict_quarantine(
    *,
    incoming_lsn: str,
    incoming_checksum: str,
    dest_checksum: str,
    change: Any = None,
) -> list[dict[str, Any]]:
    """Surface conflicting redelivery — never drop, never overwrite dest."""
    from services.quarantine_row_contract import normalize_quarantine_row

    records = list(getattr(change, "inserts", None) or []) + list(
        getattr(change, "updates", None) or []
    )
    if not records:
        records = [
            {
                "lsn": incoming_lsn,
                "incoming_checksum": incoming_checksum,
                "dest_checksum": dest_checksum,
            }
        ]
    rows: list[dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        rows.append(
            normalize_quarantine_row(
                {
                    "reason": REASON_CHECKSUM,
                    "failure_reason": REASON_CHECKSUM,
                    "stage": "cdc_exactly_once",
                    "source_record": rec,
                    "original_value": rec,
                    "error": (
                        f"LSN {incoming_lsn} dest_checksum={dest_checksum[:16]} "
                        f"incoming_checksum={incoming_checksum[:16]}"
                    ),
                    "recovery_suggestion": (
                        "Dest already committed this LSN. Inspect quarantine, "
                        "do not overwrite dest; fix the producer or replay a "
                        "matching payload."
                    ),
                    "retry_status": "open",
                    "connector": "cdc_exactly_once",
                    "lsn": incoming_lsn,
                }
            )
        )
    return rows


def next_handoff_phase(incoming_phase: str, dest_phase: str | None) -> str:
    incoming = (incoming_phase or "streaming").strip().lower()
    dest = (dest_phase or "").strip().lower()
    if incoming == "streaming" or dest == "streaming":
        return "streaming"
    return "snapshot"


def batch_apply_checksum(
    change: Any,
    *,
    incoming_lsn: str,
    pk_cols: list[str],
) -> str:
    """Dest-owned payload identity for this LSN — redelivery must match."""
    import hashlib
    import json

    from services.cdc_snapshot_window import _pk_value

    combined = combine_change_batch(change, pk_cols=pk_cols)
    updates = []
    for rec in list(combined.inserts or []) + list(combined.updates or []):
        if not isinstance(rec, dict):
            continue
        updates.append(
            {
                "pk": _pk_value(rec, pk_cols) or "",
                "cols": {str(k): rec.get(k) for k in sorted(rec)},
            }
        )
    updates.sort(key=lambda row: str(row.get("pk") or ""))
    payload = {
        "lsn": incoming_lsn,
        "updates": updates,
        "deletes": sorted(str(k) for k in list(combined.deletes or [])),
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def assert_redelivery_checksum(
    incoming: str,
    dest: str | None,
    *,
    incoming_lsn: str = "",
    change: Any = None,
) -> None:
    if not dest or not incoming:
        return
    if dest != incoming:
        raise ExactlyOnceRouteError(
            "exactly_once redelivery checksum mismatch — dest already committed "
            "this LSN with a different payload. Refuse silent overwrite; "
            "conflicting rows are quarantined.",
            reason=REASON_CHECKSUM,
            quarantine=checksum_conflict_quarantine(
                incoming_lsn=incoming_lsn or "",
                incoming_checksum=incoming,
                dest_checksum=dest,
                change=change,
            ),
        )


@dataclass
class EosBundleStream:
    """One stream inside a shared-log dest transaction (Estuary multi-binding)."""

    dest_table: str
    change: Any
    mappings: list[dict[str, Any]]
    column_types: dict[str, str]
    pk_target_cols: list[str]
    stream_key: str
    headers: list[str] = field(default_factory=list)


@dataclass
class DestWmView:
    committed_lsn: str | None = None
    epoch: int = 0
    fence_epoch: int = 0
    phase: str = ""
    apply_checksum: str = ""
    resume_blob: str = ""
    apply_seq: int = 0
    window_id: str = ""


def decide_eos_apply(
    *,
    incoming_lsn: str,
    dest_lsn: str | None,
    incoming_fence: int = 0,
    dest_fence: int = 0,
    dest_epoch: int = 0,
    incoming_phase: str = "",
    dest_phase: str = "",
    incoming_checksum: str = "",
    dest_checksum: str = "",
    incremental_snapshot: bool = False,
    incoming_window_id: str = "",
    dest_window_id: str = "",
    change: Any = None,
) -> tuple[str, int]:
    """Return ``(action, next_fence)``.

    Actions: apply | already_committed | handoff_phase | stream_wins_skip
    | window_closed_skip
    """
    assert_writer_fence(incoming_fence, dest_fence)
    fence = next_dest_fence(incoming_fence, dest_fence)
    if dest_owned_window_closed(
        incoming_window_id=incoming_window_id,
        dest_window_id=dest_window_id,
    ):
        return "window_closed_skip", fence
    if dest_owned_stream_wins(
        incoming_phase=incoming_phase,
        dest_phase=dest_phase,
        incoming_lsn=incoming_lsn,
        dest_lsn=dest_lsn,
        incremental_snapshot=incremental_snapshot,
    ):
        return "stream_wins_skip", fence
    if already_committed(incoming_lsn, dest_lsn):
        if (
            (incoming_phase or "streaming") == "streaming"
            and (dest_phase or "") == "snapshot"
        ):
            return "handoff_phase", fence
        assert_redelivery_checksum(
            incoming_checksum,
            dest_checksum or None,
            incoming_lsn=incoming_lsn,
            change=change,
        )
        return "already_committed", fence
    _ = dest_epoch
    return "apply", fence


def decide_from_view(
    *,
    incoming_lsn: str,
    dest: DestWmView,
    incoming_fence: int = 0,
    incoming_phase: str = "",
    incoming_checksum: str = "",
    incremental_snapshot: bool = False,
    change: Any = None,
) -> tuple[str, int]:
    return decide_eos_apply(
        incoming_lsn=incoming_lsn,
        dest_lsn=dest.committed_lsn,
        incoming_fence=incoming_fence,
        dest_fence=dest.fence_epoch,
        dest_epoch=dest.epoch,
        incoming_phase=incoming_phase,
        dest_phase=dest.phase,
        incoming_checksum=incoming_checksum,
        dest_checksum=dest.apply_checksum,
        incremental_snapshot=incremental_snapshot,
        incoming_window_id=extract_snapshot_window_id(
            getattr(change, "resume_token", None)
        ),
        dest_window_id=dest.window_id,
        change=change,
    )


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
        "Dest watermark is SSOT on resume (job cursor cannot invent ahead).",
        "Writer fence (lease generation) is persisted in the dest txn.",
        "Batch is reduced last-op-per-PK before apply.",
        "Shared-log multi-stream applies N tables in one dest transaction.",
        "Snapshot→stream handoff is dest-owned (same LSN does not double-write).",
        "Redelivery checksum must match the dest-committed payload; mismatch quarantines.",
        "Open raises dest fence with no data and returns the dest resume blob.",
        "Dest-owned DDD-3 stream-wins: snapshot READ cannot overwrite streaming dest.",
        "Bundle coordinator LSN cannot pass a member that did not reach it.",
        "Dest load-reduce-store merges incoming into dest rows in the apply txn.",
        "Post-commit dest verify before source ack — dest must show the LSN.",
        "apply_seq is dest-monotonic.",
        "Dest-owned incremental-snapshot window_id: closed windows are not re-applied.",
        "Quarantine replay is dest-LSN gated — dest at or past that LSN refuses overwrite.",
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
        writer_fence: int = 0,
    ) -> EosApplyResult:
        """BEGIN → fence → filter → apply → watermark → COMMIT, with optional crash."""
        current = self.watermarks.get(stream_key)
        dest_lsn = current.committed_lsn if current else None
        dest_fence = current.fence_epoch if current else 0
        dest_epoch = current.epoch if current else 0
        action, fence = decide_eos_apply(
            incoming_lsn=incoming_lsn,
            dest_lsn=dest_lsn,
            incoming_fence=writer_fence,
            dest_fence=dest_fence,
            dest_epoch=dest_epoch,
        )
        if action == "already_committed":
            return EosApplyResult(
                status="already_committed",
                committed_lsn=dest_lsn,
                batch_id=current.batch_id if current else batch_id,
                epoch=dest_epoch,
                already_committed=True,
                fence_epoch=fence,
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
                fence_epoch=fence,
                prev_lsn=dest_lsn,
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
                fence_epoch=fence,
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
