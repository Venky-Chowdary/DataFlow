"""Module 14 — Execution Engine Contract.

Charter: deterministic execution with Checkpoint / Resume / Retry / Crash /
Connection recovery / Idempotency / Partial failure. Never silent drop.
Never claim exactly-once when delivery is at-least-once.

This module is the honesty SSOT. It does not rewrite the stream engine —
it freezes semantics and provides fail-closed helpers for known gaps.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

EXECUTION_ENGINE_CONTRACT_VERSION = "execution_engine_contract.v1"

# Product delivery default — never invent exactly-once.
DEFAULT_DELIVERY_SEMANTICS = "at_least_once"


class ResumeKind(str, Enum):
    CHECKPOINT = "checkpoint_resume"
    FROM_ZERO_IDEMPOTENT = "from_zero_idempotent_sync"
    FROM_ZERO_NO_WRITES = "from_zero_no_writes"
    REFUSED = "refused"


class RetryKind(str, Enum):
    FROM_START = "retry_from_start"
    RESUME_CHECKPOINT = "resume_checkpoint"
    IN_FLIGHT_NETWORK = "in_flight_network"


class ExecutionContractError(Exception):
    """Fail-closed execution contract violation."""


def is_idempotent_sync(sync_mode: str | None) -> bool:
    """True when restart-from-zero converges (upsert/overwrite/mirror family)."""
    sync = (sync_mode or "").strip().lower()
    if not sync:
        return False
    if sync in {"upsert", "overwrite", "mirror", "replace", "scd2", "scd-2"}:
        return True
    if "upsert" in sync or "overwrite" in sync or sync.endswith("_mirror"):
        return True
    return False


def decide_resume(
    *,
    resume_requested: bool,
    checkpoint_has_progress: bool,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
) -> dict[str, Any]:
    """Decide Resume posture without inventing durable progress.

    Insert/append without a durable checkpoint **and** prior committed rows
    ⇒ REFUSED (would duplicate). Zero committed rows cannot duplicate — allow
    from-zero (orphan reclaim / claim-worker false resume). Upsert/overwrite
    without checkpoint ⇒ FROM_ZERO_IDEMPOTENT (convergent).

    ``rows_committed_known=False`` means the control plane could not answer how
    much this run already wrote (job document missing, metadata store down). An
    unknown row count is not zero: for an append that difference is a duplicated
    load, so the unknown case fails closed unless the sync mode converges.
    """
    if not resume_requested:
        return {
            "kind": ResumeKind.CHECKPOINT.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": "Fresh run — checkpoint starts empty.",
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    if checkpoint_has_progress:
        return {
            "kind": ResumeKind.CHECKPOINT.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                "Resume from last committed chunk — at-least-once; sinks must "
                "upsert / ledger to converge."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    # Reclaim with no writes yet — restart-from-zero cannot duplicate.
    if rows_committed_known and int(rows_committed or 0) <= 0:
        return {
            "kind": ResumeKind.FROM_ZERO_NO_WRITES.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                "No durable checkpoint and zero committed rows — restart from "
                "zero (cannot duplicate)."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    if is_idempotent_sync(sync_mode):
        return {
            "kind": ResumeKind.FROM_ZERO_IDEMPOTENT.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                f"No durable checkpoint; restarting from zero under convergent "
                f"sync_mode={sync_mode}."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    if not rows_committed_known:
        return {
            "kind": ResumeKind.REFUSED.value,
            "allowed": False,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                "No durable checkpoint, and the control plane could not confirm "
                "how many rows this run already committed. Unknown is not zero — "
                "restarting an append from zero could duplicate rows. Use Retry "
                "from start once the destination is known, or re-Validate."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    return {
        "kind": ResumeKind.REFUSED.value,
        "allowed": False,
        "delivery": DEFAULT_DELIVERY_SEMANTICS,
        "reason": (
            "No durable checkpoint to resume. Insert/append restart-from-zero "
            "would duplicate rows — use Retry from start or re-Validate."
        ),
        "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
    }


def decide_retry_from_start(
    *,
    status: str | None = None,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
) -> dict[str, Any]:
    """Decide whether re-running a finished attempt from zero is safe.

    Retry from start re-reads the source from the beginning. For a convergent
    sync mode that is harmless, and for an attempt that committed nothing there
    is nothing to duplicate — but an append that already put rows in the
    destination has no key to collapse them, so the second attempt writes every
    committed row a second time and both runs report success. The operator's
    action there is Resume, which continues from the committed checkpoint.

    A cancelled run is refused for a different reason: it stopped because
    somebody asked it to, and restarting it silently reverses that decision.
    """
    if (status or "").strip().lower() == "cancelled":
        return {
            "kind": ResumeKind.REFUSED.value,
            "allowed": False,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                "This run was cancelled, not failed. Restarting it from zero "
                "reverses that decision and re-reads the whole source — start "
                "it again explicitly if that is what you want."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    if rows_committed_known and int(rows_committed or 0) <= 0:
        return {
            "kind": ResumeKind.FROM_ZERO_NO_WRITES.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": "Zero committed rows — restarting from zero cannot duplicate.",
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    if is_idempotent_sync(sync_mode):
        return {
            "kind": ResumeKind.FROM_ZERO_IDEMPOTENT.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                f"sync_mode={sync_mode} converges — a second full pass lands the "
                "same rows rather than adding them."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    committed = (
        f"{int(rows_committed or 0)} row(s)"
        if rows_committed_known
        else "an unknown number of rows"
    )
    return {
        "kind": ResumeKind.REFUSED.value,
        "allowed": False,
        "delivery": DEFAULT_DELIVERY_SEMANTICS,
        "reason": (
            f"This attempt already committed {committed} under "
            f"sync_mode={sync_mode or 'append'}, which has no key to collapse a "
            "second copy. Retry from start would duplicate them — resume from "
            "the last committed checkpoint instead."
        ),
        "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
    }


def assert_retry_from_start_allowed(
    *,
    status: str | None = None,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
) -> dict[str, Any]:
    """Fail closed when a from-zero retry would duplicate committed rows."""
    decision = decide_retry_from_start(
        status=status,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
    )
    if not decision["allowed"]:
        raise ExecutionContractError(decision["reason"])
    return decision


def committed_rows_of(job: dict | None) -> tuple[int, bool]:
    """``(rows committed, whether that number is knowable)`` for a job document.

    A missing job document or an unreadable counter is *unknown*, never zero:
    treating it as zero is what turns a refused duplicate into a silent one.
    """
    if not isinstance(job, dict):
        return 0, False
    for key in ("records_processed", "rows_written"):
        if key in job:
            try:
                return int(job.get(key) or 0), True
            except (TypeError, ValueError):
                return 0, False
    cp = job.get("checkpoint")
    if isinstance(cp, dict):
        try:
            return int(cp.get("rows_processed") or 0), True
        except (TypeError, ValueError):
            return 0, False
    return 0, False


def assert_resume_allowed(
    *,
    resume_requested: bool,
    checkpoint_has_progress: bool,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
) -> dict[str, Any]:
    """Fail closed when Resume would silently duplicate."""
    decision = decide_resume(
        resume_requested=resume_requested,
        checkpoint_has_progress=checkpoint_has_progress,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
    )
    if not decision["allowed"]:
        raise ExecutionContractError(decision["reason"])
    return decision


def job_has_durable_progress(job: dict | None) -> bool:
    """True when a job document carries committed progress safe to resume."""
    if not isinstance(job, dict):
        return False
    try:
        if int(job.get("records_processed") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    cp = job.get("checkpoint")
    if not isinstance(cp, dict):
        return False
    try:
        return bool(
            int(cp.get("chunk_index") or 0) > 0
            or int(cp.get("rows_processed") or 0) > 0
            or int(cp.get("offset") or 0) > 0
            or int(cp.get("file_offset") or 0) > 0
            or cp.get("cursor_value") is not None
            or cp.get("dynamodb_cursor")
            or cp.get("kafka_cursor")
            or cp.get("es_search_after") is not None
        )
    except (TypeError, ValueError):
        return False


def resolve_reclaim_resume(job: dict | None) -> bool:
    """Whether orphan/claim reclaim should pass ``resume=True``.

    Pending jobs and mid-flight reclaim with **no** durable progress must run
    as a fresh start — forcing resume on append/Excel falsely fails Module 14.
    """
    return job_has_durable_progress(job)


def kafka_offset_commit_must_fail_closed(exc: BaseException) -> ExecutionContractError:
    """After durable checkpoint, Kafka offset commit failure must abort the job.

    Swallowing leaves delivery state unexplained (source may re-deliver while
    checkpoint claims progress). At-least-once remains honest only if the job fails.
    """
    return ExecutionContractError(
        "Kafka offset commit failed after durable checkpoint — fail closed so "
        f"delivery stays explainable (at-least-once, not silent): {exc}"
    )


def noop_checkpoint_posture() -> dict[str, Any]:
    """Honesty stamp for in-process staging phases that must not claim resume."""
    return {
        "durable": False,
        "resume_supported": False,
        "delivery": DEFAULT_DELIVERY_SEMANTICS,
        "note": (
            "No-op checkpoint used for internal staging/SCD2 phase only — "
            "parent job checkpoint remains the durable resume SSOT; this phase "
            "must never be advertised as crash-recoverable alone."
        ),
        "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
    }


def capability_matrix() -> dict[str, Any]:
    """Charter capability table — proven vs not claimed."""
    return {
        "checkpoint": {
            "available": True,
            "semantics": "Fail-closed require_save after batch commit",
        },
        "resume": {
            "available": True,
            "semantics": "At-least-once from last committed chunk; refused for insert without progress",
        },
        "retry_from_start": {
            "available": True,
            "semantics": (
                "Distinct from Resume — restarts transfer; refused for a "
                "non-convergent sync that already committed rows, and for a "
                "cancelled run"
            ),
        },
        "in_flight_network_retry": {
            "available": True,
            "semantics": "RetryBudget / with_retry on retriable IO",
        },
        "crash_recovery": {
            "available": True,
            "semantics": "Orphan job reschedule with resume when safety allows",
            "non_guarantee": "Not a fence against ambiguous append commits",
        },
        "connection_recovery": {
            "available": True,
            "semantics": "SQL writer reconnect budget + write ledger where wired",
        },
        "idempotency": {
            "available": True,
            "semantics": "Job claim + upsert/chunk ledger/keyed document — not global exactly-once",
        },
        "bulk_loading": {
            "available": True,
            "semantics": "Dest-specific bulk paths (e.g. COPY) under same quarantine contract",
        },
        "streaming": {
            "available": True,
            "semantics": "Chunked stream with ordered checkpoint",
        },
        "partial_failure": {
            "available": True,
            "semantics": "Quarantine holdout — never silent drop; rejected_details may truncate with counter",
        },
        "table_isolation": {
            "available": True,
            "semantics": "Sequential multi-stream; shared job checkpoint (at-least-once)",
            "non_guarantee": "Not independent durable cursors per table under concurrency",
        },
        "transaction_recovery": {
            "available": True,
            "semantics": "CDC transaction buffer only — not XA across heterogeneous sinks",
            "non_guarantee": "Bulk load 2PC / exactly-once CDC not claimed",
        },
        "exactly_once": {
            "available": True,
            "opt_in": True,
            "platform_claimed": False,
            "semantics": (
                "Opt-in CDC dest-owned watermark transaction "
                "(apply + _df_cdc_eos_watermarks in one dest txn). "
                "Default remains at-least-once. Ineligible routes fail closed. "
                "Not XA and not platform-wide EOS."
            ),
        },
    }


SELECTABLE_DELIVERY_SEMANTICS: frozenset[str] = frozenset(
    {
        "at_least_once",
        "exactly_once",
        # at_most_once only when a sink path explicitly supports fire-and-forget;
        # not offered as a default product selector.
    }
)


class DeliveryGuaranteeError(ValueError):
    """Raised when a client requests an impossible delivery guarantee."""


def assert_delivery_guarantee_allowed(requested: str | None) -> str:
    """Normalize delivery selection. Exactly-once is opt-in; route gate is separate."""
    raw = (requested or DEFAULT_DELIVERY_SEMANTICS).strip().lower().replace("-", "_")
    if not raw:
        return DEFAULT_DELIVERY_SEMANTICS
    if raw in {"eos", "exactlyonce"}:
        raw = "exactly_once"
    if raw == "at_most_once":
        raise DeliveryGuaranteeError(
            "at_most_once is not a selectable product guarantee — "
            "default remains at_least_once with upsert/idempotent writes."
        )
    if raw not in SELECTABLE_DELIVERY_SEMANTICS:
        raise DeliveryGuaranteeError(
            f"Unknown delivery guarantee {requested!r} — allowed: "
            f"{sorted(SELECTABLE_DELIVERY_SEMANTICS)}"
        )
    return raw


def execution_contract_dict() -> dict[str, Any]:
    """Canonical posture for proof packs / Theater / workspace security."""
    return {
        "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        "delivery_default": DEFAULT_DELIVERY_SEMANTICS,
        "selectable_delivery": sorted(SELECTABLE_DELIVERY_SEMANTICS),
        "never_silent_drop": True,
        "never_claim_exactly_once": True,
        "duplicate_prevention": [
            "idempotent_upsert",
            "chunk_ledger",
            "keyed_document",
            "job_idempotency_claim",
            "refuse_insert_resume_without_checkpoint_after_writes",
            "refuse_retry_from_start_after_committed_append",
            "allow_from_zero_when_rows_committed_zero",
        ],
        "capabilities": capability_matrix(),
        "operator_notes": [
            "Resume ≠ Retry from start.",
            "Checkpoint persistence failure aborts the job.",
            "Kafka offset commit after checkpoint must fail closed.",
            "Quarantine / rejected_rows is the partial-failure SSOT.",
            "Platform-wide exactly-once is not claimed; route opt-in dest-txn EOS is fail-closed.",
            "Append sinks require ack / watermark persistence fail-closed.",
            "Orphan/claim reclaim uses resume only when durable progress exists.",
        ],
        "docs": "docs/EXECUTION_ENGINE_CONTRACT.md",
    }
