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
) -> dict[str, Any]:
    """Decide Resume posture without inventing durable progress.

    Insert/append without a durable checkpoint ⇒ REFUSED (would duplicate).
    Upsert/overwrite without checkpoint ⇒ FROM_ZERO_IDEMPOTENT (convergent).
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


def assert_resume_allowed(
    *,
    resume_requested: bool,
    checkpoint_has_progress: bool,
    sync_mode: str | None,
) -> dict[str, Any]:
    """Fail closed when Resume would silently duplicate."""
    decision = decide_resume(
        resume_requested=resume_requested,
        checkpoint_has_progress=checkpoint_has_progress,
        sync_mode=sync_mode,
    )
    if not decision["allowed"]:
        raise ExecutionContractError(decision["reason"])
    return decision


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
            "semantics": "Distinct from Resume — restarts transfer",
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
            "available": False,
            "semantics": "Not claimed — default delivery is at-least-once",
        },
    }


SELECTABLE_DELIVERY_SEMANTICS: frozenset[str] = frozenset(
    {
        "at_least_once",
        # at_most_once only when a sink path explicitly supports fire-and-forget;
        # not offered as a default product selector.
    }
)


class DeliveryGuaranteeError(ValueError):
    """Raised when a client requests an impossible delivery guarantee."""


def assert_delivery_guarantee_allowed(requested: str | None) -> str:
    """Normalize / refuse delivery guarantee selection. Exactly-once is never allowed."""
    raw = (requested or DEFAULT_DELIVERY_SEMANTICS).strip().lower().replace("-", "_")
    if not raw:
        return DEFAULT_DELIVERY_SEMANTICS
    if raw in {"exactly_once", "eos", "exactlyonce"}:
        raise DeliveryGuaranteeError(
            "exactly_once delivery is not available — DataWrap delivery is "
            "at_least_once only (never invent exactly-once)."
        )
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
            "refuse_insert_resume_without_checkpoint",
        ],
        "capabilities": capability_matrix(),
        "operator_notes": [
            "Resume ≠ Retry from start.",
            "Checkpoint persistence failure aborts the job.",
            "Kafka offset commit after checkpoint must fail closed.",
            "Quarantine / rejected_rows is the partial-failure SSOT.",
            "Exactly-once and one-click undo are not claimed.",
            "Append sinks require ack / watermark persistence fail-closed.",
        ],
        "docs": "docs/EXECUTION_ENGINE_CONTRACT.md",
    }
