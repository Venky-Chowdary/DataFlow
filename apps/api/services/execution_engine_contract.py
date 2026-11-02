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
    FROM_CURSOR = "from_cursor_contract"
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
    if "upsert" in sync or "overwrite" in sync or "dedup" in sync or sync.endswith("_mirror"):
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


_INCREMENTAL_MODE_PREFIXES = ("incremental", "cdc")


def _is_incremental_mode(sync_mode: str | None) -> bool:
    sync = (sync_mode or "").strip().lower()
    return any(sync.startswith(prefix) for prefix in _INCREMENTAL_MODE_PREFIXES)


def decide_retry_from_start(
    *,
    status: str | None = None,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
    operator_requested: bool = False,
    has_key_contract: bool = False,
    has_cursor_contract: bool = False,
    destination_engine: str | None = None,
    source_engine: str | None = None,
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
    The unattended scheduler leaves ``operator_requested`` false. An operator
    retry (the REST route and the Pilot tool) passes true so the duplicate
    guard still runs and the cancel is an explicit new attempt.
    """
    if (status or "").strip().lower() == "cancelled" and not operator_requested:
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
    # M15: a Kafka *source* does not re-read from zero on retry — the
    # consumer resumes at the group's committed offset (D85: offsets commit
    # only after acceptance), so a retry cannot re-publish already-consumed
    # messages unless the previous attempt's offsets never committed, which
    # is the at-least-once edge the run reports honestly.
    from services.copy_kafka_common import kafka_family_name
    if kafka_family_name(source_engine or "") == "kafka":
        return {
            "kind": ResumeKind.FROM_CURSOR.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                "Kafka source resumes at the consumer group's committed offset "
                "— a retry does not re-read already-committed messages "
                "(at-least-once: messages consumed but not yet committed are "
                "re-read and the destination key collapses them)."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    # C26/D90: a Kafka topic cannot be rolled back — published messages are
    # final, and a declared key only helps log compaction (lazy, per-topic),
    # it does not make a second produce pass converge. The only honest retry
    # is a cursor-continuing incremental read or Resume from the checkpoint.
    if kafka_family_name(destination_engine or "") == "kafka":
        if has_cursor_contract and _is_incremental_mode(sync_mode):
            return {
                "kind": ResumeKind.FROM_CURSOR.value,
                "allowed": True,
                "delivery": DEFAULT_DELIVERY_SEMANTICS,
                "reason": (
                    f"sync_mode={sync_mode} carries a cursor contract — the new "
                    "attempt reads only rows after the persisted watermark, so "
                    "already-published messages are not produced again. Note: "
                    "messages already on the topic are final and cannot be "
                    "rolled back."
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
                f"This attempt already published {committed} to a Kafka topic, "
                "which cannot be rolled back or merged — retrying from start "
                "would produce a second copy downstream. Resume from the last "
                "committed checkpoint instead."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    # D208: say what the new attempt reads. An incremental append or SCD2 with a
    # cursor continues from the watermark (a full pass would re-append rows or
    # reopen history), and so does an unattended retry of an incremental
    # upsert. Only an operator re-run/retry of an upsert is a full second pass
    # (services.full_pass_rerun makes the job one).
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    canonical = normalize_sync_mode(sync_mode or "", default="")
    if requires_incremental(canonical) and canonical != "cdc" and (
        (canonical == "incremental_deduped" and not operator_requested)
        or (canonical != "incremental_deduped" and has_cursor_contract)
    ):
        return {
            "kind": ResumeKind.FROM_CURSOR.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "read_scope": "from_watermark",
            "reason": (
                f"sync_mode={sync_mode} carries a cursor contract — the new "
                "attempt continues from the persisted watermark and reads only "
                "rows after it (the tie group at the watermark is re-read and "
                "collapsed). It is not a full second pass."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    # C20/D5: the gate must check the contract, not the mode family. A declared
    # merge/upsert key collapses a second copy onto the same rows.
    if has_key_contract or is_idempotent_sync(sync_mode):
        return {
            "kind": ResumeKind.FROM_ZERO_IDEMPOTENT.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "read_scope": "full_source",
            "reason": (
                f"sync_mode={sync_mode} converges — a second full pass merges "
                "onto the declared key rather than adding a second copy."
                if has_key_contract
                else f"sync_mode={sync_mode} converges — a second full pass lands the "
                "same rows rather than adding them."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    # C20/D12: an incremental rerun does not re-read from zero — it continues
    # from the persisted watermark cursor, so only new rows are read and no
    # committed row can be written twice.
    if has_cursor_contract and _is_incremental_mode(sync_mode):
        return {
            "kind": ResumeKind.FROM_CURSOR.value,
            "allowed": True,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                f"sync_mode={sync_mode} carries a cursor contract — the new "
                "attempt continues from the persisted watermark and reads only "
                "rows after it. The committed prefix cannot be duplicated."
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


_RERUN_STATUSES = frozenset({
    "completed",
    "completed_with_quarantine",
    "cancelled",
    "success",
    "succeeded",
})


def rerun_refusal_detail(reason: str) -> dict[str, str]:
    """409 body for a refused append re-run.

    ``error`` and ``message`` are the same sentence. A client that reads only
    one of those fields still sees why the append was refused and how to
    override it (R12-001).
    """
    text = append_rerun_refusal_error(reason)
    return {
        "error": text,
        "message": text,
        "reason": (reason or text).strip(),
        "override": "Re-send with force=true to accept duplicate append rows.",
    }


def append_rerun_refusal_error(reason: str) -> str:
    """Operator text for a refused append re-run.

    The confirm path used to keep only the title ``Re-run refused`` and drop
    the reason and the ``force=true`` override (R12-001).
    """
    why = (
        reason
        or "This append already committed rows and has no key to collapse a second copy."
    ).strip()
    if why and not why.endswith("."):
        why += "."
    return (
        f"Re-run refused: {why} "
        "Re-send with force=true to accept duplicate append rows."
    )


def decide_job_rerun(
    *,
    status: str | None = None,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
    has_key_contract: bool = False,
    has_cursor_contract: bool = False,
) -> dict[str, Any]:
    """Whether a finished or cancelled job may be started again as a new job.

    Re-run keeps preflight (the new job does not set ``skip_preflight``).
    It is not a retry: retry is only for a failed or cancelled attempt that
    the operator is restarting from zero after a failure. Completed and
    completed-with-quarantine use this path. Append still refuses when the
    parent already committed rows, unless the operator passes force on the
    route that calls this decision.
    """
    st = (status or "").strip().lower()
    if st not in _RERUN_STATUSES:
        return {
            "kind": ResumeKind.REFUSED.value,
            "allowed": False,
            "delivery": DEFAULT_DELIVERY_SEMANTICS,
            "reason": (
                f"Job status {st or 'unknown'} cannot be re-run. "
                "Re-run is for completed, completed-with-quarantine, and "
                "cancelled jobs. A failed job uses retry."
            ),
            "contract_version": EXECUTION_ENGINE_CONTRACT_VERSION,
        }
    return decide_retry_from_start(
        status=st,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
        operator_requested=True,
        has_key_contract=has_key_contract,
        has_cursor_contract=has_cursor_contract,
    )


def assert_retry_from_start_allowed(
    *,
    status: str | None = None,
    sync_mode: str | None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
    has_key_contract: bool = False,
    has_cursor_contract: bool = False,
) -> dict[str, Any]:
    """Fail closed when a from-zero retry would duplicate committed rows."""
    decision = decide_retry_from_start(
        status=status,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
        has_key_contract=has_key_contract,
        has_cursor_contract=has_cursor_contract,
    )
    if not decision["allowed"]:
        raise ExecutionContractError(decision["reason"])
    return decision


_JOB_COMMITTED_KEYS = ("records_processed", "rows_written")
_SUMMARY_COMMITTED_KEYS = ("rows_written", "records_written", "records_transferred")


def committed_rows_of(job: dict | None) -> tuple[int, bool]:
    """``(rows committed, whether that number is knowable)`` for a job document.

    A missing job document or an unreadable counter is *unknown*, never zero:
    treating it as zero is what turns a refused duplicate into a silent one.

    ``records_processed`` is seeded to ``0`` on every job shell. The first
    readable key used to win, so a post-write failure that never updated the
    heartbeat looked like a clean zero and a from-zero append retried. Take
    the max of every knowable this-attempt counter, including dest summary
    and checkpoint. Do **not** read ``dest_count`` — that is table population,
    not this attempt.
    """
    if not isinstance(job, dict):
        return 0, False

    known = False
    best = 0
    unreadable = False

    def _consider(raw: object) -> None:
        nonlocal known, best, unreadable
        if raw is None:
            return
        try:
            n = int(raw)
        except (TypeError, ValueError):
            unreadable = True
            return
        known = True
        if n > best:
            best = n

    for key in _JOB_COMMITTED_KEYS:
        if key in job:
            _consider(job.get(key))
    dest = job.get("destination_summary")
    if isinstance(dest, dict):
        for key in _SUMMARY_COMMITTED_KEYS:
            if key in dest:
                _consider(dest.get(key))
    cp = job.get("checkpoint")
    if isinstance(cp, dict) and "rows_processed" in cp:
        _consider(cp.get("rows_processed"))

    if not known:
        return 0, False
    dest_for_undo = job.get("destination_summary")
    if isinstance(dest_for_undo, dict) and dest_for_undo.get("partial_batch_undo") == "cleared":
        return 0, True
    details = job.get("error_details")
    if isinstance(details, dict) and details.get("auto_create_rolled_back"):
        return 0, True
    return best, True


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
    """Normalize a delivery token. ``auto`` is resolved per route later.

    Exactly-once on an ineligible route is refused by the route gate, not here.
    """
    raw = (requested or "").strip().lower().replace("-", "_")
    if raw in {"", "auto", "default"}:
        return "auto"
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
