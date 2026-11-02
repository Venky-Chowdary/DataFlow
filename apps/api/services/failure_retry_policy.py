"""Is a failed run worth running again, or will it fail identically?

An unattended retry is only defensible when the *inputs* may have changed. A
run that stopped at Validate because a mapping is below the confidence floor,
or because a type path is lossy, will reach the same verdict on every attempt:
the schedule burns its budget, the operator sees three red runs instead of one,
and the corrective action — which is a configuration change, not time — is
buried under retry noise. Retries exist for the failures that time repairs:
a dropped connection, a deadlock, a warehouse that was resuming, a lost worker.

This module is the single owner of that question. It classifies a failure as
``transient`` (retry), ``deterministic`` (refuse, name the corrective action)
or ``unknown`` (retry once — an unrecognised error is not proof of either).

It never *permits* a retry the execution contract refuses: duplicate safety is
owned by ``services.execution_engine_contract``. This only removes retries that
cannot help.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from services.append_continuity import REBASELINE_REMEDY
from services.preflight_cursor_gate import clear_cursor_remedy

TRANSIENT = "transient"
DETERMINISTIC = "deterministic"
UNKNOWN = "unknown"

# Phases that can only be reached by re-deciding the same configuration against
# the same catalogs. Nothing about waiting changes their verdict.
_DETERMINISTIC_PHASES = frozenset(
    {"validating", "validate", "preflight", "mapping", "planning"}
)

# Deterministic refusals the engine states in words. Each is owned by a gate or
# a contract check and is resolved by changing the job, not by trying again.
_DETERMINISTIC_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"mapping confidence below floor|below the map confidence floor",
     "Open Map and confirm or remap the column(s) below the confidence floor."),
    (r"fidelity collapse|lossy .*type path|fidelity risk across type path",
     "Open Map: the source→destination type path loses values. Widen the "
     "destination type or declare the narrowing in the Risk Contract."),
    (r"unsupported (type|semantic|sync mode|conversion)|refuse[sd]? silent",
     "The requested conversion is not supported on this route — change the "
     "mapping or the destination type."),
    (r"requires primary_key|no primary key|primary key .*(missing|required)",
     "Set a primary key on the stream contract for this sync mode."),
    (r"not null .*(without|no) default|required target column",
     "The destination requires a value for this column — map a source column "
     "or give the column a default."),
    (r"column .*(cannot be matched|not found in (source|destination))",
     "Column names no longer line up — open Map and rebind the column."),
    (r"cursor .*(undeclared|unknown|not declared)",
     "Declare what the cursor column means before scheduling an incremental "
     "sync."),
    (r"schema (mismatch|drift) .*(refus|block)",
     "Approve the destination drift, or pin the contract, before re-running."),
    (r"schema change detected|breaking schema change",
     "Approve the schema change, or acknowledge the drift, before re-running."),
    (r"\bg13_source_coverage\b|\bschema_drift\b|\bno_disposition\b|"
     r"neither mapped nor declared omitted",
     "Map the unaccounted source column or declare it omitted, then "
     "acknowledge the drift before the next run."),
    # One login rejection, however the server words it. PostgreSQL says
    # 'password authentication failed for user "x"' under password auth and
    # 'role "x" does not exist' under trust/peer auth for the same unknown
    # login; SQL Server says 'Login failed for user'; Oracle ORA-01017.
    # Classifying them differently made an unattended retry depend on the
    # server's pg_hba.conf, not on the failure.
    (r"authentication failed|invalid credentials|access denied|permission "
     r"denied|not authorized|403|role \"[^\"]*\" does not exist|"
     r"login failed for user|ora-01017|invalid username/password",
     "Credentials or grants are rejected — fix them on the connector."),
    (r"strict error policy blocks partial write",
     "Quarantined cells blocked the write under the strict error policy — fix "
     "the values, widen the destination type, or relax the policy."),
    (r"cannot resume a streaming insert without a primary key",
     "Re-run as Full refresh · Overwrite, or set a primary key."),
    (r"no persisted column mappings",
     "Open Transfer Studio with this schedule's source and destination. "
     "Map the columns, run Validate, then Schedule from the Studio footer — "
     "that persists the mapping contract the beat can replay."),
    (r"decision artifact (ddl identity diverged|dest schema drifted|content_hash mismatch|content_hash does not match)",
     "Re-run Validate only when Map or dest DDL actually changed. Dest existing "
     "after the first write is not a plan change — Run now if the Map is unchanged."),
    (r"file not arrived:.*wait window elapsed",
     "The wait-for-file window ended with no new file. Check the drop path "
     "or widen file_wait_seconds, then run the schedule again."),
    # D100: a watermark that outlived its destination can never self-heal —
    # park on the first beat and name the recovery, don't re-fail every tick.
    (r"destination reset since the last incremental run",
     "The destination was reset or dropped after this cursor advanced — "
     f"clear the cursor to re-snapshot ({clear_cursor_remedy()}) "
     f"or rebaseline the route ({REBASELINE_REMEDY}), "
     "then re-run."),
)

# A relation the driver says is absent. The quoted name is captured so a
# schedule's finding can name the table (QA SC5). This is not a retry class on
# its own — a table can be mid-swap — the scheduler decides with the schedule's
# source table in hand. ``role "x" does not exist`` is a login failure and is
# classified with the credential refusals above.
_MISSING_RELATION_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r'relation "([^"]+)" does not exist', re.I),
    re.compile(r"table '([^']+)' doesn't exist", re.I),
    re.compile(r"no such table: ([^\s,;]+)", re.I),
    re.compile(r"invalid object name '([^']+)'", re.I),
    re.compile(r"collection ['\"]?([^'\"\s]+)['\"]? (?:does not exist|not found)", re.I),
)
_MISSING_RELATION_GENERIC = re.compile(
    r"ora-00942|table or view does not exist|table was not found", re.I
)


def missing_relation_name(text: str | None) -> str | None:
    """The table a "does not exist" failure names: ``""`` when it names none,
    ``None`` when the failure is not a missing relation at all."""
    message = str(text or "")
    for pattern in _MISSING_RELATION_RES:
        match = pattern.search(message)
        if match:
            return match.group(1).strip()
    return "" if _MISSING_RELATION_GENERIC.search(message) else None


# Failures whose cause is the moment, not the configuration.
_TRANSIENT_PATTERNS: tuple[str, ...] = (
    r"connection (reset|refused|aborted|closed|timed out|error)",
    r"could not connect|failed to connect|no route to host|name or service not known",
    r"timeout|timed out",
    r"deadlock|lock wait timeout|serialization failure|could not serialize",
    r"too many connections|connection pool|max connections",
    r"temporarily unavailable|service unavailable|try again|please retry",
    r"broken pipe|eof occurred|reset by peer|network is unreachable",
    r"rate limit|throttl|429|slow ?down",
    r"50[0234] (server )?error|internal server error|bad gateway",
    r"worker (lost|died|terminated)|lease expired|heartbeat lost",
    r"warehouse .*(suspend|resum)|is being resumed",
    r"file not arrived:.*no new file yet",
)


@dataclass(frozen=True)
class RetryClassification:
    """Why this failure will, or will not, behave differently next time."""

    kind: str
    reason: str
    corrective_action: str = ""

    @property
    def retryable(self) -> bool:
        return self.kind != DETERMINISTIC

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "reason": self.reason,
            "corrective_action": self.corrective_action,
            "retryable": self.retryable,
        }


def _blockers_text(blockers: Sequence[Any] | None) -> str:
    parts: list[str] = []
    for blocker in blockers or []:
        if isinstance(blocker, str):
            parts.append(blocker)
        elif isinstance(blocker, Mapping):
            parts.append(str(blocker.get("message") or ""))
            parts.append(str(blocker.get("id") or ""))
    return " ".join(parts)


def classify_failure(
    *,
    error: str | None,
    phase: str | None = None,
    rows_written: int = 0,
    blockers: Sequence[Any] | None = None,
) -> RetryClassification:
    """Classify a finished run's failure for retry purposes.

    ``phase`` and ``blockers`` matter as much as the message: a run that never
    left Validate wrote nothing because a gate said no, and the gate will say
    the same thing to the next attempt.
    """
    text = f"{error or ''} {_blockers_text(blockers)}".strip().lower()

    for pattern, action in _DETERMINISTIC_PATTERNS:
        if re.search(pattern, text):
            return RetryClassification(
                kind=DETERMINISTIC,
                reason=(
                    "This attempt was refused by a validation gate, not by a "
                    "transient fault — retrying replays the same decision."
                ),
                corrective_action=action,
            )

    for pattern in _TRANSIENT_PATTERNS:
        if re.search(pattern, text):
            return RetryClassification(
                kind=TRANSIENT,
                reason="Operational fault — a later attempt may succeed.",
            )

    phase_key = (phase or "").strip().lower()
    if phase_key in _DETERMINISTIC_PHASES and int(rows_written or 0) <= 0:
        return RetryClassification(
            kind=DETERMINISTIC,
            reason=(
                f"The run stopped in the `{phase_key}` phase without writing a "
                "row: the same configuration is re-decided against the same "
                "catalogs on every attempt."
            ),
            corrective_action=(
                "Open Validate for this job and resolve the blocking check, "
                "then run the schedule again."
            ),
        )

    return RetryClassification(
        kind=UNKNOWN,
        reason="Cause not recognised — retried once rather than assumed fatal.",
    )


def classify_job_failure(job: Mapping[str, Any] | None) -> RetryClassification:
    """Classify straight off a job document."""
    doc = job or {}
    preflight = doc.get("preflight") if isinstance(doc.get("preflight"), Mapping) else {}
    blockers = preflight.get("blockers") if isinstance(preflight, Mapping) else None
    return classify_failure(
        error=str(doc.get("error") or ""),
        phase=str(doc.get("phase") or ""),
        rows_written=int(doc.get("records_processed") or 0),
        blockers=blockers if isinstance(blockers, Sequence) else None,
    )
