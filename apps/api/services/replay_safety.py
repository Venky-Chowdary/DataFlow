"""Whether a destination write may be retried without duplicating rows.

A retry is only harmless when replaying the same batch converges to the same
destination state. Three mechanisms give us that guarantee:

1. ``idempotent_upsert`` — the write is keyed on conflict columns, so re-applying
   a row overwrites it rather than adding a second copy.
2. ``chunk_ledger`` — the destination records committed chunks in a durable table
   inside the data transaction, so a retry skips what already landed.
3. ``keyed_document`` — the sink addresses documents by a deterministic id
   derived from the primary key, making the write a natural upsert.

Everything else is append-only. For those sinks a retry after an *ambiguous*
failure (connection lost mid-write, timeout, broker error) can duplicate rows
while the job still reports success, which is the silent-corruption case the
product bar forbids. This module is the single place that decides which case a
given write falls into, so the engine, the CDC path, and the file path cannot
drift apart on the answer.

The policy deliberately distinguishes two failure shapes on append-only sinks:

* **pre-dispatch** failures (DNS, auth, refused connection, invalid config) prove
  nothing was written, so retrying is free and we do it.
* **ambiguous** failures leave the outcome unknown. We stop retrying and let the
  job fail so the operator resumes from the durable chunk checkpoint, which
  replays from a known boundary instead of guessing.

Availability is not sacrificed for the sinks that matter most: every SQL
warehouse gets ``chunk_ledger``, and any upsert-mode transfer gets
``idempotent_upsert``, so full retry budgets stay in force there.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "AMBIGUOUS_OUTCOME_SIGNALS",
    "ReplaySafety",
    "classify_replay_safety",
    "destination_has_chunk_ledger",
    "error_outcome_is_ambiguous",
    "writer_module_for",
]


# Writer modules that maintain the durable committed-chunk ledger. This is a set
# of *modules*, not destination names, because the same writer backs many
# destinations: connectors.postgresql_writer serves Redshift and CockroachDB,
# and connectors.generic_sql serves every SQLAlchemy dialect we route there.
#
# Naming the modules is what keeps this honest. A destination-name list drifts
# the moment a driver is re-routed, and a stale entry here claims a retry is
# safe when it would in fact duplicate rows — the exact failure this module
# exists to prevent. Membership is asserted against the live routing table in
# tests/test_replay_ledger_registry.py.
_LEDGER_WRITER_MODULES: frozenset[str] = frozenset(
    {
        "connectors.postgresql_writer",
        "connectors.mysql_writer",
        "connectors.generic_sql",
        # Thin delegates to connectors.generic_sql — they inherit its ledger.
        "connectors.sqlserver_writer",
        "connectors.oracle_writer",
        "connectors.sqlite_writer",
    }
)

# Destinations dispatched by an explicit branch in src.transfer.adapters rather
# than through the registry. The branch and the registry agree on the module
# today; this map exists so a future divergence is caught by the registry test
# instead of silently mis-classifying a write.
_EXPLICIT_WRITER_ROUTES: dict[str, str] = {
    "generic_sql": "connectors.generic_sql",
    "sqlite": "connectors.sqlite_writer",
}

# Sinks that address records by a deterministic id derived from the primary key.
# Writing the same record twice replaces it, so replay converges.
_KEYED_DOCUMENT_SINKS: frozenset[str] = frozenset(
    {
        "mongodb",
        "elasticsearch",
        "opensearch",
        "redis",
        "dynamodb",
        "pinecone",
        "qdrant",
        "weaviate",
        "milvus",
        "pgvector",
        "chroma",
    }
)

# Signals that a failure left the write outcome unknown. These are the cases
# where the request may well have been applied before the error surfaced, so a
# blind retry on an append-only sink risks a second copy.
AMBIGUOUS_OUTCOME_SIGNALS: tuple[str, ...] = (
    "timed out",
    "timeout",
    "connection reset",
    "connection lost",
    "lost connection",
    "broken pipe",
    "server closed the connection",
    "connection already closed",
    "eof detected",
    "eof occurred",
    "gone away",
    "ssl syscall error",
    "write timeout",
    "read timeout",
    "operation interrupted",
    "cancelled",
    "aborted",
    "502",
    "503",
    "504",
    "partial write",
    "incomplete",
)

# Signals that prove the request never reached the destination. Safe to replay
# even on an append-only sink.
_PRE_DISPATCH_SIGNALS: tuple[str, ...] = (
    "name or service not known",
    "nodename nor servname",
    "temporary failure in name resolution",
    "could not translate host name",
    "connection refused",
    "could not connect",
    "cannot connect",
    "can't connect",
    "no route to host",
    "network is unreachable",
    "authentication failed",
    "access denied",
    "password authentication failed",
    "invalid credentials",
)


@dataclass(frozen=True)
class ReplaySafety:
    """Verdict on whether a destination write may be retried in place."""

    safe: bool
    mechanism: str
    reason: str
    destination: str = ""
    write_mode: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def duplicate_risk(self) -> bool:
        """Whether an in-place retry could leave duplicate rows behind."""
        return not self.safe

    def allows_retry(self, error: BaseException | None) -> bool:
        """Whether ``error`` may be retried without risking duplicates.

        Replay-safe writes always retry. Append-only writes retry only when the
        error proves the batch never reached the destination.
        """
        if self.safe:
            return True
        if error is None:
            return False
        return not error_outcome_is_ambiguous(error)

    def to_dict(self) -> dict[str, Any]:
        return {
            "safe": self.safe,
            "mechanism": self.mechanism,
            "reason": self.reason,
            "destination": self.destination,
            "write_mode": self.write_mode,
            "duplicate_risk": self.duplicate_risk,
            "evidence": list(self.evidence),
        }


def writer_module_for(dest_type: str) -> str:
    """Resolve a destination to the writer module that will handle its rows.

    Mirrors the dispatch in ``src.transfer.adapters`` so replay classification
    is answered by the code that actually runs, not by a parallel list.
    """
    dest = _normalize(dest_type)
    if not dest:
        return ""
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
        from src.transfer.connector_registry import CONNECTOR_MODULES
    except ImportError:  # pragma: no cover - registry is always importable in-app
        return ""

    driver = _normalize(resolve_driver_type(dest)) or dest
    explicit = _EXPLICIT_WRITER_ROUTES.get(driver)
    if explicit:
        return explicit
    modules = CONNECTOR_MODULES.get(driver)
    return getattr(modules, "writer", "") if modules else ""


def destination_has_chunk_ledger(dest_type: str) -> bool:
    """Whether this destination's writer keeps a durable committed-chunk ledger."""
    return writer_module_for(dest_type) in _LEDGER_WRITER_MODULES


def error_outcome_is_ambiguous(error: BaseException) -> bool:
    """Whether a failure leaves it unknown if the batch was applied.

    Errors raised before the request left the process are unambiguous: nothing
    landed. Everything network-shaped after dispatch is treated as ambiguous,
    because assuming otherwise is what produces duplicate rows.
    """
    text = f"{type(error).__name__} {error}".lower()
    if any(sig in text for sig in _PRE_DISPATCH_SIGNALS):
        return False
    return any(sig in text for sig in AMBIGUOUS_OUTCOME_SIGNALS)


def classify_replay_safety(
    *,
    dest_type: str,
    write_mode: str = "insert",
    conflict_columns: Any = None,
    job_id: str | None = None,
    has_primary_key: bool | None = None,
) -> ReplaySafety:
    """Decide how — or whether — a destination write can be safely replayed.

    ``job_id`` matters because the chunk ledger is scoped to it. A write issued
    without one cannot look up what a previous attempt committed, so it does not
    get ledger credit even on a destination that supports one.
    """
    dest = _normalize(dest_type)
    mode = _normalize(write_mode) or "insert"
    keys = _as_key_list(conflict_columns)
    evidence: list[str] = [f"destination={dest or 'unknown'}", f"write_mode={mode}"]

    if mode in {"upsert", "merge", "update", "replace"} and keys:
        evidence.append(f"conflict_columns={','.join(keys)}")
        return ReplaySafety(
            safe=True,
            mechanism="idempotent_upsert",
            reason=(
                "Writes are keyed on "
                f"{', '.join(keys)}, so replaying a batch overwrites rows "
                "instead of appending copies."
            ),
            destination=dest,
            write_mode=mode,
            evidence=tuple(evidence),
        )

    if destination_has_chunk_ledger(dest):
        if job_id:
            evidence.append("chunk_ledger=active")
            return ReplaySafety(
                safe=True,
                mechanism="chunk_ledger",
                reason=(
                    "Committed chunks are recorded in a durable ledger inside "
                    "the write transaction, so a retry skips rows that already "
                    "landed."
                ),
                destination=dest,
                write_mode=mode,
                evidence=tuple(evidence),
            )
        evidence.append("chunk_ledger=unavailable_without_job_id")
        return ReplaySafety(
            safe=False,
            mechanism="none",
            reason=(
                "This destination supports a committed-chunk ledger but the "
                "write was issued without a job id, so an interrupted attempt "
                "cannot be told apart from a fresh one."
            ),
            destination=dest,
            write_mode=mode,
            evidence=tuple(evidence),
        )

    if dest in _KEYED_DOCUMENT_SINKS:
        if has_primary_key is False:
            evidence.append("document_id=generated")
            return ReplaySafety(
                safe=False,
                mechanism="none",
                reason=(
                    f"{dest} records are addressed by a generated id because the "
                    "source has no primary key, so a replayed batch inserts new "
                    "documents rather than replacing existing ones."
                ),
                destination=dest,
                write_mode=mode,
                evidence=tuple(evidence),
            )
        evidence.append("document_id=derived_from_primary_key")
        return ReplaySafety(
            safe=True,
            mechanism="keyed_document",
            reason=(
                f"{dest} documents are addressed by an id derived from the "
                "primary key, so re-writing a record replaces it."
            ),
            destination=dest,
            write_mode=mode,
            evidence=tuple(evidence),
        )

    return ReplaySafety(
        safe=False,
        mechanism="none",
        reason=(
            f"{dest or 'This destination'} appends without a dedupe key or a "
            "committed-chunk ledger, so a retry after an ambiguous failure "
            "could write a second copy of the batch."
        ),
        destination=dest,
        write_mode=mode,
        evidence=tuple(evidence),
    )


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _as_key_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    try:
        return [str(v) for v in value if str(v).strip()]
    except TypeError:
        return []
