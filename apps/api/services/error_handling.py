"""Error handling for the universal transfer orchestrator.

Implements retriable vs non-retriable classification, bounded exponential
backoff with jitter, retry budgets, quarantine rules, and partial-failure
policies.  The design follows the universal transfer prompt: bounded retries,
quarantine for malformed records, and resume/replay support.
"""

from __future__ import annotations

import os
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable


class TransferCancelled(Exception):
    """Raised when a user cancels a running transfer job."""


# Retriable exceptions are transient: network, rate limit, lock, timeout, etc.
RETRIABLE_EXCEPTIONS: set[str] = {
    "connectionerror",
    "connectionfailure",
    "connectionrefusederror",
    "newconnectionerror",
    "operationalerror",
    "interfaceerror",
    "autoreconnect",
    "serverselectiontimeouterror",
    "readtimeout",
    "connecttimeout",
    "timeouterror",
    "timeout",
    "transienterror",
    "temporarily unavailable",
    "too many requests",
    "rate limit",
    "deadlock",
    "lock wait timeout",
    "unable to acquire lock",
    "503",
    "502",
    "504",
    "429",
    "408",
    "500",  # some 500s are transient; service specific below
    "throttlingexception",
    "provisionedthroughputexceeded",
    "slowdown",
    "internalservererror",
    "serviceunavailable",
    "busy",
    "quota exceeded",
    "try again",
    "temporary failure in name resolution",
    "network",
    "unreachable",
    "refused",
    # Public proxy / TLS drops wrapped as RuntimeError by writers
    "server closed the connection",
    "connection reset",
    "broken pipe",
    "ssl syscall error",
    "ssl connection has been closed",
    "eof detected",
    "connection already closed",
    "lost connection",
    "server has gone away",
    "terminating connection",
}

# Non-retriable errors indicate a data or contract problem that will not fix itself.
NON_RETRIABLE_PATTERNS: set[str] = {
    "constraint",
    "duplicate",
    "unique",
    "foreign key",
    "not null",
    "violat",
    "invalid",
    "parse",
    "datatype",
    "data type",
    "value too long",
    "width overflow",
    "lossy",
    "cannot coerce",
    "access denied",
    "permission denied",
    "unauthorized",
    "forbidden",
    "authentication",
    "credential",
    "not found",
    "nosuch",
    "does not exist",
    "already exists",
    "exist",
    "unknown host",
    "name or service not known",
    "invalid bucket",
    "invalid database",
    "malformed",
    "serialization",
    "schema",
    "partial write",
    "refuse concurrent consumer",
    "cdc lease",
    "cdc_lease_conflict",
    # Destination capacity — OperationalError-class but will not self-heal on retry.
    "table is full",
    "er_record_file_full",
    "1114",
    "disk full",
    "no space left",
    "enospc",
    "tablespace is full",
    # Source format — will not self-heal on retry without a new file.
    "json file must be an array",
    "json must be an array of objects",
    "json array must contain objects",
    "json file has no object rows",
    "invalid json",
}


# Operator-facing failure catalog. Only patterns we can map accurately.
# `fix` must list *likely checks* — never a single guaranteed remedy.
_OPERATOR_FAILURE_RULES: tuple[tuple[tuple[str, ...], dict[str, str]], ...] = (
    (
        ("cdc_lease_conflict", "cdc lease conflict", "refuse concurrent consumer", "cdc resource"),
        {
            "code": "cdc_lease_conflict",
            "category": "cdc_ops",
            "confidence": "high",
            "title": "CDC lease conflict",
            "fix": (
                "Another worker holds this CDC resource. Stop the holder, wait for TTL expiry, "
                "or Force-release the lease in Job Theater (fencing generation advances), then "
                "Resume / re-run. Never run two consumers on the same slot or server_id."
            ),
        },
    ),
    (
        (
            "cdc_lsn_gap",
            "cdc_scn_gap",
            "cdc_cursor_gap",
            "before capture retention",
            "before available redo",
            "min_lsn",
            "oldest_available",
            "ora-01291",
            "ora-01292",
        ),
        {
            "code": "cdc_cursor_gap",
            "category": "cdc_ops",
            "confidence": "high",
            "title": "CDC cursor gap (retention / failover)",
            "fix": (
                "Reset the CDC watermark, set snapshot to when_needed or initial, then re-run. "
                "Continuous CDC across the gap is not possible."
            ),
        },
    ),
    (
        ("allow_append_only", "append-only (or missing pk", "cdc destination"),
        {
            "code": "cdc_append_only_sink",
            "category": "cdc_ops",
            "confidence": "high",
            "title": "Append-only CDC sink blocked",
            "fix": (
                "Use a PK upsert destination, or enable Allow append-only CDC in Destination Advanced."
            ),
        },
    ),
    (
        ("table is full", "er_record_file_full", "(1114,", " 1114,", "error 1114"),
        {
            "code": "destination_table_full",
            "category": "destination_capacity",
            "confidence": "high",
            "title": "Destination table is full (MySQL 1114)",
            "fix": (
                "MySQL ER_RECORD_FILE_FULL (1114) means the engine could not allocate more space "
                "for this table. Common verified causes: host disk full, InnoDB tablespace / "
                "innodb_data_file_path limit, MEMORY/HEAP max size, or MyISAM max_rows. Check "
                "which applies on your host (SHOW TABLE STATUS / disk / tablespace), free or "
                "expand capacity, then Resume from checkpoint. Resume alone will fail again "
                "until capacity is available."
            ),
        },
    ),
    (
        (
            "duplicate primary key",
            "keys repeat",
            "duplicate key values",
            "failed data-quality audit: duplicate",
        ),
        {
            "code": "duplicate_primary_key",
            "category": "data_quality",
            "confidence": "high",
            "title": "Duplicate identity-key values in a write batch",
            "fix": (
                "DataFlow blocked the batch because the identity key repeats inside it "
                "(same rule Validate uses: `_id` on Mongo/Redis/Dynamo; `id`/`_id` on SQL). "
                "Business fields named `id` that are not the document key are not treated as "
                "primary keys on schemaless routes. To proceed: (1) Open Validate — if this "
                "was a false `id` block on Mongo→Redis, restart API and Resume; (2) If the "
                "identity key truly duplicates, dedupe the source or switch sync mode to "
                "upsert with that key; (3) Or set an explicit stream-contract primary_key. "
                "No rows from the failed batch were committed."
            ),
        },
    ),
    (
        ("disk full", "no space left", "enospc"),
        {
            "code": "destination_disk_full",
            "category": "destination_capacity",
            "confidence": "high",
            "title": "Destination reported no free disk space",
            "fix": (
                "The driver reported ENOSPC / disk full. Free space on the destination host "
                "(or expand the volume), confirm the write path is not a full mount, then Resume. "
                "If the message was wrapped by a proxy, confirm on the DB host before assuming disk."
            ),
        },
    ),
    (
        ("tablespace is full", "innodb: error: tablespace"),
        {
            "code": "destination_tablespace_full",
            "category": "destination_capacity",
            "confidence": "high",
            "title": "Destination tablespace is full",
            "fix": (
                "InnoDB tablespace is exhausted. Expand the tablespace / data file, free space "
                "inside it, or move the table — then Resume. Do not treat this as a mapping issue."
            ),
        },
    ),
    (
        ("too many connections", "max_connections"),
        {
            "code": "destination_connection_limit",
            "category": "destination_capacity",
            "confidence": "medium",
            "title": "Destination connection limit reached",
            "fix": (
                "Likely max_connections (or pool) saturation. Reduce concurrent DataFlow jobs "
                "or raise the destination limit, then retry. Confirm with the DB admin if shared."
            ),
        },
    ),
    (
        (
            "json file must be an array of objects",
            "json must be an array of objects",
            "json array must contain objects",
            "json file has no object rows",
            "json must be an array of objects — each record",
        ),
        {
            "code": "json_shape_unsupported",
            "category": "source_format",
            "confidence": "high",
            "title": "JSON source shape is not tabular",
            "fix": (
                "DataFlow needs object rows. Accepted shapes: [{...}, ...], a wrapper like "
                '{"data":[{...}]} / {"countries":[{...}]} / GeoJSON {"features":[...]}, or one '
                "object as a single row. Arrays of strings/numbers, empty files, and invalid JSON "
                "are rejected. Re-export the file in one of those shapes, re-upload, then re-run "
                "from Source (Resume will not help if extract never started)."
            ),
        },
    ),
    (
        (
            "decimal.overflow",
            "[<class 'decimal.Overflow'>]",
            "[<class 'decimal.Overflow'>",
            "exceeded safe decimal capacity",
            "would raise decimal.Overflow",
        ),
        {
            "code": "decimal_overflow",
            "category": "data_type",
            "confidence": "high",
            "title": "Numeric value overflowed decimal capacity",
            "fix": (
                "A number was too large (or had too many digits) for the decimal path used on "
                "this transfer — common with extreme scientific notation or oversized Decimal128. "
                "Open Quarantine / the event log for the column, map overflow fields to string, "
                "or coerce null under a quarantine policy, then Resume from checkpoint. "
                "This is not a Redis/Snowflake-only issue; the same rule applies on every dest."
            ),
        },
    ),
    (
        (
            'dataflow."public"',
            '."public"',
            'schema "public"',
            "schema 'public'",
        ),
        {
            "code": "snowflake_schema_not_found",
            "category": "source_schema",
            "confidence": "high",
            "title": "Snowflake schema not found (check PUBLIC vs public)",
            "fix": (
                "Snowflake folds unquoted names to UPPERCASE. A quoted lowercase schema "
                '"public" is not the same as PUBLIC. Set the connector schema to PUBLIC '
                "(or the real schema name in uppercase), confirm the role can USE that "
                "schema, then reload sample preview. If the schema truly is missing, create "
                "it or pick an existing schema your role can access."
            ),
        },
    ),
)


def format_exception_message(error: Exception | str) -> str:
    """Stable operator-facing raw text — never empty or bare ``[<class '...'>]``."""
    if isinstance(error, str):
        text = error.strip()
        if text and not text.startswith("[<class"):
            return text
        if "Overflow" in text or "overflow" in text.lower():
            return (
                "decimal.Overflow: a numeric value exceeded safe decimal capacity "
                "(extreme scientific notation or oversized Decimal128)"
            )
        return text or "unknown error"
    name = type(error).__name__
    module = getattr(type(error), "__module__", "") or ""
    msg = str(error).strip()
    if name == "Overflow" or (module == "decimal" and name == "Overflow"):
        if not msg or msg.startswith("[<class"):
            return (
                "decimal.Overflow: a numeric value exceeded safe decimal capacity "
                "(extreme scientific notation or oversized Decimal128)"
            )
        return f"decimal.Overflow: {msg}"
    if not msg or msg.startswith("[<class"):
        return f"{module}.{name}" if module else name
    return msg


def humanize_transfer_failure(error: Exception | str) -> dict[str, Any]:
    """Turn a raw driver exception into an operator-facing failure summary.

    Honesty rules
    -------------
    - Only attach a concrete ``fix`` when the pattern is a known driver signal.
    - Phrase fixes as *likely checks*, never as a guaranteed one-click remedy.
    - Unknown errors keep the raw message and a neutral next-step — no invented root cause.

    Returns keys: code, category, title, message, fix, raw, retriable, confidence.
    """
    raw = format_exception_message(error)
    text = raw.lower()
    # Type-aware match when str(exc) is empty (decimal.Overflow).
    if isinstance(error, Exception) and type(error).__name__ == "Overflow":
        text = f"decimal.overflow {text}"
    try:
        from services.cdc_lease import CdcLeaseConflict

        if isinstance(error, CdcLeaseConflict):
            holder = error.holder_id or "another worker"
            resource = error.resource or "CDC resource"
            return {
                "code": "cdc_lease_conflict",
                "category": "cdc_ops",
                "title": "CDC lease conflict",
                "message": (
                    f"Another worker holds {resource!r} (holder {holder}). "
                    "DataFlow refuses concurrent consumers — delivery stays at-least-once."
                ),
                "fix": (
                    "Stop or wait for the holder job, or Force-release the lease in Job Theater "
                    "(fencing generation advances). Then Resume / re-run — do not run two "
                    "consumers on the same slot or server_id."
                ),
                "raw": raw,
                "retriable": False,
                "confidence": "high",
                "holder_id": error.holder_id,
                "resource": error.resource,
                "cursor_key": error.cursor_key,
            }
    except Exception:
        pass
    try:
        from services.cdc_cursor_gap import CdcCursorGapError

        if isinstance(error, CdcCursorGapError):
            dialect = error.dialect or "source"
            return {
                "code": error.code or "cdc_cursor_gap",
                "category": "cdc_ops",
                "title": "CDC cursor gap (retention / failover)",
                "message": (
                    f"{dialect} CDC resume is before retained log history "
                    f"(resume={error.resume or '?'}, retained={error.retained or '?'}). "
                    "Continuous CDC across the gap is not possible."
                ),
                "fix": (
                    "Reset the CDC watermark in Job Theater, set snapshot mode to "
                    "when_needed or initial, then re-run. Do not claim continuous CDC "
                    "across an AG / Data Guard / archive-purge gap."
                ),
                "raw": raw,
                "retriable": False,
                "confidence": "high",
                "cursor_key": error.cursor_key,
                "resume": error.resume,
                "retained": error.retained,
                "dialect": error.dialect,
            }
    except Exception:
        pass
    try:
        from services.cdc_effectively_once import CdcAppendOnlySinkError

        if isinstance(error, CdcAppendOnlySinkError):
            return {
                "code": "cdc_append_only_sink",
                "category": "cdc_ops",
                "title": "Append-only CDC sink blocked",
                "message": str(error),
                "fix": (
                    "Choose a destination that supports PK upsert, or enable "
                    "Allow append-only CDC in Destination Advanced (acknowledges "
                    "duplicate rows on redelivery). Exactly-once is not claimed."
                ),
                "raw": raw,
                "retriable": False,
                "confidence": "high",
            }
    except Exception:
        pass
    classification = classify_error(error)
    matched: dict[str, str] | None = None
    for needles, payload in _OPERATOR_FAILURE_RULES:
        if any(n in text for n in needles):
            matched = dict(payload)
            break
    if matched:
        title = matched["title"]
        fix = matched["fix"]
        confidence = matched.get("confidence", "medium")
        if matched.get("code") == "decimal_overflow":
            message = (
                f"{title}. Driver reported: {raw}. "
                f"Rows already written remain; fix the overflow column then Resume."
            )
        elif matched.get("code") == "cdc_lease_conflict":
            message = (
                f"{title}. {raw}. "
                "Concurrent CDC consumers are blocked — release or wait for the holder, then retry."
            )
        else:
            message = (
                f"{title}. Driver reported: {raw}. "
                f"Rows already written stay on the destination until you address capacity."
            )
        return {
            "code": matched["code"],
            "category": matched["category"],
            "title": title,
            "message": message,
            "fix": fix,
            "raw": raw,
            "retriable": False,
            "confidence": confidence,
        }
    # Unknown — never invent a specific root cause or fake remediation path.
    return {
        "code": "transfer_failed",
        "category": "runtime",
        "title": "Transfer failed",
        "message": raw,
        "fix": (
            "No mapped remediation for this driver message. Use the raw error, event log, and "
            "Quarantine tab to identify the cause. Fix that cause before Resume — do not assume "
            "a mapping or capacity issue without evidence."
        ),
        "raw": raw,
        "retriable": bool(classification.get("retriable")),
        "confidence": "low",
    }


@dataclass
class RetryBudget:
    max_attempts: int = field(default_factory=lambda: int(os.getenv("DATAFLOW_RETRY_MAX_ATTEMPTS", "3")))
    base_delay_seconds: float = field(default_factory=lambda: float(os.getenv("DATAFLOW_RETRY_BASE_DELAY_SECONDS", "1.0")))
    max_delay_seconds: float = field(default_factory=lambda: float(os.getenv("DATAFLOW_RETRY_MAX_DELAY_SECONDS", "60.0")))
    exponential_base: float = field(default_factory=lambda: float(os.getenv("DATAFLOW_RETRY_EXPONENTIAL_BASE", "2.0")))
    jitter: bool = field(default_factory=lambda: os.getenv("DATAFLOW_RETRY_JITTER", "true").lower() in ("1", "true", "yes"))
    budget_used: float = 0.0
    attempts_made: int = 0

    def next_delay(self) -> float:
        """Return the next delay, updating internal state."""
        delay = self.base_delay_seconds * (self.exponential_base ** self.attempts_made)
        delay = min(delay, self.max_delay_seconds)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        self.attempts_made += 1
        self.budget_used += delay
        return delay

    def has_budget(self) -> bool:
        return self.attempts_made < self.max_attempts


@dataclass
class QuarantineRecord:
    row_index: int = 0
    source_record: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    stage: str = ""
    error: str = ""
    timestamp: str = ""


def classify_error(error: Exception | str) -> dict[str, Any]:
    """Classify an error as retriable or non-retriable with evidence."""
    text = str(error).lower()
    if isinstance(error, Exception):
        exc_name = type(error).__name__.lower()
    else:
        exc_name = ""
    retriable = False
    evidence: list[str] = []

    # Structured CDC lease conflict — never auto-retry into a live holder.
    try:
        from services.cdc_lease import CdcLeaseConflict

        if isinstance(error, CdcLeaseConflict):
            return {
                "retriable": False,
                "evidence": ["cdc_lease_conflict"],
                "message": text,
                "class": exc_name,
                **error.to_dict(),
            }
    except Exception:
        pass

    for sig in RETRIABLE_EXCEPTIONS:
        if sig in text or sig in exc_name:
            retriable = True
            evidence.append(f"matched retriable signal: {sig}")

    for pattern in NON_RETRIABLE_PATTERNS:
        if pattern in text:
            retriable = False
            evidence.append(f"matched non-retriable pattern: {pattern}")

    # HTTP status code heuristics
    status_match = re.search(r"\b(4\d\d|5\d\d)\b", text)
    if status_match:
        code = int(status_match.group(1))
        if code in {408, 429, 500, 502, 503, 504}:
            # 500 is retriable only if no contract violation evidence is present
            if code == 500 and any(p in text for p in NON_RETRIABLE_PATTERNS):
                retriable = False
                evidence.append("500 but non-retriable pattern present")
            else:
                retriable = True
                evidence.append(f"HTTP {code} is retriable")
        else:
            retriable = False
            evidence.append(f"HTTP {code} is non-retriable")

    return {
        "retriable": retriable,
        "evidence": evidence,
        "message": text,
        "class": exc_name,
    }


def with_retry(
    fn: Callable[[], Any],
    *,
    budget: RetryBudget | None = None,
    on_transient: Callable[[Exception, float], None] | None = None,
) -> Any:
    """Run a function with bounded retry and backoff.  Returns the result or raises the last error."""
    budget = budget or RetryBudget()
    last_error: Exception | None = None
    while budget.has_budget():
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            classification = classify_error(exc)
            if not classification["retriable"]:
                raise
            delay = budget.next_delay()
            if on_transient:
                on_transient(exc, delay)
            time.sleep(delay)
    raise last_error or RuntimeError("Retry budget exhausted")


def quarantine_record(
    record: dict[str, Any],
    reason: str,
    stage: str,
    error: str | None = None,
    row_index: int = 0,
) -> QuarantineRecord:
    """Create a quarantine record for a malformed/invalid row."""
    from datetime import datetime, timezone
    return QuarantineRecord(
        row_index=row_index,
        source_record=record,
        reason=reason,
        stage=stage,
        error=error or "",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def should_quarantine(
    *,
    error: Exception | str,
    row_index: int,
    max_quarantine: int | None = None,
    current_quarantine_count: int = 0,
) -> bool:
    """Return True if the row should be routed to the quarantine bucket."""
    if max_quarantine is not None and current_quarantine_count >= max_quarantine:
        return False
    classification = classify_error(error)
    return not classification["retriable"]


def build_error_report(
    errors: list[dict[str, Any]],
    quarantine: list[QuarantineRecord] | None = None,
) -> dict[str, Any]:
    """Summarize errors for the observability and UI layer."""
    retriable = [e for e in errors if e.get("retriable")]
    non_retriable = [e for e in errors if not e.get("retriable")]
    return {
        "retriable_count": len(retriable),
        "non_retriable_count": len(non_retriable),
        "quarantine_count": len(quarantine or []),
        "retriable_examples": retriable[:3],
        "non_retriable_examples": non_retriable[:3],
        "quarantine_examples": [q.__dict__ for q in (quarantine or [])[:3]],
    }
