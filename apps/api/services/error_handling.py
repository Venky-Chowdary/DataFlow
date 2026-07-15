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
