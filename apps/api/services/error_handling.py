"""Error handling for the universal transfer orchestrator.

Implements retriable vs non-retriable classification, bounded exponential
backoff with jitter, retry budgets, quarantine rules, and partial-failure
policies.  The design follows the universal transfer prompt: bounded retries,
quarantine for malformed records, and resume/replay support.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from services.brand_env import getenv_brand


class TransferCancelled(Exception):
    """Raised when a user cancels a running transfer job."""


class FullRefreshDropFailed(Exception):
    """A ``full_refresh`` could not clear the destination before loading.

    Deliberately non-retriable and fatal. Continuing would silently convert an
    overwrite into an append: the previous rows survive, the new rows land on
    top, and the job reports success against a destination that now holds two
    generations of data. Failing the job keeps the destination in a state the
    operator can reason about.
    """

    def __init__(self, table_name: str, reason: str) -> None:
        self.table_name = table_name
        self.reason = reason
        super().__init__(
            f"full_refresh could not clear destination table '{table_name}': {reason}. "
            "Refusing to append onto rows that should have been replaced. "
            "Grant DROP/DELETE on the destination, or switch the sync mode to append."
        )


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
    # Lock-wait is NOT retriable here: a metadata/row lock that already
    # exhausted session lock_wait_timeout will not clear on a quick retry, and
    # outer with_retry × long lock waits is what made 5-row demos hang minutes.
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
    "blocks partial write",
    "migration risk contract",
    "writebatchblocked",
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
    # Contended DDL/DML — fail closed so operators see the lock, not a spinner.
    "lock wait timeout",
    "lock wait timeout exceeded",
    "1205",  # InnoDB lock wait timeout
    "metadata lock",
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
        # First, because a shaping refusal names a column and would otherwise be
        # read as a destination-column error by the driver-text rules below —
        # sending the operator to Map for a decision that belongs to Shape, and
        # inviting a Resume that cannot help.
        ("shaping step",),
        {
            "code": "shape_refused_row",
            "category": "data_quality",
            "confidence": "high",
            "title": "A shaping step refused a row rather than guess at its value",
            "fix": (
                "The recipe's error policy for that step is Refuse, so the run stopped at the "
                "named source row instead of writing a value it could not compute. Open Shape "
                "and either fix the step for that shape of value, or change the step's policy "
                "to Divert (quarantine the row, keep the load) or Null (write null, counted as "
                "information loss). Re-run Validate afterwards: a changed recipe is a different "
                "approved recipe."
            ),
            "primary_action": "open_shape",
        },
    ),
    (
        (
            "lock wait timeout",
            "lock wait timeout exceeded",
            "1205",
            "metadata lock",
        ),
        {
            "code": "destination_lock_timeout",
            "category": "destination",
            "confidence": "high",
            "title": "Destination table is locked",
            "fix": (
                "Close other sessions on this MySQL table (Workbench, Validate probes, "
                "stuck prior jobs), then re-run. Datawrap fails closed instead of waiting "
                "minutes behind a metadata lock."
            ),
        },
    ),
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
            "cdc_binlog_gap",
            "cdc_slot_gap",
            "cdc_ct_gap",
            "cdc_oplog_gap",
            "cdc_cursor_gap",
            "before capture retention",
            "before available redo",
            "min_lsn",
            "min_valid_version",
            "last_sync_version",
            "change tracking",
            "oldest_available",
            "gtid_purged",
            "binary log",
            "binlog",
            "wal_status",
            "replication slot",
            "changestreamhistorylost",
            "resume point may no longer be in the oplog",
            "ora-01291",
            "ora-01292",
        ),
        {
            "code": "cdc_cursor_gap",
            "category": "cdc_ops",
            "confidence": "high",
            "title": "CDC cursor gap (retention / failover)",
            "fix": (
                "If snapshot_mode=when_needed, Resume — the engine snapshots current "
                "source keys then streams from the new tip. initial/never stay fail-closed "
                "until you change mode or reset the watermark. Purged-window events are gone. "
                "Not continuous CDC, not migration_proven."
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
            "incorrect datetime value",
            "(1292,",
            " 1292,",
            "error 1292",
            "er_truncated_value",
        ),
        {
            "code": "mysql_incorrect_datetime",
            "category": "data_type",
            "confidence": "high",
            "title": "MySQL rejected a datetime literal (1292)",
            "fix": (
                "MySQL DATETIME/TIMESTAMP does not accept ISO-8601 with 'T'/'Z' "
                "(e.g. 2026-07-04T06:57:37Z). Datawrap should normalize to a Python "
                "datetime before bind using the destination column type. Confirm the "
                "Map target for that column is DATETIME (not TEXT), then Resume. If this "
                "persists after upgrade, open Validate → review that column's wire form."
            ),
        },
    ),
    (
        (
            "duplicate redis key",
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
                "Datawrap blocked the write because the mapped identity / primary-key column "
                "has duplicate values in the source batch (or a non-unique column was chosen as the key). "
                "This is a source-data check — it happens even when the destination table does not exist yet. "
                "Next step: (1) Open Map and set Primary key to a column that is unique in the source "
                "(`code`, `email`, composite key, … — not a repeating `id`/`capital`); "
                "(2) Or set stream-contract primary_key to that unique column; "
                "(3) If the source truly duplicates that key and you need every row, use append sync "
                "without declaring that column as destination PK, or dedupe upstream. "
                "No rows from the failed batch were committed as a completed transfer."
            ),
            "primary_action": "open_map_primary_key",
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
                "Likely max_connections (or pool) saturation. Reduce concurrent Datawrap jobs "
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
                "Datawrap needs object rows. Accepted shapes: [{...}, ...], a wrapper like "
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
    (
        (
            "unknown column",
            "undefined column",
            "column does not exist",
            'column "',
            "column '",
            "no such column",
            "invalid column name",
            "unrecognized name",
            "er_bad_field_error",
            "(1054,",
            "error 1054",
        ),
        {
            "code": "destination_column_missing",
            "category": "schema_mismatch",
            "confidence": "high",
            "title": "Write referenced a column that is not on the destination",
            "fix": (
                "Open Map and rematch the failing field, or run Validate again after the "
                "destination schema reload. If this is create-new, confirm Destination shows "
                "Table not found (create on write) — not Existing table with foreign columns. "
                "Do not Resume until Map/Validate agree with the live destination DDL."
            ),
            "primary_action": "open_map",
        },
    ),
    (
        (
            "relation does not exist",
            'relation "',
            "table doesn't exist",
            "table does not exist",
            "doesn't exist",
            "no such table",
            "unknown table",
            "er_no_such_table",
            "(1146,",
            "error 1146",
            "invalid object name",
        ),
        {
            "code": "destination_table_missing",
            "category": "schema_mismatch",
            "confidence": "high",
            "title": "Destination table/relation was not found at write time",
            "fix": (
                "Confirm Database + Table on Destination (same namespace Validate probed). "
                "If the table should be created, Destination must show create-on-write and "
                "the connector role needs CREATE. If it should already exist, pick it from "
                "the table list — do not rely on a name that only exists in another database "
                "on the same host."
            ),
            "primary_action": "open_destination",
        },
    ),
    (
        (
            "access denied",
            "permission denied",
            "insufficient privilege",
            "not authorized",
            "authorization failed",
            "er_accessdenied_error",
            "(1045,",
            "(1142,",
            "error 1142",
        ),
        {
            "code": "destination_permission_denied",
            "category": "destination_privileges",
            "confidence": "medium",
            "title": "Destination rejected the write for privileges/auth",
            "fix": (
                "Re-test the connector, then confirm the role can INSERT/UPDATE (and CREATE "
                "if this is create-new). Validate G2 privilege notes should list what failed — "
                "fix grants before Resume."
            ),
        },
    ),
    (
        (
            "no module named expat",
            "pyexpat",
            "xml_setalloctrackeractivationthreshold",
            "simplexmltreebuilder",
        ),
        {
            "code": "python_xml_runtime_broken",
            "category": "runtime",
            "confidence": "high",
            "title": "Python XML runtime (pyexpat) cannot load",
            "fix": (
                "S3/GCS/ADLS clients need a working pyexpat (botocore parses XML). "
                "On macOS Homebrew Python, a libexpat mismatch is common — reinstall "
                "python@3.12 + expat, or run with DYLD_LIBRARY_PATH pointing at "
                "Homebrew's libexpat before starting the API. This is an environment "
                "gap, not a Datawrap mapping/schema failure."
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


def _cursor_gap_fix(error: Any) -> str:
    """Operator next step from the snapshot plan, not a generic reset slogan."""
    plan = getattr(error, "snapshot_plan", None) or {}
    action = str(plan.get("next_action") or "")
    mode = str(plan.get("snapshot_mode") or "")
    if action == "set_when_needed" or mode == "never":
        return (
            "Set snapshot_mode=when_needed, then Resume. The engine will snapshot "
            "current source keys and stream from the new tip. snapshot_mode=never "
            "forbids a recovery snapshot. Purged-window events are gone — not "
            "continuous CDC, not migration_proven."
        )
    if mode in {"when_needed", "always", "initial_only"}:
        return (
            "Resume — engine will snapshot current source keys, then stream from "
            "the new tip. Purged-window events are gone. At-least-once upsert, not "
            "continuous CDC, not migration_proven."
        )
    if mode == "initial" or action == "set_when_needed":
        return (
            "snapshot_mode=initial will not snapshot again. Reset the CDC watermark "
            "or set when_needed, then Resume. Purged-window events are gone."
        )
    return (
        "If snapshot_mode=when_needed, Resume — the engine snapshots current source "
        "keys then streams from the new tip. Otherwise reset the watermark or set "
        "when_needed. Purged-window events are gone. Not continuous CDC, not "
        "migration_proven."
    )


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
    if isinstance(error, FullRefreshDropFailed):
        lockish = any(
            tok in raw.lower()
            for tok in ("lock wait", "1205", "metadata lock", "try restarting transaction")
        )
        return {
            "code": "full_refresh_drop_failed",
            "category": "destination",
            "title": (
                "Destination table is locked — could not clear for full refresh"
                if lockish
                else "Could not clear the destination for full refresh"
            ),
            "message": raw,
            "fix": (
                (
                    f"Another session is holding a lock on '{error.table_name}' "
                    "(common: an open Validate probe, MySQL Workbench, or a stuck "
                    "prior job). Close those connections, then re-run. Datawrap "
                    "refused to append onto uncleared rows — that would silently "
                    "double the destination."
                )
                if lockish
                else (
                    f"Grant DROP (or DELETE) on destination table '{error.table_name}', "
                    "confirm no competing lock is holding it, then re-run. "
                    "Datawrap refused to append onto rows that should have been replaced — "
                    "continuing would have silently doubled the destination."
                )
            ),
            "raw": raw,
            "retriable": False,
            "confidence": "high",
            "table": error.table_name,
        }

    if isinstance(error, AmbiguousWriteOutcome):
        return {
            "code": "ambiguous_write_outcome",
            "category": "destination",
            "title": "Write interrupted with an unknown outcome",
            "message": raw,
            "fix": (
                "Resume this job to continue from the last committed chunk. "
                "Datawrap stopped instead of re-sending the batch because this "
                "destination cannot deduplicate a replay, and retrying could "
                "have written a second copy of those rows. To make retries "
                "automatic, switch the sync mode to upsert with a primary key."
            ),
            "raw": raw,
            "retriable": False,
            "confidence": "high",
            "replay_safety": (
                error.safety.to_dict() if hasattr(error.safety, "to_dict") else {}
            ),
        }

    if (
        "no durable checkpoint to resume" in text
        or "restart-from-zero would duplicate" in text
    ):
        return {
            "code": "resume_without_checkpoint",
            "category": "execution",
            "title": "Resume needs a committed checkpoint",
            "message": raw,
            "fix": (
                "This job has no durable progress (typically 0 rows written). "
                "Re-run from Validate or start a new transfer — do not Resume. "
                "After a deploy/restart, workers restart zero-progress jobs from "
                "the beginning instead of false-failing Resume on append/Excel."
            ),
            "raw": raw,
            "retriable": False,
            "confidence": "high",
        }

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
                    "Datawrap refuses concurrent consumers — delivery stays at-least-once."
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
                "fix": _cursor_gap_fix(error),
                "raw": raw,
                "retriable": False,
                "confidence": "high",
                "cursor_key": error.cursor_key,
                "resume": error.resume,
                "retained": error.retained,
                "dialect": error.dialect,
            }
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
        elif matched.get("code") == "mysql_incorrect_datetime":
            message = (
                f"{title}. Driver reported: {raw}. "
                "ISO 'T'/'Z' literals must be normalized before MySQL DATETIME bind."
            )
        elif matched.get("code") == "duplicate_primary_key":
            message = (
                f"{title}. Driver reported: {raw}. "
                "Open Map and set Primary key to a column that is unique in the source "
                "(or use append without that PK / dedupe upstream) before Resume."
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
    if "circuit_breaker_open" in text or (
        "circuit breaker" in text and "is open" in text
    ):
        return {
            "code": "circuit_breaker_open",
            "category": "contract",
            "title": "Contract circuit breaker is OPEN",
            "message": raw,
            "fix": (
                "Reset the breaker after you fix the contract violation "
                "(Contracts or Validate bind), then re-run. "
                "Do not enqueue while the breaker is OPEN."
            ),
            "raw": raw,
            "retriable": False,
            "confidence": "high",
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
    max_attempts: int = field(default_factory=lambda: int(getenv_brand("RETRY_MAX_ATTEMPTS", "3")))
    base_delay_seconds: float = field(default_factory=lambda: float(getenv_brand("RETRY_BASE_DELAY_SECONDS", "1.0")))
    max_delay_seconds: float = field(default_factory=lambda: float(getenv_brand("RETRY_MAX_DELAY_SECONDS", "60.0")))
    exponential_base: float = field(default_factory=lambda: float(getenv_brand("RETRY_EXPONENTIAL_BASE", "2.0")))
    jitter: bool = field(default_factory=lambda: getenv_brand("RETRY_JITTER", "true").lower() in ("1", "true", "yes"))
    budget_used: float = 0.0
    attempts_made: int = 0

    def next_delay(self) -> float:
        """Return the next delay, updating internal state."""
        delay = self.base_delay_seconds * (self.exponential_base ** self.attempts_made)
        delay = min(delay, self.max_delay_seconds)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)  # nosec B311
        self.attempts_made += 1
        self.budget_used += delay
        return delay

    def has_budget(self) -> bool:
        return self.attempts_made < self.max_attempts


# Server-directed backoff cap. Salesforce/HubSpot/Graph can ask for minutes;
# honour that, but never let one header park a worker indefinitely.
_RETRY_AFTER_CAP_SECONDS = float(getenv_brand("RETRY_AFTER_MAX_SECONDS", "300.0"))


def retry_after_seconds(error: Exception) -> float | None:
    """Server-directed wait from a throttled HTTP response, in seconds.

    Salesforce (REQUEST_LIMIT_EXCEEDED), HubSpot, and Microsoft Graph all answer
    429/503 with ``Retry-After``. Ignoring it and using our own exponential
    backoff re-sends inside the penalty window, which is what turns a throttle
    into a multi-hour stall on large SaaS migrations. Supports both the
    delta-seconds and HTTP-date forms of RFC 9110 ``Retry-After``.
    """
    headers = getattr(getattr(error, "response", None), "headers", None)
    if not headers:
        return None
    try:
        raw = headers.get("Retry-After") or headers.get("retry-after")
    except Exception:  # noqa: BLE001 - header mapping may be exotic
        return None
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return max(0.0, min(float(text), _RETRY_AFTER_CAP_SECONDS))
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(text)
    except Exception:  # noqa: BLE001 - malformed header is not fatal
        return None
    if when is None:
        return None
    from datetime import datetime, timezone

    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta = (when - datetime.now(timezone.utc)).total_seconds()
    if delta <= 0:
        return 0.0
    return min(delta, _RETRY_AFTER_CAP_SECONDS)


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

    # A full_refresh that could not clear the destination must never be retried
    # by the generic write wrapper: attempt two would append onto the rows
    # attempt one failed to remove. Fail the job and let the operator fix the
    # grant or change the sync mode.
    if isinstance(error, FullRefreshDropFailed):
        return {
            "retriable": False,
            "evidence": ["full_refresh_drop_failed"],
            "message": text,
            "class": exc_name,
            "table": error.table_name,
        }

    # Already refused a replay once; an outer wrapper must not undo that call.
    if isinstance(error, AmbiguousWriteOutcome):
        return {
            "retriable": False,
            "evidence": ["ambiguous_write_outcome"],
            "message": text,
            "class": exc_name,
        }

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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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


class AmbiguousWriteOutcome(Exception):
    """A write failed in a way that may have partially landed, and cannot be replayed.

    Raised instead of retrying when the destination has no way to deduplicate a
    replayed batch. Failing here is deliberate: the job resumes from the durable
    chunk checkpoint, which restarts at a known boundary, rather than re-sending
    a batch that may already be in the destination.
    """

    def __init__(self, cause: BaseException, safety: Any) -> None:
        self.cause = cause
        self.safety = safety
        reason = getattr(safety, "reason", "") or ""
        super().__init__(
            f"Write failed with an unknown outcome and cannot be safely retried: "
            f"{cause}. {reason} Resume the job to continue from the last "
            f"committed chunk."
        )


def with_retry(
    fn: Callable[[], Any],
    *,
    budget: RetryBudget | None = None,
    on_transient: Callable[[Exception, float], None] | None = None,
    replay_safety: Any | None = None,
    on_replay_blocked: Callable[[Exception, Any], None] | None = None,
) -> Any:
    """Run a function with bounded retry and backoff.

    ``replay_safety`` is a ``services.replay_safety.ReplaySafety`` verdict for a
    destination write. When it reports that a replay could duplicate rows, an
    ambiguous failure stops the retry loop instead of re-sending the batch. A
    failed job that resumes cleanly is recoverable; a job that reports success
    with duplicated rows is not.

    Reads and other side-effect-free work pass no verdict and retry as before.
    """
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
            if replay_safety is not None and not replay_safety.allows_retry(exc):
                if on_replay_blocked:
                    try:
                        on_replay_blocked(exc, replay_safety)
                    except Exception as hook_exc:  # noqa: BLE001
                        logging.getLogger(__name__).warning(
                            "replay-blocked hook failed: %s", hook_exc
                        )
                raise AmbiguousWriteOutcome(exc, replay_safety) from exc
            delay = budget.next_delay()
            server_hint = retry_after_seconds(exc)
            if server_hint is not None:
                delay = max(delay, server_hint)
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
