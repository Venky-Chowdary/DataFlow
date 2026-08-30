"""Why a log-based CDC reader could not attach, and whether the run may degrade.

Query CDC (``WHERE cursor > watermark``) cannot observe a DELETE and cannot see
a row that was written and overwritten between two polls. Substituting it for
log capture therefore changes the guarantee the operator asked for, so the
substitution is only defensible when the *server* was never configured to emit
a change log — an operator decision DataFlow cannot repair mid-run. When the
server does emit a log and only our attach failed (slot quota, missing
REPLICATION grant), the same run would silently stop carrying deletes: that is
data divergence and must fail closed with the remedy.

One owner for that classification so every dialect answers it the same way.
"""

from __future__ import annotations

from dataclasses import dataclass

CAUSE_SERVER_NOT_CONFIGURED = "server_not_configured"
CAUSE_SLOT_QUOTA = "replication_slot_quota"
CAUSE_PRIVILEGE = "insufficient_privilege"
CAUSE_DRIVER_MISSING = "driver_missing"
CAUSE_SOURCE_UNREACHABLE = "source_unreachable"
# A MongoDB delete event carries only ``documentKey`` (``_id``). When the
# pipeline's identity is a business key, the deleted row cannot be named without
# the collection's pre-image, so the delete is unappliable — never droppable.
CAUSE_MONGO_PREIMAGE_DISABLED = "mongo_pre_images_disabled"
CAUSE_UNKNOWN = "unknown"

# Causes that keep a log-capture stream unrepairable by DataFlow but are a
# deliberate server configuration: degrade to query CDC with the loss declared.
_DEGRADABLE = frozenset({CAUSE_SERVER_NOT_CONFIGURED, CAUSE_DRIVER_MISSING})


@dataclass(frozen=True)
class LogCaptureRefusal:
    """A log-based CDC reader that would not open, classified for the operator."""

    cause: str
    detail: str
    remedy: str

    @property
    def fail_closed(self) -> bool:
        """True when degrading to query CDC would silently drop deletes."""
        return self.cause not in _DEGRADABLE

    def as_fields(self, dialect: str) -> dict[str, str | bool]:
        """Summary/theater fields describing the capture actually used."""
        return {
            "cdc_capture_requested": "log_based",
            "cdc_capture_used": "query_cursor",
            "cdc_capture_downgraded": True,
            "cdc_capture_downgrade_cause": self.cause,
            "cdc_capture_downgrade_detail": self.detail[:500],
            "cdc_capture_downgrade_remedy": self.remedy,
            "cdc_capture_dialect": str(dialect or ""),
            # Query CDC has no delete event and no intra-poll version history.
            "cdc_delete_capture": False,
        }

    def message(self, dialect: str) -> str:
        """Fail-closed error text: what was refused, why, and how to fix it."""
        return (
            f"{dialect or 'source'} log-based CDC could not attach "
            f"({self.cause}): {self.detail} — refusing to fall back to query CDC "
            "because a cursor poll cannot observe DELETEs, so the destination "
            f"would silently diverge. {self.remedy}"
        )


class LogCaptureUnavailable(RuntimeError):
    """Raised in-branch so the query-CDC fallback can read the classification."""

    def __init__(self, refusal: LogCaptureRefusal, dialect: str) -> None:
        super().__init__(refusal.message(dialect))
        self.refusal = refusal
        self.dialect = dialect


_QUOTA_MARKERS = (
    "all replication slots are in use",
    "max_replication_slots",
    "max_wal_senders",
    "too many wal senders",
    "number of requested standby connections exceeds",
)
_PRIVILEGE_MARKERS = (
    "must be superuser",
    "permission denied",
    "insufficient privilege",
    "not allowed",
    "replication privilege",
    "access denied",
    "replication slave",
    "replication client",
    "authentication failed",
    "no pg_hba.conf entry",
)
_UNREACHABLE_MARKERS = (
    "could not connect",
    "connection refused",
    "timeout expired",
    "timed out",
    "can't connect",
    "name or service not known",
    "no route to host",
    "server closed the connection unexpectedly",
)
_NOT_CONFIGURED_MARKERS = (
    "wal_level",
    "logical decoding",
    "log_bin",
    "binlog_format",
    "binlog_row_image",
    "binary logging",
    "supplemental logging",
    "change data capture is not enabled",
    "cdc is not enabled",
)


def classify_log_capture_failure(
    dialect: str,
    detail: str,
    *,
    server_log_enabled: bool | None = None,
) -> LogCaptureRefusal:
    """Classify why log capture failed.

    ``server_log_enabled`` is the reader's own reading of the server switch
    (``wal_level=logical``, ``log_bin=ON``). ``False`` is authoritative: the
    server does not emit a change log, whatever else the error text says.
    """
    text = str(detail or "").strip()
    low = text.lower()
    if server_log_enabled is False:
        return LogCaptureRefusal(
            CAUSE_SERVER_NOT_CONFIGURED,
            text or f"{dialect} is not configured to emit a change log",
            _remedy(dialect, CAUSE_SERVER_NOT_CONFIGURED),
        )
    if any(m in low for m in _QUOTA_MARKERS):
        cause = CAUSE_SLOT_QUOTA
    elif any(m in low for m in _PRIVILEGE_MARKERS):
        cause = CAUSE_PRIVILEGE
    elif any(m in low for m in _UNREACHABLE_MARKERS):
        cause = CAUSE_SOURCE_UNREACHABLE
    elif "no module named" in low or "importerror" in low:
        cause = CAUSE_DRIVER_MISSING
    elif any(m in low for m in _NOT_CONFIGURED_MARKERS):
        cause = CAUSE_SERVER_NOT_CONFIGURED
    else:
        cause = CAUSE_UNKNOWN
    return LogCaptureRefusal(cause, text or "no detail reported", _remedy(dialect, cause))


def mongo_delete_key_refusal(
    database: str, collection: str, primary_key: str
) -> LogCaptureRefusal:
    """Refusal for a Mongo delete whose business key needs a missing pre-image.

    ``documentKey`` on a delete event is ``_id`` only. A pipeline keyed on a
    business column therefore cannot address the destination row unless the
    collection records pre-images, and applying nothing would leave the deleted
    row at the destination forever — the exact divergence query CDC produces.
    """
    return LogCaptureRefusal(
        CAUSE_MONGO_PREIMAGE_DISABLED,
        f"delete on {database}.{collection} carries documentKey._id only and the "
        f"pipeline is keyed on '{primary_key}', so the deleted row cannot be "
        "identified at the destination",
        _remedy("mongodb", CAUSE_MONGO_PREIMAGE_DISABLED, collection=collection),
    )


def _remedy(dialect: str, cause: str, *, collection: str = "") -> str:
    d = (dialect or "").strip().lower()
    if cause == CAUSE_MONGO_PREIMAGE_DISABLED:
        name = collection or "<collection>"
        return (
            "Enable change-stream pre-images on the collection "
            f"(db.runCommand({{collMod: '{name}', "
            "changeStreamPreAndPostImages: {enabled: true}})) so delete events "
            "carry the business key, or key the pipeline on _id."
        )
    if cause == CAUSE_SLOT_QUOTA:
        if d.startswith("postgres"):
            return (
                "Free an unused logical slot (SELECT slot_name, active FROM "
                "pg_replication_slots WHERE NOT active; then "
                "SELECT pg_drop_replication_slot('<name>')) or raise "
                "max_replication_slots and restart PostgreSQL."
            )
        return "Free an unused replication connection or raise the server's replication limit."
    if cause == CAUSE_PRIVILEGE:
        if d.startswith("postgres"):
            return (
                "Grant the connection user REPLICATION (ALTER ROLE <user> REPLICATION) "
                "and allow a replication entry in pg_hba.conf."
            )
        if d.startswith("mysql") or d.startswith("maria"):
            return (
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '<user>'@'%' "
                "and re-run."
            )
        return "Grant the connection user log-reading privileges on the source."
    if cause == CAUSE_SERVER_NOT_CONFIGURED:
        if d.startswith("postgres"):
            return (
                "Set wal_level=logical and restart PostgreSQL to capture DELETEs; "
                "until then this stream carries inserts/updates by cursor only."
            )
        if d.startswith("mysql") or d.startswith("maria"):
            return (
                "Set log_bin=ON, binlog_format=ROW, binlog_row_image=FULL to capture "
                "DELETEs; until then this stream carries inserts/updates by cursor only."
            )
        return (
            "Enable the source's change log to capture DELETEs; until then this "
            "stream carries inserts/updates by cursor only."
        )
    if cause == CAUSE_SOURCE_UNREACHABLE:
        return "Restore connectivity to the source, then re-run — the stream is not degraded, it is unread."
    if cause == CAUSE_DRIVER_MISSING:
        return (
            "Install the log-reader driver on the worker to capture DELETEs; until "
            "then this stream carries inserts/updates by cursor only."
        )
    return "Inspect the source log-capture error above; DataFlow will not silently drop deletes."
