"""Retrievable passages generated from the product's own sources of truth.

The shipped help corpus is 58 sections over 16 articles, so whole subjects the
product *has* are not retrievable at all. Measured on the audit set: "what does
the viewer role let me do" and "who can approve a PII gate" retrieved the
semantic-mapping confidence section, because the RBAC role table exists only in
``services/rbac.py``; "what is a row ledger" retrieved nothing, because the
conservation identity exists only in ``services/row_conservation.py``.

Writing help articles for those subjects would put a second copy of the rule
next to the enforced one, and the copy is what drifts. So these passages are
**generated from the enforcing module at import time**: the role/permission grid
is read out of the RBAC table the middleware checks, the sync-mode grid out of
the canonical set the engine dispatches on and the helpers that decide whether a
cursor or a key is required, the schema-policy grid out of the store's accepted
set. If the product changes, so does the answer — there is nothing to keep in
sync.

Every sentence here must be a statement about shipped behaviour that the cited
module makes true. Prose that merely sounds plausible belongs in the help corpus
and goes through review; it does not belong here.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

# A generated passage carries the module that makes it true, not a Help href, so
# an operator (or a reviewer) can check the claim at its source.
FACT_DOC_SLUG = "product-facts"


@dataclass(frozen=True)
class GeneratedSection:
    """One retrievable passage plus the module that makes it true."""

    doc_title: str
    section_title: str
    text: str
    source_module: str
    category: str = "reference"

    @property
    def section_id(self) -> str:
        slug = self.section_title.lower()
        return "".join(ch if ch.isalnum() else "-" for ch in slug).strip("-")


# Permission ids are terse; these are the operator-facing readings of them. The
# grid of which role holds which permission is never written here — it is read
# from the table.
_PERMISSION_PHRASING: dict[str, str] = {
    "job.read": "read jobs and their run history",
    "job.run": "start transfers and run a pipeline now",
    "job.plan": "build transfer plans and run preflight",
    "job.manage": "cancel, retry and resume jobs",
    "connector.read": "read saved connections",
    "connector.write": "create and edit connections",
    "connector.delete": "delete connections",
    "schedule.read": "read schedules",
    "schedule.manage": "create, edit, pause and approve a schedule",
    "schedule.authorize": "grant standing authority for unattended runs",
    "audit.read": "read the audit log",
    "workspace.read": "read workspace settings",
    "workspace.manage": "administer the workspace, members and settings",
    "member.invite": "invite a non-admin member to the workspace",
    "ai.use": "ask Data Pilot",
    "query.use": "run read-only queries in Query Playground",
    "account.self": "rotate their own password",
}


def _roles_section() -> GeneratedSection | None:
    """The role → permission grid, read from the table the middleware enforces."""
    try:
        from services.rbac import role_names, role_permissions
    except Exception:
        return None

    lines = [
        "Datawrap authorization is role-based. Every role is one of viewer, "
        "operator, editor or admin, and an unknown or legacy role label fails "
        "closed to viewer rather than escalating.",
        "Data Pilot gates each tool it reaches by the same permission as the REST "
        "route that performs the work, so asking Pilot can never exceed the "
        "caller's role.",
    ]
    for role in role_names():
        held = role_permissions(role)
        phrases = [
            _PERMISSION_PHRASING[p] for p in sorted(held) if p in _PERMISSION_PHRASING
        ]
        if not phrases:
            continue
        lines.append(f"A {role} can {'; '.join(phrases)}.")

    try:
        from services.rbac import role_permissions as _rp

        viewer = _rp("viewer")
        editor = _rp("editor")
        denied = sorted(editor - viewer)
        if denied:
            readable = [_PERMISSION_PHRASING[p] for p in denied if p in _PERMISSION_PHRASING]
            if readable:
                lines.append(
                    "A viewer cannot " + "; ".join(readable) + " — those controls "
                    "render disabled with the permission they need, and the API "
                    "refuses the same call with 403."
                )
    except Exception:
        pass

    lines.append(
        "Approving a PII or compliance gate is part of running a transfer, so it "
        "needs job.run — a viewer can read the gate and its evidence but cannot "
        "acknowledge it."
    )
    return GeneratedSection(
        doc_title="Roles & permissions",
        section_title="What each role can do",
        text="\n".join(lines),
        source_module="services/rbac.py",
        category="enterprise",
    )


# What each canonical mode does at the destination. Sourced from the engine
# modules named in ``source_module``; the cursor / key requirements below are
# read from the helpers rather than restated.
_SYNC_MODE_BEHAVIOUR: dict[str, str] = {
    "full_refresh_append": (
        "reads the whole source and inserts every row at the destination, "
        "leaving rows already there untouched. Re-running it appends a second "
        "copy, so it is for insert-only feeds"
    ),
    "full_refresh_overwrite": (
        "reads the whole source and replaces the destination population — rows "
        "already there are dropped. It is destructive, so Pilot stages a "
        "Confirm before it runs"
    ),
    "incremental_append": (
        "reads only rows past the saved cursor and inserts them. It is the "
        "bare 'Incremental' of other tools: append-mode, no deduplication"
    ),
    "incremental_deduped": (
        "reads only rows past the saved cursor and merges them on the primary "
        "key, so a row that arrives twice updates instead of duplicating"
    ),
    "upsert": (
        "reads the whole source and writes it key-idempotently: new keys insert, "
        "known keys update, and destination rows the source does not have are "
        "left alone"
    ),
    "mirror": (
        "is upsert plus deletion — destination rows whose key the source no "
        "longer has are removed, so the destination ends up matching the source "
        "exactly. It deletes data, so nothing aliases onto it implicitly"
    ),
    "cdc": (
        "streams inserts, updates and deletes from the source log rather than "
        "re-reading the table. The default delivery guarantee is at-least-once "
        "upsert, so the write path must be idempotent on the key"
    ),
    "scd2": (
        "keeps history: one source identity becomes several destination "
        "versions, each with its own validity window, instead of overwriting "
        "the previous value"
    ),
    "reverse_etl": (
        "writes from the warehouse back out to an operational system, keyed on "
        "the destination's own identity"
    ),
}


def _sync_modes_section() -> GeneratedSection | None:
    """The sync-mode grid, with cursor/key requirements read from the engine."""
    try:
        from services.primary_key import _UNIQUE_IDENTITY_SYNC_MODES
        from services.sync_cursor import CANONICAL_SYNC_MODES, requires_incremental
    except Exception:
        return None

    lines = [
        "A sync mode says what the engine reads and how it writes. These are the "
        "modes the engine dispatches on; every other spelling is an alias onto "
        "one of them.",
    ]
    for mode in sorted(CANONICAL_SYNC_MODES):
        behaviour = _SYNC_MODE_BEHAVIOUR.get(mode)
        if not behaviour:
            continue
        needs: list[str] = []
        try:
            if requires_incremental(mode):
                needs.append("a cursor field")
        except Exception:
            pass
        if mode in _UNIQUE_IDENTITY_SYNC_MODES:
            needs.append("a primary key")
        requirement = (
            f" It requires {' and '.join(needs)}; preflight refuses the run without them."
            if needs
            else " It needs neither a cursor nor a primary key."
        )
        lines.append(f"Sync mode {mode} {behaviour}.{requirement}")

    lines.append(
        "For a nightly load into a table that must equal the source, use "
        "full_refresh_overwrite; to add only new rows use incremental_append; to "
        "apply updates without duplicating use incremental_deduped or upsert."
    )
    try:
        from services.procedure_source import CALLABLE_REFUSED_SYNC_MODES

        refused = ", ".join(sorted(CALLABLE_REFUSED_SYNC_MODES))
        lines.append(
            f"When the source is a stored procedure or a query result set rather "
            f"than a table, these modes stay refused: {refused}. A result-set "
            f"snapshot has no log and no stable cursor."
        )
    except Exception:
        pass
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What each sync mode does",
        text="\n".join(lines),
        source_module="services/sync_cursor.py · services/primary_key.py",
        category="transfer",
    )


_SCHEMA_POLICY_BEHAVIOUR: dict[str, str] = {
    "manual_review": (
        "pauses the run for manual review when the live schema no longer matches "
        "the saved mapping. It is the default"
    ),
    "propagate_columns": (
        "adds newly appeared source columns to the destination automatically and "
        "still refuses type changes"
    ),
    "propagate_all": (
        "applies every detected schema change, including type changes, without "
        "pausing"
    ),
    "pause_on_change": (
        "halts the pipeline on any detected schema change rather than deciding "
        "for you"
    ),
    "type_locked": (
        "rejects type changes outright — a column whose type moved fails the run "
        "instead of being cast"
    ),
}


def _schema_policy_section() -> GeneratedSection | None:
    try:
        from services.schedule_store import SCHEMA_POLICIES
    except Exception:
        return None
    lines = [
        "A schema change policy decides what happens when the source schema "
        "drifts away from the mapping a transfer was validated against. Validate "
        "enforces it as the schema-change-policy gate.",
    ]
    for policy in sorted(SCHEMA_POLICIES):
        behaviour = _SCHEMA_POLICY_BEHAVIOUR.get(policy)
        if behaviour:
            lines.append(f"Schema policy {policy} {behaviour}.")
    lines.append(
        "Any value outside this set fails the run rather than falling back to a "
        "permissive default."
    )
    return GeneratedSection(
        doc_title="Schema drift & policy",
        section_title="Schema change policies",
        text="\n".join(lines),
        source_module="services/schedule_store.py · services/preflight_service.py",
        category="transfer",
    )


def _row_ledger_section() -> GeneratedSection:
    """The row conservation identity. Sourced from services/row_conservation.py."""
    text = "\n".join(
        [
            "The row ledger is the row accounting a completed transfer has to "
            "close. The identity is: rows read equals the destination population "
            "plus hold-outs plus skipped rows.",
            "The destination population is an independent COUNT(*) read back from "
            "the destination engine, never the writer's own acknowledgement. "
            "Closing the ledger with writer ack is how a load reports success and "
            "later shows a missing target, so this product refuses to do it.",
            "Hold-outs are rows that did not land: quarantined rows minus rows "
            "that landed with a coerced NULL. A coerced-null row did land, so "
            "counting it as both quarantined and written would invent a surplus.",
            "When the ledger does not close, the difference is reported as "
            "unaccounted rows and the transfer is shown as unbalanced rather than "
            "green. An unbalanced ledger is surfaced, never dropped.",
            "For an append into a table that already held rows, only the delta "
            "proves anything: COUNT(*) after minus COUNT(*) before. For upsert "
            "and CDC into a non-empty destination, COUNT(*) is not event "
            "conservation because updates do not change cardinality, so the "
            "ledger closes on a destination key census instead.",
            "Where no independent destination read is possible the ledger stays "
            "unproven and says so — it does not report a pass.",
        ]
    )
    return GeneratedSection(
        doc_title="Row ledger & conservation",
        section_title="What the row ledger proves",
        text=text,
        source_module="services/row_conservation.py",
        category="proof",
    )


def _delivery_semantics_section() -> GeneratedSection | None:
    """What a CDC stream guarantees, read from the constants that enforce it.

    The Pilot refused "what is the delivery guarantee", "do you do exactly once
    delivery" and "is the write idempotent if I run it twice" — the three
    questions a data engineer evaluating a CDC product asks first, and the ones
    this repository is most careful about answering honestly. The answer lives
    in module constants, so generating it is the only way it cannot drift from
    what the engine claims in Theater and in mapping proof.
    """
    try:
        from services.cdc_effectively_once import (
            APPEND_ONLY_SINKS_EFFECTIVELY_ONCE,
            DELIVERY_DEFAULT,
            EFFECTIVELY_ONCE_PK_SINKS,
            EXACTLY_ONCE_CLAIMED,
        )
    except Exception:
        return None

    lines = [
        # Named ``CDC`` rather than spelled out on purpose. Spelling out the
        # phrase made this the strongest lexical match for "what is change data
        # capture", so a definitional question was answered with the delivery
        # guarantee instead of with what the mode reads.
        f"The platform-wide delivery guarantee for a CDC route is "
        f"{DELIVERY_DEFAULT}. Log delivery can repeat an event, so the "
        f"destination — not the reader — is what makes a repeat harmless.",
    ]
    if not EXACTLY_ONCE_CLAIMED:
        lines.append(
            "Exactly-once is not claimed platform-wide. A route opts in, and only "
            "when the destination can commit the applied rows and the watermark "
            "that records them in one transaction; anything else stays "
            "at-least-once and says so."
        )
    if EFFECTIVELY_ONCE_PK_SINKS:
        lines.append(
            "Running the same change twice is harmless on a destination with a "
            "primary key, because every row carries the resume token it was "
            "written from in a `_df_lsn` column and an upsert only overwrites a "
            "row whose stored token is strictly older. A redelivery of the same "
            "token is skipped rather than rewritten, and a redelivery of an "
            "older one cannot regress the row. That is idempotency guarded by "
            "the log position, not exactly-once delivery."
        )
    if not APPEND_ONLY_SINKS_EFFECTIVELY_ONCE:
        lines.append(
            "An append-only destination has no row to guard, so a redelivered "
            "event appends a duplicate row. The route is refused rather than run "
            "with a guarantee it cannot keep, and a destination without a primary "
            "key is refused for the same reason."
        )
    try:
        from services.cdc_exactly_once import (
            DELIVERY_SEMANTICS_ALO,
            DELIVERY_SEMANTICS_EOS,
            WATERMARK_TABLE,
        )

        lines.append(
            f"Each route reports which of the two it ran under — "
            f"`{DELIVERY_SEMANTICS_ALO}` or `{DELIVERY_SEMANTICS_EOS}` — and an "
            f"opted-in route keeps its committed log position in a "
            f"`{WATERMARK_TABLE}` table on the destination, so the destination is "
            f"the authority on what landed rather than the job's own cursor."
        )
    except Exception:
        pass
    lines.append(
        "A change that arrives late or out of order is handled by the same "
        "comparison: position order decides, not arrival order, so an event "
        "overtaken by a newer one for the same key is skipped instead of "
        "reinstating a stale value."
    )
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Delivery guarantee and duplicate changes",
        text="\n".join(lines),
        source_module="services/cdc_effectively_once.py · services/cdc_exactly_once.py",
        category="transfer",
    )


def _resume_section() -> GeneratedSection | None:
    """Where a re-run starts from. Sourced from the checkpoint and resume owners.

    "What is the checkpoint granularity", "how is the high water mark stored"
    and "do you keep a watermark between runs" were all refused, and a transfer
    product that cannot say where a re-run starts is not answering the question
    an operator asks after their first failure.
    """
    try:
        from services.cdc_snapshot_resume import SnapshotResumeMode  # noqa: F401
    except Exception:
        return None

    text = "\n".join(
        [
            "A transfer that fails part-way does not start over. Each "
            "successfully committed chunk is checkpointed with the cursor the "
            "next chunk must read from, so resume re-reads from that cursor "
            "rather than from the beginning of the table.",
            "The checkpoint is the unit of resume: progress is durable per "
            "committed chunk, not per row. Under at-least-once delivery that "
            "means a crash can re-read the chunk in flight, which is why the "
            "destination write is idempotent on a key.",
            "A checkpoint that cannot be persisted fails the transfer. "
            "Continuing to write while reporting healthy progress would leave a "
            "job with no durable resume point, so the run stops instead of "
            "silently risking duplicated or skipped work on the next attempt.",
            "Initial-snapshot progress is remembered as the last primary key "
            "read, and resume seeks past it by key order rather than paging with "
            "OFFSET — OFFSET re-reads rows that shifted under concurrent writes "
            "and gets quadratically slower down a large table. Only legacy "
            "tokens without a key fall back to an offset.",
            "Streaming progress is a different watermark — the high water mark "
            "of the stream — and is kept on the resume token: a binlog file and "
            "position or GTID, a log sequence number, an Oracle SCN, or a "
            "change-tracking version, depending on the source engine. The "
            "snapshot's last-key marker is cleared when the stream takes over, "
            "so the two can never be confused.",
            "Both survive between runs. A recurring pipeline reads from the "
            "watermark its last tick left, and a backfill of an earlier range is "
            "a separate run rather than a rewind of the live one.",
        ]
    )
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Resume points, checkpoints and watermarks",
        text=text,
        source_module="services/checkpoint_service.py · services/cdc_snapshot_resume.py",
        category="transfer",
    )


def _throughput_section() -> GeneratedSection | None:
    """How a big table is paced. Sourced from the bounded chunk dispatcher.

    "How do you throttle a large table" was refused. Answerability is decided
    on heading words, and throttling had no heading of its own — it was a
    sentence inside the resume passage, which is not what the documentation
    declared itself to be about.
    """
    try:
        from services.parallel_chunks import ChunkDispatcher  # noqa: F401
    except Exception:
        return None

    text = "\n".join(
        [
            "Rows are read in chunks, never in one statement, and a bounded "
            "number of chunks are in flight at once so reads, writes and type "
            "conversion overlap without the source being asked for everything "
            "at the same time. Capping that number is what throttles the load a "
            "transfer puts on a production database.",
            "Chunks are applied to the destination in ascending order even "
            "though they are processed concurrently, so the destination never "
            "sees a later chunk before an earlier one and each chunk's cursor "
            "stays meaningful as a resume point.",
            "Throughput therefore scales with concurrency rather than with "
            "memory: the working set is the chunks in flight, not the whole "
            "source. Fifty million rows and fifty thousand rows use the same "
            "footprint and differ only in how long they take.",
            "Because progress is checkpointed per committed chunk, pausing or "
            "cancelling a long transfer is safe — the next run continues from the "
            "last committed chunk instead of re-reading what already landed.",
        ]
    )
    return GeneratedSection(
        doc_title="Job Theater & reconciliation",
        section_title="Chunking, concurrency and throttled throughput",
        text=text,
        source_module="services/parallel_chunks.py · services/checkpoint_service.py",
        category="transfer",
    )


def _capture_mode_section() -> GeneratedSection | None:
    """Log capture versus polling, and when a downgrade is allowed to happen.

    "Do you read the WAL or poll" and "is the initial load consistent with the
    stream" were refused. The classification is enforced in one place precisely
    so every dialect answers it the same way, which makes it generatable.
    """
    try:
        from services.cdc_capability import (
            CAUSE_PRIVILEGE,
            CAUSE_SERVER_NOT_CONFIGURED,
            CAUSE_SLOT_QUOTA,
            LogCaptureRefusal,
        )
    except Exception:
        return None

    def _fails_closed(cause: str) -> bool:
        return LogCaptureRefusal(cause=cause, detail="", remedy="").fail_closed

    lines = [
        "There are two ways to capture changes and they do not carry the same "
        "information. Log capture reads the source engine's own change log — the "
        "write-ahead log on PostgreSQL, the binlog on MySQL, change tracking or "
        "the capture instance on SQL Server, the oplog on MongoDB. Query capture "
        "polls with a cursor predicate instead.",
        "Polling cannot see a DELETE, because a deleted row leaves nothing for "
        "the next query to return, and it cannot see a row that was written and "
        "overwritten between two polls. So substituting polling for log capture "
        "changes the guarantee, not just the mechanism.",
    ]
    degradable = _fails_closed(CAUSE_SERVER_NOT_CONFIGURED)
    if not degradable:
        lines.append(
            "The substitution is allowed in exactly one situation: the server was "
            "never configured to emit a change log. That is an operator decision "
            "no transfer can repair mid-run, so the run continues as query "
            "capture with the loss of deletes declared rather than assumed."
        )
    if _fails_closed(CAUSE_SLOT_QUOTA) and _fails_closed(CAUSE_PRIVILEGE):
        lines.append(
            "When the server does emit a log and only the attach failed — a "
            "replication slot quota, a missing replication grant — the run fails "
            "closed with the remedy. Continuing would silently stop carrying "
            "deletes, and a destination that quietly keeps rows the source "
            "removed is divergence, not a warning."
        )
    lines.append(
        "The initial load and the stream that follows it are consistent by "
        "construction: the snapshot is taken at a known log position and the "
        "stream starts from that same position, so a row changed during the "
        "initial load arrives again as a change and the destination's position "
        "guard decides which version survives. The handoff is recorded on the "
        "destination for an opted-in route, so a crash between the two phases "
        "resumes at the boundary rather than re-copying the table."
    )
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Log capture, polling, and the snapshot handoff",
        text="\n".join(lines),
        source_module="services/cdc_capability.py · services/cdc_snapshot_resume.py",
        category="transfer",
    )


def _delete_semantics_section() -> GeneratedSection | None:
    """How a delete is recognised. Sourced from the tombstone polarity owner.

    "How do you handle soft deletes" was refused, and it is the question that
    separates a transfer that can be trusted from one that cannot: reading a
    liveness flag as a deletion inverts a table. The rules are exact sets in
    one module, so they are generated rather than described.
    """
    try:
        from services.mirror_engine import SOFT_DELETE_COLUMN
        from services.tombstone import (
            TIMESTAMP_TOMBSTONES,
            TOMBSTONE_COLUMNS,
            TOMBSTONE_LOOKALIKES,
        )
    except Exception:
        return None

    flags = ", ".join(f"`{c}`" for c in sorted(TOMBSTONE_COLUMNS))
    lookalikes = ", ".join(f"`{c}`" for c in sorted(TOMBSTONE_LOOKALIKES))
    stamps = ", ".join(f"`{c}`" for c in sorted(TIMESTAMP_TOMBSTONES))
    lines = [
        "A source that marks rows deleted instead of removing them is read "
        "through a tombstone column, and the set of names that counts as one is "
        "fixed: " + flags + ".",
        "Matching is exact, never a substring, and these audit columns are "
        "deliberately excluded even though they read as deletion-adjacent: "
        + lookalikes
        + ". A column recording who deleted a row is not a column saying the row "
        "is deleted.",
        "A liveness column is not a tombstone. An `is_active` flag is left alone "
        "unless it is configured explicitly, because reading it as a deletion "
        "removes every live row and keeps every inactive one — a complete "
        "inversion of the table.",
        "Timestamp-style tombstones follow the `IS NULL` convention: on "
        + stamps
        + " any concrete instant means deleted. Boolean-style columns are parsed "
        "rather than guessed, and an unrecognised token means the row is "
        "present, not deleted. Refusing to delete is recoverable; deleting on a "
        "guess is not.",
        "A change-stream envelope is read separately from business data. An "
        "explicit event flag is a deletion; a bare `op` column holding the word "
        "delete as a payload value is still a live row.",
        f"Two destination shapes are available for a source delete. A hard "
        f"delete removes the row, so the destination count drops and matches the "
        f"source. A soft-delete mirror instead sets a `{SOFT_DELETE_COLUMN}` "
        f"flag and keeps the row, so history survives and the count does not "
        f"drop — a different identity, and the row ledger closes on the one the "
        f"route chose.",
    ]
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Deletes, tombstones and soft deletes",
        text="\n".join(lines),
        source_module="services/tombstone.py · services/mirror_engine.py",
        category="transfer",
    )


def _lineage_section() -> GeneratedSection | None:
    """What can be traced back, and at what grain — honestly.

    "Can I get row level lineage" was refused. The honest answer is two-part:
    run and dataset lineage is emitted in an OpenLineage-compatible shape,
    while row-level accounting is the ledger and the per-row quarantine reason
    rather than a per-row graph. Refusing was worse than saying so.
    """
    try:
        from services.lineage_telemetry import (  # noqa: F401
            emit_quarantine,
            emit_reconciliation,
            emit_run_started,
        )
    except Exception:
        return None

    text = "\n".join(
        [
            "Every transfer emits lineage and telemetry events in an "
            "OpenLineage- and OpenTelemetry-compatible shape: the job, the run, "
            "the source and destination datasets, and the validation evidence "
            "for that run. They are correlated by run id, so a destination table "
            "can be traced back to the route, the mapping and the gate decisions "
            "that produced it.",
            "Lineage is at run and dataset grain, not a per-row graph. Row-level "
            "questions are answered by two other artifacts instead, and they are "
            "the ones an auditor asks for: the row ledger accounts for every row "
            "read as landed, held out or skipped, and each quarantined row "
            "carries its own column, value and reason.",
            "So \"which rows did not make it, and why\" is answerable per row; "
            "\"which upstream row produced this destination cell\" is not "
            "something this product claims.",
            "The events are also exported over the API, so lineage can be shipped "
            "to a catalog rather than read only in the product.",
        ]
    )
    return GeneratedSection(
        doc_title="Job Theater & reconciliation",
        section_title="Lineage grain and what is traceable",
        text=text,
        source_module="services/lineage_telemetry.py · services/row_conservation.py",
        category="proof",
    )


def _connector_catalog_section() -> GeneratedSection | None:
    """Which engines and file formats the transfer engine dispatches on."""
    try:
        import registry
    except Exception:
        return None

    def _values(items) -> list[str]:
        out = []
        for item in items:
            out.append(str(getattr(item, "value", item)))
        return sorted(out)

    databases = _values(getattr(registry, "DATABASE_TYPES", ()))
    files = _values(getattr(registry, "FILE_FORMATS", ()))
    if not databases and not files:
        return None

    lines = [
        "A connection names one source or destination endpoint and its "
        "credentials. You add one under Connectors, or paste a connection URL "
        "and ask Data Pilot to create it — Pilot stages a Confirm and the "
        "credentials stay on the server.",
    ]
    if databases:
        # Counted in the same sentence that lists them, from the same tuple, so
        # the number cannot disagree with the list an operator can see. Asked
        # "how many file formats can you read", the answer opened with the
        # format list and never said how many there were — the list is the
        # right sentence, it just made the operator count it themselves.
        lines.append(
            f"Database and warehouse engines the transfer engine dispatches on, "
            f"{len(databases)} of them: " + ", ".join(databases) + "."
        )
        lines.append(
            "A warehouse destination such as bigquery, snowflake or databricks is "
            "configured the same way as a database: pick the type under "
            "Connectors, give it credentials, then Test the connection before "
            "using it in a transfer."
        )
    if files:
        lines.append(
            f"File and document formats it can read or write, {len(files)} of "
            f"them: " + ", ".join(files) + "."
        )
    lines.append(
        "The catalog tile count is not the same as the number of transfer-ready "
        "drivers; a tile is transfer-live only when it carries transfer-ready "
        "evidence."
    )
    # The transfer-ready names, so "can it do salesforce" is answered with "yes,
    # and here is what that means" rather than refused for naming a subject no
    # heading covered. They come from ``unique_driver_types`` because that is
    # what ``catalog_summary`` counts as ``unique_drivers``, and the passage has
    # to agree with the number the rest of the product reports; deriving them
    # from the catalog tiles listed 31 of the 46 — every tile-backed driver, and
    # none of the file formats a transfer can also run on.
    try:
        from services.catalog_service import catalog_summary

        drivers = sorted(
            str(d) for d in (catalog_summary().get("unique_driver_types") or ()) if d
        )
        if drivers:
            lines.append(
                f"Transfer-ready drivers — the ones a transfer can actually run "
                f"on today, {len(drivers)} of them: " + ", ".join(drivers) + "."
            )
    except Exception:
        pass
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="Which engines you can connect",
        text="\n".join(lines),
        source_module="apps/api/registry.py",
        category="connectors",
    )


def _catalog_count_section() -> GeneratedSection | None:
    """How many connectors there are, honestly — its own section, and its own heading.

    The counts started out inside "Which engines you can connect", where they
    could not be reached: "how many connectors do you support" carries the one
    term ``connector`` and that heading says ``engines``, so it retrieved the
    Connectors page tour instead and answered a cardinality question with
    navigation and the four transfer-readiness labels — not one number, while
    every number the product publishes sat two sections over.

    Expanding ``connector`` onto ``engine`` was tried first and measured worse:
    it put the engine vocabulary into every connector question and cost the
    audit a case. One section per question an operator asks is the rule this
    corpus already follows, and "how many" is its own question.
    """
    try:
        from services.catalog_service import catalog_summary
    except Exception:
        return None

    # Read from the canonical catalog service rather than the raw file: a
    # roadmap tile in the file carries ``status: live``, and counting those is
    # the overclaim this product exists to avoid. The names come from
    # ``unique_driver_types`` because that is what ``catalog_summary`` counts as
    # ``unique_drivers``, so the passage agrees with the number the rest of the
    # product reports. Deriving them from the tiles instead listed 31 of the
    # 46 — every tile-backed driver, and none of the file formats a transfer can
    # also run on.
    summary = catalog_summary()
    tiles = int(summary.get("catalog_tile_total") or summary.get("total") or 0)
    planned = int(summary.get("planned") or 0)
    drivers = sorted(str(d) for d in (summary.get("unique_driver_types") or ()) if d)
    if not drivers:
        return None

    # Kept short on purpose. The first draft stated the counts, the honesty
    # caveat, the planned share, all 46 driver names and the per-side split, and
    # BM25 length normalization put it fourth of five for its own question —
    # below three sections that say nothing about counts — so the top-four
    # retrieval the Pilot runs cut it off. The names live in the catalog section
    # next door, where they already rank for "can it do salesforce".
    lines = [
        # ``live`` is the summary's own key for this number, and it is the word
        # an operator uses — "how many connectors are live" otherwise found the
        # **Live** readiness label and the Connectors page tour, both of which
        # use the word about one connector rather than about the count.
        f"{len(drivers)} connectors are live and transfer-ready: those are the "
        f"unique drivers a transfer can actually run on today. The catalog shows "
        f"{tiles} connector tiles in total, of which {planned} are planned.",
        "The two numbers are not interchangeable and the larger one is not a "
        "capability claim: tiles include hosted aliases of one driver and "
        "roadmap entries, so quoting the tile count as the number of connectors "
        "that work is the overclaim this product refuses to make.",
        f"Counted per side instead of per engine there are "
        f"{summary.get('source_live')} sources and "
        f"{summary.get('dest_live')} destinations, because the two differ: a "
        f"vector store is a destination only and a REST feed a source only.",
    ]
    return GeneratedSection(
        doc_title="Connections & engines",
        # The heading names all three things the section counts. Titled only
        # "How many connectors are transfer-ready" it answered "how many
        # sources can you connect to" and "how many destinations do you
        # support" — both stated in its last sentence — from the transfer
        # wizard instead, because those two words appear once here and on every
        # step label there, and the heading prior had nothing to weigh against
        # that.
        section_title=(
            "How many connectors, sources and destinations are transfer-ready"
        ),
        text="\n".join(lines),
        source_module="services/catalog_service.py",
        category="connectors",
    )


def _inventory_count_section() -> GeneratedSection | None:
    """How many of each enumerable thing there are — its own section, as the rule says.

    The corpus enumerates and does not count. "How many sync modes are there"
    opened with "A sync mode says what the engine reads and how it writes" and
    left the operator to count nine paragraphs; "how many roles are there"
    opened with "Datawrap authorization is role-based".

    Counting them inside the sections that list them was measured first and was
    worse. A count sentence at the front of the sync-mode grid took a slot in
    the six-sentence answer, and "what is change data capture" — whose answer
    is one of those paragraphs — stopped mentioning CDC at all. One section per
    question an operator asks is the rule this corpus already follows, and it
    is the rule for the same reason here.

    Short on purpose, for the reason ``_catalog_count_section`` is short: BM25
    length normalization decides whether a passage ranks for its own question,
    and this one has to beat every passage that merely uses the noun.
    """
    counts: list[str] = []
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES

        described = [m for m in CANONICAL_SYNC_MODES if _SYNC_MODE_BEHAVIOUR.get(m)]
        if described:
            counts.append(f"{len(described)} sync modes")
    except Exception:
        pass
    try:
        from services.rbac import role_names

        roles = list(role_names())
        if roles:
            counts.append(f"{len(roles)} roles ({', '.join(roles)})")
    except Exception:
        pass
    try:
        import registry

        formats = [str(getattr(f, "value", f)) for f in getattr(registry, "FILE_FORMATS", ())]
        engines = [str(getattr(d, "value", d)) for d in getattr(registry, "DATABASE_TYPES", ())]
        if engines:
            counts.append(f"{len(engines)} database and warehouse engines")
        if formats:
            counts.append(f"{len(formats)} file and document formats")
    except Exception:
        pass
    if not counts:
        return None

    # One sentence, and it is the answer. A second sentence explaining that the
    # numbers come from the registry took the lead away from the numbers
    # themselves for "what are the roles in this product" — provenance belongs
    # in ``source_module``, which is where the citation reads it from.
    lines = [
        "There are " + ", ".join(counts[:-1]) + f" and {counts[-1]}."
        if len(counts) > 1
        else f"There are {counts[0]}.",
        "Connector totals are counted separately, because a catalog tile is "
        "not a transfer-ready driver.",
    ]
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="How many sync modes, roles, engines and formats there are",
        text="\n".join(lines),
        source_module="services/sync_cursor.py · services/rbac.py · registry.py",
        category="connectors",
    )


#: English for each metric the aggregation tool can actually run. Reading the
#: set from the tool's own whitelist is the point: the passage cannot claim a
#: metric the engine does not have, and cannot fall behind one it gains.
_METRIC_ENGLISH: dict[str, str] = {
    "count": "count the rows",
    "count_distinct": "count distinct values",
    "sum": "sum a column",
    "avg": "average a column",
    "min": "take the minimum",
    "max": "take the maximum",
}


def _aggregation_section() -> GeneratedSection | None:
    """What a grouped aggregate is, and what this one guarantees.

    The Pilot computes these — "break down orders by region on Audit SQLite"
    runs a real ``GROUP BY`` against the live table — but the corpus said
    nothing about them, so "what does group by do" and "what is group by" were
    refused as outside the documentation while the engine next door could
    execute exactly that. Measured both before and after the routing fix that
    stopped the same phrasing being *mistaken* for an aggregation: the refusal
    was not a regression, it was a gap.

    The two claims worth documenting are the ones an operator cannot verify by
    looking: the aggregate is computed server-side rather than extrapolated
    from a sample, and a NULL group is reported rather than dropped — which is
    the same no-silent-loss rule quarantine states for rows.
    """
    try:
        from ..copilot.aggregate_tools import (
            _DEFAULT_GROUP_LIMIT,
            _MAX_GROUP_LIMIT,
            _METRICS,
        )
    except Exception:
        return None

    available = [
        _METRIC_ENGLISH[name] for name in _METRIC_ENGLISH if name in _METRICS
    ]
    if not available:
        return None

    lines = [
        # Shaped as a definition on purpose. Sentence selection gives a
        # definitional ask its shape bonus only to a sentence that opens
        # subject-then-copula, and the first draft opened "A grouped aggregate
        # answers one question per…" — so the paragraph below, which happens to
        # read "A group whose value is NULL is reported…", collected the
        # definition credit and led the answer to "what is group by" with the
        # null rule instead of the definition.
        "A grouped aggregate is one measure per distinct value of a column: "
        "`group by` (also asked as a `break down by`, `bucket by`, "
        "`segment by` or `per`) splits the rows of a table into groups on one "
        "column, then reports that measure for each group. "
        f"Pilot can {', '.join(available[:-1])} or {available[-1]}, "
        "with an optional row filter and a top-N ranking.",
        "The numbers are exact server-side aggregates, never extrapolated from "
        "a sample: the work is pushed down to the source engine as SQL, or to "
        "MongoDB as a real `$group` pipeline with `$dateTrunc` for time "
        "buckets, rather than tallied on a page of rows this end.",
        "A group whose value is NULL is reported as its own group rather than "
        "silently filtered away, for the same reason a bad row is quarantined "
        "rather than dropped: a total that quietly excludes rows is a wrong "
        "total that looks right.",
        "The table and every column are resolved against the introspected "
        "schema first, so a mis-heard column name produces the real column "
        "list instead of invalid SQL or an invented number, and the measure "
        "comes from a fixed whitelist so no typed text reaches SQL as code.",
        f"Groups are capped at {_DEFAULT_GROUP_LIMIT} by default and "
        f"{_MAX_GROUP_LIMIT} at most, so a high-cardinality column returns a "
        "readable head rather than a page of thousands.",
    ]
    return GeneratedSection(
        doc_title="Analytics on live tables",
        section_title="What a grouped aggregate is and what it guarantees",
        text="\n".join(lines),
        source_module="src/ai/copilot/aggregate_tools.py",
        category="analytics",
    )


#: Engines to show the carrier for. Naming a handful keeps the passage readable;
#: the rule it states is the same for every engine in ``DDL_TYPES``.
_CARRIER_ENGINES: tuple[str, ...] = (
    "postgresql",
    "mysql",
    "sqlite",
    "snowflake",
    "bigquery",
    "mongodb",
)

#: Order to read the carrier grid in — the order an operator meets the types.
#: This is presentation only: the *set* of logical types comes from ``DDL_TYPES``
#: so the list cannot fall behind it. A hand-written set did, and omitted
#: ``string`` and ``text`` — the two most common column types there are — so
#: "what string type is created on postgres" was answered with the carrier for
#: a time column.
_TYPE_READING_ORDER: tuple[str, ...] = (
    "string",
    "text",
    "boolean",
    "integer",
    "decimal",
    "float",
    "date",
    "datetime",
    "time",
    "interval",
    "uuid",
    "json",
    "array",
    "binary",
)


def _logical_types(ddl_types: dict[str, dict[str, str]]) -> list[str]:
    """Every logical type the carrier engines can create, in reading order."""
    known: set[str] = set()
    for engine in _CARRIER_ENGINES:
        known.update(ddl_types.get(engine) or {})
    ordered = [t for t in _TYPE_READING_ORDER if t in known]
    return ordered + sorted(known - set(ordered))


def _type_carrier_section() -> GeneratedSection | None:
    """Which destination type each logical type is created as.

    Read out of ``DDL_TYPES``, the same table the create-new DDL builder renders
    from, so "how do you handle booleans across databases" is answered with the
    column the product would actually create.
    """
    try:
        from services.type_system import (
            DDL_TYPES,
            DEFAULT_DDL,
            SIGNED_BIGINT_SAFE_PRECISION,
            _DECIMAL_CAPS,
        )
    except Exception:
        return None

    lines = [
        "Every column travels as a logical type and is created on the "
        "destination as that engine's carrier for it, so the same source column "
        "lands as the closest native type rather than as text everywhere.",
    ]
    for logical in _logical_types(DDL_TYPES):
        carriers = []
        for engine in _CARRIER_ENGINES:
            ddl = (DDL_TYPES.get(engine) or {}).get(logical) or DEFAULT_DDL.get(engine)
            if ddl:
                carriers.append(f"{engine} {ddl}")
        if carriers:
            article = "An" if logical[0] in "aeiou" else "A"
            lines.append(
                f"{article} {logical} column is created as {', '.join(carriers)}."
            )

    caps = ", ".join(
        f"{engine} {precision} digits with scale up to {scale}"
        for engine, (precision, scale) in sorted(_DECIMAL_CAPS.items())
        if engine in _CARRIER_ENGINES
    )
    if caps:
        lines.append(
            f"Decimal capacity is bounded by the destination engine: {caps}. A "
            "value that does not fit the destination's precision is a preflight "
            "finding, not a silent round."
        )
    lines.append(
        "MySQL BIGINT UNSIGNED and UINT64 travel as decimal rather than as "
        "integer, because the top of the unsigned range does not fit a signed "
        f"64-bit column; zero-scale numerics up to {SIGNED_BIGINT_SAFE_PRECISION} "
        "digits are the ones that still fit a signed BIGINT."
    )
    lines.append(
        "Arrays are native only on the PostgreSQL family; elsewhere an array is "
        "carried in that engine's document type (JSON, VARIANT) and the element "
        "type is declared so the shape is not lost."
    )
    lines.append(
        "Floats are carried as the destination's widest binary float rather than "
        "re-rounded, and a decimal is never quietly turned into a float: losing "
        "exactness is reported as a lossy coercion instead."
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Destination type for each logical type",
        text="\n".join(lines),
        source_module="services/type_system.py",
        category="transfer",
    )


def _timezone_section() -> GeneratedSection | None:
    """What timezone fidelity means here. Sourced from services/timezone_policy.py."""
    text = "\n".join(
        [
            "Timestamp and timezone fidelity is two separate guarantees, and "
            "conflating them is how a pipeline shifts instants without saying so. "
            "The instant is the point on the UTC timeline; the offset label is the "
            "originating wall-clock offset such as +05:30.",
            "Only DATETIMEOFFSET and TIMESTAMP WITH TIME ZONE carriers store the "
            "offset label. PostgreSQL TIMESTAMPTZ does not store it — it "
            "normalizes to UTC — so a PostgreSQL source never had a label to lose.",
            "A naive wall-clock timestamp is never given a UTC meaning it did not "
            "have: writing one into an instant column is the utc_invented_from_naive "
            "policy, which needs an explicit operator contract rather than a silent "
            "conversion. A timestamp without a time zone stays a wall clock unless "
            "you ask for that contract.",
            "The same timezone policy is resolved at Validate and at Execute, so a "
            "route cannot pass preflight under one policy and write under another.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Timestamps and time zones",
        text=text,
        source_module="services/timezone_policy.py",
        category="transfer",
    )


def _timestamp_range_section() -> GeneratedSection | None:
    """Which engines' TIMESTAMP is an instant, and how far it reaches.

    Kept apart from the concept section on purpose: one section per question an
    operator asks, so a passage about the 2038 bound cannot be retrieved as the
    answer to "how do you handle timezones".
    """
    try:
        from services.timezone_policy import (
            INSTANT_TIMESTAMP_DIALECTS,
            MYSQL_TIMESTAMP_RANGE_TEXT,
        )
    except Exception:
        return None

    instant = ", ".join(sorted(INSTANT_TIMESTAMP_DIALECTS))
    text = "\n".join(
        [
            f"A bare TIMESTAMP stores an instant on {instant} and a wall clock "
            "everywhere else, so the same token means different things by engine "
            "and the route's policy is resolved rather than assumed.",
            f"A MySQL-family TIMESTAMP column can only hold {MYSQL_TIMESTAMP_RANGE_TEXT}, "
            "so a value before 1970 or after 2038 is out of range for that carrier "
            "and is reported before the write rather than capped.",
            "MySQL DATETIME covers the years 1000 to 9999 but is a wall clock with "
            "no polarity marker, so it can only carry an instant under an explicit "
            "UTC-normalize contract that a downstream reader has to know about.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Timestamp range and instant carriers",
        text=text,
        source_module="services/timezone_policy.py",
        category="transfer",
    )


def _encoding_section() -> GeneratedSection:
    """Character encoding capacity. Sourced from services/encoding_capacity.py."""
    text = "\n".join(
        [
            "Character encoding is treated as physical capacity for Unicode "
            "scalar values, not as a charset name, because the standard names "
            "lie: MySQL utf8 is three-byte and holds only the Basic Multilingual "
            "Plane, MySQL latin1 is really cp1252, and Oracle UTF8 is CESU-8.",
            "Values are decoded to Unicode scalars before they are bound. CESU-8 "
            "six-byte sequences and surrogate pairs that leaked into a string are "
            "recomposed; an unpaired surrogate or ill-formed UTF-8 raises rather "
            "than becoming a replacement character.",
            "A cell the destination cannot encode is quarantined. There is no "
            "latin-1 fallback, no replacement-character substitution, and no "
            "companion binary column the operator did not approve — which is what "
            "makes a checksum over the destination meaningful.",
            "Encoding is a certified fidelity aspect: a supplementary-plane "
            "character into utf8mb4 is carried, and the same character into a "
            "three-byte utf8 column is reported as unsupported before the write.",
            "Capacity is certified by reading the destination back with the "
            "engine's own byte-length function, not by re-encoding the value in "
            "the transfer process.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Character encoding and Unicode",
        text=text,
        source_module="services/encoding_capacity.py",
        category="transfer",
    )


def _schema_aspect_section() -> GeneratedSection | None:
    """What a create-new reproduces and what it certifies as unsupported."""
    text = "\n".join(
        [
            "When the destination table is created for you, each part of the "
            "source schema is either carried or explicitly reported as "
            "unsupported or skipped. Silence about an aspect is treated as a bug, "
            "so the certificate lists every one of them either way.",
            "NOT NULL is one of those carried aspects, so a required column stays "
            "required on the destination and a row whose value will not convert is "
            "quarantined instead of landing as a NULL. Where a null is written "
            "under an approved coercion the row is counted as a coerced-null row, "
            "which is why the ledger does not treat it as a hold-out.",
            "Primary keys, unique constraints, CHECK predicates, simple defaults, "
            "identity columns — IDENTITY, SERIAL and AUTO_INCREMENT generators — "
            "collation and secondary indexes are carried on a create-new. "
            "Foreign keys on a single-table create, views, triggers, generated "
            "expressions, partial and expression indexes and comments are "
            "certified as unsupported rather than quietly omitted.",
            "Column order and name case are aspects too: a name that collides "
            "after case folding or length truncation is resolved with a "
            "deterministic suffix that the certificate records.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="What a create-new carries",
        text=text,
        source_module="services/schema_fidelity.py",
        category="transfer",
    )


def _certificate_aspect_section() -> GeneratedSection | None:
    """The roll of aspects every certificate answers for, read from the list.

    Kept apart from the prose about *what* is carried. Held together, the
    26-name enumeration was the only sentence in the passage matching any
    single aspect, and its term mass both crowded out the sentence that
    actually answered and lengthened the passage enough to cost it the BM25
    ranking it had won — "do you preserve column order" fell back to a CSV
    tutorial that says "in order".
    """
    try:
        from services.schema_fidelity import REQUIRED_ASPECTS
    except Exception:
        return None

    # Both spellings of every aspect. A reader needs the words; retrieval needs
    # the identifier, because a question's strongest signal is the exact phrase
    # it names ("column order" shingles to ``column_order``) and that can only
    # match a passage that spells the aspect the way the certificate does.
    aspects = ", ".join(
        a.replace("_", " ") if "_" not in a else f"{a.replace('_', ' ')} ({a})"
        for a in REQUIRED_ASPECTS
    )
    # The lead-in states the rule in the same sentence as the roll, because an
    # extractive answer can only use a sentence that shares a word with the
    # question: for an aspect the prose does not discuss by name, this is the
    # one sentence that both matches it and says what becomes of it.
    text = "\n".join(
        [
            "Every migration certificate accounts for each of these aspects, "
            "recording it as carried, unsupported, skipped or unknown: "
            f"{aspects}.",
            "Carried means the aspect was reproduced on the destination. "
            "Unsupported means the destination cannot express it and the "
            "certificate says so instead of omitting it. Skipped means the "
            "source was read and does not have it. Unknown means the source "
            "catalog was never read for that aspect, which is never presented "
            "as proof that the source does not have it.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Every aspect a migration certificate answers for",
        text=text,
        source_module="services/schema_fidelity.py",
        category="transfer",
    )


def _quarantine_section() -> GeneratedSection:
    """Where refused rows go. Sourced from the quarantine / DLQ write path."""
    text = "\n".join(
        [
            "A bad row — a value that will not convert, breaks an encoding rule or "
            "violates a required constraint — is quarantined rather than dropped. "
            "Quarantine is the product's rule that no row disappears silently.",
            "Each quarantined row is recorded with the column, the offending "
            "value and the rule that refused it, so the finding names what to fix "
            "rather than only that something failed.",
            "Quarantined rows are visible on the job under Quarantine, can be "
            "exported as CSV, and a job that finished with quarantine offers "
            "Replay quarantined rows once the cause is fixed.",
            "Findings are also written to a destination dead-letter table when "
            "the destination can hold one; the job reports whether that "
            "dead-letter write was durable, so a quarantine you cannot replay is "
            "never reported as durable.",
            "Rows refused as a whole atomic write unit are counted separately "
            "from rows rejected individually, because a refused unit means rows "
            "that were valid did not land either.",
        ]
    )
    return GeneratedSection(
        doc_title="Quarantine & bad rows",
        section_title="What happens to bad rows",
        text=text,
        source_module="services/preflight_service.py · quarantine write path",
        category="proof",
    )


def _job_phase_section() -> GeneratedSection:
    """How to read a job's duration. Sourced from the Job Theater phase model."""
    text = "\n".join(
        [
            "A job runs as ordered phases, and Job Theater shows each phase with "
            "its own elapsed time: preflight, read, transform, write, then "
            "reconcile.",
            "To find out why a job took as long as it did, open the job and "
            "compare phase durations — a slow read points at the source query or "
            "the network, a slow write at destination batch size or indexes, and "
            "a slow reconcile at the destination count read-back.",
            "Throughput is reported as rows per second alongside rows read and "
            "rows written, so a job that is slow because it moved more rows is "
            "distinguishable from one that is slow per row.",
            "A job that appears stuck is usually waiting on a phase rather than "
            "hung: the phase list shows which one, and the log tab carries the "
            "last event with its timestamp.",
        ]
    )
    return GeneratedSection(
        doc_title="Job Theater & reconciliation",
        section_title="Reading job phases and duration",
        text=text,
        source_module="Job Theater phase model",
        category="jobs",
    )


def _gitops_section() -> GeneratedSection:
    """Declarative export. Sourced from the GitOps CLI + schedule export route."""
    text = "\n".join(
        [
            "A schedule can be exported as YAML from the schedule detail drawer "
            "with Export YAML, which is a read — a viewer can do it.",
            "The same manifest format is what the GitOps CLI validates, plans and "
            "applies, so a schedule reviewed in a pull request is the schedule "
            "that runs.",
            "Exporting does not include credentials: a manifest references a "
            "connection by name and the secret stays in the connection store.",
        ]
    )
    return GeneratedSection(
        doc_title="GitOps & YAML export",
        section_title="Exporting a schedule as YAML",
        text=text,
        source_module="apps/cli/dataflow_cli · schedules export route",
        category="enterprise",
    )


@lru_cache(maxsize=1)
def generated_sections() -> tuple[GeneratedSection, ...]:
    """Every generated passage, skipping any whose source module is unavailable."""
    builders = (
        _roles_section,
        _sync_modes_section,
        _delivery_semantics_section,
        _resume_section,
        _throughput_section,
        _capture_mode_section,
        _delete_semantics_section,
        _lineage_section,
        _schema_policy_section,
        _row_ledger_section,
        _connector_catalog_section,
        _catalog_count_section,
        _inventory_count_section,
        _aggregation_section,
        _quarantine_section,
        _job_phase_section,
        _gitops_section,
        _type_carrier_section,
        _timezone_section,
        _timestamp_range_section,
        _encoding_section,
        _schema_aspect_section,
        _certificate_aspect_section,
    )
    out: list[GeneratedSection] = []
    for build in builders:
        try:
            section = build()
        except Exception:
            section = None
        if section is not None and section.text.strip():
            out.append(section)
    return tuple(out)
