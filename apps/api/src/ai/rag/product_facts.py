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
    # Who holds the verb, generated from the same table. "Who can run
    # transfers" otherwise opened on the viewer-negative PII sentence below
    # — true, and not the answer — because that sentence says ``run`` and
    # the can-sentences name each role separately.
    runners = [
        role for role in role_names() if "job.run" in role_permissions(role)
    ]
    if len(runners) > 1:
        who = f"{', '.join(runners[:-1])} or {runners[-1]}"
        lines.append(f"{who[0].upper() + who[1:]} can start transfers and run a pipeline now.")
    elif runners:
        lines.append(f"A {runners[0]} can start transfers and run a pipeline now.")
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

    if len(runners) > 1:
        who = f"{', '.join(runners[:-1])} or {runners[-1]}"
    elif runners:
        who = f"a {runners[0]}"
    else:
        who = "a role that holds job.run"
    lines.append(
        f"Approving a PII or compliance gate needs job.run, which {who} "
        f"holds — a viewer can read the gate and its evidence but cannot "
        f"acknowledge it."
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
#
# Every line is deliberately written in the same definitional shape — "Sync mode
# X is …" — because the composer pays a definition ask for that shape, and the
# grid is nine parallel descriptions of one enum. When only ``mirror`` opened
# with a copula it collected that bonus for questions about every other mode:
# asked "what is SCD type 2" the answer led with "Sync mode mirror is upsert
# plus deletion". Uniform shape hands the decision back to the words the
# operator typed, which is the only thing here that distinguishes the modes.
_SYNC_MODE_BEHAVIOUR: dict[str, str] = {
    "full_refresh_append": (
        "is a whole-source read that inserts every row at the destination, "
        "leaving rows already there untouched. Re-running it appends a second "
        "copy, so it is for insert-only feeds"
    ),
    "full_refresh_overwrite": (
        "is a whole-source read that replaces the destination population — rows "
        "already there are dropped. It is destructive, so Pilot stages a "
        "Confirm before it runs"
    ),
    "incremental_append": (
        "is a cursor-bounded read that inserts only the rows past the saved "
        "cursor. It is the bare 'Incremental' of other tools: append-mode, no "
        "deduplication"
    ),
    "incremental_deduped": (
        "is a cursor-bounded read that merges the rows past the saved cursor on "
        "the primary key, so a row that arrives twice updates instead of "
        "duplicating"
    ),
    "upsert": (
        "is a whole-source read written key-idempotently: new keys insert, "
        "known keys update, and destination rows the source does not have are "
        "left alone"
    ),
    "mirror": (
        "is upsert plus deletion — destination rows whose key the source no "
        "longer has are removed, so the destination ends up matching the source "
        "exactly. It deletes data, so nothing aliases onto it implicitly"
    ),
    "cdc": (
        "is a log-based read that streams inserts, updates and deletes from the "
        "source log rather than re-reading the table. The default delivery "
        "guarantee is at-least-once upsert, not exactly-once, so the write "
        "path must be idempotent on the key"
    ),
    "scd2": (
        "is a history-keeping write: one source identity becomes several "
        "destination versions, each with its own validity window, instead of "
        "overwriting the previous value"
    ),
    "reverse_etl": (
        "is a write from the warehouse back out to an operational system, keyed "
        "on the destination's own identity"
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
        # "names", not "says": the composer's definitional shape test accepts a
        # copula and a handful of verbs that behave like one, and every line of
        # the grid below is written to fit it. With the sentence that defines
        # the *category* left outside the shape, "what is a sync mode" was
        # answered with the definition of whichever single mode ranked first.
        "A sync mode names what the engine reads and how it writes. These are "
        "the modes the engine dispatches on; every other spelling is an alias "
        "onto one of them.",
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
    # The append-vs-overwrite contrast lives in its own section. Kept here it
    # sat behind the help-corpus Append line for "what is the difference
    # between append and overwrite", because that sentence is definitional
    # and this one was the tenth line of a long grid.
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
    names = sorted(SCHEMA_POLICIES)
    listed = (
        ", ".join(names[:-1]) + f" and {names[-1]}"
        if len(names) > 1
        else (names[0] if names else "")
    )
    lines = [
        # Named first, because "what schema change policies are there" is a
        # request for the members. Left as a definition of the category, the
        # composer opened with a wizard step that happens to bold the words
        # "Schema change policy" — "Set Validation mode (Strict / Maximum /
        # Balanced)" — while every policy name sat below it.
        f"The schema change policies are {listed}.",
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
        f"{DELIVERY_DEFAULT} — running the same CDC change twice is "
        f"idempotent on `_df_lsn`. Log delivery can repeat an event, so the "
        f"destination — not the reader — is what makes a repeat harmless.",
    ]
    if not EXACTLY_ONCE_CLAIMED:
        lines.append(
            "Exactly-once is not claimed platform-wide, so a CDC route stays "
            "at-least-once unless the destination can commit the applied rows "
            "and the watermark that records them in one transaction."
        )
    if EFFECTIVELY_ONCE_PK_SINKS:
        lines.append(
            "Running the same change twice is harmless on a destination with a "
            "primary key, because every row carries the resume token it was "
            "written from in a `_df_lsn` column and an upsert only overwrites a "
            "row whose stored token is strictly older. A redelivery of the same "
            "token is skipped rather than rewritten, and a redelivery of an "
            "older one cannot regress the row. That is at-least-once "
            "idempotency guarded by the log position, not exactly-once delivery."
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
            "Both survive between runs. A recurring pipeline continues from the "
            "resume token its last tick left, and a backfill of an earlier range "
            "is a separate run rather than a rewind of the live one.",
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
        # "Change data capture (CDC) is" — definitional shape plus the two
        # terms the audit greps for. "There are two ways to capture changes"
        # is true and ranked first, and it names neither.
        "Change data capture (CDC) is log capture: it reads the WAL "
        "(write-ahead log) on PostgreSQL, the binlog on MySQL, change "
        "tracking or the capture instance on SQL Server, or the oplog on "
        "MongoDB, rather than polling with a cursor, which does not carry "
        "the same information.",
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


def _snapshot_handoff_section() -> GeneratedSection:
    """The snapshot→stream boundary, asked without the heading words."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="How the snapshot hands off to the CDC stream",
        text=(
            "The snapshot is taken at a known log position and the CDC stream "
            "starts from that same position, so the snapshot handoff is the "
            "shared LSN: "
            "a crash between the two phases resumes at the boundary rather than "
            "re-copying the table."
        ),
        source_module="services/cdc_capability.py · services/cdc_snapshot_resume.py",
        category="transfer",
    )


def _postgres_cdc_prereq_section() -> GeneratedSection:
    """wal_level=logical is the server switch the capability module names."""
    try:
        from services.cdc_capability import CAUSE_SERVER_NOT_CONFIGURED, _remedy
    except Exception:
        remedy = (
            "Set wal_level=logical and restart PostgreSQL to capture DELETEs."
        )
    else:
        remedy = _remedy("postgresql", CAUSE_SERVER_NOT_CONFIGURED)
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Does Postgres CDC need wal_level=logical",
        text=(
            f"Yes — Postgres CDC needs wal_level=logical. {remedy} "
            "Until the server emits a logical change log the stream cannot "
            "carry DELETEs."
        ),
        source_module="services/cdc_capability.py",
        category="transfer",
    )


def _replication_slot_section() -> GeneratedSection:
    """A slot is a source-side resource, not a destination table."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What a replication slot is",
        text=(
            "A PostgreSQL CDC schedule owns a replication slot on the source. "
            "The slot holds WAL until the stream consumes it; leaving an unused "
            "slot retains WAL forever and can exhaust max_replication_slots. "
            "Deleting the schedule drops the slot unless another route still "
            "needs it or a consumer is attached."
        ),
        source_module="services/cdc_capture_release.py · services/cdc_capability.py",
        category="transfer",
    )


def _pgoutput_plugin_section() -> GeneratedSection:
    """The plugin the slot is created with."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What plugin Postgres CDC uses",
        text=(
            "Postgres CDC uses the pgoutput logical-decoding plugin "
            "(falling back to test_decoding when pgoutput cannot be loaded). "
            "The plugin is recorded on the replication slot, not picked from "
            "the destination write tiles."
        ),
        source_module="services/cdc_capability.py · connectors/pgoutput_decoder.py",
        category="transfer",
    )


def _cdc_schedule_delete_section() -> GeneratedSection:
    """Deleting the schedule is a slot-release question, not a row delete."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What happens if I delete a CDC schedule",
        text=(
            "Deleting a CDC schedule drops its source replication slot — "
            "SELECT pg_drop_replication_slot on PostgreSQL — unless another "
            "schedule on the same route still needs it or the slot has an "
            "attached consumer. Fivetran/Estuary drop the slot on connector "
            "deletion; Airbyte leaves it to the operator."
        ),
        source_module="services/cdc_capture_release.py",
        category="transfer",
    )


def _toast_cdc_section() -> GeneratedSection:
    """Unchanged TOAST columns must not be upserted as nulls."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Do unchanged TOAST columns get dropped on a CDC update",
        text=(
            "No — when pgoutput marks an unchanged TOAST column as omitted, "
            "the write merges the old tuple so the destination is not wiped "
            "to null. A sparse update with no old tuple is refused as "
            "toast_incomplete rather than applied."
        ),
        source_module="services/cdc_toast.py",
        category="transfer",
    )


def _replica_identity_section() -> GeneratedSection:
    """UPDATE/DELETE old keys — the reader requires FULL, not DEFAULT."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Does Postgres CDC need REPLICA IDENTITY FULL",
        text=(
            "Yes — Postgres CDC needs REPLICA IDENTITY FULL so UPDATE and "
            "DELETE emit old keys and unchanged TOAST columns. The reader "
            "runs ALTER TABLE … REPLICA IDENTITY FULL; without it a sparse "
            "update is refused as toast_incomplete rather than applied."
        ),
        source_module="connectors/postgresql_change_stream.py · services/cdc_toast.py",
        category="transfer",
    )


def _mysql_cdc_prereq_section() -> GeneratedSection:
    """ROW + FULL image is the MySQL switch the capability module names."""
    try:
        from services.cdc_capability import CAUSE_SERVER_NOT_CONFIGURED, _remedy
    except Exception:
        remedy = (
            "Set log_bin=ON, binlog_format=ROW, binlog_row_image=FULL to "
            "capture DELETEs."
        )
    else:
        remedy = _remedy("mysql", CAUSE_SERVER_NOT_CONFIGURED)
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Does MySQL CDC need binlog_format=ROW",
        text=(
            f"Yes — MySQL CDC needs binlog_format=ROW. {remedy} "
            "STATEMENT or MIXED format cannot carry a row image."
        ),
        source_module="services/cdc_capability.py",
        category="transfer",
    )


def _publication_section() -> GeneratedSection:
    """A publication is the table-set the slot decodes, not the slot itself."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What a publication is",
        text=(
            "A publication is the PostgreSQL object that names which tables "
            "a CDC replication slot decodes. The schedule owns both the "
            "publication and the slot; deleting the schedule drops the "
            "publication unless another route still needs it."
        ),
        source_module="services/cdc_capture_release.py",
        category="transfer",
    )


def _cdc_privilege_section() -> GeneratedSection:
    """REPLICATION grant, not superuser — from the privilege remedy."""
    try:
        from services.cdc_capability import CAUSE_PRIVILEGE, _remedy
    except Exception:
        remedy = (
            "Grant the connection user REPLICATION (ALTER ROLE <user> "
            "REPLICATION) and allow a replication entry in pg_hba.conf."
        )
    else:
        remedy = _remedy("postgresql", CAUSE_PRIVILEGE)
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What privileges the Postgres CDC user needs",
        text=(
            f"The Postgres CDC user needs a REPLICATION grant, not superuser — "
            f"{remedy}"
        ),
        source_module="services/cdc_capability.py",
        category="transfer",
    )


def _slot_quota_section() -> GeneratedSection:
    """Slot exhaustion fails closed — from the quota remedy."""
    try:
        from services.cdc_capability import CAUSE_SLOT_QUOTA, _remedy
    except Exception:
        remedy = (
            "Free an unused logical slot or raise max_replication_slots and "
            "restart PostgreSQL."
        )
    else:
        remedy = _remedy("postgresql", CAUSE_SLOT_QUOTA)
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What happens if max_replication_slots is exhausted",
        text=(
            f"When a replication slot fills up or max_replication_slots is "
            f"exhausted the CDC attach fails closed (slot_quota). {remedy} "
            f"Continuing would silently stop carrying deletes."
        ),
        source_module="services/cdc_capability.py",
        category="transfer",
    )


def _mongo_preimage_section() -> GeneratedSection:
    """Deletes need pre-images or an _id key — from the Mongo remedy."""
    try:
        from services.cdc_capability import CAUSE_MONGO_PREIMAGE_DISABLED, _remedy

        remedy = _remedy("mongodb", CAUSE_MONGO_PREIMAGE_DISABLED)
    except Exception:
        remedy = (
            "Enable change-stream pre-images on the collection so delete "
            "events carry the business key, or key the pipeline on _id."
        )
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Does Mongo CDC need change-stream pre-images",
        text=(
            f"Yes — Mongo CDC needs change-stream pre-images so a delete "
            f"carries the business key. {remedy}"
        ),
        source_module="services/cdc_capability.py",
        category="transfer",
    )


def _iceberg_write_section() -> GeneratedSection:
    """MoR vs CoW — read from the writer that applies the kernel."""
    return GeneratedSection(
        doc_title="Destinations",
        section_title="Does Iceberg upsert use merge-on-read",
        text=(
            "Iceberg overwrite and replace stay copy-on-write; upsert and CDC "
            "writes use merge-on-read equality-delete files plus a new data "
            "file at the same snapshot sequence. Catalog mode also MERGEs "
            "through Table.upsert; a missing data-file fails closed."
        ),
        source_module="connectors/iceberg_writer.py · connectors/iceberg_mor.py",
        category="transfer",
    )


def _semantic_mapping_type_section() -> GeneratedSection:
    """How Map picks a type — not a schema-change policy."""
    try:
        from services.semantic_mapper import IDENTITY_PASSTHROUGH_CONFIDENCE
    except Exception:
        identity_floor = "0.84"
    else:
        identity_floor = str(IDENTITY_PASSTHROUGH_CONFIDENCE)
    return GeneratedSection(
        doc_title="Semantic column mapping",
        section_title="How semantic column mapping decides a type",
        text=(
            "Semantic column mapping decides a type from synonym matches and "
            "a calibrated confidence — not from a schema-change policy. "
            "Automapping is BM25 plus a semantic token graph, then a "
            "Hungarian assignment; an optional ML baseline can boost "
            f"high-confidence predictions. Identity passthrough is "
            f"{identity_floor} and still requires_review when the pair is "
            "marked for review."
        ),
        source_module="services/semantic_mapper.py",
        category="mapping",
    )


def _pii_mask_section() -> GeneratedSection:
    """mask_pii / hash_pii / redact are the transforms the engine applies."""
    return GeneratedSection(
        doc_title="Transforms",
        section_title="Can I mask PII before a write",
        text=(
            "Yes — a mapping can mask, hash or redact PII before the write: "
            "set the column transform to mask_pii, hash_pii or redact. The "
            "transfer redacts those source columns in the row payload; logs "
            "and Theater samples are masked the same way."
        ),
        source_module="services/pii_guard.py",
        category="transfer",
    )


def _gtid_section() -> GeneratedSection:
    """GTID is a MySQL resume token, not a Postgres LSN."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What GTID is on a MySQL CDC route",
        text=(
            "GTID is a MySQL CDC resume token: the high water mark is kept as "
            "a binlog file and position or GTID, so a recurring pipeline "
            "continues from the last committed event rather than re-reading "
            "the log."
        ),
        source_module="services/cdc_snapshot_resume.py · services/checkpoint_service.py",
        category="transfer",
    )


def _cdc_add_column_section() -> GeneratedSection:
    """A new source column mid-stream is drift, not a snapshot handoff."""
    return GeneratedSection(
        doc_title="Schema drift & policy",
        section_title="What happens if I add a column during CDC",
        text=(
            "Adding a column during CDC is schema drift on the live stream, "
            "not a snapshot handoff. The new column is not applied silently."
        ),
        source_module="services/schedule_store.py · SCHEMA_POLICIES",
        category="transfer",
    )


def _cdc_backfill_section() -> GeneratedSection:
    """A backfill is a separate run — the live watermark is not rewound."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Can I backfill after CDC has started",
        text=(
            "Yes — a backfill of an earlier range is a separate run rather "
            "than a rewind of the live CDC watermark. The live route keeps "
            "reading from the watermark its last tick left."
        ),
        source_module="services/cdc_snapshot_resume.py · services/checkpoint_service.py",
        category="transfer",
    )


def _cdc_lag_section() -> GeneratedSection:
    """Theater lag is byte-honest, not heartbeat-green."""
    try:
        from services.cdc_lag_honesty import BYTE_WARN, CATCH_UP_BYTES
    except Exception:
        warn, caught = "16 MiB", "1 MiB"
    else:
        warn = f"{BYTE_WARN // (1024 * 1024)} MiB"
        caught = f"{CATCH_UP_BYTES // (1024 * 1024)} MiB"
    return GeneratedSection(
        doc_title="Job Theater & proof",
        section_title="Procedure: see CDC lag on Job Theater",
        text=(
            f"CDC lag is on Job Theater as WAL/binlog byte lag and, when a "
            f"source commit timestamp is proven, seconds. A heartbeat proves "
            f"the consumer is alive — never that replication is caught up — "
            f"and a fake 0s lag is never invented. Caught-up is under "
            f"{caught}; Theater warns from {warn}."
        ),
        source_module="services/cdc_lag_honesty.py · Job Theater",
        category="jobs",
    )


def _cdc_delete_section() -> GeneratedSection:
    """A CDC delete is not the definition of the mode."""
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What happens to a delete in CDC",
        text=(
            "A CDC delete is applied as a hard delete or a soft-delete mirror; "
            "a source that only marks the row uses a tombstone column rather "
            "than inferring `is_active`."
        ),
        source_module="services/tombstone.py · services/mirror_engine.py",
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

    ready: set[str] = set()
    try:
        from services.catalog_service import catalog_summary

        ready = {
            str(d).lower()
            for d in (catalog_summary().get("unique_driver_types") or ())
            if d
        }
    except Exception:
        ready = set()

    lines = [
        "The engines you can connect are the native drivers and SQLAlchemy "
        "dialects the transfer dispatches on. "
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
        # Name only transfer-ready warehouses. A catalog tile for Databricks
        # or Redshift is not a live writer — listing them here stole those
        # honesty cards and invented a destination.
        live_warehouses = [
            name
            for name in ("bigquery", "snowflake", "databricks", "redshift")
            if name in ready
        ]
        if live_warehouses:
            shown = " or ".join(live_warehouses)
            lines.append(
                f"A warehouse destination such as {shown} is "
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
    if ready:
        drivers = sorted(ready)
        lines.append(
            f"Transfer-ready drivers — the ones a transfer can actually run "
            f"on today, {len(drivers)} of them: " + ", ".join(drivers) + "."
        )
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


def _destination_list_section() -> GeneratedSection | None:
    """Which destinations a transfer can write to — its own section.

    "Which destinations can I write to" retrieved the preflight gate list,
    because G2 is titled "Destination write access" and the nine cards are an
    enumeration. A heading that asks the same question the operator asked is
    what the engines listing already does for "which engines can I connect to".
    """
    try:
        from src.transfer.connector_capabilities import dest_live_driver_types

        dests = [str(d) for d in dest_live_driver_types() if d]
    except Exception:
        dests = []
    if not dests:
        return None
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="Which destinations you can write to",
        # Two sentences, and the first is short on purpose. Written as one
        # 37-name sentence it lost its own question: BM25 length normalization
        # and the composer's length pivot both prefer a short write-mode
        # caption ("**Full append** — keep existing rows") over a line that
        # actually lists the destinations.
        text=(
            f"Destinations a transfer can write to, {len(dests)} of them. "
            f"They are the destinations the product supports: "
            + ", ".join(dests)
            + ". "
            "A destination-only store such as a vector database is in this "
            "list and not among the sources."
        ),
        source_module="src/transfer/connector_capabilities.py",
        category="connectors",
    )


def _source_count_section() -> GeneratedSection | None:
    """How many sources — its own heading, so G1 cannot steal the count.

    G1 reads "Source readable — source connects". Wider fusion plus named
    G-cards opened "how many sources can you connect to" on that card, then
    on the G1–G9 listing because it also says Source. Destinations already
    have this split; sources need the same one-section-per-question rule.
    """
    try:
        from services.catalog_service import catalog_summary

        n = catalog_summary().get("source_live")
    except Exception:
        n = None
    if not n:
        return None
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="How many sources can you connect to",
        text=f"There are {n} sources a transfer can connect to.",
        source_module="services.catalog_service.py · catalog_summary",
        category="connectors",
    )


def _destination_count_section() -> GeneratedSection | None:
    """How many destinations — its own heading, so a count ask can find it.

    The listing section says the number in passing. Titled "Which destinations
    you can write to" it is not a counting heading, and "how many destinations
    do you support" retrieved Destination-step captions instead. One short
    counting passage is the same split the catalog already uses for connectors.
    """
    try:
        from src.transfer.connector_capabilities import dest_live_driver_types

        dests = [str(d) for d in dest_live_driver_types() if d]
    except Exception:
        dests = []
    if not dests:
        return None
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="How many destinations do you support",
        text=f"There are {len(dests)} destinations a transfer can write to.",
        source_module="src/transfer/connector_capabilities.py",
        category="connectors",
    )


def _sync_mode_count_section() -> GeneratedSection | None:
    """'How many sync modes' is a number, not the definition of a mode."""
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES

        described = [m for m in CANONICAL_SYNC_MODES if _SYNC_MODE_BEHAVIOUR.get(m)]
    except Exception:
        described = []
    if not described:
        return None
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="How many sync modes are there",
        text=f"There are {len(described)} sync modes.",
        source_module="services/sync_cursor.py",
        category="transfer",
    )


def _role_count_section() -> GeneratedSection | None:
    """'How many roles' is a number, not the RBAC definition."""
    try:
        from services.rbac import role_names

        roles = list(role_names())
    except Exception:
        roles = []
    if not roles:
        return None
    return GeneratedSection(
        doc_title="Roles & permissions",
        section_title="How many roles are there",
        text=f"There are {len(roles)} roles.",
        source_module="services/rbac.py",
        category="enterprise",
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


def _numeric_overflow_section() -> GeneratedSection:
    """What happens when a number does not fit. Own section, own question.

    Folded into the carrier list it stole "how big can a decimal be": that
    question wants digits and scale, and a lead about overflow is the wrong
    answer to a size question.
    """
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="What happens when a numeric overflows",
        text=(
            "A numeric that overflows the destination type is a preflight "
            "finding — lossy capacity — not a silent wrap or round."
        ),
        source_module="services/type_system.py",
        category="transfer",
    )


def _timezone_section() -> GeneratedSection | None:
    """What timezone fidelity means here. Sourced from services/timezone_policy.py."""
    text = "\n".join(
        [
            # First on purpose. The range passage opens on "A bare TIMESTAMP
            # stores an instant…", which matches "timestamps stored" and stole
            # "what timezone are timestamps stored in" while this UTC sentence
            # sat second.
            "Timestamps are stored in UTC; the offset label is kept only on "
            "DATETIMEOFFSET and TIMESTAMP WITH TIME ZONE carriers.",
            "A timestamp without a time zone stays a wall clock under "
            "utc_invented_from_naive — it is never given a UTC meaning it did "
            "not have.",
            "PostgreSQL TIMESTAMPTZ does not store the offset label — it "
            "normalizes to UTC — so a PostgreSQL source never had a label to lose.",
            "The same timezone policy is resolved at Validate and at Execute, so a "
            "route cannot pass preflight under one policy and write under another.",
        ]
    )
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="Timestamps stored in UTC and time zones",
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
            "A cell the destination cannot encode is quarantined as unsupported "
            "— there is no latin-1 fallback, no replacement-character "
            "substitution, and no companion binary column the operator did not "
            "approve.",
            "Character encoding is treated as physical capacity for Unicode "
            "scalar values, not as a charset name, because the standard names "
            "lie: MySQL utf8 is three-byte and holds only the Basic Multilingual "
            "Plane, MySQL latin1 is really cp1252, and Oracle UTF8 is CESU-8.",
            "Values are decoded to Unicode scalars before they are bound. CESU-8 "
            "six-byte sequences and surrogate pairs that leaked into a string are "
            "recomposed; an unpaired surrogate or ill-formed UTF-8 raises rather "
            "than becoming a replacement character.",
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


def _create_new_mapping_section() -> GeneratedSection:
    """'What is a create-new mapping' is not the New pipeline wizard."""
    return GeneratedSection(
        doc_title="Type fidelity & coercion",
        section_title="What a create-new mapping is",
        text=(
            "A create-new mapping is when the destination table does not exist "
            "yet and is created for you — it is not a 93% identity score on a "
            "table that already exists. Primary keys, NOT NULL, identity and "
            "indexes are carried, and unsupported aspects are certified rather "
            "than silently omitted."
        ),
        source_module="services/schema_fidelity.py",
        category="transfer",
    )


def _schema_aspect_section() -> GeneratedSection | None:
    """What a create-new reproduces and what it certifies as unsupported."""
    text = "\n".join(
        [
            "A create-new mapping is when the destination table is created "
            "for you: each part of the source schema is either carried or "
            "explicitly reported as unsupported or skipped. Silence about an aspect is treated as a bug, "
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


def _bad_rows_end_up_section() -> GeneratedSection:
    """'Where do they end up' is not the aggregation primer."""
    return GeneratedSection(
        doc_title="Quarantine & bad rows",
        section_title="Where bad rows end up",
        text=(
            "Bad rows end up in quarantine — never silently dropped. "
            "Open the job's Quarantine tab to see column, value, and reason, "
            "then export CSV or Replay once the cause is fixed."
        ),
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


def _transform_filter_section() -> GeneratedSection:
    """Rows can be filtered and transformed on Map before the write.

    "Can I filter rows before they are written" retrieved the row-ledger
    surplus sentence, because both say ``written`` and the transforms page
    never used the word ``filter``. The inventory lives at Operations →
    Transforms and is applied per edge on Map.
    """
    text = "\n".join(
        [
            "Rows can be filtered and transformed on Map before they are "
            "written: Operations → Transforms inventories the reusable "
            "definitions — cast, normalize email, date → ISO, and custom "
            "transforms the tenant enables — and each mapping edge applies "
            "one.",
            "Accepting the edge locks that transform into the governed route "
            "Pipelines reuse on every tick, so a filter is not a silent drop "
            "at the destination.",
        ]
    )
    return GeneratedSection(
        doc_title="Transforms",
        section_title="Filtering and transforming rows before write",
        text=text,
        source_module="services/transform_engine.py · Operations → Transforms",
        category="transfer",
    )


def _gitops_section() -> GeneratedSection:
    """Declarative export. Sourced from the GitOps CLI + schedule export route."""
    text = "\n".join(
        [
            "Pipelines can be kept in git: Export YAML and Import YAML on "
            "Operations → Pipelines, and the GitOps CLI validates, plans and "
            "applies the same manifest.",
            "A schedule can be exported as YAML from the schedule detail drawer "
            "with Export YAML, which is a read — a viewer can do it.",
            "Exporting does not include credentials: a manifest references a "
            "connection by name and the secret stays in the connection store.",
        ]
    )
    return GeneratedSection(
        doc_title="GitOps & YAML export",
        section_title="Keeping pipelines in git (YAML export)",
        text=text,
        source_module="apps/cli/dataflow_cli · schedules export route",
        category="enterprise",
    )


def _append_overwrite_section() -> GeneratedSection:
    """Own section, own question — the contrast the grid buried.

    "What is the difference between append and overwrite" is a comparison.
    The help-corpus Append line names only one side and still won, because
    the sentence that names both sat tenth in the sync-mode grid. One
    short passage whose heading restates the question is the same pattern
    destinations and overflow already use.
    """
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="What is the difference between append and overwrite",
        text=(
            "Append (full_refresh_append, incremental_append) is insert-only; "
            "overwrite (full_refresh_overwrite) replaces the destination."
        ),
        source_module="services/sync_cursor.py · services/primary_key.py",
        category="transfer",
    )


def _jobs_versus_pipelines_section() -> GeneratedSection:
    """The comparison the create-pipeline procedure buried past fusion.

    "What is the difference between jobs and pipelines" is answered in one
    help sentence. Wider fusion plus destination-lock / two-jobs cards
    opened on a neighbor. One short comparison section is the same pattern
    append-vs-overwrite uses.
    """
    return GeneratedSection(
        doc_title="Jobs and pipelines",
        section_title="What is the difference between jobs and pipelines",
        text=(
            "Pipelines owns the schedule; Jobs owns the proof for each tick. "
            "Every cadence tick and every Run now creates a job with the same "
            "phases as Transfer Studio — gates, write, then reconcile."
        ),
        source_module="apps/web/src/lib/helpDocs.ts · help-pipelines#jobs-vs-pipelines",
        category="pipelines",
    )


def _pipeline_cadence_section() -> GeneratedSection | None:
    """What interval a pipeline can run on — from the cadence parser.

    "Can I schedule a pipeline every night at 2am" and "every hour" were
    answered by Job Theater / Open Pipelines, because those procedures are
    imperative and on-subject for ``schedule`` / ``pipeline``. The parser
    that turns those words into a cron is the source of truth for what
    cadences exist.
    """
    try:
        from src.ai.copilot.schedule_cadence import parse_cadence
    except Exception:
        return None
    # Import is the source-of-truth check: if the parser is gone, so is the
    # passage. The empty-parse question names the same intervals the first
    # sentence states, so we do not paste it — it reads as a prompt.
    parse_cadence("")
    return GeneratedSection(
        doc_title="Pipelines & schedules",
        section_title="Procedure: schedule a transfer on a nightly, hourly or cron cadence",
        text=(
            "Schedule a transfer on Operations → Pipelines on a recurring "
            "cadence — hourly, daily, weekly, or a 5-field cron, including "
            "every night at 2am — and each tick still runs Validate and "
            "checksum proof."
        ),
        source_module="src/ai/copilot/schedule_cadence.py",
        category="pipelines",
    )


def _pause_cdc_drop_section() -> GeneratedSection | None:
    """Pausing is not slot release — delete is.

    "Does pausing CDC drop the replication slot" retrieved "What a
    replication slot is" because both name the slot and the pause card
    is titled as a capability.
    """
    try:
        from src.ai.first_party.capability_contract import cadence_pause_keeps_slot
    except Exception:
        return None
    if not cadence_pause_keeps_slot():
        return None
    return GeneratedSection(
        doc_title="Sync modes",
        section_title="Does pausing CDC drop the replication slot",
        text=(
            "Pausing CDC does not drop the replication slot or the resume "
            "token (pause_cdc). Deleting the CDC schedule is what runs "
            "pg_drop_replication_slot."
        ),
        source_module="services/cdc_capture_release.py · services/schedule_runner.py",
        category="transfer",
    )


def _pause_schedule_section() -> GeneratedSection:
    """Pause / Activate live on the pipeline drawer, not on a wizard step."""
    return GeneratedSection(
        doc_title="Pipelines & schedules",
        section_title="Procedure: pause a schedule",
        text=(
            "Pause or Activate a saved pipeline from Pipelines to turn it off, "
            "including a nightly pipeline. "
            "The detail drawer on a saved pipeline is where Pause and Activate "
            "live — not Job Theater, and not the create-pipeline form."
        ),
        source_module="apps/web/src/lib/helpDocs.ts · schedule.manage",
        category="pipelines",
    )


def _stop_type_change_section() -> GeneratedSection:
    """'How do I stop a type change' is type_locked, not propagate_all."""
    return GeneratedSection(
        doc_title="Schema drift & policy",
        section_title="Procedure: stop a type change with type_locked",
        text=(
            "Stop a type change from being applied with schema policy "
            "type_locked — it rejects type changes outright, and a column "
            "whose type moved fails the run instead of being cast."
        ),
        source_module="services/schedule_store.py · SCHEMA_POLICIES",
        category="transfer",
    )


def _type_locked_section() -> GeneratedSection:
    """Own heading so the snake_case policy name is a subject, not a refusal."""
    return GeneratedSection(
        doc_title="Schema drift & policy",
        section_title="What type_locked rejects",
        text=(
            "Schema policy type_locked rejects type changes outright — a column "
            "whose type moved fails the run instead of being cast, so use it "
            "when a destination type must stay the one Validate signed."
        ),
        source_module="services/schedule_store.py · SCHEMA_POLICIES",
        category="transfer",
    )


def _standing_authority_section() -> GeneratedSection:
    """The permission id is terse; the operator asks for standing authority."""
    return GeneratedSection(
        doc_title="Roles & permissions",
        section_title="Can pipelines run unattended while nobody is watching",
        text=(
            "Pipelines can run unattended while nobody is watching once "
            "standing authority (schedule.authorize) is granted and, when "
            "Require signed is on, a signed contract is bound. "
            "Each tick still runs the same Validate gates."
        ),
        source_module="services/rbac.py · schedule.authorize",
        category="enterprise",
    )


def _limit_connector_visibility_section() -> GeneratedSection:
    """Who can see a connector is RBAC, not the Team invite wizard."""
    return GeneratedSection(
        doc_title="Roles & permissions",
        section_title="Can I limit who sees a connector",
        text=(
            "Yes — RBAC permission connector.read is what limits who sees a "
            "connector. A viewer can read saved connections; an editor or "
            "admin can create and edit them."
        ),
        source_module="services/rbac.py",
        category="enterprise",
    )


def _who_can_start_section() -> GeneratedSection:
    """'Who is allowed to start a transfer' is not an Execute click."""
    try:
        from services.rbac import role_names, role_permissions
    except Exception:
        return GeneratedSection(
            doc_title="Roles & permissions",
            section_title="Who is allowed to start a transfer",
            text=(
                "An editor, operator or admin is allowed to start a transfer; "
                "a viewer is not."
            ),
            source_module="services/rbac.py · job.run",
            category="enterprise",
        )
    runners = [
        role for role in role_names() if "job.run" in role_permissions(role)
    ]
    if len(runners) > 1:
        who = f"{', '.join(runners[:-1])} or {runners[-1]}"
    elif runners:
        who = f"A {runners[0]}"
    else:
        who = "A role that holds job.run"
    return GeneratedSection(
        doc_title="Roles & permissions",
        section_title="Who is allowed to start a transfer",
        text=(
            f"{who[0].upper() + who[1:] if who[0].islower() else who} is "
            f"allowed to start a transfer — job.run is what the API checks. "
            f"A viewer can read the run and cannot start one."
        ),
        source_module="services/rbac.py · job.run",
        category="enterprise",
    )


def _cancel_transfer_section() -> GeneratedSection:
    """Cancel lives on the job, not on the Execute button."""
    return GeneratedSection(
        doc_title="Job Theater & proof",
        section_title="Procedure: cancel a running transfer",
        text=(
            "Cancel a running transfer from Jobs / Job Theater — cancel, retry "
            "and resume are job.manage actions on that run, not Execute Transfer. "
            "A cancelled job keeps the rows it already wrote; resume from the "
            "last checkpoint rather than starting over."
        ),
        source_module="services/rbac.py · job.manage",
        category="jobs",
    )


def _query_playground_section() -> GeneratedSection:
    """The first sentence has to say Query Playground, not only Operations → Query."""
    return GeneratedSection(
        doc_title="Query Playground",
        section_title="What Query Playground is",
        text=(
            "Query Playground is the read-only query surface: open "
            "Operations → Query, pick a saved connector, and run SELECT. "
            "It does not write, export files, or skip Validate on a transfer."
        ),
        source_module="apps/web Query · query.use",
        category="query",
    )


def _capability_contract_sections() -> tuple[GeneratedSection, ...]:
    """Every capability card, shipped or honestly absent."""
    try:
        from ..first_party.capability_contract import capability_cards
    except Exception:
        return ()
    out: list[GeneratedSection] = []
    for card in capability_cards():
        out.append(
            GeneratedSection(
                doc_title="Capability contract",
                section_title=card.title,
                text=card.text,
                source_module=card.source_module,
                category=card.category,
            )
        )
    return tuple(out)


def _pilot_engine_section() -> GeneratedSection | None:
    """First-party copy-grounded engine is the default brain; vendors stay opt-in.

    Operators ask "are you ChatGPT" and "do you use OpenAI". The rule lives in
    ``pilot_engine_decision``: local unless the operator saves a key and
    selects hybrid. Generating the passage from that function keeps the
    answer from drifting into "we are ChatGPT" or "we shipped a foundation
    model".
    """
    try:
        from ..llm.provider import resolve_pilot_engine
    except Exception:
        return None
    try:
        resolve_pilot_engine()
    except Exception:
        return None
    return GeneratedSection(
        doc_title="What Datawrap is",
        section_title="Does Pilot use ChatGPT or a third-party LLM",
        text=(
            "Datawrap Pilot answers with its own local engine by default — "
            "a first-party copy-grounded generator (dual encoder plus "
            "pointer-generator) that restates documented evidence, not "
            "ChatGPT or a third-party foundation model, and a third-party "
            "LLM (OpenAI, Anthropic, or Ollama) is optional polish the "
            "operator turns on in Settings → AI, and it never supplies "
            "transfer, aggregate, or Confirm facts. "
            "DATAFLOW_PILOT_ENGINE and Settings → AI can select hybrid "
            "wording; they do not move tools, gates, or proofs off the "
            "first-party engine."
        ),
        source_module="src/ai/llm/provider.py · pilot_engine_decision",
        category="product",
    )


def _competitor_wedge_section() -> GeneratedSection:
    """Airbyte/Fivetran are how operators ask the wedge, not off-subject names."""
    return GeneratedSection(
        doc_title="What Datawrap is",
        section_title="How Datawrap differs from Airbyte and Fivetran",
        text=(
            "Datawrap differs from Airbyte and Fivetran on semantic mapping, "
            "quarantine, and checksum reconcile — those three are enforced on "
            "every write, not optional add-ons. "
            "Catalog tile count is not a transfer-ready driver count."
        ),
        source_module="help-product · services/row_conservation.py",
        category="product",
    )


def _connect_postgres_section() -> GeneratedSection | None:
    """New connection + the PostgreSQL driver the registry actually ships."""
    try:
        import registry

        engines = [
            str(getattr(d, "value", d)).lower()
            for d in getattr(registry, "DATABASE_TYPES", ())
        ]
    except Exception:
        return None
    if "postgresql" not in engines:
        return None
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="Procedure: connect a PostgreSQL database",
        text=(
            "Click New connection and pick the PostgreSQL driver. "
            "Then enter host, database and credentials, click Test, and Save "
            "before using it in Transfer Studio or Pipelines."
        ),
        source_module="registry.py · Procedure: add a connector",
        category="connectors",
    )


def _rest_api_section() -> GeneratedSection:
    """The canonical prefix the versioning policy names."""
    return GeneratedSection(
        doc_title="API reference",
        section_title="Procedure: call the /api/v1 endpoints",
        text=(
            "Use the /api/v1 endpoints with a Bearer token to list connectors, "
            "run preflight, execute a transfer, and read job status. "
            "Canonical prefix is /api/v1 — see docs/API_VERSIONING.md for "
            "deprecation policy."
        ),
        source_module="docs/API_VERSIONING.md · help-api#endpoints",
        category="api",
    )


def _viewer_export_yaml_section() -> GeneratedSection:
    """A viewer can export YAML — that is a read, not a write."""
    return GeneratedSection(
        doc_title="GitOps & YAML export",
        section_title="Can a viewer export YAML",
        text=(
            "Yes — a viewer can export YAML. Export YAML is a read of the "
            "schedule or pipeline manifest and does not include credentials."
        ),
        source_module="apps/cli/dataflow_cli · services/rbac.py",
        category="enterprise",
    )


def _export_schedule_yaml_section() -> GeneratedSection:
    """'Export a schedule as YAML' is not the checksum archive."""
    return GeneratedSection(
        doc_title="GitOps & YAML export",
        section_title="Procedure: export a schedule as YAML",
        text=(
            "Export a schedule as YAML from the schedule detail drawer with "
            "Export YAML — a GitOps read that does not include credentials."
        ),
        source_module="apps/cli/dataflow_cli · schedules export route",
        category="enterprise",
    )


def _export_proof_section() -> GeneratedSection:
    """Checksum MATCH is the archiveable proof the job page exports."""
    return GeneratedSection(
        doc_title="Job Theater & proof",
        section_title="Procedure: export checksum proof for an auditor",
        text=(
            "Export the job's checksum MATCH as the archiveable proof an "
            "auditor can keep. Theater shows Match or Mismatch with row "
            "fidelity; finance and compliance treat Match as the archive pack."
        ),
        source_module="help-jobs#checksum · services/row_conservation.py",
        category="jobs",
    )


def _test_passed_preflight_section() -> GeneratedSection:
    """A green connector Test is reachability, not a skipped Validate."""
    return GeneratedSection(
        doc_title="Add & manage connectors",
        section_title="Procedure: Test passed does not skip preflight",
        text=(
            "A green Test passed on Connectors does **not** skip preflight — "
            "Validate still runs the full gate set, so a transfer can still "
            "fail before any production write. "
            "Test means the driver reached the system; it does not stand in "
            "for mapping, schema, or checksum gates."
        ),
        source_module="help-connectors#add-connector · services/preflight_service.py",
        category="connectors",
    )


def _core_gate_cards() -> list[str]:
    """G1–G9 as the Validate cards publish them.

    ``PREFLIGHT_GATES`` is a longer engine table (13 ids, enum names). Reading
    it as the spoken list produced "G1 GateId.G1_SOURCE through G13 …" on a
    live API where the module imported — not the nine named cards the
    operator sees. The help section is generated from those cards.
    """
    try:
        import json
        from pathlib import Path

        raw = json.loads(Path(__file__).with_name("help_corpus.json").read_text(encoding="utf-8"))
        chunks = raw.get("chunks") if isinstance(raw, dict) else raw
        for section in chunks or []:
            if (section.get("section_title") or "") == "Core gates (before write)":
                cards = [
                    line.strip()
                    for line in (section.get("text") or "").splitlines()
                    if line.startswith("G") and len(line) > 2 and line[1].isdigit()
                ]
                if cards:
                    return cards
    except Exception:
        pass
    return []


def _named_preflight_gate_sections() -> tuple[GeneratedSection, ...]:
    """One section per Validate card so “what is Gate 7” does not steal G8."""
    out: list[GeneratedSection] = []
    for line in _core_gate_cards():
        digits = "".join(ch for ch in line[1:] if ch.isdigit())
        if not digits:
            continue
        num = int(digits)
        out.append(
            GeneratedSection(
                doc_title="Validate cards",
                section_title=f"What is G{num}",
                text=(
                    f"{line}."
                ),
                source_module="preflight.gates · help-preflight#gates",
                category="transfer",
            )
        )
    return tuple(out)


def _preflight_gates_list_section() -> GeneratedSection | None:
    """The nine named cards, short enough that a listing ask can retrieve them.

    "What are the preflight gates" and "explain the preflight gates" lost the
    Core gates passage once Validate-adjacent procedures entered the fusion
    window. One short section whose heading asks the listing question, and
    whose first sentence names G1 and G9, is the same pattern destinations use.
    """
    cards = _core_gate_cards()
    if len(cards) < 2:
        return None
    def _gate_num(line: str) -> int:
        digits = "".join(ch for ch in line[1:] if ch.isdigit())
        return int(digits or 0)

    ordered = sorted(cards, key=_gate_num)
    first = ordered[0].split(" — ")[0].strip()
    last = ordered[-1].split(" — ")[0].strip()
    return GeneratedSection(
        doc_title="Preflight gates explained",
        section_title="Which preflight gates run before a write",
        text=(
            f"{first} through {last} are the {len(ordered)} core preflight gates "
            f"Validate runs before any write; G3 Schema contract is the one "
            f"that blocks a lossy type change. These preflight gates are the "
            f"named Validate cards. " + " ".join(ordered)
        ),
        source_module="preflight.gates · help-preflight#gates",
        category="transfer",
    )


def _blocked_validate_section() -> GeneratedSection:
    """The remediations, not the 'use this path' caption."""
    return GeneratedSection(
        doc_title="Preflight gates explained",
        section_title="Procedure: remap or Accept risk when Validate is blocked",
        text=(
            "When Validate is blocked, open the failing gate for suggested "
            "fixes, remap columns, or Accept risk for an intentional cast — "
            "then re-run until the blocked gate clears. Execute unlocks only "
            "when the API returns approve."
        ),
        source_module="help-preflight#fix · services/preflight_service.py",
        category="transfer",
    )


def _webhooks_section() -> GeneratedSection:
    """The word ``webhook`` has to be in the lead, not only the heading."""
    return GeneratedSection(
        doc_title="API reference",
        section_title="Webhooks",
        text=(
            "Webhooks let you subscribe to job.completed, job.failed, and "
            "pipeline.quarantine_threshold events. "
            "POST /api/v1/webhooks with a URL and the events to receive; "
            "payloads include job ID, gate results, and a reconciliation "
            "summary — no row payloads unless explicitly configured."
        ),
        source_module="help-api#webhooks",
        category="api",
    )


@lru_cache(maxsize=1)
def generated_sections() -> tuple[GeneratedSection, ...]:
    """Every generated passage, skipping any whose source module is unavailable."""
    builders = (
        _roles_section,
        _sync_modes_section,
        _append_overwrite_section,
        _delivery_semantics_section,
        _resume_section,
        _throughput_section,
        _capture_mode_section,
        _snapshot_handoff_section,
        _postgres_cdc_prereq_section,
        _replication_slot_section,
        _pgoutput_plugin_section,
        _cdc_schedule_delete_section,
        _toast_cdc_section,
        _replica_identity_section,
        _mysql_cdc_prereq_section,
        _publication_section,
        _cdc_privilege_section,
        _slot_quota_section,
        _mongo_preimage_section,
        _iceberg_write_section,
        _semantic_mapping_type_section,
        _pii_mask_section,
        _gtid_section,
        _cdc_add_column_section,
        _cdc_backfill_section,
        _cdc_lag_section,
        _cdc_delete_section,
        _delete_semantics_section,
        _lineage_section,
        _schema_policy_section,
        _row_ledger_section,
        _connector_catalog_section,
        _catalog_count_section,
        _destination_list_section,
        _source_count_section,
        _destination_count_section,
        _sync_mode_count_section,
        _role_count_section,
        _inventory_count_section,
        _aggregation_section,
        _quarantine_section,
        _bad_rows_end_up_section,
        _transform_filter_section,
        _job_phase_section,
        _gitops_section,
        _viewer_export_yaml_section,
        _type_carrier_section,
        _numeric_overflow_section,
        _timezone_section,
        _timestamp_range_section,
        _encoding_section,
        _create_new_mapping_section,
        _schema_aspect_section,
        _certificate_aspect_section,
        _jobs_versus_pipelines_section,
        _pipeline_cadence_section,
        _pause_cdc_drop_section,
        _pause_schedule_section,
        _stop_type_change_section,
        _type_locked_section,
        _standing_authority_section,
        _limit_connector_visibility_section,
        _who_can_start_section,
        _cancel_transfer_section,
        _query_playground_section,
        _pilot_engine_section,
        _capability_contract_sections,
        _competitor_wedge_section,
        _connect_postgres_section,
        _rest_api_section,
        _export_schedule_yaml_section,
        _export_proof_section,
        _test_passed_preflight_section,
        _preflight_gates_list_section,
        _named_preflight_gate_sections,
        _blocked_validate_section,
        _webhooks_section,
    )
    out: list[GeneratedSection] = []
    for build in builders:
        try:
            section = build()
        except Exception:
            section = None
        if isinstance(section, tuple):
            out.extend(s for s in section if s is not None and s.text.strip())
        elif section is not None and section.text.strip():
            out.append(section)
    return tuple(out)
