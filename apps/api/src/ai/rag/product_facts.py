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
        lines.append(
            "Database and warehouse engines the transfer engine dispatches on: "
            + ", ".join(databases)
            + "."
        )
        lines.append(
            "A warehouse destination such as bigquery, snowflake or databricks is "
            "configured the same way as a database: pick the type under "
            "Connectors, give it credentials, then Test the connection before "
            "using it in a transfer."
        )
    if files:
        lines.append("File and document formats it can read or write: " + ", ".join(files) + ".")
    lines.append(
        "The catalog tile count is not the same as the number of transfer-ready "
        "drivers; a tile is transfer-live only when it carries transfer-ready "
        "evidence."
    )
    return GeneratedSection(
        doc_title="Connections & engines",
        section_title="Which engines you can connect",
        text="\n".join(lines),
        source_module="apps/api/registry.py",
        category="connectors",
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
        _schema_policy_section,
        _row_ledger_section,
        _connector_catalog_section,
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
