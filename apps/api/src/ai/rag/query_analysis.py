"""Query understanding — the stage that was missing between an operator's words and retrieval.

Retrieval ran on the operator's raw tokens, so a question only matched when they
happened to type the documentation's vocabulary. "What happens to bad rows"
retrieved nothing because the corpus says *rejected* and *quarantine*; "how do I
see why a job was slow" retrieved nothing because nothing in it is a corpus term
except ``job``. Both are ordinary operator questions about documented behaviour.

This module turns one question into the three things the rest of the pipeline
needs:

``ask``       what kind of answer is wanted (a definition, a procedure, a
              diagnosis, a capability check, a comparison, an enumeration, a
              count). The
              composer uses this to pick sentences an operator would actually
              read first — a definition for "what is", steps for "how do I".

``terms``     the content terms that carry retrieval signal, with the generic
              discourse words ("happens", "see", "want") removed. Those words
              are why an off-subject question could look half-documented: every
              one of them is in the corpus somewhere.

``expansions`` the documentation's own vocabulary for what the operator said.
              Expansion is keyed on exact operator surface forms, so it widens
              recall for product questions without giving an off-subject
              question a foothold — "cook" and "rice" expand to nothing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache

from .lexical_index import content_terms, identifier_shingles, normalize

# Words that appear in operator questions and in almost every passage, so they
# add no retrieval signal — but which, left in, make an undocumented question
# look partly covered. ``lexical_index.STOPWORDS`` holds the closed-class words
# (articles, pronouns, prepositions); this holds the open-class discourse verbs
# and nouns that behave the same way in a question.
#
# Domain words never appear here. ``map``, ``run``, ``job``, ``write`` and
# ``mode`` are the subject of this product, not filler.
GENERIC_QUESTION_WORDS = frozenset(
    normalize(w)
    for w in """
    happen happens occur occurs work works working worked
    handle handles handled handling deal deals dealt
    treat treats treated support supports supported
    get gets getting got see seeing seen look looking
    want wants need needs needed like likes
    know knows knowing find finds finding
    make makes making take takes taking give gives giving put puts
    come comes go goes going going keep keeps
    say says said tell tells told ask asks asked answer answers
    let lets letting allow allows allowed
    thing things stuff way ways kind kinds sort sorts type_of
    able supposed actually really exactly simply just basically
    possible possibly maybe perhaps
    good bad better best worse worst
    right wrong correct incorrect
    lot lots much many more most less least
    please kindly thanks thank
    someone somebody anyone anybody everyone everybody
    something anything everything nothing
    now today currently
    guide guides tips tip example examples
    question questions asked frequently related optional checklist
    procedure step steps first second next last
    different same other another
    difference differences between compare compares comparison versus
    """.split()
)

# A generic word can still be a useful *expansion trigger* even though it is a
# useless anchor: "bad" tells us nothing on its own but does point at the
# quarantine vocabulary, and "allowed" points at the role model. So expansion
# looks words up before the generic filter runs, while ``terms`` — the set
# coverage and subject anchoring are measured on — stays filtered.

_ASK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "definition",
        re.compile(
            r"^\s*(?:so\s+)?(?:what(?:'s| is| are| does)|what\s+do\s+you\s+mean|"
            r"define|meaning\s+of|tell\s+me\s+about|"
            # "explain quarantine" is a definition. "explain the preflight
            # gates" is the G1–G9 list — leave that for enumeration so a
            # single G-card does not take the definitional +3.2.
            r"explain(?:\s+what)?(?!\s+(?:the\s+)?(?:preflight\s+)?"
            r"(?:gates|modes|roles|connectors|destinations)\b))\b"
            r"|\bwhat\s+(?:is|are)\s+(?:a|an|the)?\s*\w+\s*\??$"
            r"|\bmean(?:s|ing)?\s*\??$",
            re.I,
        ),
    ),
    (
        # A cardinality question, which is not the enumeration it looks like.
        # "How many connectors do you support" matched ``capability`` on "do you
        # support" and was answered with "Open Platform → Connectors" followed
        # by the transfer-readiness legend — navigation and four labels, and not
        # one number, while the catalog passage two sections over states every
        # count the product publishes. Listed first because the phrasings below
        # also match ``capability``, ``procedure`` and ``enumeration``, and the
        # shape they ask for is the narrowest of the four.
        "count",
        re.compile(
            r"\bhow\s+(?:many|much)\b"
            r"|\b(?:number|count|total)\s+of\b"
            r"|\bhow\s+big\b",
            re.I,
        ),
    ),
    (
        "comparison",
        re.compile(
            r"\b(?:difference\s+between|vs\.?|versus|compared\s+to|"
            r"which\s+(?:one\s+)?(?:should|is\s+better)|"
            r"better\s+than|instead\s+of)\b"
            # The noun sits between the interrogative and the verb — "which
            # *sync mode* should I pick", "which *mode* is better for a big
            # table". Without it the enumeration rule below claimed both, and a
            # request for a recommendation was answered with the whole list.
            r"|\bwhich\s+(?:\w+\s+){1,3}"
            r"(?:should|is\s+better|works?\s+better|do\s+you\s+recommend)\b",
            re.I,
        ),
    ),
    (
        "diagnosis",
        re.compile(
            r"\bwhy\s+(?:did|does|is|are|was|were|do|am|can'?t|cannot|won'?t)\b"
            r"|\b(?:fail|fails|failed|failing|failure|error|errors|broke|broken|stuck|"
            r"blocked|refused|rejected|mismatch|crash|crashed|timeout|timed\s+out)\b"
            r"|\bnot\s+working\b|\bwhat\s+went\s+wrong\b",
            re.I,
        ),
    ),
    (
        # A request to be told what there is. Ahead of ``procedure`` and
        # ``capability`` because the verb an operator hangs a listing question
        # on is usually one of theirs: "which engines can I *connect* to" was a
        # procedure and opened on the steps for connecting Cursor to MCP, and
        # "what sync modes do you *support*" was a capability and opened on the
        # definition of a sync mode instead of naming one.
        #
        # The head noun has to be **plural**. The old rule accepted the
        # singular, which made "what is a sync mode" an enumeration — a
        # definition answered with a list — and the interrogative has to open
        # the question, so "how do I list my connectors" stays a procedure.
        "enumeration",
        re.compile(
            r"^\s*(?:so\s+)?(?:what|which|list|show\s+me|tell\s+me)\b[^?]*?\b"
            r"(?:modes|options|types|gates|roles|connectors|connections|kinds|"
            r"policies|phases|steps|permissions|engines|formats|destinations|"
            r"sources|warehouses|databases|guarantees|limits)\b"
            r"|^\s*(?:please\s+)?(?:list|show\s+me)\s+(?:all\s+)?"
            r"(?:the\s+|my\s+|your\s+)?"
            r"(?:modes|gates|roles|options|types|connectors|policies)\b"
            # "Which gate blocks a lossy type change" picks one member out of a
            # set, and the answer is in the same list as the whole set — so the
            # singular is allowed here, where ``which`` opens the question and
            # cannot be introducing a definition the way "what is" does.
            r"|^\s*which\s+(?:\w+\s+){0,3}"
            r"(?:mode|option|type|gate|role|connector|policy|phase|step|"
            r"permission|engine|format)\b"
            r"|\ball\s+(?:the\s+)?(?:modes?|gates?|roles?|options?|types?)\b"
            # "Who can run transfers" is a request for the roles that hold a
            # verb, not a definition of running. Left as ``other`` it retrieved
            # the role matrix and opened on the viewer-negative PII sentence.
            r"|^\s*explain\s+(?:the\s+)?(?:preflight\s+)?"
            r"(?:modes|options|types|gates|roles|connectors|connections|"
            r"kinds|policies|phases|steps|permissions|engines|formats|"
            r"destinations|sources|warehouses|databases|guarantees|limits)\b"
            r"|^\s*who\s+can\b"
            r"|^\s*who\s+is\s+allowed\b"
            # A role plus a generic permission verb is a request for the
            # role matrix. "Can a viewer export YAML" names a product
            # action, not the list of roles, and must stay a capability.
            r"|^\s*can\s+a(?:n)?\s+(?:viewer|editor|admin|operator|approver)\s+"
            r"(?:start|run|approve|accept|authorize|create|delete|cancel|retry)\b",
            re.I,
        ),
    ),
    (
        # "Can a viewer export YAML" is a yes/no about one permission, not
        # the role matrix and not the export wizard. ``export`` in the
        # procedure rule below would otherwise open on the GitOps steps.
        "capability",
        re.compile(
            r"^\s*can\s+a(?:n)?\s+(?:viewer|editor|admin|operator|approver)\b",
            re.I,
        ),
    ),
    (
        # An outcome, not a procedure and not a diagnosis. "What happens to a
        # timestamp without timezone" matched nothing and opened on the
        # framing sentence of the timezone passage; "what happens if the
        # destination count does not match" opened on a delete-polarity
        # sentence because both say "destination count". The composer pays
        # this ask for a sentence that states the outcome (quarantined,
        # unbalanced, wall clock), and the heading prior prefers the
        # fidelity / proof passages over a wizard step.
        "consequence",
        re.compile(
            r"\bwhat\s+happens\b"
            r"|\bwhat\s+if\b"
            r"|\bwhat\s+(?:do\s+you\s+do|does\s+(?:it|the\s+\w+))\s+when\b",
            re.I,
        ),
    ),
    (
        "procedure",
        re.compile(
            r"\bhow\s+(?:do|can|would|should)\s+(?:i|we|you)\b"
            r"|\bhow\s+to\b|\bhow\s+do\s+i\b"
            r"|\b(?:steps?|walk\s+me\s+through|set\s+up|setup|configure|"
            r"enable|create|add|connect|schedule|export|import)\b",
            re.I,
        ),
    ),
    (
        "capability",
        re.compile(
            r"^\s*(?:can|could|does|do|is|are|will|would|should)\s+"
            r"(?:you|i|we|it|this|datawrap|dataflow|pilot|the\s+\w+)\b"
            r"|\bdo\s+you\s+support\b|\bis\s+it\s+possible\b|\bsupported\b"
            r"|^\s*is\s+[\w .+/-]{2,48}\s+a\s+(?:source\s+|destination\s+)?connector\b",
            re.I,
        ),
    ),
)

# The documentation's own words for things operators say differently. Each entry
# maps operator surface forms to the corpus vocabulary that covers the same
# concept. Every target term is a word the shipped documentation or the product's
# own enums actually use — this is a translation table, not invented knowledge.
_CONCEPT_EXPANSIONS: dict[str, tuple[str, ...]] = {
    # Rejected rows / quarantine
    "bad": ("reject", "quarantine", "invalid"),
    "broken": ("reject", "quarantine", "invalid"),
    "dirty": ("reject", "quarantine", "invalid"),
    "dropped": ("reject", "quarantine", "unaccounted"),
    "lost": ("reject", "quarantine", "unaccounted", "loss"),
    "discarded": ("reject", "quarantine"),
    "rejected": ("quarantine", "reject", "dlq"),
    "quarantined": ("quarantine", "reject", "dlq"),
    "dlq": ("quarantine", "reject", "dead", "letter"),
    "replay": ("quarantine", "reject", "retry"),
    "reprocess": ("quarantine", "replay", "retry"),
    # Reconciliation / proof
    "ledger": ("reconciliation", "checksum", "accounting", "conservation", "proof"),
    "accounting": ("reconciliation", "ledger", "checksum"),
    "balance": ("reconciliation", "ledger", "checksum", "conservation"),
    "balanced": ("reconciliation", "ledger", "checksum", "conservation"),
    "verify": ("reconciliation", "checksum", "proof", "validate"),
    "verified": ("reconciliation", "checksum", "proof"),
    "audit": ("reconciliation", "proof", "audit"),
    "reconcile": ("reconciliation", "checksum", "proof"),
    "proof": ("reconciliation", "checksum", "proof", "evidence"),
    # Performance / duration
    "slow": ("phas", "duration", "throughput", "performance", "theater"),
    "fast": ("throughput", "performance", "duration"),
    "latency": ("phas", "duration", "throughput", "performance"),
    "performance": ("phas", "duration", "throughput", "theater"),
    "speed": ("throughput", "duration", "performance"),
    "hang": ("phas", "stuck", "theater", "timeout"),
    "hanging": ("phas", "stuck", "theater", "timeout"),
    # Scheduling
    "nightly": ("schedule", "pipeline", "cron", "recurr", "daily"),
    # "every night at 2am" / "every hour" are how operators say nightly/hourly.
    # Without these, those words never reached the cadence vocabulary and
    # "can I schedule a pipeline to run every night" retrieved Job Theater.
    "night": ("schedule", "pipeline", "cron", "recurr", "daily", "nightly"),
    "hour": ("schedule", "pipeline", "cron", "recurr", "hourly"),
    "hourly": ("schedule", "pipeline", "cron", "recurr"),
    "daily": ("schedule", "pipeline", "cron", "recurr"),
    "weekly": ("schedule", "pipeline", "cron", "recurr"),
    "cron": ("schedule", "pipeline", "recurr", "cadence"),
    "cadence": ("schedule", "pipeline", "cron", "interval"),
    "recurring": ("schedule", "pipeline", "recurr", "cron"),
    "automate": ("schedule", "pipeline", "recurr"),
    "automated": ("schedule", "pipeline", "recurr"),
    "unattended": ("schedule", "pipeline", "authorization"),
    "watching": ("schedule", "pipeline", "authorization", "unattended"),
    "standing": ("schedule", "pipeline", "authorization", "unattended"),
    "pause": ("schedule", "pipeline", "enabled", "paused"),
    "resume": ("schedule", "pipeline", "enabled", "resume"),
    "handoff": ("snapshot", "stream", "capture", "lsn"),
    "wal": ("log", "capture", "postgres", "write"),
    "wal_level": ("logical", "cdc", "postgres", "wal"),
    "pgoutput": ("plugin", "logical", "cdc", "postgres"),
    "toast": ("pgoutput", "cdc", "unchanged"),
    "lag": ("watermark", "theater", "cdc", "wal"),
    "publication": ("publication", "slot", "cdc", "postgres"),
    "gtid": ("gtid", "binlog", "mysql", "watermark"),
    "binlog": ("binlog", "mysql", "cdc", "row"),
    "preimage": ("preimage", "changestream", "mongo", "delete"),
    "backfill": ("backfill", "watermark", "cdc", "resume"),
    "mask": ("pii", "redact", "mask", "hash"),
    "redact": ("pii", "redact", "mask"),
    "tombstone": ("delete", "soft", "cdc"),
    "create-new": ("schema", "certificate", "identity"),
    # Permissions
    "permission": ("role", "rbac", "permission", "viewer", "editor", "admin"),
    "permissions": ("role", "rbac", "permission", "viewer", "editor", "admin"),
    "rbac": ("role", "permission", "viewer", "editor", "admin"),
    "viewer": ("role", "permission", "rbac", "read"),
    "editor": ("role", "permission", "rbac"),
    "operator": ("role", "permission", "rbac"),
    "admin": ("role", "permission", "rbac", "workspace"),
    "allowed": ("role", "permission", "rbac"),
    "access": ("role", "permission", "rbac", "workspace", "acces"),
    # Connectors / engines
    "bigquery": ("connector", "warehouse", "bigquery", "destination"),
    "snowflake": ("connector", "warehouse", "snowflake", "destination"),
    "redshift": ("connector", "warehouse", "destination"),
    "databricks": ("connector", "warehouse", "destination"),
    "postgres": ("connector", "postgresql", "database"),
    "postgresql": ("connector", "postgresql", "database"),
    "mysql": ("connector", "mysql", "database"),
    "mongodb": ("connector", "mongodb", "database"),
    "kafka": ("connector", "kafka", "stream"),
    "s3": ("connector", "s3", "object", "storage"),
    "sqlserver": ("connector", "sqlserver", "database"),
    "oracle": ("connector", "oracle", "database"),
    "connection": ("connector", "connect", "credentials"),
    "credentials": ("connector", "connect", "credentials", "secret"),
    "datasource": ("connector", "source"),
    "endpoint": ("connector", "endpoint", "api"),
    # Sync modes
    "upsert": ("sync", "mode", "upsert", "merge", "deduped"),
    "merge": ("sync", "mode", "upsert", "merge"),
    "incremental": ("sync", "mode", "incremental", "cursor"),
    "append": ("sync", "mode", "append"),
    "overwrite": ("sync", "mode", "overwrite", "refresh"),
    "mirror": ("sync", "mode", "mirror", "delete"),
    "scd2": ("sync", "mode", "scd2", "history", "dimension"),
    "cdc": ("sync", "mode", "cdc", "change", "capture", "log"),
    "snapshot": ("sync", "mode", "refresh", "snapshot"),
    # Mapping / types
    "mapping": ("map", "mapp", "semantic", "column", "confidence"),
    "cast": ("map", "type", "conversion", "transform"),
    "coerce": ("map", "type", "conversion", "transform"),
    "precision": ("decimal", "type", "lossy", "scale"),
    "decimal": ("decimal", "type", "precision", "scale", "numeric"),
    "truncate": ("lossy", "type", "narrow", "overwrite"),
    "lossy": ("lossy", "type", "narrow", "risk", "contract"),
    "narrowing": ("lossy", "narrow", "type", "risk"),
    "datatype": ("type", "schema", "column"),
    # An operator writes "timezone" as one word; the documentation's heading
    # words are "time" and "zone", so the single token matched no subject and a
    # question the timezone policy answers in detail was refused outright.
    # ``instant`` is the range-passage word (2038 / bare TIMESTAMP). Expanding
    # timezone onto it made "what timezone are timestamps stored in" open on
    # that range sentence while the UTC sentence sat first in retrieval.
    "timezone": ("timestamp", "zone", "offset", "temporal", "utc"),
    "tz": ("timestamp", "zone", "offset", "utc"),
    "utc": ("timestamp", "zone", "offset", "utc"),
    "timestamp": ("timestamp", "zone", "instant", "datetime", "temporal"),
    "null": ("null", "nullable", "empty", "missing", "coerced"),
    "nullable": ("null", "nullable", "empty", "missing"),
    "encoding": ("encoding", "charset", "unicode", "utf8", "capacity"),
    "unicode": ("encoding", "charset", "unicode", "utf8"),
    "charset": ("encoding", "charset", "unicode", "utf8"),
    "collation": ("collation", "charset", "encoding", "sort"),
    "schema": ("schema", "column", "type", "drift"),
    "drift": ("drift", "schema", "policy", "detection"),
    # Gates / validation
    "gate": ("gate", "preflight", "block", "validate"),
    "gates": ("gate", "preflight", "block", "validate"),
    "blocker": ("gate", "block", "preflight", "refuse"),
    "blocked": ("gate", "block", "preflight", "refuse"),
    "validation": ("validate", "preflight", "gate"),
    "preflight": ("preflight", "gate", "validate"),
    "dryrun": ("preflight", "gate", "dry", "run"),
    "approve": ("approve", "acknowledge", "confirm", "risk", "contract"),
    "approval": ("approve", "acknowledge", "confirm", "contract"),
    "signoff": ("approve", "confirm", "contract", "sign"),
    "pii": ("pii", "compliance", "sensitive", "approve"),
    "sensitive": ("pii", "compliance", "sensitive"),
    "compliance": ("pii", "compliance", "policy"),
    # Transform
    "transform": ("transform", "shape", "recipe", "operation"),
    "transformation": ("transform", "shape", "recipe", "operation"),
    "filter": ("transform", "filter", "row", "condition"),
    "cleanup": ("transform", "trim", "normalise", "recipe"),
    "trim": ("transform", "trim", "whitespace"),
    "rename": ("transform", "map", "column"),
    # Jobs / runs
    "run": ("job", "run", "transfer", "execute"),
    "runs": ("job", "run", "transfer", "execute"),
    "history": ("job", "run", "history", "audit"),
    "log": ("job", "log", "theater", "event"),
    "logs": ("job", "log", "theater", "event"),
    "progress": ("job", "phas", "theater", "progress"),
    "cancel": ("job", "cancel", "stop"),
    "retry": ("job", "retry", "resume", "replay"),
    # Contracts / GitOps
    "contract": ("contract", "schema", "policy", "sign"),
    "yaml": ("gitop", "yaml", "export", "manifest"),
    "gitops": ("gitop", "yaml", "manifest", "export"),
    "manifest": ("gitop", "yaml", "manifest"),
    "terraform": ("gitop", "yaml", "manifest", "declarative"),
    # Interfaces
    "api": ("api", "rest", "endpoint", "reference"),
    "rest": ("api", "rest", "endpoint"),
    "sdk": ("api", "rest", "endpoint", "script"),
    "curl": ("api", "rest", "endpoint", "example"),
    "mcp": ("mcp", "agent", "server", "tool"),
    "webhook": ("webhook", "notification", "channel"),
    "notification": ("webhook", "notification", "channel", "alert"),
    "alert": ("webhook", "notification", "channel", "alert"),
    "sso": ("sso", "authentication", "saml", "enterprise"),
    "saml": ("sso", "authentication", "saml"),
    "byok": ("byok", "keys", "encryption", "enterprise"),
    "encryption": ("byok", "keys", "encryption", "transit"),
    "tenant": ("tenant", "workspace", "enterprise", "organization"),
}

# A CDC redelivery / exactly-once question is knowledge, not a job list.
# "If I run the same CDC change twice" used to plan list_jobs because it
# says "run", and the empty-history sentence then became the lead.
CDC_DELIVERY_RE = re.compile(
    r"\b(?:exactly[\s-]?once|at[\s-]?least[\s-]?once|effectively[\s-]?once|"
    r"idempotent|"
    r"redeliver|(?:same|identical)\s+(?:change|event|record)|"
    r"run(?:ning)?\s+(?:it|the\s+same).{0,32}twice|"
    r"_df_lsn|df[\s_]?lsn|"
    r"skip\s+dup(?:e|licate)s?|"
    r"replay.{0,32}same\s+(?:change|event|record)|"
    r"same\s+(?:change|event|record).{0,24}replay)\b",
    re.I,
)


def is_cdc_delivery_question(text: str) -> bool:
    """Whether the turn asks about CDC redelivery / exactly-once, not a job."""
    return bool(text and CDC_DELIVERY_RE.search(text))


# Pause CDC is cadence + slot keep. ``pause(?:ing)?`` matches "pause" /
# "pauseing" and misses the operator spelling "pausing", which is how
# "does pausing CDC drop the replication slot" never fired the heading
# prior and retrieved the slot definition instead.
PAUSE_CDC_RE = re.compile(
    r"\bpaus(?:e|ing)\s+(?:a\s+|the\s+)?cdc\b"
    r"|\bpause\s+a\s+cdc\s+pipeline\b"
    r"|\bresume\s+cdc\b"
    r"|\bkeep\s+the\s+resume\s+token\s+when\s+i\s+pause\b"
    r"|\bwithout\s+losing\s+the\s+lsn\b"
    r"|\bpaus(?:e|ing)\s+cdc\s+drop\b"
    r"|\bdoes\s+paus(?:e|ing)\s+cdc\b"
    r"|\bif\s+i\s+pause\s+cdc\b",
    re.I,
)

# ``replication slot`` otherwise expands to wal / postgres and those
# anchors pull the slot-definition card over the pause-keep card.
_REPLICATION_SLOT_EXPAND = ("slot", "wal", "cdc", "postgres")

# Custom slot name is derived. The same ``replication slot`` expansion
# otherwise opens on the WAL definition or max_replication_slots.
CUSTOM_SLOT_NAME_RE = re.compile(
    r"\bcustom\s+(?:replication\s+)?slot\s+name\b"
    r"|\bset\s+the\s+replication\s+slot\s+name\b"
    r"|\bslot\s+name\s+for\s+postgres\b",
    re.I,
)

# Incremental-by-updated_at is a shipped cursor mode. ``use incremental``
# otherwise plans a sync-mode recommendation instead of the card.
INCREMENTAL_UPDATED_AT_RE = re.compile(
    r"\bincremental\s+by\s+updated_at\b"
    r"|\bincremental\s+on\s+updated_at\b"
    r"|\buse\s+incremental\s+by\s+updated_at\b"
    r"|\bupdated_at\s+(?:as\s+a\s+)?(?:cursor|watermark)\b",
    re.I,
)


# Multi-word operator phrases that only mean something together. Matched on the
# normalized question before single-term expansion.
_PHRASE_EXPANSIONS: tuple[tuple[re.Pattern[str], tuple[str, ...]], ...] = (
    # Competitor-adjacent asks must not inherit CDC / Studio vocabulary.
    # Without these, "SSH tunnel to postgres" opened Query Playground and
    # wal_level, and "dbt Cloud" opened a connector-credentials step.
    (re.compile(r"\bdbt(?:\s+cloud)?\b", re.I),
     ("dbt", "complement", "export")),
    (re.compile(
        r"\bssh\s+tunnels?\b"
        r"|\bbastion(?:\s+host)?\b"
        r"|\bjump\s+hosts?\b"
        r"|\bconnect through (?:an?\s+)?(?:ssh|bastion|tunnel)\b",
        re.I,
    ),
     ("ssh", "tunnel", "bastion")),
    (re.compile(r"\bbad\s+(?:row|record|data)s?\b", re.I),
     ("quarantine", "reject", "invalid", "dlq")),
    (re.compile(r"\b(?:row|record)s?\s+(?:ledger|accounting|balance)\b", re.I),
     ("reconciliation", "checksum", "conservation", "ledger")),
    (re.compile(r"\bdata\s+loss|\blos(?:e|ing|t)\s+(?:row|record|data)s?\b", re.I),
     ("quarantine", "reconciliation", "checksum", "unaccounted")),
    (re.compile(
        r"\bguarantee\s+(?:no\s+)?(?:silent\s+)?data\s+loss\b"
        r"|\bzero\s+data\s+loss\b"
        r"|\bnever\s+lose\s+data\b"
        r"|\bno\s+silent\s+(?:data\s+)?loss\b",
        re.I,
    ),
     ("quarantine", "ledger", "silent", "checksum")),
    (re.compile(
        r"\bupsert\s+(?:vs\.?|versus|or)\s+merge\b"
        r"|\bmerge\s+(?:vs\.?|versus|or)\s+upsert\b"
        r"|\bdifference\s+between\s+upsert\s+and\s+merge\b"
        r"|\bis\s+upsert\s+(?:the\s+same\s+as|like)\s+merge\b",
        re.I,
    ),
     ("upsert", "merge", "dialect", "conflict")),
    (re.compile(r"\bchange\s+data\s+capture\b", re.I),
     ("cdc", "sync", "mode", "log", "change")),
    (re.compile(r"\bdead\s+letter\b", re.I), ("quarantine", "dlq", "reject")),
    (re.compile(
        r"\b(?:no|without(?:\s+a)?|missing|lack(?:s|ing)?(?:\s+a)?)\s+primary\s+key\b"
        r"|\bsource\s+has\s+no\s+primary\s+key\b",
        re.I,
    ),
     ("primary_key",)),
    (re.compile(r"\bprimary\s+key\b", re.I), ("key", "upsert", "identity", "deduped")),
    (re.compile(
        r"\bschedule\s+a\s+(?:transfer|pipeline|load|sync|job)\b"
        r"|\bschedule\s+this\s+(?:transfer|pipeline|load|sync)\b",
        re.I,
    ),
     ("cadence", "recurr", "cron")),
    (re.compile(r"\bgate\s*1\b|\bg1\b", re.I), ("g1",)),
    (re.compile(r"\bgate\s*2\b|\bg2\b", re.I), ("g2",)),
    (re.compile(r"\bgate\s*3\b|\bg3\b", re.I), ("g3",)),
    (re.compile(r"\bgate\s*4\b|\bg4\b", re.I), ("g4",)),
    (re.compile(r"\bgate\s*5\b|\bg5\b", re.I), ("g5",)),
    (re.compile(r"\bgate\s*6\b|\bg6\b", re.I), ("g6",)),
    (re.compile(r"\bgate\s*7\b|\bg7\b", re.I), ("g7",)),
    (re.compile(r"\bgate\s*8\b|\bg8\b", re.I), ("g8", "reconcil")),
    (re.compile(r"\bgate\s*9\b|\bg9\b", re.I), ("g9",)),
    (re.compile(
        r"\bsalesforce\s+oauth\b"
        r"|\bconnected\s+app\b"
        r"|\brefresh\s+salesforce\s+tokens?\b",
        re.I,
    ),
     ("salesforce_oauth",)),
    (re.compile(r"\bslack\s+(?:alerts?|notifications?)\b", re.I),
     ("slack",)),
    (re.compile(
        r"\b(?:microsoft\s+)?teams?\s+alerts?\b"
        r"|\bteams\s+notifications?\b",
        re.I,
    ),
     ("teams_notify",)),
    (re.compile(
        r"\bemail\s+alerts?\b"
        r"|\bsend\s+email\s+(?:alerts?|notifications?)\b",
        re.I,
    ),
     ("email_notify",)),
    (re.compile(
        r"\bservicenow\s+tickets?\b"
        r"|\bservicenow\s+(?:alerts?|notifications?)\b",
        re.I,
    ),
     ("servicenow_notify",)),
    (re.compile(
        r"\bis\s+slack\s+a\s+connector\b"
        r"|\bslack\s+(?:as\s+a\s+)?(?:source|destination|connector)\b",
        re.I,
    ),
     ("slack_connector",)),
    (re.compile(
        r"\bteams?\s+as\s+a\s+destination\b"
        r"|\bmicrosoft\s+teams\s+as\s+a\s+destination\b",
        re.I,
    ),
     ("teams_dest",)),
    (re.compile(r"\bhubspot\b", re.I), ("hubspot",)),
    (re.compile(r"\bstripe\b", re.I), ("stripe",)),
    (re.compile(
        r"\brow[\s-]level\s+security\b|\brls\b",
        re.I,
    ),
     ("rls",)),
    (re.compile(
        r"\bsnowflake\s+dynamic\s+tables?\b"
        r"|\bdynamic\s+tables?\b",
        re.I,
    ),
     ("snowflake_dynamic",)),
    (re.compile(
        r"\bentra\s+id\b|\bazure\s+ad\b|\benva\s+id\b",
        re.I,
    ),
     ("sso", "saml", "oidc")),
    (re.compile(r"\bcustom\s+roles?\b", re.I), ("custom_roles",)),
    (re.compile(r"\bfield[\s-]level\s+encryption\b|\bcolumn\s+encryption\b", re.I),
     ("field_encryption",)),
    (re.compile(r"\bapache\s+hudi\b|\bhudi\b", re.I), ("hudi",)),
    (re.compile(r"\bkafka\s+consumer\s+groups?\b|\bconsumer\s+groups?\b", re.I),
     ("kafka_group",)),
    (re.compile(
        r"\baws\s+secrets?\s+manager\b"
        r"|\bhashicorp\s+vault\b"
        r"|\bsecrets?\s+manager\b",
        re.I,
    ),
     ("external_vault",)),
    (re.compile(r"\bokta\b(?!\s+scim)", re.I), ("sso", "saml", "oidc")),
    (re.compile(
        r"\bazure\s+key\s+vault\b",
        re.I,
    ),
     ("external_vault",)),
    # The other sense of "key". With only the identity sense above, "can I use
    # my own encryption key" landed on whichever sync-mode sentence says the
    # word most often — upsert's "key-idempotently: new keys insert, known keys
    # update" — while the BYOK section it was asking about ranked below.
    (re.compile(r"\b(?:encryption|kms|customer[\s-]managed|private|secret)\s+keys?\b"
                r"|\bkey\s+management\b"
                # What the acronym stands for, which is how it gets asked.
                r"|\bbring\s+(?:my|your|our)\s+own\b", re.I),
     ("byok", "kms", "encryption", "secret")),
    (re.compile(
        r"\bslowly\s+changing\s+dimension\b(?!\s*(?:type\s*)?1\b)",
        re.I,
    ),
     ("scd2", "sync", "mode", "history")),
    (re.compile(r"\bscd\s*(?:type\s*)?2\b", re.I),
     ("scd2", "sync", "mode", "history", "version")),
    (re.compile(r"\breverse\s+etl\b", re.I), ("reverse", "etl", "sync", "mode")),
    (re.compile(r"\breal[\s-]?time\b", re.I), ("cdc", "stream", "sync", "mode")),
    (re.compile(r"\bone[\s-]?(?:off|time)\b", re.I), ("transfer", "studio", "manual")),
    (re.compile(r"\bevery\s+(?:night|day|hour|week)\b", re.I),
     ("schedule", "pipeline", "recurr", "cron")),
    (re.compile(r"\b(?:\d{1,2}\s*(?:am|pm)|\d{1,2}:\d{2})\b", re.I),
     ("schedule", "cron", "timezone", "pipeline")),
    (re.compile(r"\bread[\s-]?only\b", re.I), ("viewer", "role", "permission", "read")),
    (re.compile(r"\bwho\s+can\b", re.I), ("role", "permission", "rbac", "approve")),
    (re.compile(r"\bwho\s+is\s+allowed\b", re.I),
     ("role", "permission", "rbac", "editor", "admin")),
    (re.compile(r"\bin\s+git\b|\bkeep\s+.{0,24}\b(?:git|github)\b", re.I),
     ("gitop", "yaml", "export", "import", "manifest")),
    (re.compile(r"\brows?\s+that\s+failed\b"
                r"|\b(?:failed|rejected)\s+rows?\b", re.I),
     ("quarantine", "csv", "export", "reject")),
    (re.compile(r"\bfilter\s+rows\b|\bbefore\s+(?:they\s+are\s+)?written\b", re.I),
     ("transform", "filter", "map")),
    (re.compile(r"\bdestination\s+count\b"
                r"|\bcounts?\s+(?:do\s+not|don't|doesn'?t)\s+match\b", re.I),
     ("checksum", "unbalanced", "mismatch", "reconciliation")),
    # The same question without the modal. "Can I limit who sees a connector"
    # reached "Honest transfer-ready labels" — the one connector section that
    # says nothing about who may read one — because ``who can`` was the only
    # spelling of a permissions question the vocabulary knew.
    (re.compile(r"\bwho\s+(?:sees|reads|views|has\s+access)\b"
                r"|\b(?:limit|restrict|control)\s+(?:who|access)\b", re.I),
     ("role", "permission", "rbac", "viewer")),
    # Asking what happens to a half-finished run is asking about resuming from
    # a checkpoint, and it shares no word with the section that says so: the
    # answer came from the webhook event list, on the strength of ``job.failed``.
    (re.compile(r"\bfail(?:s|ed|ing)?\s+"
                r"(?:halfway|partway|midway|part\s*way|in\s+the\s+middle)\b"
                r"|\bhalf(?:\s|-)?(?:way\s+through|finished|written)\b", re.I),
     ("resume", "checkpoint", "partial", "retry")),
    (re.compile(r"\bget\s+started\b", re.I), ("first", "transfer", "studio", "guide")),
    # Operator vocabulary for subjects the documentation spells differently.
    # Each of these was refused outright: answerability is decided on whether
    # the question names something the docs have a heading about, and "high
    # water mark" shares no term with "Resume points, checkpoints and
    # watermarks" once it is tokenized into three words.
    (re.compile(r"\bhigh[\s-]?water[\s-]?marks?\b|\bhighwater\b", re.I),
     ("watermark", "resume", "checkpoint", "cursor")),
    (re.compile(r"\b(?:initial|first|inital)\s+(?:load|snapshot|sync|dump)\b"
                r"|\bbackfill\s+(?:then|before)\s+stream\b", re.I),
     ("snapshot", "handoff", "capture", "stream")),
    # "Blast radius" is how a platform engineer asks what a failure takes down
    # with it; the documentation answers in terms of the gate that blocked and
    # the tick that failed.
    (re.compile(r"\bblast\s+radius\b|\bhow\s+bad\s+is\s+it\s+if\b"
                r"|\bwhat\s+(?:else\s+)?breaks\b", re.I),
     ("fail", "gate", "block", "tick")),
    (re.compile(r"\b(?:write|writing|know|need)\s+sql\b|\bno[\s-]?code\b"
                r"|\bhand[\s-]?written\s+sql\b", re.I),
     ("playground", "query", "studio", "mapping")),
    (re.compile(r"\bthrottl\w*\b|\bback[\s-]?pressure\b|\brate[\s-]?limit\w*\b"
                r"|\btoo\s+much\s+load\b", re.I),
     ("throttle", "chunk", "concurrency", "throughput")),
    # The operator's nouns for the one analytical operation the Pilot can
    # actually execute, none of which reach the passage that documents it.
    # ``aggregation`` does not stem to ``aggregat`` the way ``aggregates``
    # does, and ``breakdown`` written as one word shares no token with "break
    # down", so "explain aggregation" and "what does a breakdown show me" were
    # both refused as outside the documentation while "break down orders by
    # region" ran a real GROUP BY one module over.
    (re.compile(r"\baggregat\w*\b|\bbreak\s*downs?\b"
                r"|\bgroup(?:ed|ing)?\s+by\b|\bper\s+(?:group|bucket)\b", re.I),
     ("group", "aggregate", "measure")),
    (re.compile(r"\bstanding\s+authorit", re.I),
     ("authorize", "unattended", "schedule", "pipeline")),
    (re.compile(r"\b(?:nobody|no[\s-]?one)\s+is\s+watching\b"
                r"|\bwithout\s+watching\b|\bunattended\b", re.I),
     ("unattended", "authorize", "signed", "schedule")),
    (re.compile(r"\btype[\s_-]?locked\b"
                r"|\bstop\s+a\s+type\s+change\b"
                r"|\btype\s+change\s+from\s+being\s+applied\b", re.I),
     ("type_locked", "schema", "policy", "type")),
    (re.compile(r"\b(?:end|ended)\s+up\b"
                r"|\bwhere\s+do\s+(?:the\s+)?(?:bad|rejected|failed)\s+rows\b", re.I),
     ("quarantine", "reject")),
    (re.compile(r"\bturn\s+off\b"
                r"|\bdisable\s+(?:a\s+|the\s+)?(?:pipeline|schedule|sync|nightly)\b"
                r"|\bpause\s+a\s+nightly\b",
                re.I),
     ("pause", "pipeline", "schedule")),
    (PAUSE_CDC_RE, ("pause_cdc",)),
    (re.compile(
        r"\bhow\s+do\s+i\s+connect\s+salesforce\b"
        r"|\badd\s+a\s+salesforce\s+connection\b"
        r"|\bset\s+up\s+salesforce\b",
        re.I,
    ),
     ("salesforce_connect",)),
    (re.compile(
        r"\bread\s+replica\b"
        r"|\bphysical\s+standby\b"
        r"|\bhot\s+standby\b"
        r"|\bcdc\s+from\s+a\s+replica\b",
        re.I,
    ),
     ("cdc_read_replica",)),
    (re.compile(r"\boracle\s+logminer\b|\blogminer\b", re.I),
     ("oracle_logminer",)),
    (re.compile(
        r"\bsql\s+server\s+cdc\b"
        r"|\bsqlserver\s+cdc\b",
        re.I,
    ),
     ("sqlserver_cdc",)),
    (re.compile(
        r"\bchange\s+tracking\b"
        r"|\bchange\s+tracking\s+instead\s+of\s+cdc\b",
        re.I,
    ),
     ("sqlserver_ct",)),
    (re.compile(
        r"\bfilter\s+cdc\s+events?\b"
        r"|\bcdc\s+event\s+filter\b"
        r"|\bfilter\s+cdc\b",
        re.I,
    ),
     ("cdc_event_filter",)),
    (re.compile(
        r"\bskip\s+deletes?\s+(?:in\s+)?cdc\b"
        r"|\bignore\s+deletes?\s+on\s+the\s+cdc\b"
        r"|\bcdc\s+skip\s+deletes?\b",
        re.I,
    ),
     ("cdc_skip_deletes",)),
    (re.compile(
        r"\bcdc\s+heartbeat\s+interval\b"
        r"|\bset\s+a\s+cdc\s+heartbeat\b"
        r"|\bheartbeat\s+interval\b",
        re.I,
    ),
     ("cdc_heartbeat_interval",)),
    (re.compile(
        r"\bcdc\s+fetch\s+size\b"
        r"|\bcdc\s+batch\s+size\b"
        r"|\bset\s+the\s+cdc\s+batch\b",
        re.I,
    ),
     ("cdc_fetch_size",)),
    (re.compile(
        r"\boracle\s+xstream\b"
        r"|\bxstream\b",
        re.I,
    ),
     ("oracle_xstream",)),
    (re.compile(
        r"\balways\s+on\b"
        r"|\bavailability\s+groups?\b",
        re.I,
    ),
     ("sqlserver_ag",)),
    (re.compile(
        r"\bmicrosoft\s+fabric\b"
        r"|\bonelake\b"
        r"|\bfabric\s+as\s+a\s+destination\b"
        r"|\bwrite\s+to\s+microsoft\s+fabric\b",
        re.I,
    ),
     ("fabric",)),
    (re.compile(
        r"\bpower\s*bi\b",
        re.I,
    ),
     ("power_bi",)),
    (re.compile(
        r"\bazure\s+data\s+factory\b"
        r"|\badf\b",
        re.I,
    ),
     ("adf",)),
    (re.compile(
        r"\bmanaged\s+identity\b",
        re.I,
    ),
     ("azure_managed_identity",)),
    (re.compile(
        r"\bcosmos\s*db\b",
        re.I,
    ),
     ("cosmos",)),
    (re.compile(
        r"\bevent\s+hubs?\b",
        re.I,
    ),
     ("event_hubs",)),
    (re.compile(
        r"\bservice\s+bus\b",
        re.I,
    ),
     ("service_bus",)),
    (re.compile(
        r"\bland\s+(?:tables?\s+)?(?:in|to|into)\s+adls\b"
        r"|\badls\s+as\s+a\s+destination\b"
        r"|\bwrite\s+to\s+adls\b"
        r"|\bdo\s+you\s+support\s+adls\b",
        re.I,
    ),
     ("adls", "driver")),
    (re.compile(
        r"\bprivate\s+link\b",
        re.I,
    ),
     ("privatelink",)),
    (re.compile(
        r"\bgcs\s+as\s+a\s+destination\b"
        r"|\bwrite\s+to\s+gcs\b"
        r"|\bdo\s+you\s+support\s+gcs\b"
        r"|\bgoogle\s+cloud\s+storage\b",
        re.I,
    ),
     ("gcs", "driver")),
    (re.compile(
        r"\bcloud\s+sql\b",
        re.I,
    ),
     ("cloud_sql",)),
    (re.compile(
        r"\bazure\s+sql\b",
        re.I,
    ),
     ("azure_sql",)),
    (re.compile(
        r"\bpub[\s/-]?sub\b",
        re.I,
    ),
     ("pubsub",)),
    (re.compile(
        r"\bcloud\s+spanner\b"
        r"|\bspanner\b",
        re.I,
    ),
     ("spanner",)),
    (re.compile(
        r"\bvertex\s+ai\b",
        re.I,
    ),
     ("vertex_ai",)),
    (re.compile(
        r"\bsharepoint\b",
        re.I,
    ),
     ("sharepoint",)),
    (re.compile(
        r"\bdynamics\s*365\b"
        r"|\bdataverse\b",
        re.I,
    ),
     ("dynamics365",)),
    (re.compile(
        r"\bpurview\b",
        re.I,
    ),
     ("purview",)),
    (re.compile(
        r"\bexcel\s+online\b"
        r"|\bexcel\s+365\b",
        re.I,
    ),
     ("excel_online",)),
    (re.compile(
        r"\bassume\s+an?\s+aws\s+iam\s+role\b"
        r"|\biam\s+role\b"
        r"|\bsts:assumerole\b",
        re.I,
    ),
     ("aws_iam_role",)),
    (CUSTOM_SLOT_NAME_RE, ("custom_slot_name",)),
    (INCREMENTAL_UPDATED_AT_RE, ("incremental_updated_at", "cursor")),
    (re.compile(
        r"\bblue[\s-]green\b"
        r"|\bzero[\s-]downtime\s+cutover\b"
        r"|\bcut\s+over\s+with\s+zero\s+downtime\b",
        re.I,
    ),
     ("blue_green_cutover",)),
    (re.compile(
        r"\bazure\s+database\s+for\s+postgresql\b"
        r"|\bazure\s+db\s+for\s+postgres",
        re.I,
    ),
     ("azure_database_postgresql", "postgresql")),
    (re.compile(
        r"\bazure\s+database\s+for\s+mysql\b"
        r"|\bazure\s+db\s+for\s+mysql\b",
        re.I,
    ),
     ("azure_database_mysql", "mysql")),
    (re.compile(
        r"\bcloud\s+sql\s+for\s+sql\s+server\b",
        re.I,
    ),
     ("cloud_sql_sqlserver",)),
    (re.compile(
        r"\bonedrive\b",
        re.I,
    ),
     ("onedrive",)),
    (re.compile(
        r"\blooker\s+studio\b",
        re.I,
    ),
     ("looker_studio",)),
    (re.compile(
        r"\blooker\b(?!\s+studio)",
        re.I,
    ),
     ("looker",)),
    (re.compile(
        r"\bbigquery\s+omni\b",
        re.I,
    ),
     ("bigquery_omni",)),
    (re.compile(
        r"\bgoogle\s+sheets\b"
        r"|\bsheets\s+as\s+a\s+destination\b",
        re.I,
    ),
     ("google_sheets",)),
    (re.compile(
        r"\balloydb\b",
        re.I,
    ),
     ("alloydb",)),
    (re.compile(
        r"\bmicrosoft\s+graph\b"
        r"|\bms\s+graph\b",
        re.I,
    ),
     ("microsoft_graph",)),
    (re.compile(
        r"\bazure\s+openai\b",
        re.I,
    ),
     ("azure_openai",)),
    (re.compile(
        r"\bazure\s+data\s+explorer\b"
        r"|\bkusto\b",
        re.I,
    ),
     ("kusto",)),
    (re.compile(
        r"\bevent\s+grid\b",
        re.I,
    ),
     ("event_grid",)),
    (re.compile(
        r"\bgoogle\s+(?:cloud\s+)?dataflow\b"
        r"|\bcloud\s+dataflow\b",
        re.I,
    ),
     ("dataflow_google",)),
    (re.compile(
        r"\bpower\s+platform\b",
        re.I,
    ),
     ("power_platform",)),
    (re.compile(
        r"\bconditional\s+access\b",
        re.I,
    ),
     ("conditional_access",)),
    (re.compile(
        r"\bcloud\s+composer\b",
        re.I,
    ),
     ("cloud_composer", "airflow")),
    (re.compile(
        r"\bazure\s+blob\b"
        r"|\bblob\s+storage\b",
        re.I,
    ),
     ("azure_blob", "adls")),
    (re.compile(
        r"\bazure\s+cache\s+for\s+redis\b"
        r"|\bazure\s+redis\b",
        re.I,
    ),
     ("azure_redis", "redis")),
    (re.compile(
        r"\bflexible\s+server\b"
        r"|\bazure\s+postgresql\s+flexible\b",
        re.I,
    ),
     ("azure_flexible_server", "postgresql")),
    (re.compile(r"\bfirebase\b", re.I), ("firebase", "firebase_ready", "fireba")),
    (re.compile(r"\bfirestore\b", re.I), ("firestore",)),
    (re.compile(r"\bbigtable\b", re.I), ("bigtable",)),
    (re.compile(r"\bdataproc\b", re.I), ("dataproc",)),
    (re.compile(
        r"\bentra\s+pim\b"
        r"|\bprivileged\s+identity\s+management\b",
        re.I,
    ),
     ("entra_pim",)),
    (re.compile(r"\bgke\b|\bgoogle\s+kubernetes\b", re.I), ("gke",)),
    (re.compile(r"\boutlook\b", re.I), ("outlook",)),
    (re.compile(r"\byoutube\b", re.I), ("youtube",)),
    (re.compile(r"\bgoogle\s+ads\b", re.I), ("google_ads",)),
    (re.compile(r"\bgoogle\s+drive\b", re.I), ("google_drive",)),
    (re.compile(r"\bgoogle\s+docs\b", re.I), ("google_docs",)),
    (re.compile(r"\bgoogle\s+analytics\b|\bga4\b", re.I), ("google_analytics",)),
    (re.compile(r"\bcloud\s+run\b", re.I), ("cloud_run",)),
    (re.compile(r"\bstream\s+analytics\b", re.I), ("stream_analytics",)),
    (re.compile(r"\bmicrosoft\s+lists\b", re.I), ("microsoft_lists",)),
    (re.compile(r"\bexchange\s+online\b", re.I), ("exchange_online",)),
    (re.compile(
        r"\bgithub\s+enterprise\s+as\s+a\s+destination\b"
        r"|\bgithub\s+enterprise\s+as\s+a\s+dest",
        re.I,
    ),
     ("github_enterprise",)),
    (re.compile(
        r"\bbigquery\s+data\s+transfer\b"
        r"|\bbq\s+dts\b",
        re.I,
    ),
     ("bq_dts",)),
    (re.compile(r"\blinked\s+dataset\b", re.I), ("bq_linked_dataset",)),
    (re.compile(r"\bcloud\s+kms\b", re.I), ("cloud_kms",)),
    (re.compile(r"\bapp\s+engine\b", re.I), ("app_engine",)),
    (re.compile(r"\bdata\s+catalog\b", re.I), ("data_catalog",)),
    (re.compile(r"\bcloud\s+build\b", re.I), ("cloud_build",)),
    (re.compile(r"\bartifact\s+registry\b", re.I), ("artifact_registry",)),
    (re.compile(r"\bcloud\s+functions?\b", re.I), ("cloud_functions",)),
    (re.compile(r"\bazure\s+files\b", re.I), ("azure_files",)),
    (re.compile(r"\blog\s+analytics\b", re.I), ("log_analytics",)),
    (re.compile(
        r"\bazure\s+ai\s+search\b"
        r"|\bcognitive\s+search\b",
        re.I,
    ),
     ("azure_ai_search",)),
    (re.compile(
        r"\bazure\s+analysis\s+services\b"
        r"|\bssas\b",
        re.I,
    ),
     ("analysis_services",)),
    (re.compile(
        r"\bcloud\s+sql\s+auth\s+proxy\b"
        r"|\bcloud\s+sql\s+proxy\b",
        re.I,
    ),
     ("cloud_sql_auth_proxy",)),
    (re.compile(r"\bsynapse\s+link\b", re.I), ("synapse_link",)),
    (re.compile(
        r"\bazure\s+table\b"
        r"|\btable\s+storage\b",
        re.I,
    ),
     ("azure_table", "table_storage")),
    (re.compile(
        r"\bazure\s+queue\b"
        r"|\bqueue\s+storage\b",
        re.I,
    ),
     ("azure_queue", "queue_storage")),
    (re.compile(
        r"\bsql\s+server\s+on\s+(?:an?\s+)?azure\s+vms?\b"
        r"|\bsql\s+server\s+on\s+azure\s+(?:virtual\s+machines?|vms?)\b",
        re.I,
    ),
     ("sqlserver_azure_vm", "sqlserver")),
    (re.compile(r"\bsplunk\b", re.I), ("splunk",)),
    (re.compile(r"\btableau\b", re.I), ("tableau",)),
    (re.compile(
        r"\bdata\s+lake\s+gen\s*2\b"
        r"|\badls\s+gen\s*2\b"
        r"|\bazure\s+data\s+lake(?:\s+storage)?(?:\s+gen\s*2)?\b",
        re.I,
    ),
     ("data_lake_gen2", "adls")),
    (re.compile(
        r"\bservice\s+principal\s+for\s+azure\s+sql\b"
        r"|\bazure\s+sql\b.{0,40}\bservice\s+principal\b"
        r"|\bservice\s+principal\b.{0,40}\bazure\s+sql\b",
        re.I,
    ),
     ("azure_sql_sp", "sqlserver")),
    (re.compile(r"\bdynamodb\b|\bdynamo\s+db\b", re.I), ("dynamodb",)),
    (re.compile(r"\belasticsearch\b", re.I), ("elasticsearch",)),
    (re.compile(r"\belastic\s+cloud\b", re.I), ("elastic_cloud", "elasticsearch")),
    (re.compile(r"\bopensearch\b", re.I), ("opensearch",)),
    (re.compile(
        r"\b(?:microsoft\s+)?teams\s+as\s+a\s+source\b",
        re.I,
    ),
     ("teams_source",)),
    (re.compile(
        r"\b(?:microsoft|office)\s+365\s+as\s+a\s+destination\b"
        r"|\bm365\s+as\s+a\s+destination\b",
        re.I,
    ),
     ("microsoft_365",)),
    (re.compile(r"\bmemorystore\b", re.I), ("memorystore", "redis")),
    (re.compile(r"\bazure\s+sql\s+edge\b", re.I), ("azure_sql_edge", "sqlserver")),
    (re.compile(r"\bintune\b", re.I), ("intune",)),
    (re.compile(r"\b(?:microsoft\s+)?defender\b", re.I), ("defender",)),
    (re.compile(r"\b(?:microsoft\s+)?sentinel\b", re.I), ("sentinel",)),
    (re.compile(
        r"\bazure\s+machine\s+learning\b"
        r"|\bazure\s+ml\b",
        re.I,
    ),
     ("azure_ml",)),
    (re.compile(r"\bsearch\s+ads\s+360\b|\bsa360\b", re.I), ("search_ads_360",)),
    (re.compile(r"\bcloud\s+tasks?\b", re.I), ("cloud_tasks",)),
    (re.compile(r"\bvpc\s+service\s+controls?\b", re.I), ("vpc_sc",)),
    (re.compile(r"\bazure\s+firewall\b", re.I), ("azure_firewall",)),
    (re.compile(
        r"\bcampaign\s+manager(?:\s+360)?\b"
        r"|\bcm360\b",
        re.I,
    ),
     ("campaign_manager",)),
    (re.compile(r"\baurora\b", re.I), ("aurora",)),
    (re.compile(r"\bdocumentdb\b|\bdocdb\b", re.I), ("documentdb",)),
    (re.compile(r"\bcloud\s+armor\b", re.I), ("cloud_armor",)),
    (re.compile(r"\bcloud\s+interconnect\b", re.I), ("cloud_interconnect",)),
    (re.compile(r"\bcloud\s+vpn\b", re.I), ("cloud_vpn",)),
    (re.compile(r"\bazure\s+arc\b", re.I), ("azure_arc",)),
    (re.compile(r"\bazure\s+lighthouse\b", re.I), ("azure_lighthouse",)),
    (re.compile(r"\bazure\s+monitor\b", re.I), ("azure_monitor",)),
    (re.compile(r"\bazure\s+devops\b", re.I), ("azure_devops",)),
    (re.compile(r"\bazure\s+boards\b", re.I), ("azure_boards",)),
    (re.compile(r"\bazure\s+migrate\b", re.I), ("azure_migrate",)),
    (re.compile(
        r"\bdisplay\s*(?:and|&)\s*video\s*360\b"
        r"|\bdv360\b",
        re.I,
    ),
     ("dv360",)),
    (re.compile(
        r"\b(?:google\s+cloud\s+)?storage\s+transfer\s+service\b"
        r"|\bgcs\s+transfer\s+service\b",
        re.I,
    ),
     ("gcs_transfer_service",)),
    (re.compile(r"\bqlik\b", re.I), ("qlik",)),
    (re.compile(
        r"\bentra\s+id\s+governance\b"
        r"|\bentra\s+governance\b",
        re.I,
    ),
     ("entra_governance",)),
    (re.compile(r"\bcloud\s+sql\s+for\s+mysql\b", re.I), ("cloud_sql_mysql",)),
    (re.compile(r"\bhdinsight\b", re.I), ("hdinsight",)),
    (re.compile(r"\bexpressroute\b|\bexpress\s+route\b", re.I), ("expressroute",)),
    (re.compile(
        r"\bssis\b"
        r"|\bsql\s+server\s+integration\s+services\b",
        re.I,
    ),
     ("ssis",)),
    (re.compile(
        r"\bssrs\b"
        r"|\bsql\s+server\s+reporting\s+services\b",
        re.I,
    ),
     ("ssrs",)),
    (re.compile(r"\bgmail\b", re.I), ("gmail",)),
    (re.compile(
        r"\bgoogle\s+calendar\b"
        r"|\bcalendar\s+as\s+a\s+source\b",
        re.I,
    ),
     ("google_calendar",)),
    (re.compile(r"\bazure\s+cdn\b", re.I), ("azure_cdn",)),
    (re.compile(r"\bazure\s+blueprints?\b", re.I), ("azure_blueprints",)),
    (re.compile(r"\bazure\s+automation\b", re.I), ("azure_automation",)),
    (re.compile(r"\bbigquery\s+ml\b|\bbqml\b", re.I), ("bigquery_ml",)),
    (re.compile(r"\brds\s+for\s+postgresql\b|\brds\s+postgres", re.I), ("rds_postgresql", "postgresql")),
    (re.compile(r"\brds\s+for\s+mysql\b", re.I), ("rds_mysql", "mysql")),
    (re.compile(r"\bcloud\s+sql\s+for\s+postgresql\b", re.I), ("cloud_sql_postgresql",)),
    (re.compile(r"\bfilestore\b", re.I), ("filestore",)),
    (re.compile(r"\bpersistent\s+disk\b", re.I), ("persistent_disk",)),
    (re.compile(r"\bdatastream\b", re.I), ("datastream",)),
    (re.compile(r"\bdataplex\b", re.I), ("dataplex",)),
    (re.compile(r"\binformatica\b", re.I), ("informatica",)),
    (re.compile(r"\btalend\b", re.I), ("talend",)),
    (re.compile(r"\bmatillion\b", re.I), ("matillion",)),
    (re.compile(r"\bgoogle\s+workspace\b|\bgsuite\b", re.I), ("google_workspace",)),
    (re.compile(r"\baks\b|\bazure\s+kubernetes\b", re.I), ("aks",)),
    (re.compile(r"\bazure\s+functions?\b", re.I), ("azure_functions",)),
    (re.compile(r"\bazure\s+batch\b", re.I), ("azure_batch",)),
    (re.compile(r"\bcloud\s+scheduler\b", re.I), ("cloud_scheduler",)),
    (re.compile(r"\bvertex\s+ai\s+search\b", re.I), ("vertex_ai_search",)),
    (re.compile(r"\bbusiness\s+central\b", re.I), ("business_central",)),
    (re.compile(r"\bapplication\s+insights\b", re.I), ("application_insights",)),
    (re.compile(r"\bsite\s+recovery\b", re.I), ("site_recovery",)),
    (re.compile(r"\bentra\s+external\s+id\b", re.I), ("entra_external_id",)),
    (re.compile(r"\bazure\s+ad\s+b2c\b", re.I), ("azure_ad_b2c",)),
    (re.compile(
        r"\bsnapshot\s+handoff\b"
        r"|\bhand\s+off\s+from\s+snapshot\b"
        r"|\bhow\s+do\s+i\s+do\s+the\s+snapshot\s+handoff\b",
        re.I,
    ),
     ("handoff", "snapshot", "lsn")),
    (re.compile(
        r"\bslot\s+fills?\b"
        r"|\bwal\s+fills?\b"
        r"|\bmax_replication_slots\b"
        r"|\breplication\s+slot\s+fills?\b",
        re.I,
    ),
     ("slot_quota",)),
    (re.compile(
        r"\bsoc\s*2\b|\bhipaa\b|\bbaa\b|\bgdpr\s+dpa\b",
        re.I,
    ),
     ("attestation", "letter", "baa", "soc2")),
    (re.compile(
        r"\b(?:custom\s+airbyte|airbyte\s+connector(?:\s+pack)?|airbyte\s+cdk|"
        r"load(?:ing)?\s+(?:an?\s+)?airbyte|airbyte\s+pack|"
        r"fivetran\s+connector(?:\s+pack)?|fivetran\s+pack|"
        r"load(?:ing)?\s+(?:an?\s+)?fivetran)\b",
        re.I,
    ),
     ("airbyte", "fivetran", "pack", "driver", "cdk")),
    (re.compile(
        r"\bundo\s+a\s+transfer\b"
        r"|\broll\s*back\s+a\s+(?:load|transfer|run)\b"
        r"|\btransfer\s+undo\b"
        r"|\bunwind\s+a\s+(?:load|transfer)\b",
        re.I,
    ),
     ("undo", "rollback", "restore")),
    (re.compile(
        r"\brest\s+api\b"
        r"|\b/api/v1\b"
        r"|\bhttp\s+api\b",
        re.I,
    ),
     ("rest", "api", "bearer")),
    (re.compile(
        r"\bgithub\s+actions\b"
        r"|\bgithub\s+ci\b"
        r"|\bcall\s+(?:this|datawrap)\s+from\s+github\b",
        re.I,
    ),
     ("github", "actions", "api")),
    (re.compile(
        r"\bviewer\s+(?:see|read|export|view)\s+secrets?\b"
        r"|\bcan\s+a\s+viewer\s+see\s+secrets?\b"
        r"|\bsecrets?\s+to\s+a\s+viewer\b",
        re.I,
    ),
     ("viewer", "secret")),
    (re.compile(r"\bopen\s*lineage\b|\bopenlineage\b", re.I),
     ("openlineage", "lineage", "dataset")),
    (re.compile(
        r"\bmirror\s+(?:vs\.?|versus|or)\s+upsert\b"
        r"|\bupsert\s+(?:vs\.?|versus|or)\s+mirror\b"
        r"|\bdifference\s+between\s+mirror\s+and\s+upsert\b",
        re.I,
    ),
     ("mirror", "upsert", "delete")),
    (re.compile(
        r"\bdifference\s+between\s+jobs\s+and\s+pipelines\b"
        r"|\bjobs\s+(?:vs\.?|versus|or)\s+pipelines\b"
        r"|\bpipelines\s+(?:vs\.?|versus|or)\s+jobs\b",
        re.I,
    ),
     ("pipeline", "job", "schedule", "tick")),
    (re.compile(
        r"\bsnowflake\s+(?:with\s+a\s+)?private\s+key\b"
        r"|\bkey[\s-]?pair\s+(?:auth|authentication)?\b"
        r"|\bsnowflake\s+key[\s-]?pair\b",
        re.I,
    ),
     ("snowflake", "key_pair", "private_key")),
    (re.compile(
        r"\bset\s+a\s+watermark\b"
        r"|\bcdc\s+watermark\b"
        r"|\bwhere\s+is\s+the\s+(?:cdc\s+)?watermark\b"
        r"|\bhow\s+is\s+the\s+(?:high[\s-]?water[\s-]?mark|watermark)\s+stored\b",
        re.I,
    ),
     ("watermark", "resume")),
    (re.compile(
        r"\bwho\s+can\s+export\s+audit\b"
        r"|\bexport\s+audit\s+logs?(?:\s+as\s+csv)?\b"
        r"|\baudit\s+logs?\s+as\s+csv\b"
        r"|/api/v1/audit/",
        re.I,
    ),
     ("audit", "csv", "audit_read", "export")),
    (re.compile(
        r"\bip\s+allow\s*lists?\b"
        r"|\bcidr\s+allow\s*lists?\b"
        r"|\ballowlist(?:ed)?\s+ips?\b",
        re.I,
    ),
     ("allowlist", "cidr")),
    (re.compile(
        r"\brequire\s+mfa\b"
        r"|\bmulti[\s-]?factor\b"
        r"|\blogin\s+mfa\b",
        re.I,
    ),
     ("mfa", "login")),
    (re.compile(
        r"\bservice\s+principal(?:\s+for\s+azure)?\b(?!\s+sql)"
        r"|\bazure\s+service\s+principal\b",
        re.I,
    ),
     ("principal", "adls", "service_account")),
    (re.compile(
        r"\bbigquery\s+as\s+a\s+destination\b"
        r"|\bdestination\s+(?:in|to|on)\s+bigquery\b",
        re.I,
    ),
     ("bigquery", "driver")),
    (re.compile(
        r"\bincremental\s+(?:vs\.?|versus|or)\s+upsert\b"
        r"|\bupsert\s+(?:vs\.?|versus|or)\s+incremental\b"
        r"|\bdifference\s+between\s+incremental\s+and\s+upsert\b",
        re.I,
    ),
     ("incremental", "upsert", "cursor")),
    (re.compile(
        r"\bfull[\s-]?refresh\s+(?:vs\.?|versus|or)\s+incremental\b"
        r"|\bincremental\s+(?:vs\.?|versus|or)\s+full[\s-]?refresh\b"
        r"|\bdifference\s+between\s+full[\s-]?refresh\s+and\s+incremental\b",
        re.I,
    ),
     ("full_refresh", "incremental")),
    (re.compile(
        r"\bscd\s*(?:type\s*)?1\b"
        r"|\bslowly\s+changing\s+dimension\s*(?:type\s*)?1\b",
        re.I,
    ),
     ("scd1",)),
    (re.compile(
        r"\btransfers?\s+in\s+parallel\b"
        r"|\brun\s+transfers?\s+in\s+parallel\b"
        r"|\bhow\s+many\s+transfers?\s+can\s+run\b",
        re.I,
    ),
     ("parallel", "inflight")),
    (re.compile(
        r"\btwo\s+jobs?\s+write\b"
        r"|\bsame\s+(?:destination\s+)?table\b"
        r"|\bdestination\s+(?:table\s+)?lock\b"
        r"|\block\s+the\s+destination\b",
        re.I,
    ),
     ("lock", "table")),
    (re.compile(
        r"\bdata\s+residency\b"
        r"|\bpin\s+data\s+to\s+a\s+region\b"
        r"|\bmulti[\s-]?region\b",
        re.I,
    ),
     ("residency", "data_region")),
    (re.compile(
        r"\bsession\s+timeout\b"
        r"|\btoken\s+ttl\b",
        re.I,
    ),
     ("session", "ttl")),
    (re.compile(
        r"\bcustom\s+domain\b"
        r"|\bvanity\s+(?:host|domain)\b",
        re.I,
    ),
     ("custom_domain", "vanity")),
    (re.compile(
        r"\bsql\s+server\b"
        r"|\bmssql\b",
        re.I,
    ),
     ("sqlserver", "driver")),
    (re.compile(
        r"\bdatabricks\b"
        r"|\bunity\s+catalog\b",
        re.I,
    ),
     ("databricks", "unity")),
    (re.compile(r"\bdelta\s+lake\b", re.I),
     ("delta",)),
    (re.compile(
        r"\bredshift\b",
        re.I,
    ),
     ("redshift",)),
    (re.compile(
        r"\bazure\s+synapse\b"
        r"|\bsynapse\s+as\s+a\s+destination\b"
        r"|\bdo\s+you\s+support\s+synapse\b",
        re.I,
    ),
     ("synapse", "azure_synapse", "synap")),
    (re.compile(
        r"\bsnowflake\s+as\s+a\s+destination\b"
        r"|\bdo\s+you\s+support\s+snowflake\b",
        re.I,
    ),
     ("snowflake", "driver")),
    (re.compile(
        r"\boracle\s+as\s+a\s+destination\b"
        r"|\bdo\s+you\s+support\s+oracle(?!\s+goldengate)\b",
        re.I,
    ),
     ("oracle", "driver")),
    (re.compile(
        r"\bpostgres(?:ql)?\s+as\s+a\s+destination\b"
        r"|\bdo\s+you\s+support\s+postgres(?:ql)?\b",
        re.I,
    ),
     ("postgresql", "driver")),
    (re.compile(
        r"\bdo\s+you\s+support\s+mongodb\b"
        r"|\bmongodb\s+as\s+a\s+(?:source|destination)\b",
        re.I,
    ),
     ("mongodb", "driver")),
    (re.compile(
        r"\bs3\s+as\s+a\s+destination\b"
        r"|\bdo\s+you\s+support\s+s3\b",
        re.I,
    ),
     ("s3", "driver")),
    (re.compile(
        r"\bunique\s+key\s+collision\b"
        r"|\bduplicate\s+keys?\b"
        r"|\btwo\s+source\s+rows\b",
        re.I,
    ),
     ("collision", "duplicate")),
    (re.compile(
        r"\bwhere\s+is\s+my\s+data\s+stored\b"
        r"|\bwhere\s+do\s+you\s+store\s+my\s+data\b"
        r"|\bdo\s+you\s+host\s+my\s+data\b",
        re.I,
    ),
     ("hosted",)),
    (re.compile(
        r"\bworkload\s+identity\b",
        re.I,
    ),
     ("workload_identity",)),
    (re.compile(
        r"\bprivate\s+service\s+connect\b",
        re.I,
    ),
     ("privatelink",)),
    (re.compile(
        r"\bcolumn[\s-]level\s+lineage\b",
        re.I,
    ),
     ("column_lineage",)),
    (re.compile(
        r"\bgcp\s+service\s+account\b"
        r"|\bgoogle\s+service\s+account\b",
        re.I,
    ),
     ("gcp", "bigquery")),
    (re.compile(
        r"\bestuary\b"
        r"|(?<!custom\s)\bfivetran\b(?!\s+connector)(?!\s+pack)"
        r"|(?<!custom\s)\bairbyte\b(?!\s+connector)(?!\s+cdk)(?!\s+pack)",
        re.I,
    ),
     ("semantic", "mapping", "quarantine", "checksum")),
    (re.compile(
        r"\bdebezium\b"
        r"|\bkafka\s+connect\b"
        r"|\bflink\s+cdc\b",
        re.I,
    ),
     ("debezium", "kafka", "bridge", "cdc")),
    (re.compile(r"\bterraform\b|\bterraform\s+provider\b", re.I),
     ("terraform", "yaml", "gitops", "provider")),
    (re.compile(
        r"\bprivate\s*link\b|\bprivatelink\b|\bvpc\s+peering\b",
        re.I,
    ),
     ("privatelink", "vpc")),
    (re.compile(r"\bairflow\b|\bspark\s+jobs?\b", re.I),
     ("airflow", "spark", "job")),
    (re.compile(r"\bgolden\s*gate\b", re.I),
     ("goldengate", "oracle")),
    (re.compile(r"\bschema\s+registry\b", re.I),
     ("registry", "kafka", "schema")),
    (re.compile(
        r"\bsnowflake\s+(?:secure\s+)?shar(?:e|ing)\b"
        r"|\bsecure\s+data\s+shar",
        re.I,
    ),
     ("share", "snowflake")),
    (re.compile(
        r"\bconfirm\s+before\b"
        r"|\bwithout\s+confirm\b"
        r"|\bhave\s+to\s+confirm\b"
        r"|\bpress\s+confirm\b",
        re.I,
    ),
     ("confirm", "requires_confirm")),
    (re.compile(r"\bkafka\s+as\s+a\s+source\b|\bsource\s+from\s+kafka\b", re.I),
     ("kafka", "source", "driver")),
    (re.compile(r"\bglue\s+catalog\b|\biceberg\s+catalog\b", re.I),
     ("glue", "iceberg", "catalog")),
    (re.compile(
        r"\b(?:chat\s*gpt|chatgpt|openai|anthropic|"
        r"third[\s-]?party\s+(?:llm|model|engine)|"
        r"generative\s+llm|"
        r"(?:our|your)\s+own\s+(?:llm|engine|model))\b",
        re.I,
    ),
     ("local", "engine", "polish", "hybrid")),
    (CDC_DELIVERY_RE, ("cdc", "least", "once", "idempotent", "lsn")),
    (re.compile(r"\b_df_lsn\b|\bdf[\s_]?lsn\b", re.I),
     ("lsn", "idempotent", "cdc")),
    (re.compile(r"\bskip\s+dup(?:e|licate)s?\b", re.I),
     ("lsn", "idempotent", "cdc")),
    (re.compile(r"\blogical\s+decoding\b", re.I),
     ("wal_level", "pgoutput", "logical")),
    (re.compile(r"\bg([1-9])\b", re.I),
     ("gate", "preflight", "validate", "schema")),
    (re.compile(r"\bhand[\s-]?off\b"
                r"|\bsnapshot\s+to\s+(?:the\s+)?(?:cdc|stream|log)\b"
                r"|\bbetween\s+snapshot\s+and\s+stream\b", re.I),
     ("snapshot", "handoff", "capture", "stream", "lsn")),
    (re.compile(r"\bwal\b|\bwrite[\s-]?ahead\b", re.I),
     ("wal", "log", "capture", "postgres")),
    (re.compile(r"\b(?:cdc\s+)?deletes?\b.+\b(?:cdc|stream|log)\b"
                r"|\bdelete\s+in\s+cdc\b", re.I),
     ("tombstone", "delete", "cdc", "soft")),
    (re.compile(r"\bcreate[\s-]?new\b"
                r"|\b93\s*%\s+identity\b"
                r"|\bidentity\s+mapping\b", re.I),
     ("create-new", "schema", "certificate", "identity")),
    (re.compile(r"\bwal[\s_]?level\b", re.I),
     ("wal_level", "logical", "cdc", "postgres")),
    (re.compile(r"\breplication\s+slots?\b", re.I),
     ("slot", "wal", "cdc", "postgres")),
    (re.compile(r"\bpgoutput\b|\boutput\s+plugin\b|\bcdc\s+plugin\b", re.I),
     ("pgoutput", "logical", "cdc", "postgres")),
    (re.compile(r"\btoast\b", re.I),
     ("toast", "pgoutput", "cdc")),
    (re.compile(r"\bcdc\s+lag\b|\breplication\s+lag\b", re.I),
     ("lag", "watermark", "theater", "wal")),
    (re.compile(r"\bgreen\s+test\b|\btest\s+passed\b.+\bskip\b"
                r"|\bskip\s+(?:validat|preflight)", re.I),
     ("preflight", "validate", "test")),
    (re.compile(r"\breplica\s+identity\b", re.I),
     ("replica", "identity", "full", "toast", "cdc")),
    (re.compile(r"\bbinlog[\s_]?format\b|\bbinlog[\s_]?row[\s_]?image\b", re.I),
     ("binlog", "row", "mysql", "cdc")),
    (re.compile(r"\bpublication\b", re.I),
     ("publication", "slot", "cdc", "postgres")),
    (re.compile(r"\bmax[\s_]?replication[\s_]?slots\b", re.I),
     ("slot", "quota", "cdc", "postgres")),
    (re.compile(r"\b(?:change[\s-]?stream\s+)?pre[\s-]?images?\b", re.I),
     ("preimage", "changestream", "mongo", "delete")),
    (re.compile(r"\bmerge[\s-]?on[\s-]?read\b|\bcopy[\s-]?on[\s-]?write\b", re.I),
     ("iceberg", "merge", "upsert", "overwrite")),
    (re.compile(r"\bsemantic\s+column\s+mapping\b|\bhow\s+does\s+mapping\b", re.I),
     ("semantic", "synonym", "confidence")),
    (re.compile(r"\bwho\s+sees\s+a\s+connector\b"
                r"|\blimit\s+who\s+sees\b", re.I),
     ("viewer", "rbac", "permission", "connector")),
    (re.compile(r"\bmask\s+pii\b|\bhash\s+pii\b|\bredact\s+pii\b", re.I),
     ("pii", "mask", "redact", "hash")),
    (re.compile(r"\bgtid\b", re.I),
     ("gtid", "binlog", "mysql", "watermark")),
    (re.compile(r"\bbackfill\b", re.I),
     ("backfill", "watermark", "cdc", "resume")),
    (re.compile(r"\badd(?:ing)?\s+a\s+column\b"
                r"|\bcolumn\s+during\s+cdc\b"
                r"|\bnew\s+column\s+during\b", re.I),
     ("schema", "drift", "policy", "column")),
    (re.compile(r"\bcdc\s+(?:user\s+)?privileges?\b"
                r"|\bprivileges?.{0,48}(?:cdc|postgres|postgresql)\b"
                r"|\b(?:postgres|postgresql|cdc).{0,48}privileges?\b"
                r"|\balter\s+role\b"
                r"|\breplication\s+(?:grant|privilege|client|slave)\b", re.I),
     ("replication", "grant", "privilege", "cdc")),
)


@lru_cache(maxsize=1)
def _generic_and_stop() -> frozenset[str]:
    from .lexical_index import STOPWORDS

    return frozenset(GENERIC_QUESTION_WORDS | STOPWORDS)


@lru_cache(maxsize=1)
def _expansion_lookup() -> dict[str, tuple[str, ...]]:
    """``_CONCEPT_EXPANSIONS`` keyed the way a retrieval term actually arrives.

    Lookups happen on ``content_terms`` output, which is stemmed and aliased, so
    a table written in natural surface forms silently misses a quarter of its own
    entries — ``gates``, ``permissions``, ``mapping`` and ``rejected`` all stem
    away from their keys. Normalizing the keys here keeps the table readable in
    the form an operator would type while still matching.
    """
    merged: dict[str, list[str]] = {}
    for surface, targets in _CONCEPT_EXPANSIONS.items():
        bucket = merged.setdefault(normalize(surface), [])
        for target in targets:
            if target not in bucket:
                bucket.append(target)
    return {key: tuple(values) for key, values in merged.items()}


@dataclass(frozen=True)
class QueryAnalysis:
    """One question, understood well enough to retrieve and answer it."""

    text: str
    ask: str
    terms: tuple[str, ...] = ()
    expansions: tuple[str, ...] = ()
    generic_terms: tuple[str, ...] = field(default=())
    #: Expansions that came from an exact multi-word phrase match. These are not
    #: guesses: "change data capture" *is* CDC, "bad rows" *are* quarantined
    #: rows. Retrieval trusts them as much as the words the operator typed,
    #: which single-word expansions ("slow" → "phase") have not earned.
    phrase_expansions: tuple[str, ...] = field(default=())

    @property
    def anchor_terms(self) -> tuple[str, ...]:
        """The operator's own words plus the phrases that restate them exactly."""
        seen = set(self.terms)
        out = list(self.terms)
        for term in self.phrase_expansions:
            if term not in seen:
                seen.add(term)
                out.append(term)
        return tuple(out)

    @property
    def loose_expansions(self) -> tuple[str, ...]:
        """Single-word expansions only — recall, not evidence."""
        anchors = set(self.anchor_terms)
        return tuple(t for t in self.expansions if t not in anchors)

    @property
    def search_terms(self) -> tuple[str, ...]:
        """Original content terms first, then the documentation's words for them."""
        seen = set(self.terms)
        out = list(self.terms)
        for term in self.expansions:
            if term not in seen:
                seen.add(term)
                out.append(term)
        return tuple(out)

    @property
    def expanded_text(self) -> str:
        """The question rewritten for a bag-of-words retriever."""
        return " ".join(self.search_terms)


# Words that are the frame of a question only in the company they keep, so the
# global stopword list cannot hold them. Each entry is a pattern and the words
# it consumes; when the pattern matches, those words stop counting as subject
# terms. The pattern is what carries the meaning instead — every frame here has
# a ``_PHRASE_EXPANSIONS`` entry that names the subject in the documentation's
# own vocabulary, so nothing is lost by dropping the operator's spelling of it.
#
# ``limit`` is the measured case. Asked "can I limit who sees a connector", the
# highest-scoring sentence in the whole corpus was "**Beta** — works with known
# limits" — a connector maturity label — because the verb of restriction and the
# noun of limitation stem to the same token, and the label says it in a short
# sentence that normalizes well. It outscored "Every role is one of viewer,
# operator, editor or admin" by more than two to one, so no relevance floor
# could have removed it: it was not padding below the answer, it *was* the lead.
#
# Kept deliberately narrow. ``limit`` is dropped only next to who/access, where
# it cannot be the noun; asked "what are the limits of the free tier" it is
# still the subject, and asked "does it rate limit" it is still the subject.
_FRAME_PHRASES: tuple[tuple[re.Pattern[str], tuple[str, ...]], ...] = (
    (
        re.compile(r"\b(?:limit|restrict|control)\s+(?:who|access|which\s+\w+\s+can)\b", re.I),
        ("limit", "restrict", "control"),
    ),
    # "SCD type 1/2" — the digit is the mode; ``type`` is filler that
    # otherwise ties every SCD card to the live-tile list and lets SCD1
    # steal "what is SCD type 2".
    (
        re.compile(r"\bscd\s*(?:type\s*)?[12]\b", re.I),
        ("type",),
    ),
    # "Workload Identity" is GCP. Left as ``identity`` it retrieved
    # Postgres REPLICA IDENTITY FULL.
    (
        re.compile(r"\bworkload\s+identity\b", re.I),
        ("identity",),
    ),
    # "Private Service Connect" shares ``connect`` with Kafka Connect.
    (
        re.compile(r"\bprivate\s+service\s+connect\b", re.I),
        ("connect", "service"),
    ),
    # Column-level vs OpenLineage run/dataset grain.
    (
        re.compile(r"\bcolumn[\s-]level\s+lineage\b", re.I),
        ("lineage",),
    ),
    # "SLA for job runtime" names a warranty this product does not publish.
    # Left as content terms it retrieved the Airbyte pack card on ``runtime``
    # and the data-loss card on ``SLA``. Framing the span drops those words
    # so the question stays refused, like uptime and price.
    (
        re.compile(
            r"\bsla\s+for\s+(?:a\s+)?(?:job|transfer)\s+runtime\b"
            r"|\b(?:job|transfer)\s+runtime\s+sla\b",
            re.I,
        ),
        # Leave ``sla`` — it is not a product heading, so the leftover
        # question has no subject and is refused. Framing ``sla`` too would
        # empty the term set and fall back to the raw words, including ``job``.
        ("job", "runtime"),
    ),
    # "how much does datawrap cost" names the brand, so retrieval treated
    # ``datawrap`` as a covered subject and spoke Workload Identity. Price
    # is not a product heading — frame the brand and the commercial words
    # so the ask stays refused, like "how much does it cost".
    (
        re.compile(
            r"\bhow\s+much\s+does\b.+\bcost\b"
            r"|\b(?:what\s+is|what'?s)\s+the\s+pric(?:e|ing)\b"
            r"|\bdo\s+you\s+have\s+pric(?:e|ing)\b"
            r"|\bpric(?:e|ing)\s+(?:page|plan|tier|model)\b",
            re.I,
        ),
        ("cost", "price", "pric", "datawrap", "dataflow"),
    ),
    # Numbered-gate asks have their own card. Left as ``gate`` they
    # retrieved the G1–G9 listing (or a neighbor card).
    (
        re.compile(r"\bgate\s*[1-9]\b|\bg[1-9]\b", re.I),
        ("gate", "preflight", "block", "validat"),
    ),
    # Salesforce OAuth / Connected App is not the transfer-ready driver card
    # and not full_refresh.
    (
        re.compile(
            r"\bsalesforce\s+oauth\b"
            r"|\bconnected\s+app\b"
            r"|\brefresh\s+salesforce\s+tokens?\b",
            re.I,
        ),
        ("salesforce", "refresh"),
    ),
    # "Teams alerts" is Microsoft Teams, not Team roles / SSO.
    (
        re.compile(r"\b(?:microsoft\s+)?teams?\s+alerts?\b", re.I),
        ("team", "send"),
    ),
    # Teams/Slack as a destination is not the webhook alert card.
    (
        re.compile(
            r"\bteams?\s+as\s+a\s+destination\b"
            r"|\bmicrosoft\s+teams\s+as\s+a\s+destination\b",
            re.I,
        ),
        ("alert", "webhook", "notify"),
    ),
    (
        re.compile(
            r"\bis\s+slack\s+a\s+connector\b"
            r"|\bslack\s+(?:as\s+a\s+)?(?:source|destination|connector)\b",
            re.I,
        ),
        ("alert", "webhook", "notify"),
    ),
    # Email alerts are not Teams / Slack.
    (
        re.compile(r"\bemail\s+alerts?\b", re.I),
        ("team", "slack", "webhook"),
    ),
    # RLS is not the row ledger.
    (
        re.compile(r"\brow[\s-]level\s+security\b|\brls\b", re.I),
        ("ledger", "quarantine", "lineage"),
    ),
    # Dynamic Tables are not the Snowflake driver card.
    (
        re.compile(r"\bdynamic\s+tables?\b", re.I),
        ("snowflake", "driver", "transfer"),
    ),
    # Azure Key Vault is the vault client, not BYOK cells.
    (
        re.compile(r"\bazure\s+key\s+vault\b", re.I),
        ("byok", "kms", "key"),
    ),
    # Viewer-see-secrets is RBAC, not BYOK wrapping. Framing ``byok``
    # drops the viewer-secrets expansion from pulling the BYOK card
    # into the lead ("Settings → Enterprise → BYOK wraps…").
    (
        re.compile(
            r"\b(?:can\s+a\s+)?viewer\s+(?:see|read|export|view)\s+secrets?\b"
            r"|\bsecrets?\s+to\s+a\s+viewer\b",
            re.I,
        ),
        ("byok", "kms", "wrap"),
    ),
    # Field-level encryption is not column-level lineage and not BYOK cells.
    (
        re.compile(r"\bfield[\s-]level\s+encryption\b|\bcolumn\s+encryption\b", re.I),
        ("encryption", "field", "level", "lineage", "byok"),
    ),
    # Kafka consumer groups are not SQL GROUP BY.
    (
        re.compile(r"\b(?:kafka\s+)?consumer\s+groups?\b", re.I),
        ("group", "consumer", "aggregat", "kafka"),
    ),
    # Pause CDC is cadence + slot keep, not the pipeline-drawer caption,
    # REPLICA IDENTITY / Mongo pre-images / snapshot handoff, or the
    # WAL / publication definition the slot expansion would otherwise add.
    (
        PAUSE_CDC_RE,
        ("drawer", "edit", "identity", "preimage", "handoff", "toast", "wal", "publication"),
    ),
    (
        re.compile(r"\bpause\s+a\s+nightly\b", re.I),
        ("full_refresh", "overwrite", "incremental", "upsert"),
    ),
    # Salesforce connect is New connection + pasted token, not OAuth
    # "connect fields" and not the transfer-ready driver card.
    (
        re.compile(
            r"\bhow\s+do\s+i\s+connect\s+salesforce\b"
            r"|\badd\s+a\s+salesforce\s+connection\b"
            r"|\bset\s+up\s+salesforce\b",
            re.I,
        ),
        ("oauth", "refresh", "driver"),
    ),
    # A read replica is not REPLICA IDENTITY FULL.
    (
        re.compile(r"\bread\s+replica\b|\bphysical\s+standby\b|\bhot\s+standby\b", re.I),
        ("identity", "toast", "full"),
    ),
    # LogMiner is the Oracle capture plugin, not the destination driver.
    (
        re.compile(r"\blogminer\b", re.I),
        ("goldengate", "destination", "driver"),
    ),
    # SQL Server CDC / change tracking is not the generic WAL definition.
    (
        re.compile(r"\bsql\s+server\s+cdc\b|\bchange\s+tracking\b", re.I),
        ("wal", "binlog", "oplog"),
    ),
    # Capture-side CDC filters are not GTID / watermark.
    (
        re.compile(r"\bfilter\s+cdc\b|\bcdc\s+event\s+filter\b", re.I),
        ("gtid", "watermark", "heartbeat"),
    ),
    # Skip-deletes is not the apply-delete / tombstone card.
    (
        re.compile(
            r"\bskip\s+deletes?\b"
            r"|\bignore\s+deletes?\b",
            re.I,
        ),
        ("tombstone", "is_active", "preflight", "validate"),
    ),
    # Heartbeat interval is not the INTERVAL type carrier.
    (
        re.compile(r"\bheartbeat\s+interval\b|\bset\s+a\s+cdc\s+heartbeat\b", re.I),
        ("interval", "type", "carrier"),
    ),
    # Fetch/batch size is not Job Theater duration.
    (
        re.compile(r"\bcdc\s+fetch\s+size\b|\bcdc\s+batch\s+size\b", re.I),
        ("phase", "duration", "delete", "theater"),
    ),
    # XStream is not the LogMiner "yes" card.
    (
        re.compile(r"\bxstream\b", re.I),
        ("logminer", "driver", "destination"),
    ),
    # Always On is not the SQL Server driver card.
    (
        re.compile(r"\balways\s+on\b|\bavailability\s+groups?\b", re.I),
        ("driver", "sqlserver", "cdc"),
    ),
    # Fabric / OneLake is not Teams alerts.
    (
        re.compile(r"\bmicrosoft\s+fabric\b|\bonelake\b|\bfabric\s+as\s+a\s+destination\b", re.I),
        ("team", "alert", "webhook", "notify"),
    ),
    # Managed identity is not Synapse and not Workload Identity.
    (
        re.compile(r"\bmanaged\s+identity\b", re.I),
        ("synapse", "workload", "identity"),
    ),
    # Cosmos is not Mongo pre-images.
    (
        re.compile(r"\bcosmos\s*db\b", re.I),
        ("mongo", "preimage", "preimag"),
    ),
    # Event Hubs is not OpenLineage events.
    (
        re.compile(r"\bevent\s+hubs?\b", re.I),
        ("lineage", "openlineage", "event"),
    ),
    # Service Bus is not the ADLS service principal.
    (
        re.compile(r"\bservice\s+bus\b", re.I),
        ("principal", "adls", "service_account"),
    ),
    # ADF is not Synapse.
    (
        re.compile(r"\bazure\s+data\s+factory\b|\badf\b", re.I),
        ("synapse", "contract"),
    ),
    # Cloud SQL is not the SQL Server driver and not Azure SQL.
    (
        re.compile(r"\bcloud\s+sql\b", re.I),
        ("sqlserver", "sql", "server", "driver", "azure", "mssql"),
    ),
    # Azure SQL is the SQL Server driver, not Synapse and not Cloud SQL.
    (
        re.compile(r"\bazure\s+sql\b", re.I),
        ("synapse", "cloud", "fabric"),
    ),
    # GCS dest is not the Iceberg Glue catalog card.
    (
        re.compile(
            r"\bgcs\s+as\s+a\s+destination\b"
            r"|\bwrite\s+to\s+gcs\b"
            r"|\bdo\s+you\s+support\s+gcs\b"
            r"|\bgoogle\s+cloud\s+storage\b",
            re.I,
        ),
        ("glue", "catalog", "iceberg"),
    ),
    # Pub/Sub is not a destination-count listing.
    (
        re.compile(r"\bpub[\s/-]?sub\b", re.I),
        ("destination", "count", "kafka"),
    ),
    # Spanner is not dbt Cloud.
    (
        re.compile(r"\bcloud\s+spanner\b|\bspanner\b", re.I),
        ("dbt", "cloud", "complement"),
    ),
    # Vertex is not Settings → AI Hybrid polish.
    (
        re.compile(r"\bvertex\s+ai\b", re.I),
        ("hybrid", "llm", "polish", "chatgpt", "engine"),
    ),
    # SharePoint is not a warehouse driver card.
    (
        re.compile(r"\bsharepoint\b", re.I),
        ("bigquery", "warehouse"),
    ),
    # Dynamics / Dataverse is not Snowflake Dynamic Tables.
    (
        re.compile(r"\bdynamics\s*365\b|\bdataverse\b", re.I),
        ("snowflake", "dynamic", "salesforce"),
    ),
    # Purview is not Teams alerts.
    (
        re.compile(r"\bpurview\b", re.I),
        ("team", "webhook", "alert", "notify"),
    ),
    # Excel Online is not the file-format list.
    (
        re.compile(r"\bexcel\s+online\b|\bexcel\s+365\b", re.I),
        ("format", "csv", "parquet", "xlsx", "exchange"),
    ),
    # IAM assume-role is not Private Link and not an RBAC role count.
    (
        re.compile(
            r"\bassume\s+an?\s+aws\s+iam\s+role\b"
            r"|\biam\s+role\b"
            r"|\bsts:assumerole\b",
            re.I,
        ),
        ("privatelink", "role", "rbac", "viewer"),
    ),
    # Custom slot name is derived, not the WAL definition or slot quota.
    (
        CUSTOM_SLOT_NAME_RE,
        ("wal", "publication", "quota", "max_replication"),
    ),
    # SCD2 is not the SCD1 leftover upsert sentence.
    (
        re.compile(r"\bscd\s*(?:type\s*)?2\b", re.I),
        ("scd1", "upsert", "mirror", "leftover"),
    ),
    # Incremental-by-updated_at is the cursor mode, not upsert-vs-incremental.
    (
        INCREMENTAL_UPDATED_AT_RE,
        ("upsert", "merge", "whole"),
    ),
    # Blue-green is not Pause/Activate.
    (
        re.compile(
            r"\bblue[\s-]green\b"
            r"|\bzero[\s-]downtime\s+cutover\b"
            r"|\bcut\s+over\s+with\s+zero\s+downtime\b",
            re.I,
        ),
        ("pause", "activate", "cadence"),
    ),
    (
        re.compile(r"\bazure\s+database\s+for\s+postgresql\b", re.I),
        ("cloud", "sql", "sqlserver", "mysql"),
    ),
    (
        re.compile(r"\bazure\s+database\s+for\s+mysql\b", re.I),
        ("cloud", "sql", "sqlserver", "postgresql"),
    ),
    (
        re.compile(r"\bcloud\s+sql\s+for\s+sql\s+server\b", re.I),
        ("mysql", "postgresql", "azure"),
    ),
    (
        re.compile(r"\bonedrive\b", re.I),
        ("bigquery", "sharepoint", "warehouse"),
    ),
    (
        re.compile(r"\blooker\s+studio\b", re.I),
        ("studio", "transfer", "bigquery"),
    ),
    (
        re.compile(r"\blooker\b(?!\s+studio)", re.I),
        ("bigquery", "studio"),
    ),
    (
        re.compile(r"\bbigquery\s+omni\b", re.I),
        ("driver", "destination"),
    ),
    (
        re.compile(r"\bgoogle\s+sheets\b", re.I),
        ("pubsub", "excel"),
    ),
    (
        re.compile(r"\balloydb\b", re.I),
        ("cloud", "sql"),
    ),
    (
        re.compile(r"\bmicrosoft\s+graph\b|\bms\s+graph\b", re.I),
        ("team", "webhook", "alert"),
    ),
    (
        re.compile(r"\bazure\s+openai\b", re.I),
        ("hybrid", "llm", "chatgpt", "pilot", "engine"),
    ),
    (
        re.compile(r"\bazure\s+data\s+explorer\b|\bkusto\b", re.I),
        ("adf", "factory", "synapse"),
    ),
    (
        re.compile(r"\bevent\s+grid\b", re.I),
        ("hubs", "event_hubs", "lineage"),
    ),
    (
        re.compile(r"\bgoogle\s+(?:cloud\s+)?dataflow\b|\bcloud\s+dataflow\b", re.I),
        ("pubsub", "datawrap"),
    ),
    (
        re.compile(r"\bpower\s+platform\b", re.I),
        ("power_bi", "powerbi", "fabric"),
    ),
    (
        re.compile(r"\bconditional\s+access\b", re.I),
        ("synapse", "allowlist", "cidr"),
    ),
    (
        re.compile(r"\bcloud\s+composer\b", re.I),
        ("azure", "sql", "cloud_sql"),
    ),
    (
        re.compile(r"\bflexible\s+server\b|\bazure\s+postgresql\s+flexible\b", re.I),
        ("cloud", "sql", "sqlserver"),
    ),
    (
        re.compile(r"\bgke\b|\bgoogle\s+kubernetes\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(r"\boutlook\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(r"\byoutube\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(r"\bgoogle\s+ads\b", re.I),
        ("pubsub", "bigquery"),
    ),
    (
        re.compile(r"\bgoogle\s+drive\b|\bgoogle\s+docs\b|\bgoogle\s+analytics\b", re.I),
        ("pubsub",),
    ),
    (
        re.compile(r"\bcloud\s+run\b", re.I),
        ("dataflow", "pubsub"),
    ),
    (
        re.compile(r"\bstream\s+analytics\b", re.I),
        ("cosmos", "mongo", "preimage"),
    ),
    (
        re.compile(r"\bmicrosoft\s+lists\b", re.I),
        ("team", "webhook"),
    ),
    (
        re.compile(r"\bexchange\s+online\b", re.I),
        ("excel", "workbook"),
    ),
    (
        re.compile(r"\bgithub\s+enterprise\s+as\s+a\s+destination\b", re.I),
        ("actions", "airflow"),
    ),
    (
        re.compile(r"\bbigquery\s+data\s+transfer\b|\bbq\s+dts\b", re.I),
        ("service_account", "credential"),
    ),
    (
        re.compile(r"\blinked\s+dataset\b", re.I),
        ("openlineage", "lineage"),
    ),
    (
        re.compile(r"\bcloud\s+kms\b", re.I),
        ("byok", "wrap", "secret"),
    ),
    (
        re.compile(r"\bapp\s+engine\b", re.I),
        ("allowlist", "cidr", "vanity"),
    ),
    (
        re.compile(r"\bdata\s+catalog\b", re.I),
        ("iceberg", "merge", "upsert"),
    ),
    (
        re.compile(r"\bcloud\s+build\b", re.I),
        ("chromium", "firefox", "safari", "browser"),
    ),
    (
        re.compile(r"\bartifact\s+registry\b", re.I),
        ("schema", "registry", "kafka"),
    ),
    (
        re.compile(r"\bcloud\s+functions?\b", re.I),
        ("cloud", "sql"),
    ),
    (
        re.compile(r"\bazure\s+files\b", re.I),
        ("synapse",),
    ),
    (
        re.compile(r"\blog\s+analytics\b", re.I),
        ("theater", "job", "tab"),
    ),
    (
        re.compile(r"\bazure\s+ai\s+search\b|\bcognitive\s+search\b", re.I),
        ("openai", "hybrid"),
    ),
    (
        re.compile(r"\bazure\s+analysis\s+services\b|\bssas\b", re.I),
        ("service_bus", "bus"),
    ),
    (
        re.compile(r"\bcloud\s+sql\s+auth\s+proxy\b|\bcloud\s+sql\s+proxy\b", re.I),
        ("driver",),
    ),
    (
        re.compile(r"\bsynapse\s+link\b", re.I),
        ("warehouse", "driver"),
    ),
    (
        re.compile(r"\bazure\s+table\b|\btable\s+storage\b", re.I),
        ("blob", "adls", "azure_blob"),
    ),
    (
        re.compile(r"\bazure\s+queue\b|\bqueue\s+storage\b", re.I),
        ("blob", "adls", "azure_blob"),
    ),
    (
        re.compile(
            r"\bsql\s+server\s+on\s+(?:an?\s+)?azure\s+vms?\b"
            r"|\bsql\s+server\s+on\s+azure\s+(?:virtual\s+machines?|vms?)\b",
            re.I,
        ),
        ("postgresql", "flexible", "postgres"),
    ),
    (
        re.compile(r"\bsplunk\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(r"\btableau\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(
            r"\bdata\s+lake\s+gen\s*2\b"
            r"|\badls\s+gen\s*2\b"
            r"|\bazure\s+data\s+lake\b",
            re.I,
        ),
        ("kusto", "explorer"),
    ),
    (
        re.compile(
            r"\bservice\s+principal\s+for\s+azure\s+sql\b"
            r"|\bazure\s+sql\b.{0,40}\bservice\s+principal\b"
            r"|\bservice\s+principal\b.{0,40}\bazure\s+sql\b",
            re.I,
        ),
        ("adls", "blob", "principal", "service_account", "tenant"),
    ),
    (
        re.compile(r"\belastic\s+cloud\b", re.I),
        ("bigtable", "opensearch"),
    ),
    (
        re.compile(r"\bopensearch\b", re.I),
        ("elasticsearch", "elastic"),
    ),
    (
        re.compile(r"\b(?:microsoft\s+)?teams\s+as\s+a\s+source\b", re.I),
        ("graph", "webhook"),
    ),
    (
        re.compile(
            r"\b(?:microsoft|office)\s+365\s+as\s+a\s+destination\b"
            r"|\bm365\s+as\s+a\s+destination\b",
            re.I,
        ),
        ("excel_online", "excel"),
    ),
    (
        re.compile(r"\bintune\b|\bdefender\b|\bsentinel\b", re.I),
        ("teams_dest", "team", "webhook"),
    ),
    (
        re.compile(r"\bazure\s+machine\s+learning\b|\bazure\s+ml\b", re.I),
        ("openai", "hybrid", "llm"),
    ),
    (
        re.compile(r"\bsearch\s+ads\s+360\b|\bsa360\b", re.I),
        ("search", "openai", "google_ads"),
    ),
    (
        re.compile(r"\bcloud\s+tasks?\b", re.I),
        ("cloud_run", "dataflow", "pubsub"),
    ),
    (
        re.compile(r"\bvpc\s+service\s+controls?\b", re.I),
        ("privatelink", "private", "link"),
    ),
    (
        re.compile(r"\bazure\s+firewall\b", re.I),
        ("allowlist", "cidr", "browser"),
    ),
    (
        re.compile(r"\bcampaign\s+manager(?:\s+360)?\b|\bcm360\b", re.I),
        ("secret", "manager", "google_ads"),
    ),
    (
        re.compile(r"\baurora\b", re.I),
        ("adls", "blob", "source"),
    ),
    (
        re.compile(r"\bdocumentdb\b|\bdocdb\b", re.I),
        ("mongo", "mongodb"),
    ),
    (
        re.compile(r"\bcloud\s+armor\b|\bcloud\s+interconnect\b|\bcloud\s+vpn\b", re.I),
        ("cloud_run", "dataflow", "pubsub"),
    ),
    (
        re.compile(
            r"\bazure\s+arc\b|\bazure\s+lighthouse\b|\bazure\s+monitor\b"
            r"|\bazure\s+devops\b|\bazure\s+boards\b|\bazure\s+migrate\b",
            re.I,
        ),
        ("synapse", "azure_synapse"),
    ),
    (
        re.compile(r"\bdisplay\s*(?:and|&)\s*video\s*360\b|\bdv360\b", re.I),
        ("campaign_manager", "ads"),
    ),
    (
        re.compile(
            r"\b(?:google\s+cloud\s+)?storage\s+transfer\s+service\b"
            r"|\bgcs\s+transfer\s+service\b",
            re.I,
        ),
        ("dataflow", "dataflow_google"),
    ),
    (
        re.compile(r"\bqlik\b", re.I),
        ("mirror", "upsert", "leftover"),
    ),
    (
        re.compile(r"\bentra\s+id\s+governance\b|\bentra\s+governance\b", re.I),
        ("saml", "oidc", "okta", "sso"),
    ),
    (
        re.compile(r"\bcloud\s+sql\s+for\s+mysql\b", re.I),
        ("sqlserver", "proxy"),
    ),
    (
        re.compile(r"\bhdinsight\b|\bexpressroute\b|\bexpress\s+route\b|\bazure\s+cdn\b|\bazure\s+blueprints?\b|\bazure\s+automation\b", re.I),
        ("mysql", "azure_database_mysql", "principal"),
    ),
    (
        re.compile(r"\bssis\b|\bsql\s+server\s+integration\s+services\b", re.I),
        ("sqlserver", "driver"),
    ),
    (
        re.compile(r"\bssrs\b|\bsql\s+server\s+reporting\s+services\b", re.I),
        ("sqlserver", "driver", "power_bi"),
    ),
    (
        re.compile(r"\bgmail\b", re.I),
        ("bigquery", "warehouse"),
    ),
    (
        re.compile(r"\bgoogle\s+calendar\b|\bcalendar\s+as\s+a\s+source\b", re.I),
        ("adls", "source", "blob"),
    ),
    (
        re.compile(r"\bbigquery\s+ml\b|\bbqml\b", re.I),
        ("destination", "warehouse"),
    ),
    (
        re.compile(r"\brds\s+for\s+postgresql\b|\brds\s+postgres|\brds\s+for\s+mysql\b", re.I),
        ("aurora",),
    ),
    (
        re.compile(r"\bfilestore\b", re.I),
        ("cloud_run", "data_catalog"),
    ),
    (
        re.compile(r"\bdatastream\b", re.I),
        ("cloud_run", "dataflow"),
    ),
    (
        re.compile(r"\bdataplex\b|\bmatillion\b", re.I),
        ("azure_monitor", "monitor"),
    ),
    (
        re.compile(r"\binformatica\b", re.I),
        ("procedure", "stored"),
    ),
    (
        re.compile(r"\btalend\b", re.I),
        ("append", "overwrite"),
    ),
    (
        re.compile(r"\bgoogle\s+workspace\b|\bgsuite\b", re.I),
        ("google_ads", "ads"),
    ),
    (
        re.compile(r"\baks\b|\bazure\s+kubernetes\b", re.I),
        ("azure_monitor", "monitor"),
    ),
    (
        re.compile(r"\bazure\s+functions?\b", re.I),
        ("cloud_functions", "cloud"),
    ),
    (
        re.compile(r"\bazure\s+batch\b", re.I),
        ("cdc_fetch_size", "fetch"),
    ),
    (
        re.compile(r"\bcloud\s+scheduler\b", re.I),
        ("cloud_run", "dataflow"),
    ),
    (
        re.compile(r"\bvertex\s+ai\s+search\b", re.I),
        ("azure_ai_search", "search"),
    ),
    (
        re.compile(r"\bbusiness\s+central\b", re.I),
        ("dynamics365", "dataverse"),
    ),
    (
        re.compile(r"\bentra\s+external\s+id\b", re.I),
        ("api", "rest"),
    ),
    (
        re.compile(r"\bazure\s+ad\s+b2c\b|\bb2c\b", re.I),
        ("adls", "principal", "service_account"),
    ),
    (
        re.compile(r"\bcloud\s+sql\s+for\s+postgresql\b", re.I),
        ("mysql", "proxy"),
    ),
    # Bare Private Link is not Job Theater.
    (
        re.compile(r"\bprivate\s+link\b", re.I),
        ("theater", "job", "open"),
    ),
    # Secrets Manager / Vault is not PrivateLink and not viewer secrets.
    (
        re.compile(
            r"\baws\s+secrets?\s+manager\b"
            r"|\bhashicorp\s+vault\b"
            r"|\bsecrets?\s+manager\b",
            re.I,
        ),
        ("secret", "aws", "manager", "privatelink"),
    ),
)


def frame_words(question: str) -> frozenset[str]:
    """Words this question uses to frame its subject rather than to name it."""
    out: set[str] = set()
    for pattern, words in _FRAME_PHRASES:
        if pattern.search(question or ""):
            out.update(normalize(w) for w in words)
    return frozenset(out)


# ChatGPT's first move is to hear the question the operator meant, not the
# tokens they typed. We do the same deterministically: strip chat filler,
# expand slang, and repair the misspellings routing already knew about. The
# rewrite never invents a product claim — it only changes how the question
# is spelled so retrieval can find the sentence that already exists.
_CHAT_PREFIX = re.compile(
    r"^\s*(?:hey|hi|hello|yo|sup|so|ok(?:ay)?|um+|uh+|well|"
    r"wait\s+so|so\s+wait|"
    r"listen|look|"
    r"honestly|basically|"
    r"in\s+other\s+words|"
    r"idk\s+but|"
    r"confused\s+about|"
    r"regarding|re|"
    r"mind\s+explaining|"
    r"is\s+there\s+a\s+way\s+to|"
    r"would\s+it\s+be\s+possible\s+to|"
    r"i\s+was\s+(?:just\s+)?wondering|"
    r"just\s+wondering|"
    r"can\s+you\s+tell\s+me(?:\s+if|\s+whether)?|"
    r"could\s+you\s+tell\s+me(?:\s+if|\s+whether)?|"
    r"do\s+you\s+know(?:\s+if|\s+whether)?|"
    r"any\s+idea(?:\s+if|\s+whether)?|"
    r"quick(?:\s+question|\s+q)?|question|"
    r"pls|please|kinda\s+confused|"
    r"real(?:ly)?\s+quick|real\s+talk|"
    r"q)[,:]?\s+",
    re.I,
)
_CHAT_SUFFIX = re.compile(
    r"[\s,]+(?:right|yeah|yes|no|pls|please|thanks|thx|lol|lmk|idk|"
    r"or\s+nah|or\s+what|tho|though|tbh)\s*[?.!]*\s*$",
    re.I,
)
_PLAIN_ENGLISH = re.compile(
    r"\b(?:explain\s+like\s+(?:i(?:'?| a)?m\s+)?(?:5|five)|"
    r"eli5|in\s+(?:plain|simple)\s+(?:english|terms)|"
    r"in\s+one\s+sentence|tldr|tl;dr|simply\s+put|"
    r"in\s+layman'?s?\s+terms)\s*[:\-–,]?\s*",
    re.I,
)
_SLANG_FRAMES: tuple[tuple[str, str], ...] = (
    (r"\bdo\s+i\s+gotta\s+have\b", "do I need"),
    (r"\bgotta\s+have\b", "do I need"),
    (r"\bdo\s+i\s+gotta\b", "do I need"),
    (r"\bgotta\s+flip\b", "need"),
    (r"\bgotta\b", "need"),
    (r"\bhow\s+come\b", "why"),
    (r"\beven\s+a\s+thing\b", ""),
    (r"\b(?:need|require)\s+logical\s+decoding\b", "need wal_level logical"),
    (r"\bskip\s+dup(?:e|licate)s?\b", "skip duplicates on _df_lsn"),
    (r"\bdupes?\b", "duplicates"),
    (r"\breplay.{0,32}same\s+(?:change|event|record)\b",
     "run the same CDC change twice"),
    (r"\bsame\s+(?:change|event|record).{0,24}replay\b",
     "run the same CDC change twice"),
    (r"\bwhere\s+do\s+(?:the\s+)?(?:bad|rejected|failed)\s+rows\s+go\b",
     "where do bad rows end up"),
    (r"\bgonna\b", "going to"),
    (r"\bwanna\b", "want to"),
    (r"\bdunno\b", "do not know"),
    (r"\b(?:what's|whats|wut'?s|wat'?s)\b", "what is"),
    (r"\b(?:how'd|howd)\b", "how do"),
    (r"\b(?:where's|wheres)\b", "where is"),
    (r"\b(?:who's|whos)\b", "who is"),
    (r"\b(?:can't|cant)\b", "cannot"),
    (r"\b(?:don't|dont)\b", "do not"),
    (r"\b(?:doesn't|doesnt)\b", "does not"),
    (r"\bu\s+sure\b", "are you sure"),
    (r"\bur\b", "your"),
    (r"\bcuz\b|\bcoz\b", "because"),
    (r"\bdo\s+i\s+need\s+to\s+flip\b", "do I need"),
    (r"\blogical\s+wal\b|\bwal\s+to\s+logical\b|\bwal\s+is\s+logical\b",
     "wal_level logical"),
    (r"\bbinlog\s+is\s+statement\b|\bstatement(?:\s+based)?\s+binlog\b",
     "binlog_format STATEMENT"),
    (r"\bdownload\s+(?:the\s+)?(?:pipeline\s+|schedule\s+)?ya?ml\b",
     "export yaml"),
    (r"\bpeek\s+at\b", "see"),
    (r"\b93\s+percent\b", "93%"),
    (r"\bcan\s+viewers\b", "can a viewer"),
    (r"\bcan\s+editors\b", "can an editor"),
    (r"\bcan\s+admins\b", "can an admin"),
    (r"\bcan\s+my\s+viewer\b", "can a viewer"),
    (r"\bcan\s+read[\s-]?only\s+users\b", "can a viewer"),
    (r"\b(?:export|download)\s+yaml\s+as\s+a\s+viewer\b",
     "can a viewer export yaml"),
    (r"\bbin\s+logs?\b", "binlog"),
    (r"\bbinlog\s+format\b", "binlog_format"),
    (r"\bwallevel\b", "wal_level"),
    (r"\brepl\s+slots?\b", "replication slot"),
    (r"\bgreen\s+checkmark\b", "green test"),
    (r"\breplciation\b|\breplicaton\b", "replication"),
    (r"\bvalidte\b|\bvaildate\b", "validate"),
    (r"\bquarentine\b", "quarantine"),
    (r"\bbinnlog\b", "binlog"),
    # High-frequency operator misspellings. Moved here so retrieval and
    # routing share one rewrite — "tranfer" is still a transfer.
    (r"\btra?ns?fe?r\b", "transfer"),
    (r"\btrasfer\b", "transfer"),
    (r"\bmigra?te?\b", "migrate"),
    (r"\bschdule\b", "schedule"),
    (r"\bmny\b", "many"),
    (r"\btbls?\b", "tables"),
    (r"\bcnt\b", "count"),
    (r"\bconnectorz\b", "connectors"),
    (r"\bdbs\b", "databases"),
    (r"\bpostgress?ql\b", "postgresql"),
    (r"\bposgres\b", "postgres"),
)

# A bare product identifier plus "?" is how operators ask for a definition.
# ChatGPT treats the fragment as "what is X". We only do it for names the
# product actually documents — "rice?" must stay "rice?".
_PRODUCT_FRAGMENT = re.compile(
    r"^\s*(?P<term>g[1-9]|wal_level|pgoutput|gtid|binlog_format|"
    r"replica\s+identity(?:\s+full)?|_df_lsn|replication\s+slots?|"
    r"type_locked|standing\s+authority|quarantine|chatgpt|"
    r"pre-?images?|toast)\s*[?.!]?\s*$",
    re.I,
)

# ChatGPT restores the dropped auxiliary. "where bad rows go" is the same
# question as "where do bad rows go"; without `do` it retrieved Job Theater.
_WHERE_BARE = re.compile(
    r"^\s*where\s+(?!do\b|does\b|did\b|can\b|is\b|are\b|will\b|would\b|"
    r"should\b|were\b|was\b)(.+?)\s+(go|end\s+up|land)\b",
    re.I,
)

_ROLE_CAN_FRAMES: tuple[tuple[str, str], ...] = (
    (r"\bviewers\s+can\b", "can a viewer"),
    (r"\beditors\s+can\b", "can an editor"),
    (r"\badmins\s+can\b", "can an admin"),
)


def rewrite_operator_question(question: str) -> str:
    """The wording the operator meant, before retrieval or tool routing.

    ChatGPT does this inside the model. We do it as an explicit rewrite so
    every ask type, expansion and tool regex sees the same cleaned question.
    Off-subject slang still expands to nothing: only discourse and known
    product misspellings change.
    """
    text = (question or "").strip()
    if not text:
        return ""
    text = _PLAIN_ENGLISH.sub("", text)
    for _ in range(4):
        nxt = _CHAT_PREFIX.sub("", text)
        if nxt == text:
            break
        text = nxt.strip()
    text = _CHAT_SUFFIX.sub("", text).strip()
    for pattern, replacement in _SLANG_FRAMES:
        text = re.sub(pattern, replacement, text, flags=re.I)
    for pattern, replacement in _ROLE_CAN_FRAMES:
        text = re.sub(pattern, replacement, text, flags=re.I)
    fragment = _PRODUCT_FRAGMENT.match(text)
    if fragment:
        text = f"what is {fragment.group('term')}"
    bare = _WHERE_BARE.match(text)
    if bare:
        verb = bare.group(2)
        if verb.lower() == "go":
            verb = "end up"
        text = f"where do {bare.group(1).strip()} {verb}" + text[bare.end():]
    text = re.sub(
        r"\bwhere\s+do\s+(?:the\s+)?(?:bad|rejected|failed)\s+rows\s+go\b",
        "where do bad rows end up",
        text,
        flags=re.I,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text or (question or "").strip()


def classify_ask(question: str) -> str:
    """What kind of answer the question wants.

    Order matters: "what is the difference between append and overwrite" is a
    comparison, not a definition, and "why did my transfer fail" is a diagnosis
    even though it also names a procedure word.
    """
    text = (question or "").strip()
    if not text:
        return "other"
    for ask, pattern in _ASK_PATTERNS:
        if ask == "definition":
            continue
        if pattern.search(text):
            return ask
    for ask, pattern in _ASK_PATTERNS:
        if ask == "definition" and pattern.search(text):
            return "definition"
    return "other"


def expand_terms(question: str, terms: tuple[str, ...]) -> tuple[str, ...]:
    """The documentation's vocabulary for what the operator said."""
    phrase, loose = expand_terms_tiered(question, terms)
    return phrase + loose


def expand_terms_tiered(
    question: str,
    terms: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Expansions split by how much they can be trusted.

    Phrase rules run on the raw question so "bad rows" can mean something its
    two words do not, and an exact multi-word match is strong evidence of the
    concept. Single-term rules then run on the normalized terms and are much
    weaker — "slow" pointing at "phase" is a useful guess, nothing more.
    """
    phrase_out: list[str] = []
    loose_out: list[str] = []
    seen = set(terms)
    lookup = _expansion_lookup()
    missing_pk = bool(
        re.search(
            r"\b(?:no|without(?:\s+a)?|missing|lack(?:s|ing)?(?:\s+a)?)\s+primary\s+key\b"
            r"|\bsource\s+has\s+no\s+primary\s+key\b",
            question or "",
            re.I,
        )
    )
    for pattern, targets in _PHRASE_EXPANSIONS:
        if not pattern.search(question or ""):
            continue
        # The identity-sense ``primary key`` expansion steals missing-PK
        # questions onto upsert / certificate aspects.
        if missing_pk and targets == ("key", "upsert", "identity", "deduped"):
            continue
        # ``replication slot`` → wal/postgres steals pause-keep onto the
        # slot-definition and wal_level cards.
        if PAUSE_CDC_RE.search(question or "") and targets == _REPLICATION_SLOT_EXPAND:
            continue
        if CUSTOM_SLOT_NAME_RE.search(question or "") and targets == _REPLICATION_SLOT_EXPAND:
            continue
        for target in targets:
            t = normalize(target)
            if t not in seen:
                seen.add(t)
                phrase_out.append(t)
    for term in terms:
        for target in lookup.get(term, ()):
            t = normalize(target)
            if t not in seen:
                seen.add(t)
                loose_out.append(t)
    return tuple(phrase_out), tuple(loose_out)


def phrase_evidence(
    question: str,
) -> tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]:
    """For each phrase rule this question fires: the words it ate, and what they mean.

    A phrase rule is a hand-written assertion that a span of the operator's own
    words names a documented concept, which is why retrieval trusts its targets
    as much as the typed words — see ``QueryAnalysis.phrase_expansions``. The
    evidence policy needs that same assertion read the other way round: *which
    typed terms the rule spoke for*, so that a concept the documentation spells
    differently is not then counted as a hole in its own answer.

    Only the terms inside the matched span are returned, not every word of the
    question. "What is change data capture zzqq" fires the CDC rule, and the
    rule has nothing to say about ``zzqq``.

    Kept per rule rather than merged into one set, because the credit is earned
    one rule at a time: it is that rule's targets which have to be in the
    evidence for its reading of the operator's words to be borne out.
    """
    text = question or ""
    missing_pk = bool(
        re.search(
            r"\b(?:no|without(?:\s+a)?|missing|lack(?:s|ing)?(?:\s+a)?)\s+primary\s+key\b"
            r"|\bsource\s+has\s+no\s+primary\s+key\b",
            text,
            re.I,
        )
    )
    out: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for pattern, targets in _PHRASE_EXPANSIONS:
        if missing_pk and targets == ("key", "upsert", "identity", "deduped"):
            continue
        if PAUSE_CDC_RE.search(text) and targets == _REPLICATION_SLOT_EXPAND:
            continue
        if CUSTOM_SLOT_NAME_RE.search(text) and targets == _REPLICATION_SLOT_EXPAND:
            continue
        consumed: list[str] = []
        for match in pattern.finditer(text):
            for term in content_terms(match.group(0)):
                if term not in consumed:
                    consumed.append(term)
        if consumed:
            out.append((tuple(consumed), tuple(normalize(t) for t in targets)))
    return tuple(out)


# Verbs and containers that appear on every wizard step. A procedure prior
# that fires on these alone is how Job Theater beat "pause a schedule".
PROCEDURE_GENERIC_TERMS = frozenset(
    {
        "connect",
        "open",
        "creat",
        "add",
        "run",
        "click",
        "use",
        "set",
        "configur",
        "enabl",
        "pipelin",
        "transfer",
        "job",
        "connector",
        "schedul",
        "step",
        "path",
        "page",
        "save",
        "pick",
        "choos",
        "procedur",
    }
)


def distinctive_procedure_terms(analysis: QueryAnalysis) -> frozenset[str]:
    """Typed words and phrase expansions that name the step, not the wizard."""
    return frozenset(
        term
        for term in (*analysis.terms, *analysis.phrase_expansions)
        if term not in PROCEDURE_GENERIC_TERMS
        and len(term) >= 3
        and "_" not in term
    )


def analyze_query(question: str) -> QueryAnalysis:
    """Understand one operator question before anything tries to retrieve for it."""
    text = rewrite_operator_question(question)
    # Dual-encoder snap onto a gold question when cosine is high and the gold
    # names no unrelated product subject. Failures stay on the deterministic
    # rewrite — a trained head must not invent a heading the operator did not ask.
    try:
        from src.ai.first_party.engine import semantic_rewrite

        snapped = semantic_rewrite(text)
        if snapped:
            text = snapped
    except Exception:
        pass
    raw = content_terms(text)
    generic = _generic_and_stop()
    # Frame words join the generic ones for this question only. They land in
    # ``generic_terms`` so the evidence policy does not demand a passage cover
    # a word the question was not really about.
    frame = frame_words(text)
    kept = tuple(t for t in raw if t not in generic and t not in frame)
    dropped = tuple(t for t in raw if t in generic or t in frame)
    # Expansion reads every term, generic ones included, so "bad" and "allowed"
    # can still point at the quarantine and role vocabulary; the dedupe below
    # keeps the generic words themselves out of the expansion set.
    phrase, loose = expand_terms_tiered(text, tuple(raw))
    phrase = tuple(t for t in phrase if t not in generic and t not in frame)
    loose = tuple(t for t in loose if t not in generic and t not in frame)
    # A question made only of discourse words ("what does it do") still has to
    # retrieve something. Prefer a framed phrase expansion ("Gate 8" → g8)
    # over falling back to the raw frame words that would retrieve the listing.
    terms = kept or phrase or tuple(raw)
    # "reverse etl" is how an operator writes the label the corpus spells
    # ``reverse_etl``. A shingle that names nothing simply has no document
    # frequency, so this costs nothing when it does not apply.
    shingles = tuple(s for s in identifier_shingles(terms) if s not in phrase)
    phrase = phrase + shingles
    return QueryAnalysis(
        text=text,
        ask=classify_ask(text),
        terms=terms,
        expansions=phrase + loose,
        generic_terms=dropped,
        phrase_expansions=phrase,
    )
