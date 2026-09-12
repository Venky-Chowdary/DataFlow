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
            r"define|meaning\s+of|tell\s+me\s+about|explain(?:\s+what)?)\b"
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
            r"|\b(?:fail|failed|failing|failure|error|errors|broke|broken|stuck|"
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
            r"|\ball\s+(?:the\s+)?(?:modes?|gates?|roles?|options?|types?)\b",
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
            r"|\bdo\s+you\s+support\b|\bis\s+it\s+possible\b|\bsupported\b",
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
    "hourly": ("schedule", "pipeline", "cron", "recurr"),
    "daily": ("schedule", "pipeline", "cron", "recurr"),
    "weekly": ("schedule", "pipeline", "cron", "recurr"),
    "cron": ("schedule", "pipeline", "recurr", "cadence"),
    "cadence": ("schedule", "pipeline", "cron", "interval"),
    "recurring": ("schedule", "pipeline", "recurr", "cron"),
    "automate": ("schedule", "pipeline", "recurr"),
    "automated": ("schedule", "pipeline", "recurr"),
    "unattended": ("schedule", "pipeline", "authorization"),
    "pause": ("schedule", "pipeline", "enabled", "paused"),
    "resume": ("schedule", "pipeline", "enabled", "resume"),
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
    "timezone": ("timestamp", "zone", "instant", "offset", "temporal", "utc"),
    "tz": ("timestamp", "zone", "instant", "offset", "utc"),
    "utc": ("timestamp", "zone", "instant", "offset", "utc"),
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

# Multi-word operator phrases that only mean something together. Matched on the
# normalized question before single-term expansion.
_PHRASE_EXPANSIONS: tuple[tuple[re.Pattern[str], tuple[str, ...]], ...] = (
    (re.compile(r"\bbad\s+(?:row|record|data)s?\b", re.I),
     ("quarantine", "reject", "invalid", "dlq")),
    (re.compile(r"\b(?:row|record)s?\s+(?:ledger|accounting|balance)\b", re.I),
     ("reconciliation", "checksum", "conservation", "ledger")),
    (re.compile(r"\bdata\s+loss|\blos(?:e|ing|t)\s+(?:row|record|data)s?\b", re.I),
     ("quarantine", "reconciliation", "checksum", "unaccounted")),
    (re.compile(r"\bchange\s+data\s+capture\b", re.I),
     ("cdc", "sync", "mode", "log", "change")),
    (re.compile(r"\bdead\s+letter\b", re.I), ("quarantine", "dlq", "reject")),
    (re.compile(r"\bprimary\s+key\b", re.I), ("key", "upsert", "identity", "deduped")),
    # The other sense of "key". With only the identity sense above, "can I use
    # my own encryption key" landed on whichever sync-mode sentence says the
    # word most often — upsert's "key-idempotently: new keys insert, known keys
    # update" — while the BYOK section it was asking about ranked below.
    (re.compile(r"\b(?:encryption|kms|customer[\s-]managed|private|secret)\s+keys?\b"
                r"|\bkey\s+management\b"
                # What the acronym stands for, which is how it gets asked.
                r"|\bbring\s+(?:my|your|our)\s+own\b", re.I),
     ("byok", "kms", "encryption", "secret")),
    (re.compile(r"\bslowly\s+changing\s+dimension\b", re.I),
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
)


def frame_words(question: str) -> frozenset[str]:
    """Words this question uses to frame its subject rather than to name it."""
    out: set[str] = set()
    for pattern, words in _FRAME_PHRASES:
        if pattern.search(question or ""):
            out.update(normalize(w) for w in words)
    return frozenset(out)


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
    for pattern, targets in _PHRASE_EXPANSIONS:
        if not pattern.search(question or ""):
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
    out: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for pattern, targets in _PHRASE_EXPANSIONS:
        consumed: list[str] = []
        for match in pattern.finditer(text):
            for term in content_terms(match.group(0)):
                if term not in consumed:
                    consumed.append(term)
        if consumed:
            out.append((tuple(consumed), tuple(normalize(t) for t in targets)))
    return tuple(out)


def analyze_query(question: str) -> QueryAnalysis:
    """Understand one operator question before anything tries to retrieve for it."""
    text = (question or "").strip()
    raw = content_terms(text)
    generic = _generic_and_stop()
    # Frame words join the generic ones for this question only. They land in
    # ``generic_terms`` so the evidence policy does not demand a passage cover
    # a word the question was not really about.
    frame = frame_words(text)
    kept = tuple(t for t in raw if t not in generic and t not in frame)
    dropped = tuple(t for t in raw if t in generic or t in frame)
    # A question made only of discourse words ("what does it do") still has to
    # retrieve something, so fall back to the raw terms rather than nothing.
    terms = kept or tuple(raw)
    # Expansion reads every term, generic ones included, so "bad" and "allowed"
    # can still point at the quarantine and role vocabulary; the dedupe below
    # keeps the generic words themselves out of the expansion set.
    phrase, loose = expand_terms_tiered(text, tuple(raw))
    phrase = tuple(t for t in phrase if t not in generic)
    loose = tuple(t for t in loose if t not in generic)
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
