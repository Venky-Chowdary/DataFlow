"""Query understanding — the stage that was missing between an operator's words and retrieval.

Retrieval ran on the operator's raw tokens, so a question only matched when they
happened to type the documentation's vocabulary. "What happens to bad rows"
retrieved nothing because the corpus says *rejected* and *quarantine*; "how do I
see why a job was slow" retrieved nothing because nothing in it is a corpus term
except ``job``. Both are ordinary operator questions about documented behaviour.

This module turns one question into the three things the rest of the pipeline
needs:

``ask``       what kind of answer is wanted (a definition, a procedure, a
              diagnosis, a capability check, a comparison, an enumeration). The
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

from .lexical_index import content_terms, normalize

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
        "comparison",
        re.compile(
            r"\b(?:difference\s+between|vs\.?|versus|compared\s+to|"
            r"which\s+(?:one\s+)?(?:should|is\s+better)|"
            r"better\s+than|instead\s+of)\b",
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
    (
        "enumeration",
        re.compile(
            r"\b(?:list|which|what|explain|describe|show)\s+(?:\w+\s+){0,3}"
            r"(?:modes?|options?|types?|gates?|roles?|connectors?|kinds?|"
            r"policies|policys?|phases?|steps?|permissions?|"
            r"are\s+there|do\s+you\s+support|are\s+available)\b"
            r"|\ball\s+(?:the\s+)?(?:modes?|gates?|roles?|options?|types?)\b",
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
    (re.compile(r"\bget\s+started\b", re.I), ("first", "transfer", "studio", "guide")),
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
    """The documentation's vocabulary for what the operator said.

    Phrase rules run on the raw question so "bad rows" can mean something its
    two words do not; single-term rules then run on the normalized terms.
    """
    out: list[str] = []
    seen = set(terms)
    lookup = _expansion_lookup()
    for pattern, targets in _PHRASE_EXPANSIONS:
        if not pattern.search(question or ""):
            continue
        for target in targets:
            t = normalize(target)
            if t not in seen:
                seen.add(t)
                out.append(t)
    for term in terms:
        for target in lookup.get(term, ()):
            t = normalize(target)
            if t not in seen:
                seen.add(t)
                out.append(t)
    return tuple(out)


def analyze_query(question: str) -> QueryAnalysis:
    """Understand one operator question before anything tries to retrieve for it."""
    text = (question or "").strip()
    raw = content_terms(text)
    generic = _generic_and_stop()
    kept = tuple(t for t in raw if t not in generic)
    dropped = tuple(t for t in raw if t in generic)
    # A question made only of discourse words ("what does it do") still has to
    # retrieve something, so fall back to the raw terms rather than nothing.
    terms = kept or tuple(raw)
    # Expansion reads every term, generic ones included, so "bad" and "allowed"
    # can still point at the quarantine and role vocabulary; the dedupe below
    # keeps the generic words themselves out of the expansion set.
    expansions = tuple(
        t for t in expand_terms(text, tuple(raw)) if t not in generic
    )
    return QueryAnalysis(
        text=text,
        ask=classify_ask(text),
        terms=terms,
        expansions=expansions,
        generic_terms=dropped,
    )
