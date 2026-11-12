"""Account for every word of a counting question about jobs (review 2, C2c).

Round 1 refused the qualifiers it had a list for; round 2 found the everyday
ones it had no list for ("orders jobs", "nightly jobs", "failed twice", "with
a duplicate key error") dropped, and the whole-history count stated. Lists of
unsupported words cannot keep up with how operators talk, so the rule is
inverted: after the status, window, verification and subject words are read,
every remaining word must be one of

* an allowed word that filters nothing ("how many", "my", "show", "did"), or
* a filter the job store applies exactly, the same one the Jobs page applies:
  a source or destination name (the Jobs page search), a connector type
  (``source_type`` / ``destination_type``) or a sync mode (``sync_mode``).

Anything else is returned as *unhandled*, and the question is refused by name,
quoting those words. Nothing here guesses what an unknown word means.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

#: Connector types as the ledger records them (``source_type`` / ``destination_type``).
_TYPES = {
    "postgresql": "postgresql", "postgres": "postgresql", "pg": "postgresql",
    "mysql": "mysql", "snowflake": "snowflake", "bigquery": "bigquery", "big query": "bigquery",
    "redshift": "redshift", "oracle": "oracle", "sqlserver": "sqlserver", "sql server": "sqlserver",
    "mssql": "sqlserver", "sqlite": "sqlite", "mongodb": "mongodb", "mongo": "mongodb",
    "dynamodb": "dynamodb", "dynamo": "dynamodb", "elasticsearch": "elasticsearch",
    "elastic": "elasticsearch", "kafka": "kafka", "s3": "s3", "gcs": "gcs", "adls": "adls",
    "csv": "csv", "parquet": "parquet", "excel": "excel", "xlsx": "excel", "databricks": "databricks",
    "synapse": "synapse", "salesforce": "salesforce", "hubspot": "hubspot", "stripe": "stripe",
    "duckdb": "duckdb", "redis": "redis", "rest api": "rest_api", "iceberg": "iceberg",
}
#: Sync modes named exactly. "incremental", "full refresh", "append" and the
#: like each stand for more than one stored mode: they are not applied.
_SYNC_MODES = {
    "cdc": "cdc", "change data capture": "cdc", "scd2": "scd2", "scd type 2": "scd2", "mirror": "mirror",
    "full_refresh_overwrite": "full_refresh_overwrite", "full_refresh_append": "full_refresh_append",
    "incremental_append": "incremental_append", "incremental_deduped": "incremental_deduped",
}
_TYPE_RE = re.compile(
    r"\b(?:(?P<prep>from|to|into|on|in|for)\s+)?(?:the\s+|my\s+|a\s+)?(?P<type>"
    + "|".join(re.escape(k).replace(r"\ ", r"\s+") for k in sorted(_TYPES, key=len, reverse=True))
    + r")\b(?:\s+(?:source|destination|database|db|warehouse|connector|connection|target)s?\b)?",
    re.I,
)
_SYNC_RE = re.compile(
    r"\b(?:(?:in|with|using|via|on)\s+)?(?:the\s+)?(?P<mode>"
    + "|".join(re.escape(k).replace(r"\ ", r"\s+") for k in sorted(_SYNC_MODES, key=len, reverse=True))
    + r")\b(?:\s+(?:sync\s+)?mode\b)?",
    re.I,
)
_QUOTED = re.compile(r"[“\"']([^”\"']{1,120})[”\"']")
#: A saved name in title case after a preposition ("on Local Postgres"): a
#: connector type inside it is part of the name, not a type filter.
_TITLED_NAME = re.compile(
    r"\b(?:for|from|to|into|on|in|via|using)\s+(?:the\s+)?((?:[A-Z][\w-]*)(?:\s+[A-Z][\w-]*){1,3})\b"
)
#: Clauses that state a cause or a size: never a filter the ledger has.
_CLAUSES = re.compile(
    r"\b(?:due\s+to|because\s+of|caused\s+by|owing\s+to|thanks\s+to)\s+[^?.,;!]+"
    r"|\bwith\s+(?:an?\s+|the\s+)?(?:[\w-]+\s+){0,4}?errors?\b"
    r"|\b(?:with|having|over|under|above|below|more\s+than|less\s+than|fewer\s+than|at\s+least|at\s+most)"
    r"\s+(?:more\s+than\s+|less\s+than\s+|over\s+|under\s+)?[\d,.]+[kmb]?\s+(?:rows?|records?)\b"
    r"|\bmore\s+than\s+(?:once|twice|\w+\s+times?)\b|\b(?:once|twice|thrice|\w+\s+times)\s+or\s+more\b",
    re.I,
)
_EXACT_STATUS = re.compile(r"\bcomplete(?:d)?[\s_]+with[\s_]+(?:errors?|warnings?|quarantine)\b", re.I)


def without_clauses(text: str) -> str:
    """``text`` with cause and size clauses removed ("due to connection resets").

    Their nouns ("connection", "rows") are not the subject of the question.
    """
    return _CLAUSES.sub(" ", _EXACT_STATUS.sub(" completed ", text or ""))


#: Status, metric and subject words: read by the status / metric parsers.
_READ_WORDS = re.compile(
    r"\b(?:need(?:s|ing)?|requir(?:e|es|ing)|want(?:s|ing)?)\s+(?:my\s+|your\s+|our\s+|any\s+)?attention\b"
    r"|\bcomplete(?:d)?[\s_]+with[\s_]+(?:errors?|warnings?|quarantine)\b|\bin\s+progress\b"
    r"|\b(?:success|failure|pass|completion)\s+rate\b|\bhow\s+long\b|\bhow\s+much\s+time\b"
    r"|\b(?:elapsed|run)\s*time\b"
    r"|\b(?:fail|fails|failed|failing|failure|failures|errors?|errored|broken|completed?|succeeded|successful|"
    r"finished|done|running|cancell?ed|canceled|queued|pending|partial(?:ly)?|paused|retrying|quarantined|"
    r"duration|percent(?:age)?|share|fraction|proportion|moved?|transferr?(?:ed)?|load(?:ed)?|synced|"
    r"process(?:ed)?|migrat(?:e|ed)|cop(?:y|ied)|rows|records)\b",
    re.I,
)
_SUBJECT = r"(?:jobs?|runs?|transfers?|syncs?)"
_SUBJECT_RE = re.compile(rf"\b{_SUBJECT}\b", re.I)
#: Words that filter nothing. Kept small on purpose: a word not here is refused.
_ALLOWED = frozenset("""
how many much number count total totals overall altogether of the a an my our your me us i we you
show list give tell display see view get find what which are is was were be been being have has had
did do does any there all please can could would will just only ones one those these them they it its
that this so far now yet ever still also then and or but with in at on for from to into via using
hey hi ok okay pilot datawrap workspace status state statuses current currently happened
right moment present most recent latest newest last up
ran started created executed launched happened occurred kicked off take takes took taking
""".split())
#: Prepositions after which a remaining word is read as a source or destination name.
_NAME_PREPS = frozenset({"for", "from", "to", "into", "on", "in", "via", "using"})
#: Words that qualify a job but name no column the ledger filters by.
_NEVER_NAMES = frozenset("""
nightly daily weekly hourly monthly yearly scheduled manual manually automatic automated recurring
retried retries retry again repeatedly twice thrice once times big large small huge tiny slow fast
long short quick quickest slowest biggest largest smallest longest shortest new old
incremental full refresh upsert upserts append overwrite snapshot snapshots
mariadb cassandra teradata db2 clickhouse neo4j azure blob
""".split())
_WORD = re.compile(r"[\w][\w.\-/]*", re.U)


@dataclass(frozen=True)
class WordAccount:
    """What the remaining words of a jobs question filter by, and what nothing does."""

    name: str = ""
    source_type: str = ""
    destination_type: str = ""
    sync_mode: str = ""
    #: Spans of the question nothing applies, in order ("nightly", "because of permissions").
    unhandled: tuple[str, ...] = field(default_factory=tuple)

    def filters(self) -> dict[str, str]:
        out = {
            "name": self.name, "source_type": self.source_type,
            "destination_type": self.destination_type, "sync_mode": self.sync_mode,
        }
        return {key: value for key, value in out.items() if value}


def _blank(text: str, span: tuple[int, int]) -> str:
    start, end = span
    return text[:start] + " " * (end - start) + text[end:]


def account_for_words(rest: str) -> WordAccount:
    """Read ``rest`` (a jobs question with its window and verification removed)."""
    # A possessive is not a word of its own: "today's" once searched for "s".
    text = re.sub(r"['’]s\b", "  ", rest or "")
    unhandled: list[tuple[int, str]] = []
    names: list[tuple[int, str]] = []
    found: dict[str, str] = {}

    # An exact status ("completed with errors") is not a cause clause.
    for match in list(_EXACT_STATUS.finditer(text)):
        text = _blank(text, match.span())
    for match in list(_CLAUSES.finditer(text)):
        unhandled.append((match.start(), match.group(0).strip()))
        text = _blank(text, match.span())
    for match in list(_QUOTED.finditer(text)) + list(_TITLED_NAME.finditer(text)):
        if not text[match.start(1):match.end(1)].strip():
            continue  # already taken by a quoted name
        names.append((match.start(1), match.group(1).strip()))
        text = _blank(text, match.span(1))
    for match in list(_TYPE_RE.finditer(text)):
        prep = (match.group("prep") or "").lower()
        key = "destination_type" if prep in ("to", "into") else "source_type"
        value = _TYPES[re.sub(r"\s+", " ", match.group("type").lower())]
        if found.get(key, value) != value:
            unhandled.append((match.start(), match.group(0).strip()))
        found[key] = value
        text = _blank(text, match.span())
    for match in list(_SYNC_RE.finditer(text)):
        value = _SYNC_MODES[re.sub(r"\s+", " ", match.group("mode").lower())]
        if found.get("sync_mode", value) != value:
            unhandled.append((match.start(), match.group(0).strip()))
        found["sync_mode"] = value
        text = _blank(text, match.span())
    for match in list(_READ_WORDS.finditer(text)):
        text = _blank(text, match.span())

    tokens = [(m.start(), m.group(0)) for m in _WORD.finditer(text)]
    runs: list[list[tuple[int, str]]] = []
    for index, (start, word) in enumerate(tokens):
        low = word.lower()
        if low in _ALLOWED or _SUBJECT_RE.fullmatch(word):
            continue
        if runs and runs[-1][-1][2] == index - 1:
            runs[-1].append((start, word, index))
        else:
            runs.append([(start, word, index)])
    for run in runs:
        words = [word for _, word, _ in run]
        start, first_index = run[0][0], run[0][2]
        span = rest[start: run[-1][0] + len(run[-1][1])]
        before = _previous(tokens, first_index)
        after = tokens[run[-1][2] + 1][1] if run[-1][2] + 1 < len(tokens) else ""
        nameable = before in _NAME_PREPS or bool(_SUBJECT_RE.fullmatch(after))
        if nameable and not any(
            w.lower() in _NEVER_NAMES or len(w) < 2 or re.fullmatch(r"[\d.\-/:]+", w) for w in words
        ):
            names.append((start, span))
        else:
            unhandled.append((start, span))
    if len(names) > 1:
        # The Jobs page search takes one name; two would be one search or the other.
        unhandled.extend(names)
        names = []
    return WordAccount(
        name=names[0][1] if names else "",
        source_type=found.get("source_type", ""),
        destination_type=found.get("destination_type", ""),
        sync_mode=found.get("sync_mode", ""),
        unhandled=tuple(span for _, span in sorted(unhandled)),
    )


def _previous(tokens: list[tuple[int, str]], index: int) -> str:
    """The word before ``index``, skipping articles and possessives."""
    i = index - 1
    while i >= 0 and tokens[i][1].lower() in ("the", "my", "our", "a", "an", "your"):
        i -= 1
    return tokens[i][1].lower() if i >= 0 else ""


def narrowing_words(filters: dict[str, str]) -> str:
    """Words that read back as ``filters`` ("“orders” from mysql cdc"), for a rewritten follow-up."""
    parts = []
    if filters.get("name"):
        parts.append(f"“{filters['name']}”")
    if filters.get("source_type"):
        parts.append(f"from {filters['source_type']}")
    if filters.get("destination_type"):
        parts.append(f"to {filters['destination_type']}")
    if filters.get("sync_mode"):
        parts.append(filters["sync_mode"])
    return " ".join(parts)


def describe_narrowing(filters: dict[str, str]) -> str:
    """How an answer names the filters it applied: " matching “orders”"."""
    from .md_safe import md_text

    parts = []
    if filters.get("search"):
        parts.append(f"matching “{md_text(filters['search'])}”")
    if filters.get("source_type"):
        parts.append(f"with source type {filters['source_type']}")
    if filters.get("destination_type"):
        parts.append(f"with destination type {filters['destination_type']}")
    if filters.get("sync_mode"):
        parts.append(f"with sync mode {filters['sync_mode']}")
    return (" " + " and ".join(parts)) if parts else ""
