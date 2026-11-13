"""Column-name matching for Map auto-map: exact → normalized → learned → synonym.

The semantic mapper (``services.mapping_pipeline``) proposes the first mapping
from types, samples and an optional LLM. Auto-map on the Map step answers a
narrower question the operator asks *after* that, against a destination table
that already exists: "for the columns still open, which destination column is
this, and how sure are you?" — and it must be explainable in one word per row.

Tiers, strongest first (the confidence is the tier's, never a model score):

``exact``            the same spelling.
``case_insensitive`` the same letters in another case (``OrderID``/``orderid``).
``normalized``       the same words once snake/camel/kebab/space spellings are
                     folded (``firstName`` ≡ ``first_name`` ≡ ``First Name``).
``learned``          this workspace confirmed the pair before
                     (:mod:`services.mapping_memory`); stronger with each
                     confirmation, never above ``normalized``.
``synonym``          both names belong to one group of the built-in synonym
                     dictionary (``src.ai.knowledge.synonyms``) after
                     abbreviation expansion (``given_name`` ≡ ``first_name``,
                     ``surname`` ≡ ``last_name``, ``total`` ≡ ``amount``).
``concept``          the same head concept once abbreviations expand
                     (``customer_email`` ≈ ``email_address``, ``order_amt`` ≈
                     ``total_amount``).
``key_suffix``       ``<entity>_id`` against a bare ``id`` (``order_id`` ≡
                     ``id``): pre-selected only when ``<entity>`` names the
                     destination table, else a hint (it may be a foreign key).
``token``            shared words only — a hint the preview leaves unchecked.

Assignment is one-to-one and global: the highest-scoring pair claims both its
source and its target first, so one strong match is never stolen by a weaker
one that happened to be listed earlier.

False friends. The dictionary groups qualified variants under one canonical
(``billing_address`` and ``shipping_address`` are both "address";
``created_at`` and ``updated_at`` both "timestamp"). Two names of one group
are a match only when at most one of them carries a qualifier — a word that
is neither the concept, a one-word synonym of it, nor a generic kind word
(``address``, ``at``, ``code``…). ``order_status`` ≈ ``status``, but
``order_status`` ≠ ``payment_status`` and ``start_date`` ≠ ``end_date``.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, Iterable, Mapping

EXACT = "exact"
CASE_INSENSITIVE = "case_insensitive"
NORMALIZED = "normalized"
LEARNED = "learned"
SYNONYM = "synonym"
CONCEPT = "concept"
KEY_SUFFIX = "key_suffix"
TOKEN = "token"

TIER_CONFIDENCE: dict[str, float] = {
    EXACT: 1.0,
    CASE_INSENSITIVE: 0.98,
    NORMALIZED: 0.95,
    LEARNED: 0.9,
    SYNONYM: 0.86,
    CONCEPT: 0.78,
    KEY_SUFFIX: 0.55,
    TOKEN: 0.6,
}
_TIER_RANK = {kind: i for i, kind in enumerate(TIER_CONFIDENCE)}

#: Below this the preview lists a suggestion but leaves it unselected.
DEFAULT_SELECT_THRESHOLD = 0.7

#: Words that name a value's kind, not which entity it belongs to.
_KIND_WORDS = frozenset({
    "address", "addr", "id", "no", "number", "num", "code", "name", "value", "val",
    "at", "on", "ts", "flag", "ind", "text", "desc", "field", "col", "column", "dt",
})
#: Key concepts never match by head word: customer_id is not order_id.
_NO_CONCEPT = frozenset({"id", "identifier"})

#: Qualifiers that name a *different* value of the same concept: prev_status is
#: not the status, max_qty is not the quantity, even against the plain name.
_CONTRAST = frozenset({
    "prev", "previous", "old", "new", "min", "minimum", "max", "maximum", "sub",
    "start", "end", "begin", "from", "to", "first", "last", "next", "orig", "original",
    "initial", "final", "before", "after", "gross", "net", "open", "close", "low", "high",
    "avg", "average", "median",
})

#: Dictionary groups whose members really are one value spelled differently.
#: Every other group is a *family* of distinct attributes (money: revenue, fee,
#: subtotal; status: state, stage; flag: is_active, is_deleted; dimensions:
#: width, height): there a member matches the family's own name, never a sibling.
_SYNONYM_GROUPS = frozenset({
    "customer_id", "client_id", "user_id", "member_id", "order_id", "product_id",
    "employee_id", "vendor_id", "shipment_id", "patient_id", "policy_id", "account_number",
    "name", "full_name", "first_name", "last_name", "middle_name", "date_of_birth", "gender",
    "ssn", "email", "email_address", "phone", "phone_number", "mobile_number", "address",
    "city", "city_name", "postal_code", "country", "country_code", "state_code",
    "province_code", "region_code", "payment_amount", "unit_price", "unit_cost",
    "line_amount", "net_amount", "gross_amount", "salary_amount", "commission_amount",
    "bonus_amount", "order_number", "invoice_number", "transaction_id", "payment_date",
    "order_date", "ship_date", "delivery_date", "currency", "credit_card", "cvv",
    "latitude", "longitude", "year", "month", "quarter", "serial_number", "work_order",
    "student_id", "course_id", "enrollment_date", "bedrooms", "bathrooms", "square_feet",
    "age", "url", "ip_address",
})

#: Abbreviation tokens that are also ordinary words or ambiguous on their own.
_NO_EXPAND = frozenset({"id", "no", "item", "val", "cat", "co", "cls", "min", "max"})

_CAMEL_1 = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")
_DIGITS = re.compile(r"(?<=[A-Za-z])(?=[0-9])|(?<=[0-9])(?=[A-Za-z])")
_SPLIT = re.compile(r"[^0-9A-Za-z]+")


def name_tokens(name: str) -> tuple[str, ...]:
    """Words of a column name, lower-cased: ``OrderID`` → ``("order", "id")``."""
    text = _CAMEL_2.sub(r"\1_\2", _CAMEL_1.sub(r"\1_\2", str(name or "").strip()))
    text = _DIGITS.sub("_", text)
    return tuple(t.lower() for t in _SPLIT.split(text) if t)


def normalize_name(name: str) -> str:
    """Snake-case form used for comparison: ``First Name`` → ``first_name``."""
    return "_".join(name_tokens(name))


@lru_cache(maxsize=1)
def _dictionaries() -> tuple[dict[str, frozenset[str]], dict[str, str], dict[str, tuple[frozenset[str], frozenset[str]]]]:
    """(name → groups, abbreviation → word, group → (canonical words, one-word synonyms))."""
    try:
        from src.ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    except ImportError:  # pragma: no cover - alternate import root
        from ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    index: dict[str, set[str]] = {}
    words: dict[str, tuple[frozenset[str], frozenset[str]]] = {}
    for canonical, synonyms in SYNONYM_DICTIONARY.items():
        canon = normalize_name(canonical)
        singles = {normalize_name(w) for w in (canonical, *synonyms) if "_" not in normalize_name(w)}
        prior = words.get(canon, (frozenset(), frozenset()))
        words[canon] = (frozenset(canon.split("_")) | prior[0], frozenset(singles) | prior[1])
        for word in (canonical, *synonyms):
            index.setdefault(normalize_name(word), set()).add(canon)
    abbreviations = {
        str(k).lower(): normalize_name(v)
        for k, v in TOKEN_ABBREVIATIONS.items()
        if str(k).lower() not in _NO_EXPAND
    }
    return {k: frozenset(v) for k, v in index.items()}, abbreviations, words


def _expanded(norm: str) -> str:
    _, abbreviations, _ = _dictionaries()
    return "_".join(abbreviations.get(t, t) for t in norm.split("_") if t)


@lru_cache(maxsize=65536)
def synonym_groups(name: str) -> frozenset[str]:
    """Canonical synonym groups a name belongs to (empty when none)."""
    index, _, _ = _dictionaries()
    norm = normalize_name(name)
    groups = set(index.get(norm, frozenset()))
    groups |= index.get(_expanded(norm), frozenset())
    compact = norm.replace("_", "")
    groups |= index.get(compact, frozenset())
    return frozenset(groups)


def _qualifiers(name: str, group: str) -> frozenset[str]:
    """Words of ``name`` that say *which* ``group`` value it is (billing, created…)."""
    _, _, words = _dictionaries()
    canon, singles = words.get(group, (frozenset(), frozenset()))
    norm = normalize_name(name)
    if norm == group or norm in singles:
        # subtotal is a one-word member that still says "a part of the total".
        return frozenset({"sub"}) if norm.startswith("sub") and len(norm) > 5 else frozenset()
    found = set()
    for t in norm.split("_"):
        if not t or t in canon or t in singles or _expanded(t) in canon or t in _KIND_WORDS:
            continue
        found.add(t)
    return frozenset(found)


def _canonical_side(name: str, group: str) -> bool:
    """The name *is* the family's own concept (amount, total_amount, status)."""
    norm = normalize_name(name)
    head = norm.split("_")[-1] if norm else ""
    return norm == group or head == group or _expanded(head) == group


def _is_other_canonical(name: str, group: str) -> bool:
    """A family member that is itself another concept (state is a place, not a status)."""
    _, _, words = _dictionaries()
    norm = normalize_name(name)
    return norm != group and norm in words


def _same_value(group: str, a: str, b: str) -> bool:
    """Both names denote the same value of ``group`` (not two qualified siblings)."""
    qa, qb = _qualifiers(a, group), _qualifiers(b, group)
    if qa and qb and qa != qb:
        return False
    if (qa ^ qb) & _CONTRAST:
        return False
    if group not in _SYNONYM_GROUPS:
        if not (_canonical_side(a, group) or _canonical_side(b, group)):
            return False
        if _is_other_canonical(a, group) or _is_other_canonical(b, group):
            return False
    return True


@lru_cache(maxsize=65536)
def concept_groups(name: str) -> frozenset[str]:
    """Groups of the whole name plus of its head (last) word, keys excluded."""
    norm = normalize_name(name)
    head = norm.split("_")[-1] if norm else ""
    found = set(synonym_groups(name))
    if head:
        found |= synonym_groups(head) | synonym_groups(_expanded(head))
    return frozenset(found - _NO_CONCEPT)


def _singular(word: str) -> str:
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith("ses") and len(word) > 4:
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 3:
        return word[:-1]
    return word


def _table_entity(table: str) -> str:
    leaf = str(table or "").split(".")[-1]
    tokens = name_tokens(leaf)
    return _singular(tokens[-1]) if tokens else ""


def _key_suffix_match(src: str, tgt: str, entities: set[str]) -> float | None:
    """``order_id`` ↔ ``id``: an entity-qualified key against a bare key."""
    s, t = normalize_name(src), normalize_name(tgt)
    for qualified, bare in ((s, t), (t, s)):
        if bare != "id" or not qualified.endswith("_id"):
            continue
        entity = _singular(qualified[: -len("_id")].split("_")[-1])
        if entity and entity in entities:
            return 0.82
        return TIER_CONFIDENCE[KEY_SUFFIX]
    return None


def score_pair(
    source: str,
    target: str,
    *,
    learned: Mapping[str, Mapping[str, int]] | None = None,
    entities: Iterable[str] = (),
) -> tuple[float, str, str] | None:
    """``(confidence, kind, reason)`` for one source/target pair, or ``None``."""
    if not source or not target:
        return None
    if source == target:
        return 1.0, EXACT, "Same name."
    if source.lower() == target.lower():
        return TIER_CONFIDENCE[CASE_INSENSITIVE], CASE_INSENSITIVE, "Same name, different letter case."
    s_norm, t_norm = normalize_name(source), normalize_name(target)
    if s_norm and s_norm == t_norm:
        return TIER_CONFIDENCE[NORMALIZED], NORMALIZED, (
            f"Same words once spelling is normalized ({s_norm})."
        )
    best: tuple[float, str, str] | None = None
    count = int(((learned or {}).get(s_norm) or {}).get(t_norm) or 0)
    if count > 0:
        conf = min(TIER_CONFIDENCE[NORMALIZED] - 0.01, TIER_CONFIDENCE[LEARNED] + 0.01 * (count - 1))
        best = (round(conf, 3), LEARNED, (
            f"This workspace confirmed {source} → {target} "
            f"{count} time{'s' if count != 1 else ''} before."
        ))
    shared = sorted(
        g for g in synonym_groups(source) & synonym_groups(target) if _same_value(g, source, target)
    )
    if shared and (best is None or best[0] < TIER_CONFIDENCE[SYNONYM]):
        best = (TIER_CONFIDENCE[SYNONYM], SYNONYM, f"Synonyms in the '{shared[0]}' group.")
    if best is not None:
        return best
    concepts = sorted(
        g for g in concept_groups(source) & concept_groups(target) if _same_value(g, source, target)
    )
    if concepts:
        return TIER_CONFIDENCE[CONCEPT], CONCEPT, (
            f"Both name the same concept ({concepts[0]})."
        )
    key = _key_suffix_match(source, target, set(entities))
    if key is not None:
        return key, KEY_SUFFIX, "An entity-qualified key against the table's bare id."
    s_tokens = {_expanded(t) for t in s_norm.split("_") if t}
    t_tokens = {_expanded(t) for t in t_norm.split("_") if t}
    overlap = s_tokens & t_tokens - {"id"}
    if overlap:
        jaccard = len(overlap) / len(s_tokens | t_tokens)
        if jaccard >= 0.5:
            return round(TIER_CONFIDENCE[TOKEN] * jaccard + 0.2, 3), TOKEN, (
                f"Shares the word{'s' if len(overlap) > 1 else ''} {', '.join(sorted(overlap))}."
            )
    return None


class _TargetIndex:
    """Lookups that bound auto-map to the targets a source could match.

    Scoring every source against every target is quadratic: two 2,000-column
    tables are four million pairs. Each tier is an equality on some key of the
    name, so candidates come from dictionaries and only those are scored.
    """

    #: A word (or head concept) shared by more targets than this is too common
    #: to suggest from: it no longer tells one column from another.
    TOKEN_FANOUT = 40

    def __init__(self, targets: list[str]) -> None:
        self.by_lower: dict[str, list[str]] = {}
        self.by_norm: dict[str, list[str]] = {}
        self.by_group: dict[str, list[str]] = {}
        self.by_concept: dict[str, list[str]] = {}
        self.by_token: dict[str, list[str]] = {}
        self.keys: list[str] = []
        for tgt in targets:
            norm = normalize_name(tgt)
            self.by_lower.setdefault(tgt.lower(), []).append(tgt)
            self.by_norm.setdefault(norm, []).append(tgt)
            for group in synonym_groups(tgt):
                self.by_group.setdefault(group, []).append(tgt)
            for group in concept_groups(tgt):
                self.by_concept.setdefault(group, []).append(tgt)
            for token in {_expanded(t) for t in norm.split("_") if t} - {"id"}:
                self.by_token.setdefault(token, []).append(tgt)
            if norm == "id" or norm.endswith("_id"):
                self.keys.append(tgt)

    def candidates(self, src: str, learned_targets: Iterable[str]) -> list[str]:
        norm = normalize_name(src)
        found: list[str] = []
        found += self.by_lower.get(src.lower(), [])
        found += self.by_norm.get(norm, [])
        for t_norm in learned_targets:
            found += self.by_norm.get(t_norm, [])
        for group in synonym_groups(src):
            found += self.by_group.get(group, [])
        for group in concept_groups(src):
            bucket = self.by_concept.get(group, [])
            if len(bucket) <= self.TOKEN_FANOUT:
                found += bucket
        if norm == "id" or norm.endswith("_id"):
            found += self.keys
        for token in {_expanded(t) for t in norm.split("_") if t} - {"id"}:
            bucket = self.by_token.get(token, [])
            if len(bucket) <= self.TOKEN_FANOUT:
                found += bucket
        return list(dict.fromkeys(found))


def suggest_assignments(
    sources: list[str],
    targets: list[str],
    *,
    locked_sources: Iterable[str] = (),
    locked_targets: Iterable[str] = (),
    learned: Mapping[str, Mapping[str, int]] | None = None,
    source_table: str = "",
    dest_table: str = "",
    alternatives: int = 3,
) -> list[dict[str, Any]]:
    """One-to-one suggestions for the open sources, best pairs first.

    ``locked_*`` are sources already mapped and targets already taken: they are
    neither re-suggested nor claimed, so "map remaining" never steals a target
    the operator chose.
    """
    # A key matches the bare id only when its entity names the DESTINATION
    # table: customer_id → orders.id is a foreign key, not the order's key.
    entities = {e for e in (_table_entity(dest_table),) if e}
    lock_s = {str(s) for s in locked_sources}
    lock_t = {str(t).lower() for t in locked_targets}
    open_sources = [s for s in dict.fromkeys(sources) if s and s not in lock_s]
    open_targets = [t for t in dict.fromkeys(targets) if t and t.lower() not in lock_t]
    order = {t: i for i, t in enumerate(open_targets)}
    index = _TargetIndex(open_targets)
    learned = learned or {}
    candidates: list[tuple[float, int, int, int, str, str, str, str]] = []
    per_source: dict[str, list[tuple[float, int, str, str]]] = {}
    for si, src in enumerate(open_sources):
        learned_targets = (learned.get(normalize_name(src)) or {}).keys()
        for tgt in index.candidates(src, learned_targets):
            hit = score_pair(src, tgt, learned=learned, entities=entities)
            if hit is None:
                continue
            conf, kind, reason = hit
            candidates.append((-conf, _TIER_RANK[kind], si, order[tgt], src, tgt, kind, reason))
            per_source.setdefault(src, []).append((-conf, _TIER_RANK[kind], tgt, kind))
    candidates.sort()
    claimed_s: set[str] = set()
    claimed_t: set[str] = set()
    chosen: dict[str, dict[str, Any]] = {}
    for neg_conf, _rank, _si, _ti, src, tgt, kind, reason in candidates:
        if src in claimed_s or tgt.lower() in claimed_t:
            continue
        claimed_s.add(src)
        claimed_t.add(tgt.lower())
        chosen[src] = {
            "source": src,
            "target": tgt,
            "confidence": round(-neg_conf, 3),
            "kind": kind,
            "reason": reason,
            "selected": -neg_conf >= DEFAULT_SELECT_THRESHOLD,
        }
    out: list[dict[str, Any]] = []
    for src in open_sources:
        if src not in chosen:
            continue
        row = chosen[src]
        ranked = sorted(per_source.get(src, []))
        row["alternatives"] = [
            {"target": tgt, "confidence": round(-c, 3), "kind": kind}
            for c, _r, tgt, kind in ranked
            if tgt != row["target"]
        ][: max(0, alternatives)]
        out.append(row)
    out.sort(key=lambda r: (-r["confidence"], _TIER_RANK[r["kind"]]))
    return out
