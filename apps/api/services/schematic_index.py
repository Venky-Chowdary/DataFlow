"""
Schematic index — million-scale column name variant lookup.

Resolves a column name variant (``stg_cust_id``, ``dim_customer_id_2``) to its
canonical semantic form. Used by the semantic mapper for abbreviation / alias
resolution.

The variant space is the synonym dictionary crossed with warehouse prefixes,
suffixes and numeric tails: about 2.5 million names (``_iter_variants`` is its
definition). It used to be materialised as one dict on the first lookup in a
process. That build cost ~15s of CPU (and a ~60MB pickle in the data dir) on
the first Map onto an existing table after every deploy, inside the 45s tool
deadline, so an SFTP CSV into an existing Postgres table timed out while the
same file into a new table (no target columns, no lookup) loaded (D189).

``lookup_schematic`` now decomposes the one name it is asked about into
prefix + term + suffix / numeric tail and checks the small dictionaries. The
answer is the entry the enumeration would have inserted first (the dict was
built with ``setdefault``), so it is the same answer the materialised index
gave, with no build and no cache file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterator

# Enterprise data-warehouse prefixes/suffixes seen in real schemas
_PREFIXES = (
    "src_", "dst_", "tgt_", "dim_", "fact_", "stg_", "raw_", "hist_", "curr_",
    "prev_", "bk_", "lk_", "fk_", "pk_", "ref_", "tmp_", "wrk_", "ods_", "dwh_",
    "mart_", "rpt_", "agg_", "sum_", "avg_", "cnt_", "max_", "min_", "tot_",
    "net_", "gross_", "orig_", "new_", "old_", "cur_", "prv_", "nxt_", "lst_",
    "fst_", "lcl_", "glb_", "ext_", "int_", "sys_", "usr_", "app_", "db_",
    "tbl_", "col_", "fld_", "attr_", "prop_", "val_", "key_", "id_", "num_",
    "cd_", "typ_", "cat_", "sub_", "meta_", "aux_", "sup_", "alt_", "pri_",
    "sec_", "ter_", "lvl_", "tier_", "grp_", "seg_", "cls_", "typ_", "stat_",
    "flg_", "ind_", "sw_", "bit_", "yn_", "is_", "has_", "can_", "was_",
    "billing_", "shipping_", "mailing_", "home_", "work_", "primary_", "secondary_",
    "customer_", "order_", "product_", "vendor_", "employee_", "account_",
    "transaction_", "payment_", "invoice_", "shipment_", "inventory_", "sales_",
    "marketing_", "finance_", "hr_", "legal_", "compliance_", "audit_",
)

_SUFFIXES = (
    "_id", "_key", "_code", "_num", "_no", "_nbr", "_name", "_desc", "_type",
    "_status", "_flag", "_ind", "_dt", "_date", "_ts", "_time", "_amt", "_qty",
    "_cnt", "_val", "_pct", "_rate", "_score", "_rank", "_seq", "_idx",
    "_txt", "_str", "_json", "_blob", "_hash", "_uuid", "_guid", "_ref",
    "_1", "_2", "_3", "_01", "_02", "_03", "_v1", "_v2", "_old", "_new",
)


def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _dictionaries() -> tuple[dict[str, list[str]], dict[str, str]]:
    try:
        from src.ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    except ImportError:
        try:
            from ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
        except ImportError:
            return {}, {}
    return SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS


#: Slices of the prefix / suffix lists each family of variants uses.
_ABBR_PREFIXES = 40
_ABBR_SUFFIXES = 20
_PREFIX_SYNONYMS = 8
_PREFIX_SUFFIX_SYNONYMS = 4
_NUMERIC_CANONICALS = 200
_NUMERIC_TAILS = 500
_PREFIXED_NUMERIC_TAILS = 50
_NUMERIC_PREFIXES = 15
_MAX_VARIANT = 128


def _iter_variants() -> Iterator[tuple[str, str]]:
    """Every ``(variant, canonical)`` pair, in insertion order (first one wins).

    This is the definition of the index. ``lookup_schematic`` answers from it
    without enumerating it; the equivalence is tested over this generator.
    """
    synonyms, abbreviations = _dictionaries()

    def add(variant: str, canonical: str) -> Iterator[tuple[str, str]]:
        v = _normalize(variant)
        if v and len(v) <= _MAX_VARIANT:
            yield v, canonical

    for canonical, syns in synonyms.items():
        yield from add(canonical, canonical)
        for s in syns:
            yield from add(s, canonical)
        for prefix in _PREFIXES:
            yield from add(f"{prefix}{canonical}", canonical)
            for s in syns[:_PREFIX_SYNONYMS]:
                yield from add(f"{prefix}{s}", canonical)
            for suffix in _SUFFIXES:
                yield from add(f"{prefix}{canonical}{suffix}", canonical)
                for s in syns[:_PREFIX_SUFFIX_SYNONYMS]:
                    yield from add(f"{prefix}{s}{suffix}", canonical)
        for suffix in _SUFFIXES:
            yield from add(f"{canonical}{suffix}", canonical)

    for abbr, full in abbreviations.items():
        yield from add(abbr, full)
        yield from add(full, full)
        for prefix in _PREFIXES[:_ABBR_PREFIXES]:
            yield from add(f"{prefix}{abbr}", full)
            yield from add(f"{prefix}{full}", full)
            for suffix in _SUFFIXES[:_ABBR_SUFFIXES]:
                yield from add(f"{prefix}{abbr}{suffix}", full)
                yield from add(f"{prefix}{full}{suffix}", full)

    # Numeric suffix variants for high-volume enterprise schemas
    for canonical in list(synonyms.keys())[:_NUMERIC_CANONICALS]:
        for n in range(_NUMERIC_TAILS):
            yield from add(f"{canonical}_{n}", canonical)
            yield from add(f"{canonical}{n}", canonical)
            yield from add(f"{canonical}_v{n}", canonical)
            if n < _PREFIXED_NUMERIC_TAILS:
                for prefix in _PREFIXES[:_NUMERIC_PREFIXES]:
                    yield from add(f"{prefix}{canonical}_{n}", canonical)


def _build_index_fresh() -> dict[str, str]:
    """The whole index as a dict. Off the request path: counts and tests only."""
    index: dict[str, str] = {}
    for variant, canonical in _iter_variants():
        index.setdefault(variant, canonical)
    return index


@dataclass(frozen=True)
class _Vocabulary:
    """The dictionaries keyed by normalised term, with insertion positions.

    A rank is a tuple that orders entries the way ``_iter_variants`` emits
    them: phase (synonyms 0, abbreviations 1, numeric 2), then the position of
    the dictionary entry, then the position inside its loops.
    """

    canonicals: tuple[str, ...]
    #: normalised canonical -> index of its dictionary entry
    canonical_at: dict[str, int]
    #: normalised synonym -> [(entry index, synonym position)]
    synonym_at: dict[str, tuple[tuple[int, int], ...]]
    #: normalised abbreviation or full form -> [(entry index, 0 abbr | 1 full, full)]
    abbreviation_at: dict[str, tuple[tuple[int, int, str], ...]]
    prefixes: tuple[str, ...]
    suffixes: tuple[str, ...]


@lru_cache(maxsize=1)
def _vocabulary() -> _Vocabulary:
    synonyms, abbreviations = _dictionaries()
    canonicals = tuple(synonyms)
    canonical_at: dict[str, int] = {}
    synonym_at: dict[str, list[tuple[int, int]]] = {}
    for ci, (canonical, syns) in enumerate(synonyms.items()):
        canonical_at.setdefault(_normalize(canonical), ci)
        for j, s in enumerate(syns):
            synonym_at.setdefault(_normalize(s), []).append((ci, j))
    abbreviation_at: dict[str, list[tuple[int, int, str]]] = {}
    for ai, (abbr, full) in enumerate(abbreviations.items()):
        abbreviation_at.setdefault(_normalize(abbr), []).append((ai, 0, full))
        abbreviation_at.setdefault(_normalize(full), []).append((ai, 1, full))
    return _Vocabulary(
        canonicals=canonicals,
        canonical_at=canonical_at,
        synonym_at={k: tuple(v) for k, v in synonym_at.items()},
        abbreviation_at={k: tuple(v) for k, v in abbreviation_at.items()},
        prefixes=_PREFIXES,
        suffixes=_SUFFIXES,
    )


def _numeric_tail(text: str, limit: int) -> int | None:
    """``n`` when ``text`` is ``str(n)`` for some ``0 <= n < limit``."""
    if not text or not text.isdigit() or str(int(text)) != text:
        return None
    n = int(text)
    return n if n < limit else None


def _resolve(v: str) -> str | None:
    """The canonical the first emitted ``(v, canonical)`` pair carries, if any."""
    if not v or len(v) > _MAX_VARIANT:
        return None
    voc = _vocabulary()
    best: tuple[tuple[int, ...], str] | None = None

    def offer(rank: tuple[int, ...], canonical: str) -> None:
        nonlocal best
        if best is None or rank < best[0]:
            best = (rank, canonical)

    def canonical_rank(term: str) -> int | None:
        return voc.canonical_at.get(term)

    # Bare term: canonical, synonym, abbreviation / full form.
    ci = canonical_rank(v)
    if ci is not None:
        offer((0, ci, 0), voc.canonicals[ci])
    for ci, j in voc.synonym_at.get(v, ()):
        offer((0, ci, 1, j), voc.canonicals[ci])
    for ai, kind, full in voc.abbreviation_at.get(v, ()):
        offer((1, ai, kind), full)

    # term + suffix
    for xi, suffix in enumerate(voc.suffixes):
        if len(v) > len(suffix) and v.endswith(suffix):
            ci = canonical_rank(v[: -len(suffix)])
            if ci is not None:
                offer((0, ci, 3, xi), voc.canonicals[ci])

    # prefix + term [+ suffix]
    for pi, prefix in enumerate(voc.prefixes):
        if len(v) <= len(prefix) or not v.startswith(prefix):
            continue
        rest = v[len(prefix):]
        ci = canonical_rank(rest)
        if ci is not None:
            offer((0, ci, 2, pi, 0), voc.canonicals[ci])
        for ci, j in voc.synonym_at.get(rest, ()):
            if j < _PREFIX_SYNONYMS:
                offer((0, ci, 2, pi, 1, j), voc.canonicals[ci])
        if pi < _ABBR_PREFIXES:
            for ai, kind, full in voc.abbreviation_at.get(rest, ()):
                offer((1, ai, 2, pi, kind), full)
        for xi, suffix in enumerate(voc.suffixes):
            if len(rest) <= len(suffix) or not rest.endswith(suffix):
                continue
            term = rest[: -len(suffix)]
            ci = canonical_rank(term)
            if ci is not None:
                offer((0, ci, 2, pi, 2, xi, 0), voc.canonicals[ci])
            for ci, j in voc.synonym_at.get(term, ()):
                if j < _PREFIX_SUFFIX_SYNONYMS:
                    offer((0, ci, 2, pi, 2, xi, 1, j), voc.canonicals[ci])
            if pi < _ABBR_PREFIXES and xi < _ABBR_SUFFIXES:
                for ai, kind, full in voc.abbreviation_at.get(term, ()):
                    offer((1, ai, 2, pi, 2, xi, kind), full)
        # prefix + canonical_n
        head, sep, tail = rest.rpartition("_")
        n = _numeric_tail(tail, _PREFIXED_NUMERIC_TAILS) if sep and pi < _NUMERIC_PREFIXES else None
        if n is not None:
            ci = canonical_rank(head)
            if ci is not None and ci < _NUMERIC_CANONICALS:
                offer((2, ci, n, 3, pi), voc.canonicals[ci])

    # canonical_n, canonical{n}, canonical_v{n}
    for width in range(1, len(str(_NUMERIC_TAILS - 1)) + 1):
        if len(v) <= width:
            break
        n = _numeric_tail(v[-width:], _NUMERIC_TAILS)
        if n is None:
            continue
        head = v[:-width]
        for kind, joiner in ((0, "_"), (1, ""), (2, "_v")):
            if joiner and not head.endswith(joiner):
                continue
            term = head[: len(head) - len(joiner)] if joiner else head
            ci = canonical_rank(term)
            if ci is not None and ci < _NUMERIC_CANONICALS:
                offer((2, ci, n, kind), voc.canonicals[ci])

    return best[1] if best is not None else None


@lru_cache(maxsize=8192)
def _lookup_normalized(v: str) -> str | None:
    return _resolve(v)


def lookup_schematic(name: str) -> str | None:
    """Return canonical form if name is a known schematic variant."""
    return _lookup_normalized(_normalize(name))


@lru_cache(maxsize=1)
def schematic_count() -> int:
    """Distinct variants the index answers for (enumerates them: not for requests)."""
    return len(_build_index_fresh())


# Tokens ignored for entity-qualifier conflict checks.
_ENTITY_STOPWORDS = frozenset({
    "at", "of", "the", "a", "an", "to", "for", "by", "and", "or", "on", "in",
})
# Domain leaves — shared leaf alone does not prove columns are the same field
# when entity prefixes conflict (order vs transaction, created vs updated).
_DOMAIN_LEAVES = frozenset({
    "amount", "id", "name", "date", "code", "status", "type", "number",
    "value", "count", "flag", "key", "timestamp", "balance", "price",
    "quantity", "total", "rate", "pct", "percent", "description", "text",
    "address", "email", "phone", "time", "uuid", "hash", "index", "seq",
    "amt", "num", "nbr", "no", "cd", "dt", "ts",
})
# Identity-kind leaves are not interchangeable. Synonym retrieval lists
# ``key`` / ``pk`` / ``uuid`` under canonical ``id``, which would otherwise
# boost ``customer_id`` onto warehouse ``customer_key`` at 0.97.
IDENTITY_KIND_LEAVES = frozenset({
    "id", "key", "pk", "code", "uuid", "guid", "oid", "sku",
})
# Back-compat alias used by older call sites.
_GENERIC_LEAVES = _DOMAIN_LEAVES | _ENTITY_STOPWORDS


def _token_set(name: str) -> set[str]:
    return {t for t in _normalize(name).split("_") if t}


def _entity_qualifiers(tokens: set[str]) -> set[str]:
    toks = set(tokens)
    if "amt" in toks:
        toks = (toks - {"amt"}) | {"amount"}
    return toks - _DOMAIN_LEAVES - _ENTITY_STOPWORDS


def _qualifier_stems_overlap(a: set[str], b: set[str]) -> bool:
    for x in a:
        for y in b:
            if x == y:
                return True
            if len(x) >= 3 and len(y) >= 3 and (x.startswith(y) or y.startswith(x)):
                return True
    return False


def _qualifiers_synonymous(src_q: set[str], tgt_q: set[str]) -> bool:
    """True when two qualifier sets name the same thing in different words.

    Compared by canonical form rather than surface token. A synonym pair differs
    in spelling by definition — ``salary`` against ``compensation`` shares no
    characters — so a raw token comparison calls every genuine synonym a
    conflicting entity, which is the opposite of what the dictionary is for.
    Qualifiers with no canonical entry fall back to their own surface, so
    ``billing`` against ``shipping`` still conflicts.
    """
    if not src_q and not tgt_q:
        return True
    if not src_q or not tgt_q:
        return False
    left = {lookup_schematic(q) or q for q in src_q}
    right = {lookup_schematic(q) or q for q in tgt_q}
    return left == right


def qualifiers_compatible(source: str, target: str) -> bool:
    """False when both sides have non-overlapping entity prefixes.

    ``order_amt`` vs ``transaction_amount`` and ``created_at`` vs ``updated_at``
    must not look like near-certain matches just because both collapse to a
    domain leaf (amount / timestamp). Stem overlap (ship≈shipping) is allowed.
    """
    src_q = _entity_qualifiers(_token_set(source))
    tgt_q = _entity_qualifiers(_token_set(target))
    if not src_q or not tgt_q:
        return True
    if not src_q.isdisjoint(tgt_q):
        return True
    return _qualifier_stems_overlap(src_q, tgt_q)


def schematic_match_boost(source: str, target: str) -> float | None:
    """High-confidence score when columns resolve to the same canonical form.

    Asymmetric rules:
    - Conflicting entity prefixes → no boost
    - Generic source → specific target (AMT → transaction_amount) → no boost
    - Specific source → bare leaf (order_amt → amount, mobile_phone → phone) → weak
    - Exact bare source → bare leaf (AMT → amount) → strong
    - Source resolves to target surface (billing_cust_id → customer_id) → strong
    """
    src_tokens = _token_set(source)
    tgt_tokens = _token_set(target)
    if "amt" in src_tokens:
        src_tokens = (src_tokens - {"amt"}) | {"amount"}
    if "amt" in tgt_tokens:
        tgt_tokens = (tgt_tokens - {"amt"}) | {"amount"}
    src_id = src_tokens & IDENTITY_KIND_LEAVES
    tgt_id = tgt_tokens & IDENTITY_KIND_LEAVES
    if src_id and tgt_id and src_id != tgt_id:
        # Surface ``id`` vs ``key`` is not proven identity, even when the
        # million-variant index collapsed both onto canonical ``id``.
        return None
    src_q = _entity_qualifiers(src_tokens)
    tgt_q = _entity_qualifiers(tgt_tokens)
    # Distinct entities (user ≠ customer, client ≠ customer) must not inherit
    # a 0.95 boost just because the synonym dictionary over-collapsed them.
    if (
        src_q
        and tgt_q
        and src_q.isdisjoint(tgt_q)
        and not _qualifier_stems_overlap(src_q, tgt_q)
        and not _qualifiers_synonymous(src_q, tgt_q)
    ):
        return None

    src_canon = lookup_schematic(source)
    tgt_canon = lookup_schematic(target)
    tgt_is_bare_leaf = len(tgt_tokens - _ENTITY_STOPWORDS) == 1
    tgt_norm = _normalize(target)

    # Index says source ≡ target surface name — trust before entity-qualifier veto
    # (billing_cust_id → customer_id is a valid enterprise mapping).
    if src_canon and tgt_norm == src_canon:
        if src_q and tgt_is_bare_leaf and len(tgt_tokens) == 1:
            return 0.76
        return 0.99 if not src_q else 0.95

    # Same canonical form and qualifiers that mean the same thing: one concept,
    # two spellings. Checked before the disjoint-token veto below, which would
    # otherwise reject every synonym pair — differing surface tokens are what
    # makes them synonyms, so the veto read ``salary`` against ``compensation``
    # as a conflict between two entities and refused to map them at all.
    same_canonical = bool(src_canon and tgt_canon and src_canon == tgt_canon)
    if same_canonical and _qualifiers_synonymous(src_q, tgt_q):
        return 0.97

    if src_q and tgt_q and src_q.isdisjoint(tgt_q):
        return None

    if same_canonical:
        if not src_q and tgt_q:
            return None
        if src_q and tgt_is_bare_leaf:
            return 0.76
        if src_q and not tgt_q:
            # Compound domain target (phone_number, total_amount) — lexical ranks.
            return None
        return 0.97

    if tgt_canon and _normalize(source) == tgt_canon:
        return 0.95
    return None
