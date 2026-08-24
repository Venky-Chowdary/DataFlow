"""Hybrid semantic column mapper — BM25 lexical retrieval + semantic token graph."""

from __future__ import annotations

import logging
import math
import re
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path

from services.create_new_risk_stamp import (
    apply_create_new_risk_stamps as _apply_create_new_risk_stamps,
)
from services.schematic_index import IDENTITY_KIND_LEAVES as _IDENTITY_KIND_LEAVES

_model_cache = None


def ml_baseline_path() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "ml"
        / "models"
        / "baseline.json"
    )


def ml_baseline_status() -> dict:
    """Operator-facing status for Map UI / Pilot — never silent about ML availability."""
    model = _load_ml_baseline()
    path = ml_baseline_path()
    return {
        "available": model is not None,
        "path": str(path),
        "path_exists": path.exists(),
        "role": "optional_boost",
        "note": (
            "ML baseline available — high-confidence predictions can boost matches."
            if model is not None
            else "ML baseline unavailable — automapping uses lexical + semantic + Hungarian only."
        ),
    }


def _load_ml_baseline():
    """Load the optional automap boost from a JSON vocabulary artifact.

    Never a pickle: the boost is optional, so it must not be able to execute
    code inside the transfer engine. A missing or malformed artifact degrades
    to lexical + semantic + Hungarian mapping, it never fails a transfer.
    """
    global _model_cache
    if _model_cache is not None:
        return _model_cache if _model_cache is not False else None

    try:
        from services.ml_baseline import load_baseline

        model = load_baseline(ml_baseline_path())
        if model is None:
            _model_cache = False
            return None
        _model_cache = model
        return _model_cache
    except Exception as exc:
        # Cache negative result so a broken artifact does not spam every map_columns call.
        _model_cache = False
        logging.getLogger(__name__).warning(
            "ML baseline unavailable (%s); using lexical/semantic mapper only",
            type(exc).__name__,
        )
    return None


def _calibrated_confidence(
    score: float,
    *,
    score_gap: float,
    requires_review: bool,
    hard_cap: float = 0.99,
    fidelity: str = "",
) -> float:
    """Keep near-tie mappings below auto-approve thresholds.

    G4 / Studio auto-approve uses ~0.85 in strict mode. A near-tie with
    confidence 0.93 would pass the gate despite ``requires_review``. Cap
    review rows at 0.84 and scale by gap so operators must confirm.

    Fidelity further spreads the signal: lossless identity stays high;
    lossy / precision-collapse never looks like an auto-approve slam dunk.
    """
    conf = min(float(score), hard_cap)
    fid = (fidelity or "").strip().lower()
    if fid in {"lossy", "lossy_cast", "precision_collapse", "truncate"}:
        conf = min(conf, 0.78)
    elif fid in {"safe_normalize", "normalize", "cast"}:
        conf = min(conf, 0.91)
    if requires_review:
        # gap 0.00 → 0.70, gap 0.07 → ~0.805, never above 0.84
        conf = min(conf, round(0.70 + max(score_gap, 0.0) * 1.5, 3), 0.84)
    return round(conf, 3)


from services.semantic_abbreviations import ABBREVIATIONS  # noqa: E402



def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).rstrip("_")


def _folded_ident(name: str) -> str:
    """Case- and separator-insensitive identifier (UserID ≡ userid ≡ user_id).

    A *leading* underscore is kept: MongoDB's reserved ``_id`` is a different
    column from ``id``, not a separator variant of it, and folding the two onto
    one slot made every re-run of a DataFlow-created collection ambiguous and
    let the document key win the assignment over the literal name match.
    """
    norm = _normalize(name)
    prefix = "_" if norm.startswith("_") else ""
    return prefix + norm.replace("_", "")


def _dest_fold_collisions(target_columns: list[str]) -> set[str]:
    """Destination names that share a folded identifier with a sibling column.

    Postgres/Snowflake fold ``UserID`` and ``userid`` onto one slot; MySQL
    keeps both. Either way Map must not auto-approve a pin onto one of them.
    """
    buckets: dict[str, list[str]] = {}
    for tgt in target_columns:
        buckets.setdefault(_folded_ident(tgt), []).append(tgt)
    collided: set[str] = set()
    for names in buckets.values():
        if len(names) > 1:
            collided.update(names)
    return collided


def _exact_name_unambiguous(
    source: str, target: str, target_columns: list[str]
) -> bool:
    """True when ``target`` is the only column whose name equals ``source``.

    Score gap measures how close the runner-up scored. In a table holding a
    family of similar names (``id`` / ``big_id`` / ``uid``) the runner-up stays
    within the review band even when the winner is a literal name equality, so
    a gap test alone marks re-runs of a table DataFlow itself created as
    ambiguous forever. Name equality is only genuinely ambiguous when a second
    destination column folds to the same identifier (``UserID`` vs ``userid``).
    """
    src_fold = _folded_ident(source)
    if not src_fold or _folded_ident(target) != src_fold:
        return False
    return sum(1 for t in target_columns if _folded_ident(t) == src_fold) == 1


def _expand_abbrev(token: str) -> str:
    return ABBREVIATIONS.get(token, token)


def _semantic_tokens(name: str) -> list[str]:
    norm = _normalize(name)
    parts = [p for p in norm.split("_") if p]
    tokens: list[str] = []
    i = 0
    # Match longest abbreviation phrase first so multi-token abbreviations like
    # "txn_dt" or "created_at" resolve to their canonical form.
    while i < len(parts):
        matched = False
        for j in range(len(parts), i, -1):
            phrase = "_".join(parts[i:j])
            if phrase in ABBREVIATIONS:
                expansion = ABBREVIATIONS[phrase]
                exp_parts = [p for p in expansion.split("_") if p]
                tokens.extend(exp_parts)
                i = j
                # Skip trailing parts already covered by the expansion
                # (email → email_address then addr → address must not double).
                already = set(tokens)
                while i < len(parts):
                    nxt = _expand_abbrev(parts[i])
                    nxt_parts = [p for p in nxt.split("_") if p]
                    if nxt_parts and all(p in already for p in nxt_parts):
                        i += 1
                        already.update(nxt_parts)
                        continue
                    break
                matched = True
                break
        if not matched:
            expansion = _expand_abbrev(parts[i])
            tokens.extend([p for p in expansion.split("_") if p])
            i += 1
    # Collapse adjacent duplicates from flattened multi-token expansions.
    deduped: list[str] = []
    for t in tokens:
        if not deduped or deduped[-1] != t:
            deduped.append(t)
    return deduped


def _semantic_form(name: str) -> str:
    return "_".join(_semantic_tokens(name))


def create_new_target_name(source_column: str) -> str:
    """Destination column name for a CREATE TABLE / ADD COLUMN proposal.

    A migration must land the operator's own column names. Expanding the source
    name to its canonical semantic form (``qty`` → ``quantity``) renamed columns
    nobody asked to rename: downstream SQL and BI on the destination break, a
    by-name reconcile disagrees with a transfer that moved every row correctly,
    and the next run maps ``qty`` onto the ``quantity`` the product itself just
    created — scored as a rename, held for review, and refused by Execute, so an
    unchanged route can never be re-run.

    The canonical form stays available as ``semantic_name`` for enrichment and
    for an operator who chooses the rename explicitly. Only illegal characters
    are repaired, by the single identifier owner.
    """
    from connectors.sql_identifiers import sanitize_identifier

    raw = (source_column or "").strip()
    return sanitize_identifier(raw, preserve_case=True) or _semantic_form(raw)


def _canonical_form(name: str) -> str:
    """Resolve enterprise schematic variant → canonical semantic form."""
    try:
        from services.schematic_index import lookup_schematic

        canon = lookup_schematic(name)
        if canon:
            return canon
    except ImportError:
        pass
    return _semantic_form(name)


def _tokenize(name: str) -> list[str]:
    """Abbreviation-expanded tokens for BM25 / IDF — not schematic-collapsed."""
    return [t for t in _semantic_form(name).split("_") if t]


def _build_idf(corpus: list[str]) -> dict[str, float]:
    n = len(corpus)
    df: Counter[str] = Counter()
    for doc in corpus:
        for tok in set(_tokenize(doc)):
            df[tok] += 1
    return {tok: math.log((n + 1) / (freq + 1)) + 1.0 for tok, freq in df.items()}


def _bm25_score(query_tokens: list[str], doc_tokens: list[str], idf: dict[str, float], avgdl: float, k1: float = 1.5, b: float = 0.75) -> float:
    if not query_tokens or not doc_tokens:
        return 0.0
    doc_len = len(doc_tokens)
    avgdl = max(avgdl, 1.0)
    tf = Counter(doc_tokens)
    score = 0.0
    for qt in query_tokens:
        if qt not in tf:
            continue
        freq = tf[qt]
        idf_val = idf.get(qt, 1.0)
        denom = freq + k1 * (1 - b + b * doc_len / avgdl)
        score += idf_val * (freq * (k1 + 1)) / denom
    return score


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


# Leaf tokens too generic to prove columns are the same when qualifiers conflict.
_ENTITY_STOPWORDS = frozenset({
    "at", "of", "the", "a", "an", "to", "for", "by", "and", "or", "on", "in",
})
_DOMAIN_LEAVES = frozenset({
    "amount", "id", "name", "date", "code", "status", "type", "number",
    "value", "count", "flag", "key", "timestamp", "balance", "price",
    "quantity", "total", "rate", "pct", "percent", "description", "text",
    "address", "email", "phone", "time", "uuid", "hash", "index", "seq",
})
_GENERIC_LEAVES = _DOMAIN_LEAVES | _ENTITY_STOPWORDS
# Same-entity ``id`` vs ``key`` is a false friend (CRM id ≠ warehouse surrogate).
# Identity-kind leaves live in schematic_index.IDENTITY_KIND_LEAVES (SSOT).
# Typed measure subtypes that must appear on the destination. ``tax`` is not
# ``total``; a generic amount bucket is not a proven tax/discount/salary column.
_MEASURE_KIND_TOKENS = frozenset({
    "tax", "vat", "gst", "discount", "net", "gross", "fee", "tip", "duty",
    "freight", "salary", "commission", "bonus", "payment", "unit",
})
_MONEY_LEAVES = frozenset({"amount", "total", "balance", "price", "cost"})
# Count/quantity is not money. Fivetran/Airbyte-class operators lose trust when
# ``order_qty`` auto-pins onto ``order_amt`` because both share ``order``.
_COUNT_LEAVES = frozenset({"quantity", "count", "units", "pieces"})
# created vs updated is polarity, not a license to ADD a sibling timestamp.
_TEMPORAL_POLARITY = frozenset({"created", "updated", "modified", "deleted", "inserted"})
# Below G4 strict (~0.85) even if Map forgets requires_review.
_AMBIGUOUS_PAIR_CAP = 0.78


def _qualifier_tokens(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} - _DOMAIN_LEAVES - _ENTITY_STOPWORDS


# Lightweight English stem rules for entity qualifiers (paid≈payment, create≈creation).
# Not a full Porter stemmer — just high-frequency migration stems.
_STEM_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("izations", ""),
    ("isation", ""),
    ("ations", ""),
    ("ation", ""),
    ("ments", ""),
    ("ment", ""),
    ("ings", ""),
    ("ing", ""),
    ("tions", ""),
    ("tion", ""),
    ("sion", ""),
    ("ness", ""),
    ("ities", "ity"),
    ("ity", ""),
    ("iers", "y"),
    ("ies", "y"),
    ("ied", "y"),
    ("ously", ""),
    ("ally", ""),
    ("ers", ""),
    ("ors", ""),
    ("er", ""),
    ("or", ""),
    ("als", "al"),
    ("al", ""),
    ("eds", ""),
    ("ed", ""),
    ("es", ""),
    ("s", ""),
)

_STEM_IRREGULAR: dict[str, str] = {
    "paid": "pay",
    "payment": "pay",
    "payments": "pay",
    "bought": "buy",
    "purchase": "buy",
    "purchased": "buy",
    "created": "create",
    "creation": "create",
    "creator": "create",
    "updated": "update",
    "updation": "update",  # common misspelling in schemas
    "shipped": "ship",
    "shipping": "ship",
    "shipment": "ship",
    "customer": "cust",
    "customers": "cust",
    "transaction": "txn",
    "transactions": "txn",
    "amount": "amt",
    "amounts": "amt",
}


def _light_stem(token: str) -> str:
    t = (token or "").strip().lower()
    if not t:
        return ""
    if t in _STEM_IRREGULAR:
        return _STEM_IRREGULAR[t]
    for suf, repl in _STEM_SUFFIXES:
        if len(t) > len(suf) + 2 and t.endswith(suf):
            return t[: -len(suf)] + repl
    return t


def _qualifier_stems_overlap(a: set[str], b: set[str]) -> bool:
    """True when qualifiers share a stem (ship ≈ shipping, paid ≈ payment)."""
    stems_a = {_light_stem(x) for x in a} | set(a)
    stems_b = {_light_stem(y) for y in b} | set(b)
    if stems_a & stems_b:
        return True
    for x in a:
        for y in b:
            if x == y:
                return True
            if len(x) >= 3 and len(y) >= 3 and (x.startswith(y) or y.startswith(x)):
                return True
            sx, sy = _light_stem(x), _light_stem(y)
            if sx and sy and (sx == sy or (len(sx) >= 3 and len(sy) >= 3 and (sx.startswith(sy) or sy.startswith(sx)))):
                return True
    return False


def _qualifiers_compatible(source: str, target: str) -> bool:
    """False when both sides carry conflicting entity prefixes."""
    src_q = _qualifier_tokens(source)
    tgt_q = _qualifier_tokens(target)
    if not src_q or not tgt_q:
        return True
    if not src_q.isdisjoint(tgt_q):
        return True
    return _qualifier_stems_overlap(src_q, tgt_q)


def _entity_agreement(source: str, target: str) -> float:
    """1.0 shared entity, 0.5 asymmetric/generic, 0.0 hard conflict."""
    src_q = _qualifier_tokens(source)
    tgt_q = _qualifier_tokens(target)
    if src_q and tgt_q:
        if not src_q.isdisjoint(tgt_q):
            return len(src_q & tgt_q) / len(src_q | tgt_q)
        if _qualifier_stems_overlap(src_q, tgt_q):
            return 0.75
        return 0.0
    if not src_q and not tgt_q:
        return 0.55
    return 0.35


def _identity_kind_leaves(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} & _IDENTITY_KIND_LEAVES


def _identity_leaf_mismatch(source: str, target: str) -> bool:
    """True when both names carry identity-kind leaves that are not the same token.

    ``cust_id`` vs ``customer_id`` shares leaf ``id`` (pin). ``cust_id`` vs
    ``customer_key`` is the same entity with a different identity kind — Map
    must confirm. High lexical similarity must not skip G4.
    """
    src = _identity_kind_leaves(source)
    tgt = _identity_kind_leaves(target)
    if not src or not tgt:
        return False
    return src != tgt


def _measure_kind_tokens(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} & _MEASURE_KIND_TOKENS


def _measure_kind_mismatch(source: str, target: str) -> bool:
    """True when the source is a typed measure the destination does not share.

    ``tax_amt`` vs ``tax_amount`` shares ``tax``. ``tax_amt`` vs ``total_amount``
    looks like a compound amount bucket because ``total`` is a domain leaf —
    that must not auto-pin as identity. ``order_qty`` vs ``order_amt`` shares
    the entity but not the measure family (count ≠ money).
    """
    src = _measure_kind_tokens(source)
    tgt = _measure_kind_tokens(target)
    if src and src.isdisjoint(tgt):
        return True
    src_money = bool(_money_leaves(source))
    tgt_money = bool(_money_leaves(target))
    src_count = bool(_count_leaves(source))
    tgt_count = bool(_count_leaves(target))
    return (src_money and tgt_count) or (src_count and tgt_money)


def _money_leaves(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} & _MONEY_LEAVES


def _count_leaves(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} & _COUNT_LEAVES


def _shared_money_family(source: str, target: str) -> bool:
    return bool(_money_leaves(source) and _money_leaves(target))


def _entity_conflict_requires_review(source: str, target: str) -> bool:
    """True when both sides name different entities (user ≠ customer).

    Schematic index collapse (``user_id`` → canonical ``customer_id``) must
    not skip G4. Shared money families still propose with review elsewhere.
    """
    if _shared_money_family(source, target):
        return False
    return _entity_agreement(source, target) == 0.0


def _temporal_polarity_conflict(source: str, target: str) -> bool:
    src = {t for t in _semantic_form(source).split("_") if t} & _TEMPORAL_POLARITY
    tgt = {t for t in _semantic_form(target).split("_") if t} & _TEMPORAL_POLARITY
    return bool(src and tgt and src != tgt)


def _reason_forces_review(reason: str) -> bool:
    return "review required" in (reason or "").lower()


# Stable Map review kinds — UI / RAG / Proof consume this stamp, not English
# parsing alone. Airbyte schema review is all-or-nothing (#74892 / #78427);
# these kinds keep quantity≠amount and user≠customer off Approve-eligible.
REVIEW_KIND_MEASURE = "measure_kind"
REVIEW_KIND_ENTITY = "entity_identity"
REVIEW_KIND_DEST_COLLISION = "dest_collision"
REVIEW_KIND_IDENTITY_LEAF = "identity_leaf"
REVIEW_KIND_TEMPORAL = "temporal_polarity"
REVIEW_KIND_LOSSY = "lossy"
REVIEW_KIND_CREATE_NEW = "create_new"
REVIEW_KIND_GENERIC = "generic"

FALSE_FRIEND_REVIEW_KINDS = frozenset(
    {
        REVIEW_KIND_MEASURE,
        REVIEW_KIND_ENTITY,
        REVIEW_KIND_DEST_COLLISION,
        REVIEW_KIND_IDENTITY_LEAF,
        REVIEW_KIND_TEMPORAL,
    }
)


def classify_review_kind(
    *,
    source: str,
    target: str,
    reason: str = "",
    requires_review: bool = False,
    create_new: bool = False,
    dest_collisions: set[str] | None = None,
) -> str | None:
    """Classify why Map held a pair. None when the pair does not need review."""
    if not requires_review:
        return None
    text = (reason or "").lower()
    collisions = dest_collisions or set()
    if target in collisions or "destination identifier collision" in text:
        return REVIEW_KIND_DEST_COLLISION
    if "measure-kind mismatch" in text or (
        source and target and _measure_kind_mismatch(source, target)
    ):
        return REVIEW_KIND_MEASURE
    if "identity leaf mismatch" in text or (
        source and target and _identity_leaf_mismatch(source, target)
    ):
        return REVIEW_KIND_IDENTITY_LEAF
    if "temporal polarity" in text or (
        source and target and _temporal_polarity_conflict(source, target)
    ):
        return REVIEW_KIND_TEMPORAL
    if (
        "entity qualifier conflict" in text
        or "conflicting entity qualifiers" in text
        or (source and target and _entity_conflict_requires_review(source, target))
    ):
        return REVIEW_KIND_ENTITY
    if "lossy type pair" in text:
        return REVIEW_KIND_LOSSY
    if create_new:
        return REVIEW_KIND_CREATE_NEW
    return REVIEW_KIND_GENERIC


def _stamp_review_kinds(
    mappings: list[dict],
    dest_collisions: set[str] | None = None,
) -> list[dict]:
    collisions = dest_collisions or set()
    for row in mappings:
        kind = classify_review_kind(
            source=str(row.get("source") or ""),
            target=str(row.get("target") or ""),
            reason=str(row.get("reasoning") or ""),
            requires_review=bool(row.get("requires_review")),
            create_new=bool(row.get("create_new")),
            dest_collisions=collisions,
        )
        if kind:
            row["review_kind"] = kind
        else:
            row.pop("review_kind", None)
    return mappings


def _is_bare_domain_leaf(name: str) -> bool:
    toks = {t for t in _semantic_form(name).split("_") if t} - _ENTITY_STOPWORDS
    return len(toks) == 1 and toks <= _DOMAIN_LEAVES


def _near_target_by_form(
    source: str,
    target_columns: list[str],
    *,
    used_targets: set[str] | None = None,
) -> tuple[str, float]:
    """Best unused destination by abbreviation-expanded form similarity."""
    used = {t.lower() for t in (used_targets or set())}
    src_form = _semantic_form(source)
    best_tgt = ""
    best = 0.0
    for tgt in target_columns:
        if tgt.lower() in used:
            continue
        if not _qualifiers_compatible(source, tgt):
            continue
        ratio = _similarity(src_form, _semantic_form(tgt))
        agreement = _entity_agreement(source, tgt)
        ratio = ratio * (0.55 + 0.45 * max(agreement, 0.15))
        # Containment bonus: phone ⊂ phone_number when entities agree
        tgt_form = _semantic_form(tgt)
        if agreement > 0 and src_form and tgt_form and (src_form in tgt_form or tgt_form in src_form):
            ratio = max(ratio, 0.72 * (0.6 + 0.4 * agreement))
        if ratio > best:
            best, best_tgt = ratio, tgt
    return best_tgt, best


def _identity_onto_numeric_landmine(source: str, src_type: str, tgt_type: str) -> bool:
    """True when Mongo/document identity would land on a numeric warehouse PK.

    Without samples the old mapper bound ``_id``→NUMBER ``id`` (~0.73). Hex
    ObjectIds never fit INTEGER/NUMBER — refuse that Map landmine up front.
    """
    from services.decision_kernel import normalize_logical_type
    from services.type_system import specialty_carrier_base

    tgt = normalize_logical_type(tgt_type)
    if tgt not in {"integer", "decimal", "float"}:
        return False
    if specialty_carrier_base(src_type) == "OBJECTID":
        return True
    src = normalize_logical_type(src_type)
    if src not in {"string", "text", "unknown"}:
        return False
    form = _normalize(source).replace(" ", "")
    return form in {"_id", "id", "objectid", "object_id", "oid", "mongo_id"}


def _type_compat_penalty(
    src_type: str,
    tgt_type: str,
    *,
    source_name: str = "",
    dest_db: str = "",
) -> float:
    """Reduce score for incompatible type pairs using the canonical type-system rules.

    Lossy pairs must not clear Map auto-approve / G4 after an Exact-name boost —
    demote hard enough that calibrated confidence stays ≤0.84.
    """
    from services.decision_kernel import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    if source_name and _identity_onto_numeric_landmine(source_name, src_type, tgt_type):
        # Stronger than generic lossy — must lose to create-new text path.
        return 0.92
    if is_lossy_coercion(src_type, tgt_type, dest_db=dest_db):
        src = normalize_logical_type(src_type)
        tgt = normalize_logical_type(tgt_type)
        if src == "binary" and tgt != "binary":
            return 0.8
        if src in ("json", "array") and tgt in ("integer", "decimal", "boolean", "date", "datetime", "time", "binary", "uuid"):
            return 0.7
        if src in ("decimal", "float", "double") and tgt == "integer":
            return 0.55
        if src in ("datetime", "timestamp") and tgt == "date":
            return 0.5
        return 0.5
    return 0.0

def _type_aware_boost(src_type: str, tgt_type: str, *, dest_db: str = "") -> float:
    """Boost score for exact or highly compatible type matches."""
    from services.decision_kernel import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    src = normalize_logical_type(src_type)
    tgt = normalize_logical_type(tgt_type)
    if src == tgt:
        return 0.05
    if is_lossy_coercion(src_type, tgt_type, dest_db=dest_db):
        return 0.0
    # Safe widening / cross-cast pairs that are not lossy.
    safe_pairs: set[tuple[str, str]] = {
        ("integer", "decimal"), ("boolean", "integer"), ("boolean", "decimal"),
        ("date", "datetime"), ("string", "text"), ("uuid", "string"), ("uuid", "text"),
        ("json", "text"), ("array", "text"), ("json", "string"), ("array", "string"),
    }
    if (src, tgt) in safe_pairs:
        return 0.03
    if src in ("string", "text", "uuid") and tgt in ("string", "text", "uuid"):
        return 0.02
    return 0.0


def _sample_consistency_boost(samples: list[str] | None, source_type: str, target_type: str) -> float:
    """Boost score when sample values parse cleanly for target logical type."""
    if not samples or len(samples) < 2:
        return 0.0
    from services.decision_kernel import (
        normalize_logical_type,
        typed_cast_incompatible_with_text_sink,
    )
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    transform = infer_transform_for_mapping(
        "col", "col", source_type, target_type, source_samples=samples,
    )
    if typed_cast_incompatible_with_text_sink(
        transform, normalize_logical_type(target_type)
    ):
        # A text carrier stores the token verbatim, so scoring the samples
        # through a typed cast measures a coercion the write never performs.
        # Y/N inferred BOOLEAN parsed 0/2 here and demoted an exact-name match
        # onto an existing TEXT column below the floor — Map then invented a
        # BOOLEAN `<col>_text` beside the operator's own column.
        return 0.0
    ok = 0
    checked = 0
    for raw in samples[:8]:
        if raw is None or str(raw).strip() == "":
            continue
        checked += 1
        _, err = apply_transform(str(raw), transform)
        if not err:
            ok += 1
    if checked < 2:
        return 0.0
    rate = ok / checked
    if rate >= 0.9:
        return 0.06
    if rate >= 0.7:
        return 0.03
    if rate < 0.2:
        # Hard demote: ObjectId/hex → DECIMAL (etc.) must lose to type-compatible targets.
        return -0.90
    if rate < 0.4:
        return -0.15
    return 0.0


def _score_pair(
    source: str,
    target: str,
    idf: dict[str, float],
    avgdl: float,
    source_role: str | None = None,
    target_role: str | None = None,
    source_type: str = "VARCHAR",
    target_type: str = "VARCHAR",
    source_samples: list[str] | None = None,
    dest_db: str = "",
    create_new_target: bool = False,
) -> tuple[float, str]:
    from services.semantic_analyzer import role_match_boost
    from services.training_lexicon import lexicon_boost

    src_norm = _normalize(source)
    tgt_norm = _normalize(target)
    src_sem = _semantic_form(source)
    tgt_sem_raw = _semantic_form(target)

    # A column this run will CREATE carries the type the kernel invents for this
    # very source, so there is no declared destination type to lose fidelity
    # against; charging a cast there billed create-new for its own carrier.
    type_penalty = (
        0.0
        if create_new_target
        else _type_compat_penalty(
            source_type, target_type, source_name=source, dest_db=dest_db
        )
    )
    type_boost = _type_aware_boost(source_type, target_type, dest_db=dest_db)
    sample_boost = _sample_consistency_boost(source_samples, source_type, target_type)
    if (
        sample_boost > -0.5
        and _identity_onto_numeric_landmine(source, source_type, target_type)
    ):
        # No samples: still refuse ObjectId/text identity → NUMBER (Validate would
        # only catch it later after Map already offered the landmine).
        sample_boost = -0.90

    def _finish(score: float, reason: str) -> tuple[float, str]:
        # Only a literal name equality may reach the top of the band: type and
        # sample boosts must never lift a near-name candidate (``_id``) into a
        # tie with the column that carries the same name (``id``).
        ceiling = 0.995 if src_norm == tgt_norm else 0.99
        adjusted = max(
            0.0, min(ceiling, float(score) - type_penalty + type_boost + sample_boost)
        )
        review_bits: list[str] = []
        if _identity_leaf_mismatch(source, target):
            src_l = "/".join(sorted(_identity_kind_leaves(source)))
            tgt_l = "/".join(sorted(_identity_kind_leaves(target)))
            adjusted = min(adjusted, _AMBIGUOUS_PAIR_CAP)
            review_bits.append(f"identity leaf mismatch ({src_l}≠{tgt_l})")
        if _measure_kind_mismatch(source, target):
            adjusted = min(adjusted, _AMBIGUOUS_PAIR_CAP)
            review_bits.append("measure-kind mismatch")
        if _entity_conflict_requires_review(source, target):
            adjusted = min(adjusted, _AMBIGUOUS_PAIR_CAP)
            review_bits.append("entity qualifier conflict")
        if _temporal_polarity_conflict(source, target):
            adjusted = min(adjusted, _AMBIGUOUS_PAIR_CAP)
            review_bits.append("temporal polarity conflict")
        if review_bits:
            reason = f"{reason} · {' · '.join(review_bits)} — review required"
        return adjusted, reason

    if src_norm == tgt_norm:
        return _finish(0.995, "Exact name match")
    if src_sem == tgt_sem_raw:
        return _finish(0.975, "Exact semantic token match")

    schematic = None
    try:
        from services.schematic_index import schematic_match_boost
        schematic = schematic_match_boost(source, target)
    except ImportError:
        pass
    if schematic is not None:
        # Blend a touch of form similarity so equal schematic hits
        # (mobile_phone → phone vs phone_number) still differentiate.
        form_ratio = _similarity(src_sem, tgt_sem_raw)
        blended = min(0.995, schematic * 0.92 + form_ratio * 0.08)
        return _finish(blended, "Schematic index match (1M+ variants)")

    # Hard entity conflict (created vs updated, order vs transaction): demote early.
    agreement = _entity_agreement(source, target)
    if agreement == 0.0:
        form_ratio = _similarity(src_sem, tgt_sem_raw)
        if _shared_money_family(source, target):
            # Same measure family, different entity (order_amt vs payment_amount).
            # Propose below G4 so Map confirms — do not auto-pin, and do not hide
            # the only dest amount behind create_new.
            return _finish(
                min(_AMBIGUOUS_PAIR_CAP, 0.58 + form_ratio * 0.22),
                "Conflicting entity qualifiers on same measure — review required",
            )
        if _identity_kind_leaves(source) or _identity_kind_leaves(target):
            # user_id vs customer_id is a dest candidate, not a license to ADD
            # a sibling column. Propose below G4 — never invent, never auto-pin.
            return _finish(
                min(_AMBIGUOUS_PAIR_CAP, 0.58 + form_ratio * 0.22),
                "Conflicting entity qualifiers on identity — review required",
            )
        if _temporal_polarity_conflict(source, target):
            return _finish(
                min(_AMBIGUOUS_PAIR_CAP, 0.58 + form_ratio * 0.22),
                "Conflicting temporal polarity — review required",
            )
        return _finish(min(0.42, form_ratio * 0.55), "Conflicting entity qualifiers")

    src_canon = _canonical_form(source)
    tgt_canon = _canonical_form(target)
    expanded = _semantic_form(source)
    if src_canon and _normalize(target) == src_canon:
        if _qualifier_tokens(source):
            return _finish(0.76, "Canonical schematic resolution (specific→bare leaf)")
        return _finish(0.99, "Canonical schematic resolution (exact target)")
    if src_canon and tgt_canon and src_canon == tgt_canon and _qualifiers_compatible(source, target):
        if _identity_leaf_mismatch(source, target):
            pass  # same canonical ``id`` is not proven identity when leaves differ
        else:
            src_q = _qualifier_tokens(source)
            tgt_q = _qualifier_tokens(target)
            if not src_q and tgt_q:
                pass  # generic → specific: fall through
            elif src_q and _is_bare_domain_leaf(target):
                return _finish(0.76, "Canonical schematic resolution (specific→generic)")
            elif src_q and not tgt_q:
                pass  # compound domain target — fall through
            elif _normalize(target) == _normalize(expanded):
                return _finish(0.985, "Canonical schematic resolution (expanded form)")
            else:
                return _finish(0.93, "Canonical schematic resolution")

    if _normalize(target) == _normalize(expanded):
        return _finish(0.94, "Abbreviation expansion match")

    # Domain expansions: mobile_phone → phone_number, etc.
    src_parts = set(src_sem.split("_")) - {""}
    if "mobile" in src_parts and "phone" in src_parts and tgt_sem_raw == "phone_number":
        return _finish(0.965, "Mobile phone → phone_number expansion")
    if "mobile" in src_parts and "phone" in src_parts and tgt_sem_raw == "phone":
        return _finish(0.86, "Mobile phone → short phone form")

    if source_role and target_role:
        boost = role_match_boost(source_role, target_role)
        if boost is not None:
            # Tie-break same-role collisions (email_addr vs usr_email) with lexical form.
            from difflib import SequenceMatcher

            lex = SequenceMatcher(None, src_norm, tgt_norm).ratio()
            adjusted = min(0.995, float(boost) * 0.82 + lex * 0.18)
            if lex >= 0.72:
                adjusted = max(adjusted, min(0.97, float(boost)))
            return _finish(adjusted, f"Semantic role match: {source_role} → {target_role} (lex={lex:.2f})")

    boosted = lexicon_boost(source, target)
    if boosted is not None:
        return _finish(boosted, "Training lexicon match (synthetic_v1)")

    # Lexical stage uses abbreviation-expanded forms — not schematic canonical
    # collapse — so order_amount vs total_amount can outrank transaction_amount.
    src_form = _semantic_form(source)
    tgt_form = _semantic_form(target)
    form_ratio = _similarity(src_form, tgt_form)

    if src_form == tgt_form:
        return _finish(0.96, "Semantic token match")

    bm25 = _bm25_score(
        src_form.split("_"),
        tgt_form.split("_"),
        idf,
        avgdl,
    )
    bm25_norm = min(bm25 / 8.0, 1.0)

    # Advanced heuristic: ML Baseline prediction
    ml_model = _load_ml_baseline()
    ml_boost = 0.0
    if ml_model:
        pred_tgt, pred_score = ml_model.predict_target(source)
        if _normalize(pred_tgt) == tgt_norm and pred_score > 0.5:
            ml_boost = min(pred_score * 0.15, 0.15)
            if pred_score > 0.8:
                return _finish(0.95, "ML Baseline highly confident match")

    src_toks = set(src_form.split("_")) - {""}
    tgt_toks = set(tgt_form.split("_")) - {""}
    overlap = len(src_toks & tgt_toks)
    shared = src_toks & tgt_toks
    only_generic_overlap = overlap > 0 and shared <= _DOMAIN_LEAVES

    # Prefer specific amount targets when source has an entity prefix
    # (order_amount → total_amount over bare amount).
    src_q = _qualifier_tokens(source)
    if src_q and _is_bare_domain_leaf(target) and only_generic_overlap:
        return _finish(
            min(0.80, 0.62 + form_ratio * 0.20 + bm25_norm * 0.05),
            "Specific source → bare domain leaf",
        )

    # Compound domain targets (total_amount) with a matching leaf + form similarity.
    if src_q and only_generic_overlap and not _is_bare_domain_leaf(target):
        return _finish(
            min(0.94, 0.72 + form_ratio * 0.22 + bm25_norm * 0.04),
            "Specific source → compound domain target",
        )

    if src_form in tgt_form or tgt_form in src_form:
        if min(len(src_form), len(tgt_form)) >= 4:
            base = 0.80 + form_ratio * 0.14 + bm25_norm * 0.04 + agreement * 0.04
            return _finish(min(0.97, max(0.86, base)), "Partial semantic overlap + form similarity")

    if overlap >= 2:
        return _finish(
            0.82 + overlap * 0.03 + bm25_norm * 0.05 + form_ratio * 0.04 + agreement * 0.03 + ml_boost,
            f"Shared tokens ({overlap}) + BM25",
        )

    fuzzy = form_ratio

    def ngrams(s, n):
        return set(s[i:i+n] for i in range(max(1, len(s)-n+1)))
    jaccard = 0.0
    s_ngrams, t_ngrams = ngrams(src_form, 3), ngrams(tgt_form, 3)
    if s_ngrams or t_ngrams:
        jaccard = len(s_ngrams & t_ngrams) / len(s_ngrams | t_ngrams)

    if only_generic_overlap:
        combined = 0.55 + fuzzy * 0.35 + jaccard * 0.08 + agreement * 0.05 + ml_boost
        return _finish(min(combined, 0.92), "Generic leaf + form similarity")

    combined = max(fuzzy * 0.75, bm25_norm * 0.88, jaccard * 0.82) + ml_boost + agreement * 0.05

    if combined >= 0.78:
        return _finish(min(combined, 0.99), "BM25 / Jaccard lexical retrieval")
    if overlap == 1 and len(src_form.split("_")) > 1:
        return _finish(min(0.70 + fuzzy * 0.22 + agreement * 0.05 + ml_boost, 0.95), "Single token overlap + form similarity")
    return _finish(min(combined, 0.99), "Character similarity")

def _hungarian_minimize(cost: list[list[float]]) -> list[int]:
    """Return row -> column assignment for rows <= columns."""
    if not cost:
        return []
    n = len(cost)
    m = len(cost[0])
    if n > m:
        raise ValueError("Hungarian solver requires rows <= columns")

    u = [0.0] * (n + 1)
    v = [0.0] * (m + 1)
    p = [0] * (m + 1)
    way = [0] * (m + 1)

    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [float("inf")] * (m + 1)
        used = [False] * (m + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = float("inf")
            j1 = 0
            for j in range(1, m + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(0, m + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break

        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break

    assignment = [-1] * n
    for j in range(1, m + 1):
        if p[j] > 0:
            assignment[p[j] - 1] = j - 1
    return assignment


def _optimal_assignment(
    source_columns: list[str],
    target_columns: list[str],
    scores: dict[tuple[str, str], tuple[float, str]],
) -> dict[str, tuple[str, float, str]]:
    """Maximum-weight one-to-one assignment across source/target columns."""
    if not source_columns or not target_columns:
        return {}

    max_score = 1.0
    assigned: dict[str, tuple[str, float, str]] = {}

    if len(source_columns) <= len(target_columns):
        cost = [
            [max_score - scores[(src, tgt)][0] for tgt in target_columns]
            for src in source_columns
        ]
        assignment = _hungarian_minimize(cost)
        for src_idx, tgt_idx in enumerate(assignment):
            if tgt_idx < 0:
                continue
            src = source_columns[src_idx]
            tgt = target_columns[tgt_idx]
            score, reason = scores[(src, tgt)]
            assigned[src] = (tgt, score, reason)
        return assigned

    # Transpose when sources outnumber targets so every target is used at most once.
    cost = [
        [max_score - scores[(src, tgt)][0] for src in source_columns]
        for tgt in target_columns
    ]
    assignment = _hungarian_minimize(cost)
    for tgt_idx, src_idx in enumerate(assignment):
        if src_idx < 0:
            continue
        src = source_columns[src_idx]
        tgt = target_columns[tgt_idx]
        score, reason = scores[(src, tgt)]
        assigned[src] = (tgt, score, reason)
    return assigned


def _alternatives(
    source: str,
    target_columns: list[str],
    scores: dict[tuple[str, str], tuple[float, str]],
    *,
    limit: int = 3,
) -> list[dict]:
    ranked = sorted(
        (
            {
                "target": target,
                "confidence": round(min(scores[(source, target)][0], 0.99), 3),
                "reasoning": scores[(source, target)][1],
            }
            for target in target_columns
        ),
        key=lambda item: item["confidence"],
        reverse=True,
    )
    return ranked[:limit]


# Create-new / identity passthrough is "will CREATE", not "proven against existing dest".
# Cap under G4 auto-approve floor (~0.85) so operators must Approve before Validate
# treats projected DDL as proven. Reserve ≥0.95 for existing-dest exact+sample match.
IDENTITY_PASSTHROUGH_CONFIDENCE = 0.84


def _create_new_physical_why_type(src_type: str, stamp: str, dest_db: str) -> str:
    """Dest-physical type for create-new Why / conversion class.

    ``ddl_type(snowflake, BIGINT)`` stays ``BIGINT``, but writers emit
    ``NUMBER(38,0)``. Classify against that carrier so BIGINT→NUMBER is
    lossless widening, not a false identity.
    """
    from services.decision_kernel import ddl_type, materialize_dest_ddl, normalize_logical_type

    why = (stamp or src_type or "").strip() or src_type
    if not dest_db:
        return why
    try:
        materialized = materialize_dest_ddl(dest_db, why) or why
    except Exception:
        materialized = why
    logical = normalize_logical_type(src_type)
    if logical == "integer":
        family = ddl_type(dest_db, "INTEGER")
        family_u = (family or "").upper().replace(" ", "")
        if family and family_u not in {"INTEGER", "BIGINT", "INT", "INT64", "SMALLINT"}:
            return family
    return materialized


def authority_mappings(
    source_columns: list[str],
    target_columns: list[str],
    **kwargs,
) -> list[dict]:
    """Single Map SSOT for RAG / Pilot / LLM / enhanced AI.

    Those layers retrieve evidence and explain. They must not invent a second
    confidence or assignment. Transfer, Validate, and G4 already consume
    ``map_columns`` — AI surfaces must too.
    """
    return map_columns(source_columns, target_columns, **kwargs)


def pair_mapping_authority(source: str, target: str) -> dict:
    """Single-pair view of the Map SSOT for RAG suggest/retrieve."""
    rows = map_columns([source], [target])
    row = rows[0] if rows else {}
    return {
        "source": source,
        "proposed_target": row.get("target"),
        "confidence": float(row.get("confidence") or 0),
        "requires_review": bool(row.get("requires_review")),
        "create_new": bool(row.get("create_new")),
        "reasoning": str(row.get("reasoning") or ""),
        "assignment_strategy": str(row.get("assignment_strategy") or ""),
        "review_kind": row.get("review_kind"),
        "authority": "semantic_mapper.map_columns",
    }


def map_columns(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    threshold: float = 0.85,
    destination_db_type: str = "",
    destination_table_exists: bool | None = None,
    source_db_type: str = "",
) -> list[dict]:
    from services.semantic_analyzer import analyze_column
    from services.conversion_contract import classify_conversion, create_new_mapping_reason
    from services.decision_kernel import (
        create_new_mapping_target_type,
        ddl_type,
    )

    floor = max(0.55, threshold - 0.3)
    src_roles: dict[str, str] = {}
    tgt_roles: dict[str, str] = {}
    src_types: dict[str, str] = {}
    tgt_types: dict[str, str] = {}
    src_samples: dict[str, list[str]] = {}
    # target column name -> the source column whose create-new carrier it is.
    create_new_pairs: dict[str, str] = {}
    dest_db = (destination_db_type or "").strip().lower()
    src_db = (source_db_type or "").strip().lower()

    if source_schemas:
        for s in source_schemas:
            analyzed = analyze_column(s.get("name", ""), s.get("inferred_type", "VARCHAR"), s.get("samples", []))
            src_roles[s["name"]] = analyzed["semantic_role"]
            src_types[s["name"]] = s.get("inferred_type", "VARCHAR")
            if s.get("samples"):
                src_samples[s["name"]] = [str(x) for x in s["samples"][:8]]
    if target_schemas:
        for t in target_schemas:
            analyzed = analyze_column(t.get("name", ""), t.get("inferred_type", "VARCHAR"), t.get("samples", []))
            tgt_roles[t["name"]] = analyzed["semantic_role"]
            tgt_types[t["name"]] = t.get("inferred_type", "VARCHAR")
    elif target_columns:
        # Names-only without typed introspect: never invent proven VARCHAR.
        # Existing tables must reload schema before create_compatible_new.
        names_only_existing = destination_table_exists is True
        # A create-new column has no declared type yet: its carrier is the one
        # this dialect will CREATE for the source column landing in it. Reading
        # VARCHAR there billed every DECIMAL/DATE source for a cast to a string
        # column the run never creates.
        src_by_folded = {_folded_ident(s): s for s in source_columns}
        for t in target_columns:
            analyzed = analyze_column(t, "VARCHAR", [])
            tgt_roles[t] = analyzed["semantic_role"]
            if names_only_existing:
                tgt_types[t] = ""
                continue
            origin = src_by_folded.get(_folded_ident(t))
            projected = ""
            if origin and destination_table_exists is False and dest_db:
                projected = str(
                    create_new_mapping_target_type(
                        src_types.get(origin, "VARCHAR"),
                        dest_db,
                        samples=src_samples.get(origin),
                        source_db=src_db,
                    )
                    or ""
                ).strip()
                if projected:
                    create_new_pairs[t] = origin
            tgt_types[t] = projected or "VARCHAR"

    if not target_columns:
        # Empty targets are NOT automatically create-new. Only invent CREATE when
        # the destination object is confirmed missing. Existing/unknown + empty
        # columns = pending schema (shared SQL/warehouse failure mode).
        out: list[dict] = []
        confirmed_missing = destination_table_exists is False
        for src in source_columns:
            new_name = create_new_target_name(src)
            src_type = src_types.get(src, "VARCHAR")
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            map_target_type = create_new_mapping_target_type(
                src_type, dest_db, samples=src_samples.get(src), source_db=src_db
            )
            why_type = _create_new_physical_why_type(
                src_type, map_target_type or dest_native, dest_db
            )
            if confirmed_missing:
                classified = classify_conversion(
                    src_type,
                    why_type,
                    dest_db=dest_db,
                    transform="none",
                )
                out.append(
                    {
                        "source": src,
                        "target": new_name,
                        "semantic_name": _semantic_form(src),
                        "confidence": IDENTITY_PASSTHROUGH_CONFIDENCE,
                        "reasoning": create_new_mapping_reason(
                            src_type, why_type, dest_db=dest_db
                        ),
                        "user_override": False,
                        "requires_review": True,
                        "source_type": src_type,
                        "target_type": map_target_type,
                        "assignment_strategy": "identity_passthrough",
                        "create_new": True,
                        "conversion_class": classified.get("conversion_class"),
                        "semantic_role": src_roles.get(src),
                    }
                )
            else:
                exists_note = (
                    "Destination table exists but column metadata did not load"
                    if destination_table_exists is True
                    else "Destination schema unavailable — not treating as create-new"
                )
                out.append(
                    {
                        "source": src,
                        "target": new_name,
                        "semantic_name": _semantic_form(src),
                        "confidence": 0.55,
                        "reasoning": (
                            f"{exists_note}. Retry destination schema load before Map "
                            "invents CREATE TABLE / identity passthrough "
                            f"(projected type {dest_native} is advisory only — "
                            "target_type left empty until Studio/Map stamp)."
                        ),
                        "user_override": False,
                        "source_type": src_type,
                        # Never stamp source/projected DDL as dest — Validate would
                        # invent fidelity greens under partial Studio.
                        "target_type": "",
                        "assignment_strategy": "pending_dest_schema",
                        "create_new": False,
                        "requires_review": True,
                    }
                )
        return _stamp_review_kinds(
            _apply_create_new_risk_stamps(out, dest_db, source_db_type=src_db)
        )

    idf = _build_idf(source_columns + target_columns)
    all_doc_lens = [len(_tokenize(c)) for c in source_columns + target_columns]
    avgdl = sum(all_doc_lens) / max(len(all_doc_lens), 1)
    used_targets: set[str] = set()
    mappings: list[dict] = []

    pair_scores: dict[tuple[str, str], tuple[float, str]] = {}
    for source in source_columns:
        for target in target_columns:
            score, reason = _score_pair(
                source,
                target,
                idf,
                avgdl,
                src_roles.get(source),
                tgt_roles.get(target),
                src_types.get(source, "VARCHAR"),
                tgt_types.get(target, "VARCHAR"),
                src_samples.get(source),
                dest_db=dest_db,
                create_new_target=create_new_pairs.get(target) == source,
            )
            pair_scores[(source, target)] = (score, reason)

    assigned_sources: set[str] = set()
    dest_collisions = _dest_fold_collisions(target_columns)
    optimal = _optimal_assignment(source_columns, target_columns, pair_scores)
    for source in source_columns:
        assigned = optimal.get(source)
        if not assigned:
            continue
        target, score, reason = assigned
        if score < floor:
            continue
        alternatives = _alternatives(source, target_columns, pair_scores)
        winner = alternatives[0]["confidence"] if alternatives else score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        score_gap = round(max(winner - runner_up, 0.0), 3)
        requires_review = score_gap < 0.08
        src_type = src_types.get(source, "VARCHAR")
        tgt_type = tgt_types.get(target, "VARCHAR")
        try:
            from services.decision_kernel import is_lossy_coercion

            lossy_pair = create_new_pairs.get(target) != source and is_lossy_coercion(
                src_type, tgt_type, dest_db=dest_db
            )
        except Exception:
            # Fail closed — unknown type authority must not green-path remaps.
            lossy_pair = True
        if lossy_pair:
            # Exact-name must not exempt DECIMAL→INTEGER / DATETIME→DATE remaps.
            requires_review = True
            score = min(float(score), 0.84)
            reason = f"{reason} · lossy type pair"
        elif reason.startswith("Exact name match") and _exact_name_unambiguous(
            source, target, target_columns
        ):
            # Unique name equality with compatible types — nothing to review.
            requires_review = False
        elif reason.startswith("Exact") and score_gap >= 0.08:
            # Decisive Exact with compatible types — review not required.
            requires_review = False
        if (
            _reason_forces_review(reason)
            or _identity_leaf_mismatch(source, target)
            or _measure_kind_mismatch(source, target)
            or _entity_conflict_requires_review(source, target)
        ):
            requires_review = True
        if target in dest_collisions:
            requires_review = True
            score = min(float(score), _AMBIGUOUS_PAIR_CAP)
            reason = f"{reason} · destination identifier collision — review required"
        assigned_sources.add(source)
        used_targets.add(target)
        mappings.append(
            {
                "source": source,
                "target": target,
                "confidence": _calibrated_confidence(
                    score, score_gap=score_gap, requires_review=requires_review,
                ),
                "reasoning": reason,
                "user_override": False,
                # May be relabeled to hungarian_with_greedy_patch after later passes.
                "assignment_strategy": "optimal_bipartite_hungarian",
                "alternatives": alternatives,
                "score_gap": score_gap,
                "requires_review": requires_review,
                "source_type": src_type,
                "target_type": tgt_type,
                **(
                    {"create_new": True}
                    if create_new_pairs.get(target) == source
                    else {}
                ),
            }
        )

    # Audit §4.1 — greedy / near-form patches mean the global solution is no
    # longer pure Hungarian; never keep the "optimal" label if we patch.
    greedy_patched = False

    for source in source_columns:
        if source in assigned_sources:
            continue
        best_target = ""
        best_score = 0.0
        best_reason = ""
        for target in target_columns:
            if target in used_targets:
                continue
            score, reason = _score_pair(
                source,
                target,
                idf,
                avgdl,
                src_roles.get(source),
                tgt_roles.get(target),
                src_types.get(source, "VARCHAR"),
                tgt_types.get(target, "VARCHAR"),
                src_samples.get(source),
                dest_db=dest_db,
                create_new_target=create_new_pairs.get(target) == source,
            )
            if score > best_score:
                best_score, best_target, best_reason = score, target, reason
        alternatives = _alternatives(source, target_columns, pair_scores)
        src_type = src_types.get(source, "VARCHAR")
        # Prefer a near-matching existing destination over inventing a column
        # when abbreviation/form similarity is strong enough (ph_num → phone).
        # Never override a hard type/sample demotion (ObjectId → DECIMAL id).
        near_tgt, near_ratio = _near_target_by_form(source, target_columns, used_targets=used_targets)
        if near_tgt:
            near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
            near_penalty = _type_compat_penalty(
                src_type, near_tgt_type, source_name=source, dest_db=dest_db
            )
            near_sample = _sample_consistency_boost(
                src_samples.get(source), src_type, near_tgt_type,
            )
            # Only hard landmines (ObjectId→DECIMAL ≈0.92) clear near-form.
            # Temporal polarity demotion (BQ TIMESTAMP ntz→instant ≈0.5) must still
            # prefer the synonym column with review over inventing DATETIME.
            if near_penalty >= 0.85 or near_sample <= -0.50:
                near_tgt, near_ratio = "", 0.0
        if near_tgt and near_ratio >= 0.62 and (not best_target or best_score < floor or near_ratio > best_score):
            # Promote near form match into the assignment set.
            near_score = max(best_score, 0.55 + near_ratio * 0.40)
            if near_score >= floor or near_ratio >= 0.70:
                greedy_patched = True
                used_targets.add(near_tgt)
                assigned_sources.add(source)
                winner = alternatives[0]["confidence"] if alternatives else near_score
                runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
                score_gap = round(max(winner - runner_up, 0.0), 3)
                requires_review = near_ratio < 0.85
                near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
                try:
                    from services.decision_kernel import is_lossy_coercion

                    near_lossy = is_lossy_coercion(src_type, near_tgt_type, dest_db=dest_db)
                except Exception:
                    near_lossy = True
                if near_lossy:
                    requires_review = True
                    near_score = min(float(near_score), 0.84)
                if (
                    _identity_leaf_mismatch(source, near_tgt)
                    or _measure_kind_mismatch(source, near_tgt)
                    or _entity_conflict_requires_review(source, near_tgt)
                ):
                    requires_review = True
                    near_score = min(float(near_score), _AMBIGUOUS_PAIR_CAP)
                if near_tgt in dest_collisions:
                    requires_review = True
                    near_score = min(float(near_score), _AMBIGUOUS_PAIR_CAP)
                mappings.append(
                    {
                        "source": source,
                        "target": near_tgt,
                        "confidence": _calibrated_confidence(
                            near_score,
                            score_gap=score_gap,
                            requires_review=requires_review,
                            hard_cap=0.97,
                        ),
                        "reasoning": (
                            f"Near-form match to existing destination "
                            f"(similarity={near_ratio:.2f}); prefer over inventing a column"
                            + (" · lossy type pair" if near_lossy else "")
                        ),
                        "user_override": False,
                        "assignment_strategy": "near_form_existing",
                        "alternatives": alternatives,
                        "score_gap": score_gap,
                        "requires_review": requires_review,
                        "source_type": src_type,
                        "target_type": near_tgt_type,
                    }
                )
                continue
        # Prefer create-new text column over a lossy existing target (e.g. ObjectId → DECIMAL).
        if (not best_target or best_score < floor) and target_columns:
            # Existing table + names-only (no typed schema) — refuse invent ADD COLUMN.
            if destination_table_exists is True and not target_schemas:
                greedy_patched = True
                mappings.append(
                    {
                        "source": source,
                        "target": create_new_target_name(source),
                        "semantic_name": _semantic_form(source),
                        "confidence": 0.55,
                        "reasoning": (
                            "Destination table exists but column types were not loaded — "
                            "retry destination schema introspect before inventing ADD COLUMN "
                            "or create-compatible carriers."
                        ),
                        "user_override": False,
                        "source_type": src_types.get(source, "VARCHAR"),
                        # Empty dest stamp — never copy source_type (Validate invent cliff).
                        "target_type": "",
                        "assignment_strategy": "pending_dest_schema",
                        "create_new": False,
                        "requires_review": True,
                        "alternatives": alternatives,
                        "score_gap": 0.0,
                    }
                )
                continue
            # Final gate: if any unused dest is a reasonable form match, map there
            # with review instead of inventing (avoids ph_number when phone exists).
            if near_tgt and near_ratio >= 0.50:
                greedy_patched = True
                used_targets.add(near_tgt)
                near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
                try:
                    from services.decision_kernel import is_lossy_coercion

                    near_lossy = is_lossy_coercion(src_type, near_tgt_type, dest_db=dest_db)
                except Exception:
                    near_lossy = True
                mappings.append(
                    {
                        "source": source,
                        "target": near_tgt,
                        "confidence": round(
                            min(0.55 + near_ratio * 0.35, 0.84 if near_lossy else 0.88),
                            3,
                        ),
                        "reasoning": (
                            f"Sub-threshold score but near existing column "
                            f"(similarity={near_ratio:.2f}) — review before create-new"
                            + (" · lossy type pair" if near_lossy else "")
                        ),
                        "user_override": False,
                        "assignment_strategy": "near_form_review",
                        "alternatives": alternatives,
                        "score_gap": 0.0,
                        "requires_review": True,
                        "source_type": src_type,
                        "target_type": near_tgt_type,
                    }
                )
                continue
            greedy_patched = True
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            map_target_type = create_new_mapping_target_type(
                src_type, dest_db, samples=src_samples.get(source), source_db=src_db
            )
            # Prefer the original source name for ADD COLUMN (_id stays _id).
            # Semantic form alone collapses _id → id, then id_text — a name that
            # operators did not approve and that often never gets DDL.
            taken = {t.lower() for t in used_targets} | {t.lower() for t in target_columns}
            candidate = source.strip() or _semantic_form(source)
            if candidate.lower() in taken:
                sem = _semantic_form(source)
                candidate = sem if sem.lower() not in taken else candidate
            if candidate.lower() in taken:
                base = re.sub(r"[^A-Za-z0-9_]+", "_", candidate).strip("_") or "field"
                candidate = f"{base}_text" if f"{base}_text".lower() not in taken else f"src_{base}"
            used_targets.add(candidate)
            mappings.append(
                {
                    "source": source,
                    "target": candidate,
                    "confidence": _calibrated_confidence(
                        IDENTITY_PASSTHROUGH_CONFIDENCE,
                        score_gap=0.0,
                        requires_review=True,
                        hard_cap=0.84,
                    ),
                    "reasoning": (
                        "No type-compatible destination column — map to a new field "
                        f"(create/ADD as {dest_native}); do not coerce into incompatible DDL"
                    ),
                    "user_override": False,
                    "source_type": src_type,
                    "target_type": map_target_type,
                    "assignment_strategy": "create_compatible_new",
                    "create_new": True,
                    "alternatives": alternatives,
                    "score_gap": 0.0,
                    "requires_review": True,
                }
            )
            continue
        greedy_patched = True
        if not best_target:
            best_target = create_new_target_name(source)
            best_score = 0.55
            best_reason = "No target match — inferred semantic name (no destination schema)"
            alternatives = []
        else:
            used_targets.add(best_target)
        winner = alternatives[0]["confidence"] if alternatives else best_score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        score_gap = round(max(winner - runner_up, 0.0), 3)
        requires_review = score_gap < 0.08
        src_type = src_types.get(source, "VARCHAR")
        tgt_type = tgt_types.get(best_target, "VARCHAR") if best_target else "VARCHAR"
        try:
            from services.decision_kernel import is_lossy_coercion

            lossy_pair = bool(
                best_target
                and create_new_pairs.get(best_target) != source
                and is_lossy_coercion(src_type, tgt_type, dest_db=dest_db)
            )
        except Exception:
            lossy_pair = bool(best_target)
        if lossy_pair:
            requires_review = True
            best_score = min(float(best_score), 0.84)
            best_reason = f"{best_reason} · lossy type pair"
        elif best_target and _exact_name_unambiguous(
            source, best_target, target_columns
        ):
            requires_review = False
        if best_target and (
            _reason_forces_review(best_reason)
            or _identity_leaf_mismatch(source, best_target)
            or _measure_kind_mismatch(source, best_target)
            or _entity_conflict_requires_review(source, best_target)
            or best_target in dest_collisions
        ):
            requires_review = True
            best_score = min(float(best_score), _AMBIGUOUS_PAIR_CAP)
        mappings.append(
            {
                "source": source,
                "target": best_target,
                "confidence": _calibrated_confidence(
                    max(best_score, 0.55),
                    score_gap=score_gap,
                    requires_review=requires_review,
                ),
                "reasoning": best_reason,
                "user_override": False,
                "assignment_strategy": "fallback_best_available",
                "alternatives": alternatives,
                "score_gap": score_gap,
                "requires_review": requires_review,
                "source_type": src_type,
                "target_type": tgt_type,
            }
        )

    if greedy_patched:
        for row in mappings:
            if row.get("assignment_strategy") == "optimal_bipartite_hungarian":
                row["assignment_strategy"] = "hungarian_with_greedy_patch"

    mappings.sort(key=lambda m: source_columns.index(m["source"]))
    return _stamp_review_kinds(
        _apply_create_new_risk_stamps(
            mappings, dest_db, source_samples=src_samples, source_db_type=src_db
        ),
        dest_collisions,
    )
