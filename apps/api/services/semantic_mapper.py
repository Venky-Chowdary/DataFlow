"""Hybrid semantic column mapper — BM25 lexical retrieval + semantic token graph."""

from __future__ import annotations

import math
import re
from collections import Counter
from difflib import SequenceMatcher
import pickle
import sys
from pathlib import Path

_model_cache = None

def _load_ml_baseline():
    global _model_cache
    if _model_cache is not None:
        return _model_cache
    
    # Try to load the ML baseline model if it exists
    try:
        model_path = Path(__file__).resolve().parents[3] / "packages" / "ml" / "models" / "baseline.pkl"
        if model_path.exists():
            # Adjust path so that baseline class can be loaded
            pkg_path = str(Path(__file__).resolve().parents[3] / "packages")
            if pkg_path not in sys.path:
                sys.path.append(pkg_path)
            import ml.train.baseline  # to ensure class is available for unpickling
            with model_path.open("rb") as f:
                _model_cache = pickle.load(f)
                return _model_cache
    except Exception:
        pass
    return None


ABBREVIATIONS: dict[str, str] = {
    "amt": "amount",
    "amount": "amount",
    "salary_amt": "salary_amount",
    "salary": "salary_amount",
    "pay_amt": "payment_amount",
    "pmt": "payment",
    "pymt": "payment",
    "pay": "payment",
    "qty": "quantity",
    "dt": "date",
    "txn": "transaction",
    "txn_dt": "transaction_date",
    "trans_dt": "transaction_date",
    "cust": "customer",
    "cust_id": "customer_id",
    "acct": "account",
    "acct_no": "account_number",
    "acct_num": "account_number",
    "desc": "description",
    "descr": "description",
    "nm": "name",
    "fname": "first_name",
    "lname": "last_name",
    "addr": "address",
    "zip": "postal_code",
    "curr": "currency",
    "ccy": "currency_code",
    "ref": "reference",
    "ref_no": "reference_number",
    "inv": "invoice",
    "inv_no": "invoice_number",
    "sku": "product_sku",
    "prod": "product",
    "sts": "status",
    "stat": "status",
    "bal": "balance",
    "tot": "total",
    "subtot": "subtotal",
    "tax_amt": "tax_amount",
    "disc": "discount",
    "emp": "employee",
    "emp_id": "employee_id",
    "full_name": "full_name",
    "dept": "department",
    "dept_code": "department_code",
    "hire_dt": "hire_date",
    "loc": "location",
    "src": "source",
    "tgt": "target",
    "ts": "timestamp",
    "created": "created_at",
    "updated": "updated_at",
    "mod_dt": "modified_at",
}


def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _expand_abbrev(token: str) -> str:
    return ABBREVIATIONS.get(token, token)


def _semantic_tokens(name: str) -> list[str]:
    norm = _normalize(name)
    parts = norm.split("_")
    return [_expand_abbrev(p) for p in parts]


def _semantic_form(name: str) -> str:
    return "_".join(_semantic_tokens(name))


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
    return _canonical_form(name).split("_")


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


def _type_compat_penalty(src_type: str, tgt_type: str) -> float:
    """Reduce score for incompatible type pairs."""
    s, t = src_type.upper(), tgt_type.upper()
    text_types = {"TEXT", "CLOB", "LONGTEXT", "BLOB", "BYTEA", "BINARY", "VARCHAR", "STRING"}
    numeric_types = {"INTEGER", "DECIMAL", "FLOAT", "NUMBER", "NUMERIC"}
    date_types = {"DATE", "TIMESTAMP"}
    
    if s in text_types and t in date_types:
        return 0.35
    if s in {"BINARY", "BLOB", "BYTEA"} and t not in text_types | {"BINARY", "BLOB", "BYTEA"}:
        return 0.4
    if s in text_types and t in numeric_types:
        return 0.25  # text to numeric needs parsing, slightly less ideal
        
    return 0.0

def _type_aware_boost(src_type: str, tgt_type: str) -> float:
    """Boost score for exact or highly compatible type matches."""
    s, t = src_type.upper(), tgt_type.upper()
    if s == t:
        return 0.05
    
    numeric_types = {"INTEGER", "DECIMAL", "FLOAT", "NUMBER", "NUMERIC"}
    date_types = {"DATE", "TIMESTAMP"}
    text_types = {"VARCHAR", "STRING", "TEXT"}
    
    if s in numeric_types and t in numeric_types:
        return 0.03
    if s in date_types and t in date_types:
        return 0.03
    if s in text_types and t in text_types:
        return 0.02
        
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
) -> tuple[float, str]:
    from services.semantic_analyzer import role_match_boost
    from services.training_lexicon import lexicon_boost

    src_norm = _normalize(source)
    tgt_norm = _normalize(target)
    src_sem = _semantic_form(source)
    tgt_sem_raw = _semantic_form(target)

    if src_norm == tgt_norm:
        return 0.995, "Exact name match"
    if src_sem == tgt_sem_raw:
        return 0.975, "Exact semantic token match"

    schematic = None
    try:
        from services.schematic_index import schematic_match_boost
        schematic = schematic_match_boost(source, target)
    except ImportError:
        pass
    if schematic is not None:
        return schematic, "Schematic index match (1M+ variants)"

    src_canon = _canonical_form(source)
    tgt_canon = _canonical_form(target)
    expanded = _semantic_form(source)
    if src_canon and tgt_canon and src_canon == tgt_canon:
        if _normalize(target) == src_canon:
            return 0.99, "Canonical schematic resolution (exact target)"
        if _normalize(target) == _normalize(expanded):
            return 0.985, "Canonical schematic resolution (expanded form)"
        return 0.93, "Canonical schematic resolution"

    if _normalize(target) == _normalize(expanded):
        return 0.94, "Abbreviation expansion match"

    if source_role and target_role:
        boost = role_match_boost(source_role, target_role)
        if boost is not None:
            return boost, f"Semantic role match: {source_role} → {target_role}"

    boosted = lexicon_boost(source, target)
    if boosted is not None:
        return boosted, "Training lexicon match (synthetic_v1)"

    src_sem = _canonical_form(source)
    tgt_sem = _canonical_form(target)

    if src_sem == tgt_sem:
        return 0.96, "Semantic token match"

    bm25 = _bm25_score(_tokenize(source), _tokenize(target), idf, avgdl)
    bm25_norm = min(bm25 / 8.0, 1.0)
    
    # Advanced heuristic: ML Baseline prediction
    ml_model = _load_ml_baseline()
    ml_boost = 0.0
    if ml_model:
        pred_tgt, pred_score = ml_model.predict_target(source)
        if _normalize(pred_tgt) == tgt_norm and pred_score > 0.5:
            ml_boost = min(pred_score * 0.15, 0.15)
            if pred_score > 0.8:
                return 0.95, "ML Baseline highly confident match"

    type_penalty = _type_compat_penalty(source_type, target_type)
    type_boost = _type_aware_boost(source_type, target_type)

    if src_sem in tgt_sem or tgt_sem in src_sem:
        if min(len(src_sem), len(tgt_sem)) >= 4:
            return max(0.92, 0.85 + bm25_norm * 0.1) - type_penalty + type_boost, "Partial semantic overlap + BM25"

    overlap = len(set(src_sem.split("_")) & set(tgt_sem.split("_")))
    if overlap >= 2:
        return 0.82 + overlap * 0.03 + bm25_norm * 0.05 - type_penalty + type_boost + ml_boost, f"Shared tokens ({overlap}) + BM25"

    fuzzy = _similarity(src_sem, tgt_sem)
    
    # Advanced heuristic: Character n-gram Jaccard (n=3)
    def ngrams(s, n):
        return set(s[i:i+n] for i in range(max(1, len(s)-n+1)))
    jaccard = 0.0
    s_ngrams, t_ngrams = ngrams(src_sem, 3), ngrams(tgt_sem, 3)
    if s_ngrams or t_ngrams:
        jaccard = len(s_ngrams & t_ngrams) / len(s_ngrams | t_ngrams)

    combined = max(fuzzy * 0.75, bm25_norm * 0.88, jaccard * 0.82) - type_penalty + type_boost + ml_boost
    
    if combined >= 0.78:
        return min(combined, 0.99), "BM25 / Jaccard lexical retrieval"
    if overlap == 1 and len(src_sem.split("_")) > 1:
        return min(0.78 - type_penalty + type_boost + ml_boost, 0.99), "Single token overlap"
    return min(combined, 0.99), "Character similarity"


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


def map_columns(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    threshold: float = 0.85,
) -> list[dict]:
    from services.semantic_analyzer import analyze_column

    floor = max(0.55, threshold - 0.3)
    src_roles: dict[str, str] = {}
    tgt_roles: dict[str, str] = {}
    src_types: dict[str, str] = {}
    tgt_types: dict[str, str] = {}

    if source_schemas:
        for s in source_schemas:
            analyzed = analyze_column(s.get("name", ""), s.get("inferred_type", "VARCHAR"), s.get("samples", []))
            src_roles[s["name"]] = analyzed["semantic_role"]
            src_types[s["name"]] = s.get("inferred_type", "VARCHAR")
    if target_schemas:
        for t in target_schemas:
            analyzed = analyze_column(t.get("name", ""), t.get("inferred_type", "VARCHAR"), t.get("samples", []))
            tgt_roles[t["name"]] = analyzed["semantic_role"]
            tgt_types[t["name"]] = t.get("inferred_type", "VARCHAR")
    elif target_columns:
        for t in target_columns:
            analyzed = analyze_column(t, "VARCHAR", [])
            tgt_roles[t] = analyzed["semantic_role"]
            tgt_types[t] = "VARCHAR"

    if not target_columns:
        return [
            {
                "source": src,
                "target": _semantic_form(src),
                "confidence": 0.72,
                "reasoning": "Inferred target name from semantic expansion",
                "user_override": False,
            }
            for src in source_columns
        ]

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
            )
            pair_scores[(source, target)] = (score, reason)

    assigned_sources: set[str] = set()
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
        assigned_sources.add(source)
        used_targets.add(target)
        mappings.append(
            {
                "source": source,
                "target": target,
                "confidence": round(min(score, 0.99), 3),
                "reasoning": reason,
                "user_override": False,
                "assignment_strategy": "optimal_bipartite_hungarian",
                "alternatives": alternatives,
                "score_gap": round(max(winner - runner_up, 0.0), 3),
                "requires_review": (winner - runner_up) < 0.08 and not reason.startswith("Exact"),
            }
        )

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
            )
            if score > best_score:
                best_score, best_target, best_reason = score, target, reason
        alternatives = _alternatives(source, target_columns, pair_scores)
        if not best_target:
            best_target = _semantic_form(source)
            best_score = 0.65
            best_reason = "No target match — inferred semantic name"
            alternatives = []
        else:
            used_targets.add(best_target)
        winner = alternatives[0]["confidence"] if alternatives else best_score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        mappings.append(
            {
                "source": source,
                "target": best_target,
                "confidence": round(min(max(best_score, 0.65), 0.99), 3),
                "reasoning": best_reason,
                "user_override": False,
                "assignment_strategy": "fallback_best_available",
                "alternatives": alternatives,
                "score_gap": round(max(winner - runner_up, 0.0), 3),
                "requires_review": (winner - runner_up) < 0.08 and not best_reason.startswith("Exact"),
            }
        )

    mappings.sort(key=lambda m: source_columns.index(m["source"]))
    return mappings
