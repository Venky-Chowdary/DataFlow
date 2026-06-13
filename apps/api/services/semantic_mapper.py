"""Hybrid semantic column mapper — BM25 lexical retrieval + semantic token graph."""

from __future__ import annotations

import math
import re
from collections import Counter
from difflib import SequenceMatcher

ABBREVIATIONS: dict[str, str] = {
    "amt": "amount",
    "amount": "amount",
    "pay_amt": "payment_amount",
    "pmt": "payment",
    "pymt": "payment",
    "pay": "payment",
    "qty": "quantity",
    "dt": "date",
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
    "dept": "department",
    "loc": "location",
    "src": "source",
    "tgt": "target",
    "ts": "timestamp",
    "created": "created_at",
    "updated": "updated_at",
    "mod_dt": "modified_at",
}


def _normalize(name: str) -> str:
    s = name.strip().lower()
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


def _tokenize(name: str) -> list[str]:
    return _semantic_form(name).split("_")


def _build_idf(corpus: list[str]) -> dict[str, float]:
    n = len(corpus)
    df: Counter[str] = Counter()
    for doc in corpus:
        for tok in set(_tokenize(doc)):
            df[tok] += 1
    return {tok: math.log((n + 1) / (freq + 1)) + 1.0 for tok, freq in df.items()}


def _bm25_score(query_tokens: list[str], doc_tokens: list[str], idf: dict[str, float], k1: float = 1.5, b: float = 0.75) -> float:
    if not query_tokens or not doc_tokens:
        return 0.0
    doc_len = len(doc_tokens)
    avgdl = max(doc_len, 1.0)
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


def _score_pair(
    source: str,
    target: str,
    idf: dict[str, float],
    source_role: str | None = None,
    target_role: str | None = None,
) -> tuple[float, str]:
    from services.semantic_analyzer import role_match_boost
    from services.training_lexicon import lexicon_boost

    if source_role and target_role:
        boost = role_match_boost(source_role, target_role)
        if boost is not None:
            return boost, f"Semantic role match: {source_role} → {target_role}"

    boosted = lexicon_boost(source, target)
    if boosted is not None:
        return boosted, "Training lexicon match (synthetic_v1)"

    src_norm = _normalize(source)
    tgt_norm = _normalize(target)
    src_sem = _semantic_form(source)
    tgt_sem = _semantic_form(target)

    if src_norm == tgt_norm:
        return 0.99, "Exact name match"
    if src_sem == tgt_sem:
        return 0.96, "Semantic token match"

    bm25 = _bm25_score(_tokenize(source), _tokenize(target), idf)
    bm25_norm = min(bm25 / 8.0, 1.0)

    if src_sem in tgt_sem or tgt_sem in src_sem:
        return max(0.92, 0.85 + bm25_norm * 0.1), "Partial semantic overlap + BM25"

    overlap = len(set(src_sem.split("_")) & set(tgt_sem.split("_")))
    if overlap >= 2:
        return 0.82 + overlap * 0.03 + bm25_norm * 0.05, f"Shared tokens ({overlap}) + BM25"

    fuzzy = _similarity(src_sem, tgt_sem)
    combined = max(fuzzy * 0.75, bm25_norm * 0.88)
    if combined >= 0.78:
        return combined, "BM25 lexical retrieval"
    if overlap == 1:
        return 0.78, "Single token overlap"
    return combined, "Character similarity"


def map_columns(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    threshold: float = 0.85,
) -> list[dict]:
    from services.semantic_analyzer import analyze_column

    del threshold
    src_roles: dict[str, str] = {}
    tgt_roles: dict[str, str] = {}

    if source_schemas:
        for s in source_schemas:
            analyzed = analyze_column(s.get("name", ""), s.get("inferred_type", "VARCHAR"), s.get("samples", []))
            src_roles[s["name"]] = analyzed["semantic_role"]
    if target_schemas:
        for t in target_schemas:
            analyzed = analyze_column(t.get("name", ""), t.get("inferred_type", "VARCHAR"), t.get("samples", []))
            tgt_roles[t["name"]] = analyzed["semantic_role"]
    elif target_columns:
        for t in target_columns:
            analyzed = analyze_column(t, "VARCHAR", [])
            tgt_roles[t] = analyzed["semantic_role"]

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
    used_targets: set[str] = set()
    mappings: list[dict] = []

    # Greedy assignment with Hungarian-like reorder: score all pairs first
    candidates: list[tuple[float, str, str, str]] = []
    for source in source_columns:
        for target in target_columns:
            score, reason = _score_pair(
                source,
                target,
                idf,
                src_roles.get(source),
                tgt_roles.get(target),
            )
            candidates.append((score, source, target, reason))
    candidates.sort(reverse=True)

    assigned_sources: set[str] = set()
    for score, source, target, reason in candidates:
        if source in assigned_sources or target in used_targets:
            continue
        if score < 0.55:
            continue
        assigned_sources.add(source)
        used_targets.add(target)
        mappings.append(
            {
                "source": source,
                "target": target,
                "confidence": round(min(score, 0.99), 3),
                "reasoning": reason,
                "user_override": False,
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
                src_roles.get(source),
                tgt_roles.get(target),
            )
            if score > best_score:
                best_score, best_target, best_reason = score, target, reason
        if not best_target:
            best_target = _semantic_form(source)
            best_score = 0.65
            best_reason = "No target match — inferred semantic name"
        else:
            used_targets.add(best_target)
        mappings.append(
            {
                "source": source,
                "target": best_target,
                "confidence": round(min(max(best_score, 0.65), 0.99), 3),
                "reasoning": best_reason,
                "user_override": False,
            }
        )

    mappings.sort(key=lambda m: source_columns.index(m["source"]))
    return mappings
