"""Hybrid semantic column mapper — BM25 lexical retrieval + semantic token graph."""

from __future__ import annotations

import math
import pickle
import re
import sys
from collections import Counter
from difflib import SequenceMatcher
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
            with model_path.open("rb") as f:
                _model_cache = pickle.load(f)
                return _model_cache
    except Exception:
        pass
    return None


ABBREVIATIONS: dict[str, str] = {
    # Amounts and quantities
    "amt": "amount",
    "amount": "amount",
    "salary": "salary_amount",
    "salary_amt": "salary_amount",
    "salary_amount": "salary_amount",
    "pay": "payment",
    "pmt": "payment",
    "pymt": "payment",
    "pay_amt": "payment_amount",
    "payment_amount": "payment_amount",
    "tax": "tax",
    "tax_amt": "tax_amount",
    "tax_amount": "tax_amount",
    "net": "net",
    "net_amt": "net_amount",
    "net_amount": "net_amount",
    "gross": "gross",
    "gross_amt": "gross_amount",
    "gross_amount": "gross_amount",
    "line": "line",
    "line_amt": "line_amount",
    "line_amount": "line_amount",
    "bal": "balance",
    "balance": "balance",
    "tot": "total",
    "total": "total",
    "subtot": "subtotal",
    "subtotal": "subtotal",
    "disc": "discount",
    "discount": "discount",
    "qty": "quantity",
    "quantity": "quantity",
    "qty_ord": "quantity_ordered",
    "quantity_ordered": "quantity_ordered",
    "price": "price",
    "prc": "price",
    "unit_prc": "unit_price",
    "unit_price": "unit_price",
    "cost": "cost",
    "unit_cost": "unit_cost",
    # Dates and timestamps
    "dt": "date",
    "date": "date",
    "ts": "timestamp",
    "timestamp": "timestamp",
    "created": "created",
    "created_at": "created_at",
    "created_dt": "created_at",
    "created_date": "created_at",
    "created_ts": "created_timestamp",
    "created_timestamp": "created_timestamp",
    "updated": "updated",
    "updated_at": "updated_at",
    "updated_dt": "updated_at",
    "updated_date": "updated_at",
    "updated_ts": "updated_timestamp",
    "updated_timestamp": "updated_timestamp",
    "mod": "modified",
    "modified": "modified",
    "modified_at": "modified_at",
    "mod_at": "modified_at",
    "mod_dt": "modified_at",
    "modified_dt": "modified_at",
    "modified_date": "modified_at",
    "modified_ts": "modified_timestamp",
    "modified_timestamp": "modified_timestamp",
    "txn": "transaction",
    "transaction": "transaction",
    "txn_dt": "transaction_date",
    "transaction_date": "transaction_date",
    "txn_id": "transaction_id",
    "transaction_id": "transaction_id",
    "trans_dt": "transaction_date",
    "trans_date": "transaction_date",
    "hire_dt": "hire_date",
    "hire_date": "hire_date",
    "ship_dt": "ship_date",
    "ship_date": "ship_date",
    "del": "delivery",
    "delivery": "delivery",
    "del_dt": "delivery_date",
    "delivery_date": "delivery_date",
    "pay_dt": "payment_date",
    "payment_dt": "payment_date",
    "payment_date": "payment_date",
    # Identifiers and customers
    "no": "number",
    "num": "number",
    "nbr": "number",
    "nr": "number",
    "number": "number",
    "ref": "reference",
    "reference": "reference",
    "ref_no": "reference_number",
    "reference_number": "reference_number",
    "inv": "invoice",
    "invoice": "invoice",
    "inv_no": "invoice_number",
    "invoice_number": "invoice_number",
    "ord": "order",
    "order": "order",
    "ord_id": "order_id",
    "order_id": "order_id",
    "order_no": "order_number",
    "order_number": "order_number",
    "cust": "customer",
    "customer": "customer",
    "cust_id": "customer_id",
    "customer_id": "customer_id",
    "cust_nm": "customer_name",
    "customer_name": "customer_name",
    "acct": "account",
    "account": "account",
    "acct_no": "account_number",
    "acct_num": "account_number",
    "account_number": "account_number",
    "emp": "employee",
    "employee": "employee",
    "emp_id": "employee_id",
    "employee_id": "employee_id",
    "dept": "department",
    "department": "department",
    "dept_code": "department_code",
    "department_code": "department_code",
    "product": "product",
    "prod": "product",
    "prod_id": "product_id",
    "product_id": "product_id",
    "sku": "product_sku",
    "product_sku": "product_sku",
    "src": "source",
    "source": "source",
    "tgt": "target",
    "target": "target",
    "loc": "location",
    "location": "location",
    # Names and contact
    "nm": "name",
    "name": "name",
    "fname": "first_name",
    "first_name": "first_name",
    "lname": "last_name",
    "last_name": "last_name",
    "full_name": "full_name",
    "desc": "description",
    "descr": "description",
    "description": "description",
    "addr": "address",
    "address": "address",
    "email": "email_address",
    "e_mail": "email_address",
    "email_address": "email_address",
    "usr": "user",
    "user": "user",
    "phone": "phone",
    "tel": "phone",
    "phone_number": "phone_number",
    "mobile": "mobile",
    "mob": "mobile",
    "cell": "mobile",
    "mobile_number": "mobile_number",
    # Status and location
    "sts": "status",
    "stat": "status",
    "status": "status",
    "zip": "postal_code",
    "zipcode": "postal_code",
    "postal": "postal_code",
    "postal_code": "postal_code",
    "country": "country",
    "country_cd": "country_code",
    "country_code": "country_code",
    "cntry": "country",
    "state": "state",
    "state_code": "state_code",
    "province": "province",
    "province_code": "province_code",
    "city": "city",
    "city_name": "city_name",
    "region": "region",
    "region_code": "region_code",
    "curr": "currency",
    "currency": "currency",
    "ccy": "currency_code",
    "curr_cd": "currency_code",
    "iso_curr": "currency_code",
    "currency_code": "currency_code",
}


def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).rstrip("_")


def _expand_abbrev(token: str) -> str:
    return ABBREVIATIONS.get(token, token)


def _semantic_tokens(name: str) -> list[str]:
    norm = _normalize(name)
    parts = norm.split("_")
    tokens: list[str] = []
    i = 0
    # Match longest abbreviation phrase first so multi-token abbreviations like
    # "txn_dt" or "created_at" resolve to their canonical form.
    while i < len(parts):
        matched = False
        for j in range(len(parts), i, -1):
            phrase = "_".join(parts[i:j])
            if phrase in ABBREVIATIONS:
                tokens.append(ABBREVIATIONS[phrase])
                i = j
                matched = True
                break
        if not matched:
            tokens.append(_expand_abbrev(parts[i]))
            i += 1
    return tokens


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
    """Reduce score for incompatible type pairs using the canonical type-system rules."""
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    if is_lossy_coercion(src_type, tgt_type):
        src = normalize_logical_type(src_type)
        tgt = normalize_logical_type(tgt_type)
        if src == "binary" and tgt != "binary":
            return 0.4
        if src in ("json", "array") and tgt in ("integer", "decimal", "boolean", "date", "datetime", "time", "binary", "uuid"):
            return 0.35
        if src in ("decimal",) and tgt == "integer":
            return 0.15
        return 0.25
    return 0.0

def _type_aware_boost(src_type: str, tgt_type: str) -> float:
    """Boost score for exact or highly compatible type matches."""
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    src = normalize_logical_type(src_type)
    tgt = normalize_logical_type(tgt_type)
    if src == tgt:
        return 0.05
    if is_lossy_coercion(src_type, tgt_type):
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
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    transform = infer_transform_for_mapping("col", "col", source_type, target_type)
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
) -> tuple[float, str]:
    from services.semantic_analyzer import role_match_boost
    from services.training_lexicon import lexicon_boost

    src_norm = _normalize(source)
    tgt_norm = _normalize(target)
    src_sem = _semantic_form(source)
    tgt_sem_raw = _semantic_form(target)

    type_penalty = _type_compat_penalty(source_type, target_type)
    type_boost = _type_aware_boost(source_type, target_type)
    sample_boost = _sample_consistency_boost(source_samples, source_type, target_type)

    def _finish(score: float, reason: str) -> tuple[float, str]:
        adjusted = max(0.0, min(0.995, float(score) - type_penalty + type_boost + sample_boost))
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
        return _finish(schematic, "Schematic index match (1M+ variants)")

    src_canon = _canonical_form(source)
    tgt_canon = _canonical_form(target)
    expanded = _semantic_form(source)
    if src_canon and tgt_canon and src_canon == tgt_canon:
        if _normalize(target) == src_canon:
            return _finish(0.99, "Canonical schematic resolution (exact target)")
        if _normalize(target) == _normalize(expanded):
            return _finish(0.985, "Canonical schematic resolution (expanded form)")
        return _finish(0.93, "Canonical schematic resolution")

    if _normalize(target) == _normalize(expanded):
        return _finish(0.94, "Abbreviation expansion match")

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

    src_sem = _canonical_form(source)
    tgt_sem = _canonical_form(target)

    if src_sem == tgt_sem:
        return _finish(0.96, "Semantic token match")

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
                return _finish(0.95, "ML Baseline highly confident match")

    if src_sem in tgt_sem or tgt_sem in src_sem:
        if min(len(src_sem), len(tgt_sem)) >= 4:
            return _finish(max(0.92, 0.85 + bm25_norm * 0.1), "Partial semantic overlap + BM25")

    overlap = len(set(src_sem.split("_")) & set(tgt_sem.split("_")))
    if overlap >= 2:
        return _finish(0.82 + overlap * 0.03 + bm25_norm * 0.05 + ml_boost, f"Shared tokens ({overlap}) + BM25")

    fuzzy = _similarity(src_sem, tgt_sem)

    # Advanced heuristic: Character n-gram Jaccard (n=3)
    def ngrams(s, n):
        return set(s[i:i+n] for i in range(max(1, len(s)-n+1)))
    jaccard = 0.0
    s_ngrams, t_ngrams = ngrams(src_sem, 3), ngrams(tgt_sem, 3)
    if s_ngrams or t_ngrams:
        jaccard = len(s_ngrams & t_ngrams) / len(s_ngrams | t_ngrams)

    combined = max(fuzzy * 0.75, bm25_norm * 0.88, jaccard * 0.82) + ml_boost

    if combined >= 0.78:
        return _finish(min(combined, 0.99), "BM25 / Jaccard lexical retrieval")
    if overlap == 1 and len(src_sem.split("_")) > 1:
        return _finish(min(0.78 + ml_boost, 0.99), "Single token overlap")
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
# Reserve 0.99 for existing-dest exact+sample match.
IDENTITY_PASSTHROUGH_CONFIDENCE = 0.92


def map_columns(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    threshold: float = 0.85,
    destination_db_type: str = "",
) -> list[dict]:
    from services.semantic_analyzer import analyze_column
    from services.type_system import ddl_type

    floor = max(0.55, threshold - 0.3)
    src_roles: dict[str, str] = {}
    tgt_roles: dict[str, str] = {}
    src_types: dict[str, str] = {}
    tgt_types: dict[str, str] = {}
    src_samples: dict[str, list[str]] = {}
    dest_db = (destination_db_type or "").strip().lower()

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
        for t in target_columns:
            analyzed = analyze_column(t, "VARCHAR", [])
            tgt_roles[t] = analyzed["semantic_role"]
            tgt_types[t] = "VARCHAR"

    if not target_columns:
        # Destination schema is unknown/empty — identity passthrough for create-new.
        # Types are projected to destination-native DDL when dest family is known so
        # writers CREATE with accurate types (e.g. MySQL INT → Snowflake NUMBER(38,0)).
        out: list[dict] = []
        for src in source_columns:
            src_type = src_types.get(src, "VARCHAR")
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            out.append(
                {
                    "source": src,
                    "target": _semantic_form(src),
                    "confidence": IDENTITY_PASSTHROUGH_CONFIDENCE,
                    "reasoning": (
                        f"New destination table — identity mapping; "
                        f"types will CREATE on first write as {dest_native}"
                    ),
                    "user_override": False,
                    "source_type": src_type,
                    "target_type": dest_native,
                    "assignment_strategy": "identity_passthrough",
                    "create_new": True,
                }
            )
        return out

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
                src_samples.get(source),
            )
            if score > best_score:
                best_score, best_target, best_reason = score, target, reason
        alternatives = _alternatives(source, target_columns, pair_scores)
        src_type = src_types.get(source, "VARCHAR")
        # Prefer create-new text column over a lossy existing target (e.g. ObjectId → DECIMAL).
        if (not best_target or best_score < floor) and target_columns:
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            candidate = _semantic_form(source)
            taken = {t.lower() for t in used_targets} | {t.lower() for t in target_columns}
            if candidate.lower() in taken:
                candidate = f"{candidate}_text" if f"{candidate}_text".lower() not in taken else f"src_{candidate}"
            used_targets.add(candidate)
            mappings.append(
                {
                    "source": source,
                    "target": candidate,
                    "confidence": IDENTITY_PASSTHROUGH_CONFIDENCE,
                    "reasoning": (
                        "No type-compatible destination column — map to a new text field "
                        f"(create/ADD as {dest_native}); do not coerce into incompatible DDL"
                    ),
                    "user_override": False,
                    "source_type": src_type,
                    "target_type": dest_native,
                    "assignment_strategy": "create_compatible_new",
                    "create_new": True,
                    "alternatives": alternatives,
                    "score_gap": 0.0,
                    "requires_review": True,
                }
            )
            continue
        if not best_target:
            best_target = _semantic_form(source)
            best_score = 0.55
            best_reason = "No target match — inferred semantic name (no destination schema)"
            alternatives = []
        else:
            used_targets.add(best_target)
        winner = alternatives[0]["confidence"] if alternatives else best_score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        mappings.append(
            {
                "source": source,
                "target": best_target,
                "confidence": round(min(max(best_score, 0.55), 0.99), 3),
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
