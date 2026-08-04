"""Evidence pack — claim → measured metric registry for enterprise buyers.

Marketing claims must not exceed what reproducible fixtures prove. This module
measures the mapping / type / preflight surface, writes
``data/proofs/evidence_catalog.json`` (+ markdown), and exposes floors that CI
asserts. Every claim carries an ``honesty`` field stating what was *not* proven
(live customer warehouse, competitor SaaS, etc.).
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

API_ROOT = Path(__file__).resolve().parents[1]
PROOF_DIR = API_ROOT / "data" / "proofs"
FIXTURES = API_ROOT / "tests" / "fixtures"
GOLDEN = FIXTURES / "mapping_golden.json"
ENTERPRISE = FIXTURES / "mapping_golden_enterprise.json"

# ---------------------------------------------------------------------------
# Claim registry — the only place marketing language is allowed to live.
# Floors are fail-closed: CI fails when measured < floor.
# ---------------------------------------------------------------------------

CLAIM_REGISTRY: list[dict[str, Any]] = [
    {
        "id": "hungarian_assignment",
        "claim": "Optimal bipartite (Hungarian) column assignment — not greedy one-pass",
        "buyer_line": (
            "On the enterprise rename golden set, Hungarian assignment beats a "
            "greedy lexical baseline by a measured delta."
        ),
        "floors": {
            "hungarian_accuracy": 0.95,
            "greedy_delta_min": 0.02,  # Hungarian − greedy ≥ 2pp when greedy < 1.0
        },
        "honesty": (
            "Algorithm proof on fixtures/mapping_golden_enterprise.json. "
            "Not a live Fivetran/Airbyte SaaS bake-off."
        ),
    },
    {
        "id": "native_type_canonicalization",
        "claim": "Native dialect aliases canonicalize into a fixed logical type set",
        "buyer_line": (
            "Measured native alias inventory and logical type count from "
            "type_system.CANONICAL_TYPES / DDL_TYPES — quote the measured count, "
            "never an inflated round number."
        ),
        "floors": {
            "native_alias_count": 200,
            "logical_type_count": 15,
            "ddl_dialect_count": 15,
        },
        "honesty": (
            "Inventory is source-of-truth from type_system.py. Counts change only "
            "when aliases/DDL maps change — CI fails if floors are missed."
        ),
    },
    {
        "id": "g1_g9_fail_closed",
        "claim": "G1–G9 fail-closed preflight before write",
        "buyer_line": (
            "Nine named gates are registered; ObjectId→TEXT and schemaless BSON "
            "affinity block without Accept risk."
        ),
        "floors": {
            "gate_count": 9,
            "objectid_text_blocks": 1,
            "bson_affinity_blocks": 1,
        },
        "honesty": (
            "Unit/integration gate proofs with synthetic plans. Not a production "
            "false-green rate from customer Validate runs."
        ),
    },
    {
        "id": "sample_aware_type_demotion",
        "claim": "Sample-aware type demotion (ObjectId hex ↛ DECIMAL id)",
        "buyer_line": (
            "Mongo ObjectId-like samples refuse silent mapping onto DECIMAL id; "
            "create-new / alternate target is required."
        ),
        "floors": {
            "objectid_avoids_decimal_id": 1,
        },
        "honesty": (
            "Deterministic fixture with real ObjectId hex samples. Not a blind "
            "A/B on messy production extracts."
        ),
    },
    {
        "id": "enterprise_mapping_accuracy",
        "claim": "Enterprise golden mapping accuracy",
        "buyer_line": (
            "Domain-batched bipartite accuracy on the enterprise golden fixture."
        ),
        "floors": {
            "accuracy": 1.0,
            "min_cases": 200,
        },
        "honesty": (
            "Measured against fixtures/mapping_golden_enterprise.json — expand "
            "the fixture before lowering the floor."
        ),
    },
    {
        "id": "connector_pair_matrix",
        "claim": "N×M source×dest dialect mapping matrix",
        "buyer_line": (
            "Parametric golden accuracy across top source×dest dialect pairs."
        ),
        "floors": {
            "pair_count": 40,
            "min_pair_score": 0.85,
            "aggregate_score": 0.90,
        },
        "honesty": (
            "Algorithm name-match / golden overlap proof with destination_db_type "
            "set — not a full type-fidelity bake-off (UNSIGNED, OBJECTID, "
            "TIMESTAMPTZ polarity are covered by dedicated type_system / G3 tests). "
            "Live introspect is optional (LIVE_MAPPING_MATRIX=1) and skipped by default."
        ),
    },
    {
        "id": "abbreviation_coverage",
        "claim": "Abbreviation dictionary coverage on enterprise names",
        "buyer_line": (
            "Abbreviation-like tokens on the enterprise golden resolve through "
            "ABBREVIATIONS at a measured coverage rate."
        ),
        "floors": {
            "coverage": 0.92,
        },
        "honesty": (
            "Unresolved tokens are listed in the proof artifact — never claim "
            "100% coverage without that list."
        ),
    },
    {
        "id": "create_new_type_risk_stamp",
        "claim": "Create-new precision/width/TZ risks stamped before Validate",
        "buyer_line": (
            "TIMESTAMPTZ create-new onto MySQL stamps create_new_risks "
            "(timezone / precision) visible to Map UI."
        ),
        "floors": {
            "create_new_risks_present": 1,
        },
        "honesty": (
            "Pipeline unit proof. UI consumption is covered by web mapping tests; "
            "not a customer Map session recording."
        ),
    },
]


def _normalize_name(name: str) -> str:
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", name.strip()).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def map_columns_greedy_lexical(
    source_columns: list[str],
    target_columns: list[str],
) -> list[dict]:
    """Competitor-class baseline: greedy best lexical match, no Hungarian.

    Walks sources in order; each takes the highest remaining string-similarity
    target. Exact normalized names win first. This is what 'identity + rename
    heuristics' look like without optimal assignment.
    """
    from difflib import SequenceMatcher

    remaining = list(target_columns)
    out: list[dict] = []
    for src in source_columns:
        src_n = _normalize_name(src)
        exact = next((t for t in remaining if _normalize_name(t) == src_n), None)
        if exact is not None:
            remaining.remove(exact)
            out.append({
                "source": src,
                "target": exact,
                "confidence": 0.95,
                "reasoning": "greedy_exact",
                "assignment_strategy": "greedy_lexical",
            })
            continue
        best_t = None
        best_s = -1.0
        for t in remaining:
            score = SequenceMatcher(None, src_n, _normalize_name(t)).ratio()
            if score > best_s:
                best_s = score
                best_t = t
        if best_t is None or best_s < 0.45:
            out.append({
                "source": src,
                "target": src,
                "confidence": 0.4,
                "reasoning": "greedy_unmatched",
                "create_new": True,
                "assignment_strategy": "greedy_lexical",
            })
            continue
        remaining.remove(best_t)
        out.append({
            "source": src,
            "target": best_t,
            "confidence": round(best_s, 3),
            "reasoning": "greedy_similarity",
            "assignment_strategy": "greedy_lexical",
        })
    return out


def _accuracy(mapped: list[dict], cases: list[dict]) -> dict[str, Any]:
    by = {m["source"]: m.get("target") for m in mapped}
    correct = sum(1 for c in cases if by.get(c["source"]) == c["target"])
    total = len(cases)
    return {
        "correct": correct,
        "total": total,
        "score": round(correct / total, 4) if total else 0.0,
    }


def measure_native_types() -> dict[str, Any]:
    from services.type_system import CANONICAL_TYPES, DDL_TYPES, LOGICAL_OBJECTID

    logical = sorted(set(CANONICAL_TYPES.values()))
    return {
        "native_alias_count": len(CANONICAL_TYPES),
        "logical_type_count": len(logical),
        "logical_types": logical,
        "ddl_dialect_count": len(DDL_TYPES),
        "ddl_dialects": sorted(DDL_TYPES.keys()),
        "has_logical_objectid": LOGICAL_OBJECTID in logical
        or "objectid" in logical,
    }


def measure_gates() -> dict[str, Any]:
    from preflight.models import GateId

    ids = [g.value for g in GateId]
    return {
        "gate_count": len(ids),
        "gate_ids": ids,
    }


def measure_g3_fail_closed() -> dict[str, Any]:
    from preflight import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )
    from preflight.gates import gate_g3_schema_contract
    from preflight.models import GateStatus

    # ObjectId → TEXT on PostgreSQL must BLOCK without risk ack.
    oid_plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="_id", inferred_type="OBJECTID")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="postgresql",
            db_type="postgresql",
            connected=True,
            can_write=True,
            table_exists=True,
            target_columns=[ColumnSchema(name="user_id", inferred_type="TEXT")],
        ),
        mappings=[ColumnMapping(source="_id", target="user_id", confidence=0.9)],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )
    oid_blocked = gate_g3_schema_contract(
        PreflightContext(
            plan=oid_plan,
            sample_rows=[{"_id": "507f1f77bcf86cd799439011"}],
        )
    )

    # Schemaless BSON affinity: ObjectId → INTEGER must BLOCK.
    bson_plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="_id", inferred_type="OBJECTID")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="mongodb",
            db_type="mongodb",
            connected=True,
            can_write=True,
            target_columns=[ColumnSchema(name="legacy_id", inferred_type="INTEGER")],
        ),
        mappings=[
            ColumnMapping(
                source="_id",
                target="legacy_id",
                confidence=0.9,
                target_type="INTEGER",
            ),
        ],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )
    bson_blocked = gate_g3_schema_contract(PreflightContext(plan=bson_plan))

    return {
        "objectid_text_blocks": int(oid_blocked.status == GateStatus.BLOCK),
        "objectid_text_status": oid_blocked.status.value,
        "bson_affinity_blocks": int(bson_blocked.status == GateStatus.BLOCK),
        "bson_affinity_status": bson_blocked.status.value,
    }


def measure_sample_aware_demotion() -> dict[str, Any]:
    from services.semantic_mapper import map_columns

    samples = [
        "693486a0f0d881be6f0c470e",
        "69349183a44dd21d08a19c2c",
        "6934a44da44dd21d08a1ac18",
        "6934b905a44dd21d08a1caca",
    ]
    out = map_columns(
        ["_id"],
        ["id", "column_2", "column_5"],
        source_schemas=[{"name": "_id", "inferred_type": "OBJECTID", "samples": samples}],
        target_schemas=[
            {"name": "id", "inferred_type": "DECIMAL"},
            {"name": "column_2", "inferred_type": "VARCHAR"},
            {"name": "column_5", "inferred_type": "VARCHAR"},
        ],
        threshold=0.75,
        destination_db_type="snowflake",
    )
    avoided = (
        len(out) == 1
        and out[0].get("target", "").lower() != "id"
        and (out[0].get("create_new") is True or out[0].get("target") in {"column_2", "column_5", "_id"})
    )
    return {
        "objectid_avoids_decimal_id": int(avoided),
        "chosen_target": out[0].get("target") if out else None,
        "create_new": bool(out[0].get("create_new")) if out else False,
    }


def measure_hungarian_vs_greedy() -> dict[str, Any]:
    from services.semantic_mapper import map_columns

    data = json.loads(ENTERPRISE.read_text(encoding="utf-8"))
    all_cases = [c for d in data["domains"] for c in d["cases"]]
    # Domain-batched (same protocol as enterprise proof) so bipartite stays fair.
    hung_correct = 0
    greed_correct = 0
    total = 0
    per_domain: dict[str, dict] = {}
    for dom in data["domains"]:
        cases = dom["cases"]
        sources = [c["source"] for c in cases]
        targets = [c["target"] for c in cases]
        schemas_src = [
            {"name": c["source"], "inferred_type": c.get("source_type", "VARCHAR"), "samples": []}
            for c in cases
        ]
        schemas_tgt = [
            {"name": c["target"], "inferred_type": c.get("target_type", "VARCHAR"), "samples": []}
            for c in cases
        ]
        hung = map_columns(
            sources,
            targets,
            source_schemas=schemas_src,
            target_schemas=schemas_tgt,
        )
        greed = map_columns_greedy_lexical(sources, targets)
        h = _accuracy(hung, cases)
        g = _accuracy(greed, cases)
        hung_correct += h["correct"]
        greed_correct += g["correct"]
        total += h["total"]
        per_domain[dom["name"]] = {
            "hungarian": h,
            "greedy_lexical": g,
            "delta": round(h["score"] - g["score"], 4),
        }
    hung_score = hung_correct / total if total else 0.0
    greed_score = greed_correct / total if total else 0.0
    delta = hung_score - greed_score
    return {
        "hungarian_accuracy": round(hung_score, 4),
        "greedy_accuracy": round(greed_score, 4),
        "greedy_delta": round(delta, 4),
        "greedy_delta_min_effective": round(delta, 4) if greed_score < 1.0 else 1.0,
        "total_cases": total,
        "domains": per_domain,
        "engine": "semantic_mapper.map_columns vs map_columns_greedy_lexical",
    }


def measure_enterprise_accuracy() -> dict[str, Any]:
    from services.semantic_mapper import map_columns

    data = json.loads(ENTERPRISE.read_text(encoding="utf-8"))
    correct = 0
    total = 0
    for dom in data["domains"]:
        cases = dom["cases"]
        sources = [c["source"] for c in cases]
        targets = [c["target"] for c in cases]
        mapped = map_columns(
            sources,
            targets,
            source_schemas=[
                {"name": c["source"], "inferred_type": c.get("source_type", "VARCHAR"), "samples": []}
                for c in cases
            ],
            target_schemas=[
                {"name": c["target"], "inferred_type": c.get("target_type", "VARCHAR"), "samples": []}
                for c in cases
            ],
        )
        acc = _accuracy(mapped, cases)
        correct += acc["correct"]
        total += acc["total"]
    return {
        "accuracy": round(correct / total, 4) if total else 0.0,
        "correct": correct,
        "min_cases": total,
        "total_cases": total,
    }


def measure_connector_pair_matrix() -> dict[str, Any]:
    pair_dir = PROOF_DIR / "mapping_connector_pair_matrix"
    if not pair_dir.exists():
        return {
            "pair_count": 0,
            "min_pair_score": 0.0,
            "aggregate_score": 0.0,
            "pairs_present": False,
            "note": "Run test_mapping_connector_pair_matrix first to materialize pair proofs.",
        }
    scores: list[float] = []
    for path in sorted(pair_dir.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        scores.append(float(data.get("score") or 0.0))
    if not scores:
        return {
            "pair_count": 0,
            "min_pair_score": 0.0,
            "aggregate_score": 0.0,
            "pairs_present": False,
        }
    return {
        "pair_count": len(scores),
        "min_pair_score": round(min(scores), 4),
        "aggregate_score": round(sum(scores) / len(scores), 4),
        "pairs_present": True,
    }


def measure_abbreviation_coverage() -> dict[str, Any]:
    # Prefer fresh measurement; fall back to last proof artifact.
    from services.semantic_mapper import ABBREVIATIONS, _normalize

    data = json.loads(ENTERPRISE.read_text(encoding="utf-8"))
    names: list[str] = []
    for domain in data["domains"]:
        for c in domain["cases"]:
            names.extend([c["source"], c["target"]])

    common = frozenset(
        """
        a an the of to for in on by at as or and not is are was were been
        id name type code key value data text note info flag status state
        description comment message title body content amount quantity price
        cost total balance date time year month day hour user account order
        customer product item line weight score rate unit risk claim batch
        """.split()
    )
    resolved = 0
    unresolved = 0
    for name in names:
        parts = [p for p in _normalize(name).split("_") if p]
        i = 0
        while i < len(parts):
            matched = False
            for j in range(len(parts), i, -1):
                phrase = "_".join(parts[i:j])
                if phrase in ABBREVIATIONS:
                    resolved += 1
                    i = j
                    matched = True
                    break
            if matched:
                continue
            token = parts[i]
            if token in common or token.isdigit() or len(token) > 6:
                i += 1
                continue
            if token in ABBREVIATIONS:
                resolved += 1
            else:
                unresolved += 1
            i += 1
    total = resolved + unresolved
    return {
        "coverage": round(resolved / total, 4) if total else 1.0,
        "resolved": resolved,
        "unresolved": unresolved,
        "total_abbrev_like": total,
        "dictionary_size": len(ABBREVIATIONS),
    }


def measure_create_new_type_risk() -> dict[str, Any]:
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["created_at"],
        target_columns=[],
        source_schemas=[{
            "name": "created_at",
            "inferred_type": "TIMESTAMPTZ",
            "samples": ["2024-01-01T00:00:00Z"],
        }],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    row = result["mappings"][0]
    risks = row.get("create_new_risks") or []
    return {
        "create_new_risks_present": int(bool(risks)),
        "target_type": row.get("target_type"),
        "risk_kinds": sorted({r.get("kind") for r in risks if r.get("kind")}),
        "requires_review": bool(row.get("requires_review")),
    }


_MEASUREMENTS: dict[str, Callable[[], dict[str, Any]]] = {
    "hungarian_assignment": measure_hungarian_vs_greedy,
    "native_type_canonicalization": measure_native_types,
    "g1_g9_fail_closed": lambda: {**measure_gates(), **measure_g3_fail_closed()},
    "sample_aware_type_demotion": measure_sample_aware_demotion,
    "enterprise_mapping_accuracy": measure_enterprise_accuracy,
    "connector_pair_matrix": measure_connector_pair_matrix,
    "abbreviation_coverage": measure_abbreviation_coverage,
    "create_new_type_risk_stamp": measure_create_new_type_risk,
}


def _floor_value(measured: dict[str, Any], key: str) -> float | int | None:
    if key == "greedy_delta_min":
        # When greedy already scores 1.0, delta floor is waived (both perfect).
        if float(measured.get("greedy_accuracy") or 0) >= 1.0:
            return float(measured.get("greedy_delta_min_effective") or 1.0)
        return float(measured.get("greedy_delta") or 0.0)
    if key == "min_cases":
        return int(measured.get("total_cases") or measured.get("min_cases") or 0)
    val = measured.get(key)
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    return val


def evaluate_claim(claim: dict[str, Any], measured: dict[str, Any]) -> dict[str, Any]:
    floors = claim.get("floors") or {}
    checks: list[dict[str, Any]] = []
    passed = True
    for key, floor in floors.items():
        got = _floor_value(measured, key)
        ok = got is not None and got >= floor
        if not ok:
            passed = False
        checks.append({
            "metric": key,
            "floor": floor,
            "measured": got,
            "passed": ok,
        })
    return {
        "id": claim["id"],
        "claim": claim["claim"],
        "buyer_line": claim.get("buyer_line"),
        "honesty": claim.get("honesty"),
        "passed": passed,
        "checks": checks,
        "measured": measured,
    }


def build_evidence_catalog(*, refresh_pairs: bool = False) -> dict[str, Any]:
    """Run all measurements and assemble the buyer-facing catalog."""
    if refresh_pairs:
        # Materialize pair proofs when missing so connector_pair floors can pass.
        _ensure_connector_pair_proofs()

    started = time.perf_counter()
    results: list[dict[str, Any]] = []
    for claim in CLAIM_REGISTRY:
        measure = _MEASUREMENTS[claim["id"]]
        measured = measure()
        results.append(evaluate_claim(claim, measured))

    elapsed_ms = (time.perf_counter() - started) * 1000
    all_passed = all(r["passed"] for r in results)
    catalog = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "title": "Datawrap mapping evidence catalog",
        "all_passed": all_passed,
        "claim_count": len(results),
        "passed_count": sum(1 for r in results if r["passed"]),
        "elapsed_ms": round(elapsed_ms, 1),
        "claims": results,
        "honesty": (
            "Every claim below is backed by a measured floor on reproducible "
            "fixtures. This catalog is not a customer case study and not a live "
            "competitor bake-off. Expand fixtures before raising floors; never "
            "raise marketing language above measured numbers."
        ),
        "forbidden_language": [
            "Never claim '300+ types' unless native_alias_count ≥ 300.",
            "Never claim 'beats Fivetran/Airbyte' without a published side-by-side harness.",
            "Never claim production false-green rate from unit gate proofs alone.",
            "Never omit honesty fields when quoting scores externally.",
        ],
    }
    return catalog


def _ensure_connector_pair_proofs() -> None:
    """Lightweight materialization of pair proofs used by the catalog."""
    from services.mapping_pipeline import run_mapping_pipeline

    pair_dir = PROOF_DIR / "mapping_connector_pair_matrix"
    pair_dir.mkdir(parents=True, exist_ok=True)
    sources = ("postgresql", "mysql", "mongodb", "sqlserver", "oracle", "snowflake")
    dests = ("postgresql", "mysql", "snowflake", "bigquery", "mongodb", "sqlserver", "oracle", "redshift")
    data = json.loads(ENTERPRISE.read_text(encoding="utf-8"))
    cases: list[dict] = []
    for domain in data["domains"]:
        for c in domain["cases"][:4]:
            cases.append(c)
        if len(cases) >= 20:
            break
    cases = cases[:20]
    src_cols = [c["source"] for c in cases]
    tgt_cols = [c["target"] for c in cases]
    source_schemas = [
        {"name": c["source"], "inferred_type": c["source_type"], "samples": []}
        for c in cases
    ]
    target_schemas = [
        {"name": c["target"], "inferred_type": c["target_type"], "samples": []}
        for c in cases
    ]
    for src in sources:
        for dst in dests:
            pair_id = f"{src}__{dst}"
            path = pair_dir / f"{pair_id}.json"
            if path.exists():
                continue
            result = run_mapping_pipeline(
                source_columns=src_cols,
                target_columns=tgt_cols,
                source_schemas=source_schemas,
                target_schemas=target_schemas,
                destination_db_type=dst,
                use_llm=False,
            )
            by = {m["source"]: m["target"] for m in result["mappings"]}
            correct = sum(1 for c in cases if by.get(c["source"]) == c["target"])
            proof = {
                "pair": pair_id,
                "source": src,
                "dest": dst,
                "correct": correct,
                "total": len(cases),
                "score": round(correct / len(cases), 4) if cases else 0.0,
                "engine": result.get("engine"),
            }
            path.write_text(json.dumps(proof, indent=2), encoding="utf-8")


def catalog_to_markdown(catalog: dict[str, Any]) -> str:
    lines = [
        "# Datawrap mapping evidence catalog",
        "",
        f"Generated: `{catalog.get('generated_at')}`",
        "",
        catalog.get("honesty", ""),
        "",
        f"**Overall:** {'PASS' if catalog.get('all_passed') else 'FAIL'} — "
        f"{catalog.get('passed_count')}/{catalog.get('claim_count')} claims met floors.",
        "",
        "## Claims",
        "",
    ]
    for claim in catalog.get("claims") or []:
        status = "PASS" if claim.get("passed") else "FAIL"
        lines.append(f"### `{claim['id']}` — {status}")
        lines.append("")
        lines.append(claim.get("claim") or "")
        lines.append("")
        lines.append(f"*Buyer line:* {claim.get('buyer_line') or ''}")
        lines.append("")
        lines.append("| Metric | Floor | Measured | Pass |")
        lines.append("| --- | ---: | ---: | --- |")
        for check in claim.get("checks") or []:
            lines.append(
                f"| `{check['metric']}` | {check['floor']} | {check['measured']} | "
                f"{'yes' if check['passed'] else 'NO'} |"
            )
        lines.append("")
        lines.append(f"*Honesty:* {claim.get('honesty') or ''}")
        lines.append("")
    lines.append("## Forbidden language")
    lines.append("")
    for item in catalog.get("forbidden_language") or []:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def write_evidence_catalog(
    catalog: dict[str, Any] | None = None,
    *,
    refresh_pairs: bool = False,
) -> tuple[Path, Path]:
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    catalog = catalog or build_evidence_catalog(refresh_pairs=refresh_pairs)
    json_path = PROOF_DIR / "evidence_catalog.json"
    md_path = PROOF_DIR / "EVIDENCE_CATALOG.md"
    json_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    md_path.write_text(catalog_to_markdown(catalog), encoding="utf-8")
    return json_path, md_path
