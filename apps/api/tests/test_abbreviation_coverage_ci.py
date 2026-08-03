"""Abbreviation coverage CI — grow dictionary from enterprise golden evidence."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "mapping_golden_enterprise.json"
PROOF_DIR = Path(__file__).resolve().parents[1] / "data" / "proofs"

# Tokens that look like abbreviations but are ordinary English / domain leaves.
_COMMON_WORDS = frozenset(
    """
    a an the of to for in on by at as or and not is are was were been being
    have has had do does did will would could should may might must can shall
    id name type code key value data text note info flag status state
    description comment message title body content amount quantity price cost
    total balance date time year month day hour user account order customer
    product item line row col column table field record entity party person
    company org organization email phone address city country zip postal
    region currency payment invoice receipt ship delivery hire birth create
    update modify delete insert select from where end start cart store gift
    score rate unit risk claim batch job grade dock door route trace paid
    copay count hours pound kilogram minimum maximum quarter billing center
    weight
    """.split()
)


def _abbrev_like_tokens(names: list[str]) -> tuple[Counter[str], Counter[str]]:
    from services.semantic_mapper import ABBREVIATIONS, _normalize

    resolved: Counter[str] = Counter()
    unresolved: Counter[str] = Counter()
    for name in names:
        parts = [p for p in _normalize(name).split("_") if p]
        i = 0
        while i < len(parts):
            matched = False
            for j in range(len(parts), i, -1):
                phrase = "_".join(parts[i:j])
                if phrase in ABBREVIATIONS:
                    resolved[phrase] += 1
                    i = j
                    matched = True
                    break
            if matched:
                continue
            token = parts[i]
            if token in _COMMON_WORDS or token.isdigit() or len(token) > 6:
                i += 1
                continue
            if token in ABBREVIATIONS:
                resolved[token] += 1
            else:
                unresolved[token] += 1
            i += 1
    return resolved, unresolved


def test_enterprise_abbreviation_coverage(tmp_path: Path) -> None:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    names: list[str] = []
    for domain in data["domains"]:
        for c in domain["cases"]:
            names.extend([c["source"], c["target"]])

    resolved, unresolved = _abbrev_like_tokens(names)
    resolved_n = sum(resolved.values())
    unresolved_n = sum(unresolved.values())
    total = resolved_n + unresolved_n
    coverage = resolved_n / total if total else 1.0

    proof = {
        "metric": "abbreviation_coverage_enterprise_golden",
        "resolved": resolved_n,
        "unresolved": unresolved_n,
        "total_abbrev_like": total,
        "coverage": round(coverage, 4),
        "top_unresolved": unresolved.most_common(25),
        "honesty": (
            "Measures abbreviation-like tokens on enterprise golden names against "
            "ABBREVIATIONS. Grow the dictionary from unresolved evidence — never "
            "claim 100% without listing remaining tokens."
        ),
    }
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    artifact = PROOF_DIR / "abbreviation_coverage.json"
    artifact.write_text(json.dumps(proof, indent=2), encoding="utf-8")
    (tmp_path / "abbreviation_coverage.json").write_text(
        json.dumps(proof, indent=2), encoding="utf-8"
    )

    assert total >= 50, "enterprise golden must yield enough abbrev-like tokens"
    assert coverage >= 0.92, (
        f"Abbreviation coverage {coverage:.1%} below 92%. "
        f"Top unresolved={unresolved.most_common(15)}. See {artifact}"
    )
    # Fail closed on high-frequency unresolved tokens (evidence to grow dict).
    hot = [t for t, n in unresolved.most_common(10) if n >= 5]
    assert not hot, f"High-frequency unresolved abbreviations need dictionary growth: {hot}"
