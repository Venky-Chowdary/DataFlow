"""Evidence pack CI — claims must not exceed measured floors."""

from __future__ import annotations

import json
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
PROOF_DIR = API_ROOT / "data" / "proofs"


def test_evidence_catalog_meets_all_claim_floors(tmp_path: Path) -> None:
    from services.evidence_pack import build_evidence_catalog, write_evidence_catalog

    catalog = build_evidence_catalog(refresh_pairs=True)
    json_path, md_path = write_evidence_catalog(catalog)
    (tmp_path / "evidence_catalog.json").write_text(
        json.dumps(catalog, indent=2), encoding="utf-8"
    )

    assert json_path.exists()
    assert md_path.exists()
    assert catalog["claim_count"] >= 8
    assert catalog["all_passed"], (
        "Evidence pack failed — marketing claims exceed measured floors. "
        f"Misses: {[c['id'] for c in catalog['claims'] if not c['passed']]}. "
        f"See {json_path}"
    )


def test_evidence_forbidden_300_plus_without_inventory() -> None:
    """Never allow an affirmative '300+' type claim when measured aliases < 300."""
    from services.evidence_pack import CLAIM_REGISTRY, measure_native_types

    measured = measure_native_types()
    aliases = measured["native_alias_count"]
    if aliases < 300:
        native = next(c for c in CLAIM_REGISTRY if c["id"] == "native_type_canonicalization")
        blob = f"{native.get('claim') or ''} {native.get('buyer_line') or ''}".lower()
        # Strip known negation phrases, then forbid leftover 300+.
        scrubbed = blob.replace("not an invented '300+'", "").replace('not an invented "300+"', "")
        scrubbed = scrubbed.replace("never an inflated", "")
        assert "300+" not in scrubbed, (
            f"Claim language affirms 300+ but measured native_alias_count={aliases}. "
            "Update the claim to the measured inventory."
        )


def test_greedy_baseline_is_weaker_or_tied_on_enterprise() -> None:
    from services.evidence_pack import measure_hungarian_vs_greedy

    result = measure_hungarian_vs_greedy()
    assert result["hungarian_accuracy"] >= result["greedy_accuracy"]
    assert result["total_cases"] >= 200


def test_evidence_catalog_markdown_lists_honesty() -> None:
    from services.evidence_pack import build_evidence_catalog, catalog_to_markdown

    catalog = build_evidence_catalog(refresh_pairs=False)
    md = catalog_to_markdown(catalog)
    assert "Honesty" in md or "honesty" in md.lower()
    assert "Forbidden language" in md
    assert "hungarian_assignment" in md
