"""Domain abbreviation gazetteer packs merge into ABBREVIATIONS (rank 27)."""

from __future__ import annotations

from services.domain_gazetteers import gazetteer_stats, load_domain_abbreviations, merge_abbreviations
from services.semantic_mapper import ABBREVIATIONS


def test_domain_packs_load():
    packs = load_domain_abbreviations()
    assert packs["cusip"] == "cusip_code"
    assert packs["ndc"] == "ndc_code"
    assert packs["lei"] == "legal_entity_identifier"
    stats = gazetteer_stats()
    assert stats["domain_entries"] >= 20


def test_merge_fills_gaps_only():
    base = {"amt": "amount", "cusip": "keep_existing"}
    merged = merge_abbreviations(base)
    assert merged["cusip"] == "keep_existing"
    assert merged["ndc"] == "ndc_code"
    assert merged["amt"] == "amount"


def test_abbreviations_ssot_includes_packs():
    assert ABBREVIATIONS.get("cusip") == "cusip_code"
    assert ABBREVIATIONS.get("ndc") == "ndc_code"
