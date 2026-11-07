"""Kernel Risk Engine (Phase C9).

Maps ConversionClass → RiskLevel. Migration Risk Contracts are minted only when
the band is Approval or Blocked — never for Safe/Info paths.
"""

from __future__ import annotations

from typing import Any, Mapping

from services.decision_kernel.conversion import ConversionClass, classify_mapping
from services.decision_kernel.models import RiskLevel


def risk_level_for_conversion(conv: Mapping[str, Any] | None) -> RiskLevel:
    """ConversionClass → operator RiskLevel (SSOT for artifact + UI)."""
    conv = conv or {}
    cls = str(conv.get("conversion_class") or "")
    if cls in {
        ConversionClass.UNSUPPORTED.value,
        ConversionClass.NEEDS_MANUAL_MAPPING.value,
        ConversionClass.MANUAL.value,
    }:
        return RiskLevel.BLOCKED
    if conv.get("requires_risk_contract") or cls in {
        ConversionClass.NEEDS_USER_APPROVAL.value,
        ConversionClass.NARROWING.value,
        ConversionClass.POTENTIALLY_LOSSY.value,
        ConversionClass.LOSSY.value,
    }:
        return RiskLevel.APPROVAL
    if cls in {
        ConversionClass.NEEDS_TRANSFORM.value,
        ConversionClass.NEEDS_QUARANTINE.value,
        ConversionClass.SEMANTIC.value,
    }:
        return RiskLevel.REVIEW
    if cls in {
        ConversionClass.IDENTITY.value,
        ConversionClass.EQUIVALENT.value,
        ConversionClass.LOSSLESS.value,
        ConversionClass.WIDENING.value,
        ConversionClass.REPRESENTATION.value,
        ConversionClass.NORMALIZATION.value,
    }:
        return RiskLevel.SAFE
    return RiskLevel.INFO


def assess_mapping_risk(
    mapping: dict[str, Any],
    *,
    destination_db_type: str = "",
    declared_source_type: str = "",
    declared_target_type: str = "",
) -> dict[str, Any]:
    """Classify one Map cell and stamp RiskLevel + contract requirement."""
    conv = classify_mapping(
        mapping,
        declared_source_type=declared_source_type,
        declared_target_type=declared_target_type,
        destination_db_type=destination_db_type,
    )
    band = risk_level_for_conversion(conv)
    needs_contract = band in {RiskLevel.APPROVAL, RiskLevel.BLOCKED} or bool(
        conv.get("requires_risk_contract")
    )
    return {
        **conv,
        "risk_level": band.value,
        "requires_risk_contract": needs_contract,
        "mint_risk_contract": needs_contract and band is not RiskLevel.BLOCKED,
        "recommended_action": (
            "block"
            if band is RiskLevel.BLOCKED
            else "mint_risk_contract"
            if needs_contract
            else "review"
            if band is RiskLevel.REVIEW
            else "proceed"
        ),
    }


def aggregate_route_risk(mapping_risks: list[Mapping[str, Any]]) -> RiskLevel:
    """Worst-band across Map cells (Blocked > Approval > Review > Info > Safe)."""
    order = [
        RiskLevel.SAFE,
        RiskLevel.INFO,
        RiskLevel.REVIEW,
        RiskLevel.APPROVAL,
        RiskLevel.BLOCKED,
    ]
    worst = RiskLevel.SAFE
    for row in mapping_risks or []:
        try:
            band = RiskLevel(str(row.get("risk_level") or RiskLevel.INFO.value))
        except ValueError:
            band = RiskLevel.INFO
        if order.index(band) > order.index(worst):
            worst = band
    return worst


__all__ = [
    "RiskLevel",
    "aggregate_route_risk",
    "assess_mapping_risk",
    "risk_level_for_conversion",
]
