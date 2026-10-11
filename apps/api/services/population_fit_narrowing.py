"""Structured evidence for string-width narrowing accepted by population fit."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from services.type_system import parse_string_carrier_width, string_width_would_narrow

_logger = logging.getLogger(__name__)
WidthPair = tuple[str, str]
ProvenWidthNarrowings = Mapping[WidthPair, Mapping[str, int]]


def _proven_pairs(
    fit_report: Any,
    mappings: list[dict[str, Any]],
    source_types: Mapping[str, str],
    destination_types: Mapping[str, str],
    *,
    destination_table_exists: bool | None,
    dest_db: str = "",
) -> ProvenWidthNarrowings:
    source_by_name = {str(k).casefold(): str(v or "") for k, v in source_types.items()}
    destination_by_name = {
        str(k).casefold(): str(v or "") for k, v in destination_types.items()
    }
    fit_targets = {
        str(getattr(target, "target", "")).casefold(): target
        for target in (getattr(fit_report, "targets", ()) or ())
        if str(getattr(target, "carrier", "")).casefold() == "string"
    }
    unfit_targets = {
        str(getattr(getattr(finding, "target", None), "target", "")).casefold()
        for finding in (getattr(fit_report, "findings", ()) or ())
    }
    max_widths = {
        str(target).casefold(): int(width)
        for target, width in (getattr(fit_report, "max_observed_widths", ()) or ())
    }
    rows_scanned = int(getattr(fit_report, "rows_scanned", 0) or 0)
    rows_total = int(getattr(fit_report, "rows_total", 0) or 0)
    probe_error = str(getattr(fit_report, "probe_error", "") or "").strip()
    complete_scan = (
        destination_table_exists is True
        and getattr(fit_report, "evidence", "") == "exact"
        and bool(getattr(fit_report, "scanned_population", False))
        and rows_scanned == rows_total
        and rows_scanned > 0
        and not getattr(fit_report, "truncated_reason", "")
        and not probe_error
    )

    proven: dict[WidthPair, Mapping[str, int]] = {}
    for mapping in mappings:
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or "").strip()
        if not source or not target:
            continue
        source_key, target_key = source.casefold(), target.casefold()
        source_type = source_by_name.get(source_key, "")
        destination_type = destination_by_name.get(target_key, "")
        if not destination_type or not string_width_would_narrow(
            source_type, destination_type, dest_db=dest_db
        ):
            continue

        fit_target = fit_targets.get(target_key)
        fit_type = str(getattr(fit_target, "target_type", "") or "")
        target_width = parse_string_carrier_width(destination_type)
        fit_width = parse_string_carrier_width(fit_type)
        pair_proven = (
            complete_scan
            and not mapping.get("create_new")
            and not mapping.get("createNew")
            and str(mapping.get("assignment_strategy") or "").casefold()
            not in {"create_compatible_new", "identity_passthrough"}
            and fit_target is not None
            and target_key not in unfit_targets
            and target_width is not None
            and fit_width == target_width
        )
        max_observed_width = max_widths.get(target_key, 0)
        _logger.info(
            "population string-width fit source=%s target=%s rows_scanned=%d "
            "max_observed_width=%d target_width=%s proven=%s",
            source,
            target,
            rows_scanned,
            max_observed_width,
            target_width,
            pair_proven,
        )
        if pair_proven:
            proven[(source_key, target_key)] = MappingProxyType(
                {
                    "rows_scanned": rows_scanned,
                    "max_observed_width": max_observed_width,
                    "target_width": int(target_width),
                }
            )
    return MappingProxyType(proven)


def proven_width_narrowings(
    fit_report: Any,
    mappings: list[dict[str, Any]],
    source_types: Mapping[str, str],
    destination_types: Mapping[str, str],
    *,
    destination_table_exists: bool | None,
    dest_db: str = "",
) -> ProvenWidthNarrowings:
    return _proven_pairs(
        fit_report,
        mappings,
        source_types,
        destination_types,
        destination_table_exists=destination_table_exists,
        dest_db=dest_db,
    )


def proof_details(proven: ProvenWidthNarrowings) -> dict[str, dict[str, int]]:
    return {
        f"{source}→{target}": dict(evidence)
        for (source, target), evidence in proven.items()
    }


def unproven_width_message(target: str, fit_report: Any) -> str:
    declared_capacity = (
        "VARCHAR/CHAR width narrowing (declared capacity; accept risk or remap)"
    )
    if fit_report is None:
        return declared_capacity

    rows_scanned = int(getattr(fit_report, "rows_scanned", 0) or 0)
    rows_total = int(getattr(fit_report, "rows_total", 0) or 0)
    reason = str(getattr(fit_report, "truncated_reason", "") or "")
    evidence = str(getattr(fit_report, "evidence", "") or "")
    probe_error = str(getattr(fit_report, "probe_error", "") or "").strip()
    if not probe_error and evidence == "unmeasured" and rows_scanned <= 0:
        return declared_capacity
    if not probe_error and reason == "reused_validate":
        return declared_capacity
    if not probe_error and rows_scanned <= 0 and not reason:
        return declared_capacity
    if not probe_error and rows_scanned >= rows_total and not reason:
        return declared_capacity
    if probe_error:
        reason = f"source population probe failed: {probe_error}"
    elif reason == "walk":
        reason = "source population walk failed"
    if not reason and rows_total and rows_scanned < rows_total:
        reason = "walk ended before the population was exhausted"
    if not reason:
        reason = "population scan was not exact"
    stopped_after = (
        f"{rows_scanned} of {rows_total} rows"
        if rows_total
        else f"{rows_scanned} rows"
    )
    return (
        f"width narrowing not proven: population scan stopped after {stopped_after} "
        f"({reason}); widen {target} to fit, "
        "remap, or accept the risk"
    )
