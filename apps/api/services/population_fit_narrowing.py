"""Reconcile declared string-width narrowing only when the full population fits."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

_logger = logging.getLogger(__name__)


def _proven_pairs(
    fit_report: Any,
    mappings: list[dict[str, Any]],
    source_types: Mapping[str, str],
    destination_types: Mapping[str, str],
    *,
    destination_table_exists: bool | None,
) -> set[tuple[str, str]]:
    from services.type_system import string_width_would_narrow

    if (
        destination_table_exists is not True
        or getattr(fit_report, "evidence", "") != "exact"
        or not getattr(fit_report, "scanned_population", False)
        or int(getattr(fit_report, "rows_scanned", 0) or 0) <= 0
        or int(getattr(fit_report, "rows_scanned", 0) or 0)
        != int(getattr(fit_report, "rows_total", -1) or 0)
        or getattr(fit_report, "truncated_reason", "")
    ):
        return set()

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
    proven: set[tuple[str, str]] = set()
    for mapping in mappings:
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or "").strip()
        if not source or not target:
            continue
        strategy = str(mapping.get("assignment_strategy") or "").casefold()
        if (
            mapping.get("create_new")
            or mapping.get("createNew")
            or strategy in {"create_compatible_new", "identity_passthrough"}
        ):
            continue
        pair = (source.casefold(), target.casefold())
        fit_target = fit_targets.get(pair[1])
        source_type = source_by_name.get(pair[0], "")
        destination_type = destination_by_name.get(pair[1], "")
        fit_type = str(getattr(fit_target, "target_type", "") or "")
        if (
            fit_target is None
            or pair[1] in unfit_targets
            or not destination_type
            or not string_width_would_narrow(source_type, destination_type)
        ):
            continue
        fit_width = re.search(r"\((\d+)\)", fit_type)
        destination_width = re.search(r"\((\d+)\)", destination_type)
        if (
            fit_width is None
            or destination_width is None
            or fit_width.group(1) != destination_width.group(1)
        ):
            continue
        proven.add(pair)
    return proven


def _matches_pair(
    issue: Any,
    source: str,
    target: str,
    source_type: str,
    target_type: str,
    *,
    related_reasons: tuple[str, ...] = (),
) -> bool:
    if isinstance(issue, Mapping):
        issue_source = str(
            issue.get("source") or issue.get("source_column") or ""
        ).casefold()
        issue_target = str(
            issue.get("target") or issue.get("target_column") or ""
        ).casefold()
        reason = str(issue.get("reason") or issue.get("message") or "").casefold()
        return (
            issue_source == source
            and issue_target == target
            and "width narrowing" in reason
        )

    text = str(issue or "").casefold()
    if not text:
        return False
    if any(reason and reason.casefold() in text for reason in related_reasons):
        return True
    return (
        (
            "width narrowing" in text
            or any("width narrowing" in reason.casefold() for reason in related_reasons)
        )
        and all(
            token.casefold() in text
            for token in (source, target, source_type, target_type)
            if token
        )
    )


def _reconcile_gates(
    result: Any,
    proven: set[tuple[str, str]],
    source_types: Mapping[str, str],
    destination_types: Mapping[str, str],
) -> bool:
    from preflight.models import GateStatus

    if not proven:
        return False
    source_by_name = {str(k).casefold(): str(v or "") for k, v in source_types.items()}
    destination_by_name = {
        str(k).casefold(): str(v or "") for k, v in destination_types.items()
    }
    changed = False
    g3_narrowing_reasons: dict[tuple[str, str], tuple[str, ...]] = {}
    for gate in result.gates:
        gate_id = str(getattr(getattr(gate, "gate_id", ""), "value", ""))
        details = dict(getattr(gate, "details", {}) or {})
        if gate_id == "g3_schema_contract":
            accepted = []
            remaining = []
            for issue in details.get("issues_detail") or []:
                if not isinstance(issue, Mapping):
                    remaining.append(issue)
                    continue
                pair = (
                    str(issue.get("source") or "").casefold(),
                    str(issue.get("target") or "").casefold(),
                )
                if (
                    pair in proven
                    and str(issue.get("severity") or "").casefold() == "block"
                    and _matches_pair(
                        issue,
                        *pair,
                        source_by_name.get(pair[0], ""),
                        destination_by_name.get(pair[1], ""),
                    )
                ):
                    accepted.append(issue)
                else:
                    remaining.append(issue)
            if accepted:
                accepted_reasons = {
                    (
                        str(issue.get("source") or "").casefold(),
                        str(issue.get("target") or "").casefold(),
                    ): str(issue.get("reason") or issue.get("message") or "")
                    for issue in accepted
                }
                g3_narrowing_reasons = {
                    pair: (reason,) for pair, reason in accepted_reasons.items()
                }
                details["issues_detail"] = remaining
                details["issues"] = [
                    issue
                    for issue in (details.get("issues") or [])
                    if not any(
                        _matches_pair(
                            issue,
                            source,
                            target,
                            source_by_name.get(source, ""),
                            destination_by_name.get(target, ""),
                            related_reasons=(reason,),
                        )
                        for (source, target), reason in accepted_reasons.items()
                    )
                ]
                details["population_fit_string_narrowing"] = accepted
                if not any(
                    str(issue.get("severity") or "").casefold() == "block"
                    for issue in remaining
                ):
                    gate.status = GateStatus.PASS
                    gate.message = (
                        "String width narrowing fits the exact scanned population"
                    )
                gate.details = details
                changed = True
        elif gate_id in {"g6_target_ddl", "g9_data_integrity"}:
            issues_key = "issues" if gate_id == "g6_target_ddl" else "issue_texts"
            issues = list(details.get(issues_key) or [])
            structured_reasons = (
                dict(g3_narrowing_reasons) if gate_id == "g9_data_integrity" else {}
            )
            for detail in details.get("issues_detail") or []:
                if not isinstance(detail, Mapping):
                    continue
                pair = (
                    str(detail.get("source") or "").casefold(),
                    str(detail.get("target") or "").casefold(),
                )
                if pair not in proven or not _matches_pair(
                    detail,
                    *pair,
                    source_by_name.get(pair[0], ""),
                    destination_by_name.get(pair[1], ""),
                ):
                    continue
                reason = str(detail.get("reason") or detail.get("message") or "")
                structured_reasons[pair] = (
                    *structured_reasons.get(pair, ()),
                    reason,
                )
            accepted = []
            remaining = []
            for issue in issues:
                matched = False
                for source, target in proven:
                    if _matches_pair(
                        issue,
                        source,
                        target,
                        source_by_name.get(source, ""),
                        destination_by_name.get(target, ""),
                        related_reasons=structured_reasons.get((source, target), ()),
                    ):
                        accepted.append(issue)
                        matched = True
                        break
                if not matched:
                    remaining.append(issue)
            if accepted:
                details[issues_key] = remaining
                details["issues_detail"] = [
                    detail
                    for detail in details.get("issues_detail") or []
                    if not isinstance(detail, Mapping)
                    or not any(
                        _matches_pair(
                            detail,
                            *pair,
                            source_by_name.get(pair[0], ""),
                            destination_by_name.get(pair[1], ""),
                        )
                        for pair in proven
                    )
                ]
                if gate_id == "g9_data_integrity":
                    details["issues"] = remaining
                    details["checks_failed"] = max(
                        0, int(details.get("checks_failed") or 0) - len(accepted)
                    )
                    details["checks_passed"] = int(
                        details.get("checks_passed") or 0
                    ) + len(accepted)
                if not remaining and (
                    gate_id != "g9_data_integrity"
                    or int(details.get("checks_failed") or 0) == 0
                ):
                    gate.status = GateStatus.PASS
                    gate.message = (
                        "String width narrowing fits the exact scanned population"
                    )
                gate.details = details
                changed = True

    if changed:
        result.blockers = [
            gate for gate in result.gates if gate.status == GateStatus.BLOCK
        ]
        result.passed = not result.blockers
    return changed


def reconcile_population_proven_string_narrowings(
    result: Any,
    fit_report: Any,
    mappings: list[dict[str, Any]],
    source_types: Mapping[str, str],
    destination_types: Mapping[str, str],
    destination_table_exists: bool | None,
    ddl_issues: list[str] | None = None,
    proof_bundle: dict[str, Any] | None = None,
    blockers: list[dict[str, Any]] | None = None,
) -> set[tuple[str, str]]:
    """Clear only string-width blockers proven by a complete exact fit scan."""
    proven = _proven_pairs(
        fit_report,
        mappings,
        source_types,
        destination_types,
        destination_table_exists=destination_table_exists,
    )
    if not proven:
        return proven

    changed = _reconcile_gates(result, proven, source_types, destination_types)
    if ddl_issues is not None:
        source_by_name = {
            str(name).casefold(): str(value or "") for name, value in source_types.items()
        }
        destination_by_name = {
            str(name).casefold(): str(value or "")
            for name, value in destination_types.items()
        }
        ddl_issues[:] = [
            issue
            for issue in ddl_issues
            if not any(
                _matches_pair(
                    issue,
                    source,
                    target,
                    source_by_name.get(source, ""),
                    destination_by_name.get(target, ""),
                )
                for source, target in proven
            )
        ]
    if changed and proof_bundle and proof_bundle.get("validation_orchestrator"):
        try:
            from services.decision_kernel import orchestrate_validation_summary

            proof_bundle["validation_orchestrator"] = orchestrate_validation_summary(
                decision_artifact=proof_bundle.get("decision_artifact") or {},
                gates=[
                    {
                        "id": gate.gate_id.value,
                        "status": gate.status.value,
                        "message": gate.message,
                    }
                    for gate in result.gates
                ],
                blockers=[
                    {
                        "id": blocker.gate_id.value,
                        "status": "block",
                        "message": blocker.message,
                    }
                    for blocker in result.blockers
                ],
            )
        except Exception as exc:
            _logger.warning("could not refresh population-fit validation summary: %s", exc)
    if blockers is not None:
        blockers[:] = [
            {
                "id": blocker.gate_id.value,
                "message": blocker.message,
                "details": blocker.details,
            }
            for blocker in result.blockers
        ]
    return proven


def reconcile_population_fit_coercion_report(
    coercion_report: Any,
    proven: set[tuple[str, str]],
) -> Any:
    if not isinstance(coercion_report, dict) or not proven:
        return coercion_report

    def reconcile_column(column: Any) -> Any:
        if not isinstance(column, dict):
            return column
        pair = (
            str(column.get("source") or "").casefold(),
            str(column.get("target") or "").casefold(),
        )
        if (
            pair in proven
            and column.get("fidelity_collapse")
            and not int(column.get("failed") or 0)
            and str(column.get("severity") or "").casefold() == "block"
        ):
            reconciled = {
                **column,
                "severity": "warn",
                "failure_class": "EXACT_POPULATION_FIT_WIDTH_NARROWING",
                "population_fit_verified": True,
            }
            reconciled.pop("blocked_by", None)
            reconciled.pop("block_reason", None)
            return reconciled
        return column

    coercion_report["columns"] = [
        reconcile_column(column)
        for column in coercion_report.get("columns") or []
    ]
    coercion_report["by_source"] = {
        source: reconcile_column(column)
        for source, column in (coercion_report.get("by_source") or {}).items()
    }
    proven_sources = {source for source, _target in proven}
    coercion_report["declared_type_blocks"] = [
        item
        for item in coercion_report.get("declared_type_blocks") or []
        if str(item.get("source") or "").casefold() not in proven_sources
    ]
    coercion_report["has_blocking_failures"] = any(
        isinstance(column, dict)
        and str(column.get("severity") or "").casefold() == "block"
        for column in coercion_report["columns"]
    )
    return coercion_report
