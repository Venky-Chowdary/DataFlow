"""AI-assisted "explain & suggest fix" for preflight validation results.

Turns a raw preflight result into a clear, structured, actionable explanation:
what failed, which column/row/value/type, why, and a concrete fix — plus
machine-readable ``suggested_actions`` the UI can turn into one-click buttons.

The explanation is built deterministically from the existing rulebook
(:mod:`services.preflight_rules`) and the value-level ``coercion_report`` so it
always works offline and is fully testable. When an LLM provider is configured
(Data Pilot infra) it is reused only to add a friendlier natural-language
narrative — never to invent the facts. If no provider is available the
deterministic narrative is used.
"""

from __future__ import annotations

from typing import Any

from services.preflight_rules import explain_gate, explain_issue


def _coercion_column_fixes(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the coercion report into per-column, actionable fix entries."""
    out: list[dict[str, Any]] = []
    for col in (report or {}).get("columns", []):
        if col.get("severity") == "ok":
            continue
        out.append({
            "column": col.get("source"),
            "target": col.get("target"),
            "source_type": col.get("source_type"),
            "target_type": col.get("target_type"),
            "severity": col.get("severity"),
            "failed": col.get("failed", 0),
            "sentinel_nulls": col.get("sentinel_nulls", 0),
            "sampled": col.get("sampled", 0),
            "sample_failures": col.get("sample_failures", []),
            "suggested_fix": col.get("suggested_fix", ""),
            "suggested_target_type": col.get("suggested_target_type"),
            "suggested_transform": col.get("suggested_transform"),
            "destination_exists": bool(col.get("destination_exists")),
            "table_exists": bool(col.get("table_exists")),
        })
    return out


def _suggested_actions(
    blockers: list[dict[str, Any]],
    column_fixes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Machine-readable next steps the UI can render as one-click actions."""
    actions: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    for cf in column_fixes:
        if cf.get("severity") == "block" and cf.get("suggested_target_type"):
            key = ("change_target_type", cf["column"], cf["suggested_target_type"])
            if key not in seen:
                seen.add(key)
                to_type = cf["suggested_target_type"]
                existing = bool(cf.get("destination_exists") or cf.get("table_exists"))
                label = (
                    f"Remap '{cf['column']}' — destination is typed; "
                    f"mapping Widen to {to_type} does not ALTER DDL"
                    if existing
                    else f"Widen '{cf['column']}' to {to_type}"
                )
                actions.append({
                    "kind": "change_target_type",
                    "column": cf["column"],
                    "target": cf.get("target"),
                    "to_type": to_type,
                    "label": label,
                    "requires_ddl": existing,
                })
        if cf.get("suggested_transform"):
            key = ("add_transform", cf["column"], cf["suggested_transform"])
            if key not in seen:
                seen.add(key)
                actions.append({
                    "kind": "add_transform",
                    "column": cf["column"],
                    "target": cf.get("target"),
                    "transform": cf["suggested_transform"],
                    "label": f"Apply {cf['suggested_transform']} transform to '{cf['column']}'",
                })

    gate_ids = {b.get("id") or b.get("gate") for b in blockers}
    blocker_text = " ".join(
        str(b.get("message") or "") + " " + str(b.get("details") or "")
        for b in blockers
    ).lower()
    if "format-control" in blocker_text or "replacement character" in blocker_text or "encoding" in blocker_text:
        actions.append({
            "kind": "normalize_control_chars",
            "transform": "strip_controls",
            "label": "Strip control characters & re-run",
        })
        actions.append({
            "kind": "quarantine_and_rerun",
            "label": "Quarantine bad cells & re-run",
        })
        actions.append({
            "kind": "open_bad_data_fix",
            "label": "Open Fix bad data dialog",
        })
    # Dry-run / integrity failures also get the strip + quarantine one-clicks so
    # operators never have to dig through a collapsed AI panel to remediate.
    if any(
        gid in gate_ids or (gid or "").startswith("g5")
        for gid in ("g5_dry_run", "g5_transform", "g9_data_integrity", "dry_run")
    ) or "dry-run" in blocker_text or "integrity" in blocker_text:
        if not any(a.get("kind") == "normalize_control_chars" for a in actions):
            actions.append({
                "kind": "normalize_control_chars",
                "transform": "strip_controls",
                "label": "Strip control characters & re-run",
            })
        if not any(a.get("kind") == "quarantine_and_rerun" for a in actions):
            actions.append({
                "kind": "quarantine_and_rerun",
                "label": "Quarantine bad cells & re-run",
            })
    if "g4_mapping_confidence" in gate_ids:
        actions.append({"kind": "review_mappings", "label": "Review and approve low-confidence mappings"})
    if "schema_drift" in gate_ids:
        actions.append({"kind": "rerun_mapping", "label": "Re-run mapping to accept the new schema"})
    if {"g1_source", "g2_destination"} & gate_ids:
        actions.append({"kind": "check_connection", "label": "Check the source/destination connection settings"})
    return actions


def _deterministic_narrative(
    passed: bool,
    issues: list[dict[str, Any]],
    column_fixes: list[dict[str, Any]],
) -> str:
    if passed:
        return "All preflight gates passed. This transfer is safe to run."
    lines: list[str] = []
    gate_titles = [i["title"] for i in issues if i.get("severity") != "warning"]
    if gate_titles:
        lines.append(
            "This transfer is blocked by "
            f"{len(gate_titles)} gate(s): {', '.join(gate_titles)}."
        )
    hard_cols = [c for c in column_fixes if c.get("severity") == "block"]
    warn_cols = [c for c in column_fixes if c.get("severity") == "warn"]
    for c in hard_cols:
        fails = c.get("sample_failures") or []
        example = f" First failing value: {fails[0]['value']!r} (row {fails[0]['row']})." if fails else ""
        lines.append(f"• {c['suggested_fix']}{example}")
    for c in warn_cols:
        lines.append(f"• {c['suggested_fix']}")
    if not hard_cols and not warn_cols:
        for i in issues:
            if i.get("severity") != "warning":
                lines.append(f"• {i['title']}: {i['why']} Fix: {i['fix']}")
    return "\n".join(lines) if lines else "Preflight reported issues — review the gate details."


def _llm_narrative(deterministic: str, issues: list[dict[str, Any]]) -> tuple[str, str]:
    """Best-effort natural-language narrative from an available LLM provider.

    Returns (narrative, provider_name). Falls back to the deterministic text
    when no provider is available or the call fails — the facts always come from
    the deterministic structure, the LLM only rephrases.
    """
    try:
        from src.ai.llm.provider import (
            DataTransferAnthropicProvider,
            DataTransferOllamaProvider,
            DataTransferOpenAIProvider,
        )

        providers = [
            DataTransferAnthropicProvider(),
            DataTransferOpenAIProvider(),
            DataTransferOllamaProvider(),
        ]
    except Exception:
        return deterministic, "deterministic"

    system = (
        "You are DataFlow's validation assistant. Explain data-transfer preflight "
        "failures to a data engineer in clear, concise language. Only use the facts "
        "provided — never invent columns, values, or fixes. Prefer short, prioritized, "
        "actionable steps."
    )
    facts = "\n".join(
        f"- [{i.get('severity', 'block')}] {i['title']}: {i['why']} Fix: {i['fix']}"
        for i in issues
    )
    prompt = (
        "Preflight validation facts:\n"
        f"{facts}\n\n"
        f"Structured summary:\n{deterministic}\n\n"
        "Write a concise explanation (max ~120 words) of why validation failed and "
        "the prioritized steps to fix it. Use plain language and keep column names in backticks."
    )
    for provider in providers:
        try:
            if not provider.is_available():
                continue
            resp = provider.generate(prompt, system=system, max_tokens=400)
            if resp.success and resp.content.strip():
                return resp.content.strip(), provider.name
        except Exception:
            continue
    return deterministic, "deterministic"


def explain_validation(
    preflight: dict[str, Any],
    *,
    dest_kind: str = "",
    validation_mode: str = "strict",
    use_llm: bool = True,
) -> dict[str, Any]:
    """Return a structured, actionable explanation of a preflight result.

    Parameters
    ----------
    preflight:
        A preflight result dict as returned by ``run_file_preflight`` /
        ``apply_policy_gates`` (contains ``passed``, ``gates``, ``blockers``,
        and optionally ``coercion_report``).
    """
    passed = bool(preflight.get("passed"))
    blockers = preflight.get("blockers") or []
    coercion_report = preflight.get("coercion_report") or {}

    issues: list[dict[str, Any]] = []
    for b in blockers:
        gate_id = b.get("id") or b.get("gate") or "general"
        guidance = b.get("guidance") or explain_gate(gate_id, b.get("message", ""), b.get("details"))
        details = b.get("details") or {}
        nested = details.get("issues") or details.get("errors") or []
        columns = [
            d.get("source") or d.get("column")
            for d in (details.get("issues_detail") or [])
            if d.get("source") or d.get("column")
        ]
        issues.append({
            "gate": gate_id,
            "title": guidance.get("title", gate_id),
            "severity": "block",
            "what": b.get("message", ""),
            "why": guidance.get("why", ""),
            "fix": guidance.get("fix", ""),
            "examples": guidance.get("examples", []),
            "columns": columns,
            "detail_messages": [str(n) for n in nested][:10],
        })

    column_fixes = _coercion_column_fixes(coercion_report)
    # Surface warn-only columns as advisory issues even when nothing blocks.
    for c in column_fixes:
        if c["severity"] == "warn":
            issues.append({
                "gate": "g3_schema_contract",
                "title": "Placeholder values become NULL",
                "severity": "warning",
                "what": f"{c['sentinel_nulls']} placeholder value(s) in '{c['column']}'",
                "why": explain_issue("lossy type coercion", dest_kind=dest_kind).get("why", ""),
                "fix": c["suggested_fix"],
                "examples": [],
                "columns": [c["column"]],
                "detail_messages": [],
            })

    actions = _suggested_actions(blockers, column_fixes)
    deterministic = _deterministic_narrative(passed, issues, column_fixes)

    narrative, provider = deterministic, "deterministic"
    if not passed and use_llm:
        narrative, provider = _llm_narrative(deterministic, issues)

    hard_count = sum(1 for i in issues if i["severity"] != "warning")
    summary = (
        "Validation passed — safe to run."
        if passed
        else f"Validation blocked: {hard_count} issue(s), {len(column_fixes)} column(s) need attention."
    )

    return {
        "passed": passed,
        "summary": summary,
        "issues": issues,
        "column_fixes": column_fixes,
        "suggested_actions": actions,
        "narrative": narrative,
        "assistant_provider": provider,
    }
