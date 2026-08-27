"""AI-assisted "explain & suggest fix" for preflight validation results.

Turns a raw preflight result into a clear, structured, actionable explanation:
what failed, which column/row/value/type, why, and a concrete fix — plus
machine-readable ``suggested_actions`` the UI can turn into one-click buttons.

The explanation is built deterministically from the existing rulebook
(:mod:`services.preflight_rules`) and the value-level ``coercion_report`` so it
always works offline and is fully testable. When an LLM provider is configured
(Datawrap Pilot infra) it is reused only to add a friendlier natural-language
narrative — never to invent the facts. If no provider is available the
deterministic narrative is used.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from services.blocker_titles import blocker_title
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


def _population_fit_column_fixes(
    fit: dict[str, Any] | None,
    *,
    table_exists: bool = False,
) -> list[dict[str, Any]]:
    """Promote g3f_population_fit overflows into the same column-fix SSOT."""
    if not isinstance(fit, dict):
        return []
    out: list[dict[str, Any]] = []
    for col in fit.get("findings") or []:
        if not isinstance(col, dict):
            continue
        try:
            unfit = int(col.get("unfit_rows") or 0)
        except (TypeError, ValueError):
            unfit = 0
        if unfit <= 0:
            continue
        examples = col.get("example_values") if isinstance(col.get("example_values"), list) else []
        rows = col.get("example_rows") if isinstance(col.get("example_rows"), list) else []
        sample_failures = []
        if examples:
            sample_failures.append({
                "row": rows[0] if rows else None,
                "value": examples[0],
                "reason": col.get("unfit_reason") or col.get("reason") or "",
            })
        out.append({
            "column": col.get("source"),
            "target": col.get("target") or col.get("source"),
            "source_type": col.get("source_type") or "",
            "target_type": col.get("target_type") or "",
            "severity": "block",
            "failed": unfit,
            "sentinel_nulls": 0,
            "sampled": unfit,
            "sample_failures": sample_failures,
            "suggested_fix": col.get("suggested_fix") or "",
            "suggested_target_type": col.get("suggested_target_type") or "",
            "suggested_transform": None,
            "destination_exists": table_exists,
            "table_exists": table_exists,
        })
    return out


def _merge_column_fixes(
    coercion: list[dict[str, Any]],
    population: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """One row per source+target. Population-fit dest widen wins."""

    def _key(row: dict[str, Any]) -> tuple[str, str]:
        return (
            str(row.get("column") or "").strip().lower(),
            str(row.get("target") or "").strip().lower(),
        )

    pop_keys = {_key(r) for r in population}
    out = [r for r in coercion if _key(r) not in pop_keys]
    out.extend(population)
    return out


def _parse_type_mismatch_columns(text: str) -> list[tuple[str, str]]:
    """Extract (source, target) from messages like ``population (VARCHAR) → population (NUMBER)``."""
    return [(src, tgt) for src, _st, tgt, _tt in _parse_type_mismatch_pairs(text)]


def _parse_type_mismatch_pairs(text: str) -> list[tuple[str, str, str, str]]:
    """Extract (source, source_type, target, target_type) from mismatch messages.

    Type tokens may nest parentheses (``NUMBER(38,0)``, ``DECIMAL(10,2)``) — a
    naive ``[^)]+`` truncates at the first ``)`` and corrupts Remap CTAs.
    """
    import re

    out: list[tuple[str, str, str, str]] = []
    # One level of nesting covers dialect carriers used in dry-run / G3 messages.
    type_tok = r"((?:[^()]|\([^()]*\))+)"
    for m in re.finditer(
        rf"([A-Za-z_][\w]*)\s*\({type_tok}\)\s*→\s*([A-Za-z_][\w]*)\s*\({type_tok}\)",
        text or "",
    ):
        out.append(
            (m.group(1), m.group(2).strip(), m.group(3), m.group(4).strip())
        )
    return out


def _remap_to_type_for_mismatch(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> str:
    """Choose a one-click target type that can actually clear the gate."""
    from services.type_system import suggest_remap_target

    return suggest_remap_target(source_type, target_type, dest_db=dest_db)


def _is_encoding_blocker(text: str) -> bool:
    t = (text or "").lower()
    return any(
        k in t
        for k in (
            "format-control",
            "replacement character",
            "encoding",
            "zero-width",
            "null byte",
            "u+200b",
            "u+0000",
        )
    )


def _is_duplicate_key_blocker(text: str, gate_ids: set[str] | None = None) -> bool:
    """True when uniqueness/identity-key collisions are the root preflight failure.

    Do not match the generic G9 why boilerplate ("duplicate keys, required nulls,
    financial precision…") — that made null/empty blockers look like duplicates.
    """
    t = (text or "").lower()
    explicit = (
        "duplicate key values",
        "duplicate key value",
        "duplicate target key",
        "keys repeat",
        "primary key candidate",
        "expect_column_unique",
        "duplicate value(s) in source sample",
        "duplicate identity",
    )
    if any(k in t for k in explicit):
        return True
    # Concrete null findings own the narrative. Also ignore G9 boilerplate that
    # lists "duplicate keys, required nulls, …" as a catalog of possible rules.
    if (
        "null/empty" in t
        or "null rate" in t
        or "for required field" in t
        or "duplicate keys, required nulls" in t
    ):
        return False
    _ = gate_ids  # reserved for future gate-scoped heuristics
    return False


def _is_type_mismatch_blocker(text: str) -> bool:
    t = (text or "").lower()
    if _parse_type_mismatch_columns(text):
        return True
    return any(
        k in t
        for k in (
            "invalid decimal",
            "invalid integer",
            "invalid boolean",
            "cannot be cast",
            "does not safely become",
            "lossy type",
            "lossy coercion",
            "unparseable",
        )
    )


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
    )
    blocker_lower = blocker_text.lower()

    # Duplicate identity keys cannot be healed by mapping transforms / Approve&apply.
    # Emit an explicit non-mutative remediation — never pretend review_mappings applies.
    if _is_duplicate_key_blocker(blocker_text, gate_ids):
        actions = [
            a for a in actions
            if a.get("kind") not in {
                "open_bad_data_fix",
                "normalize_control_chars",
                "quarantine_and_rerun",
                "review_mappings",
            }
        ]
        if not any(a.get("kind") == "fix_source_keys" for a in actions):
            actions.insert(0, {
                "kind": "fix_source_keys",
                "label": (
                    "Duplicate identity keys — cannot auto-apply. "
                    "Dedupe the source on id, map a different unique key, "
                    "or change sync mode if uniqueness is not required — then Re-run Validate."
                ),
                "mapping_applyable": False,
            })
        if not any(a.get("kind") == "review_mappings" for a in actions):
            actions.append({
                "kind": "review_mappings",
                "label": (
                    "Open Map to change which column is treated as identity "
                    "(Approve & apply will not remove source duplicates)"
                ),
                "mapping_applyable": False,
            })
        return actions

    # Declared-type / dry-run type mismatches → Widen/remap, never Strip-first.
    if _is_type_mismatch_blocker(blocker_text):
        pairs = _parse_type_mismatch_pairs(blocker_text)
        if not pairs:
            for src, tgt in _parse_type_mismatch_columns(blocker_text):
                pairs.append((src, "", tgt, ""))
        for src, src_type, tgt, tgt_type in pairs:
            to_type = _remap_to_type_for_mismatch(src_type, tgt_type)
            key = ("change_target_type", src, to_type)
            if key not in seen:
                seen.add(key)
                if to_type.upper() == "UUID":
                    label = (
                        f"Keep '{src}' as UUID — create-new writes CHAR(36)/UUID "
                        f"(bare VARCHAR/TEXT is not a fix; Strip/Quarantine cannot help)"
                    )
                else:
                    label = (
                        f"Remap '{src}' off typed {tgt or tgt_type or 'column'} → {to_type} "
                        "(Strip/Quarantine cannot fix type mismatches)"
                    )
                actions.append({
                    "kind": "change_target_type",
                    "column": src,
                    "target": tgt,
                    "to_type": to_type,
                    "label": label,
                    "requires_ddl": True,
                })
        if not any(a.get("kind") == "change_target_type" for a in actions):
            actions.append({
                "kind": "review_mappings",
                "label": "Open Map — widen or remap the incompatible typed column",
            })
        elif not any(a.get("kind") == "review_mappings" for a in actions):
            actions.append({
                "kind": "review_mappings",
                "label": "Review mappings (type mismatch — not an encoding issue)",
            })

    if _is_encoding_blocker(blocker_lower):
        # One CTA only — Strip / Quarantine live inside Fix bad data drawer.
        actions = [
            a for a in actions
            if a.get("kind") not in {"normalize_control_chars", "quarantine_and_rerun"}
        ]
        if not any(a.get("kind") == "open_bad_data_fix" for a in actions):
            actions.append({
                "kind": "open_bad_data_fix",
                "label": "Fix bad data…",
            })
    elif "g8_reconciliation" in gate_ids or "identity transform" in blocker_lower or "identity mapping" in blocker_lower:
        if not any(a.get("kind") == "review_mappings" for a in actions):
            actions.append({
                "kind": "review_mappings",
                "label": "Review mappings — identity/fingerprint mismatch (not Strip)",
            })
    elif any(
        gid in gate_ids or (gid or "").startswith("g5")
        for gid in ("g5_dry_run", "g5_transform", "g9_data_integrity", "dry_run")
    ) or "dry-run" in blocker_lower:
        # Generic dry-run / integrity without encoding — Map review only.
        # Never open the encoding-centric Bad Data drawer as the default CTA.
        if not _is_type_mismatch_blocker(blocker_text):
            if not any(a.get("kind") == "review_mappings" for a in actions):
                actions.append({
                    "kind": "review_mappings",
                    "label": "Review mappings",
                })
            # Never open the encoding-centric Fix-bad-data drawer for nulls /
            # required-field integrity — Strip/Quarantine cannot invent values.
    if "g4_mapping_confidence" in gate_ids:
        if not any(a.get("kind") == "review_mappings" for a in actions):
            actions.append({"kind": "review_mappings", "label": "Review and approve low-confidence mappings"})
    if "g6_target_ddl" in gate_ids:
        if not any(a.get("kind") == "review_mappings" for a in actions):
            actions.append({
                "kind": "review_mappings",
                "label": "Fix DDL / remap — not an encoding issue (Strip will not help)",
            })
        # Never push Fix-bad-data for DDL.
        actions = [a for a in actions if a.get("kind") not in {"open_bad_data_fix", "normalize_control_chars", "quarantine_and_rerun"}]
    if "schema_drift" in gate_ids:
        actions.append({"kind": "rerun_mapping", "label": "Re-run mapping to accept the new schema"})
    if {"g1_source", "g2_destination"} & gate_ids:
        actions.append({
            "kind": "check_connection",
            "label": "Open Connectors — fix credentials / Auth source, then Test",
        })
    return actions


def _deterministic_narrative(
    passed: bool,
    issues: list[dict[str, Any]],
    column_fixes: list[dict[str, Any]],
    *,
    decision: str = "",
) -> str:
    if passed and decision == "approve":
        return "All preflight gates passed. This transfer is safe to run."
    if passed:
        return (
            "Checks cleared with a review-grade decision — not production-approved. "
            "Re-run Validate or acknowledge remaining polarity risks before Execute unlocks."
        )
    lines: list[str] = []
    hard_issues = [i for i in issues if i.get("severity") != "warning"]
    warn_issues = [i for i in issues if i.get("severity") == "warning"]
    gate_titles = [i["title"] for i in hard_issues]
    blocker_blob = " ".join(
        f"{i.get('title')} {i.get('what')} {i.get('why')}" for i in hard_issues
    )
    if _is_duplicate_key_blocker(blocker_blob):
        lines.append(
            "Root cause: duplicate identity keys in the Validate sample. "
            "Data integrity, Target DDL, and Sample reconciliation all failed for the same reason. "
            "Mapping Approve & apply cannot dedupe source rows — fix the source key, "
            "pick a different identity column on Map, or adjust sync mode, then Re-run Validate."
        )
        for i in hard_issues[:3]:
            what = str(i.get("what") or i.get("title") or "").strip()
            if what:
                lines.append(f"• {what}")
        warn_cols = [c for c in column_fixes if c.get("severity") == "warn"]
        if warn_cols:
            cols = ", ".join(str(c.get("column") or "?") for c in warn_cols[:8])
            lines.append(
                f"Warnings (not blockers): timestamp normalize at write for {cols}."
            )
        return "\n".join(lines)

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
    if warn_cols and not hard_cols:
        for c in warn_cols:
            lines.append(f"• {c['suggested_fix']}")
    elif warn_cols:
        cols = ", ".join(str(c.get("column") or "?") for c in warn_cols[:8])
        lines.append(f"Warnings (not blockers): {len(warn_cols)} column note(s) — {cols}.")
    if not hard_cols and not warn_cols:
        for i in hard_issues:
            lines.append(f"• {i['title']}: {i['why']} Fix: {i['fix']}")
        for i in warn_issues[:3]:
            lines.append(f"• Warning — {i['title']}: {i.get('what') or i.get('why')}")
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
        "You are Datawrap's validation assistant. Explain data-transfer preflight "
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
        except Exception as exc:
            logger.warning("Validation-assistant provider %s failed: %s", provider.name, exc)
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
    proof = preflight.get("proof_bundle") or {}
    transfer_decision = proof.get("transfer_decision") or {}
    decision = str(transfer_decision.get("decision") or "").strip().lower()
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
            "title": blocker_title(
                gate_id,
                b.get("message", ""),
                catalog_title=guidance.get("title", ""),
            ),
            "severity": "block",
            "what": b.get("message", ""),
            "why": guidance.get("why", ""),
            "fix": guidance.get("fix", ""),
            "examples": guidance.get("examples", []),
            "columns": columns,
            "detail_messages": [str(n) for n in nested][:10],
        })

    column_fixes = _merge_column_fixes(
        _coercion_column_fixes(coercion_report),
        _population_fit_column_fixes(
            preflight.get("population_fit") if isinstance(preflight.get("population_fit"), dict) else {},
            table_exists=bool(
                preflight.get("destination_table_exists")
                or (preflight.get("proof_bundle") or {}).get("destination_table_exists")
            ),
        ),
    )
    # Surface warn-only columns as advisory issues even when nothing blocks.
    for c in column_fixes:
        if c["severity"] == "warn":
            sentinel = int(c.get("sentinel_nulls") or 0)
            # Coercion report also warns on ISO→warehouse temporal normalize
            # (wire_normalize) with zero sentinel nulls — do not mislabel those
            # as "placeholder → NULL" (that scared operators on healthy routes).
            if sentinel > 0:
                title = "Placeholder values become NULL"
                what = f"{sentinel} placeholder value(s) in '{c['column']}'"
            else:
                title = "Type normalize at write"
                what = (
                    c.get("suggested_fix")
                    or f"Column '{c['column']}' will be normalized for the destination type"
                )
            issues.append({
                "gate": "g3_schema_contract",
                "title": title,
                "severity": "warning",
                "what": what,
                "why": explain_issue("lossy type coercion", dest_kind=dest_kind).get("why", ""),
                "fix": c["suggested_fix"],
                "examples": [],
                "columns": [c["column"]],
                "detail_messages": [],
            })

    actions = _suggested_actions(blockers, column_fixes)
    deterministic = _deterministic_narrative(
        passed, issues, column_fixes, decision=decision,
    )

    narrative, provider = deterministic, "deterministic"
    if not passed and use_llm:
        narrative, provider = _llm_narrative(deterministic, issues)

    hard_count = sum(1 for i in issues if i["severity"] != "warning")
    approved = passed and decision == "approve"
    summary = (
        "Validation passed — safe to run."
        if approved
        else (
            "Validation review-grade — not safe to run until decision is approve."
            if passed
            else f"Validation blocked: {hard_count} issue(s), {len(column_fixes)} column(s) need attention."
        )
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
