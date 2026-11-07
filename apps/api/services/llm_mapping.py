"""LLM-assisted column mapping — hybrid with deterministic BM25 baseline."""

from __future__ import annotations

import json
import re
from typing import Any

from services.llm_policy import is_llm_enabled, is_pii_masking_enabled, mask_pii_samples


def _sanitize_samples(samples: dict[str, list[str]] | None) -> dict[str, list[str]]:
    """Mask PII in the sample values used for LLM prompts."""
    if not is_pii_masking_enabled():
        return {}
    return mask_pii_samples(samples)


_IDENTITY_TRANSFORMS = frozenset({"", "none", "identity", "null"})


def _norm_transform(value: object) -> str:
    return str(value or "").strip().lower()


def _hold_invented_transform(
    pick: dict[str, Any],
    base: dict[str, Any] | None,
    llm: dict[str, Any],
) -> dict[str, Any]:
    """Never auto-apply an LLM-invented transform — require human accept on Map.

    Deterministic baseline transform (if any) stays applied; the LLM proposal is
    surfaced as ``suggested_transform`` with ``requires_review`` forced.
    """
    llm_xf = _norm_transform(llm.get("transform"))
    base_xf = _norm_transform(
        (base or {}).get("transform") or (base or {}).get("transformation")
    )
    if not llm_xf or llm_xf in _IDENTITY_TRANSFORMS or llm_xf == base_xf:
        return pick
    suggested = llm.get("transform") or llm.get("transformation")
    pick["llm_invented_transform"] = True
    pick["suggested_transform"] = suggested
    if base is not None and ("transform" in base or "transformation" in base):
        pick["transform"] = base.get("transform", base.get("transformation"))
    else:
        # Do not stamp transform=None — that forces Execute to "none" and blocks
        # deterministic type-driven transforms (integer/decimal). Leave unset.
        pick.pop("transform", None)
        pick.pop("transformation", None)
    pick["requires_review"] = True
    note = (
        f"LLM suggested transform '{suggested}' — human accept required before Execute"
    )
    reason = str(pick.get("reasoning") or "").strip()
    pick["reasoning"] = f"{reason} · {note}" if reason else note
    return pick

_LLM_SYSTEM = (
    "You are a data engineering expert. Map source columns to destination columns. "
    "Respond with valid JSON only. Never invent destination columns not in the target list."
)


def _extract_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return None


def _build_prompt(
    source_columns: list[str],
    target_columns: list[str],
    source_samples: dict[str, list[str]] | None,
    baseline: list[dict[str, Any]],
    *,
    source_types: dict[str, str] | None = None,
    source_profiles: dict[str, dict[str, int | bool]] | None = None,
) -> str:
    from src.ai.llm.prompts import COLUMN_MAPPING_PROMPT

    from services.ai_egress import (
        column_profiles_without_cells,
        metadata_only_enabled,
    )

    meta_only = metadata_only_enabled()
    sanitized_samples: dict[str, list[str]] = {}
    if not meta_only:
        sanitized_samples = _sanitize_samples(source_samples)

    context_lines = []
    if baseline:
        context_lines.append("Deterministic baseline (use as hints, improve if wrong):")
        for m in baseline[:20]:
            context_lines.append(
                f"  {m.get('source')} -> {m.get('target')} "
                f"(conf={m.get('confidence', 0):.2f}, review={m.get('requires_review', False)})"
            )
    types = {
        str(k): str(v)
        for k, v in dict(source_types or {}).items()
        if k and str(v or "").strip()
    }
    if types:
        context_lines.append("Source types (declared/inferred; no cell values):")
        for col in source_columns[:40]:
            if col in types:
                context_lines.append(f"  {col}: {types[col]}")
    profiles = dict(source_profiles or {})
    if not profiles and source_samples:
        profiles = column_profiles_without_cells(source_samples)
    if profiles:
        context_lines.append("Column profiles (aggregates only; no cell values):")
        for col in source_columns[:40]:
            prof = profiles.get(col)
            if not isinstance(prof, dict):
                continue
            context_lines.append(
                f"  {col}: n={prof.get('n', 0)} nonempty={prof.get('non_empty', 0)} "
                f"max_len={prof.get('max_len', 0)} numeric={prof.get('looks_numeric', False)}"
            )
    if sanitized_samples:
        for col in source_columns[:12]:
            samples = sanitized_samples.get(col, [])[:3]
            if samples:
                context_lines.append(f"  samples[{col}]: {samples}")

    return COLUMN_MAPPING_PROMPT.format(
        source_columns=source_columns,
        target_columns=target_columns,
        source_samples=(
            "[withheld: metadata-only]"
            if meta_only
            else sanitized_samples
        ),
        context="\n".join(context_lines) if context_lines else "None",
    )


def _normalize_llm_mapping(
    item: dict[str, Any],
    target_columns: list[str],
    source_columns: list[str],
) -> dict[str, Any] | None:
    src = str(item.get("source", "")).strip()
    tgt = str(item.get("target", "")).strip()
    if not src or src not in source_columns or not tgt:
        return None
    targets_lower = {t.lower(): t for t in target_columns}
    resolved = targets_lower.get(tgt.lower(), tgt if tgt in target_columns else None)
    if not resolved:
        return None
    conf = float(item.get("confidence", 0.82))
    conf = max(0.0, min(1.0, conf))
    return {
        "source": src,
        "target": resolved,
        "confidence": conf,
        "reasoning": str(item.get("reason", item.get("reasoning", "LLM semantic match"))),
        "transform": item.get("transformation") or item.get("transform"),
        "method": "llm",
        "requires_review": conf < 0.88,
        "score_gap": 0.12,
    }


def _compute_llm_review(
    source: str,
    llm: dict[str, Any],
    base: dict[str, Any] | None,
) -> tuple[float, bool]:
    """Compute score gap and review flag from LLM confidence and baseline alternatives.

    The LLM confidence is the winner; the strongest baseline alternative that targets a
    different column is the runner-up. Exact-name identity no longer exempts a narrow
    gap — operators must approve ambiguous remaps before Execute.
    """
    winner_conf = llm["confidence"]
    target = llm["target"].lower()
    runner_up = 0.0
    if base and isinstance(base.get("alternatives"), list):
        runner_up = max(
            (
                a.get("confidence", 0.0)
                for a in base["alternatives"]
                if a.get("target", "").lower() != target
            ),
            default=0.0,
        )
    score_gap = max(round(winner_conf - runner_up, 3), 0.0)
    requires_review = score_gap < 0.08
    return score_gap, requires_review


def llm_provider_available() -> bool:
    try:
        from src.ai.llm.provider import (
            DataTransferAnthropicProvider,
            DataTransferOllamaProvider,
            DataTransferOpenAIProvider,
        )
        return any(
            p.is_available()
            for p in (
                DataTransferAnthropicProvider(),
                DataTransferOpenAIProvider(),
                DataTransferOllamaProvider(),
            )
        )
    except Exception:
        return False


def refine_mappings_with_llm(
    baseline_mappings: list[dict[str, Any]],
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_samples: dict[str, list[str]] | None = None,
    source_types: dict[str, str] | None = None,
    enabled: bool = True,
    job_id: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Merge LLM suggestions over BM25/Hungarian baseline. Returns (mappings, meta)."""
    from services.ai_egress import (
        column_profiles_without_cells,
        egress_scope,
        last_manifest,
        metadata_only_enabled,
    )

    meta: dict[str, Any] = {
        "llm_used": False,
        "llm_provider": None,
        "llm_error": None,
        "strategy": "deterministic_only",
        "ai_metadata_only": metadata_only_enabled(),
        "ai_egress": None,
    }

    if not enabled or not is_llm_enabled() or not target_columns or not source_columns:
        if not is_llm_enabled():
            meta["llm_policy"] = "disabled"
        return baseline_mappings, meta

    if not is_pii_masking_enabled():
        meta["llm_policy"] = "pii_masking_required"
        return baseline_mappings, meta

    if not llm_provider_available():
        meta["llm_error"] = "no_cloud_or_local_llm"
        return baseline_mappings, meta

    try:
        from src.ai.llm.fallback import DataTransferFallbackChain

        chain = DataTransferFallbackChain()
        profiles = column_profiles_without_cells(source_samples)
        outbound_samples = None if metadata_only_enabled() else source_samples
        prompt = _build_prompt(
            source_columns,
            target_columns,
            outbound_samples,
            baseline_mappings,
            source_types=source_types,
            source_profiles=profiles,
        )
        with egress_scope(
            job_id=job_id,
            purpose="column_mapping",
            column_names=source_columns,
            source_types=source_types,
        ):
            response = chain.generate(prompt, system=_LLM_SYSTEM)
        meta["ai_egress"] = last_manifest()
        if not response.success:
            meta["llm_error"] = "generation_failed"
            return baseline_mappings, meta

        parsed = _extract_json(response.content)
        if not parsed or "mappings" not in parsed:
            meta["llm_error"] = "invalid_json"
            return baseline_mappings, meta

        llm_by_source: dict[str, dict[str, Any]] = {}
        for raw in parsed.get("mappings", []):
            if not isinstance(raw, dict):
                continue
            norm = _normalize_llm_mapping(raw, target_columns, source_columns)
            if norm:
                llm_by_source[norm["source"]] = norm

        if not llm_by_source:
            meta["llm_error"] = "no_valid_mappings"
            return baseline_mappings, meta

        # ITEM 1 — LLM never decides fidelity. Baseline (deterministic Map) is
        # the authority for source→target and Execute transform. LLM output is
        # attached as suggestions only; operators must approve remaps on Map.
        merged: list[dict[str, Any]] = []
        used_targets: set[str] = set()
        baseline_by_source = {m["source"]: m for m in baseline_mappings}
        suggestion_count = 0

        from services.mapping_engine_contract import is_operator_locked

        for src in source_columns:
            base = baseline_by_source.get(src)
            llm = llm_by_source.get(src)
            if not base:
                # No deterministic row — do not invent a mapping from LLM alone.
                continue
            pick = dict(base)
            tgt = str(pick.get("target") or "").lower()
            if tgt:
                used_targets.add(tgt)
            if not llm:
                merged.append(pick)
                continue

            llm_tgt = str(llm.get("target") or "")
            base_tgt = str(base.get("target") or "")
            differs = (
                llm_tgt.lower() != base_tgt.lower()
                or (
                    _norm_transform(llm.get("transform"))
                    not in _IDENTITY_TRANSFORMS
                    and _norm_transform(llm.get("transform"))
                    != _norm_transform(base.get("transform") or base.get("transformation"))
                )
            )
            if differs or is_operator_locked(base):
                pick["engine_suggestion"] = {
                    "target": llm.get("target"),
                    "transform": llm.get("transform"),
                    "confidence": llm.get("confidence"),
                    "reasoning": llm.get("reasoning"),
                    "suppressed": True,
                    "reason": (
                        "operator_locked_mapping_not_silently_overwritten"
                        if is_operator_locked(base)
                        else "llm_suggest_only_deterministic_decides"
                    ),
                }
                if llm_tgt and llm_tgt.lower() != base_tgt.lower():
                    pick["suggested_target"] = llm_tgt
                suggestion_count += 1
                pick["requires_review"] = True
            score_gap, _review = _compute_llm_review(src, llm, base)
            pick["score_gap"] = score_gap
            pick = _hold_invented_transform(pick, base, llm)
            pick["llm_consulted"] = True
            pick["method"] = str(base.get("method") or "deterministic")
            # Telemetry only — agent tag must not imply LLM owned the decision.
            if pick.get("agent") in {None, "", "LLMMappingAgent"}:
                pick["agent"] = base.get("agent") or "DeterministicMapper"
            merged.append(pick)

        meta.update({
            "llm_used": True,
            "llm_provider": response.provider,
            "strategy": "llm_suggest_deterministic_decide",
            "llm_mapping_count": len(llm_by_source),
            "llm_suggestion_count": suggestion_count,
            "llm_decides": False,
        })
        return merged or baseline_mappings, meta

    except Exception as exc:
        meta["llm_error"] = str(exc)[:200]
        return baseline_mappings, meta
