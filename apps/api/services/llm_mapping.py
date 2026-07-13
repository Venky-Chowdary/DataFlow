"""LLM-assisted column mapping — hybrid with deterministic BM25 baseline."""

from __future__ import annotations

import json
import re
from typing import Any


# PII patterns we never want to send to a third-party LLM in sample data.
_PII_RE = re.compile(
    r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"  # email
    r"|(\b\d{3}-\d{2}-\d{4}\b)"  # SSN
    r"|(\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b)"  # credit card
    r"|(\b\d{3}-\d{3}-\d{4}\b)"  # phone
    r"|((?:\d{1,3}\.){3}\d{1,3})"  # IPv4
    r"|(https?://[^\s]+)",  # URL
    re.IGNORECASE,
)


def _sanitize_sample_value(value: str) -> str:
    """Replace PII-like sample values with placeholders before sending to an LLM."""
    if not isinstance(value, str):
        value = str(value)
    if not value:
        return value
    if _PII_RE.search(value):
        return "<redacted>"
    return value


def _sanitize_samples(
    samples: dict[str, list[str]] | None,
) -> dict[str, list[str]]:
    """Mask PII in the sample values used for LLM prompts."""
    if not samples:
        return {}
    return {
        col: [_sanitize_sample_value(v) for v in vals]
        for col, vals in samples.items()
    }

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
) -> str:
    from src.ai.llm.prompts import COLUMN_MAPPING_PROMPT

    sanitized_samples = _sanitize_samples(source_samples)

    context_lines = []
    if baseline:
        context_lines.append("Deterministic baseline (use as hints, improve if wrong):")
        for m in baseline[:20]:
            context_lines.append(
                f"  {m.get('source')} -> {m.get('target')} "
                f"(conf={m.get('confidence', 0):.2f}, review={m.get('requires_review', False)})"
            )
    if sanitized_samples:
        for col in source_columns[:12]:
            samples = sanitized_samples.get(col, [])[:3]
            if samples:
                context_lines.append(f"  samples[{col}]: {samples}")

    return COLUMN_MAPPING_PROMPT.format(
        source_columns=source_columns,
        target_columns=target_columns,
        source_samples=sanitized_samples,
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
    different column is the runner-up.  If the gap is small and the mapping is not an exact
    identity match, flag it for review.
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

    reason = str(llm.get("reasoning") or base.get("reasoning") or "")
    is_exact = source.lower().strip() == target or reason.startswith("Exact")
    requires_review = score_gap < 0.08 and not is_exact
    return score_gap, requires_review


def llm_provider_available() -> bool:
    try:
        from src.ai.llm.provider import (
            DataTransferAnthropicProvider,
            DataTransferOpenAIProvider,
            DataTransferOllamaProvider,
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
    enabled: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Merge LLM suggestions over BM25/Hungarian baseline. Returns (mappings, meta)."""
    meta: dict[str, Any] = {
        "llm_used": False,
        "llm_provider": None,
        "llm_error": None,
        "strategy": "deterministic_only",
    }

    if not enabled or not target_columns or not source_columns:
        return baseline_mappings, meta

    if not llm_provider_available():
        meta["llm_error"] = "no_cloud_or_local_llm"
        return baseline_mappings, meta

    try:
        from src.ai.llm.fallback import DataTransferFallbackChain

        chain = DataTransferFallbackChain()
        prompt = _build_prompt(source_columns, target_columns, source_samples, baseline_mappings)
        response = chain.generate(prompt, system=_LLM_SYSTEM)
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

        merged: list[dict[str, Any]] = []
        used_targets: set[str] = set()
        baseline_by_source = {m["source"]: m for m in baseline_mappings}

        for src in source_columns:
            base = baseline_by_source.get(src)
            llm = llm_by_source.get(src)
            if llm and llm["target"].lower() not in used_targets:
                pick = {**(base or {}), **llm}
                if base and llm["confidence"] < base.get("confidence", 0):
                    pick = {**llm, **base, "reasoning": f"{llm['reasoning']} · baseline={base.get('confidence', 0):.0%}"}
                score_gap, requires_review = _compute_llm_review(src, llm, base)
                pick["score_gap"] = score_gap
                pick["requires_review"] = requires_review
                pick["method"] = "hybrid_llm"
                pick["agent"] = "LLMMappingAgent"
                merged.append(pick)
                used_targets.add(pick["target"].lower())
            elif base:
                merged.append(base)

        for src, llm in llm_by_source.items():
            if src not in {m["source"] for m in merged} and llm["target"].lower() not in used_targets:
                merged.append(llm)
                used_targets.add(llm["target"].lower())

        meta.update({
            "llm_used": True,
            "llm_provider": response.provider,
            "strategy": "hybrid_llm_bm25",
            "llm_mapping_count": len(llm_by_source),
        })
        return merged or baseline_mappings, meta

    except Exception as exc:
        meta["llm_error"] = str(exc)[:200]
        return baseline_mappings, meta
