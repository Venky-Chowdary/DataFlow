"""Parity Studio: the plain-language explanation of a parity report.

The model never decides anything here. It receives the classifier's output and
catalog metadata — table and column names, column types, cause codes, counts,
the fix menu — and writes the explanation, optionally reordering the proposed
fixes. Row values are included only where the existing AI egress policy
(:mod:`services.ai_egress`, metadata-only by default) allows cells to leave and
the column is not protected; the outbound prompt passes the same egress gate and
manifest as every other generate call. When no model is configured, it times
out or answers badly, :func:`template_explanation` produces the same structure
from templates.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
from typing import Any

from services.brand_env import getenv_brand
from services.parity_state import now_iso

logger = logging.getLogger(__name__)

_SYSTEM = (
    "You explain data-migration parity reports to non-engineers. Use only the facts given. "
    "Never change a verdict, a count or a cause, and never propose an action that is not in "
    "the given fix list. Answer with JSON: {\"text\": \"<at most 120 words>\", "
    "\"fix_order\": [<fix ids from the list, most useful first>]}."
)
_JSON = re.compile(r"\{.*\}", re.S)


def _timeout_seconds() -> float:
    try:
        return max(1.0, min(120.0, float(getenv_brand("PARITY_EXPLAIN_TIMEOUT", "20") or 20)))
    except ValueError:
        return 20.0


def model_safe_detail(cause: dict[str, Any]) -> str:
    """A cause's detail as a model may read it: never who accepted it, nor their note.

    An operator's acceptance is a decision by a named person; the model gets
    the fact (``accepted_by_operator``) and the classifier's diagnosis only.
    """
    detail = str(cause.get("detail") or "")
    if cause.get("code") != "accepted_by_operator":
        return detail
    _accepted, _sep, diagnosed = detail.partition(" Diagnosed as:")
    return "Accepted by an approver." + (f" Diagnosed as:{diagnosed}" if diagnosed else "")


def grounded_facts(report: dict[str, Any], *, include_values: bool) -> dict[str, Any]:
    """Everything the model may see: classifier output and catalog metadata."""
    types: dict[str, dict[str, str]] = {}
    for ev in report.get("_tables_ev") or []:
        types[ev.name] = {c: t for c, t in ev.dest_types.items() if t}
    diffs = []
    for d in report["differences"]:
        item = {
            "id": d["id"], "table": d["table"], "column": d["column"], "kind": d["kind"],
            "rows": d["rows"], "cause": d["cause"]["code"], "explained": d["cause"]["explained"],
            "detail": model_safe_detail(d["cause"]),
            "destination_type": types.get(d["table"], {}).get(d["column"] or ""),
        }
        if include_values and not d["protected"] and d.get("example"):
            item["example"] = d["example"]
        diffs.append(item)
    return {
        "verdict": report["verdict"],
        "verdict_reason": report["verdict_reason"],
        "parity": report["parity"],
        "confidence": report["confidence"]["statement"],
        "tables": [
            {k: t[k] for k in ("name", "check", "status", "differences", "unexplained")}
            for t in report["tables"]
        ],
        "differences": diffs,
        "fixes": [
            {k: f[k] for k in ("id", "action", "title", "applicable", "status")}
            for f in report["fixes"]
        ],
    }


def _where(diff: dict[str, Any]) -> str:
    return f"{diff['table']}.{diff['column']}" if diff.get("column") else diff["table"]


def template_explanation(report: dict[str, Any]) -> dict[str, Any]:
    """The deterministic explanation: same structure as the model's, from templates."""
    parity = report["parity"]
    lines = [report["verdict_reason"]]
    bad = [d for d in report["differences"] if not d["cause"]["explained"]]
    good = [d for d in report["differences"] if d["cause"]["explained"]]
    for d in bad[:5]:
        lines.append(
            f"{d['rows']:,} row(s) in {_where(d)} ({d['kind'].replace('_', ' ')}): "
            f"{d['cause']['label'].lower()} — {d['cause']['detail']}"
        )
    if good:
        named = "; ".join(f"{_where(d)}: {d['cause']['label'].lower()}" for d in good[:4])
        lines.append(f"{parity['explained']:,} row(s) differ as expected ({named}).")
    proposed = [f for f in report["fixes"] if f["status"] == "proposed" and f["applicable"]]
    if proposed:
        lines.append(f"Proposed fix: {proposed[0]['title']}.")
    lines.append(report["confidence"]["statement"])
    return {"text": " ".join(lines), "source": "template", "generated_at": now_iso(), "fix_order": []}


def _parse(content: str, fix_ids: set[str]) -> tuple[str, list[str]] | None:
    found = _JSON.search(content or "")
    if not found:
        return None
    try:
        data = json.loads(found.group(0))
    except json.JSONDecodeError:
        return None
    text = str(data.get("text") or "").strip() if isinstance(data, dict) else ""
    if not text:
        return None
    order = [f for f in data.get("fix_order") or [] if isinstance(f, str) and f in fix_ids]
    return text[:2000], list(dict.fromkeys(order))


def _generate(prompt: str, job_id: str, columns: list[str]) -> Any:
    from services.ai_egress import egress_scope
    from src.ai.llm.fallback import DataTransferFallbackChain

    with egress_scope(job_id=job_id, purpose="parity_explanation", column_names=columns):
        return DataTransferFallbackChain().generate_prose(prompt, system=_SYSTEM)


def explain_report(report: dict[str, Any]) -> dict[str, Any]:
    """``{text, source, generated_at, fix_order}`` — from the model when one answers in time."""
    from services.ai_egress import metadata_only_enabled
    from services.llm_mapping import llm_provider_available
    from services.llm_policy import is_llm_enabled

    template = template_explanation(report)
    if not is_llm_enabled() or not llm_provider_available():
        return template
    facts = grounded_facts(report, include_values=not metadata_only_enabled())
    prompt = "Parity report facts (JSON):\n" + json.dumps(facts, default=str, sort_keys=True)
    columns = sorted({d["column"] for d in report["differences"] if d.get("column")})
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(_generate, prompt, str(report.get("job_id") or ""), columns)
        response = future.result(timeout=_timeout_seconds())
    except concurrent.futures.TimeoutError:
        logger.info("parity explanation: model timed out; using the template")
        return template
    except Exception as exc:  # noqa: BLE001 - any provider fault falls back to the template
        logger.info("parity explanation: model failed (%s); using the template", type(exc).__name__)
        return template
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    if not getattr(response, "success", False):
        return template
    parsed = _parse(str(getattr(response, "content", "") or ""), {f["id"] for f in report["fixes"]})
    if parsed is None:
        return template
    text, order = parsed
    return {"text": text, "source": "llm", "generated_at": now_iso(), "fix_order": order}


def explain_job(job_id: str, *, actor: str) -> dict[str, Any]:
    """Explain the current report, store the explanation, return the updated report."""
    from services.parity_apply import not_signed
    from services.parity_report import build_parity_report
    from services.parity_state import load_job, state_of, update_state

    job = load_job(job_id)
    not_signed(state_of(job))
    report = build_parity_report(job, reread=True, job_id=job_id)
    explanation = explain_report(report)

    def store(_job: dict[str, Any], state: dict[str, Any]) -> None:
        not_signed(state)
        state["explanation"] = {k: explanation[k] for k in ("text", "source", "generated_at")}
        state["fix_order"] = explanation.get("fix_order") or []

    _job, state, _ = update_state(job_id, store)
    from services.parity_state import audit

    audit("job.parity.explained", job=job, job_id=job_id, actor=actor,
          details={"source": explanation["source"], "version": int(state["version"]),
                   "fix_order": list(state.get("fix_order") or [])})
    report["explanation"] = state["explanation"]
    report["version"] = state["version"]
    order = {fid: i for i, fid in enumerate(state["fix_order"])}
    if order:
        decided = [f for f in report["fixes"] if f["status"] != "proposed"]
        proposed = sorted(
            (f for f in report["fixes"] if f["status"] == "proposed"),
            key=lambda f: order.get(f["id"], len(order)),
        )
        report["fixes"] = decided + proposed
    return report
