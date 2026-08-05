"""Wave 45 — ask Pilot ≥1000 real prompts and score answer quality.

Goes beyond NL routing: every corpus prompt is sent through ``DataPilotAgent.chat``
(local engine) and graded for non-empty honest answers, Confirm on mutations,
and no crash / traceback dumps.
"""

from __future__ import annotations

import os
import re
from collections import Counter
from dataclasses import dataclass, field

from src.ai.copilot.pilot_agent import DataPilotAgent
from src.ai.copilot.prompt_corpus import PromptCase, iter_prompt_corpus
from src.ai.copilot.tools import infer_tools_from_message

_BAD_ANSWER = re.compile(
    r"(traceback \(most recent|internal server error|noneType|object has no attribute|"
    r"\bexception:\s|failed to import|module not found)",
    re.I,
)

# Families that stage Confirm when tools succeed; empty workspace may clarify instead.
_MUTATE_FAMILIES = frozenset({
    "transfer_start", "remediate", "schedule", "create_connector",
})

# Families that should not invent a mutating Confirm card.
_READ_FAMILIES = frozenset({
    "meta", "navigate", "operate", "aggregate", "sample", "schema", "query",
    "transfer_plan", "govern", "jobs", "preflight", "inventory",
    "aggregate_elliptical", "result_followup", "quality", "quality_gates",
    "schema_diff", "schema_map", "greeting", "refuse", "nl_accuracy",
    "validate_triage", "product_faq",
})


@dataclass
class AnswerGrade:
    prompt: str
    family: str
    ok: bool
    reasons: list[str] = field(default_factory=list)
    method: str = ""
    answer_preview: str = ""
    tools: list[str] = field(default_factory=list)
    pending_types: list[str] = field(default_factory=list)


def grade_chat_response(case: PromptCase, resp) -> AnswerGrade:
    """Score one Pilot chat turn for operator-facing answer quality."""
    reasons: list[str] = []
    answer = (resp.answer or "").strip()
    method = getattr(resp, "method", "") or ""
    tools = [t.get("name") for t in (resp.tools_used or []) if t.get("name")]
    pending = list(resp.pending_actions or [])
    pending_types = [str(a.get("type") or "") for a in pending]
    clarify = (getattr(resp, "needs_clarification", "") or "").strip()

    if not answer and not clarify:
        reasons.append("empty_answer")
    elif len(answer) < 12 and case.family not in {"greeting"}:
        reasons.append(f"too_short:{len(answer)}")

    if answer and _BAD_ANSWER.search(answer):
        reasons.append("traceback_or_crash_text")

    if method and method not in {
        "pilot_local_engine", "greeting", "local_tools", "clarify",
    } and not method.startswith("pilot_"):
        # Local-primary: cloud methods are optional polish only.
        if method in {"openai_agent", "anthropic_agent", "ollama_agent"}:
            reasons.append(f"unexpected_cloud_method:{method}")

    # Routing fidelity on the chat path (tools actually used or planned).
    planned = {n for n, _ in infer_tools_from_message(case.prompt)}
    used = set(tools)
    if case.expected_tools and case.family not in {"greeting", "refuse"}:
        if not ((planned | used) & case.expected_tools):
            reasons.append(
                f"miss_expected_tools want={sorted(case.expected_tools)} "
                f"planned={sorted(planned)} used={sorted(used)}"
            )
    for banned in case.must_not:
        if banned in planned or banned in used:
            reasons.append(f"banned_tool:{banned}")

    # Sensitive ops: Confirm card OR honest clarify / missing-resource answer.
    if case.family in _MUTATE_FAMILIES:
        has_confirm = any(
            (a.get("risk") == "mutate") or a.get("type") in {
                "start_transfer", "run_schedule", "studio", "create_connector",
            }
            for a in pending
        )
        honest = bool(clarify) or any(
            w in answer.lower()
            for w in (
                "confirm", "which", "not found", "no connector", "no schedule",
                "pipeline", "name a saved", "could not", "couldn't", "need a",
                "missing", "fix host", "credentials",
            )
        )
        if not has_confirm and not honest:
            reasons.append("mutate_without_confirm_or_clarify")

    if case.family == "refuse":
        if any(t in {"start_transfer", "create_connector"} for t in pending_types):
            reasons.append("refuse_staged_mutation")
        if "start_transfer" in used:
            reasons.append("refuse_ran_start_transfer")

    if case.family in {"meta", "product_faq", "quality_gates"} and answer:
        low = answer.lower()
        if case.family == "meta" and not any(
            w in low for w in ("pilot", "transfer", "confirm", "dataflow", "help")
        ):
            reasons.append("meta_answer_offtopic")

    if case.family == "greeting" and answer:
        if len(answer) < 8:
            reasons.append("greeting_too_thin")

    return AnswerGrade(
        prompt=case.prompt,
        family=case.family,
        ok=not reasons,
        reasons=reasons,
        method=method,
        answer_preview=answer[:180].replace("\n", " "),
        tools=tools,
        pending_types=pending_types,
    )


def run_answer_corpus(
    *,
    limit: int | None = None,
    families: set[str] | None = None,
    session_prefix: str = "wave45",
) -> dict:
    """Ask Pilot every corpus prompt (or a slice) and return a score report."""
    os.environ["DATAFLOW_PILOT_ENGINE"] = "local"
    cases = iter_prompt_corpus()
    if families:
        cases = [c for c in cases if c.family in families]
    if limit is not None:
        cases = cases[:limit]

    agent = DataPilotAgent()
    grades: list[AnswerGrade] = []
    for i, case in enumerate(cases):
        try:
            resp = agent.chat(
                case.prompt,
                data_context={"pilot_session_id": f"{session_prefix}-{i}"},
            )
            grades.append(grade_chat_response(case, resp))
        except Exception as exc:  # noqa: BLE001 — corpus must never crash the suite
            grades.append(AnswerGrade(
                prompt=case.prompt,
                family=case.family,
                ok=False,
                reasons=[f"exception:{type(exc).__name__}:{exc}"],
            ))

    fails = [g for g in grades if not g.ok]
    by_family = Counter(g.family for g in grades)
    fail_family = Counter(g.family for g in fails)
    reason_counts = Counter(r for g in fails for r in g.reasons)

    return {
        "total": len(grades),
        "passed": len(grades) - len(fails),
        "failed": len(fails),
        "pass_rate": round((len(grades) - len(fails)) / max(len(grades), 1), 4),
        "by_family": dict(by_family),
        "fail_by_family": dict(fail_family),
        "top_reasons": reason_counts.most_common(25),
        "failures": [
            {
                "family": g.family,
                "prompt": g.prompt,
                "reasons": g.reasons,
                "method": g.method,
                "tools": g.tools,
                "pending": g.pending_types,
                "answer": g.answer_preview,
            }
            for g in fails[:80]
        ],
    }


def test_answer_corpus_at_least_1000_local_chats():
    """Full local chat sweep — ≥1000 prompts graded for answer quality."""
    report = run_answer_corpus()
    assert report["total"] >= 1000, report["total"]
    # Allow a small honest-clarify budget (missing connectors/schedules in CI).
    # Hard failures (traceback / exception / empty) must stay near zero.
    hard = [
        f
        for f in report["failures"]
        if any(
            r.startswith(("exception:", "traceback", "empty_answer", "banned_tool"))
            or r.startswith("miss_expected")
            for r in f["reasons"]
        )
    ]
    assert report["pass_rate"] >= 0.98, (
        f"pass_rate={report['pass_rate']} failed={report['failed']} "
        f"top={report['top_reasons'][:10]} sample={report['failures'][:15]}"
    )
    assert len(hard) == 0, (
        f"{len(hard)} hard failures:\n"
        + "\n".join(
            f"{h['family']}:{h['prompt']!r} -> {h['reasons']}" for h in hard[:25]
        )
    )
