"""Natural-language composer for Datawrap Pilot.

Tool results are the evidence. This module is the voice: a lead sentence,
what it means, and the next useful thing to ask or do. It never invents
a count, job id, or connector name that the facts dict did not provide.

Designed so answers feel like a copilot conversation — not a FAQ dump
and not a JSON pretty-print.
"""

from __future__ import annotations

import re
from typing import Any

from .agent import CopilotResponse
from .dialogue_acts import (
    DialogueAct,
    last_assistant_text,
    last_user_text,
)


def compose_greeting(ctx: dict[str, Any] | None = None) -> str:
    ctx = ctx or {}
    connectors = ctx.get("connectors") or []
    jobs = ctx.get("recent_jobs") or []
    n_conn = len(connectors) if isinstance(connectors, list) else 0
    n_jobs = len(jobs) if isinstance(jobs, list) else 0
    failed = 0
    if isinstance(jobs, list):
        failed = sum(
            1
            for j in jobs
            if isinstance(j, dict)
            and str(j.get("status") or "").lower() in {"failed", "error"}
        )

    if n_conn == 0 and n_jobs == 0:
        return (
            "I'm **Datawrap Pilot** — talk to me the way you would a colleague "
            "on the migration desk. I read your live workspace: connectors, "
            "tables, jobs, pipelines, and Validate proof. Nothing moves until "
            "you **Confirm**.\n\n"
            "Start anywhere: *give me a workspace briefing*, *show my jobs*, "
            "or name a table and a saved connector."
        )

    lead = (
        f"I'm **Datawrap Pilot**. Right now I can see **{n_conn}** saved "
        f"connector(s) and **{n_jobs}** recent job(s)"
    )
    if failed:
        lead += f" — **{failed}** of those jobs failed and need a look"
    lead += "."
    return (
        f"{lead}\n\n"
        "Ask in plain language. I can brief the workspace, count or sample a "
        "live table, explain a failed job, plan a transfer (Confirm before "
        "write), or walk a pipeline. Try *what's going on in my workspace?* "
        "or *summarize my pipelines*."
    )


def compose_briefing(facts: dict[str, Any]) -> str:
    """Turn briefing facts into a spoken sitrep. Every number is from ``facts``."""
    if facts.get("empty_workspace"):
        return (
            "This workspace is still empty — no saved connectors, jobs, or "
            "pipelines yet. Add a connector on **Connectors**, or ask me to "
            "save one (I'll stage Confirm). After the first transfer I can "
            "brief you the way an ops lead would: what failed, what's due, "
            "what is waiting on you."
        )

    n_conn = int(facts.get("connector_count") or 0)
    passed = int(facts.get("connectors_passed") or 0)
    failed_c = int(facts.get("connectors_failed") or 0)
    untested = int(facts.get("connectors_untested") or 0)
    n_jobs = int(facts.get("job_count") or 0)
    ok_j = int(facts.get("jobs_ok") or 0)
    fail_j = int(facts.get("jobs_failed") or 0)
    run_j = int(facts.get("jobs_running") or 0)
    n_sched = int(facts.get("schedule_count") or 0)
    en = int(facts.get("schedules_enabled") or 0)
    parked = int(facts.get("schedules_parked") or 0)
    n_ct = int(facts.get("contract_count") or 0)
    unsigned = int(facts.get("contracts_unsigned") or 0)

    lines = ["Here's the workspace as it stands — counts are from live stores, not a demo."]
    conn_bit = f"**{n_conn}** connector(s)"
    if n_conn:
        conn_bit += f" ({passed} last-test passed"
        if failed_c:
            conn_bit += f", {failed_c} failed"
        if untested:
            conn_bit += f", {untested} never tested"
        conn_bit += ")"
    names = [n for n in (facts.get("connector_names") or []) if n]
    if names:
        conn_bit += ": " + ", ".join(f"**{n}**" for n in names[:6])
        if n_conn > 6:
            conn_bit += f", and {n_conn - 6} more"
    lines.append(f"• Connections — {conn_bit}.")

    job_bit = f"**{n_jobs}** recent job(s)"
    if n_jobs:
        job_bit += f" ({ok_j} ok, {fail_j} failed, {run_j} running)"
    latest = str(facts.get("latest_failed_job") or "").strip()
    if latest:
        job_bit += f". Latest failure: {latest}"
    lines.append(f"• Transfers — {job_bit}.")

    sched_bit = f"**{n_sched}** pipeline(s), **{en}** enabled"
    if parked:
        sched_bit += f", **{parked}** parked on an approval"
    nxt = str(facts.get("next_schedule") or "").strip()
    if nxt:
        sched_bit += f". Next due: {nxt}"
    parked_names = [n for n in (facts.get("parked_names") or []) if n]
    if parked_names:
        sched_bit += " — waiting: " + ", ".join(f"**{n}**" for n in parked_names[:4])
    lines.append(f"• Pipelines — {sched_bit}.")

    if n_ct:
        lines.append(
            f"• Contracts — **{n_ct}** on file"
            + (f", **{unsigned}** not signed" if unsigned else ", all signed or active")
            + "."
        )

    attention = [str(a) for a in (facts.get("attention") or []) if a]
    if attention:
        lines.append(
            "What needs you: " + "; ".join(attention) + ". "
            "Ask me about a failed job or a parked pipeline and I'll open the finding."
        )
    else:
        lines.append(
            "Nothing is parked or failed in this snapshot. Ask me to sample a "
            "table, plan a transfer, or explain a route if you want to go deeper."
        )
    return "\n".join(lines)


def compose_thanks() -> str:
    return (
        "You're welcome. If you want the short version of the last answer, "
        "say *summarize that*. If you want the next move, ask *what should I do next?*"
    )


def compose_general(message: str, ctx: dict[str, Any] | None = None) -> str:
    """Honest general-chat path when there is no workspace or product evidence.

    Refusal wording is owned by ``unsupported_question`` — this composer only
    adds the live workspace offer and the Settings → AI path for cloud chat.
    """
    from .unsupported_question import unsupported_question_output

    base = str((unsupported_question_output(message) or {}).get("answer") or "").strip()
    ctx = ctx or {}
    n_conn = len(ctx.get("connectors") or [])
    n_jobs = len(ctx.get("recent_jobs") or [])
    live = ""
    if n_conn or n_jobs:
        live = (
            f" I can see **{n_conn}** connector(s) and **{n_jobs}** recent "
            "job(s) in this workspace if you want to switch to that."
        )
    return (
        f"{base}\n\n"
        "Turn on OpenAI or Anthropic under **Settings → AI** if you want a "
        "cloud model to chat about topics outside this product. Local Pilot "
        "still answers anything I can prove from your connectors, jobs, "
        f"pipelines, Validate runs, and docs.{live}"
    )


_SKIP_SUMMARY_LINE = (
    "Where:",
    "Tip:",
    "Note:",
    "Source:",
    "Optional narration polish",
    "Optional cloud polish",
    "Local Datawrap Pilot still answered",
    "_On your last ask",
)
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_STEP_HEADING = re.compile(
    r"^(Open the |Look for |Review |Return to |Click |Fix and |Remediate |"
    r"Use this path|I can suggest|Enterprise rules|Indexed datasets)",
    re.I,
)


def _strip_markdown_decor(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"^\*\*Short version:\*\*\s*", "", t)
    t = re.sub(r"^_On your last ask[^\n]*\n+", "", t)
    return t.strip()


def _summary_sentences(text: str, *, max_sentences: int) -> list[str]:
    """Real sentences only — never smash 'Open the job Look for' across newlines."""
    raw = _strip_markdown_decor(text)
    sentences: list[str] = []
    for raw_line in raw.replace("\n• ", "\n").replace("\n- ", "\n").splitlines():
        line = raw_line.strip().strip("-• ")
        if not line:
            continue
        if any(line.startswith(p) or p in line[:40] for p in _SKIP_SUMMARY_LINE):
            continue
        if line.startswith("**") and line.endswith("**") and "—" not in line:
            # Section heading leftover: **I can:**
            continue
        if re.match(r"^G\d+\b", line.lstrip("*")):
            gate = line.rstrip(".")
            if not gate.endswith((".", "!", "?")):
                gate += "."
            sentences.append(gate)
            if len(sentences) >= max_sentences:
                return sentences
            continue
        if not any(ch in line for ch in ".!?") and len(line.split()) <= 8:
            if _STEP_HEADING.match(line.lstrip("*")) or "—" not in line:
                continue
        # Lead "Title — definition" is one spoken sentence.
        if " — " in line and line.startswith("**"):
            after = line.split(" — ", 1)[1].strip()
            if after:
                line = after
        for piece in _SENTENCE_SPLIT.split(line):
            piece = piece.strip().strip("*")
            if not piece or any(piece.startswith(p) for p in _SKIP_SUMMARY_LINE):
                continue
            if piece.endswith(":"):
                continue
            if _STEP_HEADING.match(piece) and len(piece.split()) <= 10:
                continue
            if re.match(r"^G\d+\b", piece):
                if not piece.endswith((".", "!", "?")):
                    piece += "."
                sentences.append(piece)
                if len(sentences) >= max_sentences:
                    return sentences
                continue
            if not piece.endswith((".", "!", "?")):
                piece += "."
            sentences.append(piece)
            if len(sentences) >= max_sentences:
                return sentences
    return sentences


def summarize_text(text: str, *, max_sentences: int = 2) -> str:
    """Extractive short version of the last answer — no new claims."""
    raw = (text or "").strip()
    if not raw:
        return (
            "I don't have a previous answer to summarize. Ask me something "
            "about the workspace first."
        )
    parts = _summary_sentences(raw, max_sentences=max(12, max_sentences))
    if not parts:
        cleaned = raw.replace("\n", " ")
        return f"**Short version:** {cleaned[:280].rstrip()}."
    gates = [p for p in parts if re.match(r"^G\d+\b", p)]
    other = [p for p in parts if not re.match(r"^G\d+\b", p)]
    if gates:
        picked_list = other[:1] + gates[: max(max_sentences, 3)]
    else:
        picked_list = other[:max_sentences] or parts[:max_sentences]
    picked = " ".join(picked_list).strip()
    if not picked.endswith((".", "!", "?")):
        picked += "."
    return f"**Short version:** {picked}"


def explain_simpler(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return (
            "There isn't a previous finding to unpack. Point me at a job, "
            "pipeline, or table and I'll explain it in plain language."
        )
    short = summarize_text(raw, max_sentences=3)
    documented = "Source:" in raw or "(Help)" in raw
    if documented:
        return (
            f"{short}\n\n"
            "In plain terms: that is the documented meaning — I did not add a "
            "new claim. Ask *what should I do next?* for the operator move, or "
            "name a job if you want to inspect a live run."
        )
    return (
        f"{short}\n\n"
        "In plain terms: I only report what the workspace stores or what a "
        "live read just returned. If a number is missing, I have not measured "
        "it. Tell me which part is still unclear."
    )


def compose_next_action(
    *,
    facts: dict[str, Any] | None = None,
    last_answer: str = "",
    pending_labels: list[str] | None = None,
) -> str:
    if pending_labels:
        names = ", ".join(f"**{x}**" for x in pending_labels if x)
        return (
            f"The next move is the Confirm card still on this chat: {names}. "
            "Nothing has written yet. Press Confirm if the plan is the one "
            "you meant; otherwise say what to change."
        )
    facts = facts or {}
    attention = [str(a) for a in (facts.get("attention") or []) if a]
    if attention:
        return (
            "Next: clear what is already waiting — "
            + "; ".join(attention)
            + ". Ask me for the failed job or the parked pipeline and I'll "
            "open the finding instead of guessing a fix."
        )
    if last_answer and re.search(
        r"\b(?:Confirm below|press Confirm|Confirm card|Ready to save)\b",
        last_answer,
    ):
        return (
            "Next: review the plan in this thread and Confirm if you want it "
            "to run. I will not start a write from a follow-up 'yes' unless "
            "that Confirm card is used."
        )
    return (
        "Next useful move: *give me a workspace briefing* if you want the "
        "sitrep, or name a table and connector if you want a live count or "
        "sample. I won't invent a transfer you didn't ask for."
    )


def weave_tool_answer(message: str, parts: list[str], *, act: DialogueAct) -> str:
    """Wrap already-rendered tool prose so it reads as one spoken answer."""
    body = "\n\n".join(p.strip() for p in parts if (p or "").strip())
    if not body:
        return ""
    if act == "explain_simpler":
        return explain_simpler(body)
    if act == "summarize_last":
        return summarize_text(body)
    if act == "next_action":
        return compose_next_action(last_answer=body)
    # Conversational lead only when the first line is a raw inventory.
    first = body.split("\n", 1)[0]
    if first.startswith("You have **") or first.startswith("**") and "pipeline" in first.lower():
        return f"Here's what I found.\n\n{body}"
    return body


def compose_history_turn(
    act: DialogueAct,
    *,
    history: list[dict],
    message: str,
    ctx: dict[str, Any] | None = None,
    pending_labels: list[str] | None = None,
) -> CopilotResponse:
    last = last_assistant_text(history)
    if act == "summarize_last":
        answer = summarize_text(last)
        intent = "analytics_help"
    elif act == "explain_simpler":
        answer = explain_simpler(last)
        intent = "product_help"
    elif act == "next_action":
        answer = compose_next_action(
            last_answer=last,
            pending_labels=pending_labels,
        )
        intent = "troubleshooting"
    elif act == "thanks":
        answer = compose_thanks()
        intent = "greeting"
    else:
        answer = compose_general(message, ctx)
        intent = "product_help"
    asked = last_user_text(history)
    if asked and act in {"summarize_last", "explain_simpler"} and asked not in answer:
        answer = f"_On your last ask — “{_clip(asked, 80)}” —_\n\n{answer}"
    return CopilotResponse(
        answer=answer,
        intent=intent,
        confidence=0.2 if act == "general" else 0.72,
        method="pilot_conversation",
        reasoning=f"Dialogue act {act} over prior turn — no new warehouse claim",
        suggested_prompts=_followups_for_act(act),
    )


def compose_greeting_response(ctx: dict[str, Any] | None = None) -> CopilotResponse:
    return CopilotResponse(
        answer=compose_greeting(ctx),
        intent="greeting",
        confidence=0.9,
        method="greeting",
        suggested_prompts=[
            "Give me a workspace briefing",
            "Show my transfer jobs",
            "Show my pipelines",
            "What can you do?",
        ],
    )


def _followups_for_act(act: DialogueAct) -> list[str]:
    if act == "summarize_last":
        return ["What should I do next?", "Explain that more simply", "Give me a workspace briefing"]
    if act == "explain_simpler":
        return ["What should I do next?", "Summarize that", "Show my jobs"]
    if act == "next_action":
        return ["Give me a workspace briefing", "Show my pipelines", "Show my jobs"]
    return ["Give me a workspace briefing", "Show my jobs", "What can you do?"]


def _clip(text: str, n: int) -> str:
    text = (text or "").strip()
    if len(text) <= n:
        return text
    return text[: n - 1] + "…"
