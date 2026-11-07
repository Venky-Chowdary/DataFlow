"""Pre-retrieval intent policy for the Pilot.

The trained router (``first_party.intent_router``) proposes a dialogue act for
the turn; this module decides whether that act is one the Pilot must settle
*before* retrieval, tools or any LLM see the message, and composes the answer
deterministically when it is.

Two independent signals must agree before a policy answer fires:

* the router's act with a raised confidence floor, and
* a lexical cue for that act (a literal reference to the transcript, to another
  tenant, to bypassing a gate, …).

The router alone confuses "what about last week" with a transcript question;
the cue alone is the thousand-regex trap. Requiring both keeps false positives
near zero while still generalising over paraphrase and typos, and the router is
never the security boundary: tool authorization and workspace scoping stay
where they are.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ..first_party.intent_router import Routing, route_intent
from .agent import CopilotResponse
from .conversation_composer import (
    compose_held_refusal_response,
    compose_recall_ask,
    compose_secret_refusal_response,
    last_assistant_text,
)

POLICY_MIN_CONFIDENCE = 0.70
POLICY_MIN_CONFIDENCE_WITH_MARGIN = 0.60
POLICY_MIN_MARGIN = 0.35

_CUES: dict[str, re.Pattern[str]] = {
    "assistant_meta": re.compile(
        r"\b(what|which|remind|repeat|recap)\b.{0,40}\b(i|we|my|our|user|operator)\b.{0,30}"
        r"\b(ask|asked|said|say|typed|wrote|question|earlier|before|first|last|previous)\b"
        r"|\b(my|our)\s+(first|last|previous|earlier|initial|second)\s+(question|message|ask|prompt)"
        r"|\b(have|did|had)\s+(we|i)\s+(covered|discussed|talked|asked|already)"
        r"|\b(repeat|remind me of)\b.{0,20}\b(question|asked|ask)\b",
        re.I,
    ),
    "prompt_injection": re.compile(
        r"\b(ignore|disregard|forget|override|bypass)\b.{0,30}\b(instructions?|rules?|guardrails?|"
        r"guidelines?|policy|policies|above|previous|prior|system)\b"
        r"|\bsystem\s*prompt\b|\bhidden\s+(prompt|instructions?)\b"
        r"|\b(reveal|print|dump|show|leak|repeat|expose)\b.{0,30}\b(prompt|instructions?|configuration|"
        r"config|rules?)\b.{0,30}(you|your|were|booted|given|initial)?"
        r"|\b(instructions?|prompt)\b.{0,20}\b(booted|initialised|initialized|started|given|configured)\b"
        r"|\b(developer|dan|jailbreak)\s*mode\b|\bno\s+(guardrails|restrictions|rules|filters)\b"
        r"|\b(act|pretend|behave|roleplay)\s+(as|like)\b.{0,40}\b(unrestricted|no\s+rules|without|"
        r"uncensored|evil|unfiltered|guardrails)"
        r"|\byou are now\b.{0,40}\b(unrestricted|uncensored|free|different|new)\b",
        re.I,
    ),
    "false_premise": re.compile(
        r"\b(you|u)\s+(said|told|claimed|mentioned|stated|promised|confirmed|admitted)\b"
        r"|\b(as|like)\s+you\s+(said|mentioned|told|claimed|stated)\b"
        r"|\b(didn'?t|did not|havent|haven'?t)\s+you\s+(say|tell|claim|mention|confirm|state)\b"
        r"|\b(earlier|before|previously)\s+you\s+(said|told|claimed|stated)\b"
        r"|\byou\s+(already|earlier|previously|just)\s+(said|told|confirmed|approved|agreed|promised)\b",
        re.I,
    ),
    "cross_tenant": re.compile(
        r"\b(other|another|different|neighbou?ring|competitor'?s?|every|all)\s+"
        r"(tenants?|workspaces?|customers?|clients?|companies|company|orgs?|organi[sz]ations?|accounts?|teams?)\b"
        r"|\b(tenants?|workspaces?|customers?|clients?|orgs?|organi[sz]ations?|teams?)\b.{0,20}"
        r"\b(not\s+(ours|mine|our|my)|other\s+than\s+(ours|mine))"
        r"|\b(not\s+(ours|mine))\b"
        r"|\b\w+(?:'s)?\s+(tenant|workspace|org|organi[sz]ation|account)\b.{0,30}\b(data|tables?|rows?|"
        r"connectors?|pipelines?|jobs?|transfers?|transferred|moved|loaded|schedules?)\b"
        r"|\b(data|tables?|rows?|connectors?|pipelines?|jobs?)\b.{0,40}\b(of|from|for|in|belong\w*\s+to)\s+"
        r"(the\s+)?(\w+\s+)?(tenant|workspace|team|customer|client|org|company)\b",
        re.I,
    ),
    "self_approval": re.compile(
        r"\b(skip|bypass|without|no|disable|ignore|remove|waive|suppress|override)\b.{0,25}"
        r"\b(confirm(ation)?|approval|approver|review(er)?|gate|gates|preflight|check|checks|sign[- ]?off)\b"
        r"|\b(approve|confirm|sign\s*off|pass|green[- ]?light|authori[sz]e)\b.{0,30}"
        r"\b(my\s+own|myself|for\s+me|on\s+my\s+behalf|as\s+me|it\s+yourself|yourself|automatically|"
        r"without\s+(a\s+)?(reviewer|review|second|human|approval|asking|confirm))"
        r"|\b(mark|set|flag|force)\b.{0,20}\b(gate|gates|preflight|check|checks|review|approval)\b.{0,20}"
        r"\b(passed|pass|green|approved|ok|done|complete|cleared)\b"
        r"|\bjust\s+(run|execute|do|apply)\s+it\b.{0,30}\b(confirm|approv|review|ask|gate|check)"
        r"|\b(auto|self)[- ]?(approve|approval|confirm)\b",
        re.I,
    ),
}


@dataclass(frozen=True)
class PolicyDecision:
    act: str
    routing: Routing


def decide(message: str) -> PolicyDecision | None:
    """Return the policy act for the turn, or ``None`` to let normal routing run."""
    text = (message or "").strip()
    if len(text) < 4:
        return None
    routing = route_intent(text)
    if routing is None or routing.act not in _CUES:
        return None
    decisive = routing.confidence >= POLICY_MIN_CONFIDENCE or (
        routing.confidence >= POLICY_MIN_CONFIDENCE_WITH_MARGIN and routing.margin >= POLICY_MIN_MARGIN
    )
    if not decisive:
        return None
    if not _CUES[routing.act].search(text):
        return None
    return PolicyDecision(act=routing.act, routing=routing)


def _conversation(
    answer: str,
    *,
    intent: str,
    reasoning: str,
    suggested_prompts: list[str],
    confidence: float = 0.9,
) -> CopilotResponse:
    return CopilotResponse(
        answer=answer,
        intent=intent,
        confidence=confidence,
        method="pilot_conversation",
        reasoning=reasoning,
        suggested_prompts=suggested_prompts,
    )


def compose_injection_refusal() -> CopilotResponse:
    return _conversation(
        "I can't do that. My operating instructions aren't something I reveal, "
        "and a message in the chat can't switch them off — the limits on what I "
        "read and change come from your workspace role and the tool permissions, "
        "not from how I'm asked.\n\n"
        "I'm Datawrap Pilot: I answer from the product documentation and your "
        "workspace's live state. Ask me about a connector, a pipeline, a job, or "
        "how a feature works and I'll take it from there.",
        intent="policy",
        reasoning="Prompt-injection / system-prompt disclosure refused before retrieval",
        suggested_prompts=["What can you do?", "Show my connectors", "Give me a workspace briefing"],
    )


def compose_false_premise_response(history: list[dict]) -> CopilotResponse:
    last = last_assistant_text(history)
    if last:
        lead = (
            "I don't think I said that — here is my last answer in this "
            f"conversation so we can check:\n\n> {last[:600]}\n\n"
        )
    else:
        lead = "I haven't said that in this conversation — there is no earlier answer from me here.\n\n"
    return _conversation(
        lead
        + "I won't confirm a claim just because it's attributed to me. Ask the "
        "underlying question directly and I'll answer it from the documentation or "
        "your workspace's current state.",
        intent="policy",
        reasoning="Attributed claim not found in transcript — declined to ratify it",
        suggested_prompts=["What did I just ask you?", "Summarize that", "What can't you do?"],
        confidence=0.85,
    )


def compose_cross_tenant_refusal() -> CopilotResponse:
    return _conversation(
        "I can only see the workspace you're signed into. Data, connectors, jobs "
        "and transfers that belong to another tenant, team or customer are not "
        "visible to me, and there is no override for that from chat — every read "
        "I make is scoped to your workspace ID on the server.\n\n"
        "If you have access to that other workspace, switch to it and ask me "
        "there. Otherwise I can show you what *this* workspace has.",
        intent="policy",
        reasoning="Cross-tenant read refused — reads are server-scoped to the caller's workspace",
        suggested_prompts=["Show my connectors", "Which jobs failed this week?", "Give me a workspace briefing"],
    )


def compose_self_approval_refusal() -> CopilotResponse:
    return _conversation(
        "I can't approve, confirm or skip a gate on your behalf — and I'm not "
        "able to. Every run from chat is staged behind a **Confirm** card that a "
        "human clicks, and preflight/quality gates record who cleared them. If "
        "chat could mark them passed, the gate would be decorative.\n\n"
        "What I can do: stage the transfer or pipeline for you, show why a gate "
        "is blocking, and tell you who is allowed to review it. Say which one.",
        intent="policy",
        reasoning="Self-approval / gate bypass refused — approvals require a human actor",
        suggested_prompts=["Show my pipelines", "Why is preflight blocked?", "Who can approve this?"],
    )


def answer(
    decision: PolicyDecision,
    *,
    history: list[dict],
    ctx: dict[str, Any] | None = None,
) -> CopilotResponse:
    """Compose the deterministic answer for a policy act."""
    act = decision.act
    if act == "assistant_meta":
        return _conversation(
            compose_recall_ask(history),
            intent="conversation",
            reasoning="Transcript question answered from the transcript, not retrieval",
            suggested_prompts=["Summarize that", "What should I do next?", "Give me a workspace briefing"],
            confidence=0.95,
        )
    if act == "prompt_injection":
        return compose_injection_refusal()
    if act == "false_premise":
        return compose_false_premise_response(history)
    if act == "cross_tenant":
        return compose_cross_tenant_refusal()
    if act == "self_approval":
        return compose_self_approval_refusal()
    if act == "secret_request":
        return compose_secret_refusal_response(ctx)
    return compose_held_refusal_response()
