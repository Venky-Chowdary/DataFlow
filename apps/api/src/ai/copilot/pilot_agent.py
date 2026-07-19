"""
DataTransfer.space — Data Pilot Agent

Anthropic/Cursor-style agent: full data context, tool use, natural conversation.
Answers any data question and performs work in the app when asked.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field

from services.value_serializer import json_default
from ..knowledge.copilot_knowledge import DATA_PILOT_PERSONA, SUGGESTED_PROMPTS
from .agent import CopilotResponse
from .context_builder import get_context_builder
from .data_analyst import get_data_analyst
from .tools import (
    TOOL_DEFINITIONS,
    ToolResult,
    format_tool_results_for_llm,
    get_pilot_tools,
    infer_tools_from_message,
)

logger = logging.getLogger(__name__)
# Per-provider hard cap. Client abort is 120s — keep total LLM attempts well under that
# so the local agent can always answer before the browser times out.
_LLM_TURN_TIMEOUT_S = 20
_LLM_TOTAL_BUDGET_S = 55
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pilot-llm")


class _UnavailableAnthropic:
    """Sentinel so a broken Anthropic provider key/config does not crash the agent."""

    name = "anthropic"

    def is_available(self) -> bool:
        return False

    def generate_agent(self, *args, **kwargs) -> dict:
        return {"success": False, "error": "Anthropic provider is unavailable"}

    def generate(self, *args, **kwargs):
        from ..llm.provider import LLMResponse

        return LLMResponse(content="", success=False, provider=self.name)


def _tools_used(turn: "PilotTurn") -> list[dict]:
    return [
        {"name": tr.name, "success": tr.success, "summary": _tool_summary(tr)}
        for tr in turn.tool_results
    ]


def _tool_summary(tr: ToolResult) -> str:
    if not tr.success:
        return tr.error or "failed"
    o = tr.output
    if tr.name == "list_datasets":
        return f"{o.get('count', 0)} datasets"
    if tr.name == "analyze_dataset":
        return o.get("dataset", "analyzed")
    if tr.name == "search_connectors":
        return f"{o.get('filtered', 0)} connectors"
    if tr.name == "search_knowledge":
        return f"{o.get('count', 0)} knowledge hits"
    if tr.name == "list_jobs":
        return f"{o.get('count', 0)} jobs"
    if tr.name == "list_schedules":
        return f"{o.get('count', 0)} schedules"
    if tr.name == "list_contracts":
        return f"{o.get('count', 0)} contracts"
    if tr.name == "navigate":
        return f"→ {o.get('screen')}"
    if tr.name == "run_schedule_now":
        return f"run {o.get('name') or o.get('schedule_id')}"
    if tr.name == "list_connector_objects":
        return f"{o.get('count', 0)} objects on {o.get('connector_name')}"
    if tr.name == "introspect_connector_schema":
        return f"{o.get('column_count', 0)} cols on {o.get('table')}"
    if tr.name == "diff_schemas":
        return f"severity={o.get('severity')}"
    if tr.name == "map_connector_schemas":
        return f"{o.get('mapping_count', 0)} mappings"
    return "ok"


def _score_response(resp: CopilotResponse | None) -> float:
    """Prefer grounded workspace answers over fluent ungrounded LLM prose."""
    if not isinstance(resp, CopilotResponse):
        return -1.0
    score = float(resp.confidence or 0)
    tools = resp.tools_used or []
    ok = sum(1 for t in tools if t.get("success"))
    fail = sum(1 for t in tools if not t.get("success"))
    method = (resp.method or "").lower()
    answer = resp.answer or ""

    # Grounded evidence is the primary signal
    score += ok * 0.55
    if ok > 0:
        score += 1.0
    else:
        # Cloud prose with zero workspace checks loses to local tool answers
        if any(m in method for m in ("anthropic", "openai", "ollama", "llm")):
            score -= 1.35
        score -= fail * 0.1

    if resp.pending_actions:
        score += 0.45
    if resp.suggested_actions:
        score += 0.12

    # Clarification only helps when we have nothing better — never beat grounded success
    if resp.needs_clarification:
        score += 0.25 if ok == 0 else 0.05

    if "error" in method or method == "greeting":
        score -= 1.0
    if len(answer.strip()) < 40:
        score -= 0.3
    if "Upload a file" in answer and ok == 0:
        score -= 0.7
    # Prefer plain-language clarification questions over wrong confident answers
    if fail > 0 and ok == 0 and ("Which " in answer or "which " in answer):
        score += 0.4
    return score


@dataclass
class PilotTurn:
    tool_results: list[ToolResult] = field(default_factory=list)
    actions: list[dict] = field(default_factory=list)
    pending_actions: list[dict] = field(default_factory=list)
    needs_clarification: str = ""


class DataPilotAgent:
    """
    Primary agent — like Claude with tools or a Cursor agent.
    1. Build full platform + data context
    2. Run tool loop (Anthropic tool_use or local inference)
    3. Compose natural-language answer grounded in real data
    """

    MAX_TOOL_ITERATIONS = 6

    def __init__(self):
        self.tools = get_pilot_tools()
        self.analyst = get_data_analyst()
        self.context_builder = get_context_builder()
        self._anthropic = None

    @property
    def anthropic(self):
        if self._anthropic is None:
            try:
                from ..llm.provider import DataTransferAnthropicProvider
                self._anthropic = DataTransferAnthropicProvider()
            except Exception:
                self._anthropic = _UnavailableAnthropic()
        return self._anthropic

    def chat(
        self,
        message: str,
        history: list[dict] | None = None,
        data_context: dict | None = None,
    ) -> CopilotResponse:
        message = message.strip()
        lower_msg = message.lower()
        if not message or lower_msg in {
            "hi",
            "hello",
            "hey",
            "help",
            "yo",
            "good morning",
            "good afternoon",
            "good evening",
        }:
            return CopilotResponse(
                answer=(
                    "I'm **Data Pilot** — your AI agent for all things data. "
                    "Ask me about any dataset, PII, transfer jobs, or say "
                    "\"move logistics data to MongoDB\" and I'll help you do it."
                ),
                intent="greeting",
                confidence=1.0,
                method="greeting",
                suggested_prompts=self._starter_prompts()[:4],
            )

        # Meta questions stay on the local agent — never RAG-dump ontology shards
        # and never race cloud LLMs for a "who are you" answer.
        from .tools import _is_meta_pilot_question
        if _is_meta_pilot_question(lower_msg):
            ctx = self.context_builder.build(data_context, message)
            return self._local_agent(message, history or [], ctx, data_context)

        ctx = self.context_builder.build(data_context, message)
        system = self._build_system_prompt(ctx)

        import time as _time
        from concurrent.futures import wait, FIRST_COMPLETED

        # Race local agent vs cloud LLMs. Prefer a finished LLM answer, but never
        # make the UI wait on a hung provider once the local agent is ready.
        local_fut = _executor.submit(
            self._local_agent, message, history or [], ctx, data_context
        )
        llm_futs: list = []

        openai_ready = False
        try:
            from ..llm.provider import DataTransferOpenAIProvider

            openai_ready = DataTransferOpenAIProvider().is_available()
        except Exception:
            openai_ready = False

        if self.anthropic.is_available():
            llm_futs.append(
                _executor.submit(
                    self._anthropic_agent_loop, message, history or [], system
                )
            )
        if openai_ready:
            llm_futs.append(
                _executor.submit(
                    self._openai_agent, message, history or [], system, data_context
                )
            )
        if self._ollama_available_quick():
            llm_futs.append(
                _executor.submit(
                    self._ollama_agent, message, history or [], system, data_context
                )
            )

        pending = {local_fut, *llm_futs}
        deadline = _time.monotonic() + _LLM_TOTAL_BUDGET_S
        local_result: CopilotResponse | None = None
        best_llm: CopilotResponse | None = None

        while pending and _time.monotonic() < deadline:
            timeout = max(0.1, min(1.0, deadline - _time.monotonic()))
            done, pending = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)
            for fut in done:
                try:
                    result = fut.result()
                except Exception as exc:
                    logger.warning("Data Pilot worker failed: %s", exc)
                    continue
                if fut is local_fut:
                    local_result = result
                    # Tiny grace so a nearly-finished LLM can still win; don't stall the UI.
                    deadline = min(deadline, _time.monotonic() + 0.4)
                    continue
                if isinstance(result, CopilotResponse):
                    if best_llm is None or _score_response(result) > _score_response(best_llm):
                        best_llm = result

        candidates = [c for c in (best_llm, local_result) if isinstance(c, CopilotResponse)]
        if candidates:
            return max(candidates, key=_score_response)
        try:
            return local_fut.result(timeout=5)
        except Exception as exc:
            logger.warning("Data Pilot local agent failed: %s", exc)
            return CopilotResponse(
                answer=(
                    "Data Pilot hit an internal error answering that. "
                    "Retry, or ask about a specific job_id / preflight run_id."
                ),
                intent="error",
                confidence=0.2,
                method="pilot_error",
                suggested_prompts=self._starter_prompts()[:3],
            )

    @staticmethod
    def _ollama_available_quick() -> bool:
        try:
            from ..llm.provider import DataTransferOllamaProvider

            return DataTransferOllamaProvider().is_available()
        except Exception:
            return False

    @staticmethod
    def _append_tool_actions(turn: PilotTurn, tr: ToolResult) -> None:
        if not tr.success or not isinstance(tr.output, dict):
            err = (tr.error or "").strip()
            if err and (
                err.startswith("Which ")
                or "did you mean" in err.lower()
                or tr.name in ("run_schedule_now", "get_schedule", "open_schedule")
            ):
                turn.needs_clarification = err
            return
        out = tr.output
        risk = out.get("risk") or "safe"

        if tr.name == "navigate":
            labels = {
                "transfer": "Transfer Studio",
                "jobs": "Jobs",
                "connectors": "Connectors",
                "dashboard": "Overview",
                "settings": "Settings",
                "schedules": "Pipelines",
                "contracts": "Contracts",
                "query": "Query",
                "mcp": "MCP",
                "docs": "Docs",
                "benchmarks": "Proofs",
                "pilot": "Data Pilot",
            }
            screen = out.get("screen")
            turn.actions.append({
                "type": "navigate",
                "screen": screen,
                "risk": "safe",
                "label": f"Open {labels.get(screen, screen)}",
            })
            return

        if tr.name in ("open_job", "open_schedule", "start_transfer_studio"):
            turn.actions.append({
                "type": "navigate",
                "screen": out.get("screen"),
                "job_id": out.get("job_id"),
                "schedule_id": out.get("schedule_id"),
                "risk": "safe",
                "label": out.get("label") or f"Open {out.get('screen')}",
            })
            return

        if tr.name == "remediate_validation":
            turn.pending_actions.append({
                "id": f"studio:{out.get('kind')}:{out.get('run_id') or ''}",
                "type": "studio",
                "kind": out.get("kind"),
                "label": out.get("label"),
                "run_id": out.get("run_id"),
                "risk": "mutate",
                "payload": {"kind": out.get("kind"), "run_id": out.get("run_id")},
            })
            # Ensure Transfer is ready; safe navigate can auto-apply.
            turn.actions.append({
                "type": "navigate",
                "screen": "transfer",
                "risk": "safe",
                "label": "Open Transfer Studio",
            })
            return

        if tr.name == "run_schedule_now":
            turn.pending_actions.append({
                "id": f"run_schedule:{out.get('schedule_id')}",
                "type": "run_schedule",
                "label": out.get("label") or "Run pipeline now",
                "risk": "mutate",
                "payload": {
                    "schedule_id": out.get("schedule_id"),
                    "name": out.get("name"),
                },
            })
            turn.actions.append({
                "type": "navigate",
                "screen": "schedules",
                "schedule_id": out.get("schedule_id"),
                "risk": "safe",
                "label": "Open Pipelines",
            })
            return

        if risk == "mutate" or out.get("requires_confirm"):
            turn.pending_actions.append({
                "id": f"{tr.name}:{out.get('kind') or out.get('schedule_id') or out.get('id') or len(turn.pending_actions)}",
                "type": out.get("action") or tr.name,
                "label": out.get("label") or "Confirm this change",
                "risk": "mutate",
                "payload": out,
            })

    def _anthropic_agent_loop(
        self,
        message: str,
        history: list[dict],
        system: str,
    ) -> CopilotResponse | None:
        messages: list[dict] = []
        for msg in history[-12:]:
            role = msg.get("role", "user")
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": msg.get("content", "")})
        messages.append({"role": "user", "content": message})

        turn = PilotTurn()
        intent = self._detect_intent(message)

        for _ in range(self.MAX_TOOL_ITERATIONS):
            response = self.anthropic.generate_agent(
                messages=messages,
                system=system,
                tools=TOOL_DEFINITIONS,
                max_tokens=4096,
            )
            if not response.get("success"):
                break

            tool_calls = response.get("tool_calls") or []
            if not tool_calls:
                text = response.get("content", "").strip()
                if text:
                    return CopilotResponse(
                        answer=text,
                        intent=intent,
                        confidence=0.94,
                        method="anthropic_agent",
                        reasoning=f"Agent loop, {len(turn.tool_results)} tool calls",
                        suggested_actions=turn.actions,
                        pending_actions=turn.pending_actions,
                        needs_clarification=turn.needs_clarification,
                        suggested_prompts=self._follow_ups(message, turn),
                        data_insight=self._data_insight_from_turn(turn),
                        tools_used=_tools_used(turn),
                    )
                break

            # Append assistant tool_use message
            assistant_content = []
            if response.get("content"):
                assistant_content.append({"type": "text", "text": response["content"]})
            tool_results_content = []
            for tc in tool_calls:
                assistant_content.append({
                    "type": "tool_use",
                    "id": tc["id"],
                    "name": tc["name"],
                    "input": tc["input"],
                })
                tr = self.tools.execute(tc["name"], tc.get("input") or {})
                turn.tool_results.append(tr)
                self._append_tool_actions(turn, tr)
                tool_results_content.append({
                    "type": "tool_result",
                    "tool_use_id": tc["id"],
                    "content": json.dumps(tr.output if tr.success else {"error": tr.error}, default=json_default),
                })

            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results_content})

        return None

    def _openai_agent(
        self,
        message: str,
        history: list[dict],
        system: str,
        data_context: dict | None,
    ) -> CopilotResponse | None:
        from ..llm.provider import DataTransferOpenAIProvider
        openai = DataTransferOpenAIProvider()
        if not openai.is_available():
            return None

        planned = infer_tools_from_message(message)
        turn = PilotTurn()
        for name, args in planned:
            tr = self.tools.execute(name, args)
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)

        tool_context = format_tool_results_for_llm(turn.tool_results)
        history_text = "\n".join(
            f"{m.get('role', 'user').capitalize()}: {m.get('content', '')}"
            for m in history[-8:]
        )
        prompt = f"""{system}

Tool results for this turn:
{tool_context or 'No tools invoked.'}

History:
{history_text or 'None'}

User: {message}

Respond as Data Pilot in natural language. Ground your answer in tool results and context."""

        response = openai.generate(prompt, system=DATA_PILOT_PERSONA, max_tokens=2048)
        if not response.success or not response.content.strip():
            return None
        return CopilotResponse(
            answer=response.content.strip(),
            intent=self._detect_intent(message),
            confidence=0.9,
            method="openai_agent",
            suggested_actions=turn.actions,
            pending_actions=turn.pending_actions,
            needs_clarification=turn.needs_clarification,
            suggested_prompts=self._follow_ups(message, turn),
            data_insight=self._data_insight_from_turn(turn),
            tools_used=_tools_used(turn),
        )

    def _ollama_agent(
        self,
        message: str,
        history: list[dict],
        system: str,
        data_context: dict | None,
    ) -> CopilotResponse | None:
        from ..llm.provider import DataTransferOllamaProvider
        ollama = DataTransferOllamaProvider()
        if not ollama.is_available():
            return None

        planned = infer_tools_from_message(message)
        turn = PilotTurn()
        for name, args in planned:
            tr = self.tools.execute(name, args)
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)

        tool_context = format_tool_results_for_llm(turn.tool_results)
        history_text = "\n".join(
            f"{m.get('role', 'user').capitalize()}: {m.get('content', '')}"
            for m in history[-6:]
        )
        prompt = f"""{system}

Tool results:
{tool_context or 'None'}

History:
{history_text or 'None'}

User: {message}

Respond as Data Pilot — grounded in tool results."""

        response = ollama.generate(prompt, system=DATA_PILOT_PERSONA, max_tokens=2048)
        if not response.success or not response.content.strip():
            return None
        return CopilotResponse(
            answer=response.content.strip(),
            intent=self._detect_intent(message),
            confidence=0.85,
            method="ollama_agent",
            suggested_actions=turn.actions,
            pending_actions=turn.pending_actions,
            needs_clarification=turn.needs_clarification,
            suggested_prompts=self._follow_ups(message, turn),
            data_insight=self._data_insight_from_turn(turn),
            tools_used=_tools_used(turn),
        )

    def _local_agent(
        self,
        message: str,
        history: list[dict],
        ctx: dict,
        data_context: dict | None,
    ) -> CopilotResponse:
        intent = self._detect_intent(message)
        turn = PilotTurn()

        # Always try relevant tools locally
        for name, args in infer_tools_from_message(message):
            tr = self.tools.execute(name, args)
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)

        # Ground answers in active session IDs when the user asks about failure/status
        # without pasting an ID (Jobs / Validate feed these into data_context).
        lower = message.lower()
        wants_triage = any(
            w in lower
            for w in ("fail", "error", "blocked", "why", "status", "fix", "quarantine", "integrity")
        )
        if data_context and wants_triage:
            have_job = any(tr.name == "get_job" for tr in turn.tool_results)
            have_pf = any(tr.name == "get_preflight_run" for tr in turn.tool_results)
            if not have_job and data_context.get("job_id"):
                tr = self.tools.execute("get_job", {"job_id": str(data_context["job_id"])})
                turn.tool_results.append(tr)
                self._append_tool_actions(turn, tr)
            if not have_pf and data_context.get("preflight_run_id"):
                tr = self.tools.execute(
                    "get_preflight_run",
                    {"run_id": str(data_context["preflight_run_id"])},
                )
                turn.tool_results.append(tr)
                self._append_tool_actions(turn, tr)

        # Data analysis from session or dataset hint (skip if listing all data)
        list_only = any(tr.name == "list_datasets" for tr in turn.tool_results)
        navigated = any(tr.name == "navigate" for tr in turn.tool_results)
        has_knowledge = any(tr.name == "search_knowledge" for tr in turn.tool_results)
        has_connector = any(tr.name == "search_connectors" for tr in turn.tool_results)
        described = any(tr.name == "describe_pilot" for tr in turn.tool_results)
        live_schema = any(
            tr.name in (
                "list_connector_objects",
                "introspect_connector_schema",
                "diff_schemas",
                "map_connector_schemas",
                "list_connectors",
            )
            for tr in turn.tool_results
        )
        insight = None
        if (
            not list_only
            and not has_knowledge
            and not has_connector
            and not described
            and not live_schema
            and not (navigated and not self.analyst.wants_data_analysis(message, intent))
        ):
            insight = self.analyst.analyze_context(data_context, self.analyst.extract_dataset_hint(message))
            if not insight and self.analyst.wants_data_analysis(message, intent):
                hint = self.analyst.extract_dataset_hint(message)
                if hint:
                    analyze_tr = self.tools.execute("analyze_dataset", {"dataset_name": hint})
                    if analyze_tr.success:
                        turn.tool_results.append(analyze_tr)

        # Compose from tool results + analyst
        answer = self._compose_local_answer(message, intent, turn, insight, ctx)
        if turn.needs_clarification and turn.needs_clarification not in answer:
            answer = f"{turn.needs_clarification}\n\n{answer}".strip()
        if turn.pending_actions:
            labels = ", ".join(f"**{a.get('label')}**" for a in turn.pending_actions if a.get("label"))
            if labels and "Confirm" not in answer:
                answer = f"{answer}\n\nConfirm to proceed: {labels}.".strip()
        ok_tools = sum(1 for tr in turn.tool_results if tr.success)
        if ok_tools:
            confidence = 0.96
        elif turn.needs_clarification:
            confidence = 0.78
        else:
            confidence = 0.84
        return CopilotResponse(
            answer=answer,
            intent=intent,
            confidence=confidence,
            method="pilot_local_agent",
            reasoning=f"Local agent with {len(turn.tool_results)} tools",
            suggested_actions=turn.actions,
            pending_actions=turn.pending_actions,
            needs_clarification=turn.needs_clarification,
            suggested_prompts=self._follow_ups(message, turn),
            data_insight=self._data_insight_from_turn(turn) or (
                {
                    "dataset": insight.dataset_name,
                    "columns": len(insight.columns),
                    "rows": insight.row_count,
                    "pii_count": len(insight.pii_columns),
                    "quality_score": insight.quality_score,
                } if insight else None
            ),
            tools_used=_tools_used(turn),
        )

    def _compose_local_answer(self, message, intent, turn, insight, ctx) -> str:
        parts: list[str] = []

        for tr in turn.tool_results:
            if tr.name == "navigate" and tr.success:
                screen = tr.output.get("screen", "")
                labels = {
                    "transfer": "Transfer Studio",
                    "jobs": "Jobs",
                    "connectors": "Connectors",
                    "dashboard": "Overview",
                    "settings": "Settings",
                    "schedules": "Pipelines",
                    "contracts": "Contracts",
                    "query": "Query",
                    "mcp": "MCP",
                    "docs": "Docs",
                    "benchmarks": "Proofs",
                    "pilot": "Data Pilot",
                }
                parts.append(f"Opening **{labels.get(screen, screen)}** for you.")
            elif tr.name in ("open_job", "open_schedule", "start_transfer_studio") and tr.success:
                parts.append(f"{tr.output.get('label') or 'Opening that screen'} for you.")
            elif tr.name == "list_schedules" and tr.success:
                rows = tr.output.get("schedules", [])
                if rows:
                    lines = [f"You have **{len(rows)} pipeline schedule(s)**:"]
                    for s in rows[:8]:
                        lines.append(
                            f"• **{s.get('name')}** · {s.get('interval')}"
                            f"{' · cron ' + s['cron'] if s.get('cron') else ''}"
                            f" · next `{s.get('next_run_at') or '—'}`"
                            f" · last **{s.get('last_status') or 'never'}** ({s.get('run_count', 0)} runs)"
                        )
                    parts.append("\n".join(lines))
                else:
                    parts.append("No pipeline schedules yet. Create one from **Pipelines** or after a transfer.")
            elif tr.name == "get_schedule" and tr.success:
                s = tr.output or {}
                parts.append(
                    f"Pipeline **{s.get('name')}** (`{s.get('id')}`) · {s.get('interval')} · "
                    f"enabled={s.get('enabled')} · next `{s.get('next_run_at') or '—'}` · "
                    f"last **{s.get('last_status') or 'never'}**."
                )
            elif tr.name == "run_schedule_now" and tr.success:
                parts.append(
                    f"Ready to run pipeline **{tr.output.get('name')}**. "
                    "Confirm below to start an immediate run (does not change the regular cadence)."
                )
            elif tr.name == "list_contracts" and tr.success:
                rows = tr.output.get("contracts", [])
                if rows:
                    lines = [f"**{len(rows)} data contract(s):**"]
                    for c in rows[:8]:
                        lines.append(f"• **{c.get('name') or c.get('id')}** ({c.get('status') or '—'})")
                    parts.append("\n".join(lines))
                else:
                    parts.append("No data contracts yet. Open **Contracts** to define one.")
            elif tr.name == "list_datasets" and tr.success:
                datasets = tr.output.get("datasets", [])
                if datasets:
                    lines = [f"I have **{len(datasets)} datasets** indexed:"]
                    for ds in datasets[:6]:
                        lines.append(
                            f"• **{ds['name']}** — {ds['column_count']} columns"
                            + (f", {ds['row_count']:,} rows" if ds.get("row_count") else "")
                            + f" ({ds['source']})"
                        )
                    parts.append("\n".join(lines))
            elif tr.name == "list_jobs" and tr.success:
                jobs = tr.output.get("jobs", [])
                if jobs:
                    lines = ["Here are your **recent transfer jobs**:"]
                    for j in jobs[:5]:
                        lines.append(
                            f"• `{j.get('id', '?')}` · {j.get('source', '?')} → {j.get('destination', '?')}: "
                            f"**{j.get('status')}** ({j.get('records', 0):,} records)"
                        )
                    parts.append("\n".join(lines))
                else:
                    parts.append("No transfer jobs yet. Start one from **New Transfer**.")
            elif tr.name == "get_job" and tr.success:
                job = tr.output or {}
                lines = [
                    f"Job **`{job.get('id')}`** — **{job.get('status', '?').upper()}** · "
                    f"{job.get('source', '?')} → {job.get('destination', '?')}.",
                    f"Rows processed: {(job.get('records_processed') or 0):,} · "
                    f"rejected: {job.get('rejected_rows') or 0} · "
                    f"coerced NULL: {job.get('coerced_null_rows') or 0}.",
                ]
                route = job.get("route") or {}
                if route.get("source_table") or route.get("dest_table"):
                    lines.append(
                        f"Route: `{route.get('source_table') or '?'}` "
                        f"({route.get('source_type') or '?'}) → "
                        f"`{route.get('dest_table') or '?'}` "
                        f"({route.get('dest_type') or '?'})"
                        + (f" · {route.get('mappings_count', 0)} mappings" if route.get("mappings_count") is not None else "")
                        + (f" · sync `{route.get('sync_mode')}`" if route.get("sync_mode") else "")
                    )
                live = job.get("live_source_schema") or {}
                if live.get("columns"):
                    preview = ", ".join(
                        f"`{c.get('name')}`:{c.get('inferred_type')}"
                        for c in live["columns"][:8]
                    )
                    lines.append(
                        f"Live source schema **{live.get('connector_name')}**.`{live.get('table')}` "
                        f"({live.get('column_count')} cols): {preview}"
                    )
                if job.get("error"):
                    lines.append(f"Error: {job['error']}")
                for rem in (job.get("suggested_remediations") or [])[:4]:
                    lines.append(f"• Suggested: **{rem.get('label')}** (`{rem.get('kind')}`)")
                parts.append("\n".join(lines))
            elif tr.name == "get_preflight_run" and tr.success:
                run = tr.output or {}
                lines = [
                    f"Validation run **`{run.get('run_id')}`** — "
                    f"{'PASSED' if run.get('passed') else 'BLOCKED'} "
                    f"({run.get('passed_count', '?')}/{run.get('total_gates', '?')} gates, "
                    f"{run.get('readiness_score', '?')}% ready).",
                ]
                route = run.get("route") or {}
                if run.get("source_label") or run.get("dest_label"):
                    lines.append(
                        f"Route: {run.get('source_label', '?')} → {run.get('dest_label', '?')}"
                        + (f" · {route.get('row_count'):,} rows" if route.get("row_count") else "")
                    )
                for b in (run.get("blockers") or [])[:4]:
                    lines.append(f"• Blocker `{b.get('id')}`: {b.get('message')}")
                    if b.get("fix"):
                        lines.append(f"  Fix: {b['fix']}")
                for rem in (run.get("suggested_remediations") or [])[:3]:
                    lines.append(f"• Suggested: **{rem.get('label')}** (`{rem.get('kind')}`)")
                parts.append("\n".join(lines))
            elif tr.name == "remediate_validation" and tr.success:
                parts.append(
                    f"Proposed Studio remediation: **{tr.output.get('label')}**. "
                    "Confirm to apply it in Transfer Studio (Validate step)."
                )
            elif tr.name == "plan_transfer_route" and tr.success:
                o = tr.output or {}
                parts.append(
                    f"**Route plan** ({o.get('route_type')}): {o.get('source')} → {o.get('destination')}\n"
                    f"• Sync: **{o.get('recommended_sync')}**\n"
                    f"• Schema policy: {o.get('schema_policy')}\n"
                    f"• Gates: {', '.join(o.get('required_gates') or [])}"
                )
            elif tr.name == "explain_mapping_assurance" and tr.success:
                o = tr.output or {}
                parts.append(
                    "**Mapping assurance**\n"
                    f"• Assignment: `{o.get('assignment')}`\n"
                    f"• Scoring layers: {', '.join(o.get('scoring_layers') or [])}\n"
                    f"• Guarantees: {'; '.join(o.get('guarantees') or [])}\n"
                    f"• Honesty: {o.get('not_claimed')}"
                )
            elif tr.name == "recommend_sync_mode" and tr.success:
                o = tr.output or {}
                parts.append(
                    f"Recommended sync mode: **{o.get('recommended_mode')}** — {o.get('reason')}"
                )
            elif tr.name == "inspect_schema_policy" and tr.success:
                o = tr.output or {}
                parts.append(
                    f"Schema change `{o.get('change_type')}` → severity **{o.get('severity')}**: {o.get('action')} "
                    f"(operator review: {o.get('operator_review')})."
                )
            elif tr.name == "profile_quality_rules" and tr.success:
                o = tr.output or {}
                rules = o.get("rules") or []
                parts.append(
                    f"Quality rules for **{o.get('dataset')}** ({o.get('column_count', 0)} columns):\n"
                    + "\n".join(f"• {r}" for r in rules)
                )
            elif tr.name == "list_connector_objects" and tr.success:
                o = tr.output or {}
                objs = o.get("objects") or []
                lines = [
                    f"**{o.get('connector_name')}** ({o.get('type')}) — "
                    f"{'connected' if o.get('connected') else 'probe returned'} · "
                    f"**{o.get('count', len(objs))}** tables/collections:"
                ]
                for name in objs[:20]:
                    lines.append(f"• `{name}`")
                if len(objs) > 20:
                    lines.append(f"• …and {len(objs) - 20} more")
                if o.get("message"):
                    lines.append(f"_{o['message']}_")
                parts.append("\n".join(lines))
            elif tr.name == "introspect_connector_schema" and tr.success:
                o = tr.output or {}
                cols = o.get("columns") or []
                lines = [
                    f"Live schema **{o.get('connector_name')}**.`{o.get('table')}` "
                    f"({o.get('type')}) — **{o.get('column_count', len(cols))} columns**:"
                ]
                for c in cols[:40]:
                    null = "NULL" if c.get("nullable", True) else "NOT NULL"
                    lines.append(
                        f"• `{c.get('name')}` → **{c.get('inferred_type')}**"
                        + (f" ({c.get('data_type')})" if c.get("data_type") else "")
                        + f" · {null}"
                    )
                if len(cols) > 40:
                    lines.append(f"• …and {len(cols) - 40} more columns")
                for w in (o.get("warnings") or [])[:3]:
                    lines.append(f"⚠ {w}")
                parts.append("\n".join(lines))
            elif tr.name == "diff_schemas" and tr.success:
                o = tr.output or {}
                src = o.get("source") or {}
                dst = o.get("destination") or {}
                lines = [
                    f"Schema diff **{src.get('connector')}.{src.get('table')}** "
                    f"→ **{dst.get('connector')}.{dst.get('table')}** · "
                    f"severity **{o.get('severity')}**",
                    f"• Shared: {len(o.get('shared_columns') or [])} columns",
                    f"• Only in source: {', '.join(f'`{c}`' for c in (o.get('only_in_source') or [])[:12]) or 'none'}",
                    f"• Only in dest: {', '.join(f'`{c}`' for c in (o.get('only_in_destination') or [])[:12]) or 'none'}",
                ]
                for m in (o.get("type_mismatches") or [])[:8]:
                    lines.append(
                        f"• Type mismatch `{m.get('column')}`: "
                        f"{m.get('source_type')} → {m.get('dest_type')}"
                    )
                for b in (o.get("breaking") or [])[:6]:
                    lines.append(f"• Breaking: `{b.get('kind')}` on `{b.get('column') or b}`")
                for a in (o.get("additive") or [])[:6]:
                    lines.append(f"• Additive: `{a.get('kind')}` on `{a.get('column')}`")
                parts.append("\n".join(lines))
            elif tr.name == "map_connector_schemas" and tr.success:
                o = tr.output or {}
                src = o.get("source") or {}
                dst = o.get("destination") or {}
                lines = [
                    f"**Semantic mapping** {src.get('connector')}.`{src.get('table')}` → "
                    f"{dst.get('connector') or 'passthrough'}"
                    + (f".`{dst.get('table')}`" if dst.get("table") else "")
                    + f" — **{o.get('mapping_count', 0)} mappings**"
                    + (" (identity passthrough)" if dst.get("passthrough") else "")
                    + ":",
                ]
                for m in (o.get("mappings") or [])[:20]:
                    conf = m.get("confidence")
                    conf_s = f"{float(conf):.0%}" if conf is not None else "?"
                    lines.append(
                        f"• `{m.get('source')}` → `{m.get('target')}` ({conf_s})"
                        + (
                            f" · {m.get('source_type')}→{m.get('target_type')}"
                            if m.get("source_type") and m.get("target_type")
                            else ""
                        )
                    )
                if o.get("unmapped_source"):
                    lines.append(
                        "• Unmapped source: "
                        + ", ".join(f"`{c}`" for c in o["unmapped_source"][:12])
                    )
                if o.get("low_confidence"):
                    lines.append(
                        f"• Low-confidence pairs needing review: {len(o['low_confidence'])}"
                    )
                if o.get("type_risks"):
                    lines.append(f"• Type risks: {len(o['type_risks'])}")
                parts.append("\n".join(lines))
            elif tr.name == "list_connectors" and tr.success:
                conns = tr.output.get("connectors", [])
                if conns:
                    lines = [f"You have **{len(conns)} saved connector(s)**:"]
                    for c in conns:
                        lines.append(f"• **{c.get('name')}** ({c.get('type')}) → {c.get('database', c.get('host', ''))}")
                    parts.append("\n".join(lines))
                else:
                    parts.append("No connectors saved yet. Go to **Connectors** to add MongoDB, PostgreSQL, or Snowflake.")
            elif tr.name == "analyze_dataset" and tr.success:
                parts.append(self._format_analysis(tr.output))
            elif tr.name == "search_data" and tr.success:
                hits = tr.output.get("hits", [])
                if hits:
                    lines = [f"Found **{len(hits)} match(es)** for `{tr.output.get('query')}`:"]
                    for h in hits[:8]:
                        if h.get("match") == "column":
                            lines.append(f"• Column `{h['column']}` in **{h['dataset']}**")
                        elif h.get("match") == "value":
                            lines.append(f"• Value `{h.get('sample')}` in `{h['column']}` (**{h['dataset']}**)")
                        else:
                            lines.append(f"• Dataset **{h['dataset']}**")
                    parts.append("\n".join(lines))
                else:
                    parts.append(f"No matches for `{tr.output.get('query')}` across your datasets.")
            elif tr.name == "compare_datasets" and tr.success:
                o = tr.output
                parts.append(
                    f"Comparing **{o['dataset_a']}** ({o['column_count_a']} cols) vs "
                    f"**{o['dataset_b']}** ({o['column_count_b']} cols):\n"
                    f"• Shared: {', '.join(o['shared_columns'][:8]) or 'none'}\n"
                    f"• Only in A: {', '.join(o['only_in_a'][:6]) or 'none'}\n"
                    f"• Only in B: {', '.join(o['only_in_b'][:6]) or 'none'}"
                )
            elif tr.name == "get_transfer_capabilities" and tr.success:
                combos = tr.output.get("live_combinations", [])
                parts.append(
                    f"Universal transfer supports **{len(combos)} live routes** — "
                    "any file (CSV/JSON/JSONL/TSV) to MongoDB, PostgreSQL, Snowflake; "
                    "DB-to-DB migrations; and file exports. Tables and collections are auto-created."
                )
            elif tr.name == "search_connectors" and tr.success:
                conns = tr.output.get("connectors", [])[:8]
                lines = [f"Found **{tr.output.get('filtered', len(conns))}** matching connector(s):"]
                for c in conns:
                    status = c.get("status", "planned")
                    badge = "live" if status == "live" else status
                    lines.append(f"• **{c['name']}** ({badge}) — {c.get('description', '')[:60]}")
                parts.append("\n".join(lines))
            elif tr.name == "describe_pilot" and tr.success:
                o = tr.output or {}
                lines = [
                    "I'm **Data Pilot** — I help with routes, schema risk, "
                    "mappings, jobs, and fixes inside DataFlow. I answer from your workspace first.",
                    "**I can:**",
                ]
                for item in (o.get("can") or [])[:6]:
                    lines.append(f"• {item}")
                ds = o.get("datasets") or []
                if ds:
                    lines.append(
                        "**Indexed datasets:** "
                        + ", ".join(f"**{d.get('name')}**" for d in ds[:6] if d.get("name"))
                    )
                else:
                    lines.append(
                        "**Indexed datasets:** none yet — upload in **New Transfer** and I can profile them."
                    )
                conns = o.get("connectors") or []
                if conns:
                    lines.append(
                        "**Saved connectors:** "
                        + ", ".join(
                            f"{c.get('name')} ({c.get('type')})" for c in conns[:6] if c.get("name")
                        )
                    )
                examples = o.get("ask_examples") or []
                if examples:
                    lines.append("Try: " + " · ".join(f'"{e}"' for e in examples[:4]))
                parts.append("\n".join(lines))
            elif tr.name == "search_knowledge" and tr.success:
                hits = tr.output.get("hits", [])
                if hits:
                    lines = ["Here's what matches your question:"]
                    for h in hits[:3]:
                        summary = (h.get("summary") or h.get("text") or "").strip()
                        if summary:
                            lines.append(f"• {summary[:400]}")
                    parts.append("\n".join(lines))
                else:
                    parts.append(
                        "No solid knowledge match for that. Ask about a dataset, a job ID, "
                        "or say **what can you do** for my capabilities."
                    )

        if insight and not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            if not any(
                tr.name in (
                    "navigate",
                    "introspect_connector_schema",
                    "list_connector_objects",
                    "diff_schemas",
                    "map_connector_schemas",
                )
                for tr in turn.tool_results
            ):
                parts.append(self.analyst.compose_response(insight, message, intent))

        # Surface failures in plain language — never name internal tools.
        failed = [tr for tr in turn.tool_results if not tr.success and tr.error]
        if failed and not parts:
            lines = ["I couldn't complete that lookup:"]
            for tr in failed[:4]:
                lines.append(f"• {tr.error}")
            parts.append("\n".join(lines))
        elif failed and parts:
            # Mixed success+failure: append the failure so users see what was wrong
            clarify = [tr.error for tr in failed if tr.error and ("Which " in tr.error or "did you mean" in tr.error.lower())]
            if clarify:
                parts.append(clarify[0])

        if not parts:
            datasets = ctx.get("datasets", [])
            connectors = ctx.get("connectors") or ctx.get("saved_connectors") or []
            if datasets:
                names = ", ".join(d["name"] for d in datasets[:4])
                parts.append(
                    f"I can help with any question about your data. Available datasets: **{names}**.\n\n"
                    "Try: \"Analyze logistics data\", \"Show my jobs\", \"What PII is in HR?\", "
                    "or \"Take me to transfer\"."
                )
            elif connectors:
                names = ", ".join(
                    str(c.get("name") or c) for c in connectors[:4] if c
                )
                parts.append(
                    "I can look up live schemas and jobs on your saved connectors"
                    + (f" ({names})" if names else "")
                    + '. Try: "schema of airports on Local Postgres" or "show my pipelines".'
                )
            else:
                parts.append(
                    "I can help with connectors, jobs, pipelines, and live schemas. "
                    "Save a connector in **Connectors**, or upload a file in **New Transfer** "
                    "and I'll profile columns, PII, and quality."
                )

        return "\n\n".join(parts)

    def _format_analysis(self, output: dict) -> str:
        name = output.get("dataset", "dataset").replace("sample_", "").replace("_", " ")
        lines = [
            f"**{name}** analysis:",
            f"• {len(output.get('columns', []))} columns, {output.get('row_count', 0):,} rows",
            f"• Quality score: **{output.get('quality_score', 0):.0f}%**",
        ]
        if output.get("pii_columns"):
            lines.append(f"• PII detected: {', '.join(f'`{c}`' for c in output['pii_columns'])}")
        details = output.get("column_details", [])[:6]
        if details:
            lines.append("• Key columns:")
            for c in details:
                pii = " · PII" if c.get("is_pii") else ""
                lines.append(f"  - `{c['name']}` → {c.get('semantic_type', '?')}{pii}")
        preview = output.get("sample_preview", [])
        if preview:
            lines.append("• Sample row: " + ", ".join(f"{k}={v}" for k, v in list(preview[0].items())[:4]))
        return "\n".join(lines)

    def _build_system_prompt(self, ctx: dict) -> str:
        tool_names = ", ".join(t["name"] for t in TOOL_DEFINITIONS)
        return f"""{DATA_PILOT_PERSONA}

{self.context_builder.to_system_context(ctx)}

You are Data Pilot for DataFlow only — data knowledge, product capabilities, and in-app actions.
Available tools (internal — never name these in user-facing answers): {tool_names}.
Use tools for any factual claim about jobs, connectors, datasets, schedules, or capabilities.
Never invent IDs or warehouse state. Never mention tool names, APIs, or internal method labels in replies — write in plain product language.
For mutating actions (remediate, run schedule), propose and wait for UI confirm — do not claim they already ran.
Navigate to any screen when asked (including schedules/pipelines, contracts, query, docs, proofs)."""

    def _detect_intent(self, message: str) -> str:
        from ..knowledge.copilot_knowledge import INTENT_PATTERNS
        lower = message.lower()
        scores: dict[str, int] = {}
        for intent, keywords in INTENT_PATTERNS.items():
            score = sum(1 for kw in keywords if kw in lower)
            if score:
                scores[intent] = score
        if any(w in lower for w in ("navigate", "go to", "open", "take me")):
            scores["transfer_help"] = scores.get("transfer_help", 0) + 2
        return max(scores, key=scores.get) if scores else "product_help"

    def _data_insight_from_turn(self, turn: PilotTurn) -> dict | None:
        for tr in turn.tool_results:
            if tr.name == "analyze_dataset" and tr.success:
                o = tr.output
                return {
                    "dataset": o.get("dataset"),
                    "columns": len(o.get("columns", [])),
                    "rows": o.get("row_count", 0),
                    "pii_count": len(o.get("pii_columns", [])),
                    "quality_score": o.get("quality_score", 0),
                }
        return None

    def _follow_ups(self, message: str, turn: PilotTurn) -> list[str]:
        prompts = []
        if turn.pending_actions:
            prompts.append("What happens if I confirm?")
        if not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            prompts.append("Analyze my logistics data")
        prompts.extend([
            "Show my pipelines",
            "Show my transfer jobs",
            "Take me to contracts",
            "How does mapping assurance work?",
        ])
        return prompts[:4]

    def get_suggested_prompts(self) -> list[str]:
        return self._starter_prompts()

    def _starter_prompts(self) -> list[str]:
        datasets = self.analyst.list_datasets()
        prompts = []
        for d in datasets[:2]:
            label = d["name"].replace("sample_", "").replace("_", " ")
            prompts.append(f"Tell me everything about {label}")
        prompts.extend(SUGGESTED_PROMPTS[:4] if SUGGESTED_PROMPTS else [
            "What data do I have?",
            "Show my recent jobs",
            "Move logistics CSV to MongoDB",
            "Take me to connectors",
        ])
        return prompts


_pilot: DataPilotAgent | None = None


def get_pilot_agent() -> DataPilotAgent:
    global _pilot
    if _pilot is None:
        _pilot = DataPilotAgent()
    return _pilot
