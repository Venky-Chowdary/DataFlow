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
    if tr.name == "navigate":
        return f"→ {o.get('screen')}"
    return "ok"


@dataclass
class PilotTurn:
    tool_results: list[ToolResult] = field(default_factory=list)
    actions: list[dict] = field(default_factory=list)


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
        if not message or message.lower() in {
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
                    return result

        if local_result is not None:
            return local_result
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
        if not tr.success:
            return
        if tr.name == "navigate" and isinstance(tr.output, dict):
            turn.actions.append({"type": "navigate", "screen": tr.output.get("screen")})
        if tr.name == "remediate_validation" and isinstance(tr.output, dict):
            turn.actions.append({
                "type": "studio",
                "kind": tr.output.get("kind"),
                "label": tr.output.get("label"),
                "run_id": tr.output.get("run_id"),
            })
            turn.actions.append({"type": "navigate", "screen": "transfer"})

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
        insight = None
        if not list_only and not has_knowledge and not has_connector and not (navigated and not self.analyst.wants_data_analysis(message, intent)):
            insight = self.analyst.analyze_context(data_context, self.analyst.extract_dataset_hint(message))
            if not insight and self.analyst.wants_data_analysis(message, intent):
                hint = self.analyst.extract_dataset_hint(message)
                if hint:
                    analyze_tr = self.tools.execute("analyze_dataset", {"dataset_name": hint})
                    if analyze_tr.success:
                        turn.tool_results.append(analyze_tr)

        # Compose from tool results + analyst
        answer = self._compose_local_answer(message, intent, turn, insight, ctx)
        return CopilotResponse(
            answer=answer,
            intent=intent,
            confidence=0.88,
            method="pilot_local_agent",
            reasoning=f"Local agent with {len(turn.tool_results)} tools",
            suggested_actions=turn.actions,
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
                    "transfer": "New Transfer",
                    "jobs": "Jobs",
                    "connectors": "Connectors",
                    "dashboard": "Dashboard",
                    "settings": "Settings",
                }
                parts.append(f"Opening **{labels.get(screen, screen)}** for you.")
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
                    f"Applying Studio remediation: **{tr.output.get('label')}**. "
                    "Opening Transfer Studio so the Validate step can run the fix."
                )
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
                lines = [f"Found **{tr.output.get('filtered', len(conns))}** connectors in our 620+ catalog:"]
                for c in conns:
                    status = c.get("status", "planned")
                    badge = "live" if status == "live" else status
                    lines.append(f"• **{c['name']}** ({badge}) — {c.get('description', '')[:60]}")
                parts.append("\n".join(lines))
            elif tr.name == "search_knowledge" and tr.success:
                hits = tr.output.get("hits", [])
                if hits:
                    best = hits[0]["text"]
                    if "Assistant:" in best:
                        answer = best.split("Assistant:", 1)[1].strip()
                        parts.append(answer[:1200])
                    else:
                        parts.append(best[:800])
                    if len(hits) > 1:
                        parts.append(f"_({len(hits)} trained knowledge matches)_")
                else:
                    parts.append("No trained knowledge matched — try listing datasets or analyzing a specific file.")

        if insight and not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            if not any(tr.name == "navigate" for tr in turn.tool_results):
                parts.append(self.analyst.compose_response(insight, message, intent))

        if not parts:
            datasets = ctx.get("datasets", [])
            if datasets:
                names = ", ".join(d["name"] for d in datasets[:4])
                parts.append(
                    f"I can help with any question about your data. Available datasets: **{names}**.\n\n"
                    "Try: \"Analyze logistics data\", \"Show my jobs\", \"What PII is in HR?\", "
                    "or \"Take me to transfer\"."
                )
            else:
                parts.append(
                    "Upload a file in **New Transfer** and I'll analyze everything — "
                    "columns, PII, quality, and mapping suggestions."
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
        return f"""{DATA_PILOT_PERSONA}

{self.context_builder.to_system_context(ctx)}

You have tools to list/analyze datasets, search data, list connectors and jobs, navigate the app, and check transfer capabilities.
Use tools whenever you need fresh data. Never guess — call the tool first."""

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
        if not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            prompts.append("Analyze my logistics data")
        prompts.extend([
            "Show my transfer jobs",
            "What PII is in my data?",
            "Take me to new transfer",
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
