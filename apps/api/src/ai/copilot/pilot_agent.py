"""
DataTransfer.space — Data Pilot Agent

Anthropic/Cursor-style agent: full data context, tool use, natural conversation.
Answers any data question and performs work in the app when asked.
"""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

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
# Per-provider hard cap. Client abort is 120s — keep native LLM attempts under that
# so the local agent can always answer before the browser times out.
_LLM_TURN_TIMEOUT_S = 20
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
    o = tr.output or {}
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
    if tr.name == "sample_connector_object":
        rid = o.get("result_id") or ""
        base = f"{o.get('row_count', 0)} rows from {o.get('table')}"
        return f"{base} · {rid}" if rid else base
    if tr.name == "run_query":
        rid = o.get("result_id") or ""
        base = f"{o.get('row_count', 0)} query rows"
        return f"{base} · {rid}" if rid else base
    if tr.name == "aggregate_data":
        if o.get("group_by"):
            return f"{o.get('group_count', 0)} groups by {o.get('group_by')}"
        return f"{o.get('metric')} = {o.get('value')}"
    if tr.name == "analyze_result":
        return f"profiled {o.get('result_id') or 'result'}"
    if tr.name == "filter_result":
        return f"{o.get('match_count', 0)} filtered rows"
    if tr.name == "introspect_connector_schema":
        return f"{o.get('column_count', 0)} cols on {o.get('table')}"
    if tr.name == "diff_schemas":
        return f"severity={o.get('severity')}"
    if tr.name == "map_connector_schemas":
        return f"{o.get('mapping_count', 0)} mappings"
    return "ok"


_METRIC_LABEL = {
    "count": "row count",
    "count_distinct": "distinct values",
    "sum": "total",
    "avg": "average",
    "min": "minimum",
    "max": "maximum",
}


def _fmt_metric_value(value: Any) -> str:
    """Group digits for readability without altering the value's precision."""
    if value is None:
        return "∅"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, Decimal)):
        # Decimal keeps the driver's exact scale — never round money here.
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return str(value)


_GATE_ICON = {"pass": "✓", "block": "✗", "warn": "!", "skip": "–"}


def _render_transfer(tool: str, o: dict[str, Any]) -> str:
    """Show the plan an operator has to sign off on: route, casts, gates.

    Lossy casts and blocking gates lead, because those are the two things that
    make a transfer the wrong thing to run. A clean plan says so in one line
    rather than burying it in a wall of every mapped column.
    """
    plan = o.get("plan") if tool == "start_transfer" and isinstance(o.get("plan"), dict) else o
    src = plan.get("source") or {}
    dst = plan.get("destination") or {}
    pf = plan.get("preflight") or {}
    route = (
        f"**{src.get('connector_name')}**.`{src.get('table')}` → "
        f"**{dst.get('connector_name')}**.`{dst.get('table')}`"
    )
    lines = [f"{route} — sync `{plan.get('sync_mode')}`"]

    dest_note = (
        "destination table exists"
        if dst.get("table_exists")
        else "destination table will be created"
    )
    lines.append(
        f"• Mapped **{plan.get('mapped_count')}** of {src.get('column_count')} source "
        f"columns ({dest_note})."
    )

    unmapped = plan.get("unmapped_source_columns") or []
    if unmapped:
        lines.append(
            f"• **{len(unmapped)} source column(s) have no destination**: "
            + ", ".join(f"`{c}`" for c in unmapped[:8])
            + ("…" if len(unmapped) > 8 else "")
        )

    lossy = plan.get("lossy_conversions") or []
    if lossy:
        lines.append(f"• **{len(lossy)} lossy cast(s)** — data changes shape on write:")
        for c in lossy[:5]:
            lines.append(
                f"  – `{c.get('source_column')}` {c.get('from_type')} → "
                f"{c.get('to_type')} on `{c.get('target_column')}`"
            )
    else:
        conversions = plan.get("type_conversions") or []
        if conversions:
            lines.append(
                f"• {len(conversions)} type conversion(s), none lossy — "
                "every value round-trips."
            )

    gates = pf.get("gates") or []
    if gates:
        summary = " ".join(
            f"{_GATE_ICON.get(str(g.get('status')).lower(), '?')}{g.get('id')}" for g in gates
        )
        lines.append(
            f"• Preflight **{pf.get('passed_count')}/{pf.get('total_gates')}** "
            f"({pf.get('readiness_score')}%): {summary}"
        )
    for blocker in (pf.get("blockers") or [])[:4]:
        lines.append(f"  – **BLOCK {blocker.get('id')}**: {blocker.get('message')}")
    if pf.get("run_id"):
        lines.append(f"• Preflight run `{pf.get('run_id')}`")

    if tool == "start_transfer" and o.get("requires_confirm"):
        if o.get("destructive"):
            lines.append(
                "\n**This overwrites the destination table.** "
                "Confirm below to run it; nothing moves until you do."
            )
        else:
            lines.append("\nConfirm below to run it — nothing moves until you do.")
    return "\n".join(lines)


def _render_aggregate(o: dict[str, Any]) -> str:
    """Report an exact aggregate: the number first, then the SQL that produced it."""
    metric = str(o.get("metric") or "count")
    label = _METRIC_LABEL.get(metric, metric)
    table = o.get("table")
    column = o.get("column")
    group_by = o.get("group_by")
    rows = list(o.get("rows") or [])
    cols = list(o.get("columns") or [])
    alias = str(o.get("metric_alias") or "")
    where = (
        f"**{o.get('connector_name')}**.`{table}`" if o.get("connector_name") else f"`{table}`"
    )
    measure = f"{label} of `{column}`" if column else label
    filters = str(o.get("filters") or "")
    scope = f" where {filters}" if filters else ""

    lines: list[str] = []
    if not group_by:
        value = _fmt_metric_value(o.get("value"))
        if metric == "count":
            lines.append(f"{where} has **{value} rows**{scope}.")
        else:
            lines.append(f"{measure.capitalize()} in {where}{scope}: **{value}**.")
    else:
        groups = int(o.get("group_count") or len(rows))
        head = (
            f"{measure.capitalize()} in {where}{scope} grouped by `{group_by}` — "
            f"**{groups} group{'s' if groups != 1 else ''}**"
        )
        if o.get("truncated"):
            head += f" (top {len(rows)} shown)"
        lines.append(head)
        # Result columns come back as the generated aliases; some engines fold
        # case (Snowflake upper-cases), so match them case-insensitively.
        dim_key = cols[0] if cols else str(group_by)
        val_key = next((c for c in cols if c.lower() == alias.lower()), "")
        if not val_key:
            val_key = cols[-1] if len(cols) > 1 else alias
        lines.append(f"| `{dim_key}` | `{val_key}` |")
        lines.append("| --- | ---: |")
        for row in rows[:15]:
            dim = row.get(dim_key)
            dim_s = "∅ (null)" if dim is None else str(dim)
            if len(dim_s) > 48:
                dim_s = dim_s[:45] + "…"
            lines.append(
                f"| {dim_s.replace('|', chr(92) + '|')} | "
                f"{_fmt_metric_value(row.get(val_key))} |"
            )
        if len(rows) > 15:
            lines.append(f"_Showing 15 of {len(rows)} groups._")

    lines.append("Exact server-side aggregate over the whole table — not a sample.")
    query = str(o.get("query") or "")
    if query:
        fence = "json" if str(o.get("type") or "").lower() == "mongodb" else "sql"
        lines.append(f"```{fence}\n{query[:400]}{'…' if len(query) > 400 else ''}\n```")
    if o.get("result_id"):
        lines.append(f"Result ref `{o['result_id']}` — ask to filter or profile it.")
    return "\n".join(lines)


def _unmapped_intent_reply(message: str, ctx: dict[str, Any]) -> str:
    """Honest fallback when no tool matched — never pretend the question was answered.

    The previous capability blurb made every unmapped prompt look like the pilot
    had understood and was offering a tour. Operators reading "I can help with…"
    after asking for an export or a transfer start thought the pilot ignored them.
    """
    from .tools import _looks_like_live_data_fetch

    lower = (message or "").strip().lower()
    connectors = ctx.get("connectors") or ctx.get("saved_connectors") or []
    conn_names = [
        str(c.get("name") or "").strip()
        for c in connectors
        if isinstance(c, dict) and c.get("name")
    ][:4]

    suggestions: list[str] = []
    if any(w in lower for w in ("export", "download", "csv", "parquet", "excel")):
        suggestions.append(
            'I can\'t export files yet — sample the table and use **Query** to pull '
            'larger result sets: "sample orders on Local Postgres".'
        )
    if _looks_like_live_data_fetch(lower) or re.search(
        r"\b(?:get|fetch|pull|show|sample)\b.+\b(?:from|on|in)\b",
        lower,
    ):
        on_conn = f" on {conn_names[0]}" if conn_names else " on Local Postgres"
        suggestions.append(
            f'To pull live rows, name the table and a saved connector: '
            f'"sample users{on_conn}" or "show orders from Warehouse".'
        )
    if any(w in lower for w in ("transfer", "sync", "move", "migrate", "copy", "replicate")):
        suggestions.append(
            'I can plan a transfer and stage a start — nothing moves until you Confirm. '
            'Try: "plan transfer of orders from Local Postgres to Warehouse" or '
            '"transfer orders from Local Postgres to Warehouse as upsert".'
        )
    if any(w in lower for w in ("delete", "drop", "remove", "destroy")):
        suggestions.append(
            "I only run read-only actions and confirmed connector creates — "
            "deletes have to be done in the UI so they can't be triggered by a prompt."
        )
    if any(w in lower for w in ("schedule", "pipeline", "cron", "every hour", "daily", "nightly")):
        suggestions.append(
            'I can list and trigger existing pipelines: "show my pipelines" or '
            '"run schedule <name> now". Creating a new schedule still needs the UI.'
        )
    if any(w in lower for w in ("fix", "repair", "heal", "remediate", "quarantine")):
        suggestions.append(
            'For a failed run, paste the job id or say "fix bad data" and I\'ll open '
            "the remediation path for that transfer."
        )
    if not suggestions and any(
        w in lower for w in ("count", "sum", "average", "avg", "total", "how many", "top ")
    ):
        suggestions.append(
            'For live totals name the table and connector: '
            '"count of orders by status on Local Postgres" or '
            '"average price in products on Local Postgres".'
        )
    if not suggestions:
        on_conn = f" on {conn_names[0]}" if conn_names else ""
        suggestions.append(
            "I can count / sum / average live tables, sample and profile rows, "
            "introspect schemas, map columns, list jobs and pipelines, and open "
            "the right screen."
        )
        suggestions.append(
            f'Try: "how many rows in airports{on_conn}", '
            f'"schema of airports{on_conn}", '
            '"show my jobs", or "what can you do?".'
        )

    quoted = (message or "").strip()
    if len(quoted) > 120:
        quoted = quoted[:117] + "…"
    head = (
        f'I\'m not sure how to do “{quoted}” yet.'
        if quoted
        else "I didn't catch a specific action in that message."
    )
    return head + "\n\n" + "\n".join(f"• {s}" for s in suggestions[:3])


def _score_response(resp: CopilotResponse | None) -> float:
    """Prefer grounded workspace answers; let a tool-using LLM beat local templates."""
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
        score -= fail * 0.1

    is_llm = any(m in method for m in ("anthropic", "openai", "ollama", "llm"))
    is_local = "local" in method

    # Ungrounded fluent LLM prose loses to local refuse / clarify.
    if is_llm and ok == 0:
        score -= 1.35
    # Tool-using LLM should beat equally grounded local templates (ChatGPT-quality narration).
    elif is_llm and ok > 0:
        score += 0.45
    elif is_local and ok > 0:
        score += 0.15

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


def _resolve_pilot_engine() -> str:
    """Pick the effective engine: local always works; hybrid when keys exist + requested/auto."""
    import os

    raw = (os.environ.get("DATAFLOW_PILOT_ENGINE") or "auto").strip().lower()
    if raw in {"local", "local_first", "deterministic"}:
        return "local"
    if raw in {"hybrid", "cloud"}:
        return raw
    # auto (default): use hybrid when any cloud/ollama provider is ready.
    try:
        from ..llm.provider import (
            DataTransferAnthropicProvider,
            DataTransferOllamaProvider,
            DataTransferOpenAIProvider,
        )

        if DataTransferAnthropicProvider().is_available():
            return "hybrid"
        if DataTransferOpenAIProvider().is_available():
            return "hybrid"
        if DataTransferOllamaProvider().is_available():
            return "hybrid"
    except Exception:
        pass
    return "local"


@dataclass
class PilotTurn:
    tool_results: list[ToolResult] = field(default_factory=list)
    actions: list[dict] = field(default_factory=list)
    pending_actions: list[dict] = field(default_factory=list)
    needs_clarification: str = ""


class DataPilotAgent:
    """
    Primary agent — ChatGPT-style tool loop when an LLM key is configured,
    otherwise a deterministic local planner that still executes real tools.

    1. Build full platform + data context
    2. Run tool loop (Anthropic/OpenAI when available, else local NL routing)
    3. Compose a natural-language answer grounded in real tool results
    """

    MAX_TOOL_ITERATIONS = 6

    def __init__(self):
        self.tools = get_pilot_tools()
        self.analyst = get_data_analyst()
        self.context_builder = get_context_builder()
        self._anthropic = None
        import uuid

        # Same-process multi-turn when the UI hasn't sent pilot_session_id yet.
        self._ephemeral_session = f"ephemeral-{uuid.uuid4().hex[:12]}"

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
        history = history or []
        data_context = self._ensure_data_context(data_context, history)
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
                    "I'm **Data Pilot** — ask me anything about your workspace. "
                    "I can count and aggregate live tables, sample and profile rows, "
                    "inspect schemas, plan or stage transfers (**Confirm** before anything moves), "
                    "triage jobs, and open Fix bad data in Transfer Studio. "
                    'Try: "how many rows in airports on Local Postgres", '
                    '"plan transfer of orders from Local Postgres to Warehouse", '
                    'or "fix bad data".'
                ),
                intent="greeting",
                confidence=1.0,
                method="greeting",
                suggested_prompts=self._starter_prompts()[:4],
            )

        # Meta questions stay on the local agent — never RAG-dump ontology shards
        # and never race cloud LLMs for a "who are you" answer.
        from .tools import _is_meta_pilot_question, _looks_like_unsupported_mutation
        if _is_meta_pilot_question(lower_msg):
            ctx = self.context_builder.build(data_context, message)
            return self._local_agent(message, history, ctx, data_context)

        # Delete / export / create-schedule paraphrases must refuse locally —
        # never race cloud or RAG into a fluent "here's how to delete…" answer.
        if _looks_like_unsupported_mutation(lower_msg):
            ctx = self.context_builder.build(data_context, message)
            return self._local_agent(message, history, ctx, data_context)

        ctx = self.context_builder.build(data_context, message)

        # Local always works offline. Hybrid/cloud when keys exist (auto detects).
        engine = _resolve_pilot_engine()
        local = self._local_agent(message, history or [], ctx, data_context)
        if engine == "local":
            return local

        system = self._build_system_prompt(ctx, data_context)

        # Product path: deterministic tools once, then optional LLM narration.
        # Never re-run mutating tools in a native LLM race (orphan Confirm acks).
        polished = self._polish_with_llm(message, history or [], local, system)
        local_ok = sum(1 for t in (local.tools_used or []) if t.get("success"))
        if local.pending_actions or local_ok > 0 or local.needs_clarification:
            return polished

        # Local had nothing grounded — allow a single native LLM tool loop for hard paraphrases.
        import time as _time
        from concurrent.futures import wait, FIRST_COMPLETED

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
                    self._anthropic_agent_loop, message, history or [], system, data_context
                )
            )
        elif openai_ready:
            llm_futs.append(
                _executor.submit(
                    self._openai_agent, message, history or [], system, data_context
                )
            )
        elif self._ollama_available_quick():
            llm_futs.append(
                _executor.submit(
                    self._ollama_agent, message, history or [], system, data_context
                )
            )

        if not llm_futs:
            return polished

        pending = set(llm_futs)
        deadline = _time.monotonic() + _LLM_TURN_TIMEOUT_S
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
                if isinstance(result, CopilotResponse):
                    if best_llm is None or _score_response(result) > _score_response(best_llm):
                        best_llm = result

        candidates = [c for c in (best_llm, polished, local) if isinstance(c, CopilotResponse)]
        return max(candidates, key=_score_response)

    def _run_local_recovery(
        self,
        turn: PilotTurn,
        message: str,
        data_context: dict | None,
    ) -> None:
        """OpenAI-style follow-up tools when the first plan fails closed.

        Railway-class chatbots don't stop at \"connector not found\" — they list
        what exists and tell the operator the next accurate step.
        """
        names = {tr.name for tr in turn.tool_results}
        def _is_connector_miss(err: str) -> bool:
            low = (err or "").lower()
            return any(
                needle in low
                for needle in (
                    "no connector matched",
                    "which connector",
                    "connector not found",
                    "connector not found in store",
                    "name a saved connector",
                )
            )

        connector_miss = any(
            (not tr.success) and tr.error and _is_connector_miss(tr.error)
            for tr in turn.tool_results
        )
        if connector_miss and "list_connectors" not in names:
            tr = self.tools.execute("list_connectors", {})
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)
            if tr.success and not turn.needs_clarification:
                conns = (tr.output or {}).get("connectors") or []
                if conns:
                    listed = ", ".join(
                        f"**{c.get('name')}**" for c in conns[:6] if c.get("name")
                    )
                    turn.needs_clarification = (
                        "Which saved connector should I use? "
                        f"{listed}. Or say e.g. "
                        '"create a postgres connector at host…".'
                    )
                else:
                    turn.needs_clarification = (
                        "No saved connectors yet. Add one under **Connectors**, or paste a "
                        "connection URL and ask me to create it (Confirm required)."
                    )

        # Dataset miss → list uploads so the operator can pick a real name.
        dataset_miss = any(
            (not tr.success)
            and tr.error
            and "dataset" in tr.error.lower()
            and "not found" in tr.error.lower()
            for tr in turn.tool_results
        )
        if dataset_miss and "list_datasets" not in names:
            ds = self.tools.execute("list_datasets", {})
            turn.tool_results.append(ds)
            self._append_tool_actions(turn, ds)
            if ds.success and not turn.needs_clarification:
                datasets = (ds.output or {}).get("datasets") or []
                if datasets:
                    listed = ", ".join(
                        f"**{d.get('name')}**" for d in datasets[:6] if d.get("name")
                    )
                    turn.needs_clarification = (
                        "I couldn't find that dataset. Indexed uploads: "
                        f"{listed}. Name one exactly, or upload in **New Transfer**."
                    )
                else:
                    turn.needs_clarification = (
                        "No uploaded datasets indexed yet. Upload a CSV/JSON in "
                        "**New Transfer**, then ask me to analyze it."
                    )

        # Aggregate missing table → list objects on the named connector.
        for tr in list(turn.tool_results):
            if tr.name != "aggregate_data" or tr.success or not tr.error:
                continue
            if "which table" not in tr.error.lower():
                continue
            if "list_connector_objects" in {t.name for t in turn.tool_results}:
                break
            # Prefer connector from the failed tool args if present in error/output.
            cname = ""
            # Recover from sibling planned args via clarification text bold name.
            import re as _re

            m = _re.search(r"on \*\*([^*]+)\*\*", tr.error or "")
            if m:
                cname = m.group(1).strip()
            if not cname:
                # Fall back: ask list_connectors if we cannot scope.
                continue
            objs = self.tools.execute("list_connector_objects", {"connector_name": cname})
            turn.tool_results.append(objs)
            self._append_tool_actions(turn, objs)
            if objs.success and not turn.needs_clarification:
                rows = (objs.output or {}).get("objects") or (objs.output or {}).get("tables") or []
                table_names = []
                for o in rows[:12]:
                    if isinstance(o, dict) and (o.get("name") or o.get("table")):
                        table_names.append(str(o.get("name") or o.get("table")))
                    elif isinstance(o, str):
                        table_names.append(o)
                if table_names:
                    turn.needs_clarification = (
                        f"Which table on **{cname}**? "
                        + ", ".join(f"`{n}`" for n in table_names)
                        + '. Say e.g. "from orders".'
                    )
            break

        # Suggestions with no active dataset → list uploads so the operator can pick.
        for tr in list(turn.tool_results):
            if tr.name != "profile_quality_rules" or not tr.success:
                continue
            cols = int((tr.output or {}).get("column_count") or 0)
            if cols > 0:
                continue
            if "list_datasets" not in {t.name for t in turn.tool_results}:
                ds = self.tools.execute("list_datasets", {})
                turn.tool_results.append(ds)
                self._append_tool_actions(turn, ds)
            # Live focus: sample the remembered table so suggestions aren't empty.
            try:
                from .working_memory import get_working_memory

                sid = self._session_id(data_context)
                focus = get_working_memory().get_focus(sid) if sid else None
                if (
                    focus
                    and focus.table
                    and "sample_connector_object" not in {t.name for t in turn.tool_results}
                ):
                    sample_args = {"table": focus.table, "limit": 50}
                    if focus.connector_name:
                        sample_args["connector_name"] = focus.connector_name
                    elif focus.connector_id:
                        sample_args["connector_id"] = focus.connector_id
                    sample = self.tools.execute("sample_connector_object", sample_args)
                    turn.tool_results.append(sample)
                    self._append_tool_actions(turn, sample)
            except Exception:
                pass
            break

        # Uploaded-dataset compare miss → list datasets (and live tables if focused).
        for tr in list(turn.tool_results):
            if tr.name != "compare_datasets" or tr.success or not tr.error:
                continue
            if "not found" not in tr.error.lower():
                continue
            if "list_datasets" not in {t.name for t in turn.tool_results}:
                ds = self.tools.execute("list_datasets", {})
                turn.tool_results.append(ds)
                self._append_tool_actions(turn, ds)
            try:
                from .working_memory import get_working_memory

                sid = self._session_id(data_context)
                focus = get_working_memory().get_focus(sid) if sid else None
                if (
                    focus
                    and (focus.connector_name or focus.connector_id)
                    and "list_connector_objects" not in {t.name for t in turn.tool_results}
                ):
                    args = {}
                    if focus.connector_name:
                        args["connector_name"] = focus.connector_name
                    else:
                        args["connector_id"] = focus.connector_id
                    objs = self.tools.execute("list_connector_objects", args)
                    turn.tool_results.append(objs)
                    self._append_tool_actions(turn, objs)
                    if objs.success and not turn.needs_clarification:
                        names = (objs.output or {}).get("objects") or []
                        listed = ", ".join(f"`{n}`" for n in names[:12] if n)
                        turn.needs_clarification = (
                            f"{tr.error} Those look like live tables — on "
                            f"**{focus.connector_name or 'this connector'}** I see: "
                            f"{listed or 'none'}. "
                            'Try: "diff schema orders on PilotSQLite vs orders on Warehouse".'
                        )
            except Exception:
                pass
            break

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
                or "no connector matched" in err.lower()
                or "which connector" in err.lower()
                or "connector not found" in err.lower()
                or ("dataset" in err.lower() and "not found" in err.lower())
                or tr.name in ("run_schedule_now", "get_schedule", "open_schedule", "create_connector")
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

        if tr.name == "create_connector":
            turn.pending_actions.append({
                "id": f"create_connector:{out.get('ack_id') or len(turn.pending_actions)}",
                "type": "create_connector",
                "label": out.get("label") or "Save connector",
                "risk": "mutate",
                "payload": {
                    "ack_id": out.get("ack_id"),
                    "preview": out.get("preview") or {},
                },
            })
            turn.actions.append({
                "type": "navigate",
                "screen": "connectors",
                "risk": "safe",
                "label": "Open Connectors",
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
            # The ack id comes first: it is the only part that is unique per
            # staged mutation, so two transfers proposed in one turn stay
            # separately approvable instead of collapsing onto one row.
            marker = (
                out.get("ack_id")
                or out.get("kind")
                or out.get("schedule_id")
                or out.get("id")
                or len(turn.pending_actions)
            )
            turn.pending_actions.append({
                "id": f"{tr.name}:{marker}",
                "type": out.get("action") or tr.name,
                "label": out.get("label") or "Confirm this change",
                "risk": "mutate",
                "destructive": bool(out.get("destructive")),
                "payload": out,
            })

    def _anthropic_agent_loop(
        self,
        message: str,
        history: list[dict],
        system: str,
        data_context: dict | None = None,
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
                    self._commit_memory(
                        [(tr.name, {}) for tr in turn.tool_results],
                        turn,
                        data_context,
                    )
                    self._run_local_recovery(turn, message, data_context)
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
                args = self._with_result_context(tc["name"], tc.get("input") or {}, data_context)
                tr = self.tools.execute(tc["name"], args)
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

    @staticmethod
    def _session_id(data_context: dict | None) -> str:
        return str((data_context or {}).get("pilot_session_id") or "").strip()

    def _ensure_data_context(
        self,
        data_context: dict | None,
        history: list[dict] | None = None,
    ) -> dict:
        """Guarantee a session id so follow-ups / result refs work like Railway chat.

        The web UI sends ``pilot_session_id`` per conversation. API callers and
        tests often omit it — without a fallback, \"only paid ones\" and
        \"analyze that\" become amnesiac dead-ends. Fall back to this agent's
        ephemeral id so same-process multi-turn stays coherent.
        """
        ctx = dict(data_context or {})
        if str(ctx.get("pilot_session_id") or "").strip():
            return ctx
        ctx["pilot_session_id"] = self._ephemeral_session
        # Keep a short transcript digest for LLM narration (not used for hashing).
        if history:
            ctx["_history_turns"] = len(history)
        return ctx

    def _polish_with_llm(
        self,
        message: str,
        history: list[dict],
        local: CopilotResponse,
        system: str,
    ) -> CopilotResponse:
        """Narrate grounded local tool results with a cloud LLM when available.

        Tools stay deterministic (local). The LLM only rewrites the answer in
        natural ChatGPT-quality prose — never invents new facts.
        """
        tools = local.tools_used or []
        ok = sum(1 for t in tools if t.get("success"))
        if ok == 0 and not local.pending_actions:
            return local

        provider = None
        method = "llm_polish"
        try:
            from ..llm.provider import (
                DataTransferAnthropicProvider,
                DataTransferOpenAIProvider,
            )

            openai = DataTransferOpenAIProvider()
            if openai.is_available():
                provider = openai
                method = "openai_polish"
            else:
                anthropic = DataTransferAnthropicProvider()
                if anthropic.is_available():
                    provider = anthropic
                    method = "anthropic_polish"
        except Exception:
            return local
        if provider is None:
            return local

        tool_bits = []
        for t in tools[:8]:
            tool_bits.append(
                f"- {t.get('name')}: {'ok' if t.get('success') else 'fail'} — {t.get('summary')}"
            )
        history_text = "\n".join(
            f"{m.get('role', 'user').capitalize()}: {m.get('content', '')}"
            for m in (history or [])[-6:]
            if (m.get("content") or "").strip()
        )
        prompt = f"""Rewrite the Data Pilot answer below in clear, natural product language.

Rules:
- Keep every fact from the draft answer and tool summaries — do not invent IDs, row counts, or connectors
- Do not mention tools, APIs, or method names
- If the draft asks a clarification question, keep it
- If Confirm is required, say so plainly
- Be concise (2–8 short sentences or a short bullet list)

Tool evidence:
{chr(10).join(tool_bits) or 'None'}

History:
{history_text or 'None'}

User: {message}

Draft answer:
{local.answer}
"""
        try:
            response = provider.generate(prompt, system=system, max_tokens=900)
        except Exception:
            return local
        if not response.success or not (response.content or "").strip():
            return local
        polished = response.content.strip()
        # Guardrail: empty or tiny polish is useless; keep local.
        if len(polished) < 20:
            return local
        return CopilotResponse(
            answer=polished,
            intent=local.intent,
            confidence=min(0.96, float(local.confidence or 0.9) + 0.03),
            method=method,
            reasoning=f"Local tools + {method.split('_')[0].title()} narration",
            suggested_actions=local.suggested_actions,
            pending_actions=local.pending_actions,
            needs_clarification=local.needs_clarification,
            suggested_prompts=local.suggested_prompts,
            data_insight=local.data_insight,
            tools_used=local.tools_used,
        )

    def _plan_with_memory(
        self,
        message: str,
        data_context: dict | None,
        history: list[dict] | None = None,
    ) -> list[tuple[str, dict]]:
        """Resolve this turn against session working memory, then plan tools.

        Order matters: a bare reply answers an open question first, an elliptical
        turn edits the last query second, and only then do we parse from scratch —
        otherwise "and by region?" is parsed as a brand-new request and matches
        nothing, which is what made the pilot look amnesiac.
        """
        from .followup import (
            inherit_focus_slots,
            looks_like_followup,
            looks_like_fresh_intent,
            pending_from_assistant_clarification,
            resolve_followup,
            resolve_pending_answer,
            resolve_platform_coreference,
            resolve_table_coreference_tools,
        )
        from .working_memory import get_working_memory

        session_id = self._session_id(data_context)
        if not session_id:
            platform = resolve_platform_coreference(message, history)
            if platform:
                return platform
            return infer_tools_from_message(message)

        memory = get_working_memory()
        focus = memory.get_focus(session_id)

        pending = memory.get_pending(session_id)
        if not pending:
            soft = pending_from_assistant_clarification(history)
            if soft:
                answered = resolve_pending_answer(message, soft)
                if answered:
                    return [answered]
        if pending:
            answered = resolve_pending_answer(message, pending)
            if answered:
                memory.clear_pending(session_id)
                return [answered]
            # Fresh intents clear the slot; typos / non-answers keep it open.
            if looks_like_fresh_intent(message):
                memory.clear_pending(session_id)
            else:
                return []

        platform = resolve_platform_coreference(message, history)
        if platform:
            return platform

        table_coref = resolve_table_coreference_tools(message, focus)
        if table_coref:
            return table_coref

        planned = infer_tools_from_message(message)
        # Elliptical edits beat a fresh under-specified parse ("what about average
        # amount" would otherwise lose the remembered WHERE / table).
        if focus and looks_like_followup(message, focus):
            low = message.lower().strip()
            # Fully grounded fresh aggregate (explicit table ≠ focus) wins.
            for name, args in planned:
                if name != "aggregate_data":
                    continue
                explicit_table = str((args or {}).get("table") or "").strip().lower()
                if explicit_table and explicit_table != (focus.table or "").lower():
                    return inherit_focus_slots(planned, focus)
                if explicit_table and (args or {}).get("connector_name"):
                    return planned
            # Stored-sample row filters stay on filter_result, not a new aggregate.
            if focus.result_id and re.match(r"^(?:filter|where)\b", low):
                if planned and any(n == "filter_result" for n, _ in planned):
                    return inherit_focus_slots(planned, focus)
                return [("filter_result", {"result_id": focus.result_id})]
            edit = resolve_followup(message, focus)
            if edit is not None:
                return [("aggregate_data", edit.as_tool_args())]
            # Coreference with focus but no metric edit — still prefer table tools.
            if table_coref := resolve_table_coreference_tools(message, focus):
                return table_coref
        if not planned:
            edit = resolve_followup(message, focus)
            if edit is not None:
                # Known subject (even with missing measure) — let the tool ask
                # against the real schema rather than answering the wrong question.
                return [("aggregate_data", edit.as_tool_args())]
            # Result follow-ups when focus has a stored sample/query.
            if focus and focus.result_id:
                low = message.lower().strip()
                if re.search(r"\b(?:analyze|profile|summarize)\b.*\b(?:that|this|it|result|sample)\b", low) or low in {
                    "analyze that", "analyze this", "profile that", "summarize that",
                }:
                    return [("analyze_result", {"result_id": focus.result_id})]
                if re.match(r"^(?:filter|where)\b", low):
                    return [("filter_result", {"result_id": focus.result_id})]
        return inherit_focus_slots(planned, focus)

    def _commit_memory(
        self,
        planned: list[tuple[str, dict]],
        turn: PilotTurn,
        data_context: dict | None,
    ) -> None:
        """Persist the resolved subject, or the question we just asked."""
        session_id = self._session_id(data_context)
        if not session_id:
            return
        from .followup import clarification_slot, focus_from_tool_output
        from .working_memory import get_working_memory

        memory = get_working_memory()
        args_by_tool = {name: args for name, args in planned}

        for tr in turn.tool_results:
            if tr.success:
                update = focus_from_tool_output(tr.name, tr.output or {})
                if update:
                    memory.update_focus(session_id, **update)
                    memory.clear_pending(session_id)
            else:
                slot = clarification_slot(tr.name, args_by_tool.get(tr.name, {}), tr.error)
                if slot:
                    memory.remember_pending(session_id, slot)
                    if not turn.needs_clarification:
                        turn.needs_clarification = slot.question

    def _with_result_context(self, name: str, args: dict | None, data_context: dict | None) -> dict:
        """Inject session / last_result_id so follow-ups hit the real stored rows."""
        out = dict(args or {})
        ctx = data_context or {}
        session_id = str(ctx.get("pilot_session_id") or "").strip()
        last_result_id = str(ctx.get("last_result_id") or "").strip()
        if not last_result_id and session_id and name in ("analyze_result", "filter_result"):
            try:
                from .working_memory import get_working_memory

                focus = get_working_memory().get_focus(session_id)
                if focus and focus.result_id:
                    last_result_id = focus.result_id
            except Exception:
                pass
        if session_id and name in (
            "sample_connector_object",
            "aggregate_data",
            "run_query",
            "analyze_result",
            "filter_result",
        ):
            out.setdefault("session_id", session_id)
        if last_result_id and name in ("analyze_result", "filter_result"):
            out.setdefault("result_id", last_result_id)
        return out

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

        intent = self._detect_intent(message)
        turn = PilotTurn()
        messages: list[dict] = []
        for m in history[-10:]:
            role = m.get("role", "user")
            if role not in ("user", "assistant"):
                continue
            content = (m.get("content") or "").strip()
            if content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": message})

        # Prefer native tool calling; fall back to local plan + narration.
        used_native = False
        for _ in range(self.MAX_TOOL_ITERATIONS):
            response = openai.generate_agent(
                messages=messages,
                system=system,
                tools=TOOL_DEFINITIONS,
                max_tokens=4096,
            )
            if not response.get("success"):
                break
            used_native = True
            tool_calls = response.get("tool_calls") or []
            if not tool_calls:
                text = (response.get("content") or "").strip()
                if text:
                    self._commit_memory(
                        [(tr.name, {}) for tr in turn.tool_results],
                        turn,
                        data_context,
                    )
                    self._run_local_recovery(turn, message, data_context)
                    return CopilotResponse(
                        answer=text,
                        intent=intent,
                        confidence=0.92,
                        method="openai_agent",
                        reasoning=f"OpenAI tool loop, {len(turn.tool_results)} tool calls",
                        suggested_actions=turn.actions,
                        pending_actions=turn.pending_actions,
                        needs_clarification=turn.needs_clarification,
                        suggested_prompts=self._follow_ups(message, turn),
                        data_insight=self._data_insight_from_turn(turn),
                        tools_used=_tools_used(turn),
                    )
                break

            assistant_msg: dict = {
                "role": "assistant",
                "content": response.get("content") or None,
                "tool_calls": [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc.get("input") or {}, default=json_default),
                        },
                    }
                    for tc in tool_calls
                ],
            }
            messages.append(assistant_msg)
            for tc in tool_calls:
                args = self._with_result_context(tc["name"], tc.get("input") or {}, data_context)
                tr = self.tools.execute(tc["name"], args)
                turn.tool_results.append(tr)
                self._append_tool_actions(turn, tr)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(
                        tr.output if tr.success else {"error": tr.error},
                        default=json_default,
                    ),
                })

        if used_native and turn.tool_results:
            # Tool loop exhausted without a final text — compose locally.
            self._commit_memory(
                [(tr.name, {}) for tr in turn.tool_results],
                turn,
                data_context,
            )
            self._run_local_recovery(turn, message, data_context)
            ctx = get_context_builder().build(data_context)
            insight = self._data_insight_from_turn(turn)
            answer = self._compose_local_answer(message, intent, turn, insight, ctx)
            return CopilotResponse(
                answer=answer,
                intent=intent,
                confidence=0.88,
                method="openai_agent_compose",
                suggested_actions=turn.actions,
                pending_actions=turn.pending_actions,
                needs_clarification=turn.needs_clarification,
                suggested_prompts=self._follow_ups(message, turn),
                data_insight=insight,
                tools_used=_tools_used(turn),
            )

        # Fallback: local NL plan + OpenAI narration (no native tools).
        planned = self._plan_with_memory(message, data_context, history)
        turn = PilotTurn()
        for name, args in planned:
            tr = self.tools.execute(name, self._with_result_context(name, args, data_context))
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)
        self._commit_memory(planned, turn, data_context)
        self._run_local_recovery(turn, message, data_context)

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
            intent=intent,
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

        planned = self._plan_with_memory(message, data_context, history)
        turn = PilotTurn()
        for name, args in planned:
            tr = self.tools.execute(name, self._with_result_context(name, args, data_context))
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)
        self._commit_memory(planned, turn, data_context)
        self._run_local_recovery(turn, message, data_context)

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

        planned = self._plan_with_memory(message, data_context, history)
        if not planned:
            # Typo / non-answer while a clarification is open — re-ask, keep slot.
            session_id = self._session_id(data_context)
            if session_id:
                from .working_memory import get_working_memory

                pending = get_working_memory().get_pending(session_id)
                if pending and pending.question:
                    turn.needs_clarification = pending.question
                    hint = ""
                    if pending.candidates:
                        shown = ", ".join(f"**{c}**" for c in pending.candidates[:6])
                        hint = f"\n\nAvailable: {shown}."
                    return CopilotResponse(
                        answer=f"{pending.question}{hint}\n\nI didn't match that reply — try a name from the list, or ask a new question.".strip(),
                        intent=intent,
                        confidence=0.78,
                        method="pilot_local_engine",
                        reasoning="Re-ask open clarification",
                        needs_clarification=pending.question,
                        suggested_prompts=list(pending.candidates or [])[:4] or self._starter_prompts()[:3],
                        tools_used=[],
                    )

        for name, args in planned:
            tr = self.tools.execute(name, self._with_result_context(name, args, data_context))
            turn.tool_results.append(tr)
            self._append_tool_actions(turn, tr)
        self._commit_memory(planned, turn, data_context)
        self._run_local_recovery(turn, message, data_context)

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
                "sample_connector_object",
                "aggregate_data",
                "run_query",
                "analyze_result",
                "filter_result",
                "diff_schemas",
                "map_connector_schemas",
                "list_connectors",
            )
            for tr in turn.tool_results
        )
        # A successful workspace tool already answered the question. Never append
        # a fixture-dataset profile beside it — that made "4 rows" look like it
        # was about some unrelated uploaded CSV.
        grounded = any(tr.success for tr in turn.tool_results)
        has_session_data = bool(data_context and data_context.get("columns"))
        wants_analysis = self.analyst.wants_data_analysis(message, intent)
        insight = None
        if (
            not list_only
            and not has_knowledge
            and not has_connector
            and not described
            and not live_schema
            and not grounded
            and (has_session_data or wants_analysis)
            and not (navigated and not wants_analysis)
        ):
            # Only profile a fixture/upload when the user actually asked about
            # data, or the UI handed us a session schema. resolve_dataset(None)
            # otherwise silently profiles the first uploaded file for every
            # unmapped prompt ("export orders to csv" → random CSV analysis).
            hint = self.analyst.extract_dataset_hint(message) if wants_analysis else None
            if has_session_data or hint:
                insight = self.analyst.analyze_context(data_context, hint)
            if not insight and wants_analysis and hint:
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
            method="pilot_local_engine",
            reasoning=f"Local OpenAI-style tool loop · {len(turn.tool_results)} tools",
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
                # When Confirm is still required, do not claim we already opened the screen.
                if turn.pending_actions:
                    parts.append(
                        f"After you Confirm, I'll take you to **{labels.get(screen, screen)}**."
                    )
                else:
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
                want_failures = any(
                    w in (message or "").lower()
                    for w in ("fail", "failed", "failure", "error", "broken")
                )
                failed = [
                    j
                    for j in jobs
                    if str(j.get("status") or "").lower()
                    in {"failed", "cancelled", "error"}
                ]
                if jobs:
                    if want_failures:
                        if failed:
                            lines = [
                                f"**{len(failed)}** of your last **{len(jobs)}** job(s) failed:"
                            ]
                            show = failed[:5]
                        else:
                            lines = [
                                f"None of your last **{len(jobs)}** job(s) failed "
                                "(in this window):"
                            ]
                            show = jobs[:5]
                        for j in show:
                            lines.append(
                                f"• `{j.get('id', '?')}` · {j.get('source', '?')} -> "
                                f"{j.get('destination', '?')}: "
                                f"**{j.get('status')}** ({j.get('records', 0):,} records)"
                            )
                        if not failed:
                            lines.append(
                                "Open **Jobs** for full history, or paste a job id."
                            )
                    else:
                        lines = ["Here are your **recent transfer jobs**:"]
                        for j in jobs[:5]:
                            lines.append(
                                f"• `{j.get('id', '?')}` · {j.get('source', '?')} -> "
                                f"{j.get('destination', '?')}: "
                                f"**{j.get('status')}** ({j.get('records', 0):,} records)"
                            )
                    parts.append("\n".join(lines))
                else:
                    parts.append(
                        "No transfer jobs yet. Ask me to **plan** or **start** a transfer "
                        "(Confirm required), or open **Transfer Studio**."
                    )
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
                    f"Proposed Studio remediation: **{tr.output.get('label')}**.\n"
                    "Confirm opens **Fix bad data** in Transfer Studio — "
                    "it does not rewrite quarantine rows inside this chat."
                )
            elif tr.name in ("plan_transfer", "start_transfer") and tr.success:
                parts.append(_render_transfer(tr.name, tr.output or {}))
            elif tr.name == "start_transfer" and not tr.success and isinstance(tr.output, dict):
                # A blocked transfer still owes the operator the gate evidence.
                parts.append(f"{tr.error}\n\n{_render_transfer('plan_transfer', tr.output)}")
            elif tr.name == "plan_transfer_route" and tr.success:
                o = tr.output or {}
                if not o.get("generic"):
                    parts.append(_render_transfer("plan_transfer", o))
                else:
                    parts.append(
                        f"**Standard gate sequence**: {', '.join(o.get('required_gates') or [])}\n"
                        f"{o.get('note') or ''}\n{o.get('next') or ''}"
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
                cols = int(o.get("column_count") or 0)
                if cols <= 0:
                    gates = o.get("preflight_gates") or []
                    gate_line = (
                        f"\n\nValidate runs **{len(gates)} preflight gates**: "
                        + ", ".join(f"`{g}`" for g in gates[:9])
                        if gates
                        else "\n\nValidate runs **9 preflight gates** (G1–G9) before Execute."
                    )
                    parts.append(
                        "I can suggest quality gates, but there's **no active dataset** loaded yet.\n"
                        "• Upload a CSV/JSON in **Transfer**, or\n"
                        "• Ask me to **list datasets** / analyze a named upload, or\n"
                        "• Sample a live table: "
                        '"sample orders on <connector>"\n\n'
                        "Enterprise rules I apply once data is in scope:\n"
                        + "\n".join(f"• {r}" for r in rules[:6])
                        + gate_line
                    )
                else:
                    pii = o.get("pii_candidates") or []
                    extra = (
                        f"\nPII-looking columns: {', '.join(f'`{c}`' for c in pii[:8])}"
                        if pii else ""
                    )
                    parts.append(
                        f"Quality suggestions for **{o.get('dataset')}** "
                        f"({cols} columns):{extra}\n"
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
                        f"• `{c.get('name')}` -> **{c.get('inferred_type')}**"
                        + (f" ({c.get('data_type')})" if c.get("data_type") else "")
                        + f" · {null}"
                    )
                if len(cols) > 40:
                    lines.append(f"• …and {len(cols) - 40} more columns")
                for w in (o.get("warnings") or [])[:3]:
                    lines.append(f"⚠ {w}")
                parts.append("\n".join(lines))
            elif tr.name in ("sample_connector_object", "run_query") and tr.success:
                o = tr.output or {}
                cols = o.get("columns") or []
                rows = o.get("rows") or []
                title = (
                    f"Live sample **{o.get('connector_name')}**.`{o.get('table')}`"
                    if tr.name == "sample_connector_object"
                    else f"Query on **{o.get('connector_name')}**"
                )
                lines = [
                    f"{title} ({o.get('type')}) — **{o.get('row_count', len(rows))} rows**"
                    + (" (truncated)" if o.get("truncated") else "")
                    + f" · {len(cols)} columns · read-only"
                ]
                if o.get("result_id"):
                    lines.append(f"Result ref `{o['result_id']}` (ask to analyze or filter this).")
                if tr.name == "run_query" and o.get("query"):
                    q = str(o.get("query") or "")
                    lines.append(f"```sql\n{q[:500]}{'…' if len(q) > 500 else ''}\n```")
                # Compact markdown table (cap columns/rows for chat)
                show_cols = cols[:8]
                if show_cols and rows:
                    header = "| " + " | ".join(f"`{c}`" for c in show_cols) + " |"
                    sep = "| " + " | ".join("---" for _ in show_cols) + " |"
                    lines.append(header)
                    lines.append(sep)
                    for r in rows[:8]:
                        cells = []
                        for c in show_cols:
                            v = r.get(c)
                            s = "∅" if v is None else str(v)
                            if len(s) > 40:
                                s = s[:37] + "…"
                            cells.append(s.replace("|", "\\|"))
                        lines.append("| " + " | ".join(cells) + " |")
                    if len(cols) > 8:
                        lines.append(f"_Showing {len(show_cols)}/{len(cols)} columns_")
                analysis = o.get("analysis") or {}
                for cprof in (analysis.get("columns") or [])[:6]:
                    ex = ", ".join(f"`{e}`" for e in (cprof.get("examples") or [])[:2])
                    null_pct = round(100 * float(cprof.get("null_rate") or 0))
                    lines.append(
                        f"• `{cprof.get('column')}` — {cprof.get('inferred_kind', '?')} · "
                        f"{cprof.get('non_null')}/{analysis.get('row_count_sampled')} non-null"
                        f" ({null_pct}% null)"
                        + (f" · e.g. {ex}" if ex else "")
                    )
                signals = analysis.get("signals") or {}
                if signals.get("null_heavy_columns"):
                    lines.append(
                        "Null-heavy: "
                        + ", ".join(f"`{c}`" for c in signals["null_heavy_columns"][:6])
                    )
                lines.append("Open **Query** for larger results or exports.")
                parts.append("\n".join(lines))
            elif tr.name == "aggregate_data" and tr.success:
                parts.append(_render_aggregate(tr.output or {}))
            elif tr.name == "analyze_result" and tr.success:
                o = tr.output or {}
                analysis = o.get("analysis") or {}
                lines = [
                    f"Profile of stored result `{o.get('result_id')}`"
                    + (f" · **{o.get('connector_name')}**.`{o.get('table')}`" if o.get("table") else "")
                    + f" — **{analysis.get('row_count_sampled', o.get('row_count', 0))} rows**, "
                    f"{analysis.get('column_count', 0)} columns"
                ]
                for cprof in (analysis.get("columns") or [])[:12]:
                    kind = cprof.get("inferred_kind") or "?"
                    null_pct = round(100 * float(cprof.get("null_rate") or 0))
                    bit = (
                        f"• `{cprof.get('column')}` — **{kind}** · "
                        f"{null_pct}% null · {cprof.get('distinct_in_sample')} distinct"
                    )
                    num = cprof.get("numeric") or {}
                    if num:
                        bit += f" · min {num.get('min')} / max {num.get('max')} / mean {num.get('mean')}"
                    top = cprof.get("top_values") or []
                    if top and kind in {"string", "boolean", "integer"}:
                        tv = ", ".join(f"`{t.get('value')}`×{t.get('count')}" for t in top[:3])
                        bit += f" · top {tv}"
                    lines.append(bit)
                signals = analysis.get("signals") or {}
                if signals.get("null_heavy_columns"):
                    lines.append(
                        "Null-heavy columns: "
                        + ", ".join(f"`{c}`" for c in signals["null_heavy_columns"][:8])
                    )
                if signals.get("high_cardinality_columns"):
                    lines.append(
                        "High cardinality: "
                        + ", ".join(f"`{c}`" for c in signals["high_cardinality_columns"][:8])
                    )
                parts.append("\n".join(lines))
            elif tr.name == "filter_result" and tr.success:
                o = tr.output or {}
                cols = o.get("columns") or []
                rows = o.get("rows") or []
                filt = o.get("filter") or {}
                lines = [
                    f"Filtered `{filt.get('column')}` **{filt.get('op')}** "
                    f"`{filt.get('value') or ''}` → **{o.get('match_count', 0)}** / "
                    f"{o.get('source_row_count', 0)} rows"
                    + (f" · ref `{o.get('result_id')}`" if o.get("result_id") else "")
                ]
                show_cols = cols[:8]
                if show_cols and rows:
                    header = "| " + " | ".join(f"`{c}`" for c in show_cols) + " |"
                    sep = "| " + " | ".join("---" for _ in show_cols) + " |"
                    lines.append(header)
                    lines.append(sep)
                    for r in rows[:8]:
                        cells = []
                        for c in show_cols:
                            v = r.get(c)
                            s = "∅" if v is None else str(v)
                            if len(s) > 40:
                                s = s[:37] + "…"
                            cells.append(s.replace("|", "\\|"))
                        lines.append("| " + " | ".join(cells) + " |")
                elif not rows:
                    lines.append("_No rows matched that filter on the stored sample._")
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
            elif tr.name == "create_connector" and tr.success:
                prev = tr.output.get("preview") or {}
                parts.append(
                    f"Ready to save connector **{prev.get('name') or 'connector'}** "
                    f"({prev.get('type')}) → `{prev.get('host')}:{prev.get('port')}` "
                    f"/ `{prev.get('database') or '—'}`.\n"
                    f"Connection test: {prev.get('test') or 'ok'}.\n\n"
                    "Confirm below to save it to **Connectors**."
                )
            elif tr.name == "list_connectors" and tr.success:
                conns = tr.output.get("connectors", [])
                ask_tables = any(
                    w in (message or "").lower()
                    for w in ("table", "tables", "collections", "objects")
                )
                if conns:
                    lines = [f"You have **{len(conns)} saved connector(s)**:"]
                    for c in conns:
                        lines.append(
                            f"• **{c.get('name')}** ({c.get('type')}) → "
                            f"{c.get('database', c.get('host', ''))}"
                        )
                    if ask_tables:
                        names = [str(c.get("name") or "") for c in conns if c.get("name")]
                        sample = names[0] if names else "Local Postgres"
                        lines.append(
                            f'To list tables, name the connector: '
                            f'"list tables on {sample}".'
                        )
                    parts.append("\n".join(lines))
                else:
                    parts.append(
                        "No connectors saved yet. Go to **Connectors** to add "
                        "MongoDB, PostgreSQL, or Snowflake."
                    )
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
                    "I'm **Data Pilot** — I help with analytics, routes, schema "
                    "risk, mappings, jobs, and fixes inside DataFlow. I answer from "
                    "your workspace first; I never invent warehouse facts.",
                    "**I can:**",
                ]
                for item in (o.get("can") or [])[:8]:
                    lines.append(f"• {item}")
                cannot = o.get("cannot_yet") or []
                if cannot:
                    lines.append("**Not yet from chat:**")
                    for item in cannot[:3]:
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
                    hint = (tr.output.get("hint") or "").strip()
                    parts.append(
                        hint
                        or (
                            "No solid knowledge match for that. Ask about a dataset, a job ID, "
                            "or say **what can you do** for my capabilities."
                        )
                    )

        if insight and not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            if not any(
                tr.name in (
                    "navigate",
                    "introspect_connector_schema",
                    "sample_connector_object",
                    "run_query",
                    "analyze_result",
                    "filter_result",
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
            # Mixed success+failure: keep connector/clarification errors visible.
            for tr in failed:
                err = (tr.error or "").strip()
                if not err:
                    continue
                low = err.lower()
                if (
                    err.startswith("Which ")
                    or "did you mean" in low
                    or "no connector matched" in low
                    or "which connector" in low
                    or "connector not found" in low
                    or ("dataset" in low and "not found" in low)
                ):
                    if err not in "\n".join(parts):
                        parts.insert(0, err)
                    break

        if turn.needs_clarification and turn.needs_clarification not in "\n".join(parts):
            parts.insert(0, turn.needs_clarification)

        if not parts:
            parts.append(_unmapped_intent_reply(message, ctx))

        return "\n\n".join(parts)

    def _format_analysis(self, output: dict) -> str:
        name = output.get("dataset", "dataset").replace("sample_", "").replace("_", " ")
        cols = output.get("columns") or []
        rows = int(output.get("row_count") or 0)
        if not cols and rows == 0:
            return (
                f"**{name}** has no columns or rows I can profile yet. "
                "Re-upload the file in **New Transfer**, or name a different indexed dataset."
            )
        lines = [
            f"**{name}** analysis:",
            f"• {len(cols)} columns, {rows:,} rows",
            f"• Quality score: **{output.get('quality_score', 0):.0f}%**",
        ]
        if output.get("pii_columns"):
            lines.append(f"• PII detected: {', '.join(f'`{c}`' for c in output['pii_columns'])}")
        details = output.get("column_details", [])[:6]
        if details:
            lines.append("• Key columns:")
            for c in details:
                pii = " · PII" if c.get("is_pii") else ""
                lines.append(f"  - `{c['name']}` -> {c.get('semantic_type', '?')}{pii}")
        preview = output.get("sample_preview", [])
        if preview:
            lines.append("• Sample row: " + ", ".join(f"{k}={v}" for k, v in list(preview[0].items())[:4]))
        recs = output.get("recommendations") or []
        if recs:
            lines.append("• Suggestions:")
            for r in recs[:4]:
                lines.append(f"  - {r}")
        return "\n".join(lines)

    def _build_system_prompt(self, ctx: dict, data_context: dict | None = None) -> str:
        tool_names = ", ".join(t["name"] for t in TOOL_DEFINITIONS)
        session_bits: list[str] = []
        try:
            from .working_memory import get_working_memory

            sid = self._session_id(data_context)
            if sid:
                memory = get_working_memory()
                focus = memory.get_focus(sid)
                pending = memory.get_pending(sid)
                if focus and (focus.table or focus.connector_name or focus.result_id):
                    session_bits.append(
                        "Session focus: "
                        f"connector={focus.connector_name or '?'}, "
                        f"table={focus.table or '?'}, "
                        f"result_id={focus.result_id or 'none'}."
                    )
                if pending and pending.question:
                    session_bits.append(
                        f"Open clarification: {pending.question} "
                        f"(waiting for {pending.missing})."
                    )
        except Exception:
            pass
        session_block = ("\n".join(session_bits) + "\n") if session_bits else ""
        return f"""{DATA_PILOT_PERSONA}

{self.context_builder.to_system_context(ctx)}
{session_block}
You are Data Pilot for DataFlow only — data knowledge, product capabilities, and in-app actions.
Available tools (internal — never name these in user-facing answers): {tool_names}.
Use tools for any factual claim about jobs, connectors, datasets, schedules, or capabilities.
Never invent IDs or warehouse state. Never mention tool names, APIs, or internal method labels in replies — write in plain product language.
For mutating actions (remediate, run schedule), propose and wait for UI confirm — do not claim they already ran.
Respect session focus and open clarifications above — do not invent a different connector or table.
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
            if tr.name in (
                "sample_connector_object",
                "run_query",
                "filter_result",
                "analyze_result",
                "aggregate_data",
            ) and tr.success:
                o = tr.output or {}
                rid = o.get("result_id")
                if rid:
                    return {
                        "dataset": o.get("table") or o.get("connector_name") or "pilot_result",
                        "columns": len(
                            o.get("columns")
                            or (o.get("analysis") or {}).get("columns")
                            or []
                        ),
                        "rows": (
                            o.get("row_count")
                            or o.get("match_count")
                            or (o.get("analysis") or {}).get("row_count_sampled")
                            or o.get("group_count")
                            or 0
                        ),
                        "pii_count": 0,
                        "quality_score": 0,
                        "last_result_id": rid,
                    }
        return None

    def _follow_ups(self, message: str, turn: PilotTurn) -> list[str]:
        prompts = []
        if turn.pending_actions:
            prompts.append("What happens if I confirm?")
        has_result = any(
            tr.name in ("sample_connector_object", "run_query", "filter_result", "analyze_result") and tr.success
            for tr in turn.tool_results
        )
        if has_result:
            prompts.extend([
                "Analyze that result",
            ])
            # Prefer a concrete column filter if we know columns
            for tr in turn.tool_results:
                if tr.success and (tr.output or {}).get("columns"):
                    col = (tr.output or {})["columns"][0]
                    prompts.append(f"Filter where {col} is not null")
                    break
            else:
                prompts.append("Null rates on that result")
        if not any(tr.name == "analyze_dataset" for tr in turn.tool_results):
            # Only suggest dataset analysis when indexed uploads actually exist.
            try:
                datasets = self.analyst.list_datasets()
            except Exception:
                datasets = []
            if datasets:
                for d in datasets[:4]:
                    name = str(d.get("name") or "")
                    low = name.lower()
                    if "synonym" in low or "industry schema" in low:
                        continue
                    if len(name) >= 20 and all(
                        c in "0123456789abcdef"
                        for c in name.replace("-", "").replace("_", "")[:16]
                    ):
                        continue
                    label = name.replace("sample_", "").replace("_", " ")
                    if label and len(label) < 40:
                        prompts.append(f"Tell me about {label}")
                        break
        prompts.extend([
            "Show my pipelines",
            "Show my transfer jobs",
            "How many rows in airports on Local Postgres?",
            "What can you do?",
        ])
        # Dedupe preserve order
        seen: set[str] = set()
        out: list[str] = []
        for p in prompts:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out[:4]

    def get_suggested_prompts(self) -> list[str]:
        return self._starter_prompts()

    def _starter_prompts(self) -> list[str]:
        datasets = self.analyst.list_datasets()
        prompts = []
        for d in datasets[:4]:
            name = str(d.get("name") or "")
            # Skip RAG/catalog junk that looks like hash ids or synonym dumps.
            low = name.lower()
            if (
                "synonym" in low
                or "industry schema" in low
                or len(name) >= 20 and all(c in "0123456789abcdef" for c in name.replace("-", "").replace("_", "")[:16])
            ):
                continue
            label = name.replace("sample_", "").replace("_", " ")
            if label and len(label) < 40:
                prompts.append(f"Tell me everything about {label}")
            if len(prompts) >= 2:
                break
        prompts.extend([
            "How many rows in airports on Local Postgres?",
            "Count of orders by status on Local Postgres",
            "Show my recent jobs",
            "What can you do?",
        ])
        # Keep a couple of curated domain prompts after the proven ones.
        for extra in (SUGGESTED_PROMPTS or [])[:2]:
            if extra not in prompts and "logistics" not in extra.lower():
                prompts.append(extra)
        return prompts


_pilot: DataPilotAgent | None = None


def get_pilot_agent() -> DataPilotAgent:
    global _pilot
    if _pilot is None:
        _pilot = DataPilotAgent()
    return _pilot
