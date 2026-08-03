"""Natural-language coverage for every Pilot operation (all 39 tools).

Each operation gets several operator-style phrasings. We score:
  1) routing hits an allowed tool for that op
  2) chat() returns a usable answer (or honest clarify / Confirm)
  3) mutate ops never silently execute without Confirm/clarify
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.pilot_agent import DataPilotAgent
from src.ai.copilot.tools import infer_tools_from_message

_BAD = re.compile(
    r"(traceback \(most recent|internal server error|noneType|"
    r"object has no attribute|\bexception:\s)",
    re.I,
)


@dataclass(frozen=True)
class OpPrompt:
    op: str
    prompt: str
    expect: frozenset[str]
    mutate: bool = False


def all_operation_prompts() -> list[OpPrompt]:
    """Natural-language prompts covering every registered Pilot tool."""
    c = "Local Postgres"
    cases: list[OpPrompt] = []

    def add(op: str, prompts: list[str], *tools: str, mutate: bool = False):
        expect = frozenset(tools)
        for p in prompts:
            cases.append(OpPrompt(op=op, prompt=p, expect=expect, mutate=mutate))

    # --- discover ---
    add("list_datasets", [
        "what data do I have?",
        "list my datasets",
        "show available data",
    ], "list_datasets")
    add("search_data", [
        "search for orders in my uploads",
        "find customers in datasets",
    ], "search_data", "list_datasets", "aggregate_data")
    add("search_connectors", [
        "find my postgres connector",
        "search connectors for warehouse",
    ], "search_connectors", "list_connectors")
    add("search_knowledge", [
        "what is a semantic type in DataFlow?",
    ], "search_knowledge", "explain_product", "describe_pilot")
    add("describe_pilot", [
        "what can you do?",
        "who are you?",
        "help me with Data Pilot",
    ], "describe_pilot")

    # --- profile / live data ---
    add("analyze_dataset", [
        "analyze my HR upload",
        "tell me about the employees dataset",
    ], "analyze_dataset", "list_datasets")
    add("compare_datasets", [
        "compare orders and products",
    ], "compare_datasets")
    add("profile_quality_rules", [
        "suggest improvements for my data",
        "what quality rules do you check?",
    ], "profile_quality_rules", "search_knowledge", "explain_product", "describe_pilot")
    add("list_connector_objects", [
        f"list tables on {c}",
        f"can you get tables from {c}?",
        f"show tables on {c}",
        "tables on PostgresVenkat",
    ], "list_connector_objects")
    add("introspect_connector_schema", [
        f"what columns are on orders in {c}?",
        f"schema of airports on {c}",
        f"describe table products on {c}",
    ], "introspect_connector_schema")
    add("sample_connector_object", [
        f"sample orders on {c}",
        f"preview airports on {c}",
        f"show me some data from products on {c}",
    ], "sample_connector_object")
    add("aggregate_data", [
        f"how many rows in orders on {c}?",
        f"count of airports on {c}",
        f"sum amount by region from orders on {c}",
        f"average price in products on {c}",
        f"top 5 customers by revenue from orders on {c}",
        f"distinct statuses in orders on {c}",
        f"how many paid orders on {c}",
    ], "aggregate_data")
    add("run_query", [
        f"run sql: select * from orders limit 5 on {c}",
        f"SELECT id, status FROM orders LIMIT 10 on {c}",
    ], "run_query")
    add("analyze_result", [
        "analyze that",
        "analyze this result",
    ], "analyze_result", "analyze_dataset", "aggregate_data", "list_datasets")
    add("filter_result", [
        "filter where status is paid",
        "where region = east",
    ], "filter_result", "aggregate_data")
    add("diff_schemas", [
        f"diff schema orders on {c} vs orders on Warehouse",
        f"compare schema of products on {c} with products on Warehouse",
    ], "diff_schemas")
    add("map_connector_schemas", [
        f"map orders on {c} to orders on Warehouse",
        f"map columns from products on {c} to products on Warehouse",
    ], "map_connector_schemas")

    # --- move ---
    add("plan_transfer_route", [
        "plan a route from postgres to mysql",
        "move data from postgres to snowflake",
        "cdc from mysql to warehouse",
    ], "plan_transfer_route", "plan_transfer", "recommend_sync_mode")
    add("plan_transfer", [
        f"plan transfer of orders from {c} to Warehouse",
        f"plan transfer of products from {c} to Analytics MySQL",
    ], "plan_transfer", "plan_transfer_route", "start_transfer")
    add("start_transfer", [
        f"transfer orders from {c} to Warehouse",
        f"please transfer all products from {c} to Warehouse",
        f"can you move airports from {c} to Warehouse?",
        f"transfer orders from {c} to Warehouse as upsert",
    ], "start_transfer", "plan_transfer", mutate=True)
    add("get_transfer_capabilities", [
        "what any to any capabilities do you support?",
        "what can transfer do?",
    ], "get_transfer_capabilities", "describe_pilot")
    add("recommend_sync_mode", [
        "what sync mode should I use for CDC?",
        "recommend sync mode for incremental with updated_at",
        "which sync mode is best for upsert?",
    ], "recommend_sync_mode")

    # --- govern ---
    add("explain_mapping_assurance", [
        "how does mapping assurance work?",
        "explain mapping guarantee",
        "how do you get correct columns?",
    ], "explain_mapping_assurance", "explain_product")
    add("inspect_schema_policy", [
        "schema drift on orders",
        "what about schema drift new column?",
        "schema policy for type change",
    ], "inspect_schema_policy")

    # --- operate ---
    add("list_jobs", [
        "show my jobs",
        "list jobs",
        "recent transfers",
        "how many jobs failed",
    ], "list_jobs")
    add("get_job", [
        "why did job job_abc12345 fail?",
        "show job job_deadbeef01",
    ], "get_job", "open_job")
    add("open_job", [
        "open job job_abc12345",
    ], "open_job", "get_job", "navigate")
    add("navigate", [
        "go to connectors",
        "open transfer studio",
        "take me to jobs",
        "navigate to settings",
        "open pipelines",
    ], "navigate", "start_transfer_studio")
    add("get_preflight_run", [
        "why did validation pf_abcdef12 fail?",
        "show preflight run pf_deadbeef",
    ], "get_preflight_run")
    add("remediate_validation", [
        "fix bad data",
        "fix my mapping",
        "quarantine bad rows",
        "help me fix my mapping",
    ], "remediate_validation", mutate=True)
    add("list_schedules", [
        "show my pipelines",
        "list schedules",
        "my schedules",
    ], "list_schedules")
    add("get_schedule", [
        "show schedule Nightly Orders",
        "open pipeline Hourly CDC",
    ], "get_schedule", "open_schedule", "list_schedules", "run_schedule_now")
    add("run_schedule_now", [
        "run Nightly Orders now",
        "run schedule Hourly CDC now",
        "trigger pipeline Weekly Snapshot now",
        "run my nightly pipeline",
    ], "run_schedule_now", mutate=True)
    add("list_contracts", [
        "list contracts",
        "show data contracts",
    ], "list_contracts")
    add("open_schedule", [
        "open schedule Nightly Orders",
    ], "open_schedule", "get_schedule", "navigate", "list_schedules")
    add("start_transfer_studio", [
        "start transfer studio",
        "open the transfer studio",
    ], "start_transfer_studio", "navigate")
    add("create_connector", [
        "create a postgres connector named Demo PG host localhost user demo password secret database appdb",
        "create a mysql connector at db.example.com database shop user root password secret",
        "add a mongodb connector mongodb://localhost:27017/app",
    ], "create_connector", mutate=True)
    add("list_connectors", [
        "show my connectors",
        "list connectors",
        "how many connectors do I have",
    ], "list_connectors")

    # --- product FAQ / explain ---
    add("explain_product", [
        "what is upsert",
        "what is CDC",
        "what is DataFlow",
        "explain preflight",
        "can I use Pilot without OpenAI",
        "how does Confirm work for transfers",
        "what is append mode",
        "what quality gates do you have?",
        "explain the 9 preflight gates",
    ], "explain_product", "describe_pilot", "profile_quality_rules")

    return cases


def grade_op(case: OpPrompt, resp) -> list[str]:
    reasons: list[str] = []
    planned = {n for n, _ in infer_tools_from_message(case.prompt)}
    used = {t.get("name") for t in (resp.tools_used or []) if t.get("name")}
    hit = (planned | used) & case.expect
    if not hit:
        reasons.append(f"miss_tools want={sorted(case.expect)} planned={sorted(planned)} used={sorted(used)}")

    answer = (resp.answer or "").strip()
    clarify = (getattr(resp, "needs_clarification", "") or "").strip()
    if not answer and not clarify:
        reasons.append("empty_answer")
    if answer and _BAD.search(answer):
        reasons.append("crash_text")

    pending = list(resp.pending_actions or [])
    if case.mutate:
        has_confirm = any(
            a.get("risk") == "mutate"
            or a.get("type") in {
                "start_transfer", "run_schedule", "studio", "create_connector",
            }
            for a in pending
        )
        honest = bool(clarify) or any(
            w in answer.lower()
            for w in (
                "confirm", "which", "not found", "no connector", "no schedule",
                "pipeline", "name a saved", "could not", "couldn't", "need a",
                "missing", "credentials", "fix host", "authentication",
            )
        )
        if not has_confirm and not honest:
            reasons.append("mutate_without_confirm_or_clarify")
    return reasons


def run_all_operations(*, session_prefix: str = "ops-nl") -> dict:
    os.environ["DATAFLOW_PILOT_ENGINE"] = "local"
    cases = all_operation_prompts()
    agent = DataPilotAgent()
    failures: list[dict] = []
    by_op: dict[str, dict] = {}

    for i, case in enumerate(cases):
        bucket = by_op.setdefault(case.op, {"total": 0, "passed": 0, "failed": 0})
        bucket["total"] += 1
        try:
            resp = agent.chat(
                case.prompt,
                data_context={"pilot_session_id": f"{session_prefix}-{i}"},
            )
            reasons = grade_op(case, resp)
        except Exception as exc:  # noqa: BLE001
            reasons = [f"exception:{type(exc).__name__}:{exc}"]
            resp = None

        if reasons:
            bucket["failed"] += 1
            failures.append({
                "op": case.op,
                "prompt": case.prompt,
                "reasons": reasons,
                "answer": ((getattr(resp, "answer", None) or "")[:160] if resp else ""),
                "tools": [t.get("name") for t in (getattr(resp, "tools_used", None) or [])] if resp else [],
                "pending": [a.get("type") for a in (getattr(resp, "pending_actions", None) or [])] if resp else [],
            })
        else:
            bucket["passed"] += 1

    total = len(cases)
    failed = len(failures)
    return {
        "total": total,
        "passed": total - failed,
        "failed": failed,
        "pass_rate": round((total - failed) / max(total, 1), 4),
        "ops_covered": len(by_op),
        "by_op": by_op,
        "failures": failures,
    }


def test_all_operations_natural_language():
    report = run_all_operations()
    assert report["ops_covered"] >= 35, report["ops_covered"]
    assert report["total"] >= 80, report["total"]
    assert report["failed"] == 0, (
        f"{report['failed']}/{report['total']} failed:\n"
        + "\n".join(
            f"{f['op']}: {f['prompt']!r} -> {f['reasons']} A={f['answer']!r}"
            for f in report["failures"][:40]
        )
    )
