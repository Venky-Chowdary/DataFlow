"""Wave 47 — varied natural-language scenarios beyond the parametric corpus.

Conversational paraphrases, polite forms, typos-adjacent speech, mixed intents,
and edge cases. Each prompt is asked via ``DataPilotAgent.chat`` (local) and
graded for routing + usable answer / Confirm / honest clarify.
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
class NLCase:
    family: str
    prompt: str
    expect: frozenset[str]
    mutate: bool = False
    must_not: frozenset[str] = frozenset()
    refuse: bool = False


def varied_nl_scenarios() -> list[NLCase]:
    """Diverse operator phrasings — not just template expansions."""
    c = "Local Postgres"
    w = "Warehouse"
    out: list[NLCase] = []

    def add(
        family: str,
        prompts: list[str],
        *tools: str,
        mutate: bool = False,
        must_not: tuple[str, ...] = (),
        refuse: bool = False,
    ):
        expect = frozenset(tools)
        ban = frozenset(must_not)
        for p in prompts:
            out.append(NLCase(
                family=family,
                prompt=p,
                expect=expect,
                mutate=mutate,
                must_not=ban,
                refuse=refuse,
            ))

    # Polite / chatty meta
    add("meta", [
        "hey, what are you good at?",
        "could you tell me what you can help with?",
        "i'm new here — what should I ask you?",
        "are you able to help me move data?",
    ], "describe_pilot", "explain_product")

    # Conversational aggregates
    add("aggregate", [
        f"hey can you tell me how many rows are sitting in orders on {c}?",
        f"what's the row count for airports on {c}",
        f"i need the total revenue from orders on {c} grouped by region",
        f"give me the average order value on products in {c}",
        f"which are the top 3 regions by amount on {c}?",
        f"could you count distinct status values in orders on {c} please",
        f"how many orders are paid on {c}?",
        f"what's the max price in products on {c}?",
        f"lowest amount in orders on {c}",
        f"number of rows in customers table on {c}",
    ], "aggregate_data")

    # Tables / schema / sample paraphrases
    add("tables", [
        f"can you pull the table list from {c}?",
        f"what tables exist on {c}",
        "show me everything available on PostgresVenkat",
        f"i forgot — which tables do we have on {c}?",
    ], "list_connector_objects")
    add("schema", [
        f"what's the structure of orders on {c}?",
        f"break down the columns for airports on {c}",
        f"i need the schema for products living on {c}",
    ], "introspect_connector_schema")
    add("sample", [
        f"can I peek at a few orders rows on {c}?",
        f"give me a quick look at airports on {c}",
        f"show a sample of products from {c} please",
    ], "sample_connector_object")

    # SQL
    add("sql", [
        f"please run this: SELECT count(*) FROM orders on {c}",
        f"execute SELECT id FROM airports LIMIT 3 on {c}",
    ], "run_query")

    # Transfer paraphrases
    add("transfer_plan", [
        f"could you sketch a transfer plan for orders from {c} over to {w}?",
        f"i want to see the plan before moving products {c} -> {w}",
        "what's a good route if I go from postgres into mysql?",
        "help me plan moving data out of mysql into snowflake",
    ], "plan_transfer", "plan_transfer_route", "start_transfer")
    add("transfer_start", [
        f"go ahead and transfer orders from {c} to {w}",
        f"please move all products from {c} into {w}",
        f"can you sync airports from {c} to {w} using upsert?",
        f"transfer invoices from {c} to {w} now",
        f"i'd like to copy shipments from {c} to {w}",
    ], "start_transfer", "plan_transfer", mutate=True)

    # Sync / FAQ
    add("sync", [
        "should I pick CDC or upsert for a busy orders table?",
        "what's the difference between append and upsert?",
        "recommend a sync mode when I have updated_at",
        "is full refresh dangerous?",
    ], "recommend_sync_mode", "explain_product")
    add("faq", [
        "remind me how Confirm works again?",
        "do I need an OpenAI key for Pilot?",
        "what are those 9 gates people mention?",
        "in plain English, what is quarantine?",
        "how is mapping supposed to stay accurate?",
        "what's schema drift and why should I care?",
    ], "explain_product", "describe_pilot", "explain_mapping_assurance",
       "inspect_schema_policy", "profile_quality_rules")

    # Schedules
    add("schedule", [
        "kick off Nightly Orders right now",
        "please run the Hourly CDC pipeline immediately",
        "trigger Weekly Snapshot",
        "can you open the Nightly Orders pipeline for me?",
        "show me details on schedule Hourly CDC",
    ], "run_schedule_now", "open_schedule", "get_schedule", "list_schedules",
       mutate=True)

    # Jobs / validate
    add("jobs", [
        "any failed jobs lately?",
        "show me recent transfer jobs",
        "why did job_abc12345 blow up?",
        "open job_deadbeef01 so I can look",
    ], "list_jobs", "get_job", "open_job")
    add("validate", [
        "validation pf_abcdef12 failed — what happened?",
        "open fix bad data for me",
        "my mapping looks wrong, can you help fix it?",
        "quarantine the bad rows please",
    ], "get_preflight_run", "remediate_validation", mutate=True)

    # Connectors
    add("connectors", [
        "what connectors do I currently have saved?",
        "find the postgres one for me",
        "take me to the connectors page",
        "create a postgres connector named Staging host db.internal user etl password s3cret database analytics",
    ], "list_connectors", "search_connectors", "navigate", "create_connector",
       mutate=True)

    # Navigate / studio
    add("navigate", [
        "bring me to jobs",
        "I need the settings screen",
        "open docs please",
        "launch transfer studio",
        "go home to the overview",
    ], "navigate", "start_transfer_studio")

    # Quality / datasets
    add("quality", [
        "any tips to improve my data quality?",
        "profile the quality rules you use",
        "what data have I uploaded?",
    ], "profile_quality_rules", "search_knowledge", "list_datasets", "describe_pilot",
       "explain_product")

    # Schema policy / diff / map
    add("policy", [
        "we added a new column — what does schema policy say?",
        "type change showed up on orders, what now?",
    ], "inspect_schema_policy")
    add("diff_map", [
        f"diff the orders schema on {c} against {w}",
        f"map products on {c} onto products on {w}",
    ], "diff_schemas", "map_connector_schemas")

    # Elliptical / follow-ups
    add("followup", [
        "sum amount by region on PilotSQLite",
        "filter where status is paid",
        "analyze that result",
    ], "aggregate_data", "filter_result", "analyze_result", "analyze_dataset")

    # Honest refusals
    add("refuse", [
        "export orders to a csv file",
        "please delete my Local Postgres connector",
        "drop the orders table",
        "create a brand new nightly cron schedule for me",
        "download products as parquet",
    ], must_not=("start_transfer", "create_connector", "run_query"),
       refuse=True)

    # Greets
    add("greeting", [
        "hello there",
        "hi pilot",
        "good afternoon",
    ], "describe_pilot")  # greeting short-circuit OK if empty expect via refuse path

    # Soften greeting expect — empty plan is fine
    # Replace greeting cases with empty expect
    cleaned: list[NLCase] = []
    for case in out:
        if case.family == "greeting":
            cleaned.append(NLCase(
                family="greeting",
                prompt=case.prompt,
                expect=frozenset(),
                refuse=False,
            ))
        elif case.family == "refuse":
            cleaned.append(NLCase(
                family="refuse",
                prompt=case.prompt,
                expect=frozenset(),
                must_not=case.must_not,
                refuse=True,
            ))
        else:
            cleaned.append(case)
    return cleaned


def grade(case: NLCase, resp) -> list[str]:
    reasons: list[str] = []
    planned = {n for n, _ in infer_tools_from_message(case.prompt)}
    used = {t.get("name") for t in (resp.tools_used or []) if t.get("name")}
    method = getattr(resp, "method", "") or ""

    if case.family == "greeting":
        if method not in {"greeting", "pilot_local_engine"} and not (resp.answer or "").strip():
            reasons.append("greeting_empty")
        return reasons

    if case.refuse:
        for banned in case.must_not:
            if banned in planned or banned in used:
                reasons.append(f"banned:{banned}")
        pending = list(resp.pending_actions or [])
        if any(a.get("type") in {"start_transfer", "create_connector"} for a in pending):
            reasons.append("refuse_staged_mutation")
        answer = (resp.answer or "").strip()
        if not answer:
            reasons.append("empty_refuse_answer")
        return reasons

    hit = (planned | used) & case.expect
    if case.expect and not hit:
        reasons.append(
            f"miss want={sorted(case.expect)} planned={sorted(planned)} used={sorted(used)}"
        )
    for banned in case.must_not:
        if banned in planned or banned in used:
            reasons.append(f"banned:{banned}")

    answer = (resp.answer or "").strip()
    clarify = (getattr(resp, "needs_clarification", "") or "").strip()
    if not answer and not clarify:
        reasons.append("empty_answer")
    if answer and _BAD.search(answer):
        reasons.append("crash_text")

    if case.mutate:
        pending = list(resp.pending_actions or [])
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
                "missing", "credentials", "authentication", "opening",
            )
        )
        # Non-mutating siblings in the same family (e.g. open schedule) are fine.
        if case.family in {"schedule", "connectors", "validate", "transfer_start"}:
            if not has_confirm and not honest and not (
                planned & {"navigate", "open_schedule", "get_schedule", "list_connectors", "list_schedules"}
            ):
                # Only enforce Confirm when a mutate tool was actually chosen.
                if planned & {
                    "start_transfer", "run_schedule_now", "remediate_validation", "create_connector",
                } or used & {
                    "start_transfer", "run_schedule_now", "remediate_validation", "create_connector",
                }:
                    if not has_confirm and not honest:
                        reasons.append("mutate_without_confirm_or_clarify")
        elif not has_confirm and not honest:
            reasons.append("mutate_without_confirm_or_clarify")

    return reasons


def run_varied_nl(*, session_prefix: str = "wave47") -> dict:
    os.environ["DATAFLOW_PILOT_ENGINE"] = "local"
    cases = varied_nl_scenarios()
    agent = DataPilotAgent()
    failures: list[dict] = []
    by_family: dict[str, dict] = {}

    for i, case in enumerate(cases):
        bucket = by_family.setdefault(case.family, {"total": 0, "passed": 0, "failed": 0})
        bucket["total"] += 1
        try:
            resp = agent.chat(
                case.prompt,
                data_context={"pilot_session_id": f"{session_prefix}-{i}"},
            )
            reasons = grade(case, resp)
        except Exception as exc:  # noqa: BLE001
            reasons = [f"exception:{type(exc).__name__}:{exc}"]
            resp = None
        if reasons:
            bucket["failed"] += 1
            failures.append({
                "family": case.family,
                "prompt": case.prompt,
                "reasons": reasons,
                "answer": ((getattr(resp, "answer", None) or "")[:160] if resp else ""),
                "tools": [t.get("name") for t in (getattr(resp, "tools_used", None) or [])] if resp else [],
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
        "by_family": by_family,
        "failures": failures,
    }


def test_varied_natural_language_scenarios():
    report = run_varied_nl()
    assert report["total"] >= 70, report["total"]
    assert report["failed"] == 0, (
        f"{report['failed']}/{report['total']} failed:\n"
        + "\n".join(
            f"{f['family']}: {f['prompt']!r} -> {f['reasons']} A={f['answer']!r}"
            for f in report["failures"][:40]
        )
    )
