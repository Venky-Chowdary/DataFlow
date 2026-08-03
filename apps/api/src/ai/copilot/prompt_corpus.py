"""Enterprise Data Pilot prompt corpus — parametric real-world NL prompts.

Generates ≥1000 prompts with expected primary tools for regression. Cases are
deterministic expansions of enterprise phrasing (query / analyze / suggest /
transfer / fix / navigate / refuse), not mocked LLM responses.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class PromptCase:
    prompt: str
    expected_tools: frozenset[str]
    """At least one of these tools must appear in the local NL plan."""
    family: str
    must_not: frozenset[str] = frozenset()
    """Tools that must not be selected for this prompt."""


_TABLES = (
    "orders", "products", "customers", "employees", "invoices",
    "payments", "shipments", "airports", "events", "sessions",
)
_CONNECTORS = (
    "Local Postgres", "Warehouse", "Analytics MySQL", "Prod Mongo",
    "Snowflake Prod", "BigQuery Mart",
)
_SCREENS = (
    ("dashboard", ("overview", "home", "dashboard")),
    ("transfer", ("transfer studio", "transfer")),
    ("jobs", ("jobs", "job history")),
    ("connectors", ("connectors", "connections")),
    ("schedules", ("pipelines", "schedules")),
    ("contracts", ("contracts", "data contracts")),
    ("query", ("query", "query playground", "sql playground")),
    ("docs", ("docs", "documentation")),
    ("benchmarks", ("proofs", "benchmarks")),
    ("settings", ("settings",)),
    ("pilot", ("data pilot", "pilot")),
)
_METRICS = ("count", "sum", "average", "avg", "min", "max", "distinct count")
_GROUP_COLS = ("status", "region", "country", "category", "channel", "tier")
_FILTER_COLS = ("amount", "price", "qty", "total", "revenue")
_SYNC_MODES = ("upsert", "append", "cdc", "full refresh")
_JOB_IDS = (
    "job_abc12345", "job_deadbeef01", "aaaaaaaaaaaaaaaaaaaaaaaa",
    "bbbbbbbbbbbbbbbbbbbbbbbb", "job_wave27_demo",
)
_PF_IDS = ("pf_abcdef12", "pf_deadbeef", "pf_cafebabe99")


def _case(
    prompt: str,
    *tools: str,
    family: str,
    must_not: Iterable[str] = (),
) -> PromptCase:
    return PromptCase(
        prompt=prompt,
        expected_tools=frozenset(tools),
        family=family,
        must_not=frozenset(must_not),
    )


def iter_prompt_corpus() -> list[PromptCase]:
    """Build the full enterprise corpus (≥1000 cases)."""
    cases: list[PromptCase] = []

    # --- Meta / capability ---
    for p in (
        "what can you do?",
        "who are you?",
        "help me with data pilot",
        "what tools do you have?",
        "describe yourself",
        "how do you work?",
    ):
        cases.append(_case(p, "describe_pilot", family="meta"))

    # --- Navigation (screen change — not "show my X" content lists) ---
    for screen, labels in _SCREENS:
        for label in labels:
            for verb in ("go to", "open", "take me to", "navigate to"):
                cases.append(
                    _case(f"{verb} {label}", "navigate", family="navigate")
                )

    # --- List / operate ---
    for p in (
        "show my jobs", "list jobs", "recent transfers", "job history",
        "show my pipelines", "list schedules", "list pipelines", "my schedules",
        "show my connectors", "list connectors", "what connectors do I have",
        "list datasets", "what data do I have", "all datasets", "available data",
        "list contracts", "show data contracts",
    ):
        tool = "list_jobs"
        if "pipeline" in p or "schedule" in p:
            tool = "list_schedules"
        elif "connector" in p:
            tool = "list_connectors"
        elif "dataset" in p or "data do" in p or "available data" in p or "all datasets" in p:
            tool = "list_datasets"
        elif "contract" in p:
            tool = "list_contracts"
        cases.append(_case(p, tool, family="operate"))

    # --- Aggregates ---
    for table in _TABLES:
        for conn in _CONNECTORS:
            cases.append(_case(
                f"how many rows in {table} on {conn}?",
                "aggregate_data",
                family="aggregate",
            ))
            cases.append(_case(
                f"count of {table} on {conn}",
                "aggregate_data",
                family="aggregate",
            ))
            for metric in _METRICS:
                if metric == "count":
                    continue
                col = "amount" if metric in ("sum", "average", "avg", "min", "max") else "status"
                cases.append(_case(
                    f"{metric} of {col} in {table} on {conn}",
                    "aggregate_data",
                    family="aggregate",
                ))
            for g in _GROUP_COLS[:3]:
                cases.append(_case(
                    f"count of {table} by {g} on {conn}",
                    "aggregate_data",
                    family="aggregate",
                ))
            for fcol in _FILTER_COLS[:2]:
                cases.append(_case(
                    f"count of {table} on {conn} where {fcol} > 100",
                    "aggregate_data",
                    family="aggregate",
                ))

    # --- Sample / schema / query ---
    for table in _TABLES:
        for conn in _CONNECTORS[:4]:
            cases.append(_case(
                f"sample {table} on {conn}",
                "sample_connector_object",
                family="sample",
            ))
            cases.append(_case(
                f"preview {table} on {conn}",
                "sample_connector_object",
                family="sample",
            ))
            cases.append(_case(
                f"what columns are on {table} in {conn}?",
                "introspect_connector_schema",
                family="schema",
            ))
            cases.append(_case(
                f"describe table {table} on {conn}",
                "introspect_connector_schema",
                family="schema",
            ))
            cases.append(_case(
                f"schema of {table} on {conn}",
                "introspect_connector_schema",
                family="schema",
            ))
            cases.append(_case(
                f"list tables on {conn}",
                "list_connector_objects",
                family="schema",
            ))
            cases.append(_case(
                f"run sql: select * from {table} limit 10 on {conn}",
                "run_query",
                family="query",
            ))

    # --- Transfer plan / start ---
    for table in _TABLES[:8]:
        for src in _CONNECTORS[:3]:
            for dst in _CONNECTORS[3:6]:
                if src == dst:
                    continue
                cases.append(_case(
                    f"plan transfer of {table} from {src} to {dst}",
                    "plan_transfer", "plan_transfer_route",
                    family="transfer_plan",
                ))
                for mode in _SYNC_MODES[:3]:
                    cases.append(_case(
                        f"transfer {table} from {src} to {dst} as {mode}",
                        "start_transfer", "plan_transfer",
                        family="transfer_start",
                    ))

    # --- Sync mode / capabilities / mapping assurance ---
    for p in (
        "what sync mode should I use for CDC?",
        "recommend sync mode for incremental with updated_at",
        "what any to any capabilities do you support?",
        "how does mapping assurance work?",
        "explain mapping guarantee",
        "what about schema drift new column?",
        "schema policy for type change",
    ):
        tools = ("recommend_sync_mode",)
        if "capabilities" in p or "any to any" in p:
            tools = ("get_transfer_capabilities",)
        elif "mapping" in p:
            tools = ("explain_mapping_assurance",)
        elif "schema" in p:
            tools = ("inspect_schema_policy",)
        cases.append(_case(p, *tools, family="govern"))

    # --- Jobs / preflight / remediate ---
    for jid in _JOB_IDS:
        cases.append(_case(f"why did job {jid} fail?", "get_job", family="jobs"))
        cases.append(_case(f"open job {jid}", "open_job", family="jobs"))
        cases.append(_case(f"show job {jid}", "get_job", "open_job", family="jobs"))
    for pf in _PF_IDS:
        cases.append(_case(
            f"why did validation {pf} fail?",
            "get_preflight_run",
            family="preflight",
        ))
        cases.append(_case(
            f"show preflight run {pf}",
            "get_preflight_run",
            family="preflight",
        ))
    for p in (
        "fix bad data",
        "open fix bad data",
        "quarantine bad rows",
        "strip control characters",
        "normalize control chars",
    ):
        cases.append(_case(p, "remediate_validation", family="remediate"))

    # --- Platform inventory (never warehouse table scans) ---
    for p in (
        "how many jobs failed",
        "how many jobs do I have",
        "failed jobs",
        "failed transfers",
        "job failures",
        "how many transfers failed",
        "show my jobs",
        "list jobs",
        "recent transfers",
    ):
        cases.append(_case(
            p,
            "list_jobs",
            family="inventory",
            must_not=("aggregate_data",),
        ))
    for p in (
        "how many connectors do I have",
        "how many connectors",
        "connector count",
        "show my connectors",
        "list connectors",
    ):
        cases.append(_case(
            p,
            "list_connectors",
            family="inventory",
            must_not=("aggregate_data",),
        ))

    # --- Elliptical aggregates (table omitted; connector named) ---
    for metric, col, dim in (
        ("sum", "amount", "region"),
        ("average", "price", "status"),
        ("total", "revenue", "month"),
        ("sum", "qty", "category"),
    ):
        for conn in _CONNECTORS[:3]:
            cases.append(_case(
                f"{metric} {col} by {dim} on {conn}",
                "aggregate_data",
                family="aggregate_elliptical",
            ))
            cases.append(_case(
                f"top 5 {dim}s by {col} on {conn}",
                "aggregate_data",
                family="aggregate_elliptical",
            ))

    # --- Multi-turn / validate triage phrases ---
    # Standalone "only paid ones" needs working memory — covered by wave31 live tests.
    for p in (
        "filter where status is paid",
        "where region = east",
        "analyze that",
        "analyze this result",
        "why did validate fail",
        "why validation failed",
        "validate fail",
    ):
        if "validate" in p or "validation" in p:
            cases.append(_case(
                p,
                "list_jobs", "get_preflight_run", "remediate_validation",
                family="validate_triage",
            ))
        elif "analyze" in p:
            cases.append(_case(
                p,
                "analyze_result", "analyze_dataset", "aggregate_data",
                family="result_followup",
            ))
        else:
            cases.append(_case(
                p,
                "filter_result", "aggregate_data",
                family="result_followup",
            ))

    # --- Wave 33 NL accuracy expansions ---
    for p, tools in (
        ("distinct count of status from orders on Local Postgres", ("aggregate_data",)),
        ("how many paid orders on Local Postgres", ("aggregate_data",)),
        ("orders where amount > 10 on Local Postgres", ("aggregate_data",)),
        ("create a postgres connector at localhost database demo", ("create_connector",)),
        ("create a mysql connector at db.example.com", ("create_connector",)),
        ("explain the 9 preflight gates", ("describe_pilot", "profile_quality_rules")),
        ("is email PII in orders", ("introspect_connector_schema", "analyze_dataset")),
        ("help me fix my mapping", ("explain_mapping_assurance", "navigate")),
        ("max price in products on Warehouse", ("aggregate_data",)),
    ):
        cases.append(_case(p, *tools, family="nl_accuracy"))

    # --- Quality / suggestions / knowledge ---
    for p in (
        "data quality on my uploads",
        "profile quality rules",
        "suggest improvements for my data",
        "how can I improve data quality?",
        "recommend fixes for bad data quality",
    ):
        cases.append(_case(
            p,
            "profile_quality_rules", "search_knowledge", "describe_pilot",
            family="quality",
        ))
    for p in (
        "what quality gates do you have?",
        "what are the quality gates?",
        "list quality gates",
        "what are the preflight gates?",
    ):
        cases.append(_case(
            p,
            "describe_pilot", "search_knowledge", "profile_quality_rules",
            family="quality_gates",
        ))
    for p in (
        "what quality gates do you have?",
        "what are the quality gates?",
        "list quality gates",
        "what are the preflight gates?",
    ):
        cases.append(_case(
            p,
            "describe_pilot", "search_knowledge", "profile_quality_rules",
            family="quality_gates",
        ))

    # --- Schedules run ---
    for name in ("Nightly Orders", "Hourly CDC", "Weekly Snapshot", "Test"):
        cases.append(_case(
            f"run schedule {name} now",
            "run_schedule_now",
            family="schedule",
        ))
        cases.append(_case(
            f"trigger pipeline {name} now",
            "run_schedule_now",
            family="schedule",
        ))

    # --- Diff / map schemas ---
    for table in _TABLES[:8]:
        cases.append(_case(
            f"diff schema {table} on Local Postgres vs {table} on Warehouse",
            "diff_schemas",
            family="schema_diff",
        ))
        cases.append(_case(
            f"map {table} on Local Postgres to {table} on Warehouse",
            "map_connector_schemas",
            family="schema_map",
        ))

    # --- Honest refusals (must not invent start_transfer / delete tools) ---
    for p in (
        "export orders to csv",
        "download products as parquet",
        "delete the Local Postgres connector",
        "drop table orders",
        "create a new nightly schedule for orders",
        "build a cron pipeline every hour",
    ):
        cases.append(_case(
            p,
            family="refuse",
            must_not=("start_transfer", "create_connector", "search_knowledge", "run_query"),
        ))

    # --- Greeting (handled by agent short-circuit; local plan may be empty) ---
    for p in ("hello", "hi", "hey there", "good morning"):
        cases.append(_case(p, family="greeting"))

    # Deduplicate by prompt text while preserving order
    seen: set[str] = set()
    unique: list[PromptCase] = []
    for c in cases:
        key = c.prompt.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(c)
    return unique


def corpus_stats() -> dict:
    cases = iter_prompt_corpus()
    by_family: dict[str, int] = {}
    for c in cases:
        by_family[c.family] = by_family.get(c.family, 0) + 1
    return {"total": len(cases), "by_family": by_family}
