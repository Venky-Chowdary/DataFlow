"""Datawrap Pilot — app tools the agent can invoke (like Cursor/Claude tool use)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from ..rag.product_docs import (
    compose_documented_answer,
    names_product_subject,
    product_doc_search,
)
from .data_analyst import get_data_analyst
from .tool_permissions import current_caller_role, denial_message, is_tool_allowed
from .transfer_rules import parse_transfer_data_rules
from .unsupported_question import is_answerable_subject, unsupported_question_output


@dataclass
class ToolResult:
    name: str
    success: bool
    output: Any
    error: str = ""


@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: dict
    handler: Callable[..., ToolResult]


TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "list_datasets",
        "description": "List every dataset available — uploads, fixtures, and transfer history.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "analyze_dataset",
        "description": "Deep analysis of a dataset: columns, types, PII, quality, samples.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_name": {"type": "string", "description": "Dataset name or hint (hr, logistics, payments)"},
            },
            "required": ["dataset_name"],
        },
    },
    {
        "name": "search_data",
        "description": "Search across all datasets for columns or values matching a query.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term — column name, value, or concept"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_connectors",
        "description": "List saved database/warehouse connectors.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "create_connector",
        "description": (
            "Create a saved connector from credentials the user provided "
            "(MySQL, PostgreSQL, MongoDB, etc.). Always confirm before saving. "
            "Accepts a connection URL and/or host, port, database, username, password."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "type": {
                    "type": "string",
                    "description": "Driver type: mysql, postgresql, mongodb, snowflake, …",
                },
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "database": {"type": "string"},
                "username": {"type": "string"},
                "password": {"type": "string"},
                "connection_string": {"type": "string"},
                "ssl": {"type": "boolean"},
                "schema": {"type": "string"},
                "message": {
                    "type": "string",
                    "description": "Original user message (for credential extraction)",
                },
                "test_first": {"type": "boolean", "default": True},
            },
            "required": [],
        },
    },
    {
        "name": "list_jobs",
        "description": "List recent transfer jobs with status, IDs, and record counts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max jobs to return", "default": 10},
            },
            "required": [],
        },
    },
    {
        "name": "get_job",
        "description": (
            "Fetch a transfer job by exact ID (24-char hex ObjectId or job_id string). "
            "Use when the user pastes a job ID or asks why a specific transfer failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Transfer job ID"},
            },
            "required": ["job_id"],
        },
    },
    {
        "name": "get_transfer_capabilities",
        "description": "Show supported source→destination transfer combinations.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "navigate",
        "description": (
            "Navigate the user to an app screen: dashboard, pilot, transfer, connectors, jobs, "
            "schedules (pipelines), contracts, query, mcp, settings, docs, benchmarks (proofs)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "screen": {
                    "type": "string",
                    "enum": [
                        "dashboard",
                        "pilot",
                        "transfer",
                        "connectors",
                        "jobs",
                        "schedules",
                        "contracts",
                        "query",
                        "mcp",
                        "settings",
                        "docs",
                        "benchmarks",
                    ],
                },
            },
            "required": ["screen"],
        },
    },
    {
        "name": "get_preflight_run",
        "description": (
            "Look up a validation/preflight run by ID (pf_…) — blockers, gates, remediations. "
            "Use when the user pastes a run ID or asks why Validate failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Preflight run ID, e.g. pf_a1b2c3d4e5f6"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "remediate_validation",
        "description": (
            "Propose a Studio remediation for the active Validate step: strip control characters, "
            "open Fix bad data, quarantine posture, or review mappings. Returns an action the UI applies."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        "normalize_control_chars",
                        "open_bad_data_fix",
                        "quarantine_and_rerun",
                        "review_mappings",
                        "rerun_preflight",
                    ],
                },
                "run_id": {"type": "string"},
            },
            "required": ["kind"],
        },
    },
    {
        "name": "compare_datasets",
        "description": "Compare schemas of two datasets side by side.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_a": {"type": "string"},
                "dataset_b": {"type": "string"},
            },
            "required": ["dataset_a", "dataset_b"],
        },
    },
    {
        "name": "search_connectors",
        "description": "Search the connector catalog (tiles ≠ transfer-ready — prefer list_connectors for saved live connections).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "role": {"type": "string", "enum": ["source", "destination", "all"]},
            },
            "required": [],
        },
    },
    {
        "name": "search_knowledge",
        "description": "Search trained Datawrap Pilot knowledge base — connectors, transfers, PII, mappings, product help.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language question"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "plan_transfer_route",
        "description": (
            "Plan an any-to-any transfer route with sync mode, schema policy, validation gates, "
            "and risk controls. When source, destination, and table resolve to saved connectors, "
            "this delegates to plan_transfer and must forward contract_id, require_signed_contract, "
            "validation_mode, and schema_policy — never invent a contract, skip_preflight, or "
            "propagate_all. A generic sketch is not a plan for the operator's data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Source system, connector, file type, or table"},
                "destination": {"type": "string", "description": "Destination system, warehouse, database, file type, or table"},
                "workload": {"type": "string", "description": "full_load, incremental, cdc, file_export, or unknown"},
                "table": {"type": "string", "description": "Source table — required for a real plan"},
                "dest_table": {"type": "string", "description": "Destination table (defaults to the source name)"},
                "sync_mode": {"type": "string"},
                "leftover_nl": {
                    "type": "string",
                    "description": "Remaining operator prose (contract / migrate / data rules). Never parse skip_preflight.",
                },
                "validation_mode": {"type": "string", "enum": ["strict", "balanced", "lenient"]},
                "schema_policy": {
                    "type": "string",
                    "enum": ["manual_review", "type_locked", "pause_on_change"],
                    "description": "Spoken schema posture only — never invent propagate_all",
                },
                "contract_id": {"type": "string", "description": "Data contract to preview on the plan (read-only)"},
                "require_signed_contract": {"type": "boolean"},
            },
            "required": [],
        },
    },
    {
        "name": "plan_transfer",
        "description": (
            "Plan a real transfer between two saved connectors: introspect both schemas live "
            "(or peek a CALL/SELECT result — never treat a procedure stream name as a table), "
            "map columns, list type conversions and lossy casts, and run the 9 preflight gates. "
            "CDC/SCD2/mirror are refused for procedure/query sources. "
            "Read-only — it never moves data. Use whenever the operator asks what a transfer "
            "would do, or before starting one. When a contract_id is supplied, the plan "
            "names the bind and breaker (Confirm still fail-closed on SIGNED / OPEN)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_name": {"type": "string", "description": "Saved source connector"},
                "source_table": {"type": "string", "description": "Source table, collection, or a CALL/SELECT statement"},
                "source_read_mode": {"type": "string", "description": "table, query, or procedure"},
                "procedure_call": {"type": "string", "description": "CALL/EXEC text when source_read_mode is procedure"},
                "source_query": {"type": "string", "description": "Read-only SELECT when source_read_mode is query"},
                "procedure_params": {"type": "object", "description": "Bound :name parameters for CALL/SELECT"},
                "dest_connector_name": {"type": "string", "description": "Saved destination connector"},
                "dest_table": {"type": "string", "description": "Destination table (defaults to the source name)"},
                "sync_mode": {
                    "type": "string",
                    "description": (
                        "full_refresh_append, full_refresh_overwrite, incremental_append, "
                        "incremental_upsert, or cdc_incremental"
                    ),
                },
                "validation_mode": {"type": "string", "enum": ["strict", "balanced", "lenient"]},
                "schema_policy": {
                    "type": "string",
                    "enum": ["manual_review", "type_locked", "pause_on_change"],
                    "description": "Spoken schema posture only — never invent propagate_all",
                },
                "contract_id": {"type": "string", "description": "Data contract to preview on the plan (read-only)"},
                "require_signed_contract": {"type": "boolean"},
            },
            "required": [],
        },
    },
    {
        "name": "start_transfer",
        "description": (
            "Stage a transfer between two saved connectors for the operator to Confirm. "
            "Runs the plan and preflight first and refuses when any gate blocks. "
            "This never moves data on its own — execution happens only after Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_name": {"type": "string"},
                "source_table": {"type": "string"},
                "source_read_mode": {"type": "string"},
                "procedure_call": {"type": "string"},
                "source_query": {"type": "string"},
                "procedure_params": {"type": "object"},
                "dest_connector_name": {"type": "string"},
                "dest_table": {"type": "string"},
                "sync_mode": {"type": "string"},
                "limit": {"type": "integer", "description": "Cap rows moved (0 = all)"},
                "validation_mode": {"type": "string", "enum": ["strict", "balanced", "lenient"]},
                "schema_policy": {
                    "type": "string",
                    "enum": ["manual_review", "type_locked", "pause_on_change"],
                    "description": "Spoken schema posture only — never invent propagate_all",
                },
                "contract_id": {"type": "string", "description": "Signed data contract to enforce on Confirm"},
                "require_signed_contract": {"type": "boolean"},
            },
            "required": [],
        },
    },
    {
        "name": "explain_mapping_assurance",
        "description": "Explain the schema mapping algorithms, confidence scoring, review rules, and guarantees.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "recommend_sync_mode",
        "description": "Recommend full refresh, incremental, append, dedupe, or CDC based on workload requirements.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workload": {"type": "string"},
                "has_cursor": {"type": "boolean"},
                "has_primary_key": {"type": "boolean"},
                "needs_history": {"type": "boolean"},
                "source_read_mode": {"type": "string", "description": "table, query, or procedure"},
            },
            "required": [],
        },
    },
    {
        "name": "inspect_schema_policy",
        "description": "Inspect schema drift policy for added, removed, renamed, or type-changed fields and streams.",
        "input_schema": {
            "type": "object",
            "properties": {
                "change_type": {
                    "type": "string",
                    "enum": ["new_column", "removed_column", "new_stream", "removed_stream", "type_change", "cursor_removed", "primary_key_removed", "unknown"],
                },
                "auto_apply": {"type": "boolean"},
            },
            "required": [],
        },
    },
    {
        "name": "profile_quality_rules",
        "description": "Generate quality, PII, type, nullability, uniqueness, and reconciliation checks for a dataset.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_name": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "describe_pilot",
        "description": "Explain what Datawrap Pilot knows and can do locally — capabilities, not raw RAG dumps.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "explain_product",
        "description": (
            "Answer product how-to questions about Datawrap (transfer, mapping, preflight, "
            "connectors, PII, troubleshooting) from curated local knowledge — not RAG dumps."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "list_schedules",
        "description": (
            "List pipeline schedules (Pipelines page) with cadence, next run, last status, "
            "and bound contract / breaker when one is set."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 20},
            },
            "required": [],
        },
    },
    {
        "name": "get_schedule",
        "description": (
            "Fetch one pipeline schedule by id or name, including route, sync mode, "
            "and bound contract / breaker when one is set."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "name": {"type": "string", "description": "Schedule display name if id unknown"},
            },
            "required": [],
        },
    },
    {
        "name": "run_schedule_now",
        "description": (
            "Propose an immediate run of a pipeline schedule. Returns a pending action — "
            "the UI must confirm before the run starts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "create_schedule",
        "description": (
            "Stage a recurring pipeline (schedule) between two saved connectors for the "
            "operator to Confirm. Grounds the route against live schemas, requires "
            "preflight to clear, and stores the approved mapping on the schedule. "
            "Cadence is the operator's own wording — “nightly at 2am in Asia/Kolkata”, "
            "“every 15 minutes”, “weekly on Monday”, or a 5-field cron. This creates "
            "nothing on its own: the schedule exists only after Confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_name": {"type": "string"},
                "source_table": {"type": "string"},
                "dest_connector_name": {"type": "string"},
                "dest_table": {"type": "string"},
                "cadence": {
                    "type": "string",
                    "description": "Cadence wording, with time/timezone when stated",
                },
                "sync_mode": {"type": "string"},
                "cursor_column": {
                    "type": "string",
                    "description": "Watermark column — required for incremental modes",
                },
                "name": {"type": "string", "description": "Schedule display name"},
                "validation_mode": {"type": "string", "enum": ["strict", "balanced", "lenient"]},
                "schema_policy": {
                    "type": "string",
                    "enum": ["manual_review", "type_locked", "pause_on_change"],
                },
                "contract_id": {"type": "string"},
                "require_signed_contract": {"type": "boolean"},
            },
            "required": ["cadence"],
        },
    },
    {
        "name": "list_contracts",
        "description": "List data contracts available in the workspace.",
        "input_schema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 50}},
            "required": [],
        },
    },
    {
        "name": "open_job",
        "description": "Open the Jobs screen focused on a specific job id (safe navigate + highlight).",
        "input_schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string"}},
            "required": ["job_id"],
        },
    },
    {
        "name": "open_schedule",
        "description": "Open Pipelines focused on a schedule id or name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "start_transfer_studio",
        "description": "Open Transfer Studio to start or continue a transfer (safe navigate).",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_connector_objects",
        "description": (
            "List live tables/collections on a saved database connector "
            "(Postgres, MySQL, Mongo, Snowflake, …). Requires connector id or name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_id": {"type": "string"},
                "connector_name": {"type": "string"},
                "limit": {"type": "integer", "default": 100},
            },
            "required": [],
        },
    },
    {
        "name": "sample_connector_object",
        "description": (
            "Sample live rows from a table/collection on a saved connector "
            "(read-only). Use for “show me data from airports on Local Postgres” "
            "or “analyze the orders table”. Returns preview rows + light column profile "
            "+ durable result_id for follow-ups."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_id": {"type": "string"},
                "connector_name": {"type": "string"},
                "table": {"type": "string", "description": "Table or collection name (schema.table ok)"},
                "limit": {"type": "integer", "default": 25},
                "analyze": {"type": "boolean", "default": True},
                "session_id": {"type": "string"},
            },
            "required": ["table"],
        },
    },
    {
        "name": "run_query",
        "description": (
            "Run a read-only SQL SELECT (or Mongo JSON filter) against a saved connector. "
            "Destructive SQL is rejected. Returns capped preview rows + durable result_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_id": {"type": "string"},
                "connector_name": {"type": "string"},
                "query": {"type": "string", "description": "SELECT … or Mongo filter JSON"},
                "collection": {"type": "string", "description": "Mongo collection when needed"},
                "limit": {"type": "integer", "default": 100},
                "analyze": {"type": "boolean", "default": False},
                "session_id": {"type": "string"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "aggregate_data",
        "description": (
            "Answer an analytics question with an exact server-side aggregate: row "
            "counts, SUM/AVG/MIN/MAX, COUNT(DISTINCT), and GROUP BY / top-N "
            "breakdowns. Use this for “how many orders”, “count by status”, "
            "“average price”, “revenue by month”, “top 5 regions by revenue”. "
            "Columns are validated against the live schema, so prefer this over "
            "hand-written SQL. Totals are exact — never sampled or estimated."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_id": {"type": "string"},
                "connector_name": {"type": "string"},
                "table": {"type": "string", "description": "Table or view to aggregate"},
                "metric": {
                    "type": "string",
                    "enum": ["count", "count_distinct", "sum", "avg", "min", "max"],
                    "default": "count",
                },
                "column": {
                    "type": "string",
                    "description": "Measure column; required for every metric except count",
                },
                "group_by": {
                    "type": "string",
                    "description": (
                        "Dimension column, or a time grain (day/week/month/quarter/year) "
                        "to bucket the table's date column"
                    ),
                },
                "order": {"type": "string", "enum": ["desc", "asc"], "default": "desc"},
                "limit": {"type": "integer", "default": 20},
                "session_id": {"type": "string"},
                "where": {
                    "type": "string",
                    "description": (
                        "Filter phrase in plain language: \"status = open\", "
                        "\"amount > 100\", \"email is null\", \"in 2024\", "
                        "\"last 30 days\". Columns are validated and values are "
                        "bound as typed parameters — never inlined into SQL."
                    ),
                },
            },
            "required": ["table"],
        },
    },
    {
        "name": "analyze_result",
        "description": (
            "Profile a previously sampled/queried result by result_id (or the latest "
            "result in this Pilot session). Use for follow-ups like “analyze that”, "
            "“null rates”, “top values of email” — does not re-query the warehouse."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "result_id": {"type": "string"},
                "session_id": {"type": "string"},
                "column": {"type": "string", "description": "Optional single-column focus"},
            },
            "required": [],
        },
    },
    {
        "name": "filter_result",
        "description": (
            "Filter rows in a stored Pilot result (eq/ne/contains/gt/lt/is_null/…). "
            "Use for “show rows where email is null” or “filter status = active”."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "result_id": {"type": "string"},
                "session_id": {"type": "string"},
                "column": {"type": "string"},
                "op": {"type": "string", "default": "eq"},
                "value": {"type": "string"},
                "limit": {"type": "integer", "default": 25},
            },
            "required": ["column"],
        },
    },
    {
        "name": "introspect_connector_schema",
        "description": (
            "Live-introspect columns and types for a table/collection on a saved connector. "
            "Use for questions like “schema of airports on Local Postgres”."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_id": {"type": "string"},
                "connector_name": {"type": "string"},
                "table": {"type": "string", "description": "Table or collection name"},
            },
            "required": ["table"],
        },
    },
    {
        "name": "diff_schemas",
        "description": (
            "Diff two live connector schemas (source table vs dest table) using "
            "classify_schema_change — additive vs breaking."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_id": {"type": "string"},
                "source_connector_name": {"type": "string"},
                "source_table": {"type": "string"},
                "dest_connector_id": {"type": "string"},
                "dest_connector_name": {"type": "string"},
                "dest_table": {"type": "string"},
            },
            "required": ["source_table"],
        },
    },
    {
        "name": "map_connector_schemas",
        "description": (
            "Live-introspect source and destination tables on saved connectors, then run "
            "Datawrap's semantic column mapper (same engine as Transfer Studio Map step)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_id": {"type": "string"},
                "source_connector_name": {"type": "string"},
                "source_table": {"type": "string"},
                "dest_connector_id": {"type": "string"},
                "dest_connector_name": {"type": "string"},
                "dest_table": {"type": "string"},
                "threshold": {"type": "number", "default": 0.85},
            },
            "required": ["source_table"],
        },
    },
    {
        "name": "brief_workspace",
        "description": (
            "Spoken sitrep of this workspace: connectors (tested/failed), recent "
            "jobs, pipelines due or parked on approval, contracts. Use when the "
            "operator asks what's going on, for a briefing, or to summarize the workspace."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
]

TOOL_FAMILIES: list[dict] = [
    {
        "id": "discover",
        "label": "Discover",
        "tools": [
            "list_datasets",
            "search_data",
            "search_connectors",
            "search_knowledge",
            "describe_pilot",
            "brief_workspace",
        ],
    },
    {
        "id": "profile",
        "label": "Profile",
        "tools": [
            "analyze_dataset",
            "compare_datasets",
            "profile_quality_rules",
            "list_connector_objects",
            "introspect_connector_schema",
            "sample_connector_object",
            "aggregate_data",
            "run_query",
            "analyze_result",
            "filter_result",
            "diff_schemas",
            "map_connector_schemas",
        ],
    },
    {
        "id": "move",
        "label": "Move",
        "tools": [
            "plan_transfer_route",
            "plan_transfer",
            "start_transfer",
            "get_transfer_capabilities",
            "recommend_sync_mode",
        ],
    },
    {
        "id": "govern",
        "label": "Govern",
        "tools": ["explain_mapping_assurance", "inspect_schema_policy"],
    },
    {
        "id": "operate",
        "label": "Operate",
        "tools": [
            "list_jobs",
            "get_job",
            "navigate",
            "get_preflight_run",
            "remediate_validation",
            "list_schedules",
            "get_schedule",
            "run_schedule_now",
            "create_schedule",
            "list_contracts",
            "open_job",
            "open_schedule",
            "start_transfer_studio",
            "create_connector",
            "list_connectors",
        ],
    },
]


def get_tool_registry() -> dict:
    """Honest tool registry — counts only real TOOL_DEFINITIONS (no marketing inflation)."""
    tool_names = {t["name"] for t in TOOL_DEFINITIONS}
    families = []
    for family in TOOL_FAMILIES:
        available = [name for name in family["tools"] if name in tool_names]
        families.append({
            **{k: v for k, v in family.items() if k != "generated_actions"},
            "tools": available,
            "tool_count": len(available),
            "generated_actions": 0,
        })
    return {
        "tool_count": len(TOOL_DEFINITIONS),
        # Kept for API compat; always 0 — never invent phantom "routable actions".
        "generated_action_count": 0,
        "total_routable_actions": len(TOOL_DEFINITIONS),
        "families": families,
        "tools": [
            {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["input_schema"].get("properties", {}),
            }
            for tool in TOOL_DEFINITIONS
        ],
    }


class DataPilotTools:
    """Execute Datawrap Pilot tools against live app state."""

    def __init__(self):
        self.analyst = get_data_analyst()

    def execute(self, name: str, arguments: dict | None = None) -> ToolResult:
        args = arguments or {}
        handlers = {
            "list_datasets": self._list_datasets,
            "analyze_dataset": self._analyze_dataset,
            "search_data": self._search_data,
            "list_connectors": self._list_connectors,
            "create_connector": self._create_connector,
            "list_jobs": self._list_jobs,
            "get_job": self._get_job,
            "get_transfer_capabilities": self._get_capabilities,
            "navigate": self._navigate,
            "get_preflight_run": self._get_preflight_run,
            "remediate_validation": self._remediate_validation,
            "compare_datasets": self._compare_datasets,
            "search_connectors": self._search_connectors,
            "search_knowledge": self._search_knowledge,
            "describe_pilot": self._describe_pilot,
            "explain_product": self._explain_product,
            "plan_transfer_route": self._plan_transfer_route,
            "plan_transfer": self._plan_transfer,
            "start_transfer": self._start_transfer,
            "explain_mapping_assurance": self._explain_mapping_assurance,
            "recommend_sync_mode": self._recommend_sync_mode,
            "inspect_schema_policy": self._inspect_schema_policy,
            "profile_quality_rules": self._profile_quality_rules,
            "list_schedules": self._list_schedules,
            "get_schedule": self._get_schedule,
            "run_schedule_now": self._run_schedule_now,
            "create_schedule": self._create_schedule,
            "list_contracts": self._list_contracts,
            "open_job": self._open_job,
            "open_schedule": self._open_schedule,
            "start_transfer_studio": self._start_transfer_studio,
            "list_connector_objects": self._list_connector_objects,
            "sample_connector_object": self._sample_connector_object,
            "run_query": self._run_query,
            "aggregate_data": self._aggregate_data,
            "analyze_result": self._analyze_result,
            "filter_result": self._filter_result,
            "introspect_connector_schema": self._introspect_connector_schema,
            "diff_schemas": self._diff_schemas,
            "map_connector_schemas": self._map_connector_schemas,
            "brief_workspace": self._brief_workspace,
        }
        handler = handlers.get(name)
        if not handler:
            return ToolResult(name=name, success=False, output=None, error=f"Unknown tool: {name}")
        # One chokepoint for permissions: every path into a tool — the
        # deterministic planner, an LLM tool loop, a recovery follow-up — comes
        # through here, so none of them can reach a tool the caller's role does
        # not hold.
        role = current_caller_role()
        if not is_tool_allowed(role, name):
            return ToolResult(
                name=name,
                success=False,
                output=None,
                error=denial_message(role, name),
            )
        try:
            return handler(**args)
        except TypeError as e:
            return ToolResult(name=name, success=False, output=None, error=str(e))
        except Exception as e:
            return ToolResult(name=name, success=False, output=None, error=str(e))

    def _list_datasets(self) -> ToolResult:
        datasets = self.analyst.list_datasets()
        return ToolResult(name="list_datasets", success=True, output={"datasets": datasets, "count": len(datasets)})

    def _analyze_dataset(self, dataset_name: str = "") -> ToolResult:
        schema = self.analyst.resolve_dataset(dataset_name)
        if not schema:
            return ToolResult(
                name="analyze_dataset", success=False, output=None,
                error=f"Dataset '{dataset_name}' not found",
            )
        insight = self.analyst.analyze_schema(schema)
        return ToolResult(name="analyze_dataset", success=True, output={
            "dataset": insight.dataset_name,
            "columns": insight.columns,
            "row_count": insight.row_count,
            "quality_score": insight.quality_score,
            "pii_columns": insight.pii_columns,
            "column_details": insight.column_details,
            "sample_preview": insight.sample_preview,
            "recommendations": insight.recommendations,
        })

    def _search_data(self, query: str = "") -> ToolResult:
        q = query.lower().strip()
        hits: list[dict] = []
        for ds in self.analyst.list_datasets():
            name = ds["name"].lower()
            if q in name:
                hits.append({"dataset": ds["name"], "match": "name", "detail": ds})
                continue
            for col in ds.get("columns", []):
                if q in col.lower():
                    hits.append({"dataset": ds["name"], "match": "column", "column": col})
            schema = self.analyst.resolve_dataset(ds["name"])
            if schema and schema.samples:
                for col, vals in schema.samples.items():
                    for v in vals[:20]:
                        if q in str(v).lower():
                            hits.append({"dataset": ds["name"], "match": "value", "column": col, "sample": v})
                            break
        return ToolResult(name="search_data", success=True, output={"query": query, "hits": hits[:25]})

    def _list_connectors(self) -> ToolResult:
        summary = []
        errors: list[str] = []
        try:
            from services.connector_store import list_connectors as store_list

            for c in store_list():
                d = c.to_dict() if hasattr(c, "to_dict") else dict(c.__dict__)
                summary.append({
                    "id": str(d.get("id") or d.get("_id") or ""),
                    "name": d.get("name"),
                    "type": d.get("type") or d.get("format"),
                    "host": d.get("host"),
                    "database": d.get("database"),
                    "status": d.get("status", "saved"),
                })
        except Exception as exc:
            logging.getLogger(__name__).warning("connector_store list failed: %s", exc, exc_info=exc)
            errors.append(f"connector_store: {exc}")
        if not summary:
            try:
                from ...services.mongodb_service import get_mongodb_service

                mongo = get_mongodb_service()
                for c in mongo.list_connectors():
                    summary.append({
                        "id": c.get("id", c.get("_id", "")),
                        "name": c.get("name"),
                        "type": c.get("type"),
                        "host": c.get("host"),
                        "database": c.get("database"),
                        "status": c.get("status", "unknown"),
                    })
            except Exception as exc:
                logging.getLogger(__name__).warning("mongo list_connectors failed: %s", exc, exc_info=exc)
                errors.append(f"mongodb: {exc}")
        # Empty workspace with healthy stores is success; broken stores must not
        # greenwash as "you have zero connectors".
        if not summary and errors:
            return ToolResult(
                name="list_connectors",
                success=False,
                output={"connectors": [], "count": 0, "errors": errors},
                error=(
                    "Could not load saved connectors ("
                    + "; ".join(errors[:2])
                    + "). Check Settings → storage, then retry."
                ),
            )
        return ToolResult(
            name="list_connectors",
            success=True,
            output={"connectors": summary, "count": len(summary)},
        )

    def _create_connector(  # nosec B107
        self,
        name: str = "",
        type: str = "",
        host: str = "",
        port: int = 0,
        database: str = "",
        username: str = "",
        password: str = "",
        connection_string: str = "",
        ssl: bool = False,
        schema: str = "",
        message: str = "",
        test_first: bool = True,
    ) -> ToolResult:
        from .connector_create import build_connector_draft, draft_is_complete

        draft = build_connector_draft(
            message or "",
            {
                "name": name,
                "type": type,
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
                "schema": schema,
            },
        )
        ok, missing = draft_is_complete(draft)
        if not ok:
            from .ack_ledger import redact_payload

            return ToolResult(
                name="create_connector",
                success=False,
                output=redact_payload(draft),
                error=missing,
            )

        probe_msg = ""
        if test_first:
            try:
                from src.transfer.connector_registry import run_probe

                probe_ok, probe_msg = run_probe(
                    draft["type"],
                    {
                        "host": draft.get("host") or "",
                        "port": int(draft.get("port") or 0),
                        "database": draft.get("database") or "",
                        "username": draft.get("username") or "",
                        "password": draft.get("password") or "",
                        "schema": draft.get("schema") or "",
                        "connection_string": draft.get("connection_string") or "",
                        "ssl": bool(draft.get("ssl")),
                        "type": draft["type"],
                        "auth_mode": draft.get("auth_mode") or "",
                        "warehouse": draft.get("warehouse") or "",
                        "account": draft.get("account") or "",
                    },
                )
                if not probe_ok:
                    from .ack_ledger import redact_payload

                    return ToolResult(
                        name="create_connector",
                        success=False,
                        output=redact_payload(draft),
                        error=(
                            f"Could not connect with those credentials: {probe_msg}. "
                            "Fix host/port/user/password (use the public proxy if this is Railway), then ask again."
                        ),
                    )
            except Exception as exc:
                from .ack_ledger import redact_payload

                return ToolResult(
                    name="create_connector",
                    success=False,
                    output=redact_payload(draft),
                    error=f"Connection test failed: {exc}",
                )
        safe_preview = {
            "name": draft["name"],
            "type": draft["type"],
            "host": draft.get("host") or "(from URL)",
            "port": draft.get("port"),
            "database": draft.get("database") or "",
            "username": draft.get("username") or "",
            "ssl": bool(draft.get("ssl")),
            "auth_mode": draft.get("auth_mode") or "",
            "schema": draft.get("schema") or "",
            "has_password": bool(draft.get("password") or draft.get("connection_string")),
            "test": probe_msg or "skipped",
        }
        from .ack_ledger import get_ack_ledger

        ack_id = get_ack_ledger().put(
            kind="create_connector",
            payload=draft,
            preview=safe_preview,
        )
        return ToolResult(
            name="create_connector",
            success=True,
            output={
                "action": "create_connector",
                "label": f"Save connector “{draft['name']}” ({draft['type']})",
                "risk": "mutate",
                "requires_confirm": True,
                "ack_id": ack_id,
                # Secrets stay on the server ledger — client only gets preview + ack_id.
                "preview": safe_preview,
            },
        )

    def _list_jobs(self, limit: int = 10) -> ToolResult:
        from ...services.mongodb_service import get_mongodb_service
        mongo = get_mongodb_service()
        jobs = mongo.list_jobs(limit=limit)
        summary = [
            {
                "id": str(j.get("_id", j.get("id", ""))),
                "source": j.get("source_name", j.get("source_type", "")),
                "destination": j.get("destination_collection") or j.get("destination_type", ""),
                "status": j.get("status"),
                "records": j.get("records_processed", 0),
                "rejected_rows": j.get("rejected_rows", 0),
                "error": (j.get("error") or "")[:240] or None,
                "created_at": str(j.get("created_at", "")),
            }
            for j in jobs
        ]
        # "How many jobs?" must be answered from the whole history — the page we
        # read here is only the window we can show.
        counts = mongo.count_jobs()
        return ToolResult(
            name="list_jobs",
            success=True,
            output={
                "jobs": summary,
                "count": len(summary),
                "total": int(counts.get("total") or 0),
                "status_counts": counts.get("by_status") or {},
            },
        )

    def _get_job(self, job_id: str = "") -> ToolResult:
        from services.quarantine_from_preflight import merge_job_quarantine

        from ...services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job((job_id or "").strip())
        if not job:
            return ToolResult(
                name="get_job",
                success=False,
                output=None,
                error=f"Job '{job_id}' not found. Ask the user for the job ID shown on Jobs / Job Theater.",
            )
        quarantine = merge_job_quarantine(job)
        row_ids = {d.get("row") for d in quarantine if d.get("row") is not None}
        quarantine_row_count = len(row_ids) if row_ids else len(quarantine)
        samples = [
            {
                "row": d.get("row"),
                "column": d.get("column"),
                "value": str(d.get("value") or "")[:120],
                "reason": str(d.get("reason") or "")[:200],
            }
            for d in quarantine[:8]
        ]
        remediations: list[dict[str, str]] = []
        status = str(job.get("status") or "").lower()
        if status in {"failed", "cancelled"} or job.get("rejected_rows") or quarantine:
            remediations.append({"kind": "open_bad_data_fix", "label": "Fix bad data…"})
            remediations.append({"kind": "rerun_preflight", "label": "Re-run Validate in Transfer Studio"})
            remediations.append({"kind": "review_mappings", "label": "Review column mappings"})
        req = job.get("transfer_request") or {}
        source_ep = req.get("source") or {}
        dest_ep = req.get("destination") or {}
        route = {
            "source_connector_id": source_ep.get("connector_id"),
            "source_type": source_ep.get("format") or source_ep.get("type") or job.get("source_type"),
            "source_table": source_ep.get("table") or source_ep.get("collection") or job.get("source_name"),
            "dest_connector_id": dest_ep.get("connector_id"),
            "dest_type": dest_ep.get("format") or dest_ep.get("type") or job.get("destination_type"),
            "dest_table": dest_ep.get("table") or dest_ep.get("collection") or job.get("destination_collection"),
            "mappings_count": len(req.get("mappings") or []),
            "sync_mode": req.get("sync_mode") or job.get("operation"),
        }
        live_schema: dict | None = None
        # When job failed / quarantine, attach a live source schema snapshot if possible.
        if (
            route.get("source_connector_id")
            and route.get("source_table")
            and (status in {"failed", "cancelled"} or quarantine or job.get("rejected_rows"))
        ):
            try:
                from .schema_tools import introspect_connector_schema

                sch = introspect_connector_schema(
                    connector_id=str(route["source_connector_id"]),
                    table=str(route["source_table"]),
                )
                if sch.success and sch.output:
                    live_schema = {
                        "connector_name": sch.output.get("connector_name"),
                        "table": sch.output.get("table"),
                        "column_count": sch.output.get("column_count"),
                        "columns": [
                            {"name": c.get("name"), "inferred_type": c.get("inferred_type")}
                            for c in (sch.output.get("columns") or [])[:40]
                        ],
                    }
            except Exception:
                live_schema = None
        return ToolResult(
            name="get_job",
            success=True,
            output={
                "id": str(job.get("_id", job.get("id", job_id))),
                "status": job.get("status"),
                "source": job.get("source_name") or job.get("source_type"),
                "destination": job.get("destination_collection") or job.get("destination_database") or job.get("destination_type"),
                "source_type": job.get("source_type"),
                "destination_type": job.get("destination_type"),
                "records_processed": job.get("records_processed", 0),
                "rejected_rows": int(job.get("rejected_rows") or 0) or quarantine_row_count,
                "coerced_null_rows": job.get("coerced_null_rows", 0),
                "quarantine_issue_count": len(quarantine),
                "quarantine_row_count": quarantine_row_count,
                "quarantine_samples": samples,
                "progress_pct": job.get("progress_pct"),
                "error": job.get("error"),
                "created_at": str(job.get("created_at", "")),
                "completed_at": str(job.get("completed_at", "")),
                "sync_mode": route.get("sync_mode"),
                "route": route,
                "live_source_schema": live_schema,
                "suggested_remediations": remediations,
            },
        )

    def _get_capabilities(self) -> ToolResult:
        from ...transfer.registry import get_capabilities
        return ToolResult(name="get_transfer_capabilities", success=True, output=get_capabilities())

    def _navigate(self, screen: str = "dashboard") -> ToolResult:
        # Alias product labels → Screen ids
        aliases = {
            "overview": "dashboard",
            "home": "dashboard",
            "pipelines": "schedules",
            "pipeline": "schedules",
            "proofs": "benchmarks",
            "help": "docs",
            "playground": "query",
        }
        screen = aliases.get((screen or "").strip().lower(), (screen or "").strip().lower())
        valid = {
            "dashboard",
            "pilot",
            "transfer",
            "connectors",
            "jobs",
            "schedules",
            "contracts",
            "query",
            "mcp",
            "settings",
            "docs",
            "benchmarks",
        }
        if screen not in valid:
            return ToolResult(name="navigate", success=False, output=None, error=f"Invalid screen: {screen}")
        return ToolResult(
            name="navigate",
            success=True,
            output={"screen": screen, "action": "navigate", "risk": "safe"},
        )

    def _get_preflight_run(self, run_id: str = "") -> ToolResult:
        from services.preflight_run_store import get_preflight_run

        record = get_preflight_run(run_id)
        if not record:
            return ToolResult(
                name="get_preflight_run",
                success=False,
                output=None,
                error=f"Preflight run '{run_id}' not found. Ask the user for the pf_… ID shown on Validate.",
            )
        return ToolResult(name="get_preflight_run", success=True, output=record)

    def _remediate_validation(self, kind: str = "", run_id: str = "") -> ToolResult:
        allowed = {
            "normalize_control_chars",
            "open_bad_data_fix",
            "quarantine_and_rerun",
            "review_mappings",
            "rerun_preflight",
        }
        if kind not in allowed:
            return ToolResult(
                name="remediate_validation",
                success=False,
                output=None,
                error=f"Unknown remediation kind '{kind}'. Use one of: {', '.join(sorted(allowed))}",
            )
        labels = {
            "normalize_control_chars": "Normalize control characters…",
            "open_bad_data_fix": "Fix bad data…",
            "quarantine_and_rerun": "Quarantine bad rows and re-run…",
            "review_mappings": "Open Map step to review mappings",
            "rerun_preflight": "Re-run Validate",
        }
        return ToolResult(
            name="remediate_validation",
            success=True,
            output={
                "action": "studio",
                "kind": kind,
                "label": labels[kind],
                "run_id": run_id or None,
                "risk": "mutate",
                "requires_confirm": True,
            },
        )

    def _compare_datasets(self, dataset_a: str = "", dataset_b: str = "") -> ToolResult:
        sa = self.analyst.resolve_dataset(dataset_a)
        sb = self.analyst.resolve_dataset(dataset_b)
        if not sa or not sb:
            missing = []
            if not sa:
                missing.append(dataset_a)
            if not sb:
                missing.append(dataset_b)
            return ToolResult(name="compare_datasets", success=False, output=None, error=f"Not found: {', '.join(missing)}")
        cols_a, cols_b = set(sa.columns), set(sb.columns)
        return ToolResult(name="compare_datasets", success=True, output={
            "dataset_a": sa.name,
            "dataset_b": sb.name,
            "shared_columns": sorted(cols_a & cols_b),
            "only_in_a": sorted(cols_a - cols_b),
            "only_in_b": sorted(cols_b - cols_a),
            "column_count_a": len(cols_a),
            "column_count_b": len(cols_b),
        })

    def _search_connectors(self, query: str = "", role: str = "all") -> ToolResult:
        from ...services.catalog_service import search_catalog
        result = search_catalog(query, role, limit=20)
        return ToolResult(name="search_connectors", success=True, output=result)

    def _describe_pilot(self) -> ToolResult:
        """Local capability card — never dump raw semantic-type training shards."""
        datasets = []
        connectors = []
        try:
            ds = self._list_datasets()
            if ds.success:
                datasets = (ds.output or {}).get("datasets", [])[:8]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        try:
            lc = self._list_connectors()
            if lc.success:
                connectors = (lc.output or {}).get("connectors", [])[:8]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        from .example_phrases import example_connector_name, example_dest_connector_name

        ex_src = next(
            (str(c.get("name") or "").strip() for c in connectors if c.get("name")),
            "",
        ) or example_connector_name()
        ex_dst = example_dest_connector_name(source_hint=ex_src)
        return ToolResult(
            name="describe_pilot",
            success=True,
            output={
                "role": "Datawrap Pilot",
                "runtime": "local_engine",
                "runtime_note": (
                    "Primary brain is Datawrap's local Pilot engine "
                    "(NL → tools → compose). OpenAI / Anthropic / Ollama are optional "
                    "add-ons only — not required."
                ),
                "can": [
                    "Answer analytics questions with exact aggregates "
                    "(count / sum / avg / min / max / distinct / group by / top-N)",
                    "Plan source→destination routes and sync modes",
                    "Stage a transfer (map + 9 preflight gates) and start it after you Confirm",
                    "Inspect schema risk, mappings, and validation failures",
                    "Triage jobs by ID (validation runs or job IDs)",
                    "Search your uploaded datasets for columns, PII, and quality",
                    "List tables and describe schemas on saved connectors",
                    "Sample and analyze live table data (read-only)",
                    "Run read-only SQL / Mongo queries on saved connectors",
                    "Follow up on the last sample/query (analyze / filter stored results)",
                    "Create a saved connector from a URL or host/user/password (server ack + Confirm)",
                    "Compare source vs destination schemas and map columns",
                    "List and run pipeline schedules (with confirmation)",
                    "Open Fix bad data / quarantine paths in Transfer Studio (Confirm required)",
                    "Open any app screen (Transfer, Jobs, Pipelines, Contracts, Query, …)",
                    "Brief the live workspace (connectors, jobs, parked pipelines, contracts)",
                ],
                "cannot_yet": [
                    "Export a table to a downloadable file from chat "
                    "(sample the table or use Query for larger pulls)",
                    "Create a brand-new schedule/pipeline definition from chat "
                    "(I can list and run existing ones)",
                    "Rewrite quarantine rows in place from chat "
                    "(I open Transfer Studio Fix with your Confirm)",
                    "Delete connectors, jobs, or data",
                ],
                "tools": [t["name"] for t in TOOL_DEFINITIONS],
                "screens": [
                    "dashboard", "pilot", "transfer", "connectors", "jobs",
                    "schedules", "contracts", "query", "mcp", "settings", "docs", "benchmarks",
                ],
                "does_not": [
                    "Invent warehouse facts without checking your workspace",
                    "Dump raw training data as chat answers",
                    "Run changing actions without your Confirm",
                ],
                "datasets": [
                    {"name": d.get("name"), "columns": d.get("column_count"), "rows": d.get("row_count")}
                    for d in datasets
                ],
                "connectors": [
                    {"name": c.get("name"), "type": c.get("type")}
                    for c in connectors
                ],
                "ask_examples": [
                    f"How many rows in airports on {ex_src}?",
                    f"Count of orders by status on {ex_src} where amount > 100",
                    f"Average price in products on {ex_src}",
                    "and by region?",
                    f"Plan a transfer of orders from {ex_src} to {ex_dst}",
                    f"Transfer orders from {ex_src} to {ex_dst} as upsert",
                    "Why did job <id> fail?",
                    "Show my pipelines",
                    "Give me a workspace briefing",
                ],
                "remembers": [
                    "Last connector, table, metric, and grouping in this chat",
                    "Clarification answers (which connector / table / column)",
                ],
                "transfers": [
                    "Plans use the same mapping pipeline and 9 gates as Transfer Studio",
                    "Nothing moves until you Confirm — overwrite is never the default",
                ],
            },
        )

    def _brief_workspace(self, workspace_id: str = "") -> ToolResult:
        """Live sitrep — counts only from the same stores Jobs / Connectors use."""
        from .workspace_briefing import collect_workspace_briefing

        facts = collect_workspace_briefing(workspace_id=workspace_id or "")
        attention = [str(a) for a in (facts.get("attention") or []) if a]
        return ToolResult(
            name="brief_workspace",
            success=True,
            output={
                "facts": facts,
                "connector_count": facts.get("connector_count", 0),
                "job_count": facts.get("job_count", 0),
                "schedule_count": facts.get("schedule_count", 0),
                "contract_count": facts.get("contract_count", 0),
                "attention": attention,
                "empty_workspace": bool(facts.get("empty_workspace")),
            },
        )

    def _explain_product(self, query: str = "") -> ToolResult:
        """Curated Datawrap product answers — independent of cloud LLMs and RAG noise."""
        from ..knowledge.copilot_knowledge import PRODUCT_CAPABILITIES

        lower = (query or "").lower().strip()

        # Direct high-value FAQ snippets (beat generic intent templates).
        direct: list[tuple[re.Pattern[str], str, str]] = [
            (
                re.compile(r"\bappend(?:\s+mode)?\b"),
                "sync_mode",
                (
                    "**Append** adds source rows onto the destination without replacing existing ones. "
                    "Use it for insert-only feeds. Prefer **upsert** when you have a primary key and "
                    "need updates; prefer **CDC** for continuous change streams; "
                    "**full refresh overwrite** replaces the destination (Confirm required)."
                ),
            ),
            (
                re.compile(r"\bupsert\b"),
                "sync_mode",
                (
                    "**Upsert** inserts new rows and updates existing ones matched by primary key. "
                    "Needs a reliable key. If you only ever insert, use append; for log-based "
                    "continuous sync, use CDC."
                ),
            ),
            (
                re.compile(
                    r"\b(?:schema\s+types?|logical\s+types?|type\s+system|"
                    r"mapping\s+types?|data\s+types?\s+(?:in\s+)?(?:map|mapping|validate))\b"
                ),
                "type_system",
                (
                    "**Schema / logical types** in Datawrap are the Map→Validate contract: "
                    "source carriers (INTEGER, DECIMAL(p,s), TIMESTAMPTZ, UUID, OBJECTID, INET, "
                    "VECTOR/HALFVEC, …) stamp a destination type before write. Create-new stamps "
                    "**physical** DDL (e.g. MySQL `CHAR(36)` for UUID) with Accept-risk chips when "
                    "domain is not enforced. G3 schema contract fails closed on lossy casts; "
                    "Approve/Accept risk on Map unlocks Validate — confidence alone never invents Ready. "
                    "Ask \"list tables on your connector\" or open Transfer Studio → Map for a live schema."
                ),
            ),
            (
                re.compile(r"\bcdc\b"),
                "sync_mode",
                (
                    "**CDC** (change data capture) streams inserts/updates/deletes from the source "
                    "log when the engine supports it. Ask me \"what sync mode should I use for CDC?\" "
                    "for a workload-specific recommendation."
                ),
            ),
            (
                re.compile(r"\bfull refresh\b"),
                "sync_mode",
                (
                    "**Full refresh** reloads the whole table. Overwrite variants replace destination "
                    "rows — Pilot always asks you to **Confirm** before that runs."
                ),
            ),
            (
                re.compile(r"\boverwrite\b"),
                "sync_mode",
                (
                    "**Overwrite** (full refresh overwrite) replaces every row at the destination. "
                    "It is powerful and risky — Pilot stages Confirm and warns before anything moves. "
                    "Prefer upsert or CDC when you only need changes."
                ),
            ),
            (
                re.compile(r"\b(?:ssn|social security)\b"),
                "pii",
                (
                    "SSN-like fields are **PII**. Ask me to introspect or sample a table "
                    '(e.g. "schema of employees on your saved connector") and I flag sensitive columns. '
                    "Preflight keeps PII tags visible — I never invent column names without a live read."
                ),
            ),
            (
                re.compile(r"\b(?:g[1-9]|gate\s*[1-9]|dry\s+run|nine\s+gates|9\s+gates)\b"),
                "preflight",
                (
                    "Preflight has **9 gates** (G1–G9). **G5 Dry run** samples coercion "
                    "before Execute — fail-closed when types don't convert cleanly. "
                    "Ask \"what quality gates do you have?\" for the full list."
                ),
            ),
            (
                re.compile(r"\bconfirm\b"),
                "confirm",
                (
                    "Sensitive Pilot actions (**start transfer**, **save connector**, **run pipeline now**, "
                    "**fix mapping**) stage a Confirm card. Nothing mutates until you press Allow/Confirm. "
                    "Credentials for create-connector stay on the server ack ledger — the browser only "
                    "sees a redacted preview."
                ),
            ),
            (
                re.compile(
                    r"\b(?:without|no)\s+(?:openai|anthropic|ollama|cloud|api key)"
                    r"|\blocal(?:-|\s)?primary\b"
                    r"|\blocal\s+only\b"
                    r"|\b(?:don'?t|do\s+not|never)\s+use\s+(?:openai|anthropic|ollama|cloud)\b"
                    r"|\bare\s+you\s+local\b"
                ),
                "local_primary",
                (
                    "Yes. **Datawrap Pilot local engine is primary** — NL → tools → compose works with no "
                    "OpenAI, Anthropic, or Ollama key. Optional `DATAFLOW_PILOT_ENGINE=hybrid` can polish "
                    "narration with a cloud/local LLM, but transfers, aggregates, schema, and Confirm "
                    "do not require them."
                ),
            ),
        ]
        curated = next(
            ((intent, answer) for pattern, intent, answer in direct if pattern.search(lower)),
            None,
        )

        # The shipped operator documentation, retrieved lexically with a grounding
        # floor. Documentation leads even when a curated definition matched: the
        # curated prose carries no citation, so answering from it alone gave the
        # operator a confident paragraph they could not trace to any page.
        doc_hits = product_doc_search(query, limit=3)
        if doc_hits:
            documented = compose_documented_answer(doc_hits)
            return ToolResult(
                name="explain_product",
                success=True,
                output={
                    "intent": curated[0] if curated else "documentation",
                    # Curated regex is the lead definition; Help citations follow
                    # so every sentence sits next to a page the operator can open.
                    "answer": f"{curated[1]}\n\n{documented}" if curated else documented,
                    "capabilities": PRODUCT_CAPABILITIES[:6],
                    # No navigate action: the citations below are the control that
                    # opens the article, and a second one only competes with them.
                    "actions": [],
                    "sources": [hit.as_source() for hit in doc_hits],
                    "grounded": True,
                    "source": "product_documentation",
                },
            )

        if curated:
            return ToolResult(
                name="explain_product",
                success=True,
                output={
                    "intent": curated[0],
                    "answer": curated[1],
                    "capabilities": PRODUCT_CAPABILITIES[:6],
                    "actions": [],
                    "sources": [],
                    "grounded": False,
                    "source": "local_product_faq",
                },
            )

        # No Help hit and no explicit FAQ regex: refuse. Keyword-bucket
        # CONVERSATION_TEMPLATES are not evidence.
        return ToolResult(
            name="explain_product",
            success=True,
            output=unsupported_question_output(query),
        )

    def _search_knowledge(self, query: str = "") -> ToolResult:
        if not query.strip():
            return ToolResult(name="search_knowledge", success=False, output=None, error="query required")

        # Embedding search always returns its nearest neighbours, so a question about
        # nothing this product does still came back with three confident-looking
        # fragments ("how do I cook rice" → paste a job id, your dataset has 11
        # columns). If the question names no documented subject, refuse before
        # retrieval instead of narrating whatever the index happened to be closest to.
        if not is_answerable_subject(query):
            return ToolResult(
                name="search_knowledge",
                success=True,
                output=unsupported_question_output(query),
            )

        # Shipped operator documentation answers first, with citations. Embedding
        # similarity alone returned readable-looking fragments the operator could
        # not trace back to any page, so a documented answer looked like a guess.
        doc_hits = product_doc_search(query, limit=3)
        if doc_hits:
            return ToolResult(
                name="search_knowledge",
                success=True,
                output={
                    "query": query,
                    "answer": compose_documented_answer(doc_hits),
                    "hits": [
                        {
                            "text": hit.chunk.text[:600],
                            "score": round(hit.score, 3),
                            "type": "product_doc",
                            "summary": hit.chunk.citation,
                        }
                        for hit in doc_hits
                    ],
                    "count": len(doc_hits),
                    "empty": False,
                    "sources": [hit.as_source() for hit in doc_hits],
                    "grounded": True,
                    "source": "product_documentation",
                },
            )

        # On-vocabulary but no citable Help page: refuse. Embedding nearest
        # neighbours are not an answer — they narrated ontology shards as
        # product knowledge (subscriber_id → telecom synonym dump).
        refused = unsupported_question_output(query)
        refused["hint"] = (
            "No grounded product knowledge matched. Ask about a saved "
            "connector, table, job ID, or pf_ validation run — or say "
            "what can you do."
        )
        return ToolResult(
            name="search_knowledge",
            success=True,
            output=refused,
        )

    def _plan_transfer_route(
        self,
        source: str = "",
        destination: str = "",
        workload: str = "unknown",
        table: str = "",
        dest_table: str = "",
        sync_mode: str = "",
        leftover_nl: str = "",
        contract_id: str = "",
        require_signed_contract: Any = None,
        validation_mode: str = "",
        schema_policy: str = "",
    ) -> ToolResult:
        """Route guidance. Real plan when connectors resolve, honest sketch otherwise.

        This used to substring-match "csv" in the connector name and return a
        gate list whose IDs did not exist in ``PREFLIGHT_GATES``. Now the named
        gates come from the registry, and naming a table gets the operator the
        engine's actual mapping and gate results instead of a guess.

        Bind / validation / schema posture must survive the hop into
        ``plan_transfer``. Unbound still leaves enforce unset. A generic sketch
        names the requested posture but is not a plan for the operator's data.
        """
        bind = resolve_transfer_bind_kwargs(
            leftover_nl,
            source,
            destination,
            contract_id=contract_id,
            require_signed_contract=require_signed_contract,
            validation_mode=validation_mode,
            schema_policy=schema_policy,
        )
        source_clean, _ = parse_transfer_bind_and_rules(source)
        dest_clean, _ = parse_transfer_bind_and_rules(destination)
        source = (source_clean or source or "").strip()
        destination = (dest_clean or destination or "").strip()
        if source and destination and table:
            from .transfer_tools import plan_transfer

            planned = plan_transfer(
                source_connector_name=source,
                source_table=table,
                dest_connector_name=destination,
                dest_table=dest_table or table,
                sync_mode=sync_mode or workload,
                **bind,
            )
            if planned.success:
                return planned

        try:
            from preflight.gates import PREFLIGHT_GATES

            gate_ids = [gid.value if hasattr(gid, "value") else str(gid) for gid, _ in PREFLIGHT_GATES]
        except Exception:
            gate_ids = []

        from .example_phrases import example_connector_name, example_dest_connector_name

        return ToolResult(name="plan_transfer_route", success=True, output={
            "generic": True,
            "source": source or "source not specified",
            "destination": destination or "destination not specified",
            "required_gates": gate_ids,
            **bind,
            "note": (
                "This is the standard gate sequence, not a plan for your data. "
                "Name two saved connectors and a table and I will introspect both "
                "ends, map the columns and run the real gates."
            ),
            "next": (
                f'Try: "plan a transfer of orders from {example_connector_name()} '
                f'to {example_dest_connector_name()}".'
            ),
        })

    def _explain_mapping_assurance(self) -> ToolResult:
        return ToolResult(name="explain_mapping_assurance", success=True, output={
            "assignment": "optimal_bipartite_hungarian",
            "scoring_layers": [
                "exact normalized name",
                "semantic token expansion",
                "schematic canonicalization",
                "role compatibility",
                "BM25 lexical retrieval",
                "character n-gram similarity",
                "type compatibility penalty",
                "trained lexicon and optional ML baseline",
            ],
            "guarantees": [
                "no duplicate target assignment in automatic mappings",
                "exact matches outrank broad synonyms",
                "ambiguous close-score mappings require review",
                "preflight blocks incompatible mappings before execution",
                "reconciliation verifies row counts/checksums after execution",
            ],
            "not_claimed": "No system can infer perfect business semantics without ground truth; Datawrap fails closed when evidence is ambiguous.",
        })

    def _recommend_sync_mode(
        self,
        workload: str = "",
        has_cursor: bool = False,
        has_primary_key: bool = False,
        needs_history: bool = False,
        source_read_mode: str = "",
    ) -> ToolResult:
        w = workload.lower()
        callable_src = (source_read_mode or "").strip().lower() in {"procedure", "query"}
        if callable_src and ("cdc" in w or needs_history or "scd" in w or "mirror" in w):
            return ToolResult(name="recommend_sync_mode", success=True, output={
                "recommended_mode": "Full Refresh Append",
                "reason": (
                    "CALL/SELECT is a result-set snapshot, not a WAL/binlog or table "
                    "identity. CDC, SCD2, and mirror are refused. Use full refresh, "
                    "or incremental only when the procedure is cursor-stable."
                ),
                "requires": {
                    "cursor": False,
                    "primary_key": False,
                    "cdc_log_access": False,
                },
            })
        if "cdc" in w:
            mode = "Incremental CDC"
            reason = "Source changes should be read from a log stream and resumed from cursor state."
        elif "upsert" in w or "merge" in w or (has_primary_key and "incremental" in w):
            mode = "Incremental Upsert"
            reason = (
                "Primary key (or upsert/merge wording) lets destination rows be "
                "updated in place without a full reload."
            )
        elif has_cursor and has_primary_key and needs_history:
            mode = "Incremental Append + Deduped"
            reason = "Cursor and key allow efficient updates while preserving change history."
        elif has_cursor:
            mode = "Incremental Append"
            reason = "Cursor allows new records to be read without a full scan."
        elif "snapshot" in w or "full" in w or "overwrite" in w:
            mode = "Full Refresh Overwrite"
            reason = "Snapshot workloads should replace the destination with the latest source state."
        elif has_primary_key:
            mode = "Incremental Upsert"
            reason = "A primary key is enough to upsert; add a cursor later to avoid full scans."
        else:
            mode = "Full Refresh Append"
            reason = "Use append until cursor/key metadata is confirmed."
        return ToolResult(name="recommend_sync_mode", success=True, output={
            "recommended_mode": mode,
            "reason": reason,
            "requires": {
                "cursor": "Append" in mode or "CDC" in mode,
                "primary_key": "Upsert" in mode or "Deduped" in mode,
                "cdc_log_access": "CDC" in mode,
            },
        })

    def _profile_quality_rules(self, dataset_name: str = "") -> ToolResult:
        schema = self.analyst.resolve_dataset(dataset_name) if dataset_name else None
        columns = schema.columns if schema else []
        pii_candidates = [c for c in columns if any(t in c.lower() for t in ("email", "phone", "ssn", "card", "name"))]
        gate_ids: list[str] = []
        try:
            from preflight.gates import PREFLIGHT_GATES

            gate_ids = [
                gid.value if hasattr(gid, "value") else str(gid)
                for gid, _ in PREFLIGHT_GATES
            ]
        except Exception:
            gate_ids = []
        return ToolResult(name="profile_quality_rules", success=True, output={
            "dataset": schema.name if schema else dataset_name or "active dataset",
            "rules": [
                "declared types validated by G3 schema contract (sample-aware when rows exist)",
                "null rate checked against inferred required / NOT NULL fields",
                "primary key uniqueness when candidate key exists",
                "PII columns tagged before destination write",
                "row rejection quarantine enabled for lossy coercions",
                "post-write row count and checksum reconciliation (G8/G9)",
            ],
            "honesty": (
                "No invented numeric parse-success floor — cite preflight_gates / "
                "evidence pack measurements, not marketing thresholds."
            ),
            "preflight_gates": gate_ids,
            "pii_candidates": pii_candidates,
            "column_count": len(columns),
            "has_dataset": bool(schema and columns),
            "next_steps": (
                [
                    "Sample a live table to profile real null/type rates",
                    "Say fix bad data to open Transfer Studio remediation (Confirm)",
                    "Run Validate (9 gates) before Execute",
                ]
                if schema and columns
                else [
                    "Upload a file in Transfer or name a dataset to analyze",
                    "Or: sample <table> on <connector>",
                    "Ask what quality gates do you have for the G1–G9 list",
                ]
            ),
        })

    def _resolve_schedule(self, schedule_id: str = "", name: str = ""):
        """Resolve schedule by id or name. Returns (schedule|None, clarification|None)."""
        from services.schedule_store import get_schedule, list_schedules

        sid = (schedule_id or "").strip()
        if sid:
            sched = get_schedule(sid)
            if sched:
                return sched, None
        needle = (name or "").strip().lower()
        if not needle:
            return None, None
        exact = []
        fuzzy = []
        for s in list_schedules():
            label = (s.name or "").strip().lower()
            if label == needle:
                exact.append(s)
            elif needle in label or label in needle:
                fuzzy.append(s)
        if exact:
            return exact[0], None
        if len(fuzzy) == 1:
            return fuzzy[0], None
        if len(fuzzy) > 1:
            names = [s.name for s in fuzzy[:5] if s.name]
            listed = ", ".join(f"**{n}**" for n in names)
            return None, f"Which pipeline did you mean? {listed}"
        return None, None

    def _schedule_summary(self, s) -> dict:
        from services.schedule_store import schedule_bind_summary

        row = {
            "id": s.id,
            "name": s.name,
            "enabled": s.enabled,
            "interval": s.interval,
            "cron": s.cron or "",
            "timezone": s.timezone,
            "source_table": s.source_table,
            "dest_table": s.dest_table,
            "sync_mode": getattr(s, "sync_mode", "") or "",
            "next_run_at": s.next_run_at,
            "last_run_at": s.last_run_at,
            "last_status": s.last_status,
            "run_count": s.run_count,
        }
        validation_mode = str(getattr(s, "validation_mode", "") or "").strip()
        schema_policy = str(getattr(s, "schema_policy", "") or "").strip()
        if validation_mode:
            row["validation_mode"] = validation_mode
        if schema_policy:
            row["schema_policy"] = schema_policy
        row.update(schedule_bind_summary(s))
        return row

    def _list_schedules(self, limit: int = 20) -> ToolResult:
        from services.schedule_store import list_schedules

        rows = [self._schedule_summary(s) for s in list_schedules()[: max(1, min(int(limit or 20), 100))]]
        return ToolResult(name="list_schedules", success=True, output={"schedules": rows, "count": len(rows)})

    def _get_schedule(self, schedule_id: str = "", name: str = "") -> ToolResult:
        sched, clarify = self._resolve_schedule(schedule_id, name)
        if clarify:
            return ToolResult(name="get_schedule", success=False, output=None, error=clarify)
        if not sched:
            return ToolResult(
                name="get_schedule",
                success=False,
                output=None,
                error="Schedule not found. Ask for the pipeline name or id from Pipelines.",
            )
        return ToolResult(name="get_schedule", success=True, output=self._schedule_summary(sched))

    def _run_schedule_now(self, schedule_id: str = "", name: str = "") -> ToolResult:
        sched, clarify = self._resolve_schedule(schedule_id, name)
        if clarify:
            return ToolResult(name="run_schedule_now", success=False, output=None, error=clarify)
        if not sched:
            return ToolResult(
                name="run_schedule_now",
                success=False,
                output=None,
                error="Which pipeline should I run? Give a schedule name or id.",
            )
        from .ack_ledger import get_ack_ledger
        from services.schedule_store import assert_schedule_run_allowed

        try:
            bind = assert_schedule_run_allowed(sched)
        except ValueError as exc:
            return ToolResult(name="run_schedule_now", success=False, output=None, error=str(exc))

        sync_mode = str(getattr(sched, "sync_mode", "") or "")
        overwrite = sync_mode == "full_refresh_overwrite"
        preview = {
            "schedule_id": sched.id,
            "name": sched.name,
            "source_connector_id": getattr(sched, "source_connector_id", "") or "",
            "dest_connector_id": getattr(sched, "dest_connector_id", "") or "",
            "source_table": getattr(sched, "source_table", "") or "",
            "dest_table": getattr(sched, "dest_table", "") or "",
            "sync_mode": sync_mode,
            **bind,
        }
        validation_mode = str(getattr(sched, "validation_mode", "") or "").strip()
        schema_policy = str(getattr(sched, "schema_policy", "") or "").strip()
        if validation_mode:
            preview["validation_mode"] = validation_mode
        if schema_policy:
            preview["schema_policy"] = schema_policy
        ack_id = get_ack_ledger().put(
            kind="run_schedule",
            payload={"schedule_id": sched.id, "name": sched.name},
            preview=preview,
        )
        return ToolResult(
            name="run_schedule_now",
            success=True,
            output={
                "action": "run_schedule",
                "schedule_id": sched.id,
                "name": sched.name,
                "label": f"Run pipeline “{sched.name}” now",
                "risk": "mutate",
                "destructive": overwrite,
                "requires_confirm": True,
                "ack_id": ack_id,
                "preview": preview,
            },
        )

    def _list_contracts(self, limit: int = 50) -> ToolResult:
        from services.contract_store import get_contract_store

        store = get_contract_store()
        contracts = store.list_contracts(limit=max(1, min(int(limit or 50), 200)))
        rows = []
        for c in contracts:
            if hasattr(c, "to_dict"):
                d = c.to_dict()
            elif isinstance(c, dict):
                d = c
            else:
                d = {"id": getattr(c, "id", ""), "name": getattr(c, "name", str(c))}
            rows.append({
                "id": d.get("id") or d.get("contract_id"),
                "name": d.get("name") or d.get("title") or d.get("id"),
                "status": d.get("status"),
                "updated_at": str(d.get("updated_at") or ""),
            })
        return ToolResult(name="list_contracts", success=True, output={"contracts": rows, "count": len(rows)})

    def _open_job(self, job_id: str = "") -> ToolResult:
        jid = (job_id or "").strip()
        if not jid:
            return ToolResult(name="open_job", success=False, output=None, error="job_id required")
        return ToolResult(
            name="open_job",
            success=True,
            output={
                "action": "navigate",
                "screen": "jobs",
                "job_id": jid,
                "label": f"Open job {jid[:12]}…",
                "risk": "safe",
            },
        )

    def _open_schedule(self, schedule_id: str = "", name: str = "") -> ToolResult:
        sched, clarify = self._resolve_schedule(schedule_id, name)
        if clarify:
            return ToolResult(name="open_schedule", success=False, output=None, error=clarify)
        if not sched:
            return ToolResult(
                name="open_schedule",
                success=False,
                output=None,
                error="Schedule not found. Ask for the pipeline name from Pipelines.",
            )
        return ToolResult(
            name="open_schedule",
            success=True,
            output={
                "action": "navigate",
                "screen": "schedules",
                "schedule_id": sched.id,
                "name": sched.name,
                "label": f"Open pipeline “{sched.name}”",
                "risk": "safe",
            },
        )

    def _start_transfer_studio(self) -> ToolResult:
        return ToolResult(
            name="start_transfer_studio",
            success=True,
            output={
                "action": "navigate",
                "screen": "transfer",
                "label": "Open Transfer Studio",
                "risk": "safe",
            },
        )

    def _list_connector_objects(
        self,
        connector_id: str = "",
        connector_name: str = "",
        limit: int = 100,
    ) -> ToolResult:
        from .schema_tools import list_connector_objects

        return list_connector_objects(connector_id, connector_name, limit)

    def _sample_connector_object(
        self,
        connector_id: str = "",
        connector_name: str = "",
        table: str = "",
        limit: int = 25,
        analyze: bool = True,
        session_id: str = "",
    ) -> ToolResult:
        from .query_tools import sample_connector_object

        return sample_connector_object(
            connector_id=connector_id,
            connector_name=connector_name,
            table=table,
            limit=limit,
            analyze=analyze,
            session_id=session_id,
        )

    def _run_query(
        self,
        connector_id: str = "",
        connector_name: str = "",
        query: str = "",
        collection: str = "",
        limit: int = 100,
        analyze: bool = False,
        session_id: str = "",
    ) -> ToolResult:
        from .query_tools import run_connector_query

        return run_connector_query(
            connector_id=connector_id,
            connector_name=connector_name,
            query=query,
            collection=collection,
            limit=limit,
            analyze=analyze,
            session_id=session_id,
        )

    def _aggregate_data(
        self,
        connector_id: str = "",
        connector_name: str = "",
        table: str = "",
        metric: str = "count",
        column: str = "",
        group_by: str = "",
        order: str = "desc",
        limit: int = 20,
        session_id: str = "",
        where: str = "",
    ) -> ToolResult:
        from .aggregate_tools import aggregate_connector_data

        return aggregate_connector_data(
            connector_id=connector_id,
            connector_name=connector_name,
            table=table,
            metric=metric,
            column=column,
            group_by=group_by,
            order=order,
            limit=limit,
            session_id=session_id,
            where=where,
        )

    def _plan_transfer(
        self,
        source_connector_id: str = "",
        source_connector_name: str = "",
        source_table: str = "",
        dest_connector_id: str = "",
        dest_connector_name: str = "",
        dest_table: str = "",
        sync_mode: str = "",
        schema_policy: str = "manual_review",
        validation_mode: str = "balanced",
        source_timezone: str = "",
        source_read_mode: str = "",
        procedure_call: str = "",
        source_query: str = "",
        procedure_params: Any = None,
        contract_id: str = "",
        require_signed_contract: Any = None,
        source_filter: dict | None = None,
        upsert_key: str = "",
        dedupe_key: str = "",
        rule_questions: list | None = None,
        applied_rules: list | None = None,
        cadence: str = "",
        all_tables: bool = False,
        limit: int = 0,
    ) -> ToolResult:
        from .transfer_tools import plan_transfer

        return plan_transfer(
            source_connector_id=source_connector_id,
            source_connector_name=source_connector_name,
            source_table=source_table,
            dest_connector_id=dest_connector_id,
            dest_connector_name=dest_connector_name,
            dest_table=dest_table,
            sync_mode=sync_mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            source_timezone=source_timezone,
            source_read_mode=source_read_mode,
            procedure_call=procedure_call,
            source_query=source_query,
            procedure_params=procedure_params,
            contract_id=contract_id,
            require_signed_contract=require_signed_contract,
            source_filter=source_filter,
            upsert_key=upsert_key,
            dedupe_key=dedupe_key,
            rule_questions=rule_questions,
            applied_rules=applied_rules,
            cadence=cadence,
            all_tables=all_tables,
        )

    def _start_transfer(
        self,
        source_connector_id: str = "",
        source_connector_name: str = "",
        source_table: str = "",
        dest_connector_id: str = "",
        dest_connector_name: str = "",
        dest_table: str = "",
        sync_mode: str = "",
        schema_policy: str = "manual_review",
        validation_mode: str = "balanced",
        limit: int = 0,
        source_timezone: str = "",
        source_read_mode: str = "",
        procedure_call: str = "",
        source_query: str = "",
        procedure_params: Any = None,
        contract_id: str = "",
        require_signed_contract: Any = None,
        source_filter: dict | None = None,
        upsert_key: str = "",
        dedupe_key: str = "",
        rule_questions: list | None = None,
        applied_rules: list | None = None,
        cadence: str = "",
        all_tables: bool = False,
    ) -> ToolResult:
        from .transfer_tools import start_transfer

        return start_transfer(
            source_connector_id=source_connector_id,
            source_connector_name=source_connector_name,
            source_table=source_table,
            dest_connector_id=dest_connector_id,
            dest_connector_name=dest_connector_name,
            dest_table=dest_table,
            sync_mode=sync_mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            limit=limit,
            source_timezone=source_timezone,
            source_read_mode=source_read_mode,
            procedure_call=procedure_call,
            source_query=source_query,
            procedure_params=procedure_params,
            contract_id=contract_id,
            require_signed_contract=require_signed_contract,
            source_filter=source_filter,
            upsert_key=upsert_key,
            dedupe_key=dedupe_key,
            rule_questions=rule_questions,
            applied_rules=applied_rules,
            cadence=cadence,
            all_tables=all_tables,
        )

    def _create_schedule(
        self,
        source_connector_id: str = "",
        source_connector_name: str = "",
        source_table: str = "",
        dest_connector_id: str = "",
        dest_connector_name: str = "",
        dest_table: str = "",
        sync_mode: str = "",
        schema_policy: str = "manual_review",
        validation_mode: str = "balanced",
        cadence: str = "",
        name: str = "",
        cursor_column: str = "",
        source_timezone: str = "",
        source_read_mode: str = "",
        procedure_call: str = "",
        source_query: str = "",
        procedure_params: Any = None,
        contract_id: str = "",
        require_signed_contract: Any = None,
        source_filter: dict | None = None,
        upsert_key: str = "",
        dedupe_key: str = "",
        rule_questions: list | None = None,
        applied_rules: list | None = None,
        limit: int = 0,
    ) -> ToolResult:
        from .schedule_tools import create_schedule

        return create_schedule(
            source_connector_id=source_connector_id,
            source_connector_name=source_connector_name,
            source_table=source_table,
            dest_connector_id=dest_connector_id,
            dest_connector_name=dest_connector_name,
            dest_table=dest_table,
            sync_mode=sync_mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            cadence=cadence,
            name=name,
            cursor_column=cursor_column,
            source_timezone=source_timezone,
            source_read_mode=source_read_mode,
            procedure_call=procedure_call,
            source_query=source_query,
            procedure_params=procedure_params,
            contract_id=contract_id,
            require_signed_contract=require_signed_contract,
            source_filter=source_filter,
            upsert_key=upsert_key,
            dedupe_key=dedupe_key,
            rule_questions=rule_questions,
            applied_rules=applied_rules,
            limit=limit,
        )

    def _analyze_result(
        self,
        result_id: str = "",
        session_id: str = "",
        column: str = "",
    ) -> ToolResult:
        from .query_tools import analyze_stored_result

        return analyze_stored_result(
            result_id=result_id,
            session_id=session_id,
            column=column,
        )

    def _filter_result(
        self,
        result_id: str = "",
        session_id: str = "",
        column: str = "",
        op: str = "eq",
        value: str = "",
        limit: int = 25,
    ) -> ToolResult:
        from .query_tools import filter_stored_result

        return filter_stored_result(
            result_id=result_id,
            session_id=session_id,
            column=column,
            op=op,
            value=value,
            limit=limit,
        )

    def _introspect_connector_schema(
        self,
        connector_id: str = "",
        connector_name: str = "",
        table: str = "",
    ) -> ToolResult:
        from .schema_tools import introspect_connector_schema

        return introspect_connector_schema(connector_id, connector_name, table)

    def _diff_schemas(
        self,
        source_connector_id: str = "",
        source_connector_name: str = "",
        source_table: str = "",
        dest_connector_id: str = "",
        dest_connector_name: str = "",
        dest_table: str = "",
    ) -> ToolResult:
        from .schema_tools import diff_schemas

        return diff_schemas(
            source_connector_id,
            source_connector_name,
            source_table,
            dest_connector_id,
            dest_connector_name,
            dest_table,
        )

    def _map_connector_schemas(
        self,
        source_connector_id: str = "",
        source_connector_name: str = "",
        source_table: str = "",
        dest_connector_id: str = "",
        dest_connector_name: str = "",
        dest_table: str = "",
        threshold: float = 0.85,
    ) -> ToolResult:
        from .schema_tools import map_connector_schemas

        return map_connector_schemas(
            source_connector_id,
            source_connector_name,
            source_table,
            dest_connector_id,
            dest_connector_name,
            dest_table,
            threshold,
        )

    def _inspect_schema_policy(self, change_type: str = "unknown", auto_apply: bool = False,
                               old_schema: dict | None = None, new_schema: dict | None = None) -> ToolResult:
        if old_schema is not None or new_schema is not None:
            from services.schema_drift import classify_schema_change

            result = classify_schema_change(old_schema, new_schema)
            return ToolResult(
                name="inspect_schema_policy",
                success=True,
                output={
                    "mode": "live",
                    "severity": result.get("severity"),
                    "additive": result.get("additive") or [],
                    "breaking": result.get("breaking") or [],
                    "auto_apply": False,
                    "operator_review": result.get("severity") != "none",
                },
            )
        policies = {
            "new_column": ("non_breaking", "create target field and optionally backfill"),
            "removed_column": ("non_breaking", "retain target field but stop updating it"),
            "new_stream": ("non_breaking", "create stream/table and start first sync"),
            "removed_stream": ("non_breaking", "stop updating destination stream but retain history"),
            "type_change": ("review", "quarantine incompatible rows and require schema refresh"),
            "cursor_removed": ("breaking", "pause sync until cursor is restored or remapped"),
            "primary_key_removed": ("breaking", "pause sync until key is restored or dedupe mode changes"),
            "unknown": ("review", "detect diff and require operator approval"),
        }
        severity, action = policies.get(change_type, policies["unknown"])
        return ToolResult(name="inspect_schema_policy", success=True, output={
            "mode": "advisory",
            "change_type": change_type,
            "severity": severity,
            "auto_apply": auto_apply and severity == "non_breaking",
            "action": action,
            "operator_review": severity != "non_breaking" or not auto_apply,
        })


_META_PILOT_PHRASES = (
    "what knowledge",
    "what do you know",
    "your knowledge",
    "what can you",
    "what do you do",
    "who are you",
    "what are you",
    "how do you work",
    "your capabilities",
    "what knowledge you have",
    "knowledge you have",
    "trained knowledge",
    "what can pilot",
    "help me with data pilot",
    "help with data pilot",
    "help me with datawrap pilot",
    "help with datawrap pilot",
    "help me with dataflow pilot",
    "help with dataflow pilot",
    "describe yourself",
    "describe data pilot",
    "describe datawrap pilot",
    "tell me about yourself",
    "what tools do you have",
    "your tools",
    "what are you good at",
    "what you can help",
    "what can you help",
    "what should i ask",
    "are you able to help",
    "can you help me move",
    "able to help me move",
    "tell me what you can",
)


def _is_meta_pilot_question(lower: str) -> bool:
    if any(p in lower for p in _META_PILOT_PHRASES):
        return True
    if lower.strip() in {"capabilities", "help", "about", "about you"}:
        return True
    if re.search(r"\b(what|which)\s+(knowledge|skills|tools)\b", lower):
        return True
    if re.search(r"\btell me what you can\b", lower):
        return True
    # Brand-agnostic Pilot help ("help me with Datawrap Pilot", etc.)
    if re.search(
        r"\bhelp(?:\s+me)?\s+with\s+(?:data(?:wrap|flow|pilot)\s+)?pilot\b",
        lower,
    ):
        return True
    return False


def _looks_like_product_howto(lower: str) -> bool:
    """Product how-to / FAQ — answer from curated local FAQ, not RAG or cloud."""
    text = (lower or "").strip()
    if not text:
        return False
    howto = bool(
        re.search(
            r"\b(?:what is|what'?s|what are|how do i|how does|how to|explain|"
            r"tell me (?:everything |more )?about|where (?:do|can) i|can i|"
            r"what makes|remind me|"
            r"do i need|is .+ dangerous|how is)\b",
            text,
        )
    )
    product = bool(
        re.search(
            r"\b(?:dataflow|datawrap|datatransfer|data transfer|transfer studio|preflight|"
            r"mapping|connector|pipeline|validate|quarantine|sso|pii|gdpr|"
            r"hipaa|airbyte|fivetran|gates?|move (?:my |the )?data|sync data|"
            r"schema types?|semantic types?|type system|logical types?|"
            r"transfers?|pilot|openai|anthropic|"
            r"ollama|confirm|upsert|append|cdc|sync mode|full refresh|merge|"
            r"api key|accurate|mcp|contracts?|reconcile)\b",
            text,
        )
    )
    if howto and product:
        return True
    # Bare product identity questions (legacy DataFlow + current Datawrap brand)
    if re.search(r"\bwhat is data(?:flow|wrap|transfer)\b", text):
        return True
    if re.search(r"\bhow do i (?:transfer|move|sync|map|connect|validate)\b", text):
        return True
    # Local-primary / no-cloud FAQ
    if re.search(r"\b(?:without|no)\s+(?:openai|anthropic|ollama|cloud|api key)\b", text):
        return True
    if re.search(r"\b(?:don'?t|do\s+not|never)\s+use\s+(?:openai|anthropic|ollama|cloud)\b", text):
        return True
    if re.search(
        r"\b(?:are\s+you|you\s+(?:are|run)|runs?)\s+local(?:\s+only|\s+primary)?\b"
        r"|\blocal\s+only\b"
        r"|\blocal(?:-|\s)?primary\b",
        text,
    ):
        return True
    if re.search(r"\b(?:need|require)\s+(?:an?\s+)?(?:openai|anthropic|ollama|api)\s+key\b", text):
        return True
    if re.search(r"\b(?:append|upsert|cdc|full refresh)\s+mode\b", text):
        return True
    if re.search(r"\bfull refresh\b.+\b(?:dangerous|safe|overwrite)\b|\b(?:dangerous|safe).+\bfull refresh\b", text):
        return True
    if re.search(r"\boverwrite\b.+\b(?:safe|dangerous|risk)\b|\b(?:safe|dangerous|risk).+\boverwrite\b", text):
        return True
    if re.search(r"\bconfirm\b.+\bwork", text) or re.search(r"\bhow\s+confirm\b", text):
        return True
    if re.search(r"\bmapping\b.+\baccurat", text):
        return True
    if re.search(r"\b(?:ssn|social security|pii)\b", text) and re.search(
        r"\b(?:column|columns|look|detect|find|which)\b", text
    ):
        return True
    if re.search(r"\b(?:g[1-9]|gate\s*[1-9]|dry\s*run|9\s+gates|nine\s+gates)\b", text):
        return True
    return False


def _has_explicit_workspace_subject(lower: str) -> bool:
    """True when the ask names a live connector/table/job — keep ops tools."""
    if re.search(r"\bfrom\s+.+\s+to\s+\w", lower):
        return True
    if re.search(r"\b(?:to|into)\s+.+\s+from\s+\w", lower):
        return True
    if re.search(
        r"\bon\s+(?:local\s+)?(?:postgres|postgresql|mongodb|mongo|warehouse|mysql|snowflake|bigquery)\b",
        lower,
    ):
        return True
    if re.search(r"\bfrom\s+(?:local\s+)?(?:postgres|postgresql|mongodb|mongo|warehouse|mysql|snowflake|bigquery)\b", lower):
        return True
    if re.search(r"\b(?:job_|pf_)[A-Za-z0-9_\-]+", lower):
        return True
    if re.search(r"\bmapping assurance\b", lower):
        return True
    return False


# A pasted statement, not an English sentence that happens to contain "with".
_SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*(.+?)```", re.IGNORECASE | re.DOTALL)
_SQL_SELECT_SHAPE = re.compile(r"^\s*select\b[\s\S]*?\bfrom\b\s*\S", re.IGNORECASE)
_SQL_WITH_SHAPE = re.compile(
    r"^\s*with\s+[\"`\[]?[a-z_][\w]*[\"`\]]?\s+as\s*\(", re.IGNORECASE
)

# Phrases that are never a connector name. "errors from yesterday" used to be
# read as a table on a connector literally named "yesterday".
_TIME_PHRASE_RE = re.compile(
    r"^(?:yesterday|today|tonight|tomorrow|now|recently|lately|"
    r"(?:the\s+)?(?:last|past|previous|this|current|next)\s+"
    r"(?:few\s+|\d+\s+)?(?:second|minute|hour|day|week|month|quarter|year)s?|"
    r"\d+\s+(?:second|minute|hour|day|week|month|quarter|year)s?(?:\s+ago)?)$",
    re.IGNORECASE,
)
_NON_CONNECTOR_PHRASES = frozenset({
    "it", "them", "this", "that", "these", "those", "here", "there",
    "the database", "my database", "the source", "the destination",
    "the table", "the collection", "anywhere", "everywhere",
})


def _extract_sql_statement(message: str) -> str:
    """Return a pasted SQL statement, or "" when the text is plain English.

    Anchored at the start of the statement on purpose. The previous pattern
    matched a bare ``WITH``/``SELECT`` anywhere in the message, so
    "what is wrong with my mapping" was sent to a live database as
    ``WITH my mapping`` — ``_is_safe_sql`` allows ``WITH`` as a read-only start,
    so the garbage reached the driver before failing.
    """
    text = (message or "").strip()
    fenced = _SQL_FENCE_RE.search(text)
    if fenced:
        text = fenced.group(1).strip()
    if _SQL_SELECT_SHAPE.match(text) or _SQL_WITH_SHAPE.match(text):
        return text
    # "please run this: SELECT …" / "execute SELECT … on Local Postgres"
    prefixed = re.search(
        r"(?:(?:please\s+)?(?:run|execute)\s+(?:this\s*)?:?\s*)?(select\b[\s\S]+)$",
        text,
        re.I,
    )
    if prefixed:
        candidate = prefixed.group(1).strip()
        # Drop trailing connector hints that are not SQL.
        candidate = re.sub(
            r"\s+on\s+[A-Za-z0-9_\- ]+$",
            "",
            candidate,
            flags=re.I,
        ).strip()
        if _SQL_SELECT_SHAPE.match(candidate) or _SQL_WITH_SHAPE.match(candidate):
            return candidate
    return ""


def _clean_connector_phrase(raw: str) -> str:
    """Normalize a captured connector phrase, or "" when it cannot be one."""
    phrase = (raw or "").strip().strip("\"'`").strip(" .,;:!?")
    phrase = re.sub(r"\b(?:please|now|thanks?|thank\s+you)\b\s*$", "", phrase, flags=re.I)
    phrase = phrase.strip(" .,;:!?")
    if not phrase:
        return ""
    # "postgres connector Venkat" → "Venkat". Never strip glued names like
    # "MySQL connectionCrown" (no space after connection) — those are saved as-is.
    phrase = re.sub(
        r"^(?:(?:a|an|the)\s+)?"
        r"(?:mysql|mariadb|postgres(?:ql)?|pg|mongo(?:db)?|snowflake|redshift|"
        r"bigquery|bq|sql\s*server|mssql|oracle|sqlite|redis|dynamodb|databricks)\s+"
        r"(?:connection|connector)\s+",
        "",
        phrase,
        flags=re.I,
    ).strip()
    phrase = re.sub(
        r"^(?:(?:a|an|the)\s+)?(?:connection|connector)\s+",
        "",
        phrase,
        flags=re.I,
    ).strip()
    low = phrase.lower()
    if low in _NON_CONNECTOR_PHRASES or _TIME_PHRASE_RE.match(low):
        return ""
    return phrase


def _capture_connector_name(raw: str) -> str:
    """Capture a full connector name (multi-word). Never keep a 1-char `.+?` scrap.

    Optional quotes around a non-greedy `.+?` otherwise match a single letter
    (``Local Postgres`` → ``l``), which breaks every live DB ask.
    """
    text = (raw or "").strip()
    if not text:
        return ""
    quoted = re.match(r'^[\"\']([^\"\']+)[\"\']\s*[.?!]?$', text)
    if quoted:
        return _clean_connector_phrase(quoted.group(1))
    # Unquoted: take the whole remainder (supports "Local Postgres").
    return _clean_connector_phrase(re.sub(r"[.?!]+$", "", text).strip())


def _is_raw_knowledge_shard(text: str) -> bool:
    t = text.strip()
    if t.startswith("Semantic type:"):
        return True
    markers = ("Category:", "Patterns:", "PII:", "Data type:")
    return sum(1 for m in markers if m in t) >= 3


def _query_targets_semantic_type(query: str, text: str) -> bool:
    q = query.lower()
    # Only keep ontology shards when the user clearly asked about that concept.
    m = re.search(r"Semantic type:\s*([^.]+)", text, re.I)
    if not m:
        return False
    label = m.group(1).strip().lower()
    tokens = [t for t in re.split(r"[^a-z0-9]+", label) if len(t) > 2]
    return bool(tokens) and any(t in q for t in tokens)


def _summarize_knowledge_hit(text: str) -> str:
    """Turn a hit into a short natural sentence (local, no LLM)."""
    if "Assistant:" in text:
        return text.split("Assistant:", 1)[1].strip()[:400]
    if text.startswith("Semantic type:"):
        m = re.search(
            r"Semantic type:\s*([^.]+)\.\s*Category:\s*([^.]+)\.\s*Patterns:\s*([^.]+)",
            text,
            re.I,
        )
        if m:
            return (
                f"**{m.group(1).strip()}** is a {m.group(2).strip()} semantic type "
                f"(column patterns: {m.group(3).strip()})."
            )
    # Prefer the first prose sentence over key:value dumps.
    for line in text.splitlines():
        line = line.strip()
        if line and ":" not in line[:24]:
            return line[:400]
    return text[:280]


def _looks_like_unsupported_mutation(lower: str) -> bool:
    """True when the operator asked for delete / export / create-schedule we refuse.

    These must never fall into RAG — synonym dumps look like we can do the action.
    """
    if any(
        w in lower
        for w in (
            "export ", "export to", "download ", "download as",
            "save as csv", "save as parquet", "to csv", "to parquet", "to excel",
            "create a new schedule", "create schedule", "create a pipeline",
            "new nightly", "build a cron", "cron pipeline",
            "schedule this transfer", "schedule this nightly", "schedule it nightly",
            # Do NOT match bare "schedule nightly" / "nightly schedule" —
            # those collide with show/open schedule named "Nightly …".
        )
    ):
        return True
    if re.search(
        r"\b(?:delete|drop|destroy|remove)\b.+\b(?:connector|connection|schedule|pipeline)\b",
        lower,
    ):
        return True
    if re.search(r"\b(?:delete|drop|destroy)\b.+\b(?:table|collection)\b", lower):
        return True
    if re.search(r"\bremove\s+(?:the\s+)?[\w\s.-]+\s+connector\b", lower):
        return True
    # Create-schedule only — do not refuse show/run/open schedule <name>.
    if re.search(
        r"\b(?:create|new|build|make)\b.+\b(?:nightly|daily|hourly|cron)\b.+\bschedule\b",
        lower,
    ):
        return True
    if re.search(
        r"\bschedule\s+this\b.+\b(?:nightly|daily|hourly|every\s+\d+|cron)\b",
        lower,
    ):
        return True
    return False


def _is_noise_knowledge_hit(text: str) -> bool:
    """Synonym dumps / industry catalog shards are not answers to ops questions."""
    t = (text or "").strip()
    if not t:
        return True
    low = t.lower()
    if low.startswith("synonym group:"):
        return True
    if low.startswith("industry schema:"):
        return True
    if "synonym group:" in low and "=" in t[:80]:
        return True
    return False


def _looks_like_live_data_fetch(lower: str) -> bool:
    """True when the operator wants live table rows — never answer with RAG synonyms."""
    if re.search(
        r"\b(?:get|fetch|pull|grab|load|show|preview|sample|give\s+me)\b.+\b(?:from|on|in)\b.+"
        r"\b(?:postgres|postgresql|mysql|mongo|mongodb|snowflake|bigquery|sql\s*server|sqlite|warehouse|connector)\b",
        lower,
    ):
        return True
    if re.search(
        r"\b(?:get|fetch|pull|show|preview|sample)\b.+\b(?:data|rows|table|records)\b.+\b(?:from|on|in)\b",
        lower,
    ):
        return True
    if re.search(
        r"\b(?:users?|orders?|customers?|products?|employees?|invoices?)\s+(?:data\s+)?(?:from|on|in)\b",
        lower,
    ):
        return True
    return False


def _looks_like_domain_knowledge_query(lower: str) -> bool:
    """RAG fallback only for substantive domain questions — never chat fluff."""
    if _is_meta_pilot_question(lower):
        return False
    if _looks_like_unsupported_mutation(lower):
        return False
    if _looks_like_live_data_fetch(lower):
        return False
    if len(lower.strip()) < 16:
        return False
    fluff = ("thank", "thanks", "ok", "okay", "sure", "cool", "great", "nice", "lol")
    if lower.strip() in fluff or any(lower.startswith(f + " ") for f in fluff):
        return False
    # Destructive / unsupported intents must fall through to honest unmapped
    # replies — never invent knowledge hits that sound like we can delete/export.
    if any(
        w in lower
        for w in (
            "delete ", "drop ", "destroy ", "truncate ", "remove connector",
            "remove the ", "export ", "download ", "create a new schedule", "create schedule",
            "create a pipeline", "new nightly", "cron ",
        )
    ):
        return False
    # Ops / data verbs are not "explain mapping" — route to live tools or refuse.
    if any(
        w in lower
        for w in (
            "get ", "fetch ", "pull ", "grab ", "give me ", "show me ",
            "how many", "count ", "sum ", "average ", "list ", "sample ",
        )
    ):
        return False
    signals = (
        "what is", "what's", "whats", "how do", "how does", "explain", "mean",
        "semantic", "synonym", "pii type", "cdc", "sync mode",
        "mapping assurance", "checksum", "reconcile",
    )
    return any(s in lower for s in signals)


# Higher = preferred primary intent when multiple tools fire.
_TOOL_PRIORITY: dict[str, int] = {
    # A named source→destination move is the most specific thing an operator can
    # ask for, so it outranks the schema tools it is built from.
    "start_transfer": 110,
    "plan_transfer": 108,
    "map_connector_schemas": 100,
    "diff_schemas": 95,
    "introspect_connector_schema": 90,
    # A parsed analytics question outranks sampling and raw SQL: "count of orders
    # by status" must answer with a real GROUP BY, not 25 preview rows.
    "aggregate_data": 89,
    "sample_connector_object": 88,
    "run_query": 87,
    "analyze_result": 86,
    "filter_result": 85,
    "list_connector_objects": 84,
    "create_connector": 82,
    "remediate_validation": 80,
    # A cadence names a standing instruction, so it outranks a one-off run.
    "create_schedule": 109,
    "run_schedule_now": 78,
    "get_job": 75,
    "get_preflight_run": 74,
    "open_job": 72,
    "open_schedule": 71,
    "get_schedule": 70,
    "list_schedules": 60,
    "list_contracts": 58,
    "brief_workspace": 56,
    "list_jobs": 55,
    "list_connectors": 52,
    "search_connectors": 50,
    "plan_transfer_route": 48,
    "explain_mapping_assurance": 46,
    "get_transfer_capabilities": 45,
    "recommend_sync_mode": 44,
    "inspect_schema_policy": 42,
    "profile_quality_rules": 40,
    "search_knowledge": 22,
    "navigate": 35,
    "start_transfer_studio": 34,
    "list_datasets": 30,
    "compare_datasets": 25,
    "analyze_dataset": 20,
    "search_data": 15,
    "describe_pilot": 5,
    "explain_product": 6,
}

_LIVE_SCHEMA_TOOLS = frozenset({
    "map_connector_schemas",
    "diff_schemas",
    "introspect_connector_schema",
    "sample_connector_object",
    "aggregate_data",
    "run_query",
    "analyze_result",
    "filter_result",
    "list_connector_objects",
})


def prune_planned_tools(planned: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    """Keep a coherent primary intent — don't stack conflicting tool dumps."""
    if not planned:
        return planned
    names = {n for n, _ in planned}
    # Live DB schema wins over uploaded-dataset analysis / RAG
    if names & _LIVE_SCHEMA_TOOLS:
        planned = [
            (n, a) for n, a in planned
            if n not in ("analyze_dataset", "compare_datasets", "search_knowledge", "search_data")
        ]
    # A real aggregate answers the question; a 25-row sample or a regex-scraped
    # query on the same table is strictly worse noise beside it.
    # Explicit run_query (pasted SELECT / "run sql:") wins over NL aggregate.
    if "aggregate_data" in names and "run_query" in names:
        rq = next((a for n, a in planned if n == "run_query"), {}) or {}
        q = (rq.get("query") or "").upper()
        if any(
            tok in q
            for tok in (" GROUP BY ", " JOIN ", " UNION ", " HAVING ", " WITH ")
        ) or q.lstrip().startswith(("SELECT", "WITH", "EXPLAIN", "SHOW", "PRAGMA")):
            planned = [(n, a) for n, a in planned if n != "aggregate_data"]
            names = {n for n, _ in planned}
        else:
            planned = [
                (n, a) for n, a in planned
                if n not in ("sample_connector_object", "run_query", "analyze_dataset")
            ]
            names = {n for n, _ in planned}
    elif "aggregate_data" in names:
        planned = [
            (n, a) for n, a in planned
            if n not in (
                "sample_connector_object",
                "run_query",
                "analyze_dataset",
                "filter_result",
            )
        ]
        names = {n for n, _ in planned}
    # Platform inventory: "how many jobs failed" / "connector count" must not
    # become COUNT(*) — drop aggregate entirely beside inventory lists.
    if "list_jobs" in names or "list_connectors" in names:
        planned = [(n, a) for n, a in planned if n != "aggregate_data"]
        names = {n for n, _ in planned}
    # A concrete transfer already contains the mapping, gates and route, so the
    # generic advice tools beside it are redundant noise.
    if names & {"start_transfer", "plan_transfer", "create_schedule"}:
        planned = [
            (n, a) for n, a in planned
            if n not in (
                "plan_transfer_route", "recommend_sync_mode", "start_transfer_studio",
                "explain_mapping_assurance", "get_transfer_capabilities",
                "map_connector_schemas", "diff_schemas", "introspect_connector_schema",
                "sample_connector_object", "list_connector_objects", "search_connectors",
                "list_connectors",
            )
        ]
    # Job ID triage wins over generic list_jobs
    if "get_job" in names or "get_preflight_run" in names or "open_job" in names:
        planned = [(n, a) for n, a in planned if n != "list_jobs"]
    # Job / remediate: keep inventory lists for triage ("why did validate fail").
    if names & {"run_schedule_now", "create_connector", "create_schedule"}:
        planned = [
            (n, a) for n, a in planned
            if n not in (
                "list_schedules", "list_jobs", "search_knowledge",
                "analyze_dataset", "list_connectors", "search_connectors",
            )
        ]
    elif "remediate_validation" in names:
        planned = [
            (n, a) for n, a in planned
            if n not in (
                "list_schedules", "search_knowledge",
                "analyze_dataset", "list_connectors", "search_connectors",
            )
        ]
        # list_jobs stays as companion for validate / job triage.
        names = {n for n, _ in planned}
    # Cap to top-priority tools (navigate may accompany primary)
    ranked = sorted(
        planned,
        key=lambda p: (-_TOOL_PRIORITY.get(p[0], 0), p[0]),
    )
    keep: list[tuple[str, dict]] = []
    primary_tier = None
    for name, args in ranked:
        pri = _TOOL_PRIORITY.get(name, 0)
        if primary_tier is None:
            primary_tier = pri
            keep.append((name, args))
            continue
        # Allow companions within 25 points, plus navigate / triage lists.
        # Never attach RAG as a companion to ops tools — that caused synonym dumps.
        # Exception: quality-profile suggestions may keep a knowledge companion.
        if name == "search_knowledge" and primary_tier is not None and primary_tier >= 35:
            if "profile_quality_rules" not in {k[0] for k in keep} and "profile_quality_rules" not in names:
                continue
        if (
            name in (
                "navigate",
                "start_transfer_studio",
                "list_jobs",
                "brief_workspace",
                "list_datasets",
                "describe_pilot",
                "explain_product",
                "profile_quality_rules",
                "search_knowledge",
            )
            or pri >= primary_tier - 25
        ):
            if len(keep) < 3:
                keep.append((name, args))
    # Preserve original relative order among kept
    keep_set = {(n, json.dumps(a, sort_keys=True, default=str)) for n, a in keep}
    return [
        (n, a) for n, a in planned
        if (n, json.dumps(a, sort_keys=True, default=str)) in keep_set
    ]


# "move orders from Local Postgres to Warehouse" and its many phrasings. The
# table is captured separately from the endpoints so the planner can introspect
# a real object rather than pattern-matching the connector's name.
_TRANSFER_VERBS = r"transfer|trasfer|move|copy|sync|migrate|replicate|load|push|send|export"
# Trailing politeness / urgency after the destination must not kill the match
# ("…to Warehouse now", "…to wh please", "…to wh?").
_TRANSFER_TRAIL = (
    r"(?=(?:\s+(?:now|please|thanks|thank\s+you|for\s+me|asap|right\s+now)\b)"
    r"|(?:\s*[,;?])"
    r"|\s*$)"
)
_TRANSFER_RE = re.compile(
    rf"\b(?:{_TRANSFER_VERBS})\b"
    r"(?:\s+(?:a|an|the|all|my|our|these|those))?"
    r"(?:\s+(?:transfer|copy|sync|data|rows|records|everything))?"
    r"(?:\s+(?:of|for))?"
    r"\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+(?:table|collection|dataset))?"
    r"\s+(?:from|out\s+of)\s+(?P<src>.+?)"
    r"\s+(?:to|into|onto|over\s+to|->)\s+(?P<dst>.+?)"
    rf"{_TRANSFER_TRAIL}",
    re.IGNORECASE,
)
# "moving products Local Postgres -> Warehouse" (no explicit from)
_TRANSFER_ARROW_RE = re.compile(
    rf"\b(?:{_TRANSFER_VERBS}|moving)\b"
    r"(?:\s+(?:a|an|the|all|my|our))?"
    r"\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"\s+(?P<src>[A-Za-z0-9_][A-Za-z0-9_\- ]{0,48}?)"
    r"\s*->\s*"
    r"(?P<dst>.+?)"
    rf"{_TRANSFER_TRAIL}",
    re.IGNORECASE,
)
# "push orders to Warehouse from Local Postgres" (destination before source)
_TRANSFER_TO_FROM_RE = re.compile(
    rf"\b(?:{_TRANSFER_VERBS})\b"
    r"(?:\s+(?:a|an|the|all|my|our))?"
    r"(?:\s+(?:transfer|copy|sync|data|rows|records))?"
    r"(?:\s+(?:of|for))?"
    r"\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+(?:table|collection|dataset))?"
    r"\s+(?:to|into|onto|over\s+to|->)\s+(?P<dst>.+?)"
    r"\s+(?:from|out\s+of)\s+(?P<src>.+?)"
    rf"{_TRANSFER_TRAIL}",
    re.IGNORECASE,
)
# "transfer orders to Warehouse" (source omitted — plan route / clarify later)
_TRANSFER_TO_ONLY_RE = re.compile(
    rf"\b(?:{_TRANSFER_VERBS})\b"
    r"(?:\s+(?:a|an|the|all|my|our))?"
    r"(?:\s+(?:transfer|copy|sync|data|rows|records))?"
    r"(?:\s+(?:of|for))?"
    r"\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+(?:table|collection|dataset))?"
    r"\s+(?:to|into|onto|over\s+to|->)\s+(?P<dst>.+?)"
    rf"{_TRANSFER_TRAIL}",
    re.IGNORECASE,
)
# Trailing qualifiers that belong to the run, not to the destination's name.
_TRANSFER_TAIL_RE = re.compile(
    r"\s+(?:as|using|with|in|via)\s+(?P<mode>[\w\s-]{2,40})$",
    re.IGNORECASE,
)
# When the operator is asking rather than instructing, plan instead of staging.
# Ambiguity resolves toward the read-only branch on purpose.
_PLAN_ONLY_WORDS = (
    "plan", "would", "dry run", "dry-run", "preview", "what happens",
    "check", "simulate", "estimate", "before i", "safe", "should i",
    "is it ok", "risk",
)
# Explicit bind only — never invent a contract from "data contract" / "open contracts".
_CONTRACT_CLAUSE_RE = re.compile(
    r"(?:,\s*)?(?:with|under|using|against|bind(?:ing)?|enforce(?:ing)?)\s+"
    r"(?:the\s+)?(?:signed\s+)?(?:data\s+)?contract(?:\s+id)?\s*[:=]?\s*"
    r"(?P<cid>[A-Za-z][A-Za-z0-9_.:-]{1,63})",
    re.IGNORECASE,
)
_CONTRACT_ID_BARE_RE = re.compile(
    r"\bcontract(?:\s+id)?\s*[:=]?\s*"
    r"(?P<cid>[A-Za-z]{2,}[-_][A-Za-z0-9_.:-]+|[0-9a-fA-F]{8,})\s*$",
    re.IGNORECASE,
)
_CONTRACT_ID_STOP = frozenset({
    "id", "the", "a", "an", "my", "our", "this", "that", "data", "signed",
})
_VALIDATION_CLAUSE_RE = re.compile(
    r"(?:,\s*)?(?:with|using|in)?\s*(?:strict|balanced|lenient)\s+validation\b",
    re.IGNORECASE,
)
_RULES_CLAUSE_RE = re.compile(
    r"(?:,\s*)?(?:following|per|under|obey(?:ing)?)\s+(?:the\s+)?(?:data|migration)\s+rules\b",
    re.IGNORECASE,
)
_SCHEMA_CLAUSE_RE = re.compile(
    r"(?:,\s*)?(?:with|using)?\s*(?:type-locked|type locked|lock types|lock type)\s+schema\b"
    r"|(?:,\s*)?(?:with|using)?\s*pause\s+on\s+(?:schema\s+)?change\b",
    re.IGNORECASE,
)


def _strip_transfer_tail(dest: str) -> tuple[str, str]:
    """Split "Warehouse as upsert" into the connector and the sync mode."""
    from .transfer_tools import normalize_sync_mode

    # A trailing clause ("…to wh, is that safe?") is commentary, not a name.
    dest = re.split(r"[,;?]", dest, maxsplit=1)[0].strip()
    dest = re.sub(
        r"\s+(?:now|please|thanks|thank\s+you|for\s+me|asap|right\s+now)\s*$",
        "",
        dest,
        flags=re.IGNORECASE,
    ).strip()
    tail = _TRANSFER_TAIL_RE.search(dest)
    if not tail:
        return dest.strip(), ""
    spoken = tail.group("mode").strip()
    mode = normalize_sync_mode(spoken, default="")
    if not mode:
        # "into orders in production" — the tail was part of the name.
        return dest.strip(), ""
    return dest[: tail.start()].strip(), mode


def parse_transfer_bind_and_rules(message: str) -> tuple[str, dict[str, Any]]:
    """Pull explicit contract / validation / schema posture from NL.

    Never invents a contract id. Never parses skip_preflight. Selecting a
    contract defaults require-signed. ``migrate`` / data-or-migration-rules
    language selects strict validation — the Studio fail-fast bar — without
    relaxing schema_policy.
    """
    extras: dict[str, Any] = {}
    original = str(message or "")
    extras.update(parse_transfer_rule_posture(original.lower()))
    extras.update(parse_transfer_schema_posture(original.lower()))
    text = original
    hit = _CONTRACT_CLAUSE_RE.search(text) or _CONTRACT_ID_BARE_RE.search(text)
    if hit:
        cid = str(hit.group("cid") or "").strip()
        if cid and cid.lower() not in _CONTRACT_ID_STOP:
            extras["contract_id"] = cid
            extras["require_signed_contract"] = True
            text = f"{text[:hit.start()]} {text[hit.end():]}"
    text = _VALIDATION_CLAUSE_RE.sub(" ", text)
    text = _RULES_CLAUSE_RE.sub(" ", text)
    text = _SCHEMA_CLAUSE_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, extras


def resolve_transfer_bind_kwargs(
    *texts: str,
    contract_id: str = "",
    require_signed_contract: Any = None,
    validation_mode: str = "",
    schema_policy: str = "",
) -> dict[str, Any]:
    """Merge spoken + explicit bind / data-rule posture. Never invents a bind.

    Explicit kwargs win when set. Unbound leaves the dict without
    ``contract_id`` so ``enforce_contract`` stays unset. ``skip_preflight``
    and ``propagate_all`` are never returned.
    """
    parsed: dict[str, Any] = {}
    for blob in texts:
        if blob:
            parsed.update(parse_transfer_bind_and_rules(str(blob))[1])
    out: dict[str, Any] = {}
    cid = str(contract_id or parsed.get("contract_id") or "").strip()
    if cid:
        out["contract_id"] = cid
        if require_signed_contract is None:
            out["require_signed_contract"] = bool(parsed.get("require_signed_contract", True))
        else:
            out["require_signed_contract"] = bool(require_signed_contract)
    mode = str(validation_mode or parsed.get("validation_mode") or "").strip()
    if mode in {"strict", "balanced", "lenient"}:
        out["validation_mode"] = mode
    policy = str(schema_policy or parsed.get("schema_policy") or "").strip()
    if policy in {"manual_review", "type_locked", "pause_on_change"}:
        out["schema_policy"] = policy
    return out


def parse_transfer_rule_posture(lowered: str) -> dict[str, Any]:
    """Explicit validation posture. Empty when the tool default should stand."""
    if any(w in lowered for w in ("lenient", "permissive", "best effort", "best-effort")):
        return {"validation_mode": "lenient"}
    if any(w in lowered for w in (
        "strict", "zero loss", "zero-loss", "fail fast", "fail-fast",
        "data rules", "migration rules", "migrate", "migration",
    )):
        return {"validation_mode": "strict"}
    if "balanced" in lowered:
        return {"validation_mode": "balanced"}
    return {}


def parse_transfer_schema_posture(lowered: str) -> dict[str, Any]:
    """Explicit schema policy only — never invent propagate_all from chat."""
    if "pause on" in lowered and "change" in lowered:
        return {"schema_policy": "pause_on_change"}
    if any(w in lowered for w in ("type-locked", "type locked", "lock types", "lock type")):
        return {"schema_policy": "type_locked"}
    return {}


# An object named explicitly: "table users" / "the users table" / "collection events".
_TABLE_PREFIX_RE = re.compile(
    r"\b(?:from\s+|of\s+|in\s+|for\s+)?(?:the\s+)?(?:table|collection|dataset)\s+"
    r"[`\"']?(?P<named>[A-Za-z_][\w.$]*)[`\"']?",
    re.IGNORECASE,
)
_TABLE_SUFFIX_RE = re.compile(
    r"\b(?:the\s+)?[`\"']?(?P<named>[A-Za-z_][\w.$]*)[`\"']?\s+(?:table|collection)\b",
    re.IGNORECASE,
)
_ALL_TABLES_RE = re.compile(
    r"\b(?:all|every|each)\s+(?:the\s+)?(?:tables?|collections?)\b"
    r"|\b(?:whole|entire|full)\s+(?:database|schema|db)\b",
    re.IGNORECASE,
)
_ROUTE_FROM_TO_RE = re.compile(
    r"\b(?:from|out\s+of)\s+(?P<src>.+?)\s+(?:to|into|onto|over\s+to|->)\s+(?P<dst>.+?)"
    r"(?=[,;?]|$)",
    re.IGNORECASE,
)
_ROUTE_TO_FROM_RE = re.compile(
    r"\b(?:to|into|onto)\s+(?P<dst>.+?)\s+(?:from|out\s+of)\s+(?P<src>.+?)(?=[,;?]|$)",
    re.IGNORECASE,
)
_ROUTE_ARROW_RE = re.compile(
    r"(?P<src>[A-Za-z0-9_][\w .\-]{0,48}?)\s*->\s*(?P<dst>[A-Za-z0-9_][\w .\-]{0,48})",
    re.IGNORECASE,
)
_TRANSFER_VERB_RE = re.compile(rf"\b(?:{_TRANSFER_VERBS}|moving)\b", re.IGNORECASE)
_BARE_OBJECT_WORDS = frozenset({
    "data", "rows", "records", "everything", "all", "it", "them", "tables",
    "table", "stuff", "things",
})


def _extract_named_table(text: str) -> tuple[str, str]:
    """Pull an explicitly named object out of the route text.

    "transfer data from sql to postgres from table users" states the table
    after the destination — reading the tail as part of the destination's name
    is why that phrasing used to resolve to nothing at all.
    """
    for pattern in (_TABLE_PREFIX_RE, _TABLE_SUFFIX_RE):
        for match in pattern.finditer(text or ""):
            name = match.group("named").strip()
            # "transfer table users" — the verb is not the object being moved.
            if name.lower() in _BARE_OBJECT_WORDS or _TRANSFER_VERB_RE.fullmatch(
                name.lower()
            ):
                continue
            stripped = f"{text[:match.start()]} {text[match.end():]}"
            return name, re.sub(r"\s+", " ", stripped).strip().strip(",;").strip()
    return "", (text or "").strip()


def _extract_route_endpoints(text: str) -> tuple[str, str, str]:
    """Return ``(source, destination, sync_mode)`` from route-only text."""
    route = (
        _ROUTE_FROM_TO_RE.search(text or "")
        or _ROUTE_TO_FROM_RE.search(text or "")
        or _ROUTE_ARROW_RE.search(text or "")
    )
    if not route:
        return "", "", ""
    src = _capture_connector_name(route.group("src"))
    dst, mode = _strip_transfer_tail(route.group("dst").strip().strip("\"'"))
    return src, _capture_connector_name(dst), mode


def _asks_for_schema(lower: str) -> bool:
    """True when the operator actually asked to see a schema, not just a transfer."""
    return bool(
        re.search(r"\b(?:schema|columns|column list|describe|ddl|data ?types)\b", lower)
        and re.search(r"\b(?:show|list|what|describe|see|get|print|introspect)\b", lower)
    )


# Wording that turns a transfer request into a standing one. Only consulted when
# a transfer route was already parsed, so "show my schedules" cannot reach it.
_SCHEDULE_INTENT_RE = re.compile(
    r"\b(?:schedule|scheduled|scheduling|automate|automated|recurring|"
    r"repeat|repeatedly|on\s+a\s+schedule)\b",
    re.IGNORECASE,
)


def parse_transfer_intent(message: str) -> dict | None:
    """Extract source/destination/table/data rules from a transfer request.

    Rule clauses are parsed and removed *before* the route is read, so
    "…to Warehouse, only rows where status = active, upsert on id" resolves to
    the connector **Warehouse** and a filter — not to a connector whose name is
    the rest of the sentence. Rules the engine cannot apply come back as
    questions on the intent; the caller must refuse rather than move data the
    operator did not ask for.
    """
    cleaned, extras = parse_transfer_bind_and_rules(normalize_operator_typos(message))
    cleaned, rules = parse_transfer_data_rules(cleaned)
    extras.update(rules.as_intent_fields())
    text = cleaned.strip()
    if not text:
        return None
    match = (
        _TRANSFER_RE.search(text)
        or _TRANSFER_TO_FROM_RE.search(text)
        or _TRANSFER_ARROW_RE.search(text)
    )
    _matched_table = match.group("table").strip().lower() if match else ""
    if match and (
        _matched_table in _BARE_OBJECT_WORDS
        or _TRANSFER_VERB_RE.fullmatch(_matched_table)
    ):
        # "transfer data from A to B from table users" — the object is named
        # elsewhere in the sentence, so re-read the route without it.
        match = None
    if not match:
        named, route_text = _extract_named_table(text)
        if named and _TRANSFER_VERB_RE.search(text):
            src, dst, mode = _extract_route_endpoints(route_text)
            if dst:
                lowered = text.lower()
                return {
                    "source_table": named,
                    "source_connector_name": src[:80],
                    "dest_connector_name": dst[:80],
                    "sync_mode": mode or normalize_sync_mode_for_message(lowered),
                    # No source named, or a rule we could not apply: plan, never mutate.
                    "plan_only": (
                        not src
                        or rules.blocking
                        or any(w in lowered for w in _PLAN_ONLY_WORDS)
                    ),
                    **extras,
                }
        if _ALL_TABLES_RE.search(text) and _TRANSFER_VERB_RE.search(text):
            src, dst, mode = _extract_route_endpoints(text)
            if src or dst:
                return {
                    "source_table": "",
                    "source_connector_name": src[:80],
                    "dest_connector_name": dst[:80],
                    "sync_mode": mode,
                    "all_tables": True,
                    "plan_only": True,
                    **extras,
                }
        # Table + destination only — still stage a plan so Confirm/clarify can ask source.
        soft = _TRANSFER_TO_ONLY_RE.search(text)
        if soft and not re.search(r"\bfrom\b|\bout\s+of\b", text, re.I):
            table = soft.group("table").strip()
            dest, mode = _strip_transfer_tail(soft.group("dst").strip().strip("\"'"))
            if table and dest and table.lower() not in {
                "data", "rows", "records", "everything", "all", "it", "them",
            }:
                lowered = text.lower()
                if not mode:
                    mode = normalize_sync_mode_for_message(lowered)
                return {
                    "source_table": table,
                    "source_connector_name": "",
                    "dest_connector_name": dest[:80],
                    "sync_mode": mode,
                    "plan_only": True,  # missing source → plan/clarify, never mutate
                    **extras,
                }
        return None
    table = match.group("table").strip()
    source = match.group("src").strip().strip("\"'")
    dest, mode = _strip_transfer_tail(match.group("dst").strip().strip("\"'"))
    if not table or not source or not dest:
        return None
    # Bare "data/rows/records" is not a real table — ask or plan the route instead.
    if table.lower() in _BARE_OBJECT_WORDS:
        return None
    lowered = text.lower()
    if not mode:
        mode = normalize_sync_mode_for_message(lowered)
    return {
        "source_table": table,
        "source_connector_name": source[:80],
        "dest_connector_name": dest[:80],
        "sync_mode": mode,
        # Asking what a transfer *would* do must never stage a mutation, and
        # neither may a request carrying a rule we could not apply.
        "plan_only": rules.blocking or any(w in lowered for w in _PLAN_ONLY_WORDS),
        **extras,
    }


def normalize_sync_mode_for_message(lowered: str) -> str:
    from .transfer_tools import normalize_sync_mode

    for phrase in ("overwrite", "replace", "truncate", "upsert", "merge", "append", "cdc"):
        if phrase in lowered:
            return normalize_sync_mode(phrase, default="")
    return ""


# High-frequency operator misspellings. A typo must not cost the operator the
# whole intent: "tranfer" is still a transfer.
_TYPO_FIXES: tuple[tuple[str, str], ...] = (
    (r"\btra?ns?fe?r\b", "transfer"),
    (r"\btrasfer\b", "transfer"),
    (r"\bmigra?te?\b", "migrate"),
    (r"\bschdule\b", "schedule"),
    (r"\bmny\b", "many"),
    (r"\btbls?\b", "tables"),
    (r"\bcnt\b", "count"),
    (r"\bconnectorz\b", "connectors"),
    (r"\bdbs\b", "databases"),
    (r"\bpostgress?ql\b", "postgresql"),
    (r"\bposgres\b", "postgres"),
)


def normalize_operator_typos(message: str) -> str:
    """Repair common misspellings before any intent parsing."""
    text = message or ""
    for pattern, replacement in _TYPO_FIXES:
        text = re.sub(pattern, replacement, text, flags=re.I)
    return text


def infer_tools_from_message(message: str) -> list[tuple[str, dict]]:
    """Local tool routing when no LLM tool-use is available."""
    message = normalize_operator_typos(message)
    lower = message.lower()
    planned: list[tuple[str, dict]] = []

    # Delete/export/schedule paraphrases must not plan list/search tools —
    # "Warehouse connector" otherwise matches connector inventory routing.
    if _looks_like_unsupported_mutation(lower):
        return []

    if _is_meta_pilot_question(lower):
        planned.append(("describe_pilot", {}))
        return planned

    from .dialogue_acts import classify_dialogue_act

    _act = classify_dialogue_act(message)
    # Sitrep asks own a dedicated tool. Inventory verbs ("show my jobs") and
    # named objects (job_/pf_) keep their existing routers.
    if _act == "briefing" and not re.search(
        r"\b(?:job_|pf_)[a-z0-9]"
        r"|(?:show|list|open)\s+(?:my\s+)?(?:jobs?|pipelines?|schedules?|connectors?)\b"
        r"|(?:plan|start|stage)\s+transfer\b"
        r"|\bsample\b|\bcount\s+rows\b|\bfix\s+(?:my\s+)?mapping\b",
        lower,
    ):
        planned.append(("brief_workspace", {}))
        return planned

    nav_map = {
        "pilot": ["data pilot", "go to pilot", "open pilot"],
        "transfer": ["start transfer", "new transfer", "upload", "move data", "go to transfer", "transfer studio"],
        "jobs": ["show jobs", "my jobs", "job history", "recent transfers", "go to jobs", "transfer jobs", "show my transfer", "jobs"],
        "connectors": ["connectors", "connections", "add connector", "go to connectors"],
        "dashboard": ["dashboard", "overview", "home", "go home"],
        "schedules": ["pipelines", "schedules", "scheduled pipelines", "go to pipelines", "open pipelines"],
        "contracts": ["contracts", "data contracts", "go to contracts"],
        "query": ["query", "query playground", "sql playground", "go to query"],
        "settings": ["settings", "sso", "security settings", "settings screen"],
        "mcp": ["mcp", "go to mcp"],
        "docs": ["docs", "documentation", "help docs", "go to docs"],
        "benchmarks": ["proofs", "benchmarks", "go to proofs"],
    }
    nav_verbs = ("go", "open", "show", "take me", "navigate", "bring me", "need the", "i need")
    # Short "docs please" / "pipelines please" / "overview please"
    short_nav = {
        "docs": r"^\s*(?:docs|documentation)\s*(?:please|pls)?\s*[.?!]*$",
        "schedules": r"^\s*(?:pipelines|schedules)\s*(?:please|pls)?\s*[.?!]*$",
        "jobs": r"^\s*(?:jobs|job\s+history)\s*(?:please|pls)?\s*[.?!]*$",
        "connectors": r"^\s*(?:connectors|connections)\s*(?:please|pls)?\s*[.?!]*$",
        "dashboard": r"^\s*(?:overview|dashboard|home)\s*(?:please|pls)?\s*[.?!]*$",
        "transfer": r"^\s*(?:transfer(?:\s+studio)?)\s*(?:please|pls)?\s*[.?!]*$",
        "settings": r"^\s*settings\s*(?:please|pls)?\s*[.?!]*$",
        "mcp": r"^\s*(?:mcp(?:\s+(?:page|tools?|server))?)\s*(?:please|pls)?\s*[.?!]*$",
        "contracts": r"^\s*(?:contracts|data\s+contracts)(?:\s+screen)?\s*(?:please|pls)?\s*[.?!]*$",
        "query": r"^\s*(?:query(?:\s+playground)?|sql\s+playground)\s*(?:please|pls)?\s*[.?!]*$",
        "benchmarks": r"^\s*(?:proofs|benchmarks)\s*(?:please|pls)?\s*[.?!]*$",
    }
    for screen, pat in short_nav.items():
        if re.search(pat, lower):
            planned.append(("navigate", {"screen": screen}))
            break
    # Telegraphic "go pipelines" / "go contracts" (missing "to").
    if not planned:
        go_short = re.search(
            r"^\s*go\s+(?:to\s+)?"
            r"(pipelines?|schedules?|contracts?|connectors?|connections?|"
            r"jobs?|query|mcp|docs|proofs|benchmarks|settings|transfer|"
            r"overview|dashboard|home|pilot)\s*[.?!]*$",
            lower,
        )
        if go_short:
            token = go_short.group(1)
            screen = {
                "pipeline": "schedules", "pipelines": "schedules",
                "schedule": "schedules", "schedules": "schedules",
                "contract": "contracts", "contracts": "contracts",
                "connector": "connectors", "connectors": "connectors",
                "connection": "connectors", "connections": "connectors",
                "job": "jobs", "jobs": "jobs",
                "proof": "benchmarks", "proofs": "benchmarks",
                "benchmark": "benchmarks", "benchmarks": "benchmarks",
                "overview": "dashboard", "home": "dashboard", "dashboard": "dashboard",
            }.get(token, token)
            planned.append(("navigate", {"screen": screen}))
    if not planned:
        for screen, phrases in nav_map.items():
            label = screen.replace("_", " ")
            if re.search(
                rf"(go to|open|show|take me to|navigate to|bring me to|need the|i need the)\s+"
                rf"(?:the\s+)?{re.escape(label)}",
                lower,
            ):
                planned.append(("navigate", {"screen": screen}))
                break
            if screen == "schedules" and re.search(r"(go to|open|show|take me to|bring me to)\s+pipelines?", lower):
                planned.append(("navigate", {"screen": "schedules"}))
                break
            if screen == "contracts" and re.search(r"\bcontracts?\s+screen\b", lower):
                planned.append(("navigate", {"screen": "contracts"}))
                break
            if screen == "mcp" and re.search(r"\bmcp\s+tools?\b", lower):
                planned.append(("navigate", {"screen": "mcp"}))
                break
            if screen == "dashboard" and re.search(r"\b(?:go home|home to the overview|overview)\b", lower):
                if any(w in lower for w in ("go", "home", "overview", "bring", "take")):
                    planned.append(("navigate", {"screen": "dashboard"}))
                    break
            if any(p in lower for p in phrases) and any(w in lower for w in nav_verbs):
                # Avoid treating content asks ("show my jobs" inventory) as navigate when
                # list_* tools will handle them — only navigate for directional language.
                if screen in {"jobs", "connectors", "schedules"} and not any(
                    v in lower for v in ("go to", "take me", "navigate to", "bring me", "open ", "need the", "i need")
                ):
                    continue
                planned.append(("navigate", {"screen": screen}))
                break

    if any(
        w in lower
        for w in (
            "all datasets",
            "what data",
            "available data",
            "list datasets",
            "list my datasets",
            "show datasets",
            "show my datasets",
            "my datasets",
            "what files",
        )
    ):
        planned.append(("list_datasets", {}))

    setup = re.search(r"(?:set up|setup|configure|connect)\s+(.+?)\s+as\s+(?:a\s+)?(?:source|destination)", lower)
    if setup:
        q = setup.group(1).strip()
        role = "destination" if "destination" in lower else "source"
        planned.append(("search_connectors", {"query": q[:40], "role": role}))
    elif re.search(r"\bconnectors?\b|\bconnections?\b", lower) and not any(
        v in lower for v in ("go to", "take me", "navigate to")
    ):
        # "open connectors" is navigate; "find my postgres connector" is search.
        bare_open_connectors = bool(
            re.search(
                r"\b(?:go to|open|take me to|navigate to)\s+(?:the\s+)?(?:connectors?|connections?)\b",
                lower,
            )
        ) and not re.search(
            r"\b(?:find|search|locate)\b.+\b(?:connectors?|connections?)\b|"
            r"\b(?:connectors?|connections?)\b.+\b(?:named|called|for)\b",
            lower,
        )
        if bare_open_connectors and "find" not in lower and "search" not in lower:
            pass  # leave navigate alone
        elif any(
            w in lower
            for w in (
                "search", "find", "source", "destination", "setup",
                "postgres", "postgresql", "mysql", "mongo", "snowflake",
                "bigquery", "shopify", "warehouse",
            )
        ) and any(w in lower for w in ("search", "find", "locate", "which", "where")):
            role = (
                "destination" if "destination" in lower or "warehouse" in lower
                else "source" if "source" in lower
                else "all"
            )
            q = re.sub(r".*(?:search|find|locate)\s+", "", lower).strip() or lower
            q = re.sub(r"\b(?:my|the|a|an)\s+", " ", q)
            q = re.sub(r"\bconnectors?\b|\bconnections?\b", " ", q).strip()
            planned.append(("search_connectors", {"query": (q or "connector")[:40], "role": role}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "connectors")
            ]
        elif not bare_open_connectors:
            planned.append(("list_connectors", {}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "connectors")
            ]

    # Create / save connector from credentials pasted in chat
    from .connector_create import wants_create_connector

    # "find the postgres one" — engine named without the word connector
    if (
        "search_connectors" not in {n for n, _ in planned}
        and not wants_create_connector(message)
        and re.search(
            r"\b(?:find|search|locate)\b.+\b(?:postgres|postgresql|mysql|mongo|snowflake|bigquery)\b",
            lower,
        )
        and "connector" not in lower
        and "connection" not in lower
    ):
        eng = re.search(
            r"\b(postgres(?:ql)?|mysql|mongo(?:db)?|snowflake|bigquery)\b",
            lower,
        )
        if eng:
            planned.append(("search_connectors", {"query": eng.group(1), "role": "all"}))
            planned = [(n, a) for n, a in planned if n != "search_data"]

    if wants_create_connector(message):
        planned = [(n, a) for n, a in planned if n not in ("search_connectors", "list_connectors", "search_knowledge")]
        planned.append(("create_connector", {"message": message}))

    # Pipelines / schedules
    if any(w in lower for w in ("list schedules", "list pipelines", "my pipelines", "my schedules", "show pipelines", "show schedules", "show my pipelines", "show my schedules")):
        planned.append(("list_schedules", {"limit": 20}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "schedules")
        ]
    elif any(w in lower for w in ("pipeline", "schedule")) and any(w in lower for w in ("list", "show", "what")):
        if "run" not in lower and not any(v in lower for v in ("go to", "take me", "navigate to", "open ")):
            planned.append(("list_schedules", {"limit": 20}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "schedules")
            ]

    # Open / show a named pipeline (must win over bare "open pipelines" navigate).
    open_sched = re.search(
        r"\b(?:open|show|view|get)\s+(?:the\s+)?(?:schedule|pipeline)\s+(.+?)(?:\s+for\s+me)?\s*[.?!]*$",
        lower,
    ) or re.search(
        r"\b(?:open|show|view|get)\s+(.+?)\s+(?:schedule|pipeline)(?:\s+for\s+me)?\s*[.?!]*$",
        lower,
    ) or re.search(
        r"\b(?:can\s+you\s+|could\s+you\s+|please\s+)?open\s+(?:the\s+)?(.+?)\s+pipeline(?:\s+for\s+me)?\s*[.?!]*$",
        lower,
    ) or re.search(
        r"\b(?:details|detail|info)\s+(?:on|for|about)\s+(?:the\s+)?(?:schedule|pipeline)\s+(.+?)\s*$",
        lower,
    ) or re.search(
        r"\bshow\s+me\s+details\s+on\s+(?:schedule|pipeline)\s+(.+?)\s*$",
        lower,
    )
    if not open_sched and re.search(r"\b(?:details|detail|info)\b", lower) and not re.search(
        r"\b(?:connector|job|transfer|table|schema|dataset|mapping)\b", lower
    ):
        # "details about Nightly Orders" / "Nightly Orders details"
        open_sched = re.search(
            r"\b(?:details|detail|info)\s+(?:on|for|about)\s+(?:the\s+)?(.+?)\s*$",
            lower,
        ) or re.search(
            r"^\s*(.+?)\s+(?:details|detail|info)\s*$",
            lower,
        )
    if not open_sched and re.search(
        r"^\s*(?:show|view|open)\s+(?:the\s+)?([A-Za-z][A-Za-z0-9_\-]{1,40}(?:\s+[A-Za-z][A-Za-z0-9_\-]{0,40}){0,3})\s*$",
        lower,
    ) and not re.search(
        r"\b(?:connector|job|transfer|table|schema|dataset|mapping|failed|"
        r"sample|data|from|on|in|rows|columns|me|some|a\s+sample|"
        r"pipelines?|schedules?|jobs?|connectors?|contracts?|proofs?)\b",
        lower,
    ):
        # "show Nightly Orders" — short title, no sample/table language
        open_sched = re.search(
            r"^\s*(?:show|view|open)\s+(?:the\s+)?([A-Za-z][A-Za-z0-9_\-]{1,40}"
            r"(?:\s+[A-Za-z][A-Za-z0-9_\-]{0,40}){0,3})\s*$",
            lower,
        )
    if open_sched and "run" not in lower and "trigger" not in lower and "kick" not in lower:
        sched_name = _clean_connector_phrase(open_sched.group(1))
        sched_name = re.sub(
            r"^(?:the\s+|my\s+|a\s+|an\s+)?(?:schedule|pipeline)\s+",
            "",
            sched_name,
            flags=re.I,
        ).strip()
        sched_name = re.sub(r"\s+(?:schedule|pipeline)$", "", sched_name, flags=re.I).strip()
        if sched_name and sched_name not in {"now", "it", "this", "that", "list", "all"}:
            sched_name = re.sub(r"^(?:the|my|a|an)\s+", "", sched_name, flags=re.I).strip()
            tool = "open_schedule" if any(w in lower for w in ("open", "view")) else "get_schedule"
            if "detail" in lower or "info" in lower:
                tool = "get_schedule"
            planned.append((tool, {"name": sched_name}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "schedules")
                and n != "sample_connector_object"
            ]

    run_sched = re.search(
        r"(?:run|trigger|execute|kick\s*off|start|fire|schdule|schedule)\s+(?:schedule|pipeline)\s+(.+?)(?:\s+(?:right\s+)?now)?\s*$",
        lower,
    ) or re.search(
        r"(?:run|trigger|execute|kick\s*off|fire|schdule)\s+(.+?)\s+(?:schedule|pipeline)(?:\s+(?:right\s+)?now)?\s*$",
        lower,
    ) or re.search(
        r"(?:run|trigger|execute|kick\s*off|fire|schdule)\s+(?:my\s+)?(.+?)\s+(?:pipeline|schedule)\s*$",
        lower,
    ) or re.search(
        r"(?:run|trigger|execute|kick\s*off|fire|schdule|schedule)\s+(.+?)\s+(?:right\s+)?now\s*$",
        lower,
    ) or re.search(
        r"(?:please\s+)?(?:run|trigger|execute|kick\s*off|fire|schdule)\s+(?:the\s+)?"
        r"(.+?)(?:\s+(?:pipeline|schedule))?(?:\s+(?:immediately|right\s+now|now))?\s*$",
        lower,
    ) or re.search(
        # "fire Nightly Orders" / "execute Nightly" (name-only, no now/pipeline word)
        r"^\s*(?:fire|execute|trigger|kick\s*off)\s+(?:the\s+|my\s+)?"
        r"([A-Za-z][A-Za-z0-9_\- ]{1,48}?)\s*$",
        lower,
    ) or re.search(
        # "do the nightly one now" / "do nightly now"
        r"^\s*(?:do|run)\s+(?:the\s+)?([A-Za-z][A-Za-z0-9_\- ]{1,40}?)"
        r"(?:\s+one)?\s+(?:right\s+)?now\s*$",
        lower,
    )
    if any(
        w in lower
        for w in (
            "run now", "run schedule", "run pipeline", "trigger schedule", "trigger pipeline",
            "run my", "execute schedule", "execute pipeline", "kick off", "kickoff",
            "schdule", "fire ",
        )
    ) or (
        re.search(r"\b(?:run|trigger|execute|kick\s*off|fire|schdule)\b.+\b(?:now|pipeline|schedule|immediately)\b", lower)
        and "sql" not in lower
        and "query" not in lower
    ) or (
        re.search(r"\bschedule\b.+\bnow\b", lower)
        and "every" not in lower
        and "automatically" not in lower
        and "create" not in lower
        and "sql" not in lower
    ) or (
        re.search(r"\b(?:trigger|kick\s*off|fire)\s+[a-zA-Z]", lower)
        and "sql" not in lower
        and "query" not in lower
    ) or (
        re.search(r"^\s*execute\s+[A-Za-z]", lower)
        and "sql" not in lower
        and "query" not in lower
        and "select" not in lower
    ) or (
        re.search(r"^\s*(?:do|run)\s+(?:the\s+)?[a-z].+\bnow\b", lower)
        and "sql" not in lower
        and "every" not in lower
    ):
        name = ""
        if run_sched:
            name = _clean_connector_phrase(run_sched.group(1))
            # Strip filler words from the schedule title capture.
            name = re.sub(
                r"^(?:the\s+|my\s+|a\s+|an\s+)?(?:schedule|pipeline)\s+",
                "",
                name,
                flags=re.I,
            ).strip()
            name = re.sub(r"^(?:my\s+|the\s+|a\s+|an\s+)", "", name, flags=re.I).strip()
            name = re.sub(r"\s+(?:schedule|pipeline)$", "", name, flags=re.I).strip()
            name = re.sub(r"\s+(?:right\s+)?now$", "", name, flags=re.I).strip()
            name = re.sub(r"\s+immediately$", "", name, flags=re.I).strip()
            name = re.sub(r"\s+one$", "", name, flags=re.I).strip()
        if name.lower() in {"now", "it", "this", "that", "schedule", "pipeline", "my", "the"}:
            name = ""
        planned.append(("run_schedule_now", {"name": name} if name else {}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "schedules")
        ]

    # Pause / stop / resume / clone — no mutate tools yet; open the named pipeline in UI.
    manage_sched = None
    if not re.search(
        r"\b(?:change\s+data\s+capture|cdc|sync\s+mode|write\s+mode|upsert|append)\b",
        lower,
    ):
        manage_sched = re.search(
            r"\b(?:pause|stop|disable|resume|clone|duplicate)\s+"
            r"(?:the\s+)?(?:schedule|pipeline)\s+(.+?)\s*$",
            lower,
        ) or re.search(
            r"\b(?:pause|stop|disable|resume|clone|duplicate)\s+"
            r"(?:the\s+)?(.+?)\s+(?:schedule|pipeline)\s*$",
            lower,
        ) or re.search(
            r"\b(?:pause|stop|disable|resume|clone|duplicate)\s+"
            r"(?:the\s+)?([A-Za-z][A-Za-z0-9_\- ]{1,48}?)\s*$",
            lower,
        )
    if manage_sched and "run_schedule_now" not in {n for n, _ in planned}:
        mname = _clean_connector_phrase(manage_sched.group(1))
        mname = re.sub(r"\s+(?:schedule|pipeline)$", "", mname, flags=re.I).strip()
        if mname and mname not in {"now", "it", "this", "that", "list", "all", "change"}:
            planned.append(("open_schedule", {"name": mname}))
            planned.append(("navigate", {"screen": "schedules"}))
            planned = [(n, a) for n, a in planned if n != "sample_connector_object"]

    # Inventory of signed contracts — not "what is a data contract" (definition).
    if (
        re.search(
            r"\b(?:list|show|my)\s+(?:data\s+)?contracts?\b"
            r"|\b(?:what|which)\s+(?:data\s+)?contracts?\s+(?:do\s+i|do\s+we|are\s+(?:there|mine|signed))\b"
            r"|\b(?:data\s+)?contracts?\s+(?:i\s+have|we\s+have|in\s+(?:the|this)\s+workspace)\b",
            lower,
        )
        and not re.search(r"\bwhat\s+is\s+(?:a\s+)?data\s+contract\b", lower)
    ):
        if not any(v in lower for v in ("go to", "take me", "navigate to", "open ")):
            planned.append(("list_contracts", {"limit": 50}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "contracts")
            ]

    # List jobs when the operator wants contents — not when they only want the screen.
    _nav_only = any(v in lower for v in ("go to", "take me to", "navigate to", "open "))
    # Platform inventory — Datawrap's own jobs/connectors, never warehouse tables.
    # "how many jobs failed" must list jobs, not SELECT COUNT(*) FROM jobs.
    _platform_job_inventory = bool(
        re.search(
            r"\b(?:how many\s+jobs|jobs?\s+(?:that\s+)?failed|failed\s+jobs|job\s+failures|"
            r"failed\s+transfers|how many\s+transfers\s+failed|jobs?\s+that\s+failed)\b",
            lower,
        )
    ) and not re.search(r"\bon\s+[a-z0-9]", lower)
    if (not _nav_only) and (
        _platform_job_inventory
        or any(
            p in lower
            for p in (
                "list jobs",
                "my jobs",
                "show my jobs",
                "show jobs",
                "recent transfers",
                "job history",
                "show my transfers",
                "list transfers",
            )
        )
        or (
            any(w in lower for w in ("jobs", "transfers", "history"))
            and any(w in lower for w in ("list", "show", "recent", "my"))
            and "dataset" not in lower
        )
    ):
        planned.append(("list_jobs", {"limit": 10}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "jobs")
        ]

    _platform_connector_inventory = bool(
        re.search(r"\b(?:how many\s+connectors|connector\s+count)\b", lower)
    ) and not re.search(r"\bon\s+[a-z0-9]", lower)
    if _platform_connector_inventory or re.search(
        r"\b(?:list|show|what(?:'s|s)?)\s+(?:my\s+)?(?:databases|dbs)\b",
        lower,
    ) or re.search(r"\b(?:can\s+u|can\s+you|could\s+you)\s+list\s+(?:my\s+)?(?:databases|dbs)\b", lower):
        planned.append(("list_connectors", {}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "connectors")
        ]

    pf_match = re.search(r"\bpf_[a-f0-9]{8,}\b", lower)
    if pf_match or any(
        w in lower
        for w in (
            "preflight run",
            "validation run",
            "why did validate",
            "why validation failed",
            "why did validation",
            "validate fail",
            "validation fail",
        )
    ):
        if pf_match:
            planned.append(("get_preflight_run", {"run_id": pf_match.group(0)}))
        else:
            # No pf_ id — show recent jobs so the operator can pick a failed run.
            planned.append(("list_jobs", {"limit": 8}))
            planned.append(("remediate_validation", {"kind": "open_bad_data_fix"}))

    job_match = re.search(r"\b([a-f0-9]{24})\b", lower) or re.search(r"\b(job_[a-z0-9_-]{6,})\b", lower)
    if job_match and not (pf_match and job_match.group(1) == pf_match.group(0)):
        jid = job_match.group(1)
        if any(w in lower for w in ("open", "show", "go to", "take me")):
            planned.append(("open_job", {"job_id": jid}))
        else:
            planned.append(("get_job", {"job_id": jid}))
    elif any(
        w in lower
        for w in (
            "why did this job fail",
            "why did the transfer fail",
            "why did the last transfer fail",
            "why did last transfer fail",
            "why did my transfer fail",
            "why did my job fail",
            "last transfer fail",
            "job failed",
            "transfer failed",
            "analyze job",
            "open my last job",
            "show my last job",
            "last job",
            "status of my last transfer",
            "status of the last transfer",
            "status of last transfer",
            "get job details",
            "job details",
            "last transfer status",
        )
    ) or re.search(r"\bwhy\s+did\s+(?:the\s+)?(?:last\s+)?(?:transfer|job)\s+fail\b", lower) or re.search(
        r"\b(?:status|details?)\s+of\s+(?:my\s+|the\s+)?(?:last\s+)?(?:transfer|job)\b",
        lower,
    ) or re.search(r"\b(?:open|show|get)\s+(?:my\s+|the\s+)?last\s+(?:job|transfer)\b", lower):
        planned.append(("list_jobs", {"limit": 5}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "jobs")
        ]

    if any(w in lower for w in (
        "strip control", "strip controls", "fix bad data", "format-control",
        "normalize control", "quarantine bad", "heal quarantine", "heal the quarantine",
        "repair bad", "repair quarantine", "fix quarantine", "open bad data",
        "quarantine the bad", "bad rows please",
    )) or re.search(r"\bheal\s+(?:the\s+)?quarantine\b", lower):
        kind = "open_bad_data_fix"
        if any(w in lower for w in ("strip control", "normalize control", "format-control")):
            kind = "normalize_control_chars"
        elif any(w in lower for w in ("quarantine", "heal quarantine", "heal the quarantine")) or re.search(
            r"\bheal\s+(?:the\s+)?quarantine\b", lower
        ):
            kind = "quarantine_and_rerun"
        planned.append(("remediate_validation", {"kind": kind}))
        planned.append(("navigate", {"screen": "transfer"}))

    # Mapping review — open Map step, don't invent column rewrites in chat.
    if (
        re.search(r"\b(?:fix|review|repair)\s+(?:the\s+)?(?:mapping|mappings|map)\b", lower)
        or re.search(r"\bmapping\s+(?:looks?\s+)?(?:wrong|broken|bad)\b", lower)
        or re.search(r"\bhelp\s+(?:me\s+)?fix\s+(?:it|my\s+mapping|the\s+mapping)\b", lower)
        or re.search(r"\b(?:what'?s|whats|what is)\s+wrong\s+with\s+(?:the\s+|my\s+)?mapping\b", lower)
        or re.search(r"\bwrong\s+with\s+(?:the\s+|my\s+)?mapping\b", lower)
        or re.search(r"\bmapping\s+is\s+(?:incorrect|wrong|broken|bad)\b", lower)
        or re.search(r"\bmy\s+mapping\s+is\s+(?:incorrect|wrong|broken|bad)\b", lower)
    ):
        if "remediate_validation" not in {n for n, _ in planned}:
            planned.append(("remediate_validation", {"kind": "review_mappings"}))
            planned.append(("navigate", {"screen": "transfer"}))

    if any(w in lower for w in (
        "new transfer", "start a transfer", "open transfer studio",
        "start transfer studio", "open the transfer studio", "launch transfer studio",
    )) and not any(p[0] == "navigate" for p in planned):
        planned.append(("start_transfer_studio", {}))

    if any(w in lower for w in ("capabilities", "what can transfer", "supported", "any to any")):
        planned.append(("get_transfer_capabilities", {}))

    transfer_intent = parse_transfer_intent(message)
    if transfer_intent:
        plan_only = transfer_intent.pop("plan_only", False)
        transfer_intent = {k: v for k, v in transfer_intent.items() if v}
        # A cadence ("nightly at 2am") or an explicit "schedule this" is a
        # standing instruction, not a single run — staging one run instead would
        # move the data once and quietly never again.
        recurring = bool(transfer_intent.get("cadence")) or (
            bool(_SCHEDULE_INTENT_RE.search(lower))
            and "run_schedule_now" not in {n for n, _ in planned}
        )
        if recurring and not plan_only:
            planned.append(("create_schedule", {
                k: v for k, v in transfer_intent.items() if k != "all_tables"
            }))
        else:
            planned.append(("plan_transfer" if plan_only else "start_transfer", transfer_intent))

    if not transfer_intent and any(
        w in lower
        for w in (
            "plan transfer", "transfer plan", "route plan", "plan a route", "plan route",
            "plan the route", "route from", "good route", "moving data", "move from",
            "migrate from", "move data", "copy data", "sync data", "cdc from",
            "copy from", "sync from", "replicate from", "out of", "into snowflake",
            "into mysql", "into warehouse",
        )
    ):
        cleaned, extras = parse_transfer_bind_and_rules(message)
        route_text = cleaned.lower()
        src, dst = "", ""
        route = re.search(
            r"(?:from|source|out of)\s+(.+?)\s+(?:to|into|->|destination)\s+(.+?)\s*$",
            route_text,
        ) or re.search(
            r"move\s+(?:data|rows|records)?\s*(?:from\s+)?(.+?)\s+(?:to|into)\s+(.+?)\s*$",
            route_text,
        ) or re.search(
            r"(?:cdc|copy|sync|replicate)\s+(?:data\s+)?(?:from\s+)?(.+?)\s+(?:to|into)\s+(.+?)\s*$",
            route_text,
        ) or re.search(
            r"(?:plan|route)\s+(?:a\s+|the\s+)?(?:route\s+)?(?:from\s+)?(.+?)\s+(?:to|into)\s+(.+?)\s*$",
            route_text,
        ) or re.search(
            r"(?:moving|move)\s+(?:data\s+)?(?:out\s+of\s+)?(.+?)\s+(?:into|to)\s+(.+?)\s*$",
            route_text,
        ) or re.search(
            r"go\s+from\s+(.+?)\s+into\s+(.+?)\s*$",
            route_text,
        )
        if route:
            src = _capture_connector_name(route.group(1))
            dst = _capture_connector_name(route.group(2))
        planned.append(("plan_transfer_route", {
            "source": src or cleaned[:80] or message[:80],
            "destination": dst or cleaned[-80:] or message[-80:],
            "workload": "cdc" if "cdc" in lower else "unknown",
            **{k: v for k, v in extras.items() if v},
        }))
        # Prefer route planning over a bare sync-mode recommendation.
        planned = [(n, a) for n, a in planned if n != "recommend_sync_mode"]

    if any(w in lower for w in ("mapping algorithm", "mapping guarantee", "100% accuracy", "correct columns", "assurance", "how does mapping")):
        planned.append(("explain_mapping_assurance", {}))

    # Sync-mode recommendation only when the operator asks to choose/recommend a mode —
    # not for "what is upsert" / bare CDC how-tos (those are product FAQ).
    _sync_howto = bool(
        re.search(r"\b(?:what is|what'?s|explain|tell me about|how does|how do)\b", lower)
    ) and any(
        w in lower
        for w in (
            "upsert", "cdc", "incremental", "sync mode", "merge", "full refresh",
            "append mode", "append",
        )
    )
    if _sync_howto:
        planned.append(("explain_product", {"query": message[:240]}))
    elif (
        "plan_transfer_route" not in {n for n, _ in planned}
        and "start_transfer" not in {n for n, _ in planned}
        and "plan_transfer" not in {n for n, _ in planned}
    ) and (
        any(
            w in lower
            for w in (
                "which sync mode",
                "what sync mode",
                "what write mode",
                "which write mode",
                "recommend sync",
                "best sync mode",
                "choose sync",
                "sync mode for",
                "should i use cdc",
                "should i use upsert",
                "enable change data capture",
                "enable cdc",
                "start cdc",
                "turn on cdc",
                "make it upsert",
                "make it cdc",
                "make it append",
                "switch to cdc",
                "switch to upsert",
                "switch to append",
                "use upsert",
                "use cdc",
                "use append",
            )
        ) or (
            any(w in lower for w in ("sync mode", "write mode", "cdc", "incremental", "dedupe", "full refresh", "upsert", "merge"))
            and any(w in lower for w in ("recommend", "suggest", "choose", "which", "best for", "should", "enable", "use", "make it", "switch"))
        )
    ):
        planned.append(("recommend_sync_mode", {
            "workload": message[:120],
            "has_cursor": "cursor" in lower or "updated_at" in lower or "timestamp" in lower,
            "has_primary_key": (
                "primary key" in lower
                or "primary_key" in lower
                or bool(re.search(r"\bpk\b", lower))
            ),
            "needs_history": "history" in lower or "audit" in lower,
        }))

    if any(w in lower for w in ("schema drift", "schema change", "new column", "removed column", "type change", "schema policy")):
        change_type = "unknown"
        if "new column" in lower:
            change_type = "new_column"
        elif "removed column" in lower:
            change_type = "removed_column"
        elif "type change" in lower:
            change_type = "type_change"
        planned.append(("inspect_schema_policy", {"change_type": change_type, "auto_apply": "auto" in lower}))
        # Don't also mis-parse "schema drift on orders" as table=drift.
        # (introspect patterns below would otherwise steal it)

    # Live DB schema (saved connectors) — not uploaded datasets
    # Skip when we already scheduled schema-policy inspection.
    _policy_planned = any(n == "inspect_schema_policy" for n, _ in planned)
    schema_of = None if _policy_planned else re.search(
        r"(?:schema|columns|structure)\s+(?:of|for|on)\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using|living\s+on)\s+(.+))?$",
        lower,
    ) or re.search(
        r"(?:i\s+need\s+)?(?:the\s+)?schema\s+for\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using|living\s+on)\s+(.+))?$",
        lower,
    )
    columns_on = None if _policy_planned else re.search(
        r"(?:what\s+)?columns\s+(?:are\s+)?(?:on|in|for)\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    ) or re.search(
        r"break\s+down\s+the\s+columns?\s+for\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    ) or re.search(
        # "what columns does orders have on sales"
        r"(?:what\s+)?columns\s+does\s+([a-zA-Z0-9_.-]+)\s+have"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    ) or re.search(
        r"what\s+(?:are\s+the\s+)?columns\s+(?:of|for|in)\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    )
    describe_table = None if _policy_planned else re.search(
        r"describe\s+(?:table\s+)?([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    )
    # Natural paraphrases: "what's the airports table look like in Local Postgres"
    table_look = None if _policy_planned else (
        re.search(
            r"(?:what(?:'s| is)|show)\s+(?:the\s+)?"
            r"([a-zA-Z0-9_.-]+)\s+table"
            r"(?:\s+look(?:s)?\s+like)?"
            r"(?:\s+(?:on|in|from|using)\s+(.+))$",
            lower,
        ) or re.search(
            r"look(?:s)?\s+like\s+(?:the\s+)?([a-zA-Z0-9_.-]+)\s+(?:table|schema)"
            r"(?:\s+(?:on|in|from|using)\s+(.+))$",
            lower,
        ) or re.search(
            r"(?:table|schema)\s+([a-zA-Z0-9_.-]+)"
            r"\s+(?:on|in|from|using)\s+(.+)$",
            lower,
        )
    )
    if schema_of or columns_on or describe_table or table_look:
        m = schema_of or columns_on or describe_table or table_look
        table = (m.group(1) or "").strip()
        # Reject inventory / policy nouns mistaken as table names
        if table.lower() not in {
            "drift", "change", "policy", "tables", "collections", "list", "schema", "schemas",
        }:
            connector_name = ""
            if m.lastindex and m.lastindex >= 2 and m.group(2):
                connector_name = _capture_connector_name(m.group(2))
            args: dict = {"table": table}
            if connector_name:
                args["connector_name"] = connector_name
            planned.append(("introspect_connector_schema", args))

    tables_on = re.search(
        r"(?:can\s+you\s+|could\s+you\s+|please\s+|pls\s+|hey\s+)?"
        r"(?:list|show|get|fetch|pull|grab|what(?:\s+are)?)\s+"
        r"(?:the\s+|all\s+)?"
        r"(?:tables|tbls|collections|objects|schemas)\s+"
        r"(?:from|on|in|for|of)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:tables|tbls|collections|objects|schemas)\s+(?:from|on|in|for|of)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:can\s+you\s+|could\s+you\s+|please\s+)?"
        r"(?:pull|get|fetch|grab|show|list)\s+(?:the\s+)?"
        r"(?:table\s+list|list\s+of\s+tables)\s+(?:from|on|in|for)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:table\s+list|list\s+of\s+tables)\s+(?:from|on|in|for)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:which|what)\s+tables\s+(?:do\s+we\s+have|exist|are\s+there|are\s+available)\s+(?:on|in|from)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:everything|all\s+(?:tables|objects))\s+available\s+on\s+(.+)$",
        lower,
    ) or re.search(
        # "how many tables available in X" / typo availabale / "give me tables available in X"
        r"(?:can\s+you\s+|could\s+you\s+|please\s+)?"
        r"(?:how\s+many|number\s+of|count(?:\s+of)?|give\s+me|show\s+me|list|get)\s+"
        r"(?:the\s+|all\s+)?"
        r"tables?\s+availab(?:le|ale)\s+(?:on|in|from|for)\s+(.+)$",
        lower,
    ) or re.search(
        r"tables?\s+availab(?:le|ale)\s+(?:on|in|from|for)\s+(.+)$",
        lower,
    )
    if tables_on and "introspect_connector_schema" not in [p[0] for p in planned]:
        cname = _capture_connector_name(tables_on.group(1))
        if cname:
            planned.append(("list_connector_objects", {"connector_name": cname}))
            # Never also sample a fake table named "tables".
            planned = [(n, a) for n, a in planned if n != "sample_connector_object"]
    elif re.search(
        r"\b(?:list|show|get)\s+tables\b|\bwhat\s+tables\s+(?:do\s+i\s+have|are\s+there|exist)\b|"
        r"\bmy\s+tables\b|\btables\s+do\s+i\s+have\b",
        lower,
    ) and "list_connector_objects" not in [p[0] for p in planned]:
        # Bare inventory ask — list connectors so we can ask which one to expand.
        planned.append(("list_connectors", {}))
        planned = [
            (n, a) for n, a in planned
            if not (n == "navigate" and (a or {}).get("screen") == "connectors")
        ]

    # Bare saved-connector name → list tables (not "I'm not sure how to do X").
    if not planned and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{1,80}", message.strip()):
        bare = message.strip()
        try:
            from services.connector_store import list_connectors as _list_saved

            saved = _list_saved() or []
            from .schema_tools import _match_score

            scored = [
                (_match_score(bare, str(c.get("name") or ""), str(c.get("type") or "")), c)
                for c in saved
                if isinstance(c, dict)
            ]
            scored = [(s, c) for s, c in scored if s >= 95.0]
            if len(scored) == 1:
                planned.append((
                    "list_connector_objects",
                    {"connector_name": str(scored[0][1].get("name") or bare)},
                ))
        except Exception:
            pass

    # Aggregations: "count of orders by status", "average price in products",
    # "top 5 regions by revenue", "revenue by month". Parsed structurally; the
    # tool then grounds every name in the live schema.
    # Skip when the operator pasted / ran explicit SQL — that is run_query's job.
    from .aggregate_tools import parse_aggregation_request

    _explicit_sql_intent = bool(
        re.search(r"(?:run|execute)\s+(?:this\s+)?(?:sql|query)\b", lower)
        or re.search(r"(?:please\s+)?(?:run|execute)\s+this\s*:", lower)
        or re.search(r"\b(?:run|execute)\s+select\b", lower)
        or _SQL_SELECT_SHAPE.match(message.strip())
        or _SQL_WITH_SHAPE.match(message.strip())
        or re.search(r"(?:please\s+)?(?:run|execute)\s+this:\s*select\b", lower)
        or re.search(r":\s*select\b", lower)
    )
    agg = parse_aggregation_request(message)
    if (
        agg is not None
        and not _explicit_sql_intent
        and "aggregate_data" not in [p[0] for p in planned]
        and "list_connector_objects" not in [p[0] for p in planned]
    ):
        # Incomplete slots (missing table/column) still plan — the tool + recovery
        # clarify or auto-pick a unique table. Silent [] is a chatbot dead-end.
        # "how many tables on sales" is inventory, not aggregate over a table named tables.
        _inv_tables = (agg.table or "").lower() in {
            "tables", "table", "collections", "objects", "schemas", "databases",
        }
        _inv_col = re.search(r"tables?\s+availab", str(agg.column or ""), re.I)
        if (_inv_tables or _inv_col) and "list_connector_objects" in [p[0] for p in planned]:
            pass
        elif (_inv_tables or _inv_col) and (agg.connector_name or agg.table):
            # "how many tables available in PostgresVenkat" → table slot = connector.
            cname = agg.connector_name or (agg.table if _inv_col else "")
            if cname:
                planned.append(("list_connector_objects", {
                    "connector_name": cname,
                }))
        else:
            planned.append(("aggregate_data", agg.as_tool_args()))

    # Sample / analyze live table data
    # Prefer "show the data from <table>" before the generic "show …" pattern —
    # otherwise "show the data from countries" captures table=`data`.
    sample_m = re.search(
        r"(?:show|give\s+me|get|fetch|pull)\s+(?:me\s+)?(?:the\s+)?(?:data|rows|records)\s+"
        r"(?:from|in|on)\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    ) or re.search(
        r"(?:sample|preview|show(?:\s+me)?(?:\s+some)?(?:\s+data)?(?:\s+from)?|rows?\s+from|"
        r"analyze|profile|peek(?:\s+at)?|(?:give\s+me\s+(?:a\s+)?)?quick\s+look\s+at)\s+(?:the\s+|a\s+)?"
        r"([a-zA-Z0-9_.-]+)(?:\s+(?:table|collection|rows))?"
        r"(?:\s+(?:on|in|from|using)\s+(.+))$",
        lower,
    ) or re.search(
        # "preview first 5 of orders on warehouse"
        r"(?:sample|preview|peek(?:\s+at)?)\s+(?:the\s+|a\s+)?"
        r"(?:first|top)\s+\d{1,4}\s+(?:rows?\s+)?(?:of|from)\s+"
        r"([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))$",
        lower,
    ) or re.search(
        r"(?:sample|preview|show(?:\s+me)?(?:\s+data)?(?:\s+from)?|analyze|profile)\s+"
        r"([a-zA-Z0-9_.-]+)"
        r"\s+(?:on|in|from|using)\s+(.+)$",
        lower,
    ) or re.search(
        r"(?:show|give\s+me)\s+(?:a\s+)?(?:sample|preview|peek|look|quick\s+look)\s+(?:of|at)\s+"
        r"([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))$",
        lower,
    ) or re.search(
        r"(?:data|rows)\s+(?:in|from)\s+([a-zA-Z0-9_.-]+)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))$",
        lower,
    ) or re.search(
        # "can you get users data from postgres" / "get users from Local Postgres"
        r"(?:can\s+you\s+|could\s+you\s+|please\s+)?"
        r"(?:get|fetch|pull|grab|load|give\s+me)\s+(?:the\s+)?"
        r"([a-zA-Z0-9_.-]+)\s+(?:data|rows|table|records|collection)?\s*"
        r"(?:from|on|in|using)\s+(.+)$",
        lower,
    ) or re.search(
        # "users data from postgres" — transfer verbs handled elsewhere
        r"([a-zA-Z0-9_.-]+)\s+(?:data|rows|records)\s+"
        r"(?:from|on|in)\s+(.+)$",
        lower,
    )
    if sample_m and "sample_connector_object" not in [p[0] for p in planned]:
        # Don't steal transfer / schema / inventory intents
        if (
            "schema" not in lower
            and "columns" not in lower
            and "describe" not in lower
            and "list_connector_objects" not in [p[0] for p in planned]
            and not any(w in lower for w in (" to ", " into ", "->"))
            and not re.search(r"\b(?:move|transfer|migrate|sync|copy|replicate)\b.+\b(?:from|to)\b", lower)
        ):
            table = (sample_m.group(1) or "").strip()
            cname = ""
            if sample_m.lastindex and sample_m.lastindex >= 2 and sample_m.group(2):
                cname = _capture_connector_name(sample_m.group(2))
            # "get tables from X" is inventory, not a table named "tables".
            if table.lower() in {"tables", "collections", "objects", "schemas", "databases"}:
                if cname and "list_connector_objects" not in [p[0] for p in planned]:
                    planned.append(("list_connector_objects", {"connector_name": cname}))
            elif table and table not in {"data", "rows", "me", "some", "the", "my", "all"}:
                args = {"table": table, "analyze": "analy" in lower or "profile" in lower}
                if cname:
                    args["connector_name"] = cname
                planned.append(("sample_connector_object", args))
                planned = [(n, a) for n, a in planned if n != "search_knowledge"]

    # Schedule tools always beat accidental sample parses ("details on schedule X").
    if any(
        n in {
            "open_schedule", "get_schedule", "run_schedule_now",
            "list_schedules", "create_schedule",
        }
        for n, _ in planned
    ):
        planned = [(n, a) for n, a in planned if n != "sample_connector_object"]

    # Explicit SQL — either "run this sql: …" or a genuinely pasted statement.
    explicit_sql = re.search(
        r"(?:run|execute)\s+(?:this\s+)?(?:sql|query)\s*[:\-]?\s*(.+)$",
        message,
        re.IGNORECASE | re.DOTALL,
    ) or re.search(
        r"(?:please\s+)?(?:run|execute)\s+this\s*:\s*(.+)$",
        message,
        re.IGNORECASE | re.DOTALL,
    ) or re.search(
        r"(?:please\s+)?(?:run|execute)\s+(select\b.+)$",
        message,
        re.IGNORECASE | re.DOTALL,
    )
    raw_sql = (
        (explicit_sql.group(1) or "").strip()
        if explicit_sql
        else _extract_sql_statement(message)
    )
    if raw_sql and "run_query" not in [p[0] for p in planned]:
        sql = raw_sql.strip().strip("`")
        # Optional "on Connector" suffix
        on_m = re.search(r"\s+(?:on|using|against)\s+[\"']?([^\"'\n]+?)[\"']?\s*$", sql, re.IGNORECASE)
        cname = ""
        if on_m:
            cname = _clean_connector_phrase(on_m.group(1))
            sql = sql[: on_m.start()].strip()
        args = {"query": sql, "analyze": "analy" in lower}
        if cname:
            args["connector_name"] = cname
        planned.append(("run_query", args))

    # Follow-ups on last stored sample/query
    analyze_follow = re.search(
        r"\b(?:analyze|profile|summarize|null\s*rates?|top\s+values?|"
        r"cardinality|stats?(?:\s+on)?)\b.*\b(?:that|this|these|it|result|sample|rows?)\b",
        lower,
    ) or re.search(
        r"\b(?:analyze|profile)\s+(?:that|this|these|it|the\s+result|the\s+sample)\b",
        lower,
    ) or (
        re.search(r"\b(?:null\s*rates?|top\s+values?|column\s+profile)\b", lower)
        and not sample_m
    )
    focus_col = re.search(
        r"(?:of|for|on)\s+[\"']?([a-zA-Z_][a-zA-Z0-9_]*)[\"']?\s*$",
        lower,
    )
    # Bare "summarize that" is a recap of the last answer, not a result profile.
    _last_answer_recap = bool(
        re.match(
            r"^\s*(?:summarize\s+(?:that|this|it|what\s+you\s+(?:just\s+)?said)"
            r"|tl;?dr|in\s+(?:a\s+)?(?:sentence|nutshell)|short\s+version|recap(?:\s+that)?)"
            r"\s*[.!?]*$",
            lower,
        )
    )
    if analyze_follow and not _last_answer_recap and "analyze_result" not in [p[0] for p in planned]:
        # Don't steal fresh table sample intents
        if "sample_connector_object" not in [p[0] for p in planned]:
            args = {}
            if focus_col and focus_col.group(1) not in {
                "that", "this", "these", "result", "sample", "rows", "data", "null",
            }:
                args["column"] = focus_col.group(1)
            planned.append(("analyze_result", args))

    filter_m = re.search(
        r"(?:filter|show\s+rows?)\s+(?:where\s+)?"
        r"[\"']?([a-zA-Z_][a-zA-Z0-9_]*)[\"']?\s*"
        r"(is\s+not\s+null|is\s+null|equals?|\bis\b|=|!=|<>|contains|like|>|>=|<|<=)\s*"
        r"[\"']?([^\"'\n]*?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:rows?\s+where|where)\s+"
        r"[\"']?([a-zA-Z_][a-zA-Z0-9_]*)[\"']?\s*"
        r"(is\s+not\s+null|is\s+null|equals?|\bis\b|=|!=|<>|contains|like|>|>=|<|<=)\s*"
        r"[\"']?([^\"'\n]*?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:filter|where)\s+[\"']?([a-zA-Z_][a-zA-Z0-9_]*)[\"']?\s*"
        r"(is\s+not\s+null|is\s+null)",
        lower,
    )
    if filter_m and "filter_result" not in [p[0] for p in planned]:
        if "sample_connector_object" not in [p[0] for p in planned] and "run_query" not in [
            p[0] for p in planned
        ]:
            col = (filter_m.group(1) or "").strip()
            if col in {"where", "rows", "row", "show", "filter", "the", "a"}:
                col = ""
            op_raw = ""
            val = ""
            if filter_m.lastindex and filter_m.lastindex >= 2:
                op_raw = (filter_m.group(2) or "").strip()
            if filter_m.lastindex and filter_m.lastindex >= 3:
                val = (filter_m.group(3) or "").strip().strip("\"'")
            op = "eq"
            if "not null" in op_raw:
                op = "not_null"
                val = ""
            elif "null" in op_raw:
                op = "is_null"
                val = ""
            elif op_raw in {"!=", "<>"}:
                op = "ne"
            elif op_raw in {">"}:
                op = "gt"
            elif op_raw in {">="}:
                op = "gte"
            elif op_raw in {"<"}:
                op = "lt"
            elif op_raw in {"<="}:
                op = "lte"
            elif "contain" in op_raw or "like" in op_raw:
                op = "contains"
            args = {"column": col, "op": op, "value": val}
            # Strip a trailing "on <connector>" accidentally captured in value.
            if args.get("value"):
                args["value"] = re.sub(
                    r"\s+on\s+[A-Za-z0-9_\- ]+$",
                    "",
                    str(args["value"]),
                    flags=re.I,
                ).strip()
            if col:
                planned.append(("filter_result", args))

    diff_m = re.search(
        r"diff\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?\s+vs\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:diff|compare)\s+(?:schema(?:s)?\s+(?:of\s+)?)?[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+"
        r"(?:on|in)\s+[\"']?(.+?)[\"']?\s+(?:vs|versus|and|with|against)\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?",
        lower,
    ) or re.search(
        # "diff the orders schema on Local Postgres against Warehouse"
        r"(?:diff|compare)\s+(?:the\s+)?([a-zA-Z0-9_.-]+)\s+schema\s+"
        r"(?:on|in)\s+(.+?)\s+(?:against|vs|versus|with)\s+(.+?)\s*$",
        lower,
    ) or re.search(
        r"(?:diff|compare)\s+(?:schema\s+)?[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+vs\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?",
        lower,
    )
    if diff_m and "diff_schemas" not in [p[0] for p in planned]:
        if diff_m.lastindex and diff_m.lastindex >= 4:
            planned.append(("diff_schemas", {
                "source_table": diff_m.group(1).strip(),
                "source_connector_name": diff_m.group(2).strip(),
                "dest_table": diff_m.group(3).strip(),
                "dest_connector_name": diff_m.group(4).strip(),
            }))
        elif diff_m.lastindex and diff_m.lastindex >= 3:
            # table + source connector + dest connector (same table name assumed)
            planned.append(("diff_schemas", {
                "source_table": diff_m.group(1).strip(),
                "source_connector_name": _capture_connector_name(diff_m.group(2)),
                "dest_table": diff_m.group(1).strip(),
                "dest_connector_name": _capture_connector_name(diff_m.group(3)),
            }))
        else:
            planned.append(("diff_schemas", {
                "source_table": diff_m.group(1).strip(),
                "dest_table": diff_m.group(2).strip(),
            }))

    map_m = re.search(
        r"(?:map|mapping|map columns|map schema)\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+"
        r"(?:on|in|from)\s+[\"']?(.+?)[\"']?\s+(?:to|onto|->)\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in|to)\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"map\s+(?:schema\s+(?:of\s+)?)?[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+"
        r"(?:on|in)\s+[\"']?(.+?)[\"']?\s+to\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"map\s+schema\s+of\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+"
        r"(?:on|in)\s+(.+?)\s+to\s+(.+?)\s*$",
        lower,
    ) or re.search(
        # "map customers to dim_customer" — connectors clarified at execution
        r"\bmap\s+(?:table\s+|schema\s+)?"
        r"[\"']?([a-zA-Z_][a-zA-Z0-9_.-]*)[\"']?\s+"
        r"(?:to|onto|->)\s+[\"']?([a-zA-Z_][a-zA-Z0-9_.-]*)[\"']?\s*$",
        lower,
    )
    if map_m and "map_connector_schemas" not in [p[0] for p in planned]:
        # Prefer map over a competing introspect of the same table.
        planned = [(n, a) for n, a in planned if n != "introspect_connector_schema"]
        if map_m.lastindex and map_m.lastindex >= 4:
            planned.append(("map_connector_schemas", {
                "source_table": map_m.group(1).strip(),
                "source_connector_name": map_m.group(2).strip(),
                "dest_table": map_m.group(3).strip(),
                "dest_connector_name": map_m.group(4).strip(),
            }))
        elif map_m.lastindex and map_m.lastindex >= 3:
            planned.append(("map_connector_schemas", {
                "source_table": map_m.group(1).strip(),
                "source_connector_name": _capture_connector_name(map_m.group(2)),
                "dest_table": map_m.group(1).strip(),
                "dest_connector_name": _capture_connector_name(map_m.group(3)),
            }))
        else:
            planned.append(("map_connector_schemas", {
                "source_table": map_m.group(1).strip(),
                "dest_table": map_m.group(2).strip(),
            }))

    product_gate_ask = any(
        w in lower
        for w in (
            "what quality gates",
            "what are the quality",
            "list quality gates",
            "preflight gates",
            "what gates",
            "9 gates",
            "nine gates",
            "quality gates do you",
            "what are your gates",
            "explain the 9",
            "explain the nine",
            "explain preflight",
            "explain the gates",
            "g1-g9",
            "g1–g9",
        )
    )
    if product_gate_ask:
        # Prefer honest product gate list over ontology RAG shards.
        planned.append(("explain_product", {"query": message[:240]}))
        planned.append(("profile_quality_rules", {}))
        planned = [(n, a) for n, a in planned if n != "search_knowledge"]
    elif any(w in lower for w in (
        "quality rules", "quality gates", "data quality", "profile rules",
        "suggest improvements", "suggestions for my data", "how can i improve",
        "data recommendations", "recommend fixes", "suggest quality",
        "quality suggestions", "suggest quality rules",
    )):
        planned.append(("profile_quality_rules", {}))
        if any(w in lower for w in ("suggest", "recommend", "improve", "fix")):
            planned.append(("search_knowledge", {"query": message[:200]}))

    # Mapping repair — always Confirm via Transfer Studio (never invent rewrites in chat).
    if any(
        w in lower
        for w in (
            "fix my mapping", "fix mapping", "mapping broken", "wrong mapping",
            "help me fix my mapping", "repair mapping", "repair my mapping",
            "wrong with mapping", "whats wrong with mapping", "what's wrong with mapping",
            "mapping is incorrect", "mapping is wrong",
        )
    ):
        if "remediate_validation" not in {n for n, _ in planned}:
            planned.append(("remediate_validation", {"kind": "review_mappings"}))
            planned.append(("navigate", {"screen": "transfer"}))
        planned = [
            (n, a) for n, a in planned
            if n not in ("search_knowledge", "explain_mapping_assurance", "explain_product")
        ]

    if re.search(r"\bpii\b", lower):
        tm = re.search(r"\bin\s+[\"']?([A-Za-z_][A-Za-z0-9_]*)[\"']?", lower)
        table = (tm.group(1) if tm else "").strip()
        if table and table not in {"it", "that", "this", "my", "the"}:
            planned.append(("introspect_connector_schema", {"table": table}))
            planned = [(n, a) for n, a in planned if n != "search_knowledge"]

    # Uploaded dataset compare — only when not already a live schema diff
    if "diff_schemas" not in [p[0] for p in planned]:
        compare = re.search(
            r"compare\s+(?:dataset\s+)?(\w+)\s+(?:and|vs|with|to)\s+(?:dataset\s+)?(\w+)",
            lower,
        )
        if compare and "schema" not in lower:
            planned.append(("compare_datasets", {
                "dataset_a": compare.group(1),
                "dataset_b": compare.group(2),
            }))

    # Explicit knowledge / ontology asks beat dataset search.
    if re.search(
        r"\b(?:search\s+knowledge|knowledge\s+for|semantic\s+types?|ontology)\b",
        lower,
    ):
        planned.append(("search_knowledge", {"query": message[:200]}))
        planned = [(n, a) for n, a in planned if n != "search_data"]
    else:
        search = re.search(r"(?:search|find)\s+(?:for\s+)?['\"]?(\w+)['\"]?", lower)
        if search and "search_data" not in [p[0] for p in planned]:
            # Avoid treating connector/table lookups as dataset search
            if not any(p[0] in _LIVE_SCHEMA_TOOLS for p in planned):
                planned.append(("search_data", {"query": search.group(1)}))

    # Telegraphic count: "count rows airports Local Postgres" / "how big is orders on sales"
    if "aggregate_data" not in [p[0] for p in planned]:
        tele = re.search(
            r"\b(?:count|how many)\s+rows?\s+([A-Za-z_][A-Za-z0-9_]*)\s+"
            r"([A-Za-z0-9_][A-Za-z0-9_\- ]{1,48}?)\s*$",
            lower,
        ) or re.search(
            r"\b(?:how\s+big\s+is|size\s+of|row\s+count\s+(?:for|of)|how\s+large\s+is)\s+"
            r"([A-Za-z_][A-Za-z0-9_.]*)"
            r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
            lower,
        )
        if tele:
            cname = ""
            if tele.lastindex and tele.lastindex >= 2 and tele.group(2):
                cname = _capture_connector_name(tele.group(2))
            args = {
                "metric": "count",
                "table": tele.group(1).strip(),
            }
            if cname:
                args["connector_name"] = cname
            planned.append(("aggregate_data", args))

    # "does orders have updated_at on sales" → live schema introspect
    has_col = re.search(
        r"\bdoes\s+([A-Za-z_][A-Za-z0-9_.]*)\s+have\s+([A-Za-z_][A-Za-z0-9_]*)"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    ) or re.search(
        r"\b(?:has|have)\s+([A-Za-z_][A-Za-z0-9_.]*)\s+(?:got\s+)?(?:a\s+|an\s+)?"
        r"([A-Za-z_][A-Za-z0-9_]*)\s+column"
        r"(?:\s+(?:on|in|from|using)\s+(.+))?$",
        lower,
    )
    if has_col and "introspect_connector_schema" not in [p[0] for p in planned]:
        args = {"table": has_col.group(1).strip()}
        if has_col.lastindex and has_col.lastindex >= 3 and has_col.group(3):
            args["connector_name"] = _capture_connector_name(has_col.group(3))
        planned.append(("introspect_connector_schema", args))

    # Connector health / ping — prove connectivity by listing live objects.
    health = re.search(
        r"\b(?:is|check\s+if)\s+([A-Za-z][A-Za-z0-9_\-]{1,48})\s+"
        r"(?:is\s+)?(?:connected|healthy|reachable|up|online)\b",
        lower,
    ) or re.search(
        r"\b(?:ping|test)\s+([A-Za-z][A-Za-z0-9_\- ]{1,48}?)\s+connector\b",
        lower,
    ) or re.search(
        r"\b(?:test|ping|check|validate)\s+(?:the\s+)?connection\s+(?:to|for)\s+"
        r"(?:connector\s+)?([A-Za-z][A-Za-z0-9_\- ]{1,48}?)\s*$",
        lower,
    ) or re.search(
        r"\b(?:test|ping|validate)\s+(?:the\s+)?(?:connector\s+)?"
        r"([A-Za-z][A-Za-z0-9_\- ]{1,48}?)\s*$",
        lower,
    )
    if health and "list_connector_objects" not in [p[0] for p in planned]:
        cname = _capture_connector_name(health.group(1))
        cname = re.sub(r"\s+connector$", "", cname, flags=re.I).strip()
        # Drop accidental "if …" leftovers from soft parses.
        cname = re.sub(r"^if\s+", "", cname, flags=re.I).strip()
        if cname and cname not in {
            "connection", "the", "my", "a", "an", "it", "this", "that",
            "mapping", "schema", "data", "job", "transfer", "if",
        }:
            planned.append(("list_connector_objects", {"connector_name": cname}))
            planned = [
                (n, a) for n, a in planned
                if n not in ("list_connectors", "search_knowledge", "explain_product")
            ]
    analyst = get_data_analyst()
    hint = analyst.extract_dataset_hint(message)
    data_signals = [
        "analyze", "what's in", "what is in", "tell me about", "tell me everything about", "pii",
        "preview", "sample", "quality", "how many rows",
    ]
    # "columns"/"schema" alone often mean live DB — only analyze uploaded data
    # when the user clearly asks to analyze / profile a dataset file.
    if any(s in lower for s in data_signals) and not any(p[0] in _LIVE_SCHEMA_TOOLS for p in planned):
        if hint:
            planned.append(("analyze_dataset", {"dataset_name": hint}))
        elif re.search(r"\banalyze\b", lower) and "analyze_dataset" not in [p[0] for p in planned]:
            # Named dataset that the index doesn't know yet — still invoke so
            # recovery can list indexed uploads instead of a dead-end reply.
            m = re.search(
                r"analyze\s+(?:the\s+)?(.+?)(?:\s+data(?:set)?)?\s*$",
                lower,
            )
            name = (m.group(1) if m else "").strip(" \"'")
            if name and name not in {"this", "that", "it", "my", "the"}:
                planned.append(("analyze_dataset", {"dataset_name": name}))

    if _looks_like_product_howto(lower) and not _has_explicit_workspace_subject(lower):
        # Curated local FAQ — don't wipe stronger product/ops tools already planned.
        keep = {
            "profile_quality_rules", "describe_pilot", "explain_product",
            "explain_mapping_assurance", "remediate_validation", "navigate",
            "inspect_schema_policy", "get_transfer_capabilities", "list_jobs",
            "brief_workspace",
            "list_connectors", "list_schedules", "aggregate_data",
            "list_connector_objects", "sample_connector_object", "plan_transfer",
            "start_transfer", "plan_transfer_route", "create_connector",
            "run_schedule_now", "create_schedule", "introspect_connector_schema",
        }
        names = {n for n, _ in planned}
        if planned and (names & keep):
            # Keep intentional RAG companion beside quality profiling suggestions.
            if not ("profile_quality_rules" in names and "search_knowledge" in names):
                planned = [(n, a) for n, a in planned if n != "search_knowledge"]
            if "explain_product" not in names and not (
                names & {"profile_quality_rules", "describe_pilot", "explain_mapping_assurance", "remediate_validation"}
            ):
                planned.insert(0, ("explain_product", {"query": message[:240]}))
        else:
            planned = [("explain_product", {"query": message[:240]})]
        # Mapping repair already staged Confirm — don't dilute with a FAQ essay.
        if "remediate_validation" in {n for n, _ in planned} and re.search(
            r"\b(?:mapping|mappings)\b", lower
        ) and re.search(r"\b(?:wrong|fix|repair|broken|bad)\b", lower):
            planned = [(n, a) for n, a in planned if n != "explain_product"]
    elif not planned and _looks_like_domain_knowledge_query(lower):
        planned.append(("search_knowledge", {"query": message[:200]}))

    # A stated transfer is the request; inventory/advice tools that merely share
    # its vocabulary ("jobs", "upsert", "schema") must not answer in its place.
    _staged = {n for n, _ in planned} & {
        "start_transfer", "plan_transfer", "create_schedule"
    }
    if _staged:
        planned = [
            (n, a) for n, a in planned
            if n not in {
                "list_jobs", "recommend_sync_mode", "list_connectors",
                "search_knowledge", "get_transfer_capabilities",
            }
            and not (n == "introspect_connector_schema" and not _asks_for_schema(lower))
        ]

    # Off-topic / general-web asks must not become product RAG. Vocabulary
    # overlap ("capital") is not evidence — only a Help hit, a pasted id, or
    # an explicit knowledge search is.
    if _act == "general":
        from ..rag.evidence import names_identifier

        explicit_knowledge = bool(
            re.search(
                r"\b(?:search\s+knowledge|knowledge\s+for|semantic\s+types?|ontology)\b",
                lower,
            )
        )
        keep_rag = (
            explicit_knowledge
            or names_identifier(message)
            or bool(product_doc_search(message, limit=1))
        )
        if not keep_rag:
            planned = [(n, a) for n, a in planned if n != "search_knowledge"]

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[tuple[str, dict]] = []
    for name, args in planned:
        key = f"{name}:{json.dumps(args, sort_keys=True, default=str)}"
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, args))
    return prune_planned_tools(unique)


def format_tool_results_for_llm(results: list[ToolResult]) -> str:
    parts = []
    for r in results:
        if r.success:
            parts.append(f"Tool `{r.name}` result:\n{json.dumps(r.output, indent=2, default=json_default)[:4000]}")
        else:
            parts.append(f"Tool `{r.name}` failed: {r.error}")
    return "\n\n".join(parts)


_tools: DataPilotTools | None = None


def get_pilot_tools() -> DataPilotTools:
    global _tools
    if _tools is None:
        _tools = DataPilotTools()
    return _tools
