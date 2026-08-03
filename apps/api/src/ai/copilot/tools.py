"""Data Pilot — app tools the agent can invoke (like Cursor/Claude tool use)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from .data_analyst import get_data_analyst


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
        "description": "Search trained Data Pilot knowledge base — connectors, transfers, PII, mappings, product help.",
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
        "description": "Plan an any-to-any transfer route with sync mode, schema policy, validation gates, and risk controls.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Source system, connector, file type, or table"},
                "destination": {"type": "string", "description": "Destination system, warehouse, database, file type, or table"},
                "workload": {"type": "string", "description": "full_load, incremental, cdc, file_export, or unknown"},
                "table": {"type": "string", "description": "Source table — required for a real plan"},
                "dest_table": {"type": "string", "description": "Destination table (defaults to the source name)"},
                "sync_mode": {"type": "string"},
            },
            "required": [],
        },
    },
    {
        "name": "plan_transfer",
        "description": (
            "Plan a real transfer between two saved connectors: introspect both schemas live, "
            "map columns, list type conversions and lossy casts, and run the 9 preflight gates. "
            "Read-only — it never moves data. Use whenever the operator asks what a transfer "
            "would do, or before starting one."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_connector_name": {"type": "string", "description": "Saved source connector"},
                "source_table": {"type": "string", "description": "Source table or collection"},
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
            },
            "required": ["source_table"],
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
                "dest_connector_name": {"type": "string"},
                "dest_table": {"type": "string"},
                "sync_mode": {"type": "string"},
                "limit": {"type": "integer", "description": "Cap rows moved (0 = all)"},
            },
            "required": ["source_table"],
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
        "description": "Explain what Data Pilot knows and can do locally — capabilities, not raw RAG dumps.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_schedules",
        "description": "List pipeline schedules (Pipelines page) with cadence, next run, and last status.",
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
        "description": "Fetch one pipeline schedule by id or name.",
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
            "DataFlow's semantic column mapper (same engine as Transfer Studio Map step)."
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
]

TOOL_FAMILIES: list[dict] = [
    {
        "id": "discover",
        "label": "Discover",
        "tools": ["list_datasets", "search_data", "search_connectors", "search_knowledge", "describe_pilot"],
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
    """Execute Data Pilot tools against live app state."""

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
        }
        handler = handlers.get(name)
        if not handler:
            return ToolResult(name=name, success=False, output=None, error=f"Unknown tool: {name}")
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
            return ToolResult(name="create_connector", success=False, output=draft, error=missing)

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
                    },
                )
                if not probe_ok:
                    return ToolResult(
                        name="create_connector",
                        success=False,
                        output=draft,
                        error=(
                            f"Could not connect with those credentials: {probe_msg}. "
                            "Fix host/port/user/password (use the public proxy if this is Railway), then ask again."
                        ),
                    )
            except Exception as exc:
                return ToolResult(
                    name="create_connector",
                    success=False,
                    output=draft,
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
        return ToolResult(name="list_jobs", success=True, output={"jobs": summary, "count": len(summary)})

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
            "normalize_control_chars": "Fix bad data…",
            "open_bad_data_fix": "Fix bad data…",
            "quarantine_and_rerun": "Fix bad data…",
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
        return ToolResult(
            name="describe_pilot",
            success=True,
            output={
                "role": "Data Pilot",
                "runtime": "local_engine",
                "runtime_note": (
                    "Primary brain is DataFlow's local OpenAI-style tool loop "
                    "(DATAFLOW_PILOT_ENGINE=local). No third-party API required."
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
                    "How many rows in airports on Local Postgres?",
                    "Count of orders by status on Local Postgres where amount > 100",
                    "Average price in products on Local Postgres",
                    "and by region?",
                    "Plan a transfer of orders from Local Postgres to Warehouse",
                    "Transfer orders from Local Postgres to Warehouse as upsert",
                    "Why did job <id> fail?",
                    "Show my pipelines",
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

    def _search_knowledge(self, query: str = "") -> ToolResult:
        if not query.strip():
            return ToolResult(name="search_knowledge", success=False, output=None, error="query required")
        try:
            from ..rag.pipeline import get_rag_pipeline
            rag = get_rag_pipeline()
            rag.ingestion.ensure_knowledge_loaded()
            result = rag.retriever.retrieve(query, n_results=8)
            hits = []
            for d in result.documents:
                text = (d.text or "").strip()
                score = float(d.score or 0)
                # Drop low-score noise and raw ontology shards that read like debug dumps.
                if score < 0.28:
                    continue
                meta_type = str(d.metadata.get("type") or "").lower()
                # Prefer real product/docs hits over synthetic catalog training docs.
                if meta_type in {"synthetic_catalog", "catalog_schema", "generated_qna"} and score < 0.55:
                    continue
                if "650+" in text or "620+" in text:
                    # Catalog marketing counts are not transfer-ready evidence.
                    continue
                if _is_raw_knowledge_shard(text) and not _query_targets_semantic_type(query, text):
                    continue
                hits.append({
                    "text": text[:600],
                    "score": round(score, 3),
                    "type": d.metadata.get("type", ""),
                    "summary": _summarize_knowledge_hit(text),
                })
                if len(hits) >= 4:
                    break
            return ToolResult(
                name="search_knowledge",
                success=True,
                output={"query": query, "hits": hits, "count": len(hits)},
            )
        except Exception as e:
            return ToolResult(name="search_knowledge", success=False, output=None, error=str(e))

    def _plan_transfer_route(
        self,
        source: str = "",
        destination: str = "",
        workload: str = "unknown",
        table: str = "",
        dest_table: str = "",
        sync_mode: str = "",
    ) -> ToolResult:
        """Route guidance. Real plan when connectors resolve, honest sketch otherwise.

        This used to substring-match "csv" in the connector name and return a
        gate list whose IDs did not exist in ``PREFLIGHT_GATES``. Now the named
        gates come from the registry, and naming a table gets the operator the
        engine's actual mapping and gate results instead of a guess.
        """
        if source and destination and table:
            from .transfer_tools import plan_transfer

            planned = plan_transfer(
                source_connector_name=source,
                source_table=table,
                dest_connector_name=destination,
                dest_table=dest_table or table,
                sync_mode=sync_mode or workload,
            )
            if planned.success:
                return planned

        try:
            from preflight.gates import PREFLIGHT_GATES

            gate_ids = [gid.value if hasattr(gid, "value") else str(gid) for gid, _ in PREFLIGHT_GATES]
        except Exception:
            gate_ids = []

        return ToolResult(name="plan_transfer_route", success=True, output={
            "generic": True,
            "source": source or "source not specified",
            "destination": destination or "destination not specified",
            "required_gates": gate_ids,
            "note": (
                "This is the standard gate sequence, not a plan for your data. "
                "Name two saved connectors and a table and I will introspect both "
                "ends, map the columns and run the real gates."
            ),
            "next": 'Try: "plan a transfer of orders from Local Postgres to Warehouse".',
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
            "not_claimed": "No system can infer perfect business semantics without ground truth; DataFlow fails closed when evidence is ambiguous.",
        })

    def _recommend_sync_mode(
        self,
        workload: str = "",
        has_cursor: bool = False,
        has_primary_key: bool = False,
        needs_history: bool = False,
    ) -> ToolResult:
        w = workload.lower()
        if "cdc" in w:
            mode = "Incremental CDC"
            reason = "Source changes should be read from a log stream and resumed from cursor state."
        elif has_cursor and has_primary_key and needs_history:
            mode = "Incremental Append + Deduped"
            reason = "Cursor and key allow efficient updates while preserving change history."
        elif has_cursor:
            mode = "Incremental Append"
            reason = "Cursor allows new records to be read without a full scan."
        elif "snapshot" in w or "full" in w:
            mode = "Full Refresh Overwrite"
            reason = "Snapshot workloads should replace the destination with the latest source state."
        else:
            mode = "Full Refresh Append"
            reason = "Use append until cursor/key metadata is confirmed."
        return ToolResult(name="recommend_sync_mode", success=True, output={
            "recommended_mode": mode,
            "reason": reason,
            "requires": {
                "cursor": mode.startswith("Incremental"),
                "primary_key": "Deduped" in mode,
                "cdc_log_access": "CDC" in mode,
            },
        })

    def _profile_quality_rules(self, dataset_name: str = "") -> ToolResult:
        schema = self.analyst.resolve_dataset(dataset_name) if dataset_name else None
        columns = schema.columns if schema else []
        pii_candidates = [c for c in columns if any(t in c.lower() for t in ("email", "phone", "ssn", "card", "name"))]
        return ToolResult(name="profile_quality_rules", success=True, output={
            "dataset": schema.name if schema else dataset_name or "active dataset",
            "rules": [
                "type parse success rate >= 99.5%",
                "null rate checked against inferred required fields",
                "primary key uniqueness when candidate key exists",
                "PII columns tagged before destination write",
                "row rejection quarantine enabled for lossy coercions",
                "post-write row count and checksum reconciliation",
            ],
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
        return {
            "id": s.id,
            "name": s.name,
            "enabled": s.enabled,
            "interval": s.interval,
            "cron": s.cron or "",
            "timezone": s.timezone,
            "source_table": s.source_table,
            "dest_table": s.dest_table,
            "next_run_at": s.next_run_at,
            "last_run_at": s.last_run_at,
            "last_status": s.last_status,
            "run_count": s.run_count,
        }

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
        return ToolResult(
            name="run_schedule_now",
            success=True,
            output={
                "action": "run_schedule",
                "schedule_id": sched.id,
                "name": sched.name,
                "label": f"Run pipeline “{sched.name}” now",
                "risk": "mutate",
                "requires_confirm": True,
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
    "describe yourself",
    "describe data pilot",
    "tell me about yourself",
    "what tools do you have",
    "your tools",
)


def _is_meta_pilot_question(lower: str) -> bool:
    if any(p in lower for p in _META_PILOT_PHRASES):
        return True
    if lower.strip() in {"capabilities", "help", "about", "about you"}:
        return True
    return bool(re.search(r"\b(what|which)\s+(knowledge|skills|tools)\b", lower))


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
    return ""


def _clean_connector_phrase(raw: str) -> str:
    """Normalize a captured connector phrase, or "" when it cannot be one."""
    phrase = (raw or "").strip().strip("\"'`").strip(" .,;:!?")
    phrase = re.sub(r"\b(?:please|now|thanks?|thank\s+you)\b\s*$", "", phrase, flags=re.I)
    phrase = phrase.strip(" .,;:!?")
    if not phrase:
        return ""
    low = phrase.lower()
    if low in _NON_CONNECTOR_PHRASES or _TIME_PHRASE_RE.match(low):
        return ""
    return phrase


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


def _looks_like_domain_knowledge_query(lower: str) -> bool:
    """RAG fallback only for substantive domain questions — never chat fluff."""
    if _is_meta_pilot_question(lower):
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
            "export ", "download ", "create a new schedule", "create schedule",
            "create a pipeline", "new nightly", "cron ",
        )
    ):
        return False
    signals = (
        "what is", "what's", "whats", "how do", "how does", "explain", "mean",
        "pallet", "schema", "mapping", "pii", "cdc", "sync", "transfer",
        "column", "type", "connector", "warehouse", "mongodb", "snowflake",
        "postgres", "quality", "quarantine", "checksum", "reconcile",
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
    "run_schedule_now": 78,
    "get_job": 75,
    "get_preflight_run": 74,
    "open_job": 72,
    "open_schedule": 71,
    "get_schedule": 70,
    "list_schedules": 60,
    "list_contracts": 58,
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
            if n not in ("sample_connector_object", "run_query", "analyze_dataset")
        ]
        names = {n for n, _ in planned}
    # Platform inventory: "how many jobs failed" / "connector count" must not
    # become COUNT(*) — drop aggregate entirely beside inventory lists.
    if "list_jobs" in names or "list_connectors" in names:
        planned = [(n, a) for n, a in planned if n != "aggregate_data"]
        names = {n for n, _ in planned}
    # A concrete transfer already contains the mapping, gates and route, so the
    # generic advice tools beside it are redundant noise.
    if names & {"start_transfer", "plan_transfer"}:
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
    # Explicit run / remediate shouldn't also dump unrelated lists
    if "run_schedule_now" in names or "remediate_validation" in names or "create_connector" in names:
        planned = [
            (n, a) for n, a in planned
            if n not in ("list_schedules", "list_jobs", "search_knowledge", "analyze_dataset", "list_connectors", "search_connectors")
        ]
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
        # Allow companions within 25 points, plus navigate / RAG alongside quality.
        if (
            name in ("navigate", "start_transfer_studio", "search_knowledge")
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
_TRANSFER_VERBS = r"transfer|move|copy|sync|migrate|replicate|load|push|send|export"
_TRANSFER_RE = re.compile(
    rf"\b(?:{_TRANSFER_VERBS})\b"
    r"(?:\s+(?:a|an|the))?"
    r"(?:\s+(?:transfer|copy|sync|data|rows|records|everything))?"
    r"(?:\s+(?:of|for))?"
    r"\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+(?:table|collection|dataset))?"
    r"\s+(?:from|out\s+of)\s+(?P<src>.+?)"
    r"\s+(?:to|into|onto|over\s+to|->)\s+(?P<dst>.+?)\s*$",
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


def _strip_transfer_tail(dest: str) -> tuple[str, str]:
    """Split "Warehouse as upsert" into the connector and the sync mode."""
    from .transfer_tools import normalize_sync_mode

    # A trailing clause ("…to wh, is that safe?") is commentary, not a name.
    dest = re.split(r"[,;?]", dest, maxsplit=1)[0].strip()
    tail = _TRANSFER_TAIL_RE.search(dest)
    if not tail:
        return dest.strip(), ""
    spoken = tail.group("mode").strip()
    mode = normalize_sync_mode(spoken, default="")
    if not mode:
        # "into orders in production" — the tail was part of the name.
        return dest.strip(), ""
    return dest[: tail.start()].strip(), mode


def parse_transfer_intent(message: str) -> dict | None:
    """Extract source/destination/table from a transfer request, or None."""
    text = (message or "").strip()
    if not text:
        return None
    match = _TRANSFER_RE.search(text)
    if not match:
        return None
    table = match.group("table").strip()
    source = match.group("src").strip().strip("\"'")
    dest, mode = _strip_transfer_tail(match.group("dst").strip().strip("\"'"))
    if not table or not source or not dest:
        return None
    lowered = text.lower()
    if not mode:
        mode = normalize_sync_mode_for_message(lowered)
    return {
        "source_table": table,
        "source_connector_name": source[:80],
        "dest_connector_name": dest[:80],
        "sync_mode": mode,
        # Asking what a transfer *would* do must never stage a mutation.
        "plan_only": any(w in lowered for w in _PLAN_ONLY_WORDS),
    }


def normalize_sync_mode_for_message(lowered: str) -> str:
    from .transfer_tools import normalize_sync_mode

    for phrase in ("overwrite", "replace", "truncate", "upsert", "merge", "append", "cdc"):
        if phrase in lowered:
            return normalize_sync_mode(phrase, default="")
    return ""


def infer_tools_from_message(message: str) -> list[tuple[str, dict]]:
    """Local tool routing when no LLM tool-use is available."""
    lower = message.lower()
    planned: list[tuple[str, dict]] = []

    if _is_meta_pilot_question(lower):
        planned.append(("describe_pilot", {}))
        return planned

    nav_map = {
        "pilot": ["data pilot", "go to pilot", "open pilot"],
        "transfer": ["start transfer", "new transfer", "upload", "move data", "go to transfer", "transfer studio"],
        "jobs": ["show jobs", "my jobs", "job history", "recent transfers", "go to jobs", "transfer jobs", "show my transfer"],
        "connectors": ["connectors", "connections", "add connector", "go to connectors"],
        "dashboard": ["dashboard", "overview", "home", "go home"],
        "schedules": ["pipelines", "schedules", "scheduled pipelines", "go to pipelines", "open pipelines"],
        "contracts": ["contracts", "data contracts", "go to contracts"],
        "query": ["query", "query playground", "sql playground", "go to query"],
        "settings": ["settings", "sso", "security settings"],
        "mcp": ["mcp", "go to mcp"],
        "docs": ["docs", "documentation", "help docs", "go to docs"],
        "benchmarks": ["proofs", "benchmarks", "go to proofs"],
    }
    nav_verbs = ("go", "open", "show", "take me", "navigate", "bring me")
    for screen, phrases in nav_map.items():
        label = screen.replace("_", " ")
        if re.search(rf"(go to|open|show|take me to|navigate to)\s+{re.escape(label)}", lower):
            planned.append(("navigate", {"screen": screen}))
            break
        if screen == "schedules" and re.search(r"(go to|open|show|take me to)\s+pipelines?", lower):
            planned.append(("navigate", {"screen": "schedules"}))
            break
        if any(p in lower for p in phrases) and any(w in lower for w in nav_verbs):
            planned.append(("navigate", {"screen": screen}))
            break

    if any(w in lower for w in ("all datasets", "what data", "available data", "list datasets", "what files")):
        planned.append(("list_datasets", {}))

    setup = re.search(r"(?:set up|setup|configure|connect)\s+(.+?)\s+as\s+(?:a\s+)?(?:source|destination)", lower)
    if setup:
        q = setup.group(1).strip()
        role = "destination" if "destination" in lower else "source"
        planned.append(("search_connectors", {"query": q[:40], "role": role}))
    elif any(w in lower for w in ("connectors", "connections")) and not any(
        v in lower for v in ("go to", "take me", "navigate to", "open ")
    ):
        if any(w in lower for w in ("search", "find", "source", "destination", "setup", "postgres", "snowflake", "shopify")):
            role = "destination" if "destination" in lower or "warehouse" in lower else "source" if "source" in lower else "all"
            q = re.sub(r".*(?:search|find|setup)\s+", "", lower).strip() or lower
            planned.append(("search_connectors", {"query": q[:40], "role": role}))
        else:
            planned.append(("list_connectors", {}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "connectors")
            ]

    # Create / save connector from credentials pasted in chat
    from .connector_create import wants_create_connector

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

    run_sched = re.search(
        r"(?:run|trigger|execute)\s+(?:schedule|pipeline)\s+[\"']?([^\"'\n]+?)[\"']?(?:\s+now)?\s*$",
        lower,
    ) or re.search(r"run\s+[\"']?([^\"'\n]+?)[\"']?\s+(?:schedule|pipeline)\s*now", lower)
    if any(w in lower for w in ("run now", "run schedule", "run pipeline", "trigger schedule", "trigger pipeline")):
        name = ""
        if run_sched:
            name = run_sched.group(1).strip()
        else:
            m = re.search(r"(?:schedule|pipeline)\s+[\"']?([a-z0-9 _-]+)[\"']?", lower)
            if m:
                name = m.group(1).strip()
            else:
                m2 = re.search(r"run\s+[\"']?([a-z0-9 _-]+)[\"']?\s+now", lower)
                if m2 and m2.group(1) not in ("schedule", "pipeline", "it", "this"):
                    name = m2.group(1).strip()
        planned.append(("run_schedule_now", {"name": name} if name else {}))

    if any(w in lower for w in ("contracts", "data contract")) and any(w in lower for w in ("list", "show", "what")):
        # "show contracts" may also navigate — prefer list when asking for contents.
        if not any(v in lower for v in ("go to", "take me", "navigate to", "open ")):
            planned.append(("list_contracts", {"limit": 50}))
            planned = [
                (n, a) for n, a in planned
                if not (n == "navigate" and (a or {}).get("screen") == "contracts")
            ]

    # List jobs when the operator wants contents — not when they only want the screen.
    _nav_only = any(v in lower for v in ("go to", "take me to", "navigate to", "open "))
    # Platform inventory — DataFlow's own jobs/connectors, never warehouse tables.
    # "how many jobs failed" must list jobs, not SELECT COUNT(*) FROM jobs.
    _platform_job_inventory = bool(
        re.search(
            r"\b(?:how many\s+jobs|jobs?\s+failed|failed\s+jobs|job\s+failures|"
            r"failed\s+transfers|how many\s+transfers\s+failed)\b",
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
    if _platform_connector_inventory:
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
    elif any(w in lower for w in ("why did this job fail", "why did the transfer fail", "job failed", "transfer failed", "analyze job")):
        planned.append(("list_jobs", {"limit": 5}))

    if any(w in lower for w in ("strip control", "strip controls", "fix bad data", "format-control", "normalize control", "quarantine bad")):
        kind = "normalize_control_chars"
        if "quarantine" in lower:
            kind = "quarantine_and_rerun"
        elif "fix bad data" in lower or "open fix" in lower:
            kind = "open_bad_data_fix"
        planned.append(("remediate_validation", {"kind": kind}))
        planned.append(("navigate", {"screen": "transfer"}))

    if any(w in lower for w in ("new transfer", "start a transfer", "open transfer studio")) and not any(
        p[0] == "navigate" for p in planned
    ):
        planned.append(("start_transfer_studio", {}))

    if any(w in lower for w in ("capabilities", "what can transfer", "supported", "any to any")):
        planned.append(("get_transfer_capabilities", {}))

    transfer_intent = parse_transfer_intent(message)
    if transfer_intent:
        plan_only = transfer_intent.pop("plan_only", False)
        transfer_intent = {k: v for k, v in transfer_intent.items() if v}
        planned.append(("plan_transfer" if plan_only else "start_transfer", transfer_intent))

    if not transfer_intent and any(
        w in lower for w in ("plan transfer", "transfer plan", "route plan", "move from", "migrate from")
    ):
        src, dst = "", ""
        route = re.search(
            r"(?:from|source)\s+[\"']?([^\"'\n]+?)[\"']?\s+(?:to|into|->|destination)\s+[\"']?([^\"'\n]+?)[\"']?\s*$",
            lower,
        ) or re.search(
            r"move\s+[\"']?([^\"'\n]+?)[\"']?\s+(?:to|into)\s+[\"']?([^\"'\n]+?)[\"']?",
            lower,
        )
        if route:
            src, dst = route.group(1).strip()[:80], route.group(2).strip()[:80]
        planned.append(("plan_transfer_route", {
            "source": src or message[:80],
            "destination": dst or message[-80:],
            "workload": "unknown",
        }))

    if any(w in lower for w in ("mapping algorithm", "mapping guarantee", "100% accuracy", "correct columns", "assurance", "how does mapping")):
        planned.append(("explain_mapping_assurance", {}))

    if any(w in lower for w in ("sync mode", "cdc", "incremental", "dedupe", "full refresh")):
        planned.append(("recommend_sync_mode", {
            "workload": message[:80],
            "has_cursor": "cursor" in lower or "updated_at" in lower or "timestamp" in lower,
            "has_primary_key": "primary key" in lower or " id" in lower,
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

    # Live DB schema (saved connectors) — not uploaded datasets
    schema_of = re.search(
        r"(?:schema|columns|structure)\s+(?:of|for|on)\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?(.+?)[\"']?)?\s*$",
        lower,
    )
    columns_on = re.search(
        r"(?:what\s+)?columns\s+(?:are\s+)?(?:on|in|for)\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?(.+?)[\"']?)?",
        lower,
    )
    describe_table = re.search(
        r"describe\s+(?:table\s+)?[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?(.+?)[\"']?)?",
        lower,
    )
    # Natural paraphrases: "what's the airports table look like in Local Postgres"
    table_look = re.search(
        r"(?:what(?:'s| is)|show)\s+(?:the\s+)?"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+table"
        r"(?:\s+look(?:s)?\s+like)?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?))[\"']?\s*$",
        lower,
    ) or re.search(
        r"look(?:s)?\s+like\s+(?:the\s+)?[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:table|schema)"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?))[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:table|schema)\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?)[\"']?\s*$",
        lower,
    )
    if schema_of or columns_on or describe_table or table_look:
        m = schema_of or columns_on or describe_table or table_look
        table = (m.group(1) or "").strip()
        connector_name = (m.group(2) or "").strip() if m.lastindex and m.lastindex >= 2 else ""
        connector_name = re.sub(r"\b(please|now|table|schema)\b", "", connector_name).strip(" .,")
        args: dict = {"table": table}
        if connector_name:
            args["connector_name"] = connector_name
        planned.append(("introspect_connector_schema", args))

    tables_on = re.search(
        r"(?:list|show|what)\s+(?:tables|collections|objects)\s+(?:on|in|for)\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    ) or re.search(r"(?:tables|collections)\s+(?:on|in)\s+[\"']?(.+?)[\"']?", lower)
    if tables_on and "introspect_connector_schema" not in [p[0] for p in planned]:
        cname = tables_on.group(1).strip().strip("\"'")
        planned.append(("list_connector_objects", {"connector_name": cname}))

    # Aggregations: "count of orders by status", "average price in products",
    # "top 5 regions by revenue", "revenue by month". Parsed structurally; the
    # tool then grounds every name in the live schema.
    # Skip when the operator pasted / ran explicit SQL — that is run_query's job.
    from .aggregate_tools import parse_aggregation_request

    _explicit_sql_intent = bool(
        re.search(r"(?:run|execute)\s+(?:this\s+)?(?:sql|query)\b", lower)
        or _SQL_SELECT_SHAPE.match(message.strip())
        or _SQL_WITH_SHAPE.match(message.strip())
    )
    agg = parse_aggregation_request(message)
    if (
        agg is not None
        and not _explicit_sql_intent
        and "aggregate_data" not in [p[0] for p in planned]
    ):
        # Incomplete slots (missing table/column) still plan — the tool + recovery
        # clarify or auto-pick a unique table. Silent [] is a chatbot dead-end.
        planned.append(("aggregate_data", agg.as_tool_args()))

    # Sample / analyze live table data
    sample_m = re.search(
        r"(?:sample|preview|show(?:\s+me)?(?:\s+some)?(?:\s+data)?(?:\s+from)?|rows?\s+from|"
        r"analyze|profile|peek(?:\s+at)?)\s+(?:the\s+)?"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:table|collection)?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?))[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:sample|preview|show(?:\s+me)?(?:\s+data)?(?:\s+from)?|analyze)\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:data|rows)\s+(?:in|from)\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?"
        r"(?:\s+(?:on|in|from|using)\s+[\"']?([^\"'\n]+?))[\"']?",
        lower,
    )
    if sample_m and "sample_connector_object" not in [p[0] for p in planned]:
        # Don't steal pure schema introspect intents
        if "schema" not in lower and "columns" not in lower and "describe" not in lower:
            table = (sample_m.group(1) or "").strip()
            cname = (sample_m.group(2) or "").strip() if sample_m.lastindex and sample_m.lastindex >= 2 else ""
            cname = re.sub(r"\b(table|schema|collection)\b", "", cname)
            cname = _clean_connector_phrase(cname)
            if table and table not in {"data", "rows", "me", "some", "the"}:
                args = {"table": table, "analyze": "analy" in lower or "profile" in lower}
                if cname:
                    args["connector_name"] = cname
                planned.append(("sample_connector_object", args))

    # Explicit SQL — either "run this sql: …" or a genuinely pasted statement.
    explicit_sql = re.search(
        r"(?:run|execute)\s+(?:this\s+)?(?:sql|query)\s*[:\-]?\s*(.+)$",
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
    if analyze_follow and "analyze_result" not in [p[0] for p in planned]:
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
            if col:
                planned.append(("filter_result", args))

    diff_m = re.search(
        r"diff\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?\s+vs\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    ) or re.search(
        r"(?:diff|compare)\s+(?:schema(?:s)?\s+(?:of\s+)?)?[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+"
        r"(?:on|in)\s+[\"']?(.+?)[\"']?\s+(?:vs|versus|and|with)\s+"
        r"[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?",
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
        r"map\s+[\"']?([a-zA-Z0-9_.-]+)[\"']?\s+(?:on|in)\s+[\"']?(.+?)[\"']?\s+to\s+[\"']?(.+?)[\"']?\s*$",
        lower,
    )
    if map_m and "map_connector_schemas" not in [p[0] for p in planned]:
        if map_m.lastindex and map_m.lastindex >= 4:
            planned.append(("map_connector_schemas", {
                "source_table": map_m.group(1).strip(),
                "source_connector_name": map_m.group(2).strip(),
                "dest_table": map_m.group(3).strip(),
                "dest_connector_name": map_m.group(4).strip(),
            }))
        else:
            planned.append(("map_connector_schemas", {
                "source_table": map_m.group(1).strip(),
                "source_connector_name": map_m.group(2).strip(),
                "dest_connector_name": map_m.group(3).strip(),
            }))

    if any(w in lower for w in (
        "quality rules", "quality gates", "data quality", "profile rules",
        "suggest improvements", "suggestions for my data", "how can i improve",
        "data recommendations", "recommend fixes",
    )):
        planned.append(("profile_quality_rules", {}))
        if any(w in lower for w in ("suggest", "recommend", "improve", "fix")):
            planned.append(("search_knowledge", {"query": message[:200]}))

    # Uploaded dataset compare — only when not already a live schema diff
    if "diff_schemas" not in [p[0] for p in planned]:
        compare = re.search(r"compare\s+(\w+)\s+(?:and|vs|with|to)\s+(\w+)", lower)
        if compare and "schema" not in lower:
            planned.append(("compare_datasets", {"dataset_a": compare.group(1), "dataset_b": compare.group(2)}))

    search = re.search(r"(?:search|find)\s+(?:for\s+)?['\"]?(\w+)['\"]?", lower)
    if search and "search_data" not in [p[0] for p in planned]:
        # Avoid treating connector/table lookups as dataset search
        if not any(p[0] in _LIVE_SCHEMA_TOOLS for p in planned):
            planned.append(("search_data", {"query": search.group(1)}))

    analyst = get_data_analyst()
    hint = analyst.extract_dataset_hint(message)
    data_signals = [
        "analyze", "what's in", "what is in", "tell me about", "pii",
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

    if not planned and _looks_like_domain_knowledge_query(lower):
        planned.append(("search_knowledge", {"query": message[:200]}))

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
