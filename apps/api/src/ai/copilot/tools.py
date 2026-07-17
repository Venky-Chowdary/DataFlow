"""Data Pilot — app tools the agent can invoke (like Cursor/Claude tool use)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable

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
        "name": "list_jobs",
        "description": "List recent transfer jobs with status and record counts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max jobs to return", "default": 10},
            },
            "required": [],
        },
    },
    {
        "name": "get_transfer_capabilities",
        "description": "Show supported source→destination transfer combinations.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "navigate",
        "description": "Navigate the user to an app screen: dashboard, transfer, connectors, jobs, settings.",
        "input_schema": {
            "type": "object",
            "properties": {
                "screen": {
                    "type": "string",
                    "enum": ["dashboard", "pilot", "transfer", "connectors", "jobs", "mcp", "settings"],
                },
            },
            "required": ["screen"],
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
        "description": "Search 620+ connector catalog — Postgres, Snowflake, Shopify, S3, etc.",
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
]

TOOL_FAMILIES: list[dict] = [
    {
        "id": "discover",
        "label": "Discover",
        "tools": ["list_datasets", "search_data", "search_connectors", "search_knowledge"],
        "generated_actions": 620,
    },
    {
        "id": "profile",
        "label": "Profile",
        "tools": ["analyze_dataset", "compare_datasets", "profile_quality_rules"],
        "generated_actions": 180,
    },
    {
        "id": "move",
        "label": "Move",
        "tools": ["plan_transfer_route", "get_transfer_capabilities", "recommend_sync_mode"],
        "generated_actions": 720,
    },
    {
        "id": "govern",
        "label": "Govern",
        "tools": ["explain_mapping_assurance", "inspect_schema_policy"],
        "generated_actions": 140,
    },
    {
        "id": "operate",
        "label": "Operate",
        "tools": ["list_jobs", "navigate"],
        "generated_actions": 80,
    },
]


def get_tool_registry() -> dict:
    tool_names = {t["name"] for t in TOOL_DEFINITIONS}
    families = []
    generated_total = 0
    for family in TOOL_FAMILIES:
        available = [name for name in family["tools"] if name in tool_names]
        generated_total += int(family.get("generated_actions", 0))
        families.append({
            **family,
            "tools": available,
            "tool_count": len(available),
        })
    return {
        "tool_count": len(TOOL_DEFINITIONS),
        "generated_action_count": generated_total,
        "total_routable_actions": len(TOOL_DEFINITIONS) + generated_total,
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
            "list_jobs": self._list_jobs,
            "get_transfer_capabilities": self._get_capabilities,
            "navigate": self._navigate,
            "compare_datasets": self._compare_datasets,
            "search_connectors": self._search_connectors,
            "search_knowledge": self._search_knowledge,
            "plan_transfer_route": self._plan_transfer_route,
            "explain_mapping_assurance": self._explain_mapping_assurance,
            "recommend_sync_mode": self._recommend_sync_mode,
            "inspect_schema_policy": self._inspect_schema_policy,
            "profile_quality_rules": self._profile_quality_rules,
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
        from ...services.mongodb_service import get_mongodb_service
        mongo = get_mongodb_service()
        connectors = mongo.list_connectors()
        summary = [
            {
                "id": c.get("id", c.get("_id", "")),
                "name": c.get("name"),
                "type": c.get("type"),
                "host": c.get("host"),
                "database": c.get("database"),
                "status": c.get("status", "unknown"),
            }
            for c in connectors
        ]
        return ToolResult(name="list_connectors", success=True, output={"connectors": summary, "count": len(summary)})

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
                "created_at": str(j.get("created_at", "")),
            }
            for j in jobs
        ]
        return ToolResult(name="list_jobs", success=True, output={"jobs": summary, "count": len(summary)})

    def _get_capabilities(self) -> ToolResult:
        from ...transfer.registry import get_capabilities
        return ToolResult(name="get_transfer_capabilities", success=True, output=get_capabilities())

    def _navigate(self, screen: str = "dashboard") -> ToolResult:
        valid = {"dashboard", "pilot", "transfer", "connectors", "jobs", "mcp", "settings"}
        if screen not in valid:
            return ToolResult(name="navigate", success=False, output=None, error=f"Invalid screen: {screen}")
        return ToolResult(name="navigate", success=True, output={"screen": screen, "action": "navigate"})

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

    def _search_knowledge(self, query: str = "") -> ToolResult:
        if not query.strip():
            return ToolResult(name="search_knowledge", success=False, output=None, error="query required")
        try:
            from ..rag.pipeline import get_rag_pipeline
            rag = get_rag_pipeline()
            rag.ingestion.ensure_knowledge_loaded()
            result = rag.retriever.retrieve(query, n_results=6)
            hits = [
                {"text": d.text[:600], "score": round(d.score, 3), "type": d.metadata.get("type", "")}
                for d in result.documents
            ]
            return ToolResult(name="search_knowledge", success=True, output={"query": query, "hits": hits, "count": len(hits)})
        except Exception as e:
            return ToolResult(name="search_knowledge", success=False, output=None, error=str(e))

    def _plan_transfer_route(self, source: str = "", destination: str = "", workload: str = "unknown") -> ToolResult:
        source_l = source.lower()
        dest_l = destination.lower()
        workload_l = workload.lower()
        is_file_source = any(x in source_l for x in ("csv", "json", "jsonl", "tsv", "file", "s3"))
        is_file_dest = any(x in dest_l for x in ("csv", "json", "jsonl", "parquet", "file", "s3"))
        is_cdc = workload_l == "cdc" or any(x in source_l for x in ("postgres", "mysql", "sql server", "oracle"))
        route_type = (
            "file_to_file" if is_file_source and is_file_dest else
            "file_to_database" if is_file_source else
            "database_to_file" if is_file_dest else
            "database_to_database"
        )
        gates = [
            "source_contract",
            "schema_inference",
            "semantic_mapping",
            "type_compatibility",
            "destination_probe",
            "dry_run_transform",
            "capacity_check",
            "row_count_checksum_reconciliation",
        ]
        return ToolResult(name="plan_transfer_route", success=True, output={
            "route_type": route_type,
            "source": source or "source not specified",
            "destination": destination or "destination not specified",
            "recommended_sync": "cdc_incremental" if is_cdc and workload_l != "full_load" else "full_refresh_overwrite",
            "schema_policy": "detect_before_run_pause_on_breaking_changes",
            "required_gates": gates,
            "review_required_when": [
                "mapping score gap below threshold",
                "primary key or cursor removed",
                "type coercion is lossy",
                "destination contract conflicts",
            ],
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

    def _inspect_schema_policy(self, change_type: str = "unknown", auto_apply: bool = False) -> ToolResult:
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
            "change_type": change_type,
            "severity": severity,
            "auto_apply": auto_apply and severity == "non_breaking",
            "action": action,
            "operator_review": severity != "non_breaking" or not auto_apply,
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
        })


def infer_tools_from_message(message: str) -> list[tuple[str, dict]]:
    """Local tool routing when no LLM tool-use is available."""
    lower = message.lower()
    planned: list[tuple[str, dict]] = []

    nav_map = {
        "pilot": ["data pilot", "automate", "new chat", "go to pilot"],
        "transfer": ["start transfer", "new transfer", "upload", "move data", "go to transfer"],
        "jobs": ["show jobs", "my jobs", "job history", "recent transfers", "go to jobs", "transfer jobs", "show my transfer"],
        "connectors": ["connectors", "connections", "add connector", "go to connectors"],
        "dashboard": ["dashboard", "overview", "home"],
        "settings": ["settings", "sso", "security settings"],
    }
    for screen, phrases in nav_map.items():
        if re.search(rf"(go to|open|show|take me to|navigate to)\s+{re.escape(screen.replace('_', ' '))}", lower):
            planned.append(("navigate", {"screen": screen}))
            break
        if any(p in lower for p in phrases) and any(w in lower for w in ("go", "open", "show", "take me", "navigate")):
            planned.append(("navigate", {"screen": screen}))
            break

    if any(w in lower for w in ("all datasets", "what data", "available data", "list datasets", "what files")):
        planned.append(("list_datasets", {}))

    setup = re.search(r"(?:set up|setup|configure|connect)\s+(.+?)\s+as\s+(?:a\s+)?(?:source|destination)", lower)
    if setup:
        q = setup.group(1).strip()
        role = "destination" if "destination" in lower else "source"
        planned.append(("search_connectors", {"query": q[:40], "role": role}))
    elif any(w in lower for w in ("connectors", "connections")) and "go" not in lower:
        if any(w in lower for w in ("search", "find", "source", "destination", "setup", "postgres", "snowflake", "shopify")):
            role = "destination" if "destination" in lower or "warehouse" in lower else "source" if "source" in lower else "all"
            q = re.sub(r".*(?:search|find|setup)\s+", "", lower).strip() or lower
            planned.append(("search_connectors", {"query": q[:40], "role": role}))
        else:
            planned.append(("list_connectors", {}))

    if any(w in lower for w in ("jobs", "transfers", "history")) and "dataset" not in lower:
        planned.append(("list_jobs", {"limit": 10}))

    if any(w in lower for w in ("capabilities", "what can transfer", "supported", "any to any")):
        planned.append(("get_transfer_capabilities", {}))

    if any(w in lower for w in ("plan transfer", "transfer plan", "route plan", "move from", "migrate from")):
        planned.append(("plan_transfer_route", {"source": message[:80], "destination": message[-80:], "workload": "unknown"}))

    if any(w in lower for w in ("mapping algorithm", "mapping guarantee", "100% accuracy", "accuracy", "correct columns", "assurance")):
        planned.append(("explain_mapping_assurance", {}))

    if any(w in lower for w in ("sync mode", "cdc", "incremental", "dedupe", "full refresh")):
        planned.append(("recommend_sync_mode", {
            "workload": message[:80],
            "has_cursor": "cursor" in lower or "updated_at" in lower or "timestamp" in lower,
            "has_primary_key": "primary key" in lower or "id" in lower,
            "needs_history": "history" in lower or "audit" in lower,
        }))

    if any(w in lower for w in ("schema drift", "schema change", "new column", "removed column", "type change")):
        change_type = "unknown"
        if "new column" in lower:
            change_type = "new_column"
        elif "removed column" in lower:
            change_type = "removed_column"
        elif "type change" in lower:
            change_type = "type_change"
        planned.append(("inspect_schema_policy", {"change_type": change_type, "auto_apply": "auto" in lower}))

    if any(w in lower for w in ("quality rules", "quality gates", "data quality", "profile rules")):
        planned.append(("profile_quality_rules", {}))

    compare = re.search(r"compare\s+(\w+)\s+(?:and|vs|with|to)\s+(\w+)", lower)
    if compare:
        planned.append(("compare_datasets", {"dataset_a": compare.group(1), "dataset_b": compare.group(2)}))

    search = re.search(r"(?:search|find)\s+(?:for\s+)?['\"]?(\w+)['\"]?", lower)
    if search and "search_data" not in [p[0] for p in planned]:
        planned.append(("search_data", {"query": search.group(1)}))

    analyst = get_data_analyst()
    hint = analyst.extract_dataset_hint(message)
    data_signals = [
        "analyze", "what's in", "what is in", "tell me about", "pii", "columns",
        "schema", "preview", "sample", "quality", "how many rows",
    ]
    if hint and any(s in lower for s in data_signals):
        planned.append(("analyze_dataset", {"dataset_name": hint}))

    if not planned and len(message) > 12:
        planned.append(("search_knowledge", {"query": message[:200]}))

    return planned


def format_tool_results_for_llm(results: list[ToolResult]) -> str:
    parts = []
    for r in results:
        if r.success:
            parts.append(f"Tool `{r.name}` result:\n{json.dumps(r.output, indent=2, default=str)[:4000]}")
        else:
            parts.append(f"Tool `{r.name}` failed: {r.error}")
    return "\n\n".join(parts)


_tools: DataPilotTools | None = None


def get_pilot_tools() -> DataPilotTools:
    global _tools
    if _tools is None:
        _tools = DataPilotTools()
    return _tools
