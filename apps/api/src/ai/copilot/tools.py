"""Data Pilot — app tools the agent can invoke (like Cursor/Claude tool use)."""

from __future__ import annotations
import json
import re
from dataclasses import dataclass, field
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
]


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
