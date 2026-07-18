"""Build full context for Data Pilot — every data source in the platform."""

from __future__ import annotations

from .data_analyst import get_data_analyst


class PilotContextBuilder:
    """Assembles everything the agent needs to answer any data question."""

    def build(self, data_context: dict | None = None, message: str = "") -> dict:
        analyst = get_data_analyst()
        datasets = analyst.list_datasets()

        session = None
        if data_context and (data_context.get("columns") or data_context.get("preflight_run_id") or data_context.get("job_id")):
            session = {
                "name": data_context.get("name") or data_context.get("filename", "active upload"),
                "columns": data_context.get("columns", []),
                "row_count": data_context.get("row_count", 0),
                "samples": data_context.get("samples") or data_context.get("column_samples") or {},
                "preflight_run_id": data_context.get("preflight_run_id"),
                "job_id": data_context.get("job_id"),
                "validation_status": data_context.get("validation_status"),
                "route": data_context.get("route"),
                "blockers": data_context.get("blockers") or [],
            }

        connectors = self._safe_connectors()
        jobs = self._safe_jobs()
        capabilities = self._safe_capabilities()
        rag_snippets = self._safe_rag(message)

        dataset_summaries = []
        for ds in datasets[:12]:
            entry = {
                "name": ds["name"],
                "source": ds["source"],
                "columns": ds["columns"][:15],
                "column_count": ds["column_count"],
                "row_count": ds["row_count"],
                "industry": ds.get("industry"),
            }
            if message and self._dataset_relevant(ds, message):
                schema = analyst.resolve_dataset(ds["name"])
                if schema:
                    insight = analyst.analyze_schema(schema)
                    entry["pii_columns"] = insight.pii_columns
                    entry["quality_score"] = insight.quality_score
            dataset_summaries.append(entry)

        return {
            "session_data": session,
            "datasets": dataset_summaries,
            "dataset_count": len(datasets),
            "training_schema_profiles": self._training_profile_count(),
            "connectors": connectors,
            "recent_jobs": jobs,
            "transfer_capabilities": {
                "live_count": len(capabilities.get("live_combinations", [])),
                "operations": capabilities.get("operations", []),
                "auto_ddl": capabilities.get("auto_ddl"),
            },
            "rag_knowledge": rag_snippets,
        }

    def to_system_context(self, ctx: dict) -> str:
        """Anthropic-style system context block."""
        parts = [
            "## Platform State",
            f"- {ctx['dataset_count']} datasets indexed (uploads + fixtures + transfers)",
            f"- {ctx.get('training_schema_profiles', 620)}+ connector schema profiles in training knowledge",
            f"- {len(ctx['connectors'])} saved connectors",
            f"- {len(ctx['recent_jobs'])} recent transfer jobs",
            f"- {ctx['transfer_capabilities']['live_count']} live transfer routes (any file/DB/warehouse)",
            "",
        ]

        if ctx.get("session_data"):
            s = ctx["session_data"]
            parts.extend([
                "## Active User Session",
                f"Dataset: **{s['name']}** — {s.get('row_count', 0):,} rows, {len(s.get('columns') or [])} columns",
                f"Columns: {', '.join((s.get('columns') or [])[:20])}",
            ])
            if s.get("preflight_run_id"):
                parts.append(f"Preflight run ID: `{s['preflight_run_id']}` (use get_preflight_run)")
            if s.get("job_id"):
                parts.append(f"Transfer job ID: `{s['job_id']}` (use get_job)")
            if s.get("validation_status"):
                parts.append(f"Validation status: **{s['validation_status']}**")
            if s.get("route"):
                parts.append(f"Route: {s['route']}")
            for b in (s.get("blockers") or [])[:4]:
                parts.append(f"- Blocker: {b}")
            parts.append("")
            parts.append(
                "When validation is blocked, prefer remediate_validation "
                "(normalize_control_chars / open_bad_data_fix / quarantine_and_rerun)."
            )
            parts.append("")

        if ctx.get("datasets"):
            parts.append("## Available Datasets")
            for ds in ctx["datasets"][:8]:
                line = f"- **{ds['name']}** ({ds['source']}): {ds['column_count']} cols"
                if ds.get("row_count"):
                    line += f", {ds['row_count']:,} rows"
                if ds.get("pii_columns"):
                    line += f", PII: {', '.join(ds['pii_columns'][:4])}"
                parts.append(line)
            parts.append("")

        if ctx.get("connectors"):
            parts.append("## Connectors")
            for c in ctx["connectors"][:6]:
                parts.append(f"- {c.get('name')} ({c.get('type')}) → {c.get('database', c.get('host', ''))}")
            parts.append("")

        if ctx.get("recent_jobs"):
            parts.append("## Recent Jobs")
            for j in ctx["recent_jobs"][:5]:
                jid = j.get("id") or "?"
                parts.append(
                    f"- `{jid}` · {j.get('source', '?')} → {j.get('destination', '?')}: "
                    f"{j.get('status')} ({j.get('records', 0):,} records)"
                )
            parts.append("")
            parts.append("When the user cites a job ID, call get_job before answering.")
            parts.append("")

        if ctx.get("rag_knowledge"):
            parts.append("## Trained Knowledge")
            parts.extend(ctx["rag_knowledge"][:4])
            parts.append("")

        return "\n".join(parts)

    def _dataset_relevant(self, ds: dict, message: str) -> bool:
        lower = message.lower()
        name = ds["name"].lower()
        return name in lower or (ds.get("industry") or "") in lower

    def _safe_connectors(self) -> list[dict]:
        try:
            from ...services.mongodb_service import get_mongodb_service
            return [
                {"name": c.get("name"), "type": c.get("type"), "database": c.get("database"), "host": c.get("host")}
                for c in get_mongodb_service().list_connectors()
            ]
        except Exception:
            return []

    def _safe_jobs(self) -> list[dict]:
        try:
            from ...services.mongodb_service import get_mongodb_service
            return [
                {
                    "id": str(j.get("_id", j.get("id", ""))),
                    "source": j.get("source_name"),
                    "destination": j.get("destination_collection") or j.get("destination_type"),
                    "status": j.get("status"),
                    "records": j.get("records_processed", 0),
                    "error": (j.get("error") or "")[:160] or None,
                }
                for j in get_mongodb_service().list_jobs(limit=8)
            ]
        except Exception:
            return []

    def _safe_capabilities(self) -> dict:
        try:
            from ...transfer.registry import get_capabilities
            return get_capabilities()
        except Exception:
            return {}

    def _safe_rag(self, message: str) -> list[str]:
        if not message.strip():
            return []
        try:
            from ..rag.pipeline import get_rag_pipeline
            rag = get_rag_pipeline()
            rag.ingestion.ensure_knowledge_loaded()
            result = rag.retriever.retrieve(message, n_results=4)
            return [d.text[:400] for d in result.documents]
        except Exception:
            return []

    def _training_profile_count(self) -> int:
        try:
            from ..training.universal_source_registry import get_universal_schema_count
            return get_universal_schema_count().get("estimated_schema_profiles", 620)
        except Exception:
            return 620


_builder: PilotContextBuilder | None = None


def get_context_builder() -> PilotContextBuilder:
    global _builder
    if _builder is None:
        _builder = PilotContextBuilder()
    return _builder
