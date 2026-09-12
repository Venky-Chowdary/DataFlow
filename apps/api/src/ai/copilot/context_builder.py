"""Build full context for Datawrap Pilot — every data source in the platform."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from services.brand_env import getenv_brand
from .data_analyst import get_data_analyst

# The reads below decorate an answer; they never carry it. A tool that must see
# live data (``list_connector_objects``, ``get_job``) goes through its own
# permissioned path, so a missing connector list costs the operator a less
# specific preamble and nothing more. That is why each one is bounded: a
# metadata store which has stopped answering must cost a turn one wait, and
# 3 seconds is the bound the RAG read here already ran under.
CONTEXT_BUDGET_S = 3.0

# It must not cost the *next* turn the same wait either. ``MongoService.connect``
# builds a fresh client per call and forgets that the last one failed, so with no
# memo an unreachable store is redialled at full server-selection timeout on
# every question asked. The cooldown trades context that may be this stale for
# turns that stay answerable while the store is down.
CONTEXT_COOLDOWN_S = 30.0

_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pilot-ctx")
_cooldown_lock = threading.Lock()
_cooling: dict[str, float] = {}


def _cooling_off(source: str) -> bool:
    with _cooldown_lock:
        return time.monotonic() < _cooling.get(source, 0.0)


def _begin_cooldown(source: str) -> None:
    with _cooldown_lock:
        _cooling[source] = time.monotonic() + CONTEXT_COOLDOWN_S


def reset_context_cooldowns() -> None:
    """Forget which sources are cooling off, so the next turn redials them."""
    with _cooldown_lock:
        _cooling.clear()


def _gather(
    reads: dict[str, tuple[Callable[[], Any], Any]],
) -> tuple[dict[str, Any], list[str]]:
    """Run independent context reads together under one shared deadline.

    Submitting every read before collecting any is what makes the budget a
    budget: run serially, three 3-second reads cost nine seconds, which is the
    stall this exists to bound. A source already known to be down is not
    submitted at all, and one that misses the deadline is abandoned rather than
    waited on — a started read cannot be cancelled, so the turn walks away from
    it and the thread finishes into nothing.

    The fallback lives here instead of inside each read so that a read which
    fails is distinguishable from one that legitimately found nothing: an empty
    workspace must not look like an outage, and an outage must not be reported
    as an empty workspace.
    """
    degraded = [name for name in reads if _cooling_off(name)]
    futures = {
        name: _pool.submit(read)
        for name, (read, _fallback) in reads.items()
        if name not in degraded
    }
    deadline = time.monotonic() + CONTEXT_BUDGET_S
    gathered: dict[str, Any] = {}
    for name, future in futures.items():
        try:
            gathered[name] = future.result(
                timeout=max(0.0, deadline - time.monotonic())
            )
        except Exception:
            _begin_cooldown(name)
            degraded.append(name)
    for name in degraded:
        gathered[name] = reads[name][1]
    return gathered, degraded


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

        # RAG is optional — hub/embedding init must never block chat.
        rag_on = (getenv_brand("PILOT_RAG") or "").lower() in {"1", "true", "on", "yes"}
        reads: dict[str, tuple[Callable[[], Any], Any]] = {
            "connectors": (self._read_connectors, []),
            "recent_jobs": (self._read_jobs, []),
            "transfer_capabilities": (self._read_capabilities, {}),
        }
        if rag_on and message.strip():
            reads["rag_knowledge"] = (lambda: self._read_rag(message), [])
        gathered, degraded = _gather(reads)
        connectors = gathered["connectors"]
        jobs = gathered["recent_jobs"]
        capabilities = gathered["transfer_capabilities"]
        rag_snippets = gathered.get("rag_knowledge") or []

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
            # Skip heavy per-dataset PII/quality scans during chat context build.
            # Tools (analyze_dataset) still run that work on demand.
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
            # Which sources could not be read this turn. Consumers must prefer
            # this over the length of an empty list: "0 connectors" is a claim
            # about the workspace and this is a claim about the read.
            "context_degraded": degraded,
        }

    def to_system_context(self, ctx: dict) -> str:
        """Anthropic-style system context block."""
        # A source that could not be read is reported as unread. Printing its
        # length instead turns an outage into the false statement "0 saved
        # connectors", which the model would then repeat to the operator.
        degraded = set(ctx.get("context_degraded") or ())

        def _count(key: str, noun: str, value: str) -> str:
            if key in degraded:
                return f"- {noun}: could not be read this turn — say so, do not report zero"
            return f"- {value}"

        parts = [
            "## Platform State",
            f"- {ctx['dataset_count']} datasets indexed (uploads + fixtures + transfers)",
            f"- {ctx.get('training_schema_profiles', 620)}+ connector schema profiles in training knowledge",
            _count("connectors", "saved connectors", f"{len(ctx['connectors'])} saved connectors"),
            _count("recent_jobs", "recent transfer jobs", f"{len(ctx['recent_jobs'])} recent transfer jobs"),
            _count(
                "transfer_capabilities",
                "live transfer routes",
                f"{ctx['transfer_capabilities']['live_count']} live transfer routes (any file/DB/warehouse)",
            ),
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

    # The reads below raise on failure on purpose. ``_gather`` owns the budget,
    # the fallback and the record that a source was unreachable; swallowing the
    # error here would hand back an empty list that reads as a true answer.
    def _read_connectors(self) -> list[dict]:
        from ...services.mongodb_service import get_mongodb_service
        return [
            {"name": c.get("name"), "type": c.get("type"), "database": c.get("database"), "host": c.get("host")}
            for c in get_mongodb_service().list_connectors()
        ]

    def _read_jobs(self) -> list[dict]:
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

    def _read_capabilities(self) -> dict:
        from ...transfer.registry import get_capabilities
        return get_capabilities()

    def _read_rag(self, message: str) -> list[str]:
        # Bounded like the rest, which is what keeps an embedding-model download
        # or a hub retry from becoming the turn's latency.
        from ..rag.pipeline import get_rag_pipeline

        rag = get_rag_pipeline()
        rag.ingestion.ensure_knowledge_loaded()
        result = rag.retriever.retrieve(message, n_results=4)
        return [d.text[:400] for d in result.documents]

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
