"""
DataTransfer.space — Copilot Data Analyst

Analyzes real universal data and composes natural-language responses
about columns, values, PII, quality — not just transfer steps.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .. import analyze_schema_enhanced, generate_mappings_enhanced
from ..training.universal_data_feeder import UniversalDataFeeder, UniversalSchema


@dataclass
class DataInsight:
    dataset_name: str
    columns: list[str]
    row_count: int
    quality_score: float
    pii_columns: list[str]
    column_details: list[dict] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    sample_preview: list[dict] = field(default_factory=list)


class CopilotDataAnalyst:
    """Analyze actual data and speak about it in natural language."""

    def __init__(self):
        self.feeder = UniversalDataFeeder()
        self._cache: dict[str, DataInsight] = {}

    def list_datasets(self, include_catalog: bool = False) -> list[dict]:
        schemas = self.feeder.feed_all()
        if not include_catalog:
            schemas = [s for s in schemas if s.source != "catalog"]
        return [
            {
                "name": s.name,
                "source": s.source,
                "columns": s.columns,
                "column_count": len(s.columns),
                "row_count": s.row_count,
                "industry": s.industry,
                "file_type": s.file_type,
            }
            for s in schemas
        ]

    def resolve_dataset(self, hint: str | None) -> UniversalSchema | None:
        """Find a dataset by name hint from message or context."""
        schemas = self.feeder.feed_all()
        if not schemas:
            return None

        if not hint:
            uploads = [s for s in schemas if s.source == "upload"]
            return uploads[0] if uploads else schemas[0]

        hint_lower = hint.lower().replace("_", " ").replace("-", " ")
        for schema in schemas:
            name_lower = schema.name.lower()
            if hint_lower in name_lower or name_lower in hint_lower:
                return schema
            for col in schema.columns:
                if hint_lower in col.lower():
                    return schema

        industry_map = {
            "hr": "hr", "human": "hr", "employee": "hr",
            "logistics": "logistics", "shipping": "logistics", "freight": "logistics",
            "finance": "finance", "payment": "finance", "transaction": "finance",
            "retail": "retail", "order": "retail", "customer": "retail",
            "health": "healthcare", "patient": "healthcare", "medical": "healthcare",
        }
        for keyword, industry in industry_map.items():
            if keyword in hint_lower:
                for s in schemas:
                    if s.industry == industry or industry in s.name.lower():
                        return s

        return None

    def analyze_schema(self, schema: UniversalSchema) -> DataInsight:
        cache_key = schema.name
        if cache_key in self._cache:
            return self._cache[cache_key]

        column_samples = schema.samples or {}
        if not column_samples and schema.columns:
            column_samples = {c: [] for c in schema.columns}

        analysis = analyze_schema_enhanced(column_samples)

        column_details = []
        for col in analysis.columns:
            samples = column_samples.get(col.column_name, [])[:3]
            column_details.append({
                "name": col.column_name,
                "semantic_type": col.semantic_type or col.inferred_type,
                "confidence": col.confidence,
                "is_pii": col.is_pii,
                "compliance": [c.value for c in col.compliance] if col.compliance else [],
                "samples": samples,
            })

        preview = []
        if schema.samples:
            for i in range(min(3, max(len(v) for v in schema.samples.values()) if schema.samples else 0)):
                row = {}
                for col, vals in schema.samples.items():
                    if i < len(vals):
                        row[col] = vals[i]
                if row:
                    preview.append(row)

        insight = DataInsight(
            dataset_name=schema.name,
            columns=schema.columns,
            row_count=schema.row_count,
            quality_score=analysis.quality_score,
            pii_columns=analysis.pii_columns,
            column_details=column_details,
            recommendations=analysis.recommendations,
            sample_preview=preview,
        )
        self._cache[cache_key] = insight
        return insight

    def analyze_context(
        self,
        data_context: dict | None,
        dataset_hint: str | None = None,
    ) -> DataInsight | None:
        """Analyze from frontend context or uploaded files."""
        if data_context and data_context.get("columns"):
            cols = data_context["columns"]
            samples = data_context.get("samples") or data_context.get("column_samples") or {}
            schema = UniversalSchema(
                name=data_context.get("name", data_context.get("filename", "your data")),
                source="session",
                columns=cols if isinstance(cols, list) else list(cols),
                samples=samples if isinstance(samples, dict) else {},
                row_count=data_context.get("row_count", 0),
            )
            return self.analyze_schema(schema)

        schema = self.resolve_dataset(dataset_hint)
        if schema:
            return self.analyze_schema(schema)
        return None

    def compose_response(
        self,
        insight: DataInsight,
        user_message: str,
        intent: str,
    ) -> str:
        """Turn data insights into conversational natural language."""
        lower = user_message.lower()
        name = insight.dataset_name.replace("_", " ").replace("sample ", "")

        if intent == "pii_compliance" or any(w in lower for w in ("pii", "sensitive", "personal", "privacy", "gdpr", "hipaa")):
            return self._compose_pii_response(insight, name)

        if intent == "mapping_help" or any(w in lower for w in ("map", "column", "field", "schema", "type")):
            return self._compose_schema_response(insight, name)

        if any(w in lower for w in ("how many", "count", "rows", "records", "size")):
            return self._compose_stats_response(insight, name)

        if any(w in lower for w in ("sample", "preview", "show", "look like", "example")):
            return self._compose_preview_response(insight, name)

        return self._compose_overview_response(insight, name)

    def _compose_overview_response(self, insight: DataInsight, name: str) -> str:
        col_summary = ", ".join(f"**{c['name']}** ({c['semantic_type']})" for c in insight.column_details[:6])
        if len(insight.column_details) > 6:
            col_summary += f", and {len(insight.column_details) - 6} more"

        parts = [
            f"I've analyzed your **{name}** dataset. Here's what I found:",
            "",
            f"It has **{len(insight.columns)} columns**"
            + (f" and **{insight.row_count:,} rows**" if insight.row_count else ""),
            f" with a data quality score of **{insight.quality_score:.0f}%**.",
            "",
            f"The columns break down as: {col_summary}.",
        ]

        if insight.pii_columns:
            parts.extend([
                "",
                f"Heads up — I detected **{len(insight.pii_columns)} PII column(s)**: "
                f"{', '.join(f'`{c}`' for c in insight.pii_columns)}. "
                f"These need compliance handling before any transfer.",
            ])

        if insight.recommendations:
            parts.extend(["", insight.recommendations[0]])

        parts.extend([
            "",
            "Ask me about specific columns, PII details, or mapping suggestions — I work directly on your data.",
        ])
        return "\n".join(parts)

    def _compose_pii_response(self, insight: DataInsight, name: str) -> str:
        if not insight.pii_columns:
            pii_details = [c for c in insight.column_details if c["is_pii"]]
            if not pii_details:
                return (
                    f"Good news — I scanned **{name}** and didn't find columns that match known PII patterns. "
                    f"Your {len(insight.columns)} columns appear to be non-sensitive identifiers and metrics. "
                    f"Quality score: {insight.quality_score:.0f}%."
                )

        lines = [
            f"In **{name}**, I found **{len(insight.pii_columns)} sensitive column(s)**:",
            "",
        ]
        for col in insight.column_details:
            if col["is_pii"] or col["name"] in insight.pii_columns:
                compliance = ", ".join(col["compliance"]) if col["compliance"] else "GDPR"
                sample = col["samples"][0] if col["samples"] else "—"
                masked = self._mask_sample(sample, col["semantic_type"])
                lines.append(
                    f"• **{col['name']}** — {col['semantic_type']} ({compliance}). "
                    f"Sample: `{masked}`"
                )

        lines.extend([
            "",
            "I'd recommend masking or excluding these fields in your destination, "
            "or enabling encryption for the transfer.",
        ])
        return "\n".join(lines)

    def _compose_schema_response(self, insight: DataInsight, name: str) -> str:
        lines = [f"Here's the schema for **{name}**:", ""]
        for col in insight.column_details:
            conf = f"{col['confidence'] * 100:.0f}%"
            pii_tag = " · PII" if col["is_pii"] else ""
            lines.append(f"• **{col['name']}** → {col['semantic_type']} (confidence {conf}{pii_tag})")

        if len(insight.column_details) >= 2:
            src = [c["name"] for c in insight.column_details]
            tgt = [c["name"].replace("cust_", "customer_").replace("_addr", "_address") for c in insight.column_details]
            try:
                mappings = generate_mappings_enhanced(src, tgt, insight.column_details[0].get("samples") and {
                    c["name"]: c["samples"] for c in insight.column_details if c["samples"]
                })
                high = [m for m in mappings if m.confidence >= 0.85][:3]
                if high:
                    lines.extend(["", "Strong mapping candidates I see:"])
                    for m in high:
                        lines.append(f"• `{m.source_column}` → `{m.target_column}` ({m.confidence * 100:.0f}%)")
            except Exception:
                pass

        return "\n".join(lines)

    def _compose_stats_response(self, insight: DataInsight, name: str) -> str:
        return (
            f"**{name}** contains **{insight.row_count:,} rows** across **{len(insight.columns)} columns**. "
            f"Data quality score is **{insight.quality_score:.0f}%**. "
            f"{len(insight.pii_columns)} column(s) flagged as PII."
        )

    def _compose_preview_response(self, insight: DataInsight, name: str) -> str:
        if not insight.sample_preview:
            cols = insight.column_details[:4]
            preview_lines = []
            for col in cols:
                sample = col["samples"][0] if col["samples"] else "—"
                preview_lines.append(f"  {col['name']}: {sample}")
            return (
                f"Here's a snapshot of **{name}**:\n\n"
                + "\n".join(preview_lines)
                + f"\n\n({len(insight.columns)} total columns, {insight.row_count:,} rows)"
            )

        lines = [f"Sample rows from **{name}**:", ""]
        for i, row in enumerate(insight.sample_preview[:3], 1):
            pairs = ", ".join(f"{k}={v}" for k, v in list(row.items())[:4])
            lines.append(f"{i}. {pairs}")
        return "\n".join(lines)

    def _mask_sample(self, value: str, semantic_type: str | None) -> str:
        if not value or value == "—":
            return "—"
        st = (semantic_type or "").lower()
        if "ssn" in st or "social" in st:
            return "***-**-" + value[-4:] if len(value) >= 4 else "***"
        if "email" in st:
            parts = value.split("@")
            return f"{parts[0][:2]}***@{parts[1]}" if len(parts) == 2 else "***"
        if "phone" in st:
            return "***-" + value[-4:] if len(value) >= 4 else "***"
        if len(value) > 8:
            return value[:3] + "***"
        return value

    def extract_dataset_hint(self, message: str) -> str | None:
        lower = message.lower()
        schemas = self.feeder.feed_all()
        for schema in schemas:
            if schema.name.lower().replace("_", " ") in lower:
                return schema.name
            if schema.industry and schema.industry in lower:
                return schema.name

        for word in ("hr", "logistics", "payment", "retail", "finance", "healthcare", "employee", "shipping"):
            if word in lower:
                return word
        return None

    def wants_data_analysis(self, message: str, intent: str) -> bool:
        lower = message.lower()
        data_signals = [
            "my data", "my file", "my csv", "my json", "analyze", "what's in",
            "what is in", "show me", "tell me about", "columns", "rows", "pii",
            "schema", "sample", "preview", "quality", "detect", "find",
            "hr", "logistics", "payment", "retail", "employee", "shipment",
        ]
        if any(s in lower for s in data_signals):
            return True
        if intent in ("pii_compliance", "mapping_help") and intent != "transfer_help":
            return True
        if re.search(r"\b(column|field)\s+\w+", lower):
            return True
        return False


_analyst: CopilotDataAnalyst | None = None


def get_data_analyst() -> CopilotDataAnalyst:
    global _analyst
    if _analyst is None:
        _analyst = CopilotDataAnalyst()
    return _analyst
