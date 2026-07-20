"""Semantic vector field routing — embed vs metadata vs PII exclude vs skip.

Honesty
-------
Uses real column signals already in DataFlow (semantic roles, PII name/value
guards, optional analysis ``is_pii`` / ``semantic_type``). Never invents text
to embed. PII columns are routed to ``exclude_pii`` so they do not land in
vector metadata or content — operators can still override in Studio, but the
writer fail-closes if the chosen content column is excluded.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Literal

VectorAction = Literal["embed", "metadata", "exclude_pii", "skip"]

# Roles that are good RAG content (prefer longest among these).
_PRIMARY_EMBED_ROLES = frozenset({"long_text", "description"})

_SKIP_ROLES = frozenset({"binary_data"})

_METADATA_ROLES = frozenset({
    "identifier",
    "order_number",
    "invoice_number",
    "transaction_id",
    "customer_id",
    "status",
    "sku",
    "currency_code",
    "country_code",
    "state_code",
    "city_name",
    "region_code",
    "department",
    "date_value",
    "created_timestamp",
    "updated_timestamp",
    "payment_date",
    "order_date",
    "hire_date",
    "ship_date",
    "delivery_date",
    "quantity",
    "quantity_ordered",
    "unit_price",
    "unit_cost",
    "tax_amount",
    "discount_amount",
    "line_amount",
    "net_amount",
    "gross_amount",
    "payment_amount",
    "order_total",
    "numeric_value",
    "tracking_number",
    "origin_city",
    "destination_city",
    "shipment_weight_kg",
})

# PII / PHI semantic roles — never embed, never store as vector metadata.
_PII_ROLES = frozenset({
    "first_name",
    "last_name",
    "full_name",
    "email_address",
    "phone_number",
    "mobile_number",
    "address",
    "postal_code",
    "account_number",
    "salary_amount",
    "commission_amount",
    "bonus_amount",
})

_EMBED_NAME_HINTS = frozenset({
    "content", "body", "text", "description", "desc", "summary", "notes",
    "comment", "comments", "message", "narrative", "article", "document",
})

_EMBEDDING_NAME_HINTS = frozenset({
    "embedding", "embeddings", "vector", "vectors", "vec", "dense_vector",
})


@dataclass(frozen=True)
class FieldRouting:
    column: str
    action: VectorAction
    confidence: float
    reason: str
    semantic_role: str = ""
    is_pii: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VectorRoutingPlan:
    fields: list[FieldRouting]
    content_column: str | None
    embedding_column: str | None
    metadata_columns: list[str]
    exclude_pii_columns: list[str]
    skip_columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "fields": [f.to_dict() for f in self.fields],
            "content_column": self.content_column,
            "embedding_column": self.embedding_column,
            "metadata_columns": list(self.metadata_columns),
            "exclude_pii_columns": list(self.exclude_pii_columns),
            "skip_columns": list(self.skip_columns),
            "summary": {
                "embed": self.content_column,
                "metadata_count": len(self.metadata_columns),
                "exclude_pii_count": len(self.exclude_pii_columns),
                "skip_count": len(self.skip_columns),
            },
        }


def _avg_len(samples: Iterable[Any]) -> float:
    vals = [str(s) for s in samples if s is not None and str(s).strip()]
    if not vals:
        return 0.0
    return sum(len(v) for v in vals) / len(vals)


def _analysis_hint(column: str, analysis_columns: list[dict[str, Any]] | None) -> dict[str, Any]:
    for row in analysis_columns or []:
        name = str(row.get("column_name") or row.get("name") or "")
        if name == column:
            return row
    return {}


def recommend_vector_field_roles(
    columns: list[str],
    *,
    samples_by_column: dict[str, list[Any]] | None = None,
    schema: dict[str, str] | None = None,
    analysis_columns: list[dict[str, Any]] | None = None,
) -> VectorRoutingPlan:
    """Recommend embed / metadata / exclude_pii / skip for each column."""
    from services.compliance_guard import detect_pii_fields
    from services.pii_guard import detect_pii, is_sensitive_name
    from services.semantic_analyzer import analyze_column

    samples_by_column = samples_by_column or {}
    schema = schema or {}

    # Build synthetic rows for compliance_guard value scan.
    sample_rows: list[dict[str, Any]] = []
    max_n = max((len(v) for v in samples_by_column.values()), default=0)
    for i in range(min(max_n, 50)):
        row: dict[str, Any] = {}
        for col in columns:
            vals = samples_by_column.get(col) or []
            if i < len(vals):
                row[col] = vals[i]
        if row:
            sample_rows.append(row)
    pii_report = detect_pii_fields(columns, sample_rows)
    sensitive = set(pii_report.get("sensitive_fields") or [])

    fields: list[FieldRouting] = []
    for col in columns:
        samples = list(samples_by_column.get(col) or [])
        inferred = schema.get(col) or "VARCHAR"
        role_info = analyze_column(col, inferred_type=inferred, samples=[str(s) for s in samples[:20]])
        role = str(role_info.get("semantic_role") or "")
        role_conf = float(role_info.get("confidence") or 0.0)
        hint = _analysis_hint(col, analysis_columns)
        analysis_pii = bool(hint.get("is_pii"))
        analysis_type = str(hint.get("semantic_type") or "").lower()

        sample_pii = any(detect_pii(s).get("has_pii") for s in samples[:20])
        name_pii = is_sensitive_name(col) or col in sensitive
        is_pii = analysis_pii or name_pii or sample_pii or role in _PII_ROLES

        lower = col.lower()
        avg = _avg_len(samples)

        if is_pii or role in _PII_ROLES or analysis_type in {
            "email", "phone", "ssn", "credit_card", "person_name", "address", "iban",
        }:
            fields.append(FieldRouting(
                column=col,
                action="exclude_pii",
                confidence=max(role_conf, 0.9 if analysis_pii else 0.8),
                reason="PII/PHI — excluded from embed content and vector metadata",
                semantic_role=role,
                is_pii=True,
            ))
            continue

        if role in _SKIP_ROLES or "binary" in inferred.lower() or "blob" in inferred.lower():
            fields.append(FieldRouting(
                column=col,
                action="skip",
                confidence=max(role_conf, 0.85),
                reason="Binary / non-text — skip for vector destinations",
                semantic_role=role,
                is_pii=False,
            ))
            continue

        if lower in _EMBEDDING_NAME_HINTS or "embedding" in lower or lower.endswith("_vec"):
            fields.append(FieldRouting(
                column=col,
                action="skip",
                confidence=0.95,
                reason="Precomputed embedding vector column",
                semantic_role="embedding_vector",
                is_pii=False,
            ))
            continue

        if role in _PRIMARY_EMBED_ROLES or lower in _EMBED_NAME_HINTS or avg >= 80:
            conf = 0.92 if role in _PRIMARY_EMBED_ROLES or lower in _EMBED_NAME_HINTS else min(0.85, 0.55 + avg / 400)
            fields.append(FieldRouting(
                column=col,
                action="embed",
                confidence=max(role_conf, conf),
                reason=(
                    f"Long text / description role ({role or 'heuristic'}) — preferred embed content"
                    if role in _PRIMARY_EMBED_ROLES or lower in _EMBED_NAME_HINTS
                    else f"Long string values (avg_len={avg:.0f}) — embed candidate"
                ),
                semantic_role=role,
                is_pii=False,
            ))
            continue

        if role in _METADATA_ROLES or avg < 80:
            fields.append(FieldRouting(
                column=col,
                action="metadata",
                confidence=max(role_conf, 0.7),
                reason=f"Scalar / identifier role ({role or 'short_text'}) — vector metadata",
                semantic_role=role,
                is_pii=False,
            ))
            continue

        fields.append(FieldRouting(
            column=col,
            action="metadata",
            confidence=0.55,
            reason="Default to metadata (safe for RAG filters)",
            semantic_role=role,
            is_pii=False,
        ))

    return _plan_from_fields(fields, samples_by_column=samples_by_column)


def _plan_from_fields(
    fields: list[FieldRouting],
    *,
    samples_by_column: dict[str, list[Any]] | None = None,
) -> VectorRoutingPlan:
    samples_by_column = samples_by_column or {}
    embed_candidates = [f for f in fields if f.action == "embed"]
    embedding_cols = [f for f in fields if f.semantic_role == "embedding_vector"]
    content_column: str | None = None
    if embed_candidates:
        # Prefer highest confidence, then longest average sample.
        embed_candidates.sort(
            key=lambda f: (
                f.confidence,
                _avg_len(samples_by_column.get(f.column) or []),
                1 if f.column.lower() in _EMBED_NAME_HINTS else 0,
            ),
            reverse=True,
        )
        content_column = embed_candidates[0].column
        # Demote other embed candidates to metadata so only one content column is selected.
        remapped: list[FieldRouting] = []
        for f in fields:
            if f.action == "embed" and f.column != content_column:
                remapped.append(FieldRouting(
                    column=f.column,
                    action="metadata",
                    confidence=f.confidence,
                    reason=f"Secondary text — metadata (primary embed is {content_column})",
                    semantic_role=f.semantic_role,
                    is_pii=False,
                ))
            else:
                remapped.append(f)
        fields = remapped

    embedding_column = embedding_cols[0].column if embedding_cols else None
    metadata_columns = [f.column for f in fields if f.action == "metadata"]
    exclude_pii_columns = [f.column for f in fields if f.action == "exclude_pii"]
    skip_columns = [f.column for f in fields if f.action == "skip"]

    return VectorRoutingPlan(
        fields=fields,
        content_column=content_column,
        embedding_column=embedding_column,
        metadata_columns=metadata_columns,
        exclude_pii_columns=exclude_pii_columns,
        skip_columns=skip_columns,
    )
