"""Gate-8 for Weaviate: every source value, read back from the objects (D231).

A table source lands one object per row; each mapped value is an object
property. Embeddings are opaque, properties are not: re-read the source,
list the class completely, and require every source row to be an object whose
properties equal it. Report how rows became objects, and any stored text the
writer capped, instead of passing silently.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from services.pgvector_document_verify import (
    _source_for_proof,
    boolean_keys,
    carried_fields,
    compare_documents,
    numeric_keys,
)

logger = logging.getLogger(__name__)


def object_expansion(source_rows: int, documents: list[Mapping[str, Any]]) -> str:
    """'30 rows -> 164 objects (row 7 = 135 chunks)' or one object per row."""
    total = sum(int(d.get("_df_object_count") or 1) for d in documents)
    if total == source_rows and all(int(d.get("_df_object_count") or 1) == 1 for d in documents):
        return f"{source_rows} rows -> {total} objects (one per row)"
    multi = sorted(
        (d for d in documents if int(d.get("_df_object_count") or 1) > 1),
        key=lambda d: -int(d.get("_df_object_count") or 1),
    )
    detail = ", ".join(
        f"row {d.get('source_id')} = {int(d.get('_df_object_count') or 1)} chunks" for d in multi[:3]
    )
    more = f", +{len(multi) - 3} more" if len(multi) > 3 else ""
    return f"{source_rows} rows -> {total} objects ({detail}{more})"


def content_caps(documents: list[Mapping[str, Any]]) -> list[str]:
    """Rows whose stored ``content`` copy of the embedding text was capped."""
    out = []
    for d in documents:
        if d.get("_df_content_truncated") is True:
            out.append(
                f"row {d.get('source_id')}: stored 'content' (embedding text copy) capped at "
                f"{int(d.get('_df_content_store_limit') or 0):,} of "
                f"{int(d.get('_df_content_original_len') or 0):,} chars"
            )
    return out


def gate8_weaviate_report(
    *,
    cfg: Mapping[str, Any],
    table_name: str,
    mappings: list[dict[str, Any]],
    source_rows: Any,
    source_endpoint: Any,
    read_sample: Callable[..., list[dict[str, Any]]],
    run_records: list[dict[str, Any]] | None,
    base: dict[str, Any],
    source_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """The Gate-8 report for a Weaviate class, or ``None`` to keep the identity-only one."""
    from connectors.weaviate_writer import read_document_properties, weaviate_property_name

    if not isinstance(source_rows, int) or source_rows < 0:
        return None
    fields = carried_fields(mappings, {})
    if not fields:
        return None
    provenance: dict[str, str] = {}
    try:
        src = _source_for_proof(
            source_endpoint, mappings, source_rows, read_sample, run_records, provenance
        )
        if src is None:
            return None
        docs, data_types = read_document_properties(cfg, table_name=table_name)
    except Exception as exc:  # noqa: BLE001 — the identity-only report stands
        logger.warning("Weaviate value proof unavailable: %s", type(exc).__name__)
        return None
    # Read each field back under the property the writer stored it as.
    stored = {key: weaviate_property_name(key, data_types) for _s, key in fields}
    renamed = {key: name for key, name in stored.items() if name != key}
    fields = [(source, stored[key]) for source, key in fields]
    instant = {k for _s, k in fields if data_types.get(k) in ("date", "date[]")}
    proof = compare_documents(
        src, docs, fields, numeric_keys(mappings, source_schema),
        boolean_keys(mappings, source_schema), instant
    )
    expansion = object_expansion(source_rows, docs)
    caps = content_caps(docs)
    proof = {
        **proof,
        "objects": sum(int(d.get("_df_object_count") or 1) for d in docs),
        "row_object_expansion": expansion,
        "content_caps": caps,
        "source_provenance": provenance.get("source", ""),
        "property_names": renamed,
    }
    report = {
        **base,
        "target_rows": len(docs),
        "target_checksum": "",
        "document_value_proof": proof,
        "skipped_readback": False,
        "migration_proven": False,
        "unproven": False,
    }
    names = ", ".join(proof.get("fields") or [])
    if not proof["passed"]:
        return {
            **report,
            "passed": False,
            "message": (
                f"Weaviate objects do not hold {proof['unmatched_source_rows']} of "
                f"{proof['source_rows']} source row(s) value for value (fields: {names}); "
                f"{expansion}; first: {proof['examples'][:1]}"
            ),
        }
    cap_note = (
        " " + "; ".join(caps) + "; every mapped source property is stored complete."
        if caps
        else ""
    )
    if renamed:
        cap_note += " Stored as Weaviate properties: " + ", ".join(
            f"{key} -> {name}" for key, name in sorted(renamed.items())
        ) + "."
    return {
        **report,
        "passed": True,
        "message": (
            f"Weaviate objects hold every source value of {proof['source_rows']} row(s) "
            f"(fields: {names}; NULL as null); {expansion}.{cap_note} "
            "Embedding vectors are opaque and not compared."
        ),
    }
