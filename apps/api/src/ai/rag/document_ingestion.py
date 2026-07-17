"""
DataTransfer.space — Document Ingestion

Ingest schemas, column names, and data patterns into the vector store.
"""

from __future__ import annotations
from typing import Any

from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS
from ..knowledge.synonyms import SYNONYM_DICTIONARY
from ..knowledge.industry_schemas import INDUSTRY_SCHEMAS
from ..knowledge.type_conversions import TYPE_CONVERSION_MATRIX
from .vector_store import get_vector_store


class DataTransferDocumentIngestion:
    """Ingest knowledge and user data into the RAG vector store."""

    def __init__(self):
        self.vector_store = get_vector_store()
        self._knowledge_loaded = False

    def ingest_knowledge_base(self) -> dict:
        """Ingest the built-in knowledge base into the vector store."""
        texts = []
        metadatas = []
        ids = []

        # Semantic patterns
        for i, pattern in enumerate(SEMANTIC_PATTERNS):
            all_terms = pattern.patterns + pattern.synonyms
            text = (
                f"Semantic type: {pattern.name}. "
                f"Category: {pattern.category.value}. "
                f"Patterns: {', '.join(all_terms[:10])}. "
                f"PII: {pattern.is_pii}. "
                f"Data type: {pattern.data_type}."
            )
            texts.append(text)
            metadatas.append({
                "type": "semantic_pattern",
                "name": pattern.name,
                "category": pattern.category.value,
                "is_pii": pattern.is_pii,
            })
            ids.append(f"pattern_{i}")

        # Synonyms
        for canonical, synonyms in SYNONYM_DICTIONARY.items():
            text = f"Synonym group: {canonical} = {', '.join(synonyms[:15])}"
            texts.append(text)
            metadatas.append({
                "type": "synonym",
                "canonical": canonical,
            })
            ids.append(f"syn_{canonical}")

        # Industry schemas
        for industry, schema in INDUSTRY_SCHEMAS.items():
            cols = ", ".join(schema["columns"].keys())
            text = f"Industry schema: {schema['name']}. Columns: {cols}"
            texts.append(text)
            metadatas.append({
                "type": "industry_schema",
                "industry": industry,
            })
            ids.append(f"industry_{industry}")

        # Type conversions
        for src, targets in TYPE_CONVERSION_MATRIX.items():
            target_list = ", ".join(targets.keys())
            text = f"Type conversion from {src} to: {target_list}"
            texts.append(text)
            metadatas.append({
                "type": "type_conversion",
                "source_type": src,
            })
            ids.append(f"conv_{src}")

        self.vector_store.add_documents(texts, metadatas, ids)
        self._knowledge_loaded = True

        return {
            "ingested": len(texts),
            "patterns": len(SEMANTIC_PATTERNS),
            "synonyms": len(SYNONYM_DICTIONARY),
            "industries": len(INDUSTRY_SCHEMAS),
            "type_conversions": len(TYPE_CONVERSION_MATRIX),
        }

    def ingest_schema(
        self,
        schema_name: str,
        columns: dict[str, Any],
        industry: str | None = None,
    ) -> dict:
        """Ingest a user schema into the vector store."""
        texts = []
        metadatas = []
        ids = []

        for col_name, col_info in columns.items():
            if isinstance(col_info, list):
                samples = col_info[:5]
                text = f"Column: {col_name}. Sample values: {', '.join(str(v) for v in samples)}"
                meta = {"type": "user_column", "schema": schema_name, "column": col_name}
            elif isinstance(col_info, dict):
                samples = col_info.get("samples", col_info.get("values", []))[:5]
                sem_type = col_info.get("semantic_type", "")
                text = (
                    f"Column: {col_name}. Semantic type: {sem_type}. "
                    f"Samples: {', '.join(str(v) for v in samples)}"
                )
                meta = {
                    "type": "user_column",
                    "schema": schema_name,
                    "column": col_name,
                    "semantic_type": sem_type,
                }
            else:
                text = f"Column: {col_name}. Value: {col_info}"
                meta = {"type": "user_column", "schema": schema_name, "column": col_name}

            if industry:
                meta["industry"] = industry

            texts.append(text)
            metadatas.append(meta)
            ids.append(f"schema_{schema_name}_{col_name}")

        added_ids = self.vector_store.add_documents(texts, metadatas, ids)
        return {"schema": schema_name, "columns_ingested": len(added_ids)}

    def ingest_natural_language(self, query: str, context: dict | None = None) -> str:
        """Store a natural language query with optional context for learning."""
        meta = {"type": "nl_query"}
        if context:
            meta["context"] = str(context)
        ids = self.vector_store.add_documents([query], [meta])
        return ids[0]

    def ensure_knowledge_loaded(self):
        """Load knowledge base if not already loaded."""
        if not self._knowledge_loaded and self.vector_store.document_count == 0:
            self.ingest_knowledge_base()
