"""
DataTransfer.space — RAG Pipeline

End-to-end RAG pipeline orchestrating ingestion, retrieval, and generation.
"""

from __future__ import annotations
from typing import Optional

from .document_ingestion import DataTransferDocumentIngestion
from .retriever import DataTransferRetriever
from .generator import DataTransferRAGGenerator, RAGResponse
from .embedding_service import get_embedding_service
from .vector_store import get_vector_store

_pipeline: Optional["DataTransferRAGPipeline"] = None


class DataTransferRAGPipeline:
    """Complete RAG pipeline for DataTransfer.space."""

    def __init__(self):
        self.ingestion = DataTransferDocumentIngestion()
        self.retriever = DataTransferRetriever()
        self.generator = DataTransferRAGGenerator()
        self.embedding_service = get_embedding_service()
        self.vector_store = get_vector_store()
        self._initialized = False

    def initialize(self) -> dict:
        """Initialize the pipeline with knowledge base."""
        if not self._initialized:
            result = self.ingestion.ingest_knowledge_base()
            self._initialized = True
            return result
        return {"status": "already_initialized", "documents": self.vector_store.document_count}

    def query(self, question: str, n_results: int = 5) -> RAGResponse:
        """Answer a natural language query."""
        self.ingestion.ensure_knowledge_loaded()
        retrieval = self.retriever.retrieve(question, n_results=n_results)
        return self.generator.generate_natural_language_response(question, retrieval)

    def analyze_column(
        self,
        column_name: str,
        sample_values: list[str] | None = None,
    ) -> RAGResponse:
        """RAG-enhanced column analysis."""
        self.ingestion.ensure_knowledge_loaded()
        retrieval = self.retriever.retrieve_for_column(column_name, sample_values)
        return self.generator.generate_schema_analysis(column_name, retrieval, sample_values)

    def suggest_mapping(
        self,
        source_column: str,
        target_column: str,
    ) -> RAGResponse:
        """Suggest mapping between two columns."""
        self.ingestion.ensure_knowledge_loaded()
        mapping_info = self.retriever.retrieve_for_mapping(source_column, target_column)
        return self.generator.generate_mapping_suggestion(
            source_column, target_column, mapping_info
        )

    def ingest_schema(
        self,
        schema_name: str,
        columns: dict,
        industry: str | None = None,
    ) -> dict:
        """Ingest a user schema."""
        self.ingestion.ensure_knowledge_loaded()
        return self.ingestion.ingest_schema(schema_name, columns, industry)

    def suggest_transforms(
        self,
        source_type: str,
        target_type: str,
        semantic_type: str | None = None,
    ) -> RAGResponse:
        """Suggest data transformations."""
        return self.generator.suggest_transformations(source_type, target_type, semantic_type)

    def learn_correction(
        self,
        source_column: str,
        target_column: str,
        user_confirmed: bool = True,
    ):
        """Self-learning from user corrections."""
        if user_confirmed:
            text = f"User confirmed mapping: {source_column} → {target_column}"
            self.vector_store.add_documents(
                [text],
                [{"type": "user_correction", "source": source_column, "target": target_column}],
                [f"correction_{source_column}_{target_column}"],
            )

    def get_status(self) -> dict:
        """Get pipeline status and capabilities."""
        return {
            "initialized": self._initialized,
            "embedding_backend": self.embedding_service.backend,
            "embedding_dimension": self.embedding_service.dimension,
            "vector_store_backend": self.vector_store.backend,
            "document_count": self.vector_store.document_count,
            "capabilities": [
                "semantic_search",
                "column_analysis",
                "column_mapping",
                "synonym_resolution",
                "type_conversion",
                "natural_language_query",
                "self_learning",
            ],
        }


def get_rag_pipeline() -> DataTransferRAGPipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = DataTransferRAGPipeline()
    return _pipeline
