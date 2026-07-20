"""
DataTransfer.space — AI API Router

REST endpoints for AI semantic analysis capabilities.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..ai import (
    ComplianceFramework,
    analyze_column,
    analyze_schema,
    generate_mappings,
)

router = APIRouter(prefix="/ai", tags=["AI Semantic Engine"])


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST/RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class ColumnAnalysisRequest(BaseModel):
    """Request to analyze a single column"""
    column_name: str = Field(..., description="Name of the column to analyze")
    sample_values: list[str] = Field(default=[], description="Sample data values")

    model_config = {
        "json_schema_extra": {
            "example": {
                "column_name": "customer_email",
                "sample_values": ["john@example.com", "jane@company.org"]
            }
        }
    }


class ColumnAnalysisResponse(BaseModel):
    """Response from column analysis"""
    column_name: str
    inferred_type: str
    semantic_type: Optional[str]
    category: Optional[str]
    confidence: float
    is_pii: bool
    compliance: list[str]
    suggested_transformations: list[str]
    null_percentage: float
    unique_percentage: float
    sample_values: list[str]
    statistics: dict
    warnings: list[str]


class SchemaAnalysisRequest(BaseModel):
    """Request to analyze a complete schema"""
    columns: dict[str, list[str]] = Field(
        ...,
        description="Map of column names to sample values"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "columns": {
                    "id": ["1", "2", "3"],
                    "email": ["test@example.com"],
                    "phone": ["+1-555-1234"],
                    "ssn": ["123-45-6789"]
                }
            }
        }
    }


class SchemaAnalysisResponse(BaseModel):
    """Response from schema analysis"""
    columns: list[ColumnAnalysisResponse]
    pii_columns: list[str]
    compliance_requirements: dict[str, list[str]]
    quality_score: float
    recommendations: list[str]


class MappingRequest(BaseModel):
    """Request to generate column mappings"""
    source_columns: list[str] = Field(..., description="Source column names")
    target_columns: list[str] = Field(..., description="Target column names")
    source_samples: Optional[dict[str, list[str]]] = Field(
        default=None,
        description="Optional sample data for source columns"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_columns": ["cust_id", "cust_name", "email_addr", "amt"],
                "target_columns": ["customer_id", "full_name", "email", "amount"],
                "source_samples": {
                    "cust_id": ["C001", "C002"],
                    "cust_name": ["John Doe", "Jane Smith"]
                }
            }
        }
    }


class MappingSuggestionResponse(BaseModel):
    """A single mapping suggestion"""
    source_column: str
    target_column: str
    confidence: float
    reason: str
    transformation_needed: bool
    suggested_transformation: Optional[str]


class MappingResponse(BaseModel):
    """Response from mapping generation"""
    mappings: list[MappingSuggestionResponse]
    unmapped_source: list[str]
    unmapped_target: list[str]
    overall_confidence: float


class PIIDetectionRequest(BaseModel):
    """Request to detect PII in columns"""
    columns: dict[str, list[str]] = Field(
        ...,
        description="Map of column names to sample values"
    )


class PIIColumn(BaseModel):
    """Details of a detected PII column"""
    column_name: str
    semantic_type: str
    compliance_frameworks: list[str]
    risk_level: str
    recommended_actions: list[str]


class PIIDetectionResponse(BaseModel):
    """Response from PII detection"""
    has_pii: bool
    pii_count: int
    pii_columns: list[PIIColumn]
    compliance_summary: dict[str, list[str]]


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/analyze/column", response_model=ColumnAnalysisResponse)
async def api_analyze_column(request: ColumnAnalysisRequest):
    """
    Analyze a single column using AI semantic analysis.

    This endpoint uses our proprietary semantic engine to:
    - Detect the semantic type (email, phone, SSN, etc.)
    - Identify if the column contains PII
    - Determine compliance requirements (GDPR, HIPAA, etc.)
    - Suggest appropriate transformations
    - Calculate confidence scores
    """
    try:
        result = analyze_column(request.column_name, request.sample_values)

        return ColumnAnalysisResponse(
            column_name=result.column_name,
            inferred_type=result.inferred_type,
            semantic_type=result.semantic_type,
            category=result.category.value if result.category else None,
            confidence=result.confidence,
            is_pii=result.is_pii,
            compliance=[c.value for c in result.compliance],
            suggested_transformations=result.suggested_transformations,
            null_percentage=result.null_percentage,
            unique_percentage=result.unique_percentage,
            sample_values=result.sample_values,
            statistics=result.statistics,
            warnings=result.warnings,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/schema", response_model=SchemaAnalysisResponse)
async def api_analyze_schema(request: SchemaAnalysisRequest):
    """
    Analyze a complete schema with multiple columns.

    This endpoint provides comprehensive analysis of your entire dataset:
    - Individual analysis for each column
    - PII detection across all columns
    - Compliance requirements aggregation
    - Overall data quality scoring
    - Actionable recommendations
    """
    try:
        result = analyze_schema(request.columns)

        columns = [
            ColumnAnalysisResponse(
                column_name=col.column_name,
                inferred_type=col.inferred_type,
                semantic_type=col.semantic_type,
                category=col.category.value if col.category else None,
                confidence=col.confidence,
                is_pii=col.is_pii,
                compliance=[c.value for c in col.compliance],
                suggested_transformations=col.suggested_transformations,
                null_percentage=col.null_percentage,
                unique_percentage=col.unique_percentage,
                sample_values=col.sample_values,
                statistics=col.statistics,
                warnings=col.warnings,
            )
            for col in result.columns
        ]

        compliance_req = {
            k.value: v for k, v in result.compliance_requirements.items()
        }

        return SchemaAnalysisResponse(
            columns=columns,
            pii_columns=result.pii_columns,
            compliance_requirements=compliance_req,
            quality_score=result.quality_score,
            recommendations=result.recommendations,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/map", response_model=MappingResponse)
async def api_generate_mappings(request: MappingRequest):
    """
    Generate intelligent column mappings between source and target schemas.

    Our AI engine uses multiple strategies:
    - Exact name matching
    - Normalized name matching
    - Semantic type correlation
    - Token overlap scoring
    - Synonym recognition

    Each mapping includes a confidence score and explanation.
    """
    try:
        mappings = generate_mappings(
            request.source_columns,
            request.target_columns,
            request.source_samples,
        )

        mapped_targets = set()
        mapped_sources = set()

        mapping_responses = []
        for m in mappings:
            if m.target_column != "<unmapped>":
                mapped_targets.add(m.target_column)
                mapped_sources.add(m.source_column)

            mapping_responses.append(MappingSuggestionResponse(
                source_column=m.source_column,
                target_column=m.target_column,
                confidence=m.confidence,
                reason=m.reason,
                transformation_needed=m.transformation_needed,
                suggested_transformation=m.suggested_transformation,
            ))

        unmapped_source = [c for c in request.source_columns if c not in mapped_sources]
        unmapped_target = [c for c in request.target_columns if c not in mapped_targets]

        confident_mappings = [m for m in mappings if m.confidence > 0.5]
        overall_confidence = (
            sum(m.confidence for m in confident_mappings) / len(confident_mappings)
            if confident_mappings else 0.0
        )

        return MappingResponse(
            mappings=mapping_responses,
            unmapped_source=unmapped_source,
            unmapped_target=unmapped_target,
            overall_confidence=round(overall_confidence, 3),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/detect-pii", response_model=PIIDetectionResponse)
async def api_detect_pii(request: PIIDetectionRequest):
    """
    Detect PII (Personally Identifiable Information) in your data.

    This endpoint scans all columns for:
    - Personal identifiers (SSN, passport, driver's license)
    - Contact information (email, phone, address)
    - Financial data (credit cards, bank accounts)
    - Health information (MRN, diagnoses)

    Returns compliance requirements for GDPR, CCPA, HIPAA, PCI-DSS, etc.
    """
    try:
        schema_analysis = analyze_schema(request.columns)

        pii_columns = []
        for col in schema_analysis.columns:
            if col.is_pii:
                risk_level = "high" if col.confidence > 0.9 else "medium" if col.confidence > 0.7 else "low"

                pii_columns.append(PIIColumn(
                    column_name=col.column_name,
                    semantic_type=col.semantic_type or "unknown",
                    compliance_frameworks=[c.value for c in col.compliance],
                    risk_level=risk_level,
                    recommended_actions=col.suggested_transformations[:3],
                ))

        compliance_summary = {
            k.value: v for k, v in schema_analysis.compliance_requirements.items()
        }

        return PIIDetectionResponse(
            has_pii=len(pii_columns) > 0,
            pii_count=len(pii_columns),
            pii_columns=pii_columns,
            compliance_summary=compliance_summary,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class VectorRoutingRequest(BaseModel):
    columns: list[str] = Field(..., description="Source column names")
    samples: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Optional sample values keyed by column name",
    )
    schema_types: dict[str, str] = Field(
        default_factory=dict,
        description="Optional inferred types keyed by column name",
    )
    analysis_columns: list[dict] = Field(
        default_factory=list,
        description="Optional enhanced analysis rows (column_name, is_pii, semantic_type)",
    )


@router.post("/vector-routing")
async def api_vector_routing(request: VectorRoutingRequest):
    """Recommend embed / metadata / exclude_pii / skip for vector destinations.

    Uses semantic roles + PII guards. Studio applies the plan into Advanced
    vector fields; writers strip ``exclude_pii_columns`` from metadata.
    """
    try:
        from services.semantic_vector_routing import recommend_vector_field_roles

        if not request.columns:
            raise HTTPException(status_code=400, detail="columns are required")
        plan = recommend_vector_field_roles(
            request.columns,
            samples_by_column=request.samples or {},
            schema=request.schema_types or {},
            analysis_columns=request.analysis_columns or [],
        )
        return plan.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/embedding-cache")
async def api_embedding_cache_stats():
    """Durable SQLite embedding cache status (entries, session hit rate, path)."""
    try:
        from services.embedding_cache import cache_stats

        return cache_stats().to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/embedding-cache")
async def api_embedding_cache_clear(
    model: Optional[str] = None,
    clear_memory: bool = True,
):
    """Clear durable embedding cache (and optionally process L1 memory)."""
    try:
        from services.embedding_cache import clear_cache
        from services.vectorization import clear_memory_cache

        result = clear_cache(model=model)
        memory_cleared = 0
        if clear_memory:
            memory_cleared = clear_memory_cache()
        return {**result, "memory_cleared": memory_cleared}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/semantic-types")
async def list_semantic_types():
    """
    List all supported semantic types.

    Returns the complete catalog of data types our AI can recognize,
    including PII categories and compliance mappings.
    """
    from ..ai.semantic_engine import SEMANTIC_TYPES

    return {
        "count": len(SEMANTIC_TYPES),
        "types": [
            {
                "name": st.name,
                "category": st.category.value,
                "is_pii": st.is_pii,
                "compliance": [c.value for c in st.compliance],
                "patterns": st.patterns[:5],
            }
            for st in SEMANTIC_TYPES
        ]
    }


@router.get("/compliance-frameworks")
async def list_compliance_frameworks():
    """
    List all supported compliance frameworks.
    """
    return {
        "frameworks": [
            {"id": f.value, "name": f.name.replace("_", " ")}
            for f in ComplianceFramework
        ]
    }


# ═══════════════════════════════════════════════════════════════════════════════
# RAG + ENHANCED AI ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

class RAGQueryRequest(BaseModel):
    query: str = Field(..., description="Natural language data query")
    model_config = {"json_schema_extra": {"example": {"query": "move my customer data to Snowflake"}}}


class RAGQueryResponse(BaseModel):
    answer: str
    reasoning: str
    confidence: float
    method: str
    sources: list[dict] = []


class RAGIngestRequest(BaseModel):
    schema_name: str = Field(..., description="Name for the schema")
    columns: dict = Field(..., description="Column name to samples/metadata mapping")
    industry: Optional[str] = Field(None, description="Industry tag (logistics, finance, etc.)")


class RAGIngestResponse(BaseModel):
    schema_name: str
    columns_ingested: int
    total_documents: int


class EnhancedAnalysisRequest(BaseModel):
    columns: dict[str, list[str]] = Field(..., description="Column names to sample values")


class EnhancedAnalysisResponse(BaseModel):
    columns: list[dict]
    pii_columns: list[str]
    quality_score: float
    recommendations: list[str]
    method: str


class EnhancedMappingRequest(BaseModel):
    source_columns: list[str]
    target_columns: list[str]
    source_samples: Optional[dict[str, list[str]]] = None


class EnhancedMappingResponse(BaseModel):
    mappings: list[dict]
    overall_confidence: float
    method: str
    reasoning: str


class TransformSuggestionRequest(BaseModel):
    source_type: str
    target_type: str
    semantic_type: Optional[str] = None
    source_column: Optional[str] = None
    target_column: Optional[str] = None


class TransformSuggestionResponse(BaseModel):
    answer: str
    reasoning: str
    confidence: float
    transformations: list[str] = []


class ModelsStatusResponse(BaseModel):
    capabilities: dict
    rag: dict
    llm_providers: dict
    evaluation: Optional[dict] = None


@router.post("/rag/query", response_model=RAGQueryResponse)
async def api_rag_query(request: RAGQueryRequest):
    """Natural language data queries powered by RAG + chain-of-thought."""
    try:
        from ..ai import query_natural_language
        result = query_natural_language(request.query)
        return RAGQueryResponse(
            answer=result["answer"],
            reasoning=result.get("reasoning", ""),
            confidence=result.get("confidence", 0.0),
            method=result.get("method", "rag"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rag/ingest", response_model=RAGIngestResponse)
async def api_rag_ingest(request: RAGIngestRequest):
    """Ingest schema/data into the RAG knowledge base."""
    try:
        from ..ai.rag.pipeline import get_rag_pipeline
        pipeline = get_rag_pipeline()
        pipeline.initialize()
        result = pipeline.ingest_schema(request.schema_name, request.columns, request.industry)
        status = pipeline.get_status()
        return RAGIngestResponse(
            schema_name=result["schema"],
            columns_ingested=result["columns_ingested"],
            total_documents=status["document_count"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/enhanced", response_model=EnhancedAnalysisResponse)
async def api_analyze_enhanced(request: EnhancedAnalysisRequest):
    """RAG-enhanced schema analysis with chain-of-thought reasoning."""
    try:
        from ..ai import analyze_schema_enhanced
        result = analyze_schema_enhanced(request.columns)
        columns = []
        for col in result.columns:
            col_dict = {
                "column_name": col.column_name,
                "semantic_type": col.semantic_type,
                "inferred_type": col.inferred_type,
                "confidence": col.confidence,
                "is_pii": col.is_pii,
                "compliance": [c.value for c in col.compliance],
            }
            if hasattr(col, "canonical_form"):
                col_dict["canonical_form"] = col.canonical_form
            if hasattr(col, "rag_confidence"):
                col_dict["rag_confidence"] = col.rag_confidence
            if hasattr(col, "method"):
                col_dict["method"] = col.method
            columns.append(col_dict)
        return EnhancedAnalysisResponse(
            columns=columns,
            pii_columns=result.pii_columns,
            quality_score=result.quality_score,
            recommendations=result.recommendations,
            method="chain_of_thought",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/map/enhanced", response_model=EnhancedMappingResponse)
async def api_map_enhanced(request: EnhancedMappingRequest):
    """LLM + RAG powered column mapping with synonym intelligence."""
    try:
        from ..ai import generate_mappings_enhanced
        mappings = generate_mappings_enhanced(
            request.source_columns, request.target_columns, request.source_samples,
        )
        mapping_dicts = []
        reasoning = ""
        for m in mappings:
            mapping_dicts.append({
                "source_column": m.source_column,
                "target_column": m.target_column,
                "confidence": m.confidence,
                "reason": m.reason,
                "canonical_source": m.canonical_source,
                "canonical_target": m.canonical_target,
                "transformation_needed": m.transformation_needed,
                "suggested_transformation": m.suggested_transformation,
            })
            if m.reasoning:
                reasoning = m.reasoning
        confident = [m for m in mappings if m.confidence > 0.5]
        overall = sum(m.confidence for m in confident) / len(confident) if confident else 0.0
        return EnhancedMappingResponse(
            mappings=mapping_dicts,
            overall_confidence=round(overall, 3),
            method=mappings[0].method if mappings else "enhanced",
            reasoning=reasoning,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/suggest/transforms", response_model=TransformSuggestionResponse)
async def api_suggest_transforms(request: TransformSuggestionRequest):
    """AI transformation suggestions for data type conversions."""
    try:
        from ..ai.rag.pipeline import get_rag_pipeline
        pipeline = get_rag_pipeline()
        result = pipeline.suggest_transforms(
            request.source_type, request.target_type, request.semantic_type,
        )
        transforms = []
        if request.semantic_type:
            from ..ai.knowledge.semantic_patterns import get_pattern_by_name
            pattern = get_pattern_by_name(request.semantic_type)
            if pattern:
                transforms = pattern.transformations
        return TransformSuggestionResponse(
            answer=result.answer,
            reasoning=result.reasoning,
            confidence=result.confidence,
            transformations=transforms,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/status", response_model=ModelsStatusResponse)
async def api_models_status(run_evaluation: bool = False):
    """Model health, capabilities, and optional evaluation metrics."""
    try:
        from ..ai import get_ai_capabilities
        capabilities = get_ai_capabilities()
        evaluation = None
        if run_evaluation:
            from ..ai.training.evaluation import DataTransferEvaluator
            evaluator = DataTransferEvaluator()
            evaluation = evaluator.run_full_evaluation()
        return ModelsStatusResponse(
            capabilities=capabilities,
            rag=capabilities.get("rag", {}),
            llm_providers=capabilities.get("llm_providers", {}),
            evaluation=evaluation,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
