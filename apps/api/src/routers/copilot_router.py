"""
DataTransfer.space — Copilot API Router

Customer-facing chat + separate training agent endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

router = APIRouter(prefix="/copilot", tags=["AI Copilot"])


class ChatMessage(BaseModel):
    role: str = Field(..., description="user or assistant")
    content: str = Field(..., description="Message text")


class CopilotChatRequest(BaseModel):
    message: str = Field(..., description="User message")
    history: list[ChatMessage] = Field(default_factory=list, description="Prior conversation")
    data_context: Optional[dict] = Field(None, description="Active upload/session data for analysis")
    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "What's in my HR data?",
                "history": [],
                "data_context": {"name": "sample_hr.csv", "columns": ["employee_id", "email"], "row_count": 5},
            }
        }
    }


class CopilotChatResponse(BaseModel):
    answer: str
    intent: str
    confidence: float
    method: str
    reasoning: str = ""
    suggested_actions: list[dict] = []
    suggested_prompts: list[str] = []
    sources: list[dict] = []
    data_insight: Optional[dict] = None
    tools_used: list[dict] = []


class TrainRequest(BaseModel):
    include_embedding_tune: bool = Field(False, description="Also fine-tune embedding model")
    force: bool = Field(False, description="Force full retrain")


class TrainResponse(BaseModel):
    run_id: str
    status: str
    metrics: dict = {}
    errors: list[str] = []


@router.post("/chat", response_model=CopilotChatResponse)
async def copilot_chat(request: CopilotChatRequest):
    """
    Customer-facing AI Copilot chat.
    Uses trained knowledge from universal data + intent-aware responses.
    """
    try:
        from ..ai.copilot import get_copilot_agent
        agent = get_copilot_agent()
        history = [{"role": m.role, "content": m.content} for m in request.history]
        result = agent.chat(request.message, history, data_context=request.data_context)
        return CopilotChatResponse(
            answer=result.answer,
            intent=result.intent,
            confidence=result.confidence,
            method=result.method,
            reasoning=result.reasoning,
            suggested_actions=result.suggested_actions,
            suggested_prompts=result.suggested_prompts,
            sources=result.sources,
            data_insight=result.data_insight,
            tools_used=getattr(result, "tools_used", []) or [],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets")
async def copilot_datasets():
    """List universal datasets available for copilot analysis."""
    from ..ai.copilot.data_analyst import get_data_analyst
    return {"datasets": get_data_analyst().list_datasets()}


@router.get("/prompts")
async def copilot_prompts():
    """Suggested starter prompts for the copilot UI."""
    from ..ai.copilot import get_copilot_agent
    return {"prompts": get_copilot_agent().get_suggested_prompts()}


@router.post("/train", response_model=TrainResponse)
async def train_copilot(request: TrainRequest):
    """
    Run the separate Training Agent.
    Feeds universal data, synthesizes conversations, updates RAG knowledge.
    """
    try:
        from ..ai.training.training_agent import get_training_agent
        agent = get_training_agent()
        run = agent.run_full_training(
            include_embedding_tune=request.include_embedding_tune,
            force=request.force,
        )
        return TrainResponse(
            run_id=run.id,
            status=run.status,
            metrics=run.metrics,
            errors=run.errors,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/train/status")
async def train_status():
    """Training agent status and last run metrics."""
    from ..ai.training.training_agent import get_training_agent
    return get_training_agent().get_status()


@router.get("/status")
async def copilot_status():
    """Copilot and training agent health."""
    from ..ai.copilot import get_copilot_agent
    from ..ai.training.training_agent import get_training_agent
    from ..ai.rag.pipeline import get_rag_pipeline
    from ..ai.copilot.pilot_agent import get_pilot_agent

    rag = get_rag_pipeline()
    training = get_training_agent()
    return {
        "copilot": "ready",
        "data_pilot": "ready",
        "agent_mode": (
            "anthropic_tools" if get_pilot_agent().anthropic.is_available()
            else "openai_tools" if _openai_available()
            else "ollama_tools" if _ollama_available()
            else "local_tools"
        ),
        "suggested_prompts": get_copilot_agent().get_suggested_prompts(),
        "rag": rag.get_status(),
        "training_agent": training.get_status(),
    }


def _openai_available() -> bool:
    from ..ai.llm.provider import DataTransferOpenAIProvider
    return DataTransferOpenAIProvider().is_available()


def _ollama_available() -> bool:
    from ..ai.llm.provider import DataTransferOllamaProvider
    return DataTransferOllamaProvider().is_available()
