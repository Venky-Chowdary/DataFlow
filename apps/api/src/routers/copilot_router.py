"""
DataTransfer.space — Copilot API Router

Customer-facing chat + separate training agent endpoints.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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
    pending_actions: list[dict] = []
    needs_clarification: str = ""
    suggested_prompts: list[str] = []
    sources: list[dict] = []
    data_insight: Optional[dict] = None
    tools_used: list[dict] = []


class ToolRegistryResponse(BaseModel):
    tool_count: int
    generated_action_count: int
    total_routable_actions: int
    families: list[dict] = []
    tools: list[dict] = []


class ModelCapabilitiesResponse(BaseModel):
    active_provider: str
    active_model: str
    agent_mode: str
    pilot_engine: str = "local"
    fallback_order: list[str]
    providers: list[dict]
    guarantees: list[str]


class TrainRequest(BaseModel):
    include_embedding_tune: bool = Field(False, description="Also fine-tune embedding model")
    force: bool = Field(False, description="Force full retrain")


class ConfirmActionRequest(BaseModel):
    ack_id: str = Field(..., description="Server-side approval id from pending_actions")
    actor: str = Field("", description="Who confirmed (user email / local id)")
    reason: str = Field("", description="Optional reason for audit trail")


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
            pending_actions=getattr(result, "pending_actions", None) or [],
            needs_clarification=getattr(result, "needs_clarification", "") or "",
            suggested_prompts=result.suggested_prompts,
            sources=result.sources,
            data_insight=result.data_insight,
            tools_used=getattr(result, "tools_used", []) or [],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _start_confirmed_transfer(payload: dict) -> dict:
    """Launch a transfer the operator explicitly confirmed.

    The engine is reached through the same job-creation path the Transfer
    Studio uses, so chat-started runs are ordinary jobs: they appear in Jobs,
    stream progress, reconcile, and quarantine identically. ``skip_preflight``
    is forced off here as well as at staging time — a tampered ack still cannot
    bypass the gates.
    """
    from ..transfer.background import run_transfer_async
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import EndpointConfig, TransferRequest

    src = dict(payload.get("source") or {})
    dst = dict(payload.get("destination") or {})
    if not src.get("connector_id") or not dst.get("connector_id"):
        raise HTTPException(status_code=400, detail="Transfer approval is missing its endpoints.")

    request_obj = TransferRequest(
        source=EndpointConfig.from_dict("database", src),
        destination=EndpointConfig.from_dict("database", dst),
        mappings=list(payload.get("mappings") or []),
        column_types=dict(payload.get("column_types") or {}),
        sync_mode=str(payload.get("sync_mode") or "full_refresh_append"),
        schema_policy=str(payload.get("schema_policy") or "manual_review"),
        validation_mode=str(payload.get("validation_mode") or "balanced"),
        limit=max(0, int(payload.get("limit") or 0)),
        skip_preflight=False,
        triggered_by="data-pilot",
    )
    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request_obj)
    run_transfer_async(job_id, request_obj)
    return {
        "job_id": job_id,
        "status": "queued",
        "source": f"{src.get('connector_id')}.{src.get('table')}",
        "destination": f"{dst.get('connector_id')}.{dst.get('table')}",
        "sync_mode": request_obj.sync_mode,
        "preflight_run_id": payload.get("preflight_run_id") or "",
    }


@router.post("/confirm")
async def copilot_confirm(request: ConfirmActionRequest):
    """Consume a Pilot mutation ack (create_connector / start_transfer / run_schedule).

    Secrets and schedule ids stay on the server ledger — the browser only sends ack_id.
    """
    from services.connector_store import create_connector

    from ..ai.copilot.ack_ledger import get_ack_ledger

    ack_id = (request.ack_id or "").strip()
    if not ack_id:
        raise HTTPException(status_code=400, detail="ack_id required")

    ledger = get_ack_ledger()
    peek = ledger.peek(ack_id)
    if not peek:
        raise HTTPException(
            status_code=404,
            detail="Approval not found or expired. Ask Pilot to create the connector again.",
        )

    payload, err = ledger.claim(
        ack_id,
        actor=request.actor or "pilot-ui",
        reason=request.reason or "confirmed",
    )
    if err:
        raise HTTPException(status_code=409 if "already" in err.lower() else 400, detail=err)
    assert payload is not None

    if payload.get("_idempotent"):
        # Replaying a confirmed ack returns the original outcome — it never
        # creates a second connector or launches a second transfer.
        echo = {k: v for k, v in payload.items() if k != "_idempotent"}
        return {"ok": True, "idempotent": True, "kind": peek.get("kind"), **echo}

    kind = peek.get("kind") or ""
    if kind == "start_transfer":
        try:
            result = await _start_confirmed_transfer(payload)
        except HTTPException:
            ledger.release_claim(ack_id)
            raise
        except Exception as exc:
            ledger.release_claim(ack_id)
            raise HTTPException(status_code=400, detail=f"Failed to start transfer: {exc}") from exc
        ledger.finalize(
            ack_id,
            actor=request.actor or "pilot-ui",
            reason=request.reason or "confirmed",
            result=result,
        )
        return {"ok": True, "idempotent": False, "kind": kind, **result}

    if kind == "create_connector":
        try:
            conn = create_connector(payload)
        except Exception as exc:
            ledger.release_claim(ack_id)
            raise HTTPException(status_code=400, detail=f"Failed to save connector: {exc}") from exc
        result = {
            "connector_id": conn.id,
            "name": conn.name,
            "type": conn.type,
        }
        ledger.finalize(
            ack_id,
            actor=request.actor or "pilot-ui",
            reason=request.reason or "confirmed",
            result=result,
        )
        return {"ok": True, "idempotent": False, "kind": kind, **result}

    if kind == "run_schedule":
        schedule_id = str(payload.get("schedule_id") or "").strip()
        if not schedule_id:
            ledger.release_claim(ack_id)
            raise HTTPException(status_code=400, detail="Approval is missing schedule_id")
        try:
            from services.schedule_store import get_schedule
            from ..services.schedule_runner import _run_schedule

            sched = get_schedule(schedule_id)
            if not sched:
                ledger.release_claim(ack_id)
                raise HTTPException(status_code=404, detail="Schedule not found")
            job_id = _run_schedule(schedule_id)
            if not job_id:
                ledger.release_claim(ack_id)
                raise HTTPException(
                    status_code=400,
                    detail="Could not start pipeline — check connectors",
                )
        except HTTPException:
            raise
        except Exception as exc:
            ledger.release_claim(ack_id)
            raise HTTPException(status_code=400, detail=f"Failed to run pipeline: {exc}") from exc
        result = {
            "job_id": job_id,
            "schedule_id": schedule_id,
            "name": str(payload.get("name") or getattr(sched, "name", "") or ""),
            "status": "queued",
        }
        ledger.finalize(
            ack_id,
            actor=request.actor or "pilot-ui",
            reason=request.reason or "confirmed",
            result=result,
        )
        return {"ok": True, "idempotent": False, "kind": kind, **result}

    ledger.release_claim(ack_id)
    raise HTTPException(status_code=400, detail=f"Unsupported approval kind: {kind}")


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


@router.get("/tools", response_model=ToolRegistryResponse)
async def copilot_tools():
    """Expose Data Pilot tool registry and generated connector actions."""
    from ..ai.copilot.tools import get_tool_registry
    return get_tool_registry()


@router.get("/models", response_model=ModelCapabilitiesResponse)
async def copilot_models():
    """Expose cloud/local AI model capabilities and safe fallback order."""
    from ..ai.llm.provider import get_model_capabilities
    return get_model_capabilities()


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
    from ..ai.rag.pipeline import get_rag_pipeline
    from ..ai.training.training_agent import get_training_agent

    rag = get_rag_pipeline()
    training = get_training_agent()
    from ..ai.copilot.tools import get_tool_registry
    from ..ai.llm.provider import get_model_capabilities
    model_capabilities = get_model_capabilities()
    return {
        "copilot": "ready",
        "data_pilot": "ready",
        "agent_mode": model_capabilities["agent_mode"],
        "suggested_prompts": get_copilot_agent().get_suggested_prompts(),
        "tool_registry": get_tool_registry(),
        "model_capabilities": model_capabilities,
        "rag": rag.get_status(),
        "training_agent": training.get_status(),
    }


def _openai_available() -> bool:
    from ..ai.llm.provider import DataTransferOpenAIProvider
    return DataTransferOpenAIProvider().is_available()


def _ollama_available() -> bool:
    from ..ai.llm.provider import DataTransferOllamaProvider
    return DataTransferOllamaProvider().is_available()
