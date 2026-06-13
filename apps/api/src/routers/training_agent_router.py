"""
DataTransfer.space — Dedicated Training Agent API

Separate from Copilot chat — manages continuous model training on universal data.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

router = APIRouter(prefix="/training-agent", tags=["Training Agent"])


class TrainRequest(BaseModel):
    include_embedding_tune: bool = Field(False, description="Fine-tune embedding model")
    force: bool = Field(False, description="Force retrain even if recently completed")


class TrainResponse(BaseModel):
    run_id: str
    status: str
    metrics: dict = {}
    errors: list[str] = []


@router.get("/status")
async def training_agent_status():
    """Dedicated training agent health and last run metrics."""
    from ..ai.training.training_agent import get_training_agent
    from ..ai.training.training_scheduler import TRAINING_INTERVAL_SECONDS
    agent = get_training_agent()
    status = agent.get_status()
    status["interval_seconds"] = TRAINING_INTERVAL_SECONDS
    status["description"] = "Trains Data Pilot on 620+ connectors, industry schemas, uploads, and transfer history"
    return status


@router.post("/run", response_model=TrainResponse)
async def run_training(request: TrainRequest, background_tasks: BackgroundTasks):
    """Trigger a full training run from universal data."""
    from ..ai.training.training_agent import get_training_agent

    def _run():
        get_training_agent().run_full_training(
            include_embedding_tune=request.include_embedding_tune,
            force=request.force,
        )

    background_tasks.add_task(_run)
    return TrainResponse(run_id="scheduled", status="running", metrics={"message": "Training started in background"})


@router.post("/run/sync", response_model=TrainResponse)
async def run_training_sync(request: TrainRequest):
    """Run training synchronously (for admin/debug)."""
    try:
        from ..ai.training.training_agent import get_training_agent
        run = get_training_agent().run_full_training(
            include_embedding_tune=request.include_embedding_tune,
            force=request.force,
        )
        return TrainResponse(run_id=run.id, status=run.status, metrics=run.metrics, errors=run.errors)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets")
async def training_datasets():
    """Universal data sources the training agent feeds on."""
    from ..ai.training.universal_data_feeder import UniversalDataFeeder
    return UniversalDataFeeder().get_status()
