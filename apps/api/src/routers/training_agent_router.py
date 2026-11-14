"""
Datawrap — Dedicated Training Agent API

Separate from Copilot chat — manages continuous model training on universal data.
"""

from fastapi import APIRouter, HTTPException, Request
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
    status["description"] = "Trains Datawrap Pilot on 620+ connectors, industry schemas, uploads, and transfer history"
    return status


def _platform_admin_only(request: Request) -> None:
    """SEC-10: a training run is platform-wide."""
    from services.workspace_access import require_platform_admin, tenant_scope

    require_platform_admin(
        request, tenant_scope(request), "ai.training.run",
        "Training runs for every workspace: platform administrators only.",
    )


@router.post("/run", response_model=TrainResponse)
async def run_training(request: TrainRequest, http_request: Request):
    """Trigger a full training run from universal data.

    Runs on the app's training runtime thread; API shutdown stops it.
    Platform-wide, so platform administrators only (SEC-10).
    """
    _platform_admin_only(http_request)
    from ..ai.training.training_runtime import app_training_runtime
    from ..ai.training.training_scheduler import start_admin_training

    start_admin_training(
        app_training_runtime(http_request.app),
        include_embedding_tune=request.include_embedding_tune,
        force=request.force,
    )
    return TrainResponse(run_id="scheduled", status="running", metrics={"message": "Training started in background"})


@router.post("/run/sync", response_model=TrainResponse)
async def run_training_sync(request: TrainRequest, http_request: Request):
    """Run training and wait for it (for admin/debug).

    The run is on the training runtime's thread, never the event loop.
    Platform-wide, so platform administrators only (SEC-10).
    """
    _platform_admin_only(http_request)
    try:
        from ..ai.training.training_runtime import app_training_runtime
        from ..ai.training.training_scheduler import run_admin_training

        run = await run_admin_training(
            app_training_runtime(http_request.app),
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
