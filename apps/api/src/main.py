"""
DataTransfer.space — API Server

Enterprise-grade data transfer platform with AI-powered semantic analysis.
"""

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from services.health_service import aggregate_health
from services.platform_config import (
    apply_railway_defaults,
    cors_origins,
    docs_enabled,
    enforce_production_config,
    is_production,
    is_railway,
    vector_store_dir,
)

from .middleware.auth_middleware import AuthMiddleware
from .middleware.tenant_middleware import TenantMiddleware
from .routers.ai_router import router as ai_router
from .routers.audit_router import router as audit_router
from .routers.auth_router import router as auth_router
from .routers.automation_router import router as automation_router
from .routers.catalog_router import router as catalog_router
from .routers.connectors_router import router as connectors_router
from .routers.contracts_router import router as contracts_router
from .routers.copilot_router import router as copilot_router
from .routers.mcp_router import router as mcp_router
from .routers.preflight_router import router as preflight_router
from .routers.query_router import router as query_router
from .routers.saved_connectors_router import router as saved_connectors_router
from .routers.schedules_router import router as schedules_router
from .routers.training_agent_router import router as training_agent_router
from .routers.transfer_router import router as transfer_router
from .routers.repair_router import router as repair_router
from .routers.ops_router import router as ops_router
from .routers.usage_router import router as usage_router
from .routers.workspace_router import router as workspace_router
from .services.rbac import RBACMiddleware

logger = logging.getLogger("dataflow.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management.

    Bind the HTTP server as soon as critical config is ready. Heavy work
    (RAG / HF model download, orphaned-job resume) runs *after* yield so
    Railway ``/health`` liveness can pass within the healthcheck window.
    """
    apply_railway_defaults()
    enforce_production_config()
    os.environ.setdefault("DATAFLOW_VECTOR_STORE_DIR", str(vector_store_dir()))
    try:
        from services.integrations_store import apply_integrations_to_env

        apply_integrations_to_env()
    except Exception as ie:
        print(f"[!] Integrations bootstrap warning: {ie}")

    print(f"[*] DataTransfer.space API starting (env={'production' if is_production() else 'development'})…")
    try:
        from .services.driver_bootstrap import ensure_platform_drivers

        driver_report = ensure_platform_drivers()
        app.state.driver_report = driver_report
        if driver_report["ready"]:
            print("[+] Platform connector drivers ready")
        else:
            missing = ", ".join(m["package"] for m in driver_report["missing"])
            print(f"[!] Platform drivers incomplete: {missing}")
    except Exception as de:
        print(f"[!] Driver bootstrap warning: {de}")

    training_enabled = os.getenv("DATAFLOW_TRAINING", "off" if is_production() else "on").lower() not in (
        "off", "0", "false",
    )

    # Accept traffic immediately — do not block on HuggingFace / RAG init.
    app.state.ready = False
    print("[+] HTTP listener ready (background warm-up starting)")

    async def _warm_up():
        try:
            from .ai.knowledge.semantic_patterns import get_pattern_count
            from .ai.knowledge.synonyms import get_synonym_count
            from .ai.rag.pipeline import get_rag_pipeline
            from .ai.training.training_scheduler import run_training_loop

            pipeline = get_rag_pipeline()
            init_result = await asyncio.to_thread(pipeline.initialize)
            print(f"[+] RAG Pipeline initialized: {init_result.get('ingested', 0)} documents")
            print(f"[+] Knowledge Base: {get_pattern_count()} patterns, {get_synonym_count()} synonyms")

            async def _background_training():
                await asyncio.sleep(120)
                try:
                    from .ai.training.training_agent import get_training_agent
                    training = get_training_agent()
                    run = await asyncio.to_thread(training.run_full_training, False, False)
                    ready = run.metrics.get("copilot_evaluation", {}).get("ready", False)
                    examples = run.metrics.get("conversation_examples", 0)
                    print(f"[+] Copilot Training Agent: {run.status} ({examples} examples, ready={ready})")
                except Exception as te:
                    print(f"[!] Copilot training warning: {te}")

            if training_enabled:
                app.state.training_task = asyncio.create_task(_background_training())
                asyncio.create_task(run_training_loop())
                print("[+] Training Agent enabled")
            else:
                print("[*] Training Agent disabled (DATAFLOW_TRAINING=off)")

            from .services.schedule_runner import run_schedule_loop
            asyncio.create_task(run_schedule_loop())
            print("[+] Pipeline scheduler started")
        except Exception as e:
            print(f"[!] RAG initialization warning: {e}")

        try:
            from services.transfer_scheduler import start as start_transfer_scheduler

            start_transfer_scheduler()

            from .services.mongodb_service import get_mongodb_service
            from .services.worker_leases import get_worker_lease_store
            from .transfer.background import run_transfer_async
            from .transfer.models import transfer_request_from_dict

            mongo = get_mongodb_service()
            lease_store = get_worker_lease_store()
            resumed = 0
            for job in mongo.list_jobs(limit=200):
                if job.get("status") in ("pending", "running", "paused", "retrying") and job.get("transfer_request"):
                    payload = job["transfer_request"]
                    if payload.get("requires_file_reupload"):
                        mongo.update_job_status(job["_id"], "failed", error="File re-upload required after restart")
                        continue
                    if lease_store.is_held(job["_id"]):
                        continue
                    request = transfer_request_from_dict(payload)
                    run_transfer_async(job["_id"], request, resume=True)
                    resumed += 1
            print(f"[+] Orphaned job resume scan complete ({resumed} job(s) rescheduled)")
        except Exception as e:
            print(f"[!] Orphaned job resume warning: {e}")
        finally:
            app.state.ready = True
            print("[+] Warm-up complete — readiness probes may pass")

    warm_task = asyncio.create_task(_warm_up())
    app.state.warm_task = warm_task

    yield

    warm = getattr(app.state, "warm_task", None)
    if warm and not warm.done():
        warm.cancel()
        try:
            await warm
        except asyncio.CancelledError:
            pass
    task = getattr(app.state, "training_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    print("[*] DataTransfer.space API shutting down…")


_docs = "/docs" if docs_enabled() else None
_redoc = "/redoc" if docs_enabled() else None

app = FastAPI(
    title="DataTransfer.space API",
    description="Universal Data Transfer Platform API",
    version="1.0.0",
    docs_url=_docs,
    redoc_url=_redoc,
    openapi_url="/openapi.json" if docs_enabled() else None,
    lifespan=lifespan,
)

_cors_origins = cors_origins()
# Railway deploys give each service a *.up.railway.app public domain, which is
# not known until runtime.  Match exactly one Railway subdomain so origins like
# https://evil.up.railway.app.attacker.com cannot pass.  The env var allows an
# explicit override if the default is too restrictive.
_cors_origin_regex = os.getenv("CORS_ORIGIN_REGEX")
if not _cors_origin_regex and is_railway():
    _cors_origin_regex = r"https://[a-zA-Z0-9_-]+\.up\.railway\.app$"

app.add_middleware(RBACMiddleware)
app.add_middleware(AuthMiddleware)
app.add_middleware(TenantMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=_cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    start_time = time.time()
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    try:
        response = await call_next(request)
    except (Exception, BaseException) as exc:
        # Catch unhandled endpoint and TaskGroup exceptions here so the outer
        # Starlette/anyio task group does not surface an ExceptionGroup and
        # crash the worker. Re-raise process-control exceptions.
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            raise
        # Unwrap a single ExceptionGroup so a clear message reaches the client.
        if isinstance(exc, BaseExceptionGroup) and len(getattr(exc, "exceptions", [])) == 1:
            exc = exc.exceptions[0]
        logger.exception("Unhandled error on %s", request.url.path)
        detail = str(exc) if not is_production() else "An unexpected error occurred"
        response = JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "detail": detail},
            headers={"X-Correlation-ID": correlation_id},
        )
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    response.headers["X-Correlation-ID"] = correlation_id

    path = request.url.path
    if (
        request.method != "GET"
        and path.startswith("/api/v1/")
        and "/health" not in path
    ):
        try:
            from services.audit_log import append_audit_event
            actor = getattr(request.state, "user_email", None) or "anonymous"
            append_audit_event(
                action=f"http.{request.method.lower()}",
                resource=path,
                actor=actor,
                level="error" if response.status_code >= 500 else "info",
                correlation_id=correlation_id,
                details={"status": response.status_code, "ms": round(process_time * 1000, 1)},
            )
        except Exception:
            pass

    return response


app.include_router(ai_router, prefix="/api/v1")
app.include_router(saved_connectors_router, prefix="/api/v1")
app.include_router(connectors_router, prefix="/api/v1")
app.include_router(preflight_router, prefix="/api/v1")
app.include_router(copilot_router, prefix="/api/v1")
app.include_router(training_agent_router, prefix="/api/v1")
app.include_router(transfer_router, prefix="/api/v1")
app.include_router(mcp_router, prefix="/api/v1")
app.include_router(automation_router, prefix="/api/v1")
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(schedules_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1")
# Compatibility mount when VITE_API_BASE omits /api/v1 (hits /auth/login).
app.include_router(auth_router)
app.include_router(audit_router, prefix="/api/v1")
app.include_router(workspace_router, prefix="/api/v1")
app.include_router(contracts_router, prefix="/api/v1")
app.include_router(query_router, prefix="/api/v1")
app.include_router(usage_router, prefix="/api/v1")
app.include_router(ops_router, prefix="/api/v1")
app.include_router(repair_router, prefix="/api/v1")


@app.get("/")
async def root():
    payload = {
        "name": "DataTransfer.space",
        "version": "1.0.0",
        "status": "operational",
        "environment": "production" if is_production() else "development",
    }
    if docs_enabled():
        payload["docs"] = "/docs"
    return payload


@app.get("/health")
async def health_check():
    """Liveness — process is up and accepting traffic.

    Railway deploy healthchecks must hit this path. Keep it cheap and never
    block on Mongo/RAG/catalog so a slow warm-up cannot fail the deploy.
    """
    return {
        "status": "healthy",
        "liveness": True,
        "ready": bool(getattr(app.state, "ready", False)),
    }


@app.get("/health/ready")
async def health_ready():
    """Readiness — dependencies (Mongo, storage, drivers) are usable."""
    payload = aggregate_health()
    payload["ready"] = bool(getattr(app.state, "ready", False))
    if not payload["ready"] and payload.get("status") == "healthy":
        payload["status"] = "starting"
    return payload


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus exposition format for job/CDC/quarantine ops metrics."""
    from services.ops_metrics import prometheus_text

    return PlainTextResponse(
        content=prometheus_text(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/api/v1")
async def api_info():
    return {"version": "1.0.0", "status": "ok"}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s", request.url.path)
    detail = str(exc) if not is_production() else "An unexpected error occurred"
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": detail,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
