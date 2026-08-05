"""
Datawrap — API Server

Enterprise-grade data transfer platform with AI-powered semantic analysis.
"""

import asyncio
import logging
import os
from services.brand_env import getenv_brand
import time
import uuid
from contextlib import asynccontextmanager, nullcontext

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from services.cors_policy import TenantAwareCORSMiddleware
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
from .routers.resource_acl_router import router as resource_acl_router
from .routers.cdc_mapping_review_router import router as cdc_mapping_review_router
from .routers.transforms_router import router as transforms_router
from .routers.copilot_router import router as copilot_router
from .routers.mcp_router import router as mcp_router
from .routers.migration_kernel_router import router as migration_kernel_router
from .routers.ops_router import router as ops_router
from .routers.preflight_router import router as preflight_router
from .routers.query_router import router as query_router
from .routers.repair_router import router as repair_router
from .routers.saved_connectors_router import router as saved_connectors_router
from .routers.schedules_router import router as schedules_router
from .routers.training_agent_router import router as training_agent_router
from .transfer.engine import DuplicateTransferSubmission
from .routers.transfer_router import router as transfer_router
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
    # Configure logging before anything else can emit a line, so no startup
    # message escapes with Uvicorn's default handler and no correlation fields.
    try:
        from services.logging_config import configure_logging

        configure_logging()
    except Exception as le:  # pragma: no cover - logging must never block boot
        print(f"[!] Logging bootstrap warning: {le}")
    os.environ.setdefault("DATAFLOW_VECTOR_STORE_DIR", str(vector_store_dir()))
    try:
        from services.integrations_store import apply_integrations_to_env

        apply_integrations_to_env()
    except Exception as ie:
        print(f"[!] Integrations bootstrap warning: {ie}")

    print(f"[*] Datawrap API starting (env={'production' if is_production() else 'development'})…")
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

    training_enabled = getenv_brand("TRAINING", "off" if is_production() else "on").lower() not in (
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
        except Exception as e:
            print(f"[!] RAG initialization warning: {e}")

        # Scheduler must start even when RAG warm-up fails — otherwise due
        # pipelines sit on Next run forever with Runs=0.
        try:
            from services.schedule_store import import_file_schedules_into_mongo

            imported = await asyncio.to_thread(import_file_schedules_into_mongo)
            if imported:
                print(f"[+] Imported {imported} pipeline schedule(s) from schedules.json → MongoDB")

            from .services.schedule_runner import run_schedule_loop

            asyncio.create_task(run_schedule_loop())
            print("[+] Pipeline scheduler started")
        except Exception as e:
            print(f"[!] Pipeline scheduler failed to start: {e}")

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

    # Opt-in OpenTelemetry. Off by default; when enabled, every transfer root
    # span and phase span is exported. Failure here must never block boot.
    try:
        from services.tracing import ensure_provider, tracing_enabled

        if tracing_enabled():
            ensure_provider()
    except Exception as te:
        print(f"[!] Tracing bootstrap warning: {te}")

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
    try:
        from services.tracing import shutdown_tracing

        shutdown_tracing()
    except Exception:
        pass
    print("[*] Datawrap API shutting down…")


_docs = "/docs" if docs_enabled() else None
_redoc = "/redoc" if docs_enabled() else None

app = FastAPI(
    title="Datawrap API",
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

# Enterprise CORS: no wildcard methods/headers, explicit list only.
# Credentials are only sent from the configured origins (cors_origins) or the
# Railway subdomain regex.  Add ``X-Workspace-Id`` / ``X-Correlation-ID`` to the
# allowlist so multi-tenant and trace headers work across origins.
_ALLOW_HEADERS = [
    "accept",
    "accept-encoding",
    "accept-language",
    "authorization",
    "content-type",
    "origin",
    "traceparent",
    "tracestate",
    "x-correlation-id",
    "x-requested-with",
    "x-workspace-id",
]
_ALLOW_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
_EXPOSE_HEADERS = ["X-Correlation-ID", "X-Process-Time", "X-Trace-Id"]

app.add_middleware(
    TenantAwareCORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=_cors_origin_regex,
    allow_credentials=True,
    allow_methods=_ALLOW_METHODS,
    allow_headers=_ALLOW_HEADERS,
    expose_headers=_EXPOSE_HEADERS,
)


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    start_time = time.time()
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    # Bridge the inbound correlation id (and any W3C traceparent) into the
    # transfer root span so an API call and the job it starts share one
    # operator-visible identity. No-ops when tracing is off.
    try:
        from services.tracing import (
            current_trace_id,
            set_correlation_id,
            set_traceparent,
            start_span,
        )

        set_correlation_id(correlation_id)
        set_traceparent(request.headers.get("traceparent"))
    except Exception:
        start_span = None  # type: ignore[assignment]
        current_trace_id = lambda: ""  # noqa: E731

    span_cm = (
        start_span(
            f"HTTP {request.method} {request.url.path}",
            attributes={
                "http.method": request.method,
                "http.route": request.url.path,
                "dataflow.correlation_id": correlation_id,
            },
            kind="server",
        )
        if start_span is not None
        else nullcontext()
    )

    trace_id = ""
    try:
        with span_cm:
            response = await call_next(request)
            try:
                trace_id = current_trace_id() or ""
            except Exception:
                trace_id = ""
    except BaseException as exc:
        # Catch unhandled endpoint and TaskGroup exceptions here so the outer
        # Starlette/anyio task group does not surface an ExceptionGroup and
        # crash the worker. Re-raise process-control exceptions.
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            raise
        # Unwrap a single exception group (anyio TaskGroup / asyncio) so a clear
        # message reaches the client.  Duck-type ``exceptions`` to stay portable.
        if len(getattr(exc, "exceptions", [])) == 1:
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
    if trace_id:
        response.headers["X-Trace-Id"] = trace_id
    # Baseline security headers for the enterprise API.  HSTS is intentionally
    # omitted here so TLS termination (Cloudflare, AWS ALB, etc.) can add it.
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")

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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    return response


app.include_router(ai_router, prefix="/api/v1")
app.include_router(saved_connectors_router, prefix="/api/v1")
app.include_router(connectors_router, prefix="/api/v1")
app.include_router(preflight_router, prefix="/api/v1")
app.include_router(copilot_router, prefix="/api/v1")
app.include_router(training_agent_router, prefix="/api/v1")
app.include_router(transfer_router, prefix="/api/v1")
app.include_router(mcp_router, prefix="/api/v1")
app.include_router(migration_kernel_router, prefix="/api/v1")
app.include_router(automation_router, prefix="/api/v1")
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(schedules_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1")
# Compatibility mount when VITE_API_BASE omits /api/v1 (hits /auth/login).
app.include_router(auth_router)
app.include_router(audit_router, prefix="/api/v1")
app.include_router(cdc_mapping_review_router, prefix="/api/v1")
app.include_router(workspace_router, prefix="/api/v1")
app.include_router(contracts_router, prefix="/api/v1")
app.include_router(resource_acl_router, prefix="/api/v1")
app.include_router(transforms_router, prefix="/api/v1")
app.include_router(query_router, prefix="/api/v1")
app.include_router(usage_router, prefix="/api/v1")
app.include_router(ops_router, prefix="/api/v1")
app.include_router(repair_router, prefix="/api/v1")


@app.get("/")
async def root():
    payload = {
        "name": "Datawrap",
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
        # Present only after proxy-write hardening is deployed. Use this to
        # confirm Railway is not still running a pre-fix API image.
        "features": {
            "proxy_write_ledger": True,
            "proxy_write_reconnect": True,
        },
    }


@app.get("/api/v1/health")
async def health_check_v1():
    """Same liveness under the versioned API prefix (web proxy /api/v1/health)."""
    return await health_check()


@app.get("/health/ready")
async def health_ready():
    """Readiness — dependencies (Mongo, storage, drivers) are usable.

    Returns 503 Service Unavailable until warm-up completes and all
    dependency checks report healthy so Railway does not route traffic
    to a pod that cannot accept transfer jobs.
    """
    payload = aggregate_health()
    warm = bool(getattr(app.state, "ready", False))
    payload["ready"] = warm and payload.get("status") == "healthy"
    if not payload["ready"] and payload.get("status") == "healthy":
        payload["status"] = "starting"
    status_code = 200 if payload["ready"] else 503
    return JSONResponse(content=payload, status_code=status_code)


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
    return {
        "version": "1.0.0",
        "status": "ok",
        "api_prefix": "/api/v1",
        "policy_url": "/docs/API_VERSIONING.md",
        "deprecation": None,
        "honesty": "Public control-plane API is /api/v1 only until a documented /api/v2 exists.",
    }


@app.exception_handler(DuplicateTransferSubmission)
async def duplicate_transfer_handler(
    request: Request, exc: DuplicateTransferSubmission
):
    """Report a double-submit as a conflict that names the run already in flight.

    Registered centrally so every submit path — Transfer Studio, the Pilot, the
    schedule runner, retries — answers the same way instead of each route
    inventing its own handling.
    """
    logger.info(
        "Duplicate transfer submission on %s deduplicated to job %s (%s)",
        request.url.path,
        exc.existing_job_id or "unknown",
        exc.existing_status or "in progress",
    )
    return JSONResponse(
        status_code=409,
        content={
            "error": "duplicate_transfer",
            "detail": str(exc),
            "existing_job_id": exc.existing_job_id,
            "existing_status": exc.existing_status,
            "job_id": exc.existing_job_id,
        },
    )


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

    # Container deployments bind to all interfaces; Railway/ALB provide the firewall.
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))  # nosec: B104
