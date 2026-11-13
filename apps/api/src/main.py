"""
Datawrap — API Server

Enterprise-grade data transfer platform with AI-powered semantic analysis.
"""

import asyncio
import logging
import os
import threading
import time
from contextlib import asynccontextmanager, nullcontext

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from services.brand_env import getenv_brand
from services import connector_executor
from services.connector_store import ConnectorStoreError
from services.control_plane_store import ControlPlaneUnavailable
from services.transfer_plan_store import PlanStoreBusy, PlanStoreLockUnavailable, PlanStoreUnreadable
from services.shared_doc_store import SharedDocConflict
from services.cors_policy import TenantAwareCORSMiddleware
from services import health_service
from services.safe_errors import public_error
from services.upload_limits import PayloadTooLarge
from services.procedure_source import ProcedureSourceError
from services.data_residency import ResidencyRefused
from services.audit_coverage import AuditAccessMiddleware, http_route_template

# Tests and callers historically import the route-template helper from the
# app module. Keep that name stable; the implementation lives in audit_coverage.
_http_route_template = http_route_template
from services.tracing import sanitize_correlation_id
from services.platform_config import (
    apply_railway_defaults,
    cors_origin_regex,
    cors_origins,
    docs_enabled,
    is_production,
    validate_production_config,
    vector_store_dir,
)

from services.api_errors import ApiError, api_error_handler

from .middleware.auth_middleware import AuthMiddleware
from .middleware.production_config_middleware import ProductionConfigMiddleware
from .middleware.tenant_middleware import TenantMiddleware
from .middleware.proxy_fix_middleware import ProxyFixMiddleware
from .middleware.own_host_redirect_middleware import OwnHostRedirectMiddleware
from .middleware.workspace_ip_allowlist_middleware import WorkspaceIpAllowlistMiddleware
from .middleware.tenant_scope_middleware import TenantScopeMiddleware
from .middleware.connector_access_middleware import ConnectorAccessMiddleware
from .routers.ai_router import router as ai_router
from .routers.audit_router import router as audit_router
from .routers.auth_router import router as auth_router
from .routers.mfa_router import admin_router as mfa_admin_router, router as mfa_router
from .routers.iam_router import router as iam_router
from .routers.scim_router import router as scim_router
from .routers.automation_router import router as automation_router
from .routers.catalog_router import router as catalog_router
from .routers.connectors_router import router as connectors_router
from .routers.jobs_router import router as jobs_router
from .routers.contact_router import router as contact_router
from .routers.contracts_router import router as contracts_router
from .routers.fidelity_router import router as fidelity_router
from .routers.parity_router import router as parity_router
from .routers.reversibility_router import router as reversibility_router
from .routers.exit_router import router as exit_router
from .routers.resource_acl_router import router as resource_acl_router
from .routers.cdc_mapping_review_router import router as cdc_mapping_review_router
from .routers.transform_library_router import router as transform_library_router
from .routers.transforms_router import router as transforms_router
from .routers.copilot_router import router as copilot_router
from .routers.mcp_router import oauth_resource_router, router as mcp_router
from .routers.ops_router import router as ops_router
from .routers.preflight_router import router as preflight_router
from .routers.query_router import router as query_router
from .routers.repair_router import router as repair_router
from .routers.saved_connectors_router import router as saved_connectors_router
from .routers.schedules_router import router as schedules_router
from .routers.signup_router import router as signup_router
from .routers.rule_compiler_router import router as rule_compiler_router
from .routers.shape_router import router as shape_router
from .routers.journals_router import router as journals_router
from .routers.status_router import router as status_router
from .routers.training_agent_router import router as training_agent_router
from .transfer.engine import DuplicateTransferSubmission
from .routers.transfer_router import router as transfer_router
from .routers.usage_router import router as usage_router
from .routers.team_router import router as team_router
from .routers.invite_router import router as invite_router
from .routers.workspace_router import router as workspace_router
from .services.rbac import RBACMiddleware

logger = logging.getLogger("dataflow.api")
_SECURITY_SETTINGS_SNAPSHOT_RECORDED = False
_SECURITY_SETTINGS_SNAPSHOT_LOCK = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management.

    Bind the HTTP server as soon as critical config is ready. Heavy work
    (RAG / HF model download, orphaned-job resume) runs *after* yield so
    Railway ``/health`` liveness can pass within the healthcheck window.
    """
    apply_railway_defaults()
    # SIGTERM sets a process-wide flag before uvicorn's own handler runs, so
    # long-lived responses (the job stream) end at once and the teardown below
    # is reached within --timeout-graceful-shutdown (ops review High 4).
    from services import shutdown_signal

    shutdown_signal.reset()
    shutdown_signal.install()
    # Do not sys.exit here. The socket is not accepting yet, so a fatal
    # config used to fail Railway's /health probe as "service unavailable"
    # for the whole window. Liveness stays up; other routes return 503.
    from services.process_role import topology_errors

    config_errors = validate_production_config() + topology_errors()
    app.state.config_errors = config_errors
    if config_errors:
        for msg in config_errors:
            print(f"[FATAL] Production config: {msg}")
        print("[!] Refusing traffic except /health until production config is fixed")
    from services.platform_config import persistent_storage_warnings

    for msg in persistent_storage_warnings():
        print(f"[!] Persistent storage: {msg}")
    # Configure logging before anything else can emit a line, so no startup
    # message escapes with Uvicorn's default handler and no correlation fields.
    try:
        from services.logging_config import configure_logging

        configure_logging()
    except Exception as le:  # pragma: no cover - logging must never block boot
        print(f"[!] Logging bootstrap warning: {le}")
    # Review (low): a flag set differently under DATAWRAP_ and DATAFLOW_ is named, never its values.
    from services.brand_env import warn_brand_flag_conflicts

    warn_brand_flag_conflicts(logger)
    from services.local_file_policy import local_file_settings, warn_if_local_file_confinement_off

    warn_if_local_file_confinement_off(logging.getLogger("datawrap.security"))
    from services.platform_aws_credentials import (
        platform_aws_settings,
        warn_if_platform_aws_credentials_allowed,
    )

    warn_if_platform_aws_credentials_allowed(logging.getLogger("datawrap.security"))
    from services.platform_gcp_azure_credentials import (
        platform_gcp_azure_settings,
        warn_if_platform_gcp_azure_credentials_allowed,
    )

    warn_if_platform_gcp_azure_credentials_allowed(logging.getLogger("datawrap.security"))
    from services.sso_authorization import warn_sso_auto_provision_without_domains

    warn_sso_auto_provision_without_domains(logging.getLogger("datawrap.security"))
    try:
        # Saved IP allowlists with no proxy hop count: say so once at boot.
        from services.ip_allowlist_policy import warn_if_hop_count_unset

        warn_if_hop_count_unset(logging.getLogger("datawrap.security"))
    except Exception as ae:  # pragma: no cover - a hint must never block boot
        print(f"[!] IP allowlist startup check skipped: {ae}")
    global _SECURITY_SETTINGS_SNAPSHOT_RECORDED
    with _SECURITY_SETTINGS_SNAPSHOT_LOCK:
        record_snapshot = not _SECURITY_SETTINGS_SNAPSHOT_RECORDED
        _SECURITY_SETTINGS_SNAPSHOT_RECORDED = True
    if record_snapshot:
        try:
            from services import audit_log, rbac
            from services.audit_coverage import record_security_event
            from services.team_store import require_workspace_isolation
            from src.services import auth_service as _auth_service

            try:
                audit_retention_days = audit_log._retention_days()
            except Exception:
                audit_retention_days = "invalid"
            record_security_event(
                "system.security_settings.snapshot",
                outcome="succeeded",
                actor="system",
                details={
                    "production": bool(is_production()),
                    "auth_required": bool(_auth_service.auth_required()),
                    "workspace_isolation": bool(require_workspace_isolation()),
                    "rbac_mode": str(rbac.UNRULED_ROUTE_MODE),
                    "audit_store": (
                        "mongo" if audit_log._mongo_collection() is not None else "file"
                    ),
                    "audit_retention_days": audit_retention_days,
                    "docs_enabled": bool(docs_enabled()),
                    "config_error_count": len(config_errors),
                    **local_file_settings(),
                    **platform_aws_settings(),
                    **platform_gcp_azure_settings(),
                },
            )
        except Exception as exc:
            logger.warning(
                "Security settings snapshot failed (%s)",
                type(exc).__name__,
            )
    from services.rbac import record_unruled_routes_allowed_startup

    record_unruled_routes_allowed_startup()
    # K1: once per deployment, give workspace-less API keys their workspace
    # back (idempotent, audited, never blocks boot).
    from services.orphaned_api_keys import run_orphaned_api_key_migration

    run_orphaned_api_key_migration()
    # Durable mail outbox: a restart can leave 'queued' rows behind —
    # re-queue them through the send lane so no reset/OTP mail is lost.
    try:
        from services.signup_service import (
            recover_mail_outbox,
            start_mail_outbox_sweeper,
        )

        recover_mail_outbox()
        # A row in retry backoff at restart is not due yet; keep sweeping.
        start_mail_outbox_sweeper()
    except Exception as oe:  # pragma: no cover - recovery must not block boot
        print(f"[!] Mail outbox drain warning: {oe}")
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

    from .ai.training.training_runtime import TrainingRuntime
    from .ai.training.training_scheduler import training_enabled as _training_enabled

    training_enabled = _training_enabled()
    # Always one runtime: admin routes (/training-agent/run, /copilot/train)
    # run on it even with background training off, and shutdown stops them.
    # Kept local too: a second lifespan of this app replaces app.state's, and
    # each lifespan must stop its own.
    training = TrainingRuntime()
    app.state.training_runtime = training

    # Build the route table now, off the request path: FastAPI would
    # otherwise do it on the event loop inside the first request (usually the
    # first health probe), which then takes ~1s instead of milliseconds.
    from services.route_table_warmup import warm_route_table

    await asyncio.to_thread(warm_route_table, app)

    # Accept traffic immediately — do not block on HuggingFace / RAG init.
    app.state.ready = False
    # Loop tasks started below are kept so shutdown can cancel them (M-9).
    app.state.background_tasks = []
    print("[+] HTTP listener ready (background warm-up starting)")

    async def _warm_up():
        try:
            from .ai.knowledge.semantic_patterns import get_pattern_count
            from .ai.knowledge.synonyms import get_synonym_count
            from .ai.rag.pipeline import get_rag_pipeline
            from .ai.training.training_scheduler import run_background_training, run_training_loop

            pipeline = get_rag_pipeline()
            init_result = await asyncio.to_thread(pipeline.initialize)
            print(f"[+] RAG Pipeline initialized: {init_result.get('ingested', 0)} documents")
            print(f"[+] Knowledge Base: {get_pattern_count()} patterns, {get_synonym_count()} synonyms")

            if training_enabled:
                # Own daemon threads + stop event: shutdown never waits on a run.
                app.state.training_task = training.start_task(
                    run_background_training(training), name="training-warmup")
                app.state.training_loop_task = training.start_task(
                    run_training_loop(runtime=training), name="training-loop")
                print("[+] Training Agent enabled")
            else:
                print("[*] Training Agent disabled (DATAFLOW_TRAINING=off)")
        except Exception as e:
            print(f"[!] RAG initialization warning: {e}")

        # H-5: tenant settings, BYOK keys, ACLs and the other control-plane
        # stores live in the shared store on a multi-replica deployment. Import
        # this node's files once (idempotent; the files are left in place).
        try:
            from services.shared_doc_store import import_local_files

            imported_stores = await asyncio.to_thread(import_local_files)
            if imported_stores:
                print(f"[+] Imported control-plane store file(s) into the shared store: {sorted(imported_stores)}")
        except Exception as import_exc:  # noqa: BLE001 - a store that cannot be reached fails closed on use
            print(f"[!] Control-plane store import skipped: {import_exc}")

        # Review M3: exports written before workspaces had folders move to the
        # folder of the workspace whose job records own them. Idempotent, and
        # off the lifespan's critical path (round 2, Low): a large flat folder
        # never delays the API's readiness.
        from services.export_migration import migrate_flat_exports_at_startup, start_in_background

        app.state.flat_export_migration = start_in_background(migrate_flat_exports_at_startup)
        # Review round 3 (H3): the shared SQLite/DuckDB volumes are partitioned per
        # workspace. The partition time is recorded before any request is served
        # (connectors saved after it prove no ownership); owned flat files then
        # move home off the critical path.
        try:
            from services.embedded_volume import mark_partitioned, migrate_flat_volume_files_at_startup

            mark_partitioned()
            app.state.flat_volume_migration = start_in_background(migrate_flat_volume_files_at_startup)
        except OSError as exc:
            print(f"[!] SQLite/DuckDB volume partition marker not written: {exc}")
        # Cadence stays up when RAG warm-up fails — otherwise due pipelines
        # sit on Next run forever with Runs=0. Multi-replica API pods set
        # SCHEDULE_LOOP=0; src.scheduler_main owns the loop there.
        try:
            from services.process_role import schedule_loop_enabled
            from services.schedule_store import import_file_schedules_into_mongo

            imported = await asyncio.to_thread(import_file_schedules_into_mongo)
            if imported:
                print(f"[+] Imported {imported} pipeline schedule(s) from schedules.json → MongoDB")

            # QA Snowflake D12: report schedules no workspace can see (dry run
            # unless DATAFLOW_SCHEDULE_ORPHAN_REPAIR=apply). Never fatal.
            try:
                from services.schedule_orphans import startup_orphan_scan

                await asyncio.to_thread(startup_orphan_scan)
            except Exception as orphan_exc:  # noqa: BLE001
                print(f"[!] Schedule orphan scan skipped: {orphan_exc}")

            if schedule_loop_enabled():
                from .services.schedule_runner import run_schedule_loop

                app.state.background_tasks.append(asyncio.create_task(run_schedule_loop()))
                print("[+] Pipeline scheduler started")
            else:
                print("[+] Pipeline scheduler not started in this process (SCHEDULE_LOOP off)")
        except Exception as e:
            print(f"[!] Pipeline scheduler failed to start: {e}")

        try:
            from services.process_role import api_executes_transfers, orphan_resume_enabled
            from services.scheduler_mode import scheduler_mode

            if api_executes_transfers():
                from services.transfer_scheduler import start as start_transfer_scheduler

                start_transfer_scheduler()

                # Phase F5 — claim-queue pull on API when the operator left the
                # API claim loop on (single replica, or no worker Deployment yet).
                try:
                    from services.worker_fleet import start_api_claim_loop

                    if start_api_claim_loop():
                        print(f"[+] API claim loop started (scheduler_mode={scheduler_mode()})")
                    else:
                        print(f"[+] Transfer scheduler local mode (scheduler_mode={scheduler_mode()})")
                except Exception as claim_exc:
                    print(f"[!] API claim loop not started: {claim_exc}")
            else:
                print(
                    "[+] This process enqueues transfers only "
                    f"(scheduler_mode={scheduler_mode()})"
                )

            if orphan_resume_enabled():
                from services.orphan_jobs import start_orphan_sweeper, sweep_orphan_jobs

                settled = sweep_orphan_jobs()
                counts: dict[str, int] = {}
                for row in settled:
                    counts[row["action"]] = counts.get(row["action"], 0) + 1
                start_orphan_sweeper()
                print(f"[+] Orphaned job sweep started (first pass: {counts or 'none'})")
            else:
                print("[+] Orphan resume scan skipped")

            # D10: the per-step stall watchdog flags and then fails a job whose
            # active phase goes quiet past its threshold — an orphan sweep only
            # sees a job once every owner signal is gone, which a live-but-stuck
            # worker never is.
            try:
                from services.job_watchdog import start_stall_watchdog

                start_stall_watchdog()
                print("[+] Job stall watchdog started")
            except Exception as e:  # noqa: BLE001 — a bad watchdog must not kill boot
                print(f"[!] Stall watchdog failed to start: {e}")
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

    shutdown_signal.request()
    # Running exit-rehearsal verifications (PR 402) stop inside the budgeted
    # stop_background_threads below: claims released, nothing closed.
    warm = getattr(app.state, "warm_task", None)
    if warm and not warm.done():
        warm.cancel()
        try:
            await warm
        except asyncio.CancelledError:
            pass
    # This lifespan's training runtime: sets its stop event, cancels its tasks,
    # waits <= 1 s (PR 410).
    await training.shutdown()
    # M-9: nothing this app started may outlive it. The schedule loops are
    # cancelled; the four threads are stopped and joined off the event loop
    # (the claim loop drains in-flight transfers first, bounded by
    # WORKER_DRAIN_SECONDS, and releases their leases), and the OTel log and
    # trace flushes run last inside that budget.
    from services.background_lifecycle import cancel_background_tasks, stop_background_threads

    await cancel_background_tasks(getattr(app.state, "background_tasks", []))
    app.state.background_tasks = []
    # The OTel log and trace flushes are the last budgeted stop (an unreachable
    # collector cannot hold the process past the shutdown budget).
    await asyncio.to_thread(stop_background_threads)
    app.state.ready = False
    print("[*] Datawrap API shutting down…")
    # The process exits next; an in-process restart (tests) starts clean.
    shutdown_signal.reset()


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


def _build_commit() -> str:
    """Best-effort deployed git SHA for health reporting.

    Env first — Railway injects RAILWAY_GIT_COMMIT_SHA, Docker images may
    carry DATAFLOW_COMMIT_SHA/SOURCE_COMMIT at build time. Otherwise the
    checkout's HEAD. "unknown" rather than a hardcoded fake.
    """
    env = (
        getenv_brand("DATAFLOW_COMMIT_SHA")
        or os.getenv("RAILWAY_GIT_COMMIT_SHA")
        or os.getenv("SOURCE_COMMIT")
        or ""
    ).strip()
    if env:
        return env[:12]
    try:
        import subprocess

        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        out = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else "unknown"
    except Exception:
        return "unknown"


_BUILD_COMMIT = _build_commit()

_cors_origins = cors_origins()
# No implicit origin pattern: any https://*.up.railway.app page could be a
# stranger's, so credentials go only to the exact origins in cors_origins()
# (an operator may still set CORS_ORIGIN_REGEX deliberately).
_cors_origin_regex = cors_origin_regex()

# Innermost: binds the authenticated caller for saved-connector resolution
# (services.connector_access.current_caller), inside the tenant scope it narrows.
app.add_middleware(ConnectorAccessMiddleware)
app.add_middleware(TenantScopeMiddleware)
app.add_middleware(AuditAccessMiddleware)
app.add_middleware(RBACMiddleware)
# Workspace IP allowlist: after auth (needs the actor), before RBAC/routers.
app.add_middleware(WorkspaceIpAllowlistMiddleware)
app.add_middleware(AuthMiddleware)
app.add_middleware(TenantMiddleware)
# Outermost: a missing production secret must not reach auth or transfer.
app.add_middleware(ProductionConfigMiddleware)

# Enterprise CORS: no wildcard methods/headers, explicit list only.
# Credentials are only sent from the exact configured origins (cors_origins),
# or an operator-set CORS_ORIGIN_REGEX.  Add ``X-Workspace-Id`` / ``X-Correlation-ID`` to the
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
    from services.audit_coverage import begin_request_audit, end_request_audit

    token = begin_request_audit()
    try:
        return await _add_timing_header_response(request, call_next)
    finally:
        end_request_audit(token)


async def _add_timing_header_response(request: Request, call_next):
    from services.audit_coverage import current_request_audit

    start_time = time.time()
    raw_correlation_id = request.headers.get("X-Correlation-ID")
    correlation_id, correlation_id_rejected = sanitize_correlation_id(raw_correlation_id)
    request.state.correlation_id = correlation_id
    request.state.correlation_id_rejected = correlation_id_rejected
    tracker = current_request_audit()
    if tracker is not None:
        tracker["correlation_id_rejected"] = bool(correlation_id_rejected)
    if correlation_id_rejected:
        from services.ops_events import emit_event

        emit_event(
            logger,
            "http.correlation_id.rejected",
            outcome="failed",
            correlation_id_rejected=True,
            length=len(raw_correlation_id or ""),
            route_path=request.url.path,
            correlation_id=correlation_id,
        )
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
            request.method,
            attributes={
                "http.request.method": request.method,
                "http.method": request.method,
                "url.path": request.url.path,
                "url.scheme": request.url.scheme,
                "dataflow.correlation_id": correlation_id,
            },
            kind="server",
        )
        if start_span is not None
        else nullcontext()
    )

    trace_id = ""
    try:
        with span_cm as span:
            try:
                response = await call_next(request)
            except BaseException:
                if span is not None:
                    try:
                        span.set_attributes(
                            {
                                "http.response.status_code": 500,
                                "http.status_code": 500,
                            }
                        )
                    except Exception as span_exc:
                        logger.debug("Could not set HTTP span status attributes: %s", span_exc)
                raise
            finally:
                route_template = http_route_template(
                    request.url.path,
                    getattr(request.scope.get("route"), "path", None),
                )
                if route_template and span is not None:
                    try:
                        span.update_name(f"{request.method} {route_template}")
                        span.set_attributes({"http.route": route_template})
                    except Exception as span_exc:
                        logger.debug("Could not set HTTP span route: %s", span_exc)
            if span is not None:
                try:
                    span.set_attributes(
                        {
                            "http.response.status_code": response.status_code,
                            "http.status_code": response.status_code,
                        }
                    )
                    if response.status_code >= 500:
                        from opentelemetry.trace import Status, StatusCode

                        span.set_status(Status(StatusCode.ERROR))
                except Exception as span_exc:
                    logger.debug("Could not set HTTP span response status: %s", span_exc)
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
        route_template = http_route_template(
            request.url.path,
            getattr(request.scope.get("route"), "path", None),
        )
        safe_fields = {"method": request.method}
        if route_template:
            safe_fields["route_template"] = route_template
        detail, correlation_id = public_error(
            exc,
            event="http.request.unhandled",
            public_message="An unexpected error occurred.",
            status_code=500,
            **safe_fields,
        )
        logger.error(
            "http.request.unhandled.traceback",
            exc_info=exc,
            extra={
                "event": "http.request.unhandled.traceback",
                "correlation_id": correlation_id,
            },
        )
        response = JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": detail,
                "correlation_id": correlation_id,
            },
            headers={"X-Correlation-ID": correlation_id},
        )
    if tracker is not None:
        tracker["workspace_header_rejected"] = bool(
            getattr(request.state, "workspace_header_rejected", False)
        )
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    # M-7: request count and latency by route template (never the raw path).
    from services.ops_metrics import record_http_request, route_template

    record_http_request(
        method=request.method,
        route=route_template(request.scope),
        status_code=response.status_code,
        duration_seconds=process_time,
    )
    response.headers["X-Correlation-ID"] = correlation_id
    if trace_id:
        response.headers["X-Trace-Id"] = trace_id
    # Baseline security headers for the enterprise API.
    # HSTS is set when the request is HTTPS or the proxy says it is
    # (X-Forwarded-Proto). If TLS terminates in front of this process and that
    # header is not forwarded, set DATAFLOW_HSTS=1 on the API service.
    # DATAFLOW_HSTS=0 turns it off. CSP is an API policy (no document origin).
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if request.url.path.startswith(("/docs", "/redoc")):
        response.headers.setdefault(
            "Content-Security-Policy",
            "frame-ancestors 'none'; base-uri 'self'",
        )
    else:
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
        )
    hsts_flag = getenv_brand("DATAFLOW_HSTS", "").strip().lower()
    forwarded = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    https = request.url.scheme == "https" or forwarded == "https"
    if hsts_flag in {"1", "true", "on", "yes"}:
        https = True
    elif hsts_flag in {"0", "false", "off", "no"}:
        https = False
    if https:
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains",
        )

    path = request.url.path
    named_admin = False
    if request.method != "GET" and path.startswith("/api/v1/"):
        try:
            from starlette.concurrency import run_in_threadpool

            from services.admin_audit import record_unhandled_admin_change

            # An audit append can seal the chain tip in the shared anchor store: off the event loop.
            named_admin = await run_in_threadpool(record_unhandled_admin_change, request, response.status_code)
        except Exception as exc:
            logging.getLogger(__name__).warning("Admin audit suppressed: %s", exc, exc_info=exc)
    if (
        request.method != "GET"
        and not named_admin
        and path.startswith("/api/v1/")
        and "/health" not in path
        and (tracker is None or tracker.get("named", 0) == 0)
    ):
        try:
            from services.audit_log import append_audit_event
            from services.audit_coverage import request_principal
            from services.workspace_access import authorized_workspace_id

            actor = getattr(request.state, "user_email", None) or "anonymous"
            workspace_id = authorized_workspace_id(request)
            details = {
                "status": response.status_code,
                "ms": round(process_time * 1000, 1),
            }
            if getattr(request.state, "correlation_id_rejected", False):
                details["correlation_id_rejected"] = True
            if getattr(request.state, "workspace_header_rejected", False):
                details["workspace_header_rejected"] = True
            from starlette.concurrency import run_in_threadpool

            await run_in_threadpool(
                append_audit_event,
                action=f"http.{request.method.lower()}",
                resource=path,
                actor=actor,
                level="error" if response.status_code >= 500 else "info",
                correlation_id=correlation_id,
                workspace_id=workspace_id,
                details=details,
                principal=request_principal(request),
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    return response


# A redirect to the API's own host (redirect_slashes' 307) goes out relative,
# never naming the internal host or its plain-http scheme (D339).
app.add_middleware(OwnHostRedirectMiddleware)
# Outermost: client address and scheme from the trusted proxy chain
# (DATAWRAP_TRUSTED_PROXY_COUNT, ProxyFix semantics) before anything reads them.
app.add_middleware(ProxyFixMiddleware)


app.include_router(ai_router, prefix="/api/v1")
app.include_router(saved_connectors_router, prefix="/api/v1")
app.include_router(connectors_router, prefix="/api/v1")
app.include_router(jobs_router, prefix="/api/v1")
app.include_router(contact_router, prefix="/api/v1")
app.include_router(preflight_router, prefix="/api/v1")
app.include_router(copilot_router, prefix="/api/v1")
app.include_router(training_agent_router, prefix="/api/v1")
app.include_router(transfer_router, prefix="/api/v1")
app.include_router(mcp_router, prefix="/api/v1")
app.include_router(oauth_resource_router)
app.include_router(automation_router, prefix="/api/v1")
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(schedules_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1")
# Compatibility mount when VITE_API_BASE omits /api/v1 (hits /auth/login).
app.include_router(auth_router)
# Two-factor (TOTP) self-service and the challenge exchange, on both mounts.
app.include_router(mfa_router, prefix="/api/v1")
app.include_router(mfa_router)
app.include_router(mfa_admin_router, prefix="/api/v1")
# Self-serve signup: every route 404s while DATAWRAP_SIGNUP_ENABLED is off.
app.include_router(signup_router, prefix="/api/v1")
app.include_router(signup_router)
app.include_router(audit_router, prefix="/api/v1")
app.include_router(iam_router, prefix="/api/v1")
app.include_router(scim_router, prefix="/api/v1")
app.include_router(cdc_mapping_review_router, prefix="/api/v1")
app.include_router(workspace_router, prefix="/api/v1")
app.include_router(team_router, prefix="/api/v1")
app.include_router(invite_router, prefix="/api/v1")
app.include_router(contracts_router, prefix="/api/v1")
app.include_router(fidelity_router, prefix="/api/v1")
app.include_router(parity_router, prefix="/api/v1")
app.include_router(reversibility_router, prefix="/api/v1")
app.include_router(exit_router, prefix="/api/v1")
app.include_router(resource_acl_router, prefix="/api/v1")
app.include_router(transforms_router, prefix="/api/v1")
app.include_router(transform_library_router, prefix="/api/v1")
app.include_router(query_router, prefix="/api/v1")
app.include_router(usage_router, prefix="/api/v1")
app.include_router(ops_router, prefix="/api/v1")
app.include_router(repair_router, prefix="/api/v1")
app.include_router(shape_router, prefix="/api/v1")
app.include_router(journals_router, prefix="/api/v1")
app.include_router(status_router, prefix="/api/v1")
app.include_router(rule_compiler_router, prefix="/api/v1")


@app.get("/")
async def root():
    payload = {
        "name": "Datawrap",
        "version": "1.0.0",
        "commit": _BUILD_COMMIT,
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
    Incomplete production config does not kill the process. The missing
    settings are written to the process log, not this public body. Every
    other route stays closed until they are fixed.
    """
    errors = list(getattr(app.state, "config_errors", None) or [])
    # Cheap in-memory counters (D181): liveness stays liveness, but the
    # numbers show a pool held by stuck connector calls, and ``ready`` says
    # whether a connector call can start at all (D281: "healthy and ready"
    # while every row read hung).
    pool = connector_executor.readiness()
    return {
        "status": "misconfigured" if errors else "healthy",
        "liveness": True,
        "version": "1.0.0",
        "commit": _BUILD_COMMIT,
        "ready": bool(getattr(app.state, "ready", False)) and not errors and pool["ok"],
        "connector_pool": pool,
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
    # Off the event loop and shared between close probes (M-8): the checks
    # open a database connection and touch the data directory.
    from starlette.concurrency import run_in_threadpool

    payload = await run_in_threadpool(health_service.cached_health)
    pool = connector_executor.readiness()
    payload["connector_pool"] = pool
    if not pool["ok"] and payload.get("status") == "healthy":
        payload["status"] = "degraded"
        payload["detail"] = pool["reason"]
    warm = bool(getattr(app.state, "ready", False))
    payload["ready"] = warm and payload.get("status") == "healthy"
    if not payload["ready"] and payload.get("status") == "healthy":
        payload["status"] = "starting"
    status_code = 200 if payload["ready"] else 503
    return JSONResponse(content=payload, status_code=status_code)


@app.get("/metrics")
async def prometheus_metrics(request: Request):
    """Prometheus exposition format: HTTP, job-store and CDC ops metrics.

    The deployment-wide series (HTTP traffic, job gauges) are for platform
    administrators only (src.metrics_app.metrics_response).
    """
    from src.metrics_app import metrics_response

    return await metrics_response(request)


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


@app.exception_handler(ApiError)
async def api_error_envelope(request: Request, exc: ApiError):
    return await api_error_handler(request, exc)


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
            "existing_created_at": exc.existing_created_at,
            "job_id": exc.existing_job_id,
        },
    )


@app.exception_handler(ResidencyRefused)
async def residency_refused_handler(request: Request, exc: ResidencyRefused):
    """A destination outside the workspace's data region is a 403 on every submit path.

    Job creation raises it (``services.data_residency``); the two Studio run
    routes answer it themselves first, and this covers every other route that
    queues a job (retry, rerun, replay, upload-and-run).
    """
    logger.info("Residency refusal on %s", request.url.path)
    return JSONResponse(status_code=403, content={"error": "data_residency", "detail": str(exc)})


@app.exception_handler(ConnectorStoreError)
async def connector_store_error_handler(request: Request, exc: ConnectorStoreError):
    """A connector write the configured store refused: say so, never a generic 500."""
    logger.error("Connector store write refused on %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={"error": "connector_store_unavailable", "detail": str(exc)},
    )


@app.exception_handler(ControlPlaneUnavailable)
async def control_plane_unavailable_handler(request: Request, exc: ControlPlaneUnavailable):
    """A control-plane store (tenants, BYOK keys, ACLs, ...) cannot be read: fail closed, retryable.

    Callers never fall back to a permissive default or a node's local file
    (ops review High 1, Medium 6); the user gets a clean 503 instead of a 500.
    """
    logger.warning("Control-plane store unavailable on %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={
            "error": "control_plane_unavailable",
            "detail": "Workspace settings are temporarily unavailable. Retry shortly.",
        },
        headers={"Retry-After": "5"},
    )


@app.exception_handler(PlanStoreUnreadable)
async def plan_store_unreadable_handler(request: Request, exc: PlanStoreUnreadable):
    """The transfer-plan store file is damaged: refuse, never answer "no plans".

    The store logged the path and error class once at ERROR; the response
    names the files an operator repairs or restores (the newest backup
    generation that still parses), not their location.
    """
    if exc.readable_backup:
        remedy = f"restore the newest readable backup, {exc.readable_backup}"
    else:
        remedy = (
            "restore it from your own backups (no readable backup was found in "
            "transfer_plans.json.bak, .bak.1 or .bak.2)"
        )
    return JSONResponse(
        status_code=503,
        content={
            "error": "plan_store_unreadable",
            "detail": (
                "Transfer plans cannot be loaded: the plan store file (transfer_plans.json in "
                "the data directory) is unreadable or damaged, so nothing was read or changed. "
                f"An operator must repair it or {remedy}, then retry."
            ),
        },
    )


@app.exception_handler(PlanStoreLockUnavailable)
async def plan_store_lock_unavailable_handler(request: Request, exc: PlanStoreLockUnavailable):
    """The plan store's lock file is not writable: an operator must fix it; not retryable."""
    return JSONResponse(
        status_code=503,
        content={
            "error": "plan_store_lock_unavailable",
            "detail": (
                "Transfer plans cannot be saved: the plan store's lock file "
                "(transfer_plans.json.lock in the data directory) cannot be opened for writing, "
                "so nothing was changed. An operator must make it writable by the API's user "
                "(mode 0600; it is never written to) or fix the volume."
            ),
        },
    )


@app.exception_handler(PlanStoreBusy)
async def plan_store_busy_handler(request: Request, exc: PlanStoreBusy):
    """Another writer held the transfer-plan store past the lock timeout: retryable."""
    logger.warning("Transfer plan store busy on %s", request.url.path)
    return JSONResponse(
        status_code=503,
        content={
            "error": "plan_store_busy",
            "detail": "The plan is being saved by another request. Retry shortly.",
        },
        headers={"Retry-After": "2"},
    )


@app.exception_handler(SharedDocConflict)
async def shared_doc_conflict_handler(request: Request, exc: SharedDocConflict):
    """Concurrent writers kept winning a control-plane record after every retry: retryable, not a 500."""
    logger.warning("Control-plane write conflict on %s after retries: %s", request.url.path, exc)
    return JSONResponse(
        status_code=409,
        content={
            "error": "concurrent_update",
            "detail": "Another change to the same settings was saved at the same moment. Retry the request.",
            "retry_after_sec": 1,
        },
        headers={"Retry-After": "1"},
    )


@app.exception_handler(PayloadTooLarge)
async def payload_too_large_handler(request: Request, exc: PayloadTooLarge):
    """An upload or its gzip expansion crossed the configured byte ceiling."""
    logger.warning("Payload refused on %s: %s", request.url.path, exc)
    content: dict[str, object] = {
        "error": "payload_too_large",
        "detail": str(exc),
        "limit_bytes": exc.limit,
        "setting": f"DATAFLOW_{exc.setting}",
    }
    if exc.actual is not None:
        content["actual_bytes"] = exc.actual
    return JSONResponse(status_code=413, content=content)


@app.exception_handler(ProcedureSourceError)
async def procedure_source_error_handler(request: Request, exc: ProcedureSourceError):
    """A query/procedure source the request cannot have is a 422, not a 500.

    The callable intake (``services.callable_intake``) answers these on each route.
    This handler catches one raised anywhere else in a handler, such as an
    ``is_callable_source`` on a shape the intake never saw.
    """
    logger.info("Query/procedure source refused on %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=422,
        content={"error": "callable_source_refused", "detail": str(exc)},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    route_template = getattr(request.scope.get("route"), "path", None)
    safe_fields = {"method": request.method}
    if route_template:
        safe_fields["route_template"] = route_template
    detail, correlation_id = public_error(
        exc,
        event="http.request.unhandled",
        public_message="An unexpected error occurred.",
        status_code=500,
        **safe_fields,
    )
    logger.error(
        "http.request.unhandled.traceback",
        exc_info=exc,
        extra={
            "event": "http.request.unhandled.traceback",
            "correlation_id": correlation_id,
        },
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": detail,
            "correlation_id": correlation_id,
        },
        headers={"X-Correlation-ID": correlation_id},
    )


if __name__ == "__main__":
    import uvicorn

    # Container deployments bind to all interfaces; Railway/ALB provide the firewall.
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))  # nosec: B104
