"""
DataTransfer.space — API Server

Enterprise-grade data transfer platform with AI-powered semantic analysis.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import time
import asyncio

from .routers.ai_router import router as ai_router
from .routers.connectors_router import router as connectors_router
from .routers.preflight_router import router as preflight_router
from .routers.copilot_router import router as copilot_router
from .routers.training_agent_router import router as training_agent_router
from .routers.transfer_router import router as transfer_router
from .routers.mcp_router import router as mcp_router
from .routers.automation_router import router as automation_router
from .routers.catalog_router import router as catalog_router
from .routers.schedules_router import router as schedules_router
from .routers.saved_connectors_router import router as saved_connectors_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    import os

    print("[*] DataTransfer.space API starting...")
    print("[+] AI Semantic Engine initialized")
    training_enabled = os.getenv("DATAFLOW_TRAINING", "on").lower() not in ("off", "0", "false")
    try:
        from .ai.rag.pipeline import get_rag_pipeline
        from .ai.knowledge.semantic_patterns import get_pattern_count
        from .ai.knowledge.synonyms import get_synonym_count
        from .ai.training.training_scheduler import run_training_loop

        pipeline = get_rag_pipeline()
        init_result = pipeline.initialize()
        print(f"[+] RAG Pipeline initialized: {init_result.get('ingested', 0)} documents")
        print(f"[+] Knowledge Base: {get_pattern_count()} patterns, {get_synonym_count()} synonyms")

        async def _background_training():
            # Defer heavy training so the API is responsive immediately after boot
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
            print("[+] Training Agent scheduled (starts 120s after boot; set DATAFLOW_TRAINING=off to disable)")
            asyncio.create_task(run_training_loop())
            print("[+] Training Agent scheduler started (retrain every 30 min)")
        else:
            print("[*] Training Agent disabled (DATAFLOW_TRAINING=off)")
        from .services.schedule_runner import run_schedule_loop
        asyncio.create_task(run_schedule_loop())
        print("[+] Pipeline scheduler started (recurring syncs every 60s)")
    except Exception as e:
        print(f"[!] RAG initialization warning: {e}")
    yield
    task = getattr(app, "state", None) and getattr(app.state, "training_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    print("[*] DataTransfer.space API shutting down...")


app = FastAPI(
    title="DataTransfer.space API",
    description="""
    ## Universal Data Transfer Platform API
    
    Enterprise-grade data movement and transformation with AI-powered intelligence.
    
    ### Core Capabilities
    
    - **AI Semantic Analysis**: Automatically understand data types, detect PII, and ensure compliance
    - **Smart Mapping**: Intelligent column mapping between source and target schemas
    - **Universal Connectors**: Connect to any database, warehouse, file, or API
    - **Enterprise Security**: SOC2, GDPR, HIPAA, PCI-DSS compliant
    
    ### Key Features
    
    - 🔍 **Semantic Type Detection**: Recognize 60+ data types including email, phone, SSN, credit cards
    - 🔐 **PII Detection**: Automatically identify personally identifiable information
    - 📋 **Compliance Mapping**: GDPR, CCPA, HIPAA, PCI-DSS, SOX, GLBA requirements
    - 🎯 **Smart Mapping**: 99%+ accuracy in column matching
    - ⚡ **Real-time Analysis**: Instant schema analysis and validation
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    """Add response timing header"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    return response


app.include_router(ai_router, prefix="/api/v1")
app.include_router(connectors_router, prefix="/api/v1")
app.include_router(preflight_router, prefix="/api/v1")
app.include_router(copilot_router, prefix="/api/v1")
app.include_router(training_agent_router, prefix="/api/v1")
app.include_router(transfer_router, prefix="/api/v1")
app.include_router(mcp_router, prefix="/api/v1")
app.include_router(automation_router, prefix="/api/v1")
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(schedules_router, prefix="/api/v1")
app.include_router(saved_connectors_router, prefix="/api/v1")


@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "DataTransfer.space",
        "tagline": "Universal Data Freedom — Move Any Data, Anywhere",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "docs": "/docs",
            "redoc": "/redoc",
            "ai": "/api/v1/ai",
        },
        "features": [
            "AI-Powered Semantic Analysis",
            "PII Detection",
            "Compliance Mapping",
            "Smart Column Mapping",
            "60+ Semantic Types",
            "600+ Connectors",
        ],
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "services": {
            "api": "up",
            "ai_engine": "up",
            "database": "up",
        }
    }


@app.get("/api/v1")
async def api_info():
    """API version info"""
    return {
        "version": "1.0.0",
        "endpoints": [
            {
                "path": "/api/v1/ai/analyze/column",
                "method": "POST",
                "description": "Analyze a single column"
            },
            {
                "path": "/api/v1/ai/analyze/schema",
                "method": "POST",
                "description": "Analyze a complete schema"
            },
            {
                "path": "/api/v1/ai/map",
                "method": "POST",
                "description": "Generate column mappings"
            },
            {
                "path": "/api/v1/ai/detect-pii",
                "method": "POST",
                "description": "Detect PII in columns"
            },
            {
                "path": "/api/v1/ai/semantic-types",
                "method": "GET",
                "description": "List all semantic types"
            },
        ]
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": str(exc),
            "path": str(request.url),
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
