"""Automation ideas — category cards for Data Pilot (beats static competitor templates)."""

from fastapi import APIRouter

router = APIRouter(prefix="/automations", tags=["Automations"])

AUTOMATION_IDEAS = [
    {
        "id": "logistics_mongo",
        "category": "logistics",
        "title": "Logistics CSV → MongoDB",
        "prompt": "Move my logistics CSV to MongoDB with auto-created collection and typed columns",
        "description": "Upload freight/shipment data, detect CUST_ID and AMT semantics, create MongoDB collection automatically.",
    },
    {
        "id": "pii_scan_hr",
        "category": "compliance",
        "title": "PII scan on HR data",
        "prompt": "What PII is in my HR data and what compliance frameworks apply?",
        "description": "Scan employee columns for GDPR/HIPAA tags before any transfer.",
    },
    {
        "id": "payments_snowflake",
        "category": "finance",
        "title": "Payments → Snowflake warehouse",
        "prompt": "Transfer payments data to Snowflake with auto DDL and typed columns",
        "description": "Load transaction CSV/JSON into Snowflake with CREATE TABLE IF NOT EXISTS.",
    },
    {
        "id": "retail_compare",
        "category": "retail",
        "title": "Compare retail vs logistics schemas",
        "prompt": "Compare retail and logistics datasets — shared columns and differences",
        "description": "Side-by-side schema diff before merging datasets.",
    },
    {
        "id": "pg_migration",
        "category": "data_ops",
        "title": "PostgreSQL → MongoDB migration",
        "prompt": "Migrate data from PostgreSQL table to MongoDB collection",
        "description": "Universal DB-to-DB migration with zero manual schema mapping.",
    },
    {
        "id": "job_audit",
        "category": "data_ops",
        "title": "Transfer job audit",
        "prompt": "Show my recent transfer jobs and flag any failures",
        "description": "Review job history, record counts, and status in plain language.",
    },
    {
        "id": "quality_report",
        "category": "analytics",
        "title": "Data quality report",
        "prompt": "Analyze all my datasets and give me quality scores and PII summary",
        "description": "Cross-dataset quality dashboard from Data Pilot.",
    },
    {
        "id": "file_export",
        "category": "data_ops",
        "title": "JSON to CSV export",
        "prompt": "Convert my JSON file to CSV export",
        "description": "File-to-file conversion via universal transfer engine.",
    },
    {
        "id": "connector_setup",
        "category": "data_ops",
        "title": "Set up Snowflake connector",
        "prompt": "Take me to connectors — I need to add Snowflake",
        "description": "Navigate and configure warehouse destination.",
    },
    {
        "id": "semantic_map",
        "category": "analytics",
        "title": "Semantic column mapping",
        "prompt": "Map logistics columns to a PostgreSQL warehouse schema with AI",
        "description": "210 semantic types + RAG — AMT→amount, cust_id→customer_id.",
    },
    {
        "id": "preflight_gates",
        "category": "compliance",
        "title": "Run preflight before transfer",
        "prompt": "Explain preflight gates and take me to start a new transfer",
        "description": "8 validation gates — zero rows moved until all pass.",
    },
    {
        "id": "search_amount",
        "category": "finance",
        "title": "Find amount columns",
        "prompt": "Search all my data for amount and payment columns",
        "description": "Cross-dataset search for financial fields.",
    },
]

CATEGORIES = [
    {"id": "all", "label": "All"},
    {"id": "logistics", "label": "Logistics"},
    {"id": "finance", "label": "Finance"},
    {"id": "retail", "label": "Retail"},
    {"id": "compliance", "label": "Compliance"},
    {"id": "analytics", "label": "Analytics"},
    {"id": "data_ops", "label": "Data Ops"},
]


@router.get("/ideas")
async def automation_ideas(category: str = "all"):
    ideas = AUTOMATION_IDEAS
    if category and category != "all":
        ideas = [i for i in ideas if i["category"] == category]
    return {"categories": CATEGORIES, "ideas": ideas}


@router.get("/categories")
async def automation_categories():
    return {"categories": CATEGORIES}
