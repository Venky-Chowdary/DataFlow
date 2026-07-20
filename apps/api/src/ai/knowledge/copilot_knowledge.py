"""
DataTransfer.space — Copilot Knowledge Base

Product knowledge, conversation templates, and intent patterns for customer-facing AI.
"""

from __future__ import annotations

DATA_PILOT_PERSONA = """You are **Data Pilot** — the intelligent agent for DataTransfer.space.

You work like Claude or a Cursor agent: thoughtful, precise, and action-oriented.
You have full visibility into the user's datasets, connectors, transfer jobs, and platform capabilities.
You answer ANY question about their data — columns, PII, quality, samples, comparisons, statistics.
When the user asks you to do something in the app, use your tools: navigate, analyze datasets, list jobs, etc.

Rules:
- Speak naturally in complete sentences — never dump raw JSON unless asked
- Ground every answer in real data from tools and context — never invent column names or row counts
- When analyzing data, cite specific columns, PII flags, and quality scores
- When the user wants to transfer, move, or configure something — use tools and give clear next steps
- Use markdown: **bold** for emphasis, `code` for column names, bullet lists for clarity
- Be concise but thorough — like Anthropic's Claude"""

COPILOT_PERSONA = DATA_PILOT_PERSONA  # backward compatible

PRODUCT_CAPABILITIES = [
    "Move data from CSV, JSON, JSONL, TSV files to MongoDB, PostgreSQL, Snowflake, and other certified transfer-ready drivers (roadmap catalog tiles are labeled Planned, not live)",
    "AI semantic mapping: understands AMT=amount, cust=customer, 210+ semantic types",
    "8 preflight validation gates before every transfer — catch errors before they happen",
    "Automatic PII detection with GDPR, HIPAA, PCI-DSS, CCPA compliance tagging",
    "Saved connectors with connection testing and health monitoring",
    "Zero-code wizard: Source → AI Analysis → Destination → Preflight → Execute",
    "Self-learning: corrections you confirm improve future mappings",
]

INTENT_PATTERNS: dict[str, list[str]] = {
    "greeting": [
        "hello", "hi", "hey", "good morning", "good afternoon", "help me", "who are you",
    ],
    "transfer_help": [
        "move", "transfer", "migrate", "sync", "copy", "load", "import", "export", "send",
        "upload", "push", "pull", "etl", "pipeline",
    ],
    "mapping_help": [
        "map", "mapping", "column", "schema", "match", "transform", "rename", "field",
        "semantic", "align",
    ],
    "pii_compliance": [
        "pii", "gdpr", "hipaa", "pci", "ssn", "personal", "sensitive", "compliance",
        "privacy", "mask", "encrypt", "redact",
    ],
    "connector_help": [
        "connector", "mongodb", "postgres", "postgresql", "snowflake", "bigquery",
        "mysql", "connection", "database", "warehouse", "connect",
    ],
    "preflight_help": [
        "preflight", "validation", "gate", "quality", "check", "validate", "error before",
    ],
    "product_help": [
        "how does", "what is", "how do i", "can i", "feature", "pricing", "security",
        "sso", "okta", "settings", "dashboard",
    ],
    "troubleshooting": [
        "error", "failed", "not working", "issue", "problem", "fix", "broken", "timeout",
    ],
}

CONVERSATION_TEMPLATES: list[dict] = [
    {
        "intent": "greeting",
        "user": "Hello, what can you help me with?",
        "assistant": (
            "Welcome to DataTransfer.space! I'm your AI Copilot. I can help you:\n\n"
            "• **Move data** — CSV, JSON, or files to MongoDB, Snowflake, and other certified destinations\n"
            "• **Map columns** — AI understands abbreviations like AMT, cust_id, qty\n"
            "• **Check PII & compliance** — GDPR, HIPAA, PCI-DSS before you transfer\n"
            "• **Run preflight checks** — 8 validation gates catch issues early\n\n"
            "Try: \"Move my logistics CSV to MongoDB\" or \"What PII is in my HR data?\""
        ),
    },
    {
        "intent": "transfer_help",
        "user": "How do I move my CSV file to MongoDB?",
        "assistant": (
            "Here's how to transfer your CSV to MongoDB:\n\n"
            "1. Go to **Transfer** in the sidebar\n"
            "2. **Step 1 — Source**: Upload your CSV file\n"
            "3. **Step 2 — AI Analysis**: Review detected column types and PII flags\n"
            "4. **Step 3 — Destination**: Pick a saved MongoDB connector (or add one in Connectors)\n"
            "5. **Step 4 — Preflight**: Run 8 validation gates\n"
            "6. **Step 5 — Execute**: Start the transfer\n\n"
            "Need a connector first? Go to **Connectors → Add Connector → MongoDB**, test, and save."
        ),
        "actions": [{"label": "Start Transfer", "route": "transfer"}],
    },
    {
        "intent": "mapping_help",
        "user": "How does AI column mapping work?",
        "assistant": (
            "Our AI mapping engine uses three layers:\n\n"
            "**Layer 1 — Pattern matching** (instant): Recognizes 210+ semantic types and 777 synonyms. "
            "AMT → amount, cust_name → customer_name, email_addr → email.\n\n"
            "**Layer 2 — RAG retrieval**: Searches your schemas and industry templates for similar columns.\n\n"
            "**Layer 3 — LLM reasoning**: Chain-of-thought analysis for ambiguous mappings.\n\n"
            "Each mapping shows a confidence score. Review low-confidence mappings in the Transfer wizard "
            "before executing."
        ),
    },
    {
        "intent": "pii_compliance",
        "user": "Does my data contain PII?",
        "assistant": (
            "DataTransfer automatically detects PII during AI Analysis. We scan for:\n\n"
            "• **Personal IDs** — SSN, passport, driver's license\n"
            "• **Contact info** — email, phone, address\n"
            "• **Financial** — credit cards, bank accounts\n"
            "• **Health** — MRN, diagnosis codes (HIPAA)\n\n"
            "Upload your file in the Transfer wizard — Step 2 shows PII flags and compliance tags "
            "(GDPR, HIPAA, PCI-DSS). You can mask or exclude sensitive columns before transfer."
        ),
        "actions": [{"label": "Analyze Data", "route": "transfer"}],
    },
    {
        "intent": "connector_help",
        "user": "How do I connect to MongoDB?",
        "assistant": (
            "To set up a MongoDB connector:\n\n"
            "1. Go to **Connectors** → **Add Connector**\n"
            "2. Select **MongoDB**\n"
            "3. Enter host (e.g. `localhost`), port (`27017`), and database name\n"
            "4. Click **Test Connection** — you'll see a green check if it works\n"
            "5. **Save** — the connector appears in your list and in the Transfer wizard\n\n"
            "Saved connectors persist in MongoDB so you don't re-enter credentials."
        ),
        "actions": [{"label": "Add Connector", "route": "connectors"}],
    },
    {
        "intent": "preflight_help",
        "user": "What are preflight gates?",
        "assistant": (
            "Preflight gates validate your transfer **before** it runs — saving time and preventing data loss:\n\n"
            "1. **Schema compatibility** — source columns map to destination\n"
            "2. **Type safety** — data types are compatible\n"
            "3. **Null handling** — required fields won't be empty\n"
            "4. **PII policy** — sensitive data flagged\n"
            "5. **Row count sanity** — reasonable volume check\n"
            "6. **Mapping confidence** — low-confidence mappings flagged\n"
            "7. **Destination reachability** — connector is online\n"
            "8. **Size estimation** — transfer fits within limits\n\n"
            "Run preflight in Transfer Step 4. Failed gates show exactly what to fix."
        ),
    },
    {
        "intent": "product_help",
        "user": "What makes DataTransfer different from Airbyte or Fivetran?",
        "assistant": (
            "DataTransfer.space is built for **semantic intelligence**, not just connector count:\n\n"
            "• **AI-first mapping** — understands what data *means*, not just column names\n"
            "• **Universal sources** — files + databases + APIs in one platform\n"
            "• **Preflight gates** — validate before transfer, not after failure\n"
            "• **Built-in PII/compliance** — GDPR, HIPAA, PCI-DSS from day one\n"
            "• **Zero-code wizard** — no DevOps or consultants required\n"
            "• **Self-learning** — corrections improve future mappings via RAG\n\n"
            "Our motto: *Universal Data Freedom — Move Any Data, Anywhere, with AI Intelligence*."
        ),
    },
    {
        "intent": "troubleshooting",
        "user": "My transfer failed, what should I do?",
        "assistant": (
            "Let's troubleshoot:\n\n"
            "1. **Check Jobs** — see the error message and failed step\n"
            "2. **Test your connector** — Connectors → Test Connection\n"
            "3. **Re-run preflight** — Transfer Step 4 shows which gate failed\n"
            "4. **Review mappings** — low-confidence mappings often cause type errors\n"
            "5. **Check file format** — we support CSV, JSON, JSONL, TSV\n\n"
            "Share the error message and I'll help you fix it."
        ),
        "actions": [{"label": "View Jobs", "route": "jobs"}],
    },
]

SUGGESTED_PROMPTS = [
    "Analyze the HR data",
    "What PII is in my logistics file?",
    "Show column types in payments data",
    "How many rows in retail dataset?",
    "Map employee columns to MongoDB",
    "Check data quality on my uploads",
]

QUICK_REPLIES: dict[str, str] = {
    "transfer": (
        "To start a transfer, go to **Transfer** → upload your file → follow the 5-step wizard. "
        "I can walk you through each step if you'd like."
    ),
    "mongodb": (
        "MongoDB is our primary live connector. Add it under **Connectors**, test the connection, "
        "then select it as your destination in the Transfer wizard."
    ),
    "csv": (
        "CSV files are fully supported. Upload in Transfer Step 1 — we auto-detect columns and "
        "run AI semantic analysis on Step 2."
    ),
    "json": (
        "JSON and JSONL files work great. Nested objects are flattened automatically during transfer."
    ),
    "snowflake": (
        "Snowflake transfers are available via our enterprise API. For file-based workflows, "
        "export to CSV/JSON first, or contact us for direct Snowflake connector access."
    ),
}


def get_copilot_documents() -> list[dict]:
    """Build vector-store documents from copilot knowledge."""
    docs = []

    docs.append({
        "id": "copilot_persona",
        "text": COPILOT_PERSONA + " Capabilities: " + "; ".join(PRODUCT_CAPABILITIES),
        "metadata": {"type": "copilot_knowledge", "category": "persona"},
    })

    for i, cap in enumerate(PRODUCT_CAPABILITIES):
        docs.append({
            "id": f"copilot_cap_{i}",
            "text": f"DataTransfer capability: {cap}",
            "metadata": {"type": "copilot_knowledge", "category": "capability"},
        })

    for i, tmpl in enumerate(CONVERSATION_TEMPLATES):
        docs.append({
            "id": f"copilot_tmpl_{i}",
            "text": f"User question: {tmpl['user']}\nAssistant answer: {tmpl['assistant']}",
            "metadata": {"type": "copilot_training", "intent": tmpl["intent"]},
        })

    for keyword, reply in QUICK_REPLIES.items():
        docs.append({
            "id": f"copilot_qr_{keyword}",
            "text": f"When user mentions {keyword}: {reply}",
            "metadata": {"type": "copilot_knowledge", "category": "quick_reply", "keyword": keyword},
        })

    return docs
