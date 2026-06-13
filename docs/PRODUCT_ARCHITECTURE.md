# DataTransfer.space — Product Architecture & Intelligence Model

## What This Product Is

**DataTransfer.space** is a universal data movement platform: move, transform, validate, and synchronize data from **any source** to **any destination**, with AI that understands what the data *means* — not just what columns are named.

**Motto:** *Universal Data Freedom — Move Any Data, Anywhere, with AI Intelligence*

---

## Why This Product Is Unique (And Why No One Built It This Way)

| Gap in Market | Airbyte / Fivetran | Informatica | **DataTransfer.space** |
|---------------|-------------------|-------------|------------------------|
| AI semantic mapping | Manual | Manual | **210 patterns + RAG + LLM** |
| Universal file + DB + API | DB-only or file-only | Partial | **Single platform** |
| Zero-code + NL interface | DevOps required | Consultants required | **Natural language + wizard** |
| Pre-flight quality gates | After failure | Batch ETL | **8 gates before transfer** |
| PII auto-detection | Add-on / manual | Partial | **Built-in, 40+ compliance tags** |
| Self-healing schema drift | Breaks pipelines | Manual remap | **RAG learns corrections** |
| Enterprise security at SMB price | $100K+ | $100K+ | **SOC2/GDPR from day one** |

**Why no one fully built this:** Incumbents optimized for *connector count* and *managed pipelines*, not *semantic intelligence*. AI-first data movement requires a new stack: RAG over universal schemas, synonym dictionaries, industry templates, and LLM fallback — not bolted-on column matchers.

---

## Required Enterprise Screens

| Screen | Purpose | Status |
|--------|---------|--------|
| **Dashboard** | Active transfers, success rate, records processed, AI accuracy | Built |
| **New Transfer** | Source → AI Analysis → Mapping → Destination → Execute | In progress |
| **Connectors** | 600+ catalog + saved connections + test/save | Built (MongoDB live) |
| **Connections** | Credential vault, health scores, last used | Partial |
| **Pipelines** | Scheduled/recurring syncs | Planned |
| **Monitoring** | Live throughput, errors, SLA | Planned |
| **Governance** | PII registry, lineage, policy engine | Planned |
| **Settings** | SSO/Okta, team, security, API keys, audit | UI mock |
| **AI Copilot** | "Move Shopify orders to BigQuery" | **Built (floating panel)** |

---

## How AI / ML / LLM Works

### Three-Layer Intelligence Stack

```
Layer 1: Pattern Engine (always on, <10ms)
  └─ 210 semantic types, 777 synonyms, regex + token matching
  └─ Handles: AMT→amount, cust_name→customer_name, email detection

Layer 2: RAG Pipeline (vector retrieval)
  └─ Embeddings: sentence-transformers (fallback: TF-IDF)
  └─ Store: ChromaDB (320+ ingested knowledge documents)
  └─ Retrieves: patterns, industry schemas, past user corrections

Layer 3: LLM Chain-of-Thought (when API keys available)
  └─ Providers: Anthropic → OpenAI → Ollama → Local fallback
  └─ 6-step reasoning: identify → classify → map → validate → transform → recommend
```

### Semantic Column Analysis Flow

1. **Name analysis** — tokenize `cust_email_addr` → [cust, email, addr]
2. **Synonym lookup** — `cust` ∈ customer group, `amt` ∈ amount group
3. **Sample validation** — regex match emails, phones, SSN patterns on data
4. **RAG retrieval** — find similar columns from knowledge base
5. **Confidence score** — combine name (40%) + sample (40%) + RAG (20%)
6. **PII + compliance** — tag GDPR/HIPAA/PCI-DSS if applicable

### Column Mapping Flow

```
Source: [cust_id, cust_name, AMT, email_addr]
Target: [customer_id, full_name, amount, email_address]

Mapping strategies (highest confidence wins):
  1. Exact match (98%)
  2. Normalized match (95%)
  3. Synonym match (88%) — cust ↔ customer
  4. Semantic type match (85%) — both detected as Email Address
  5. Token overlap (60-75%)
  6. RAG-enhanced (boost +10%)
  7. LLM reasoning (final tie-break)
```

### How LLMs Are "Trained" on Universal Data

We do **not** fine-tune a foundation model from scratch. Instead:

1. **Knowledge base** — 210 hand-curated semantic patterns across 10 industries
2. **Synonym dictionary** — 777 entries + 80 abbreviation tokens (AMT, QTY, SSN, etc.)
3. **Synthetic training data** — 2,160+ generated column/mapping examples (`training/data_synthesis.py`)
4. **Industry schemas** — logistics, finance, healthcare, retail templates
5. **RAG ingestion** — all knowledge embedded into vector store at startup
6. **User corrections** — `learn_correction()` adds confirmed mappings to vector store
7. **Evaluation harness** — classification, mapping, PII recall metrics on every release

Optional: embedding fine-tuning pipeline prepares JSONL pairs from synonyms (prep-only today).

---

## Data Types Handled

### File Formats (Source)
| Format | Status | Parser |
|--------|--------|--------|
| JSON | Live | `file_parser.py` |
| CSV / TSV | Live | `file_parser.py` |
| JSONL / NDJSON | Live | `file_parser.py` |
| Excel | Planned | openpyxl |
| Parquet | Planned | pyarrow |
| PDF / Word | Planned | extraction pipeline |

### Semantic Types (210+)
Contact (email, phone), Personal (name, SSN, DOB), Financial (amount, currency, credit card), Geographic (address, zip), Temporal (date, timestamp), Identifiers (PK, FK, SKU), Health (MRN, ICD), Status/Enums, Numeric, Text/URL

### Database Type Mapping
Universal conversion matrix maps semantic types → PostgreSQL, MySQL, MongoDB, Snowflake, BigQuery native types.

### Connectors
620 catalog entries; **live today:** MongoDB (read/write), PostgreSQL + Snowflake (legacy API). Roadmap: all major warehouses, SaaS, cloud storage.

---

## Enterprise Security Model

| Control | Implementation |
|---------|----------------|
| Encryption at rest | AES-256 (planned: customer-managed keys) |
| Encryption in transit | TLS 1.3 |
| Credential storage | MongoDB today → HashiCorp Vault (planned) |
| SSO / SAML | Okta, Entra ID, Google Workspace (Settings UI) |
| RBAC | Admin / Editor / Viewer roles (Settings UI) |
| Audit logs | All transfer + config events (planned persistence) |
| Compliance | SOC2, GDPR, HIPAA, PCI-DSS tagging on PII columns |
| Network | IP allowlist, Private Link (Settings UI) |

---

## Architecture (Current vs Target)

```
TODAY                          TARGET
─────                          ──────
Web (5177)                     Web (5177)
  └─ file → MongoDB              └─ any source → any dest
API (8001)                       └─ AI mapping wizard
  ├─ connectors (MongoDB)      API (unified)
  └─ ai (RAG/LLM)                ├─ connectors (all drivers)
Legacy API (8000)                ├─ ai (RAG/LLM)
  ├─ PG/Snowflake transfers      ├─ preflight (8 gates)
  └─ preflight gates             └─ orchestration (jobs/SSE)
MongoDB (local)                PostgreSQL + Vault + ChromaDB
```

---

## Roadmap to Top Product

1. **Wire AI into Transfer UI** — semantic analysis + mapping review step
2. **Unify APIs** — merge legacy PG/Snowflake into main API
3. **Use saved connectors in transfers** — not hardcoded localhost
4. **Preflight gates in UI** — block bad transfers before execution
5. **AI Copilot panel** — natural language transfer creation
6. **Real SSO + secrets vault** — enterprise auth
7. **Streaming job progress** — SSE with live record counts
8. **Schema drift detection** — auto-remap on source change
