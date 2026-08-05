# Datawrap — Product Architecture & Intelligence Model

## What This Product Is

**Datawrap** is a universal data movement platform: move, transform, validate, and synchronize data from **any source** to **any destination**, with AI that understands what the data *means* — not just what columns are named.

**Motto:** *Universal Data Freedom — Move Any Data, Anywhere, with AI Intelligence*

---

## Why This Product Is Unique (And Why No One Built It This Way)

| Gap in Market | Airbyte / Fivetran | Informatica | **Datawrap** |
|---------------|-------------------|-------------|------------------------|
| AI semantic mapping | Manual | Manual | **BM25 + Hungarian + synonym/role graph (optional AI path)** |
| Universal file + DB + API | DB-only or file-only | Partial | **Single platform** |
| Zero-code + NL interface | DevOps required | Consultants required | **Natural language + wizard** |
| Pre-flight quality gates | After failure | Batch ETL | **9 core gates (G1–G9) before transfer** |
| PII auto-detection | Add-on / manual | Partial | **Built-in compliance tagging on columns** |
| Schema drift | Breaks pipelines | Manual remap | **Detect + operator approve (Studio policy)** |
| Enterprise security posture | $100K+ | $100K+ | **Security questionnaire pack / posture report — not auditor-certified SOC 2/GDPR** |

**Why no one fully built this:** Incumbents optimized for *connector count* and *managed pipelines*, not *semantic intelligence + fail-closed preflight*. Studio column mapping is BM25 + Hungarian assignment over synonym/role graphs; optional LLM/RAG assists operators or powers vector destinations — it is not the default Studio mapper.

---

## Required Enterprise Screens

| Screen | Purpose | Status |
|--------|---------|--------|
| **Dashboard** | Active transfers, success rate, records processed, AI accuracy | Built |
| **New Transfer** | Source → AI Analysis → Mapping → Destination → Execute | In progress |
| **Connectors** | Catalog (certified + roadmap) + saved connections + test/save | Built (certified drivers live; roadmap Planned) |
| **Connections** | Credential vault, health scores, last used | Partial |
| **Schedules** | Scheduled/recurring syncs (workspace nav) | **Built** |
| **Monitoring** | Live throughput, errors, SLA | Partial (Jobs / Overview) |
| **Governance** | PII registry, lineage, policy engine | Planned |
| **Settings** | SSO/Okta, team, security posture, API keys, audit | **Built** (audit persistence still deepening) |
| **AI Copilot / Pilot** | "Move Shopify orders to BigQuery" | **Built** |

---

## How AI / ML / LLM Works

### Studio column mapper (default path)

```
Always-on (Transfer Studio Map step):
  └─ Tokenize + synonym / abbreviation dictionaries
  └─ Semantic role + type compatibility scoring
  └─ BM25 retrieval over candidate fields
  └─ Hungarian assignment for globally consistent column pairs
  └─ Operator pin / remap when confidence is below threshold (G4)
```

### Optional AI / RAG paths (not the default Studio mapper)

```
Optional LLM assist (when workspace API keys are configured):
  └─ Providers: Anthropic → OpenAI → Ollama → local fallback
  └─ Used for Pilot NL plans and operator suggestions — still subject to Map review + G1–G9

Optional vector / RAG:
  └─ Embeddings (e.g. sentence-transformers or hosted embedding APIs) for vector destinations
      and optional retrieval assist — not required for BM25+Hungarian Studio mapping
```

### Semantic Column Analysis Flow

1. **Name analysis** — tokenize `cust_email_addr` → [cust, email, addr]
2. **Synonym lookup** — `cust` ∈ customer group, `amt` ∈ amount group
3. **Sample validation** — regex match emails, phones, SSN patterns on data
4. **BM25 + role graph** — rank destination candidates; Hungarian picks a conflict-free map
5. **Confidence score** — combine name, sample evidence, and type/role compatibility (G4 threshold)
6. **PII + compliance tags** — surface GDPR/HIPAA/PCI-DSS-style tags when patterns match (operator-visible; not a certification claim)

### Column Mapping Flow

```
Source: [cust_id, cust_name, AMT, email_addr]
Target: [customer_id, full_name, amount, email_address]

Default Studio strategies (highest confidence wins; then Hungarian):
  1. Exact match
  2. Normalized match
  3. Synonym match — cust ↔ customer
  4. Semantic type / role match — both detected as Email Address
  5. BM25 / token overlap
  6. Optional LLM suggestion (tie-break assist only; operator still reviews)
```

### How intelligence improves without claiming fine-tuned SOC certifications

We do **not** fine-tune a foundation model from scratch as the product path. Instead:

1. **Pattern + synonym dictionaries** — curated semantic types and abbreviations (AMT, QTY, SSN, etc.)
2. **BM25 + Hungarian mapper** — deterministic Studio column assignment with confidence floors
3. **Industry schema templates** — logistics, finance, healthcare, retail starting points
4. **Operator corrections** — pinned maps become the contract for the route
5. **Evaluation harness** — classification / mapping golden sets on release (report measured floors, never invent %)
6. **Optional embeddings** — for vector destinations or AI assist paths when configured

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
| Audit logs | Transfer + config events (persistence deepening) |
| Compliance posture | Security questionnaire pack / posture report download; PII column tagging (GDPR/HIPAA/PCI-DSS *tags*). **Not** auditor-certified SOC 2 / GDPR attestation from day one. |
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
  └─ ai (optional LLM/RAG)       ├─ connectors (all drivers)
Legacy API (8000)                ├─ mapping (BM25+Hungarian; optional AI)
  ├─ PG/Snowflake transfers      ├─ preflight (G1–G9)
  └─ preflight gates             └─ orchestration (jobs/SSE)
MongoDB (local)                PostgreSQL + Vault (+ optional vector store)
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
