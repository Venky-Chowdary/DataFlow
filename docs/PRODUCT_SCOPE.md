# DataFlow — Universal One-Click Data Platform

## Product scope

DataFlow is **not** a payment-file-only tool. It is a **universal data operations platform**:

| Operation | Example |
|-----------|---------|
| **Upload** | Any file (CSV, Excel, JSON, PDF, Word, …) → any database |
| **Migration** | PostgreSQL → Snowflake, SQL Server → MongoDB |
| **Dump** | Database → CSV, Excel, JSON, SQL file |
| **Convert** | CSV → Word, Excel → PDF, any format → any format |
| **Transfer** | API → database, database → database, file → file |

### One-click flow (always the same)

1. **Source** — upload any file, connect any database (connection string or host/port/user/pass), or API URL  
2. **Destination** — any database/warehouse or file export format  
3. **Transfer** — preflight gates → AI semantic mapping → transfer → reconciliation  

### Guarantees

- **Fail-fast**: 8 preflight gates block bad jobs before any data moves  
- **Semantic mapping**: AI maps columns by meaning (any naming convention)  
- **Checkpoint/resume**: no silent failure mid-transfer (engine phase)  
- **Any DB**: PostgreSQL, SQL Server, MySQL, Oracle, MongoDB, Snowflake, BigQuery, Redis, Databricks  

### Implementation phases

## Phase 1: UI + preflight + universal config | **Done**
| Phase 1b: File→PostgreSQL write + reconciliation | **Done** (checkpoint batches)
| Real DB drivers + file parsers | In progress
| LLM mapping engine | Next |
| Checkpoint/Temporal orchestration | Next |
| Format conversion engine | Next |

Payment/CSA files are **one supported file type**, not the product focus.
