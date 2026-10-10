# DataWrap — Competitive Analysis (Migration Assurance)

**Status:** Fact-checked rewrite (Phase E4) — 2026-08-08  
**Positioning:** Heterogeneous **database migration + assurance**, not ELT SaaS fleet sync.  
**Peer group:** AWS DMS + SCT, Datafold, Informatica, Debezium-class CDC, Qlik Replicate, Oracle GoldenGate, Estuary — *not* Airbyte/Fivetran as primary competitors.

> Do not cite this document for “650+ live connectors” or “beats Fivetran on SaaS breadth.” Those claims are false for this product. Certified inventory is `unique_drivers` + `PRODUCTION_SKU` (see `transfer_ready_matrix.json`).

---

## What we sell

Move schemas and data across heterogeneous engines **with fail-closed type fidelity, operator-visible Decision Artifacts, quarantine, and full-population checksum reconciliation**. CDC is documented **at-least-once** until proven otherwise.

---

## Head-to-head (honest)

| Dimension | DataWrap (today) | AWS DMS + SCT | Datafold | Informatica | Debezium / Estuary / Qlik / GoldenGate |
|-----------|------------------|---------------|----------|-------------|----------------------------------------|
| Job to be done | Migration + proof | Migration + CDC + shallow validate | Diff / QA only | Enterprise ETL/ELT | Streaming CDC / replicate |
| Type fidelity intent | Canonical logical + width / Decision Kernel | Vendor maps; SCT assists | N/A (compares) | Strong but heavy | Schema registry / Avro etc. |
| Full-table checksum reconcile | **Yes** (order-independent, spill-to-disk) | Row-level validate (limited) | Data-diff specialty | Partial / add-ons | Not the product |
| Semantic map into existing schema | BM25 + Hungarian + calibrated confidence | SCT + manual | N/A | Strong | Weak / none |
| Fail-closed invent / TZ / missing | Explicit (kernel + writers) | Mixed | N/A | Configurable | Connector-dependent |
| SaaS API fleet | **2 certified** (SF, HubSpot); rest Planned | Weak | N/A | Broad | Varies |
| Horizontal scale | Process-local scheduler (Phase F) | Managed | SaaS | Enterprise grid | Kafka / managed fleets |
| Compliance certs | Docs / posture — **not** SOC2 claim | AWS shared responsibility | SOC2 (vendor) | Enterprise suite | Varies |

---

## Competitor notes (no false weakness lists)

### AWS DMS + SCT
- **Strengths:** Managed fleet, broad engine pairs, built-in validation option, SCT for schema conversion.
- **Gaps vs us:** Validation is not a full order-independent population checksum with Decision Artifact gating; SCT UX and heterogeneous type edge cases remain painful. Our wedge is **assurance depth + semantic map into existing targets**.

### Datafold
- **Strengths:** Best-in-class cross-DB data diff for analytics QA.
- **Gaps vs us:** Does not move data or own preflight DDL invent. We **move + prove**; they **prove diffs**. Partner narrative, not “replace Datafold.”

### Informatica / Talend
- **Strengths:** Enterprise governance, decades of connectors, professional services.
- **Gaps vs us:** Cost, time-to-value, consultant dependency. We win only on **focused migration programmes** with proof packs, not on global IT modernization RFPs.

### Debezium / Estuary / Qlik Replicate / GoldenGate
- **Strengths:** Streaming CDC transport maturity (replication connections, lag curves).
- **Gaps vs us:** Not migration-assurance UI + type invent + Map/Validate Decision Artifact. Our CDC is **correct at-least-once semantics** today; PostgreSQL transport is `START_REPLICATION` streaming by default with peek fallback (F4, live-proven on PG; see `KEYSET_AND_BULK_IO.md`). Still do not claim Debezium parity: AG/DG dual-node failover and Oracle/SQL Server live matrices remain open.

### Airbyte / Fivetran (secondary)
- **Hired for:** Land many SaaS APIs into a warehouse with minimal engineering.
- **Reality:** We lose that bake-off on connector count, incremental SaaS, managed ops. Mention only to **redirect** buyers: if they need 200 SaaS sources, hire them; if they need Oracle→Postgres with proof, hire us.

**Fact corrections vs prior in-repo draft:** Fivetran has dbt orchestration and file/object connectors; Airbyte has substantial file/format support and a much larger catalog than “300.” Prior checkmarks claiming otherwise are **withdrawn**.

---

## Claims we will not make

- “650+ / 740 live connectors”
- “Exactly-once CDC” without proof artifact
- “Beats Fivetran/Airbyte on SaaS”
- SOC 2 / HIPAA / ISO certification without auditor evidence
- Sample-only G8/G9 as population proof (full SHA-256 reconcile is the proof)

---

## Evidence pointers

| Artifact | Path |
|----------|------|
| Certified routes | `apps/api/src/transfer/registry.py` → `PRODUCTION_SKU` |
| Matrix report | `apps/api/data/proofs/transfer_ready_matrix.json` |
| Catalog honesty | `enrich_catalog_entry` / `catalog_summary.unique_drivers` |
| Buyer pack | `docs/BUYER_EVIDENCE_PACK.md` |
| Scope | `docs/PRODUCT_SCOPE.md` |

## G-VEC: Vector / AI-ready

**Parity score:** Datawrap **5/10** vs Airbyte/Fivetran **6/10**.

| Capability | Status | Evidence test file | Verified-live engine |
| --- | --- | --- | --- |
| pgvector source-id btree / optional HNSW indexes and vector / halfvec storage | M7 live coverage; default remains `vector` with no HNSW index | `apps/api/tests/test_pgvector_m7_live.py` | pgvector extension 0.8.7 at `:5434` |
| Stable vector writes, unchanged-document skip, fingerprints, usage, verified delete and stale cleanup | Live writer contracts cover only the named engines; Milvus was not available | `apps/api/tests/test_vector_m6_live.py`, `apps/api/tests/test_vector_document_skip.py`, `apps/api/tests/test_vector_fingerprint_live.py` | Weaviate 1.26.6; Pinecone Local `v1.0.0.rc0`; pgvector and Qdrant have separate live suites |
| CDC document-key delete, cleanup and redelivery | End-to-end proof is at-least-once and currently limited to pgvector and Qdrant | `apps/api/tests/test_cdc_postgres_vector_live.py` | pgvector `:5434`; Qdrant `:6335` |
| Embedding provider routing and usage accounting | Paid-provider tests use fake responses; deterministic hash embeddings are used for live writer tests | `apps/api/tests/test_embedding_providers.py` | pgvector, Qdrant, Weaviate, Pinecone Local (hash model only) |
| Chunking strategies and safe record templates | Unit coverage uses injected tokenizers; real tiktoken has not been tested | `apps/api/tests/test_document_chunking.py`, `apps/api/tests/test_vector_template.py` | No live embedding provider |
| Run-detail embedding usage estimate | Component-rendered summary with a test for tokens, calls and estimated cost | `apps/web/src/components/transfer/EmbeddingUsageSummary.test.tsx` | No engine required; not an engine-live capability |

Gaps keeping Datawrap below parity:
- Paid providers are tested only against fake responses.
- Milvus has not run live; Pinecone evidence is Pinecone Local only.
- CDC for Weaviate, Pinecone, and Milvus has not run end to end.
- No managed rerank or hybrid search.
- The rate limiter is process-local.
- Token chunking has not been tested with real tiktoken.

CDC is at-least-once; exactly-once is not claimed. No retrieval-quality claim is made.
