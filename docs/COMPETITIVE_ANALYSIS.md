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

## G-CONN M5: SDK source breadth and transfer evidence

For this connector slice, Datawrap is **6/10** versus **Airbyte/Fivetran at 9/10**. Named evidence is synthetic: `apps/api/tests/test_gconn_sdk_transfer_routing.py` verifies descriptor-driven routing and source-only catalog enrichment; `apps/api/tests/test_gconn_sdk_transfer_e2e.py` exercises GitHub/Jira full-reread PK upsert and Intercom's fresh-destination paginated read. The SDK matrix does not establish live vendor compatibility, quotas, real-volume throughput, or destination-role readiness.

The increment is three SDK sources (GitHub, Jira, Intercom) plus the existing HubSpot SDK source—not parity with the hundreds of connectors available from Airbyte/Fivetran. Catalog enrichment remains `beta` / `source_only`; transfer capabilities are full-refresh-only. Intercom existing-destination epoch remapping and SDK `resume=True` Gate-8 are strict xfails and remain unverified; see `docs/CONNECTOR_CERTIFICATION.md` under **Known engine gaps (G-CONN M5)**.

## G-LAKE: Iceberg / lakehouse (M6)

DataWrap's lakehouse score remains **6/10**; the proposed increase is pending runtime exactly-once route selection and has not been applied (`tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`).

| System | Iceberg commit/write pattern | DataWrap comparison and test evidence | Sources |
| --- | --- | --- | --- |
| Kafka Connect Iceberg sink | Coordinator commits; source offsets are stored in snapshot summaries for exactly-once recovery. | DataWrap mirrors the watermark into the snapshot summary and table properties and fences stale writers, but the runtime classifier still refuses Iceberg (`tests/test_iceberg_eos.py::test_apply_mirrors_watermark_properties_and_snapshot_summary`, `tests/test_iceberg_eos.py::test_zombie_writer_redecides_after_fence_steal`, `tests/test_cdc_exactly_once.py::test_named_matrix_artifact_matches_measured`). | [Kafka Connect sink](https://iceberg.apache.org/docs/nightly/kafka-connect/); [Coordinator.java](https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java) |
| Flink Iceberg sink | Checkpoint/job IDs are recorded in snapshot summaries; recovery walks snapshot ancestry. | DataWrap's table-property watermark remains available after snapshot expiry (`tests/test_iceberg_eos.py::test_watermark_survives_snapshot_expiration`, `tests/test_iceberg_maintenance.py::test_m2_watermark_resolves_after_m5_expiration`). | [SinkUtil.java](https://github.com/apache/iceberg/blob/main/flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/sink/SinkUtil.java); [recovery issue #14090](https://github.com/apache/iceberg/issues/14090) |
| Fivetran Managed Data Lake | Copy-on-write updates with managed snapshot and orphan-file maintenance. | DataWrap's catalog path is copy-on-write and guards snapshot expiry; compaction and orphan removal remain unsupported (`tests/test_iceberg_eos.py::test_registry_describes_filesystem_and_catalog_delete_paths`, `tests/test_iceberg_maintenance.py::test_expiry_preserves_current_tag_and_newest_and_rejects_expired_id`, `tests/test_iceberg_maintenance.py::test_compaction_and_orphan_removal_are_typed_unsupported_operations`). | [Managed Data Lake](https://fivetran.com/docs/managed-data-lake-service); [copy-on-write strategy](https://fivetran.com/docs/managed-data-lake-service/troubleshooting/copy-on-write-update-strategy) |
| Airbyte S3 Data Lake | Merge-on-read equality deletes avoid rewriting data files. | DataWrap keeps catalog writes copy-on-write; equality-delete behavior is confined to the filesystem path (`tests/test_iceberg_eos.py::test_registry_describes_filesystem_and_catalog_delete_paths`). | [S3 Data Lake docs](https://docs.airbyte.com/integrations/destinations/s3-data-lake); [PR 82749](https://github.com/airbytehq/airbyte/pull/82749) |
| Snowflake Iceberg tables | Uses position deletes (v2) or deletion vectors (v3), not equality deletes. | DataWrap's catalog writer uses copy-on-write rather than equality-delete merge-on-read; no Snowflake live compatibility claim is made (`tests/test_iceberg_eos.py::test_registry_describes_filesystem_and_catalog_delete_paths`). | [Snowflake Iceberg table management](https://docs.snowflake.com/en/user-guide/tables-iceberg-manage) |

**Scope limits:** DataWrap does not ship a Delta Lake writer; Databricks reads Iceberg through UniForm/federation, which is not native Delta support (`tests/test_first_party_capability_contract.py::test_loss_upsert_and_airbyte_pack_leads_do_not_steal_neighbors`). Glue, Hive, Nessie, and Polaris catalogs, throughput at scale, and live catalog 5xx recovery remain unverified (`tests/test_iceberg_commit.py::test_live_rest_concurrent_upserts_keep_one_row_per_key`, `tests/test_iceberg_commit.py::test_unknown_commit_is_recovered_by_commit_id`).
