# DataWrap Architecture Audit — Migration Assurance Backend

**Branch:** `devin/architecture-audit-1785928587`  
**Audit date:** 2026-08-05  
**Scope:** Backend (`apps/api`), preflight package (`packages/preflight`), and frontend decision logic (`apps/web/src/lib`)  
**Method:** Static discovery, automated duplication scans, targeted inspection of schema/introspection/mapping/validation/execution/proof modules. Not every line was read, but all major subsystems were sampled and measured.

---

## 1. Executive summary

DataWrap already has many of the *pieces* of a serious migration-assurance platform: a logical type system, schema introspection for many dialects, semantic mapping, multi-gate preflight, an execution engine, Gate-8 reconciliation, CDC streams, and a growing connector catalog. However, the architecture is **not yet a single authoritative decision engine**. Decisions are recomputed in multiple places, the canonical type model is buried in one 8,300-line monolith, the frontend owns business rules that should live only in the backend, and several source trees (`services/` vs `src/services/`, multiple capability registries, duplicated lossy-coercion tables) create ambiguity about which code is actually running.

The project is best understood as a **collection of competing sub-engines** rather than one deterministic Migration Decision Kernel. Before claiming enterprise-grade correctness, the backend must be refactored so that **Map, Validate, Execute, and Proof consume one immutable Decision Artifact** produced by a single kernel.

This report lists the current subsystems, the most important problems, and a phased plan to reach an enterprise-grade backend.

---

## 2. Codebase scale and shape

| Metric | Value |
|--------|-------|
| Python files in `apps/api/services`, `apps/api/connectors`, `apps/api/src/transfer`, `apps/api/src/routers` | 354 |
| Total classes | 331 |
| Total functions / methods | 4,096 |
| Lines in `services/*.py` | ~82,872 |
| Lines in `connectors/*.py` | ~45,928 |
| Lines in `src/transfer/*.py` | ~17,227 |
| Tests collected (`pytest --collect-only`) | **12,640** |
| `ruff --select F` lint errors | **163** (mostly unused imports / dead code) |

The test surface is large, but the tests are heavily white-box and fixture-driven. A true migration-assurance platform also needs **connector-pair property tests and golden datasets** (numeric edge cases, temporal edge cases, Unicode, large text, JSON, binary, overflow) that run against real or emulated endpoints, not just dictionaries.

---

## 3. Current architecture overview

### 3.1 Subsystems

| Subsystem | Primary files | Responsibility |
|-----------|---------------|----------------|
| **Canonical type / DDL model** | `services/type_system.py` (8,363 lines) | Logical types, canonicalization, destination DDL mapping, lossy-coercion rules, decimal/integer budgets, carrier width/polarity checks. |
| **Schema introspection** | `services/schema_introspect.py` (4,635 lines), `connectors/generic_sql.py` (4,604 lines) | Discover source/destination tables, columns, constraints, indexes, partitions; per-DB `_*_to_logical` mappers (PostgreSQL, MySQL, SQL Server, Oracle, BigQuery, Snowflake, ClickHouse, Kafka, etc.). |
| **Schema inference** | `services/schema_inference.py` | Infer logical types from sample strings. |
| **Semantic mapping** | `services/semantic_mapper.py` (1,898 lines), `services/mapping_pipeline.py` (828 lines), `services/llm_mapping.py` | Map source columns to destination columns by name similarity, BM25, semantic graph, LLM suggestions. |
| **Mapping proof / fidelity** | `services/mapping_proof.py` (1,025 lines), `services/mapping_quality.py` (557 lines), `services/mapping_engine_contract.py` (250 lines) | Per-column fidelity verdict, conversion classification, operator-lock preservation, evidence stamping. |
| **Transformation engine** | `services/transform_engine.py` (1,357 lines), `services/transform_resolver.py` (167 lines) | Apply and resolve value-level transforms (trim, case, date parsing, hash, cast, etc.). |
| **Preflight / validation** | `packages/preflight/src/preflight/engine.py`, `packages/preflight/src/preflight/gates.py`, `services/preflight_service.py` (2,310 lines), `services/validation_mode_contract.py`, `services/ddl_compatibility.py` | G1-G9 gates, validation-mode contract, DDL compatibility checks, risk-contract hydration. |
| **Execution engine** | `src/transfer/engine.py` (4,371 lines), `src/transfer/stream.py` (2,875 lines), `src/transfer/file_stream.py` (1,141 lines), `src/transfer/adapters.py` (2,155 lines) | Chunking, parallel transfer, checkpoint/resume, retries, CDC routing, file streaming. |
| **Reconciliation / proof** | `services/reconciliation.py` (5,763 lines), `src/transfer/reconcile_step.py`, `services/data_integrity.py` (1,378 lines) | Gate-8 row-count / checksum / sample reconciliation, integrity audits. |
| **CDC** | `services/cdc_*.py` (many), `connectors/*_change_stream.py`, `connectors/oracle_logminer.py`, `connectors/pgoutput_decoder.py` | LSN/cursor handoff, Debezium bridge, multi-stream resume, transaction buffers. |
| **Connector catalog** | `src/transfer/connector_capabilities.py` (985 lines), `services/connector_capability_registry.py` (1,075 lines), `src/transfer/registry.py` (376 lines), `connectors/base.py`, `connectors/<driver>.py` | Capability registry, live-matrix, production SKU, driver dispatch. |
| **API layer** | `src/routers/*.py` | FastAPI endpoints for connectors, transfers, preflight, jobs, contracts, auth, etc. |
| **Frontend decision layer** | `apps/web/src/lib/mapping.ts`, `apps/web/src/lib/schemaIntelligence.ts`, `apps/web/src/lib/typeCarrierFidelity.ts`, `apps/web/src/lib/validateDecisionPath.ts`, `apps/web/src/lib/validateHonestyControls.ts` | UI mapping logic, type-risk detection, validation-mode constants, transform vocabularies, ack-tier logic. |

### 3.2 Data flow (current)

```
Source connector / file
      ↓
Schema introspect  ──→  schema_inference / type_system  ──→  canonical column types
      ↓
Endpoint intelligence  ──→  mapping_pipeline + semantic_mapper  ──→  mapping proposals
      ↓
Preflight engine (G1-G9) + validation_mode_contract  ──→  pass / block / risk contract
      ↓
transfer engine ──→ adapters ──→ connector writers ──→ destination
      ↓
reconcile_step + reconciliation  ──→  Gate-8 proof / quarantine
```

The flow is conceptually correct, but the **arrow labels are not single functions**. Each arrow is implemented by several competing modules, and the frontend sometimes runs its own version of the same arrow.

---

## 4. Findings

### 4.1 Canonical type system: powerful but monolithic and partially bypassed

`services/type_system.py` is the closest thing to a canonical model. It defines logical constants (`LOGICAL_STRING`, `LOGICAL_DECIMAL`, etc.), `CANONICAL_TYPES`, `DDL_TYPES`, `DEFAULT_DDL`, and functions such as `normalize_logical_type`, `ddl_type`, `ddl_carrier_type`, `is_lossy_coercion`, `build_column_types`. It also contains hundreds of narrow helper functions for timezone polarity, UUID collapse, vector dimensions, national charset, money/year domains, etc.

**Problems**

1. **Monolith at 8,363 lines.** `type_system.py` owns canonical types, DDL rendering, coercion budgets, string-width arithmetic, numeric bounds, temporal polarity, and specialty-carrier collapse. It is too large to reason about in one file and cannot be reviewed or tested in isolation.
2. **Per-DB logical mappers live in `schema_introspect.py`.** `schema_introspect.py` contains `_pg_to_logical`, `_mysql_to_logical`, `_oracle_to_logical`, `_sqlserver_to_logical`, `_bq_to_logical`, `_sf_to_logical`, `_ch_to_logical`, `_arrow_to_logical`, `_kafka_value_to_logical`, `salesforce_field_to_logical`, `hubspot_property_to_logical`. These functions are responsible for the first step of the canonical conversion (native → logical) but they are in the *introspection* module, not the type system module.
3. **Introspection re-implements logic that `type_system` already has.** For example, `_pg_to_logical` parses `DECIMAL(p,s)`, `VARCHAR(n)`, `TIMESTAMPTZ`, `ARRAY<...>` with hand-written regexes that mirror (and can drift from) `type_system.normalize_logical_type` and `ddl_carrier_type`.
4. **`schema_introspect.py` contains its own sample-driven inference that returns different labels.** `_infer_logical_from_strings` maps samples to `"JSON"`, `"BINARY"`, `"UUID"`, `"DATE"`, `"DATETIME"`, `"TIME"`, `"BOOLEAN"`, `"TEXT"`, `"VARCHAR"`. `type_system.CANONICAL_TYPES` uses `"string"`, `"integer"`, `"decimal"`, etc. The two name spaces are not consistently reconciled.
5. **`schema_inference.py` defines its own `LOGICAL_TYPES` frozenset** (`VARCHAR`, `TEXT`, `INTEGER`, `DECIMAL`, `FLOAT`, `BOOLEAN`, `DATE`, `DATETIME`, `TIME`, `UUID`, `JSON`, `ARRAY`, `BINARY`) and maps everything else to `VARCHAR`. This is another independent source of truth.

**Risk:** The same native type can be normalized differently depending on whether it came from `schema_introspect`, `schema_inference`, or `type_system`, leading to inconsistent mapping, DDL, and fidelity verdicts.

### 4.2 Multiple lossy-coercion / safe-promotion tables

A migration-assurance platform must have **one** classification for whether a conversion is safe. DataWrap currently has several:

| Location | Table / function | Purpose |
|----------|------------------|---------|
| `services/type_system.py` | `is_lossy_coercion` | The most complete check (timezone, decimal params, string width, UUID, vector, charset, etc.). |
| `packages/preflight/src/preflight/gates.py` | `LOSSY_COERCIONS` set | Fallback preflight table for when `type_system` cannot be imported. |
| `services/mapping_quality.py` | `_SAFE_PROMOTIONS` frozenset | Safe semantic promotions used by mapping scoring. |
| `services/mapping_proof.py` | `_MUTATING_TRANSFORMS`, `_LOSSY_CAST_TRANSFORMS`, `_PRESERVE_TRANSFORMS` | Transform-centric fidelity classification. |
| `apps/web/src/lib/typeCarrierFidelity.ts` | `stringWidthWouldNarrow`, `decimalWouldCollapse`, `isTzAwareTemporal`, etc. | Frontend type-risk detection. |

These tables can disagree. For example, `preflight/gates.py` marks `("FLOAT", "DECIMAL")` as lossy, but the writer path and mapping proof may classify it differently depending on the code path. The frontend has its own `stringWidthWouldNarrow` that duplicates `type_system.string_width_would_narrow`.

**Risk:** A column can be green in the UI, pass preflight, and still be quarantined at write time because the lossy-coercion decision is not computed once.

### 4.3 Two `services` packages with ambiguous import paths

`apps/api` contains both `services/` and `src/services/`. Some files in `src/services/` are compatibility shims that re-export from `services/`, but others differ:

- `services/__init__.py` is **empty**; `src/services/__init__.py` exports `MongoDBService`, `get_mongodb_service`, `FileParser`, `ParseResult`.
- `services/mongodb_service.py` is the 1,320-line canonical implementation; `src/services/mongodb_service.py` is a 12-line shim.
- `services/data_contract.py` is the 389-line canonical implementation; `src/services/data_contract.py` is a 16-line shim.
- However, files such as `src/services/preflight_service.py`, `src/services/transfer_plan_store.py`, etc., still exist in both trees with **different** contents.

Import counts:
- `from services.` : **2,487**
- `from src.services.` : **95**
- `from src.transfer.` : **540**
- `from transfer.` : **27**

`src/transfer/engine.py` (lines 27-99) wraps the bulk of its service imports in `try/except` blocks to import from `services` or `src.services` depending on `PYTHONPATH`, which is a sign that the package layout is not deterministic.

**Risk:** Runtime behavior changes based on how the worker is launched. Two engineers can run the "same" code and get different behavior because different module paths resolve.

### 4.4 Three overlapping capability / certification registries

1. `services/connector_capability_registry.py` — static `CAPABILITY_REGISTRY` dictionary with marketing-style tiers and `transfer_ready` flags.
2. `src/transfer/connector_capabilities.py` — dynamic driver-capability matrix (`_DRIVER_CAPS`, `_FILE_CAPS`, `TRANSFER_READY_CATALOG_IDS`, `CATALOG_ID_ALIASES`) with runtime driver-availability checks.
3. `src/transfer/registry.py` — `LIVE_MATRIX` and `PRODUCTION_SKU` that enumerate source/destination route pairs.

The docs say the static registry is "marketing-only" and overwritten by `connector_capabilities`, but the code still imports and references `CAPABILITY_REGISTRY` in tests and probes. Having three places where a connector is considered "live" is exactly the catalog-honesty problem the `.cursor/rules` warn against.

**Risk:** A connector can be marked `transfer_ready` in one registry and "planned" in another, leading to the kind of "over-certified" bugs the cursor audit flagged.

### 4.5 Frontend owns migration decisions

The absolute design principle in the implementation brief is:

> Every migration decision must exist exactly once. Backend is authoritative. Frontend only renders backend decisions.

The current frontend violates this:

| Decision | Backend authority | Frontend duplicate |
|----------|-------------------|------------------|
| Transform vocabulary | `services/transform_resolver.py` `UI_TO_ENGINE` / `ENGINE_TO_UI` | `apps/web/src/lib/mapping.ts` `UI_TO_ENGINE_TRANSFORM` / `ENGINE_TO_UI_TRANSFORM` |
| Transform → lossy/mutate/preserve | `services/mapping_proof.py` `transform_fidelity` | `apps/web/src/lib/mapping.ts` `engineStampedRiskChip` + `isSafeNormalizeMapping` |
| Confidence threshold per validation mode | `services/validation_mode_contract.py` | `apps/web/src/lib/mapping.ts` `confidenceThresholdForMode` |
| Type-risk detection (width, decimal, TZ) | `services/type_system.py` | `apps/web/src/lib/typeCarrierFidelity.ts` (`stringWidthWouldNarrow`, `decimalWouldCollapse`, `parseDecimalPrecisionScale`) |
| Mapping ack tier (approve / review / accept_risk) | `services/migration_risk_contract.py` | `apps/web/src/lib/mapping.ts` `mappingAckTier` |
| Sample → logical type | `services/schema_inference.py` | `apps/web/src/lib/mapping.ts` `inferLogicalFromSample` |
| Nested-document detection | backend schema intelligence | `apps/web/src/lib/schemaIntelligence.ts` `detectNestedDocumentFields` |
| Validation-mode constants | `services/validation_mode_contract.py` | `apps/web/src/lib/transferConstants.ts` `VALIDATION_MODES` |
| Honesty controls for Validate UI | preflight proof bundle | `apps/web/src/lib/validateHonestyControls.ts` `buildValidateHonestyControls` |

**Concrete mismatches in transform vocabulary**

Backend `UI_TO_ENGINE` (`transform_resolver.py`):
- `mask_pii` → `mask_pii`
- `normalize_unicode` → `normalize_unicode`

Frontend `UI_TO_ENGINE_TRANSFORM` (`mapping.ts`):
- `mask_pii` → `hash_pii`
- `normalize_unicode` → `strip_controls`
- `base64` (engine id) is not represented; frontend collapses it to `binary`
- `url`, `iban`, `postal` are mapped to UI `none` but engine does not

**Risk:** An operator selects "mask PII" in the UI, the frontend sends `hash_pii` to the engine, and the mapping round-trips as a different transform. This is a direct violation of the mapping-engine contract.

### 4.6 Execution engine is tightly coupled to many services

`src/transfer/engine.py` imports directly from 20+ service modules and contains fallback import blocks for both `services.*` and `src.services.*`. It also imports `ai.training.training_scheduler` and many transformation modules. At 4,371 lines it is more than an execution planner; it embeds preflight, policy, row filtering, SCD2, CDC, connection recovery, and checkpoint logic.

**Problems**
- The engine recomputes confidence thresholds and validation logic that `preflight_service` already computes (`confidence_threshold_for_mode` is called multiple times in `engine.py` at lines 1377, 1465, 2206, 3042, 3673).
- It mixes orchestration with business rules, violating single-responsibility.
- The engine does not consume a single precomputed Decision Artifact; it derives decisions again at run time.

### 4.7 Mapping and endpoint intelligence recompute each other

`src/transfer/endpoint_intelligence.py` builds `type_mappings` and `mapping_preview` itself by calling `ddl_type` and `run_mapping_pipeline`. `src/transfer/engine.py` and `mapping_pipeline.py` call similar functions. The mapping decision is produced in at least three places:

1. `mapping_pipeline.run_mapping_pipeline`
2. `endpoint_intelligence` plan builder
3. `engine.py` runtime mapping refinement
4. `semantic_mapper.map_columns`

None of these return or consume a single, versioned `MigrationDecision` object. Each re-derives target types, confidence, and risk.

### 4.8 Preflight lives in two packages

The preflight engine is in `packages/preflight/src/preflight/` (a separate package) and is used by `services/preflight_service.py`, which adds its own gates, context wrappers, and proof-bundle logic. There is also `services/preflight_runtime.py`, `services/preflight_rules.py`, `services/preflight_proof_bundle.py`, `services/preflight_run_store.py`.

This split means the **canonical G1-G9 gate definitions are not in the same package as the proof bundle and UI wiring**, making it hard to keep gate semantics, risk contracts, and validation-mode contracts aligned.

### 4.9 Connector writers carry DDL and type logic

`connectors/generic_sql.py` (4,604 lines) contains merge/upsert SQL for many dialects, temporary-table DDL, and type-specific handling. Individual writers (`postgresql_writer.py`, `mysql_writer.py`, `bigquery_writer.py`, `snowflake_writer.py`, `iceberg_writer.py`) each contain create-table DDL, type coercion, and target-specific workarounds. There is no shared `ConnectorWriter` abstraction that receives a canonical DDL statement and a validated decision; instead each writer re-derives how to render types and handle overflows.

### 4.10 Reconciliation is large and probably mixed with mapping logic

`services/reconciliation.py` is 5,763 lines, the largest module in the backend. It likely contains row-count, checksum, sample, and constraint validation code, but its size suggests it also handles mapping comparison and proof packaging. This should be split into `reconciliation/proof.py`, `reconciliation/checksum.py`, `reconciliation/sampling.py`, `reconciliation/constraint.py`.

### 4.11 Dead code and lint debt

- `ruff --select F` reports **163 errors**, mostly unused imports. This indicates dead imports, likely from rapid expansion.
- The codebase has **12,640 tests**, but many are narrowly scoped fixture tests. There is no visible **connector-pair property test harness** or **golden dataset** that exercises the full `native → canonical → native` path across many source/destination/type combinations.

### 4.12 CDC and streaming are separate code paths

CDC is implemented in `connectors/*_change_stream.py` plus many `services/cdc_*.py` modules. Batch transfer is in `src/transfer/engine.py` and `src/transfer/stream.py`. The two paths share no common checkpoint/abstraction layer, which makes "exactly-once" or "at-least-once" proofs harder to reason about. The current honest posture is at-least-once, which is correct, but the proof engine does not have a unified way to assert this for both batch and streaming.

### 4.13 Security and secrets

- `services/secret_vault.py`, `services/byok_key_manager.py`, and connector modules handle credentials. This was flagged in the previous cursor audit as needing hardening (SSO RelayState, enrollment gating, etc.).
- The duplicate `services/` / `src/services/` tree means a stale `auth_service.py` or `rbac.py` could be imported depending on `PYTHONPATH`.

### 4.14 Observability is fragmented

There are separate modules for `audit_log.py`, `audit_anchor.py`, `lineage_telemetry.py`, `data_integrity.py`, `ops_metrics.py`, `phase_profile.py`, `tracing.py`, `logging_config.py`. There is no unified `MigrationTelemetry` interface that every subsystem writes to. Building a single audit trail from discovery through proof requires stitching these together.

---

## 5. Root causes

The findings cluster around five root causes:

1. **No Decision Artifact.** Map, Validate, Execute, and Proof each recompute their own view of the migration. There is no immutable `MigrationDecision` object that every subsystem consumes.
2. **Canonical model is split and monolithic at the same time.** `type_system.py` is too big, but the per-DB logical mappers live elsewhere and other modules define their own lossy-coercion tables.
3. **Frontend/backend boundary is wrong.** The frontend is not a pure renderer; it contains business rules, transform dictionaries, and risk classification that can contradict the backend.
4. **Package structure is ambiguous.** `services/` vs `src/services/`, `from transfer.` vs `from src.transfer.`, and multiple capability registries create non-determinism.
5. **Rapid feature expansion without consolidation.** Many connectors, gates, and tests were added, but shared abstractions were not extracted. The result is a codebase that works for specific routes but is not a general migration kernel.

---

## 6. Phased implementation plan

The plan is **evolutionary**, not a big-bang rewrite. Each phase has a measurable exit criteria.

### Phase 0 — Stabilize the foundation (1 sprint)

**Goal:** Make the codebase deterministic and green before building the kernel.

1. **Resolve the `services/` vs `src/services/` duplication.**
   - Make `apps/api/services` the single canonical package.
   - Convert `apps/api/src/services` to a true compatibility namespace that re-exports from `apps/api/services`, or remove it and update the 95 `from src.services` imports.
   - Remove all `try/except` dual-import blocks (e.g., in `src/transfer/engine.py` lines 27-99 and 100-152).

2. **Fix `ruff --select F` errors.**
   - Clean the 163 unused-import / dead-code issues.
   - Add `ruff` to CI so the count stays at zero.

3. **Unify the package import convention.**
   - Standardize on `from services. ...` and `from src.transfer. ...`.
   - Update `apps/api` entrypoints so `PYTHONPATH` is consistent.

**Exit criteria**
- `ruff --select F .` returns 0.
- `pytest --collect-only` still collects all 12,640 tests.
- `from src.services` imports are gone or are guaranteed shims.

### Phase 1 — Refactor the canonical type system (1-2 sprints)

**Goal:** One canonical type model with a clear `native → canonical → native` flow.

1. **Split `services/type_system.py` into a package:**
   - `services/type/canonical.py` — logical constants and normalization.
   - `services/type/ddl.py` — destination DDL rendering.
   - `services/type/coercion.py` — lossy-coercion, safe promotion, width/precision checks.
   - `services/type/carriers.py` — parametric carriers (DECIMAL(p,s), VARCHAR(n), VECTOR(n), etc.).
   - `services/type/dialects/` — per-DB native-to-canonical mappers (`pg.py`, `mysql.py`, `sqlserver.py`, `oracle.py`, `bq.py`, `sf.py`, `ch.py`, `avro.py`, `arrow.py`, `saas.py`).
   - `services/type/__init__.py` — re-export the public API (`normalize`, `ddl_for`, `is_lossy`, `compare`).

2. **Move per-DB `_*_to_logical` functions** from `schema_introspect.py` into the new dialect modules.

3. **Make `schema_inference.py`, `mapping_quality.py`, `mapping_proof.py`, and `preflight/gates.py` call the canonical package** instead of defining their own lossy-coercion / safe-promotion tables.

4. **Introduce a `TypeCarrier` dataclass** that carries precision, scale, width, timezone, charset, nullability, and identity/generated metadata. Stop passing raw strings for types.

**Exit criteria**
- No module outside `services/type` defines canonical-type or lossy-coercion tables.
- A single property-based test can generate arbitrary native type strings and prove `native → canonical → DDL` is deterministic.
- `pytest` type-system tests pass.

### Phase 2 — Build the Migration Decision Kernel (2 sprints)

**Goal:** A single backend authority that produces one immutable `MigrationDecision`.

1. **Create `services/migration_kernel.py`** with a `MigrationKernel` class and a `MigrationDecision` dataclass:
   - `discover(source)` → `SchemaModel`
   - `introspect(destination)` → `SchemaModel`
   - `map(source_schema, dest_schema, config)` → `MappingModel`
   - `validate(mapping, config)` → `ValidationModel` (G1-G9, risk contracts)
   - `execute(mapping, validation, config)` → `ExecutionPlan`
   - `prove(run, plan)` → `ProofArtifact`

2. **The `MigrationDecision` contains:**
   - Source and destination `SchemaModel`
   - `MappingModel` with per-column `ConversionClassification` (identity/equivalent/widening/representation/normalization/business-review/potentially-lossy/lossy/unsupported/unknown)
   - `ValidationModel` with gate statuses, risk contracts, execution mode
   - `ExecutionPlan` with chunking, sync mode, checkpoint, resume policy
   - `ProofPlan` with row-count, checksum, sample, constraint checks

3. **Make all routers and the execution engine consume `MigrationDecision`.**
   - `POST /transfer/map` returns a `MappingModel`.
   - `POST /transfer/validate` returns a `ValidationModel` derived from the stored `MappingModel`.
   - `POST /transfer/execute` receives a `MigrationDecision` id and executes the already-approved plan.
   - `src/transfer/engine.py` becomes a thin executor that reads the `ExecutionPlan`; it does not recompute confidence thresholds or mapping logic.

4. **Version the decision artifact.** Add `decision_id`, `version`, `created_at`, and a hash so Map/Validate/Execute/Proof can prove they used the same decision.

**Exit criteria**
- The frontend no longer computes mapping confidence, risk tiers, or transform vocabulary; it only calls backend endpoints.
- `MigrationDecision` is the only object passed from Map → Validate → Execute → Proof.
- Tests prove that mutating a stored decision invalidates the execution hash.

### Phase 3 — Connector-agnostic writer abstraction (2 sprints)

**Goal:** Writers are simple implementations of a canonical `Connector` protocol.

1. **Define `Connector` protocols:**
   - `SourceConnector.discover()`, `.read_batch()`, `.read_stream()`, `.get_capabilities()`
   - `DestinationConnector.introspect()`, `.create_or_alter_table(ddl)`, `.write_batch()`, `.write_stream()`, `.reconcile()`

2. **Refactor `connectors/base.py` to use the protocol and the `TypeCarrier`/`MigrationDecision`.**

3. **Migrate one high-value connector pair first** (e.g., PostgreSQL → PostgreSQL) through the new kernel to prove the abstraction.

4. **Move dialect-specific DDL from `generic_sql.py` into `services/type/dialects/`** so each dialect renders its own create/alter/merge/upsert SQL.

**Exit criteria**
- Adding a new connector requires implementing the `Connector` protocol, not editing `generic_sql.py`.
- PostgreSQL → PostgreSQL and CSV → PostgreSQL routes pass end-to-end property tests through the kernel.

### Phase 4 — Validation, preflight, and risk contracts (1-2 sprints)

**Goal:** One validation engine that consumes the `MigrationDecision`.

1. **Merge `packages/preflight` into `services/preflight/`** (or keep it as a pure-kernel dependency) so gates, risk contracts, and proof bundles live together.

2. **Make every gate operate on `MigrationDecision`:**
   - G1: source connectivity / schema discovery
   - G2: destination connectivity / privileges / existing schema
   - G3: type fidelity (uses canonical `is_lossy`)
   - G4: mapping confidence
   - G5: dry-run sample transform
   - G6: DDL generation and identity
   - G7: capacity / storage
   - G8: execution policy (resume, retry, at-least-once)
   - G9: constraint / PK / uniqueness

3. **Implement the `ConversionClassification` algorithm** exactly as specified in the mission: identity, equivalent, widening, representation-change, normalization, business-review, potentially-lossy, lossy, unsupported, unknown. Each classification returns confidence, evidence, impact, recommendation, execution policy, and audit info.

4. **Make `validation_mode_contract.py` the only place validation modes are defined.** Remove `confidenceThresholdForMode` from the frontend.

**Exit criteria**
- All nine gates run from a single `PreflightEngine` over `MigrationDecision`.
- UI displays gate results verbatim from the backend; it does not compute risk chips.

### Phase 5 — Proof and reconciliation as first-class artifacts (1-2 sprints)

**Goal:** Every transfer produces a measurable, signed proof.

1. **Split `services/reconciliation.py` into a package:**
   - `proof/row_count.py`
   - `proof/checksum.py`
   - `proof/sampling.py`
   - `proof/constraint.py`
   - `proof/audit.py`

2. **Define `ProofArtifact` dataclass** with source checksum, target checksum, quarantine list, gate pass/fail, and operator signature.

3. **Sign proof artifacts** using the BYOK key manager (`services/byok_key_manager.py`) so they are tamper-evident.

4. **Make proof the input to "Resume"** — a transfer can only resume if its previous `ProofArtifact` hash matches the stored checkpoint.

**Exit criteria**
- Every successful transfer writes a `ProofArtifact`.
- A mismatch in `ProofArtifact` between Map and Execute fails closed.

### Phase 6 — Performance, observability, and hardening (2-3 sprints)

**Goal:** The platform is fast, observable, and secure enough for enterprise production.

1. **Build a unified telemetry interface** (`services/telemetry.py`) that every subsystem writes structured events to.
2. **Add property-based / golden-dataset tests** for numeric, temporal, Unicode, binary, JSON, overflow, and schema-drift cases.
3. **Run multi-million-row benchmarks** against the supported route matrix.
4. **Harden SSO/vault, RBAC, and tenant isolation** based on the cursor audit findings.
5. **Add chaos/recovery tests** for worker crash, network partition, and CDC lag.

**Exit criteria**
- 10M-row transfer completes with checksum match and bounded memory.
- Chaos tests pass for worker crash mid-transfer.
- Security review closes SSO/vault gaps.

---

## 7. Immediate next steps for this branch

The first commit on `devin/architecture-audit-1785928587` should be **non-breaking stabilization**:

1. Run `ruff check --select F --fix .` and commit the lint cleanup.
2. Add a `services/__init__.py` that exports the public surface, and make `src/services` a pure re-export shim.
3. Remove dual `try/except` import blocks in `src/transfer/engine.py` and other modules.
4. Add a CI check that fails on `ruff --select F`.

After stabilization, the next commit should start Phase 1: create `services/type/` and begin moving the canonical model.

---

## 8. Summary of the most urgent problems

| # | Problem | Severity | Phase |
|---|---------|----------|-------|
| 1 | `services/` vs `src/services/` duplicate and ambiguous imports | High | 0 |
| 2 | `type_system.py` monolith; per-DB mappers in `schema_introspect.py` | High | 1 |
| 3 | Multiple independent lossy-coercion / safe-promotion tables | High | 1 |
| 4 | Frontend owns mapping risk, transform vocab, confidence thresholds | High | 2 |
| 5 | No single `MigrationDecision` artifact consumed by Map/Validate/Execute/Proof | Critical | 2 |
| 6 | Execution engine recomputes mapping/validation logic | High | 2 |
| 7 | Three overlapping connector capability registries | Medium | 0/3 |
| 8 | Preflight split across `packages/preflight` and `services/preflight_*` | Medium | 4 |
| 9 | Connector writers embed DDL and type logic | Medium | 3 |
| 10 | `reconciliation.py` is 5,763 lines and mixed with proof logic | Medium | 5 |
| 11 | 163 `ruff F` errors and dead imports | Low | 0 |
| 12 | No unified telemetry / observability layer | Medium | 6 |

The project has strong conceptual foundations, but the architecture must be consolidated around a single backend Decision Kernel before it can be marketed or sold as an enterprise migration-assurance platform. The roadmap above is designed to do that incrementally while keeping tests green and existing routes working.
