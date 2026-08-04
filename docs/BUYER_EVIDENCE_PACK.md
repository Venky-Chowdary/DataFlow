# Buyer evidence pack (diligence)

Use this pack instead of marketing screenshots when Fortune 100 teams ask “prove it.”

## 1. Core preflight gates (G1–G9)

| Gate | Package |
|------|---------|
| G1–G9 | `packages/preflight` (`GateId`, `PREFLIGHT_GATES`) |
| Host policy extras | Studio Validate may add sync/schema/validation policy gates — distinct from core G1–G9 |
| Constraint findings | Structured FK findings + optional **sample** orphan probe; opt-in **population** orphan scan is the only path to RI `proven` (`docs/POPULATION_ORPHAN_SCAN.md`) — never invents population RI from sample alone |
| Root Cause Engine | `apps/api/services/root_cause_engine.py` — one fidelity root → many impacted gates (no duplicate blockers) |
| Mapping confidence SSOT | `g4_mapping_confidence` only hard-blocks; proof/G9 report (`docs/MAPPING_CONFIDENCE_AUTHORITY.md`) |
| Validation modes | `strict` / `maximum` / `balanced` / `migration` / `discovery` / `audit` with guarantees + non-guarantees (`docs/VALIDATION_MODE_CONTRACT.md`); `discovery`/`audit` **never write** |
| Risk Contract | Immutable signed Accept Risk (`docs/MIGRATION_RISK_CONTRACT.md`); writers honor execution policy |
| Coverage honesty | Sample ≠ population (`docs/VALIDATION_COVERAGE_CONTRACT.md`); Gate-8 stamps `checksum_match` / `assurance_level` |
| Quarantine DLQ | Fail-closed if control-plane DLQ cannot persist rejects (`docs/QUARANTINE_DLQ_FAIL_CLOSED.md`) |
| Rollback workflow | Signed plans; only `DISCARD_STAGING` executable — no population undo (`docs/MIGRATION_ROLLBACK.md`) |
| Proof post-write | Signed packs stamp `assurance`; `migration_proven` only for full_checksum post-write — never from pre-write / sample / writer-ack alone (`docs/PROOF_POST_WRITE_CONTRACT.md`) |
| Quarantine row contract | Every reject stamps original/expected/actual/reason/transform/recovery/PKs/job/connector/retry (`docs/QUARANTINE_ROW_CONTRACT.md`) |
| Validate decision path | Root Cause → Affected Gates → Impact → Actions → Preview → Risk Contract → Execute; Execute-ready ≠ `migration_proven` (`docs/VALIDATE_DECISION_PATH.md`) |
| Conversion contract | Charter 7-class ConversionClass + Map→DDL identity hash; invent (p,s)/FSP/TZ needs approval; AI type matrix non-authoritative (`docs/CONVERSION_CONTRACT.md`) |
| Mapping engine contract | Operator locks never silently overwritten; charter evidence fields stamped (`docs/MAPPING_ENGINE_CONTRACT.md`) |
| Execution engine contract | At-least-once honesty; refuse insert resume without checkpoint; Kafka offset commit fail-closed; never claim exactly-once (`docs/EXECUTION_ENGINE_CONTRACT.md`) |

Do **not** claim “8 gates,” “ten gates,” or invent a marketed “G10 constraints” gate. Required core gates remain **G1–G9**.

## 2. PRODUCTION_SKU routes

Source of truth: `apps/api/src/transfer/registry.py` → `PRODUCTION_SKU`.

UI ledger: Workspace → Benchmarks → Integrity / PRODUCTION_SKU panel (reads API metrics).

Rule: if a route is not in `PRODUCTION_SKU`, it is not a committed migration claim.

### Offline pair assurance (datatype / DDL / coercion)

| Claim | Module | Proves | Does **not** prove |
|-------|--------|--------|--------------------|
| `pair_assurance_offline` | `apps/api/services/pair_assurance.py` | Every PRODUCTION_SKU **database→database** pair: type inventory × create-new DDL stamp × lossy/risk/coercion fail-closed × mapping contract × fixture transforms | Live transfer, checksum/Gate-8 population, FK/orphan RI, one-click transfer undo, CDC exactly-once |
| `connector_pair_matrix` | `tests/test_mapping_connector_pair_matrix.py` | Name-match golden score across dialect labels | Type/DDL fidelity |

Proof artifacts: `apps/api/data/proofs/pair_assurance/{src}__{dst}.json` + `_summary.json`.

Live execute + reconcile remains `tests/test_production_sku_matrix.py` (separate claim).

## 3. Mapping engine

- Studio map: BM25 + semantic token graph + Hungarian assignment (`apps/api/services/semantic_mapper.py`)
- Fidelity SSOT: `preserve` / `cast` / `mutate` / `lossy_cast` (`mapping_proof.py`)
- Operator risk ack: G4 + web `mapping.ts` tiers (Approve / Review / Accept risk)
- Optional LLM assist is hybrid and constrained — not a substitute for gates
- **Map≡CREATE:** explicit Map `target_type` is preserved by `safe_ddl_logical_type(..., honor_explicit=True)` — writers must not rewrite approved DDL from sample inference; unfit values quarantine on write (`test_map_equals_create_ddl.py`)
- **Map≡CREATE (BigQuery):** `bq_schema_field` applies Map `NUMERIC`/`BIGNUMERIC(p,s)` via SchemaField `precision`/`scale` — never strips to bare platform invent (`test_bigquery_writer.py`)
- **Map≡CREATE (bare DECIMAL):** generic_sql `_sa_type_for_logical` uses `ddl_type` SSOT for bare `DECIMAL`/`NUMBER` — never invents `Numeric(38,15)` when the destination default is `(38,10)` (`test_generic_sql_decimal_carrier.py`)
- **Map≡CREATE (Iceberg):** bare `DECIMAL` → `decimal(38,10)` for quarantine + Arrow; oversize stamps fail closed to `string` — never silent `decimal128` clamp (`test_iceberg_decimal_map_create.py`)
- **Map≡CREATE (SQLite):** `DECIMAL`/`NUMERIC`/`MONEY` rematerialize to `TEXT` — never NUMERIC affinity invent that stores high-precision values as IEEE `real` (`test_sqlite_decimal_affinity_map_create.py`)
- **Map≡CREATE (SQLite UUID/JSON/TIMESTAMP):** foreign `UUID`/`GUID`/`JSON`/`TIMESTAMP`/… rematerialize to `TEXT`/`INTEGER` — never NUMERIC affinity invent (`12345`→integer, `550e8400`→`inf`) (`test_sqlite_uuid_json_timestamp_affinity_map_create.py`)
- **Map≡CREATE (binary typmod):** foreign `BINARY(n)`/`VARBINARY(n)`/`BYTES(n)`/`fixed(n)` rematerialize to dest wire (`BYTES`/`VARBYTE`/`BYTEA`/`fixed`/`RAW`/`BLOB`) — never illegal CREATE pass-through (`test_binary_typmod_materialize_map_create.py`)
- **Map≡CREATE (bare DECIMAL):** bare `DECIMAL`/`NUMERIC`/`NUMBER` rematerialize to `ddl_type` SSOT `(p,s)` so quarantine can parse capacity — never MySQL `(10,0)` invent or silent money truncate (`test_bare_decimal_materialize_map_create.py`); Snowflake bare uses SSOT `NUMBER(38,10)` not batch invent
- **Map≡CREATE (foreign temporal):** foreign `TIMESTAMP`/`DATETIME`/`DATETIME2`/`TIMESTAMPTZ`/… rematerialize to dest `ddl_type` SSOT — never SQL Server `TIMESTAMP`→ROWVERSION, MySQL session-TZ `TIMESTAMP`, or Snowflake bare `TIMESTAMP` polarity invent (`test_foreign_temporal_materialize_map_create.py`); native wires (`DATETIME2`, `DATETIME(6)`, `TIMESTAMP_NTZ`, …) still pass through
- **Map≡CREATE (foreign float):** foreign `FLOAT4`/`HALF`/`REAL`/`BINARY_FLOAT`/… rematerialize to dest IEEE wire — never illegal CREATE aliases; Spanner single → `FLOAT32` (never invent `REAL`) (`test_foreign_float_materialize_map_create.py`); native `REAL`/`FLOAT`/`FLOAT64`/`BINARY_*`/`float` wires still pass through
- **Map≡CREATE (foreign specialty):** `VECTOR(n)`→`VECTOR(FLOAT,n)` / `vector(n)`, `BIT`→boolean polarity, bare `ENUM`/`SET`, off-engine `MONEY`/`YEAR`/`MEDIUMINT` rematerialize to `ddl_type` SSOT (`test_foreign_specialty_materialize_map_create.py`); native MySQL `ENUM('…')`/`YEAR`/`MEDIUMINT`, SQL Server `MONEY`/`BIT`, PG `BIT(n)`/`vector(n)` still pass through
- **Map≡CREATE (BigQuery writer):** `bq_type` / `bq_schema_field` / quarantine resolve use `materialize_dest_ddl` — never blind `ddl_type` invent (`TIMESTAMP`→`DATETIME`); foreign `VARCHAR`/`INTEGER` → `STRING`/`INT64`; parameterized `NUMERIC(p,s)` Map stamps kept (`test_bigquery_writer_map_create_materialize.py`)
- **Map≡CREATE (generic_sql SA):** `_sa_type_for_logical` honors NTZ vs TZ polarity (Oracle/DuckDB `TIMESTAMP` never invents `WITH TIME ZONE`; Databricks session-`TIMESTAMP` stays aware), Databricks `FLOAT` not `Double`, and `INTEGER` not `BigInteger` (`test_generic_sql_sa_map_create_fidelity.py`)
- **Map≡CREATE (Databricks TIMESTAMP stamp):** native Map `TIMESTAMP` (session-TZ) is not rematerialized to `TIMESTAMP_NTZ` — foreign `DATETIME2`/`TIMESTAMPTZ` still legalize via SSOT (`test_databricks_timestamp_map_stamp_authority.py`)
- **Map≡ALTER:** explicit Map `target_type` is a hard widen ceiling — PostgreSQL/MySQL/Snowflake **and** generic_sql (SQL Server / Oracle / DuckDB) backfill may ALTER **up to** the stamp, never past it (`desired_types_honoring_map_stamps`, `_widen_existing_columns_sa` stamp ceiling, `test_map_equals_alter_ddl.py`); overflow cells quarantine instead of silent widen

## 4. Delivery / CDC

- Default CDC delivery: **at-least-once**
- Destinations must upsert with primary key / LSN (or equivalent) guards
- Checkpoint persistence failures **abort** the job (`CheckpointPersistenceError`)

Workspace security posture exposes `cdc_delivery` / `cdc_honesty` for questionnaires.

## 5. Security posture (not certification)

| Control | Status |
|---------|--------|
| Secret vault (Fernet / AWS SM) | Shipped; prod plaintext ban |
| Connector / transfer_request encryption | Shipped |
| MCP auth + RBAC when required | Shipped |
| BYOK local/wrapped | Shipped |
| BYOK AWS KMS envelope | Implemented (requires boto3 + IAM) |
| SOC 2 / HIPAA / PCI auditor letters | **Not claimed** until artifacts exist |

## 6. Suggested test anchors

```text
apps/api/tests/test_production_sku_*.py
apps/api/tests/test_production_audit_gates.py
packages/preflight/tests/test_gates.py
apps/api/tests/test_checkpoint_service.py
apps/web/src/lib/mapping.test.ts
```

Regenerate route counts with `scripts/measure_connector_type_coverage.py` when publishing a diligence PDF.
