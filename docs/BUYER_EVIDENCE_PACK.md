# Buyer evidence pack (diligence)

Use this pack instead of marketing screenshots when Fortune 100 teams ask “prove it.”

## 1. Core preflight gates (G1–G9)

| Gate | Package |
|------|---------|
| G1–G9 | `packages/preflight` (`GateId`, `PREFLIGHT_GATES`) |
| Host policy extras | Studio Validate may add sync/schema/validation policy gates — distinct from core G1–G9 |
| Constraint findings | Structured FK findings + optional **sample** orphan probe — block in strict/maximum when dest FK columns are unmapped or sample orphans are found unless `fk_risk_acknowledged`; **never invents population RI proof** (`proven` requires full population orphan scan with zero orphans) |

Do **not** claim “8 gates,” “ten gates,” or invent a marketed “G10 constraints” gate. Required core gates remain **G1–G9**.

## 2. PRODUCTION_SKU routes

Source of truth: `apps/api/src/transfer/registry.py` → `PRODUCTION_SKU`.

UI ledger: Workspace → Benchmarks → Integrity / PRODUCTION_SKU panel (reads API metrics).

Rule: if a route is not in `PRODUCTION_SKU`, it is not a committed migration claim.

### Offline pair assurance (datatype / DDL / coercion)

| Claim | Module | Proves | Does **not** prove |
|-------|--------|--------|--------------------|
| `pair_assurance_offline` | `apps/api/services/pair_assurance.py` | Every PRODUCTION_SKU **database→database** pair: type inventory × create-new DDL stamp × lossy/risk/coercion fail-closed × mapping contract × fixture transforms | Live transfer, checksum/Gate-8 population, FK/orphan RI, rollback, CDC exactly-once |
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
- **Map≡CREATE (binary typmod):** foreign `BINARY(n)`/`VARBINARY(n)`/`BYTES(n)`/`fixed(n)` rematerialize to dest wire (`BYTES`/`VARBYTE`/`BYTEA`/`fixed`/`RAW`/`BLOB`) — never illegal CREATE pass-through (`test_binary_typmod_materialize_map_create.py`)
- **Map≡CREATE (bare DECIMAL):** bare `DECIMAL`/`NUMERIC`/`NUMBER` rematerialize to `ddl_type` SSOT `(p,s)` so quarantine can parse capacity — never MySQL `(10,0)` invent or silent money truncate (`test_bare_decimal_materialize_map_create.py`); Snowflake bare uses SSOT `NUMBER(38,10)` not batch invent
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
