# DataFlow Preflight & Data Governance Rulebook

This document is the source of truth for the rules that govern whether a transfer is approved, reviewed, or blocked in DataFlow. It is intended for product, engineering, and operations teams and is aligned with the Universal Data Transfer orchestration framework: canonical intermediate models, connector capability discovery, hard gates, soft gates, validation evidence, quarantine, and reversible transforms.

---

## 1. Purpose

DataFlow is a **universal any-to-any data transfer platform**. Before a transfer writes any data to the destination, the preflight engine checks the source, the destination, the schema mapping, the data values, and the configured policy. The goals are:

- **No data loss.** Never silently drop, truncate, or coerce values unless the user explicitly approves a lossy transform.
- **No guessing.** Every mapping has a confidence score, evidence, and a reversible/lossy flag.
- **Clear user guidance.** When a transfer cannot proceed, the user receives the exact reason and a concrete fix.
- **Risk posture transparency.** Hard gates block unsafe transfers; soft gates request review but still allow execution.

---

## 2. Gate Taxonomy

| Gate type | Definition | What it affects |
|---|---|---|
| **Hard gate** | A condition that must pass before a transfer can write data. If it fails, the transfer is blocked. | Data loss, connectivity, primary-key uniqueness, DDL incompatibility, write permissions, reconciliation mismatch. |
| **Soft gate** | A confidence or policy observation that should be reviewed but does not block execution. | Mapping confidence, schema drift warnings, capacity, PII/compliance risk, null-rate drift. |

---

## 3. Preflight Gate Rules

| Gate ID | Title | Category | Why it may block | What to do | Examples |
|---|---|---|---|---|---|
| **G1 Source** | Source connectivity | Hard | DataFlow cannot read the source. | Check host, port, credentials, network, and that the source file/table/collection exists. | PostgreSQL connection refused; S3 bucket not found; CSV parse error. |
| **G2 Destination** | Destination connectivity | Hard | DataFlow can read the source but cannot reach or write to the destination. | Check destination host, credentials, write permissions, and that the database/schema/bucket exists. | MongoDB auth failed; Snowflake warehouse suspended; bucket write denied. |
| **G3 Schema Contract** | Schema contract / type coercion | Hard | A source value cannot be stored in the target type without losing precision or failing. | Change the target column type to a wider or compatible type, or add a safe transform. For schemaless destinations (MongoDB, DynamoDB, Redis) this gate is relaxed because no DDL type contract is enforced. | VARCHAR `'abc'` → INTEGER is impossible; TIMESTAMP → DATE truncates time; DECIMAL → INTEGER drops fractional cents. |
| **G4 Mapping Confidence** | Mapping confidence | Soft | The AI mapper is not certain that a source column means the same thing as the target column. | Review the mapping panel, confirm or re-map the column, and approve. Improve column names or provide a sample. | `'amt'` mapped to `'amount'` with low confidence; `'created'` vs `'updated'`. |
| **G5 Dry Run / Integrity** | Transform dry-run / integrity | Hard | A sample row could not be transformed or violates an integrity rule. | Fix the source value, choose a less strict transform, or switch validation mode. For schemaless destinations values are stored as-is when possible. | `'2024-13-01'` cannot be parsed as a date; required identifier is null. |
| **G6 Target DDL** | Target DDL compatibility | Hard | The target table or collection cannot accept the data as mapped. | Allow DataFlow to create/alter the target, remove duplicate keys, widen a column, or map to a compatible existing column. | Duplicate `_id` values in MongoDB; VARCHAR(10) cannot fit a 50-character email; target column does not exist. |
| **G7 Capacity** | Capacity / staging | Soft | The local staging volume may not have enough free space. | Free disk space, reduce the batch size, or stream directly to destination. | A 100 GB export needs 300 GB free staging. |
| **G8 Reconciliation** | Reconciliation | Soft | Source and target row counts or key sets do not match after the transfer. | Investigate missing rows, duplicates, or filter differences. | 1000 source rows but 980 written. |
| **G9 Data Integrity** | Data integrity audit | Hard | The sample violates duplicate keys, required nulls, financial precision loss, or encoding anomalies. | Clean the source data, adjust the mapping, or switch validation mode to `balanced` for non-critical fields. | Primary key has duplicates; amount lost a decimal place; `user_id` is 60% null. |
| **G10 Schema Policy** | Schema policy | Soft | The selected schema policy is conservative or the target schema is unknown. | Switch to `propagate` or `auto_create` if the target is allowed to evolve. | Manual-review policy prevents auto-create. |
| **G11 Validation Posture** | Validation posture | Soft | The validation mode is strict and the user may want more leniency. | Change to `balanced` or `maximum` only if the transfer is non-critical and data quality is low. | Strict mode rejects moderate-confidence mappings. |
| **Proof Bundle** | Composite readiness / trust decision | Hard | The composite readiness score, semantic confidence, or reconciliation proof is below the threshold. | Review the proof panel, fix data-quality issues, approve mappings manually, or lower the threshold. | Semantic confidence 0.61 < configured threshold 0.75. |
| **Schema Drift** | Schema drift | Hard | The live source or destination schema no longer matches the saved mapping contract. | Re-run the mapping step, approve the new contract, or set the schema policy to propagate safe changes. | A new `deleted_at` column appeared; target column type changed from VARCHAR to INTEGER. |

---

## 4. Data Governance Rules

### 4.1 When a transfer is allowed

A transfer is **approved** when:
1. The source and destination are reachable (G1, G2 pass).
2. No data-loss coercion would occur for SQL destinations, or the destination is schemaless and the transform is reversible (G3 passes).
3. The dry-run sample transforms cleanly and no integrity rule is violated (G5, G9 pass).
4. The target can accept the data (G6 passes).
5. The proof-bundle readiness score and reconciliation check are acceptable (Proof Bundle passes).
6. Compliance risk is acceptable or the user approves it (soft gate).

A transfer is **reviewable** when a soft gate (G4, G7, G8, G10, G11) is below the threshold but no hard gate fails. The user can execute after review.

A transfer is **blocked** when any hard gate fails. The user must fix the root cause before re-running preflight.

### 4.2 When a transfer is blocked

- **Source or destination is not reachable.** No data can be read or written.
- **A required identifier is null or duplicated.** The target cannot guarantee row identity.
- **A value cannot be coerced safely.** VARCHAR `'abc'` → INTEGER, TIMESTAMP → DATE without approval, or DECIMAL → INTEGER with money values.
- **Target DDL is incompatible.** The target table already exists with conflicting types, a column is too narrow, or the primary key would be violated.
- **Reconciliation fails.** The number of rows or the key set in the target does not match the source.
- **PII/compliance risk is not acceptable and the user has not approved.** Email, phone, SSN, etc. in a restricted context.

### 4.3 Type coercion and data loss

- **Lossless transforms are preferred.** VARCHAR → VARCHAR, DECIMAL → DECIMAL, TIMESTAMP → TIMESTAMP, JSON → JSON.
- **Lossy transforms require explicit approval.** INTEGER → VARCHAR is safe; VARCHAR → INTEGER is only safe for numeric strings. TIMESTAMP → DATE truncates time. DECIMAL → INTEGER drops fractional values.
- **Schemaless destinations store the source type as-is.** MongoDB, DynamoDB, and Redis do not enforce a DDL type contract, so G3 is skipped. The user can still choose a typed target schema if needed.
- **Binary and JSON values are never silently flattened.** JSON is stored as JSON/JSONB/VARIANT depending on the target. Binary is preserved as base64 or bytes.

### 4.4 Nulls, duplicates, and keys

- **Primary keys must be unique and non-null.** For SQL, the primary key is the target key column. For schemaless destinations, only the real `_id` field is enforced as a primary key; other `*_id` columns are treated as normal foreign keys and may repeat.
- **High null rate is a warning, not a blocker, for optional fields.** A required identifier with a high null rate is a hard blocker.
- **Duplicate source rows are flagged but not blocked by default.** They are flagged in the data-integrity audit; the target will reject the second occurrence if a uniqueness constraint applies.

### 4.5 Quarantine and retry

- **Malformed records are quarantined, not dropped.** A record with an unparseable value is written to a quarantine location and reported, not silently lost.
- **Transient errors are retried with bounded exponential backoff and jitter.** Connection errors, rate limits, and timeouts are retriable. Constraint violations, parse errors, and data-type errors are not retriable.
- **A failed transfer is resumable.** Job state is persisted; the user can retry from the last successful checkpoint.

### 4.6 PII, compliance, and security

- **PII detection runs on every sample.** Email, phone, name, SSN, credit card, address, etc. are flagged.
- **Raw PII is masked in logs, telemetry, and validation samples.**
- **Transfers with PII require review in strict mode.** In balanced mode, the user can proceed after seeing the compliance risk.
- **Never log secrets or raw credentials.** Connector credentials are encrypted or tokenized.

---

## 5. Validation Modes

| Mode | Confidence threshold | Behavior | Use case |
|---|---|---|---|
| **Strict** | 0.85 | Hard block on low confidence, type coercion, and DDL issues. | Production, financial, compliance-sensitive data. |
| **Balanced** | 0.75 | Soft gate on confidence, allows most well-typed transfers, but still hard-blocks data loss and integrity failures. | Default for most users. |
| **Maximum** | 0.95 | Hard block on all but the highest-confidence mappings and cleanest data. | Medical, financial, regulatory use cases. |

The proof-bundle confidence floor is `max(0.55, threshold - 0.3)`. Mappings below the floor are too uncertain to proceed; mappings between the floor and the threshold are accepted for the data-integrity audit but flagged as low confidence.

---

## 6. Common Errors and User Guidance

### 6.1 "Semantic mapping confidence too low"

- **What it means:** The weakest automatic mapping is below the confidence threshold.
- **Fix:** Open the mapping panel, review each column, approve correct mappings, and re-map wrong ones. If the column names are intentionally unusual, lower the validation mode or confidence threshold.
- **When it is safe:** The values actually match the target concept; the low confidence is only because the column names are ambiguous.

### 6.2 "Dry-run / integrity failed"

- **What it means:** A sample row could not be transformed or violates a rule.
- **Fix:** Open the issue list, find the failing value, and either fix it in the source or change the target type. For MongoDB/DynamoDB/Redis, the value can usually be stored as-is.
- **Examples:** `'2024-13-01'` is not a valid date; `$1,000.00` needs a DECIMAL target; `null` in a required `user_id`.

### 6.3 "Target DDL incompatible"

- **What it means:** The target cannot accept the mapped data.
- **Fix:**
  - Allow DataFlow to create the table/collection.
  - Widen a VARCHAR column.
  - Remove duplicate primary keys in the source.
  - Map to an existing column with a compatible type.
- **Schemaless exception:** For MongoDB/DynamoDB/Redis, only `_id` uniqueness is enforced; other fields can hold any type.

### 6.4 "PII/compliance review required"

- **What it means:** The sample contains fields that look like PII.
- **Fix:** Mask, tokenize, or drop the PII fields, or approve the transfer after confirming your data governance policy allows it.

### 6.5 "Source schema changed" / "Destination schema changed"

- **What it means:** The schema no longer matches the saved mapping contract.
- **Fix:** Re-run the mapping step and approve the new contract, or set the schema policy to propagate safe changes.

---

## 7. Real-World Examples

### 7.1 CSV → Snowflake

- **Rule:** The CSV must be UTF-8, delimited, and parseable. `amount` becomes `DECIMAL(38,10)`, `quantity` becomes `INTEGER`, `is_gift` becomes `BOOLEAN`, `order_date` becomes `DATE`, `shipped_at` becomes `TIMESTAMP_TZ`, `metadata` becomes `VARIANT`, `notes` becomes `VARCHAR`.
- **Blockers:** Non-numeric `amount`, dates in multiple formats, mixed delimiters, null `order_id`.
- **Fix:** Normalize the CSV, set consistent date formats, and use `DECIMAL` for money.

### 7.2 JSON → MongoDB

- **Rule:** JSON objects are stored as documents. `_id` is the primary key; other `*_id` fields may repeat. Numbers, booleans, strings, and nested objects are preserved as native BSON types.
- **Blockers:** Duplicate `_id` values, missing `_id` when the user expects one.
- **Fix:** Remove duplicates or let MongoDB generate ObjectIds for missing `_id` values.

### 7.3 PostgreSQL → MySQL

- **Rule:** Tables and columns are mapped by name. Type coercion is checked: `VARCHAR` → `VARCHAR` is safe, `TIMESTAMP` → `DATETIME` is safe, `NUMERIC` → `DECIMAL` is safe, `INTEGER` → `INTEGER` is safe.
- **Blockers:** `TEXT` → `VARCHAR(255)` that overflows, `JSONB` → `TEXT` without approval, duplicate primary keys.
- **Fix:** Use `TEXT` for long strings, `JSON` for JSON, and `DECIMAL(38,10)` for money.

### 7.4 DynamoDB → S3/MinIO

- **Rule:** DynamoDB items are flattened. Nested maps become JSON strings, lists are preserved as JSON, numbers are kept as strings or coerced to DECIMAL, booleans stay booleans.
- **Blockers:** DynamoDB binary keys that cannot be serialized as base64.
- **Fix:** Use a base64 or string representation for binary keys.

---

## 8. Connector-Specific Rules

### 8.1 Schemaless destinations (MongoDB, DynamoDB, Redis)

- **No DDL type contract.** G3 schema-contract checks are skipped.
- **Only `_id` is enforced as a primary key.** Other `*_id` columns are not required to be unique.
- **Optional fields are allowed.** A column with 100% nulls does not block.
- **Values are stored as-is when no target type is specified.** The user can still request a typed target schema if the destination supports it.

### 8.2 SQL destinations (PostgreSQL, MySQL, Snowflake, BigQuery, etc.)

- **DDL type contract is enforced.** Lossy coercions are blocked or warned.
- **Primary keys and unique constraints are honored.** Duplicates and nulls in required keys are blocked.
- **Auto-create is allowed when the policy permits.** DataFlow can create the target table with the inferred schema.

### 8.3 File sources and destinations (CSV, JSON, JSONL, TSV, Parquet, Excel)

- **Encoding is detected.** UTF-8 is preferred; replacement characters raise a warning.
- **Delimiter is inferred for CSV/TSV.**
- **Headers are required for CSV/TSV unless the user provides a schema.**
- **Parquet and Excel preserve native types.** No type inference needed.
- **JSON/JSONL can be objects or arrays of objects.** Nested objects are preserved as JSON or flattened with a path separator.

---

## 9. Mapping and Semantic Intelligence

- **Mapping priority:**
  1. Exact name and alias matches.
  2. Type and constraint compatibility.
  3. Schema evolution or contract rules.
  4. Semantic similarity (synonyms, ontology, glossary).
  5. Lineage or historical mapping evidence.
  6. Value-profile cross-checks on samples.
- **Each mapping produces:** source path, target path, transform expression, confidence score, evidence, lossy flag, reversible flag, and fallback/quarantine rule.
- **Conflict resolution:** If two source fields map to one target, the higher-confidence candidate wins and the runner-up is reported. If one source field must fan out, a deterministic derivation rule is applied.
- **Unknown target schema:** DataFlow creates identity columns with the inferred source type when the target is unknown. The user can review and rename before execution.

---

## 10. Observability and Lineage

- **Every preflight and transfer emits structured events.** Run started, preflight completed, stage duration, reconciliation, quarantine, lineage, run completed, run failed.
- **Events are correlated by `run_id` and `job_id`.**
- **Validation evidence is preserved.** The user can see which gates passed, which failed, sample quality, and confidence.
- **Rejected/quarantined records are counted and surfaced.**

---

## 11. Maintenance

- This rulebook is a living document. As new connectors, data types, and compliance requirements are added, the rulebook and the backend `apps/api/services/preflight_rules.py` must be updated together.
- Backend implementation references: `apps/api/services/preflight_rules.py`, `apps/api/services/preflight_service.py`, `apps/api/services/validation_plan.py`, `packages/preflight/src/preflight/gates.py`, `apps/api/services/error_handling.py`, `apps/api/services/lineage_telemetry.py`.
