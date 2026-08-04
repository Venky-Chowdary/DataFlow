# Migration Risk Contract

## Promise

Accepting a fidelity risk is not a boolean checkbox. It is an immutable, signed
**Migration Risk Contract** that records what may be lost and what the writer
must do.

`risk_acknowledged: true` alone **does not** unlock Execute-approve.

## Default policy

`FAIL_JOB`

Never silently continue. A continue policy must be chosen explicitly.

## Continue policies (may unlock Validate → Execute)

| Policy | Meaning |
|--------|---------|
| `CAST_AND_CONTINUE` | Lossy cast at write; document precision/null expectations |
| `TRANSFORM_AND_CONTINUE` | Named transform owns the conversion |
| `QUARANTINE_ROW` | Rejecting cells go to quarantine / DLQ; job may continue |
| `SKIP_ROW` | Drop the row from the primary write (must be audited) |
| `RETRY` | Transient retry only — not a fidelity escape |

## Fail-closed policies (awareness only)

| Policy | Meaning |
|--------|---------|
| `FAIL_JOB` | Default — job fails if this path would lose fidelity |
| `STOP_TABLE` | Stop the current table/stream |
| `ABORT_TRANSACTION` | Abort the active transaction when available |

## Contract fields

- Risk ID, severity, root cause
- Column, source type, destination type, transform
- Rows sampled, estimated rows, expected failure %
- Expected precision loss / truncation / nulls
- Execution / quarantine / retry / rollback policies
- Approved by, timestamp, reason
- Signature (`mrc-sha256:…`), proof-pack reference, mapping hash

## Map → Validate → Execute

1. Map **Accept · cast & continue** emits a draft contract with
   `execution_policy=CAST_AND_CONTINUE` (explicit — not the default).
2. Validate hydrates/signs the draft (`services.migration_risk_contract`).
3. Proof bundle refuses `decision=approve` when lossy mappings lack a
   **verified continue-policy** contract.
4. **Writers honor the contract** (`build_mapped_rows_with_details` +
   `reject_on_strict_policy`):
   - `FAIL_JOB` / `STOP_TABLE` / `ABORT_TRANSACTION` → abort partial write
     even when the job error_policy is `quarantine`
   - `CAST_AND_CONTINUE` / `TRANSFORM_AND_CONTINUE` → cast failures follow
     `quarantine_policy` (default: row holdout to DLQ; never invent NULL)
   - `QUARANTINE_ROW` / `SKIP_ROW` → hold out bad rows; batch may continue
   - Rejected cells stamp `execution_policy` + `risk_id` for audit

## Honesty

- Rollback remains **not productized** (`docs/MIGRATION_ROLLBACK.md`).
- Sample rows on the contract do not prove population outcomes.
- Signature is a tamper-evident digest, not a PKI certificate.
- Fit/type quarantine matrix (`apply_write_quarantine_matrix`) still uses
  job-level policy for post-transform DECIMAL/width failures — transform-path
  contract enforcement is Module 1b; matrix per-column contracts are 1c.

## Code SSOT

- `apps/api/services/migration_risk_contract.py`
- Proof enforcement: `apps/api/services/preflight_proof_bundle.py`
- Write enforcement: `apps/api/connectors/writer_common.py`
- Map emit: `apps/web/src/lib/mapping.ts` → `acknowledgeMappingRisk`
