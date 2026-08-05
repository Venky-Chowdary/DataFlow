# Migration Risk Contract

## Promise

Accepting a fidelity risk is not a boolean checkbox. It is an immutable, signed
**Migration Risk Contract** that records what may be lost and what the writer
must do.

`risk_acknowledged: true` alone **does not** clear G3/G4/G6 (DDL) / type
coercion severity and **does not** unlock Execute-approve. Only a verified
continue-policy Risk Contract clears lossy gates.

## Default policy

`FAIL_JOB`

Never silently continue. A continue policy must be chosen explicitly.

## Continue policies (may unlock Validate → Execute)

Runtime SSOT: `execution_policy_semantics()` / `resolve_write_action_for_mapping`.

| Policy | Write action | Behavior |
|--------|--------------|----------|
| `CAST_AND_CONTINUE` | `quarantine` (or `coerce_null`) | Cast failure → row holdout; `disposition=cast_failure` |
| `TRANSFORM_AND_CONTINUE` | `quarantine` (or `coerce_null`) | Transform failure → row holdout; `disposition=transform_failure` |
| `QUARANTINE_ROW` | `quarantine` | Entire row held in DLQ for replay; `quarantine_required=true` |
| `SKIP_ROW` | `skip_row` | Row dropped from primary; audit skip (`disposition=skipped`), not replay-quarantine |
| `STOP_COLUMN` | `stop_column` | Failing column omitted; **other columns on the row still write**; job continues |

## Fail-closed policies (do not unlock Validate; abort write unit)

| Policy | Write action | Behavior |
|--------|--------------|----------|
| `FAIL_JOB` | `fail` | Default — abort job write (`stop_scope=job`) |
| `STOP_TABLE` | `stop_table` | Abort current table/stream write unit (`stop_scope=table`) |
| `ABORT_TRANSACTION` | `abort_transaction` | Request txn abort when sink supports it; otherwise fail-closed with `transaction_available=false` |
| `RETRY` | `retry_then_fail` | **Exactly one** `apply_transform` re-attempt, then abort (never silent quarantine) |

## Contract fields

- Risk ID, severity, root cause
- Migration ID, table, column
- Source type, destination type, transform
- Loss classification (precision_loss / truncation / cast / mutate / …)
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
   - `FAIL_JOB` / `STOP_TABLE` / `ABORT_TRANSACTION` / exhausted `RETRY` →
     abort partial write even when the job error_policy is `quarantine`
   - `STOP_COLUMN` → omit failing column; write remaining columns (no job abort)
   - `CAST_AND_CONTINUE` / `TRANSFORM_AND_CONTINUE` → follow `quarantine_policy`
     (default row holdout; never invent NULL unless policy asks coerce)
   - `QUARANTINE_ROW` → hold out row for DLQ replay; `SKIP_ROW` → drop with
     skip audit (not replay quarantine)
   - Rejected cells stamp `execution_policy`, `disposition`, `risk_id`, `stop_scope`

## Honesty

- Rollback remains **staging discard only** when planned (`DISCARD_STAGING`);
  warehouse snapshot/swap/Iceberg branch restore are **not** executed
  (`docs/MIGRATION_ROLLBACK.md`).
- Sample rows on the contract do not prove population outcomes.
- Signature is a tamper-evident digest, not a PKI certificate.
- `ABORT_TRANSACTION` does not invent a transaction on sinks without one.
- Quarantine DLQ is fail-closed durable; it is **not** destination 2PC.

## Code SSOT

- `apps/api/services/migration_risk_contract.py`
- Proof enforcement: `apps/api/services/preflight_proof_bundle.py`
- Write enforcement: `apps/api/connectors/writer_common.py`
- Map emit: `apps/web/src/lib/mapping.ts` → `acknowledgeMappingRisk`
