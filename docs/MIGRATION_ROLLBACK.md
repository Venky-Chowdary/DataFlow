# Migration rollback strategies (Module 6)

DataWrap is a **Migration Assurance Platform**. This document states what rollback
operators can rely on — and what is **not** claimed.

## Productized workflow

Every job receives a signed **rollback plan** (`destination_summary.rollback_plan`):

| Strategy | Executable | What it does |
|----------|------------|--------------|
| `DOCUMENT_ONLY` | No | Audit posture + runbook (default for append/incremental) |
| `DISCARD_STAGING` | **Yes** | Drops `{table}_df_staging` only — never mutates primary |
| `REQUIRE_WAREHOUSE_RESTORE` | No | Overwrite landed on primary — DBA time-travel / PITR required |

API SSOT: `services.migration_rollback` (`plan_rollback`, `execute_rollback`).

## Guarantees

- Rollback plan is immutable (HMAC signature) — tampered plans refuse execution
- `DISCARD_STAGING` never touches the primary table
- `population_undo_claimed` is always `false`
- Execution requires `approved_by` + `reason` (audit)

## Non-guarantees / not productized

- One-click **transfer undo** of committed production rows
- Destination **snapshot restore** as a Studio action
- Blue-green / synonym **staging swap**
- Branch/undo for warehouse DDL after create-new
- Exactly-once CDC rewind

## Operator runbook (cutover)

1. **Before Execute** — Validate must pass; export Gate-8 + mapping proof.
2. **Prefer staging** — land into `{table}_df_staging` / non-prod schema; Gate-8 reconcile.
3. **If promote blocked** — primary untouched; execute `DISCARD_STAGING` or re-Validate.
4. **Cutover** — swap consumers only after proof review + risk contract.
5. **If Execute fails after partial primary write** — checkpoint resume only when safe; else re-land staging.
6. **If production already swapped** — restore from **your** warehouse backup / time-travel. DataWrap does not replace DBA restore tooling.

## Related

- `services/recovery_honesty.py` — machine-readable claims (`staging_discard` available; `transfer_undo` not)
- `docs/MIGRATION_RISK_CONTRACT.md` — default `rollback_strategy=DOCUMENT_ONLY`
- `docs/QUARANTINE_DLQ_FAIL_CLOSED.md` — rejected rows must be durable
