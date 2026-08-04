# Migration rollback strategies (honest)

Datawrap is a **Migration Assurance Workbench**. This document states what rollback
operators can rely on today — and what is **not** productized.

## What exists today

| Capability | Reality |
|------------|---------|
| Quarantine (not silent drop) | Bad cells/rows held out of the primary write path; remediations re-enter Validate |
| Mapping repair + re-Validate | Change Map transforms/risk ack → re-run G1–G9 before Execute |
| Checkpoint / resume | Fail-closed persistence; resume refuses unsafe age/write_mode combinations |
| CDC leases | Prevent concurrent consumers; delivery remains **at-least-once** |
| Iceberg / upsert writers (SKU routes) | Idempotent upserts where the destination contract allows PK/LSN guards |
| Gate-8 proof export | Portable HMAC packs for cutover evidence — not an undo button |

## What is not productized (do not claim)

- One-click **staging swap** / blue-green table rename orchestration
- Destination **snapshot restore** as a first-class Studio action
- Branch/undo for warehouse DDL after create-new
- Exactly-once CDC rewind / Qlik-class continuous replication undo
- Automatic financial reverse-ETL of already-committed business rows

## Operator runbook (cutover)

1. **Before Execute** — Validate must pass G1–G9; export Gate-8 sample plan + mapping proof.
2. **Prefer create-new or staging schema** — land into a non-production schema/table; Gate-8 reconcile against source samples.
3. **Cutover** — swap consumers (view, synonym, app config) only after proof review + risk ack.
4. **If Validate fails mid-flight** — quarantine + Map repair; do not force `skip_preflight`.
5. **If Execute fails after partial write** — use checkpoint resume only when safety evaluation allows; otherwise re-land into a clean staging target.
6. **If production already swapped** — restore from **your** warehouse backup / time-travel (Snowflake Time Travel, PG PITR, etc.). Datawrap does not replace DBA restore tooling.

## Related docs

- `docs/PRODUCT_SCOPE.md` — what the product is / is not
- `docs/BUYER_EVIDENCE_PACK.md` — diligence artifacts
- `docs/ops/` — custom domain, CDC leases, tip-anchor honesty
