# Quarantine Row Contract (Module 9)

## Promise

Every rejected / quarantined row is a **first-class recovery artifact**.

Charter fields (always stamped):

| Field | Meaning |
|-------|---------|
| `original_value` | Value that failed |
| `expected_type` | Destination / expected type |
| `actual_type` | Source / observed type |
| `failure_reason` | Why it was held out |
| `transform_attempted` | Transform id that ran (or `none`) |
| `recovery_suggestion` | Operator next step |
| `source_pk` | Source identity when known (`source_pk_proven`) |
| `destination_pk` | Dest identity when known |
| `job_id` | Owning job (required for durable DLQ) |
| `connector` | Destination / path label |
| `retry_status` | `open` \| `pending_replay` \| `promoted` \| `replay_failed` \| `abandoned` |

## Honesty

- **Never invent** primary keys — stamp `*_pk_proven=false` when unknown
- Durable `persist_rejected_rows` **fails closed** without `job_id`
- Legacy keys (`reason`, `values`, `source_values`) remain for replay compatibility

## Code SSOT

- `apps/api/services/quarantine_row_contract.py`
- Wired from `append_write_quarantine_detail`, `persist_rejected_rows`,
  `quarantine_rows_from_preflight`

## Related

- `docs/QUARANTINE_DLQ_FAIL_CLOSED.md` — DLQ durability
- `docs/MIGRATION_RISK_CONTRACT.md` — QUARANTINE_ROW execution policy
