# Execution Engine Contract (Module 14)

## Promise

Execution is **deterministic and explainable**. Bad rows quarantine or the job fails — never silent drop. Delivery default is **at-least-once**. Exactly-once is **not claimed**.

## Delivery honesty

| Claim | Truth |
|-------|--------|
| At-least-once | Default |
| Exactly-once | **Not claimed** |
| Never duplicate | Convergent sinks (upsert/ledger/job claim) + refuse insert resume without checkpoint — not a global exactly-once guarantee |
| Never silent lose | Quarantine holdout or fail-closed |

## Resume vs Retry

| Action | Meaning |
|--------|---------|
| **Resume** | Continue from last committed checkpoint chunk |
| **Retry from start** | New attempt; not the same as Resume |
| Insert/append without durable progress | **Refused** (would duplicate) |
| Upsert/overwrite without progress | May restart from zero (convergent) |

## Fail-closed rules (Module 14)

1. Checkpoint `require_save` failure aborts the job
2. Kafka offset commit **after** durable checkpoint must abort — never swallow
3. No-op staging checkpoint: `durable=false`, `resume_supported=false`
4. Insert resume without progress → `ExecutionContractError`

## Capability matrix

See `execution_contract_dict()` / `capability_matrix()` in code. Summary:

- Checkpoint, Resume, Retry, Connection recovery, Streaming, Bulk, Partial failure (quarantine): **available** with documented non-guarantees
- Table isolation: sequential multi-stream (shared checkpoint)
- Transaction recovery: CDC buffer only — not XA bulk 2PC
- Exactly-once / transfer undo: **not available**

## Code SSOT

- `apps/api/services/execution_engine_contract.py`
- Wired: `engine` resume decision, `stream` Kafka offset commit, `_NoOpCheckpointService`, `recovery_honesty.honesty_dict`

## Related

- `docs/MIGRATION_ROLLBACK.md`
- `docs/QUARANTINE_DLQ_FAIL_CLOSED.md`
- `docs/BUYER_EVIDENCE_PACK.md` CDC delivery section
