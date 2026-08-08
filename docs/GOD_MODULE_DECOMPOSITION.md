# God-module decomposition (Phase F8)

## Policy

| Rule | Meaning |
|------|---------|
| Freeze | Listed modules cannot grow past `apps/api/module_size_budgets.json` |
| Facade | New call sites import stable surfaces (`decision_kernel`, `*_api`, `merge_registry`) |
| Extract then shrink | After moving code out, lower `max_lines` in the same PR |
| No parallel authorities | Extraction must not fork invent / MERGE / reconcile rules |

## Stable interfaces (today)

| Concern | Import |
|---------|--------|
| Type invent / width / lossy | `services.decision_kernel` / `services.decision_kernel.types` |
| Execute Decision Artifact | `services.decision_kernel.execute_gate` |
| Reconciliation checksums | `services.reconciliation_api` |
| Writer quarantine / LSN | `connectors.writer_common_api` |
| SQL MERGE strategy inventory | `connectors.merge_registry` |

## Extraction order (remaining)

1. `type_system` → `type_system/ddl.py`, `width.py`, `coercion.py` (kernel already facades)
2. `generic_sql` → `connectors/merge/<dialect>.py` (registry is the map)
3. `reconciliation` → per-engine `verify_*.py` modules
4. `writer_common` → `cdc_lsn.py`, `quarantine_wire.py`, `write_result.py`
5. `engine.py` / `TransferPage.tsx` → orchestration shells only

## CI

```bash
python apps/api/scripts/check_module_size_budgets.py
```

Artifact: `apps/api/data/proofs/module_size_budgets.json`

## ADR — budget bump 2026-08-08

| Module | Was | Now | Why |
|--------|-----|-----|-----|
| `services/reconciliation.py` | 5900 | 5920 | Gate-8 upsert keyed checksum + Mongo keyed fingerprint (product correctness) |
| `connectors/writer_common.py` | 5100 | 5120 | `gate8_writer_meta` / written_ids stamping for upsert reconcile |

## ADR — extract 2026-08-08 (wave4)

| Module | Change | Why |
|--------|--------|-----|
| `src/transfer/engine.py` | 5212 → ≤5200 | Extracted `reconcile_phase_heartbeat` → `src/transfer/reconcile_heartbeat.py` |

Next extraction must lower these ceilings after moving keyed-verify helpers into `reconciliation_api` / `writer_common_api`.
