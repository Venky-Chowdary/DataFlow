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
| Type invent / width / materialize | `services.decision_kernel` / `services.decision_kernel.type_invent` |
| Lossy / precision collapse (until type_lossy) | `services.decision_kernel.types` → still facades `type_system` orchestrators |
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

## ADR — extract 2026-08-09 (C2 invent body)

| Module | Change | Why |
|--------|--------|-----|
| `services/type_system.py` | 8850 → 7450 | Invent/normalize/materialize/width bodies → `decision_kernel/type_invent.py`; shims remain |
| `src/transfer/engine.py` | 5286 → ≤5200 | Additive + transform mapping stamps → `mapping_write_stamp.py` |
| `services/preflight_service.py` | 2573 → ≤2500 | Policy-gate merge → `preflight_policy_gates.py` |

Next: `type_lossy.py` for `is_lossy_coercion` / `is_precision_collapse_coercion`; specialty helpers stay in `type_system` until C2c.

## ADR — extract 2026-08-10 (Gate-8 read side)

| Module | Change | Why |
|--------|--------|-----|
| `services/reconciliation.py` | 6339 → ≤4700 | Per-engine sample reads → `services/target_sample.py`; Oracle catalog identity / LOB comparison / read-back → `services/reconciliation_oracle.py`. Delegating entry points (`read_target_sample`, `verify_oracle_table`) stay in `reconciliation` |
| `services/target_sample.py` | new | One owner for "read an ordered, key-scoped sample back out of the destination" across engine families |

Oracle moved first because its object identity is not derivable from the typed
name (quoted vs folded are different tables): the read side must resolve the
stored spelling through `services/sql_object_identity.py`, the same resolver the
writer and introspection use.

## ADR — extract 2026-08-12 (create-new risk stamp)

| Module | Change | Why |
|--------|--------|-----|
| `services/semantic_mapper.py` | 2103 → 1922, budget 2100 → 1980 | Projected-carrier → physical-DDL risk stamping → `services/create_new_risk_stamp.py` |
| `services/create_new_risk_stamp.py` | new | One owner for "what does adopting the destination's physical type cost" |

The stamp was already written to avoid importing `mapping_pipeline` so the two
would not cycle; giving it a module states that boundary instead of relying on a
comment. `semantic_mapper` keeps a private alias, so its own two call sites and
the pipeline read unchanged.

## ADR — extract 2026-08-12 (streaming foreign-key carry)

| Module | Change | Why |
|--------|--------|-----|
| `src/transfer/stream.py` | 3418 → 3335, budget 3400 → 3350 | Source FK measurement, parents-first ordering, and post-load constraint carry → `src/transfer/stream_foreign_keys.py` |
| `src/transfer/stream_foreign_keys.py` | new | One owner for "referential constraints cannot be created alongside the rows" |
| `src/transfer/stream.py` | over budget again after the incremental fix | Reader-side row accounting for Gate-8 → `src/transfer/stream_row_accounting.py` |
| `src/transfer/stream_row_accounting.py` | new | One owner for "a source count of zero is a measurement, not an absence" |

`stream.py` had drifted 18 lines past its freeze. Foreign-key carry moved rather
than the budget rising: it is a self-contained concern with a single reason to
change (constraints are measured on the source, then re-added once every table
has landed), it was used nowhere outside `stream.py`, and it depends only on
endpoint resolution plus a lazy `services.foreign_key_orchestration` import.
`stream.py` keeps private aliases so the streaming call sites read unchanged.
