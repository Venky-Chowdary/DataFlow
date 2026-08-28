# Validation Coverage Contract (Module 4)

## Promise

**Sample validation never claims population correctness.**

Every validation surface stamps an explicit coverage contract:

| Field | Meaning |
|-------|---------|
| `layer` | `schema` \| `sample` \| `population` \| `execution` \| `post_write` |
| `population_proof` | `true` only when `layer=population` and the probe completed clean |
| `guarantees` / `non_guarantees` | What was and was not proven |
| `rows_examined` / `estimated_population` | Explainability counts when known |

## P0 fixes in this module

### Gate-8 checksum mismatch always fails (Enterprise GA)

When whole-table checksums diverge (both digests present):

- `passed` is **always** `false` — sample success cannot override
- `checksum_match` is **always** `false`
- `population_proof` is **always** `false`
- `assurance_level` = `none` (failed)
- Key-aligned sample may attach as **diagnostic only** (`sample_compare` retained)
- Message states checksum mismatch and that sample cannot override

### FK / orphan fail-closed

When destination/source FK metadata exists but the sample orphan probe cannot run
(no connector, no sample, unsupported engine):

- Emit `fk_orphan_probe_unavailable` (block in strict/maximum unless FK risk acknowledged)
- Never silent soft-pass

Composite FKs are scanned as MATCH SIMPLE tuples (same algorithm as destination
post-write RI). `composite_fk_not_probed` remains only when child/parent column
counts do not pair — never a silent skip.

### G9 integrity

`run_integrity_audit` returns `validation_coverage` with `layer=sample` and
`population_proof=false`.

## Code SSOT

- `apps/api/services/validation_coverage.py`
- `apps/api/services/reconciliation.py` (`stamp_post_write_phase`, `reconcile`)
- `apps/api/services/sample_orphan_probe.py`
- `apps/api/services/population_orphan_probe.py` (Module 11 — opt-in RI proven)
- `apps/api/services/data_integrity.py`

## Guarantees

- Operators can distinguish schema / sample / full_checksum / population coverage
- Checksum mismatch is never invisible behind a green sample pass

## Non-guarantees

- Sample orphan probe ≠ population RI
- Full checksum match ≠ FK / constraint proof
- Population orphan scan is **opt-in** (`run_population_orphan_scan=true`) — default Validate leaves `population_orphan_probe_ran=false` and `proven=false`
- Composite FKs use MATCH SIMPLE tuple anti-join / tuple-IN (`services.fk_tuple_scan`). A failed scan or arity mismatch still leaves `population_orphan_count=null` (never invent proven)
- Self-referential FKs alias the parent (`df_fk_parent`) so the anti-join is not `FROM emp JOIN emp` (ambiguous columns). Dest post-write RI uses the same helper.
