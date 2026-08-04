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

### Gate-8 sample cannot hide checksum mismatch

When whole-table checksums diverge and a key-aligned sample passes:

- `passed` may be `true` in balanced mode (job continues under sample assurance)
- `checksum_match` is **always** `false`
- `population_proof` is **always** `false`
- `coverage` / `assurance_level` = `sample`
- Message says **Sample-only assurance** and **NOT proven** — never “Row fidelity verified”

Strict mode still fails on checksum mismatch.

### FK / orphan fail-closed

When destination/source FK metadata exists but the sample orphan probe cannot run
(no connector, no sample, unsupported engine):

- Emit `fk_orphan_probe_unavailable` (block in strict/maximum unless FK risk acknowledged)
- Never silent soft-pass

Composite FKs emit `composite_fk_not_probed` instead of a silent skip.

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
- Composite FKs remain incomplete until tuple anti-join ships — incomplete scans leave `population_orphan_count=null` (never invent proven)
