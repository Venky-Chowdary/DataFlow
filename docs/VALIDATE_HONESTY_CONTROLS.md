# Validate Honesty Controls (Module 16)

## Promise

Validate UI never lets operators confuse:

- Sample orphan probe with **RI proven**
- Execute-ready with **migration_proven**
- Silent invent with **lossless** ConversionClass

## Controls

| Control | Default | Meaning |
|---------|---------|---------|
| Population orphan scan checkbox | Off | Sets `run_population_orphan_scan=true` on next Validate — only path to RI `proven` |
| Coverage honesty panel | Always | Shows RI headline, ConversionClass summary, DDL identity hash |
| Decision path | Module 10 | Root Cause → … → Execute |

## Honesty copy

- Sample Validate never claims population correctness
- RI proven requires opt-in population orphan scan with zero orphans
- Execute-ready is not migration_proven

## Code SSOT

- `apps/web/src/lib/validateHonestyControls.ts`
- Wired: `ValidateDashboard`, `TransferPage` → `runPreflight`

## Related

- `docs/POPULATION_ORPHAN_SCAN.md`
- `docs/CONVERSION_CONTRACT.md`
- `docs/VALIDATE_DECISION_PATH.md`
- `docs/PROOF_POST_WRITE_CONTRACT.md`
