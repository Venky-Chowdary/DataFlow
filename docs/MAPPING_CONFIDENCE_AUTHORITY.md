# Mapping Confidence Authority (Module 3)

## Promise

**One threshold. One hard gate.**

`g4_mapping_confidence` is the sole hard authority for:

- confidence below Map floor
- ambiguous mappings requiring review
- (risk-ack / STRUCT paths remain G4 as well, but root-cause as fidelity when lossy)

## What must not happen

| Surface | Before Module 3 | After |
|---------|-----------------|-------|
| G4 gate | Blocks | Blocks (SSOT) |
| Proof bundle | Also blocked with "Semantic mapping confidence too low" | Reports `min_confidence` + `confidence_authority` only |
| G9 `mapping_confidence` check | `blocks_transfer: true` | Warnings only; `authority: g4_mapping_confidence` |

## Operator view

Root Cause Engine emits `kind: mapping_confidence` when G4 blocks on floor/ambiguous
review — not a second proof/G9 face.

## Guarantees

- Execute cannot unlock while G4 is blocked on confidence
- Proof pack still records min confidence for audit
- G9 integrity audit does not invent a parallel confidence lock

## Non-guarantees

- High confidence ≠ semantic correctness proven
- Sample Validate does not prove population uniqueness of mapped keys

## Code SSOT

- Gate: `packages/preflight/src/preflight/gates.py` → `gate_g4_mapping_confidence`
- Proof: `apps/api/services/preflight_proof_bundle.py` (`confidence_authority`)
- G9 report: `apps/api/services/data_integrity.py` → `_check_mapping_confidence`
- Root: `apps/api/services/root_cause_engine.py` → `mapping_confidence`
