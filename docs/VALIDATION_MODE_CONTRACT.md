# Validation Mode Contract (Module 7)

## Promise

Every validation mode states **guarantees**, **non-guarantees**, and **coverage**.
Sample validation never claims population proof in any mode.

## Modes

| Mode | Write | Confidence floor | Posture |
|------|-------|------------------|---------|
| `strict` | Yes | 0.85 | Hard-block fidelity unless Risk Contract continues |
| `maximum` | Yes | 0.95 | Stricter Strict (legacy Studio) |
| `balanced` | Yes | 0.75 | Approved risks unlock; Gate-8 may be sample-assured |
| `migration` | Yes | 0.75 | Warn recoverable; hard-block unrecoverable |
| `discovery` | **No** | 0.0 | Report-only — Execute refused |
| `audit` | **No** | 0.85 | Hard-block audit trail — Execute refused |

## Engine fail-closed

`assert_mode_allows_write` runs before destination mutation.
`discovery` / `audit` raise `ValidationModeWriteRefused`.

## Code SSOT

- `apps/api/services/validation_mode_contract.py`
- Stamped on Validate: `validation_mode_contract` in preflight JSON
- Studio: `apps/web/src/lib/transferConstants.ts` → `VALIDATION_MODES`

## Related

- `docs/VALIDATION_COVERAGE_CONTRACT.md` — sample ≠ population
- `docs/MIGRATION_RISK_CONTRACT.md` — approved risks
- `docs/BUYER_EVIDENCE_PACK.md` — diligence claims
