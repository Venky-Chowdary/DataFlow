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

`lenient` (offered by Pilot/MCP) is its own mode with the balanced confidence floor; it is not an alias of `balanced`. Any other unknown mode
fails closed to `strict`; `g11_validation_posture` names the requested and the
applied mode and its confidence threshold.

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


## Per-mode cell holdout (one policy: `validation_mode_contract.mode_holdout_kind`)

| Mode | File / object-store source | Database source |
|---|---|---|
| strict / maximum | holds nothing out; a refused cell blocks Validate, and Execute refuses a partial write | same |
| balanced | holds out numeric cast refusals (`Invalid integer` / `Invalid decimal`, QA R4-D02) while at least half the sampled cells cast (`CAST_SHARE_RULE`), and dates the read cannot pin to one real day — an invalid calendar day (QA browser 3) or a slash date that is a different day under DMY and MDY in an ISO column (QA R5) — below one in five sampled values | holds nothing out |
| lenient | holds out every per-cell cast refusal while at least half the sampled cells cast (`CAST_SHARE_RULE`) | same |

Known value change: PostgreSQL `varchar(n)` / `char(n)` silently trims trailing
spaces beyond `n` on assignment (SQL standard); such a cell is stored without
those spaces rather than refused.
