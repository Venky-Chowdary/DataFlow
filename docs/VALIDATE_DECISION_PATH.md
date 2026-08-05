# Validate Decision Path (Module 10)

## Promise

Validate does not dump duplicate gate failures as the primary story.

Operators follow one ordered path:

1. **Root Cause**
2. **Affected Gates**
3. **Business Impact**
4. **Recommended Actions**
5. **Preview Changes**
6. **Risk Contract**
7. **Execute**

One root cause may impact many gates. Those gates are evidence — not separate problems.

## Honesty

| Claim | Truth |
|-------|--------|
| Execute unlocked | Gates + decision + required Risk Contracts clear under the active mode |
| `migration_proven` | **Only** post-write Gate-8 `full_checksum` (Module 8) |
| Sample Validate | Never claims population proof |
| Decision path | Presenter over engine `root_causes` / display blockers — does not change gate outcomes |

## Code SSOT

- `apps/web/src/lib/validateDecisionPath.ts` — path builder
- `apps/web/src/components/transfer/ValidateDashboard.tsx` — UI surface
- Engine: `apps/api/services/root_cause_engine.py` + Risk Contract Module 1

## Related

- `docs/MIGRATION_RISK_CONTRACT.md`
- `docs/PROOF_POST_WRITE_CONTRACT.md`
- `docs/VALIDATION_COVERAGE_CONTRACT.md`
- `docs/MAPPING_CONFIDENCE_AUTHORITY.md`
