# Population Orphan Scan (Module 11)

## Promise

Referential integrity is **proven** only after a completed full-table population orphan scan reports **zero** orphans for every applicable FK.

Sample Validate never invents population RI.

## Opt-in

| Field | Default | Meaning |
|-------|---------|---------|
| `run_population_orphan_scan` | `false` | Expensive full-table anti-join; must be requested |

When false: `population_orphan_probe_ran=false`, `proven=false` (honest).

When true and complete with zero orphans: `proven=true`, coverage=`population_orphan_probe`.

## Completeness

| Situation | `complete` | `population_orphan_count` | `proven` |
|-----------|------------|---------------------------|----------|
| All single-column FKs scanned, 0 orphans | true | 0 | true |
| Orphans found | true | N>0 | false |
| Composite FK / scan error / missing table | false | `null` | false |
| Not requested | n/a | n/a | false |

## Code SSOT

- `apps/api/services/population_orphan_probe.py`
- Wired from `run_file_preflight(..., run_population_orphan_scan=)`
- API: `PreflightRequest.run_population_orphan_scan`
- Posture: `preflight.constraint_hints.referential_integrity_posture`

## Related

- `docs/VALIDATION_COVERAGE_CONTRACT.md`
- `docs/MIGRATION_RISK_CONTRACT.md` — FK risk ack when scan cannot run
