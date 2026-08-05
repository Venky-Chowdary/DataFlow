# Historical Success Contract (Module 17)

## Promise

`historical_success` on a mapping is **measured** or explicitly **unmeasured**.

Never invent `0.99` / greenwash when no load history exists.

## Evidence shape

| Field | Meaning |
|-------|---------|
| `measured` | True only when rate computed from real runs |
| `success_rate` | `(rows_written - rejected) / rows_written` or `null` |
| `runs_observed` | Usable loads with row counts |
| `scope` | `route_load_history` or `none` |
| `never_invented` | Always `true` |

## Honesty

- Route-scoped aggregates are stamped — **not** invented per-column rates
- Legacy bare float `historical_success` is discarded
- Zero / missing history → `measured=false`, `success_rate=null`

## Code SSOT

- `apps/api/services/historical_success_contract.py`
- Wired: Validate `proof_bundle.historical_success`, mapping evidence stamp

## Related

- `docs/MAPPING_ENGINE_CONTRACT.md`
- `apps/api/services/data_quality_history.py` (ring buffer source)
