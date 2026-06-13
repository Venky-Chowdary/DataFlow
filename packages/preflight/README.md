# Preflight Gate Engine

Eight gates run **before** any production data is written. If any gate blocks, **zero rows are moved**.

| Gate | Check |
|------|--------|
| G1 | Source readable / parseable |
| G2 | Destination reachable with write access |
| G3 | Schema contract — no lossy type coercion |
| G4 | Mapping confidence ≥ threshold; required fields mapped |
| G5 | Dry-run transform on sample rows |
| G6 | Target DDL compatible (no UNIQUE violations) |
| G7 | Staging capacity sufficient |
| G8 | Post-transfer reconciliation (runs after transfer) |

```python
from preflight import PreflightEngine, PreflightContext, TransferPlan

engine = PreflightEngine(fail_fast=True)
result = engine.run(PreflightContext(plan=transfer_plan))
if not result.passed:
    raise PreflightBlocked(result.blockers)
```
