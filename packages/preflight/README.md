# Preflight Gate Engine

Core engine gates run **before** any production data is written. If any gate
blocks, **zero rows are moved**. Host layers (Studio Validate) may append
additional policy gates (sync contract, schema policy, validation posture).

| Gate | Check |
|------|--------|
| G1 | Source readable / parseable |
| G2 | Destination reachable with write access |
| G3 | Schema contract — no lossy type coercion (schemaless dests SKIP, not green PASS) |
| G4 | Mapping confidence ≥ threshold; required fields mapped |
| G5 | Dry-run transform on sample rows |
| G6 | Target DDL compatible (blocks when destination not connected) |
| G7 | Staging capacity sufficient (unknown/missing byte estimate fails closed) |
| G8 | Pre-write sample reconciliation (requires Validate samples; post-write checksum runs after Execute) |
| G9 | Data integrity audit (unproven / not-configured audit fails closed) |

**Required core gates = G1–G9 only.** Do not add a `GateId` for optional extras.

### Soft constraint hints (host / Studio policy — not a gate)

`assess_constraint_compatibility(ctx) -> list[str]` in `constraint_hints.py`
returns informational FK / relational warnings when destination foreign-key
metadata is present. Hosts may attach the list as `constraint_hints` on the
Validate result. Hints never flip `passed` and must not be marketed as a
numbered gate.

```python
from preflight import PreflightEngine, PreflightContext, TransferPlan
from preflight import assess_constraint_compatibility

engine = PreflightEngine(fail_fast=True)
result = engine.run(PreflightContext(plan=transfer_plan, sample_rows=samples))
if not result.passed:
    raise PreflightBlocked(result.blockers)
hints = assess_constraint_compatibility(ctx)  # soft warnings only
```
