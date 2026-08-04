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

### Constraint findings (host / Studio policy — not a numbered GateId)

`assess_constraint_compatibility(ctx) -> list[dict]` in `constraint_hints.py`
returns structured FK findings when destination foreign-key metadata is present.
Severity is `block` / `ack_required` / `info` based on validation mode and
`fk_risk_acknowledged`. Hosts attach findings as `constraint_findings` /
`constraint_hints` and must flip `passed=false` when
`constraint_findings_block_transfer(...)` is true. **Coverage is destination FK
metadata only — never invent population orphan / RI proof.**

```python
from preflight import PreflightEngine, PreflightContext, TransferPlan
from preflight import (
    assess_constraint_compatibility,
    constraint_findings_block_transfer,
    referential_integrity_posture,
)

engine = PreflightEngine(fail_fast=True)
result = engine.run(PreflightContext(plan=transfer_plan, sample_rows=samples))
if not result.passed:
    raise PreflightBlocked(result.blockers)
findings = assess_constraint_compatibility(ctx)
if constraint_findings_block_transfer(findings, validation_mode="strict"):
    raise PreflightBlocked("unmapped destination FK columns")
assert referential_integrity_posture(findings)["proven"] is False
```
