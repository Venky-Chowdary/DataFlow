# Root Cause Engine

## Promise

Validation exists to make every migration decision **explainable**.

Never list the same TEXT→INTEGER failure as three unrelated blockers
(Schema contract + Mapping confidence + Data integrity). Emit **one root cause**
that names every impacted gate.

## Shape

Each `root_causes[]` entry includes:

| Field | Purpose |
|-------|---------|
| `root_id` / `kind` / `title` / `summary` | Identity |
| `business_impact` | Why Execute is locked |
| `affected_columns` | Columns |
| `affected_rows_sample` / `estimated_total_rows` | Coverage honesty |
| `risk_level` | Severity |
| `recommended_fix` / `alternative_fixes` | What to do |
| `recovery_strategy` | How to recover |
| `expected_runtime_impact` | Cost of the fix |
| `quarantine_policy` / `rollback_policy` | Holdout / undo honesty |
| `documentation` | Doc pointer |
| `impacted_gates` / `absorbed_blocker_ids` | Which gate checks collapsed |

## Audit vs operator view

- **`gates[]`** — unchanged. Every gate still ran; status remains auditable.
- **`blockers[]`** — operator-facing. Absorbed gate blockers are replaced by a
  single root-cause blocker (`details.root_cause = true`).
- **`root_causes[]`** — SSOT for Validate UI and proof packs.

## Kinds (Module 2)

| Kind | Collapses |
|------|-----------|
| `fidelity_collapse` | G3 / G4(risk) / G9 lossy type paths |
| `mapping_confidence` | G4 confidence floor / ambiguous review (Module 3) |
| `duplicate_identity` | Identity / uniqueness multi-gate failures |

## Code SSOT

- `apps/api/services/root_cause_engine.py`
- Wired from `run_file_preflight` / `apply_policy_gates`
- UI: `apps/web/src/lib/validateIssueGrouping.ts` prefers `root_causes` when present

## Related

- Risk contracts: `docs/MIGRATION_RISK_CONTRACT.md`
- Rollback honesty: `docs/MIGRATION_ROLLBACK.md`
