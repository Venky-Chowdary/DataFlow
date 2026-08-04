# Mapping Engine Contract (Module 13)

## Promise

Operator-locked mappings are **never silently overwritten** by auto-map or LLM.

Every mapping stamps charter evidence fields (or honest nulls — never invented science).

## Operator lock

A mapping is locked when any of:

- `user_override`
- `risk_acknowledged` / risk contract
- `approved` / `operator_approved`
- `intentional_omit`

Locked rows keep target / type / transform. Engine alternatives attach as `engine_suggestion` with `suppressed=true`.

## Evidence fields

| Field | Meaning |
|-------|---------|
| `confidence` | Score when known |
| `semantic_evidence` | Method / tokens / semantic score |
| `lexical_evidence` | BM25 / name similarity / strategy |
| `datatype_compatibility` | Types + conversion_class / fidelity |
| `constraint_compatibility` | PK/FK notes when known |
| `historical_success` | Only when measured — never invented |
| `ai_explanation` | Reasoning string |
| `user_overrides` | Lock posture |
| `version_history` | Append-only revision crumbs when version supplied |

## Code SSOT

- `apps/api/services/mapping_engine_contract.py`
- Wired: `llm_mapping.refine_mappings_with_llm`, `mapping_pipeline.run_mapping_pipeline(prior_mappings=…)`, `POST /transfer/map`

## Related

- Plan revisions: `transfer_plan_store.PlanRevision`
- Conversion classes: `docs/CONVERSION_CONTRACT.md`
