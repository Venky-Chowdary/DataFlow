# AI and preflight gate policy

**AI never decides G1–G9 pass/fail.**

Pilot, MCP, RAG, and `/suggest/*` endpoints may **suggest** mappings or transforms.
Deterministic engines own gate decisions:

| Gate | Decision owner |
|------|----------------|
| G1–G3, G5–G7 | Preflight adapters + probes |
| G4 | Mapping confidence / risk_ack / `requires_review` (operator) |
| G8 | Reconciliation checksums / sample plans |
| G9 | Data integrity audit (encoding, uniqueness, precision, …) |

## Invented transforms

When an LLM proposes a transform that differs from the deterministic baseline,
the pipeline:

1. Does **not** auto-apply the invented transform
2. Sets `requires_review=True` and `llm_invented_transform=True`
3. Surfaces `suggested_transform` for human accept on Map

`/ai/suggest/transforms` always returns `auto_apply=false` and
`requires_human_accept=true`.

## Env controls

- `LLM_ENABLED` — global off switch
- `PII_MASKING` — required on for any LLM sample path

See `apps/api/services/llm_policy.py` (`ai_may_decide_preflight_gate`).
