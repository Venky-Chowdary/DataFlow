# AI Hardening Ledger

Track one item per run. Status: `NOT_STARTED` | `IN_PROGRESS` | `DONE_VERIFIED` | `PARTIAL` | `BLOCKED`.

| # | Item | Status | Proof command | Notes |
|---|------|--------|---------------|-------|
| 1 | LLM never in correctness path | **DONE_VERIFIED** | `cd apps/api && python -m pytest tests/test_item1_llm_never_correctness_path.py tests/test_llm_mapping.py -q` + CI `ai-offline-correctness` | Unchanged by N2 — suggestions only |
| 2 | Ollama integration depth | NOT_STARTED | — | — |
| 3 | RAG quality | NOT_STARTED | — | — |
| 4 | Prompt injection via customer data | PARTIAL | N2 metadata-only withholds cells from mapper prompts (see item 8). Broader injection (instructions in column names, RAG corpus) is still open | Do not close ITEM 4 on N2 |
| 5 | Eval harness | NOT_STARTED | — | — |
| 6 | Chatbot UX | NOT_STARTED | — | — |
| 7 | Decompose tools.py | NOT_STARTED | — | — |
| 8 | AI egress manifest + metadata-only mapper (N2) | **DONE_VERIFIED** | `cd apps/api && python -m pytest tests/test_ai_egress_n2.py tests/test_llm_mapping.py tests/test_item1_llm_never_correctness_path.py tests/test_signed_proof_pack.py tests/test_evidence_chain.py -q` | Canonical `services/ai_egress.py`. Default on. No cloud LLM key — live OpenAI/Anthropic not claimed |

---

## ITEM 1 — DONE_VERIFIED (2026-08-09)

### Defect
`refine_mappings_with_llm` merged LLM suggestions **over** the deterministic
baseline (`pick = {**base, **llm}`), so a cloud/local model could change
source→target and suppress type-driven transforms — putting the LLM in the
Map→Execute correctness path.

### Fix
1. Baseline target/transform remain the Execute authority.
2. LLM remaps attach as `suggested_target` / `engine_suggestion` with
   `requires_review`; `meta.llm_decides = False`.
3. LLM-only invents (no baseline row) are ignored.
4. Held LLM transforms no longer force Execute `transform=none` (deterministic
   integer/decimal stamps still apply).
5. `is_llm_enabled()` honors bare `LLM_ENABLED` for offline CI.

### Proof output

```
pytest tests/test_item1_llm_never_correctness_path.py tests/test_llm_mapping.py -q
17 passed in 9.88s

- refine_adversarial_llm_cannot_change_decision_targets
- pipeline_llm_on_vs_off_identical_decisions (decision SHA-256 identical)
- execute_llm_disabled_vs_adversarial_identical_checksum (SQLite table SHA-256)
- offline_env_forces_llm_policy_off
```

### CI
Job `ai-offline-correctness` in `.github/workflows/ci.yml`: no API keys,
`LLM_ENABLED=false`, network-free Map/LLM suite.

### Self-check
- Can the LLM change any data-fidelity decision? **NO**
- Suite passes with no network / no API keys? **YES** (this job)
- LLM output used without validation against ground truth? **NO** for decisions
  (suggestions only; ITEM 4 will harden prompt injection separately)
- Measured? **YES** — identical decision fingerprints + checksums

---

## ITEM 8 — N2 AI egress + metadata-only (2026-09-01)

### Capability
Security review asked what left the customer boundary toward a model, and
whether the mapper ever saw cell values. N3's chain existed; there was no
enforced mode and no per-job record.

### Fix
1. Canonical owner `apps/api/services/ai_egress.py`.
2. Metadata-only **defaults on**. Mapper prompts carry names, types, aggregate
   profiles — never cells. Residual cells after strip → refusal stub.
3. Every `Provider.generate` / `generate_agent` records `action=ai.egress`
   with `payload_sha256` of the bytes handed to the provider. Never stores
   cells. Cloud → `crossed_customer_boundary=true`.
4. ITEM 1 unchanged (`llm_decides` remains false).

### Honesty
No cloud LLM key in this environment. Independent reread is a file-backed
audit JSONL via `manifests_for_job`, not `last_manifest()`. Opt-out
`DATAWRAP_AI_METADATA_ONLY=false` is not the regulated default.

