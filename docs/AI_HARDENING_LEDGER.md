# AI Hardening Ledger

Track one item per run. Status: `NOT_STARTED` | `IN_PROGRESS` | `DONE_VERIFIED` | `PARTIAL` | `BLOCKED`.

| # | Item | Status | Proof command | Notes |
|---|------|--------|---------------|-------|
| 1 | LLM never in correctness path | **DONE_VERIFIED** | `cd apps/api && python -m pytest tests/test_item1_llm_never_correctness_path.py tests/test_llm_mapping.py -q` (17 passed) + CI `ai-offline-correctness` | See below |
| 2 | Ollama integration depth | NOT_STARTED | — | — |
| 3 | RAG quality | NOT_STARTED | — | — |
| 4 | Prompt injection via customer data | NOT_STARTED | — | — |
| 5 | Eval harness | NOT_STARTED | — | — |
| 6 | Chatbot UX | NOT_STARTED | — | — |
| 7 | Decompose tools.py | NOT_STARTED | — | — |

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
