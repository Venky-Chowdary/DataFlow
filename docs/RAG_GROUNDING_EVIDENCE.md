# Chatbot / RAG grounding evidence

What an operator asks Pilot must be answered from something we can name: the shipped
product documentation, or a live read of their workspace. A provider returning HTTP 200
is not evidence. This is the measured behaviour of both answer surfaces
(`POST /api/v1/ai/rag/query`, `POST /api/v1/copilot/chat`) under every provider state.

## Path

```
question
  → lexical/BM25 retrieval over the shipped help corpus (58 chunks, generated from
    apps/web/src/lib/helpDocs.ts)
  → grounding floor (no passage over the floor ⇒ refuse, do not guess)
  → optional LLM narration, constrained to the retrieved passages
  → evidence-retention check (src/ai/rag/evidence.py)
  → fall back to the exact documentation / local answer when the check fails
```

`services.semantic_mapper.map_columns` remains the only mapping authority. RAG and Pilot
explain; they never remap a column or change a fidelity decision.

## Provider matrix — measured

Harness: `rag_matrix.py` (5 provider states × 4 questions × 2 endpoints = 40 calls), with a
local OpenAI-compatible stub for the provider behaviours. Reported values are the API's own
`method`, `confidence`, `grounded` and citation count.

| provider state | documented question | unsupported question ("how do I cook rice") |
| --- | --- | --- |
| no provider configured | `product_doc` / `pilot_local_engine`, grounded, 3–4 citations | `ungrounded` conf 0.10 / conf 0.20, 0 citations |
| provider ignores the prompt | `product_doc` / `pilot_local_engine`, grounded, 3–4 citations | `ungrounded` conf 0.10 / conf 0.20, 0 citations |
| provider narrates the context | `product_doc_llm` / `openai_polish`, grounded, 3–4 citations | `ungrounded` conf 0.10 / conf 0.20, 0 citations |
| provider returns an error | `product_doc` / `pilot_local_engine`, grounded, 3–4 citations | `ungrounded` conf 0.10 / conf 0.20, 0 citations |
| invalid credentials | `product_doc` / `pilot_local_engine`, grounded, 3–4 citations | `ungrounded` conf 0.10 / conf 0.20, 0 citations |

Reading of that table:

- A configured provider changes only the *prose* (`product_doc` → `product_doc_llm`,
  `pilot_local_engine` → `openai_polish`). Citations, grounded state and confidence are
  unchanged, because narration adds no evidence.
- Every provider failure mode (down, erroring, unauthorised, ignoring the prompt) degrades to
  the deterministic documented answer rather than to silence or to invented prose.
- An unsupported question is refused in all five states. It never receives a narrated answer,
  a citation, or a confidence above 0.2.

## Guards that produce that behaviour

| guard | file | what it rejects |
| --- | --- | --- |
| retrieval grounding floor | `src/ai/rag/product_docs.py` | passages that do not actually match the question's terms |
| product-subject check | `src/ai/rag/lexical_index.py` (`names_product_subject`) | "how do I …" phrasing about a subject the corpus never covers |
| narration evidence retention | `src/ai/rag/evidence.py` (`retains_evidence`) | prose that stopped talking about the retrieved passages |
| polish fact retention | `src/ai/rag/evidence.py` (`keeps_draft_facts`) | a rewrite that dropped the draft's counts, IDs or subject |
| narration gate | `src/ai/copilot/pilot_agent.py` (`carries_evidence`) | handing a refusal or uncited prose to a provider to rewrite |
| structured-provider exclusion | `src/ai/llm/provider.py` (`speaks_prose`) | the local column-analysis provider being used for narration |
| confidence ceiling | `src/ai/copilot/pilot_agent.py` (`_evidence_ceiling`) | uncited product prose reported at the confidence of a live count |

`grounded` is now returned by `/api/v1/copilot/chat` as well, computed from the same
`carries_evidence` helper the narration gate uses, so the UI cannot show a "grounded" answer
that has neither a citation nor a live read behind it.

## An answer is only proof if the operator can read it

Two defects found in the recorded browser run — the grounded answer was correct in the API
response and still useless on screen:

| Defect | Cause | Fix |
| --- | --- | --- |
| "What is append mode?" answered, then the screen jumped to public Help | the turn's suggested `navigate` action was auto-executed, and `help` was cast to a workspace `Screen` | `applyPilotSafeActions` navigates only when the turn actually ran the `navigate` tool, at most once, and only to a screen in the authenticated set (`landing` excluded); documented answers no longer emit a navigate action at all — their citations are the control |
| "How many jobs do I have?" listed 5 ids while Jobs showed `All (50)` | `list_jobs` reported the size of the page it read as the count | `MongoDBService.count_jobs` counts the whole history (total + per-status, one aggregate) and `job_narration.narrate_jobs` leads with that total, labelling the bullets as the most recent excerpt |

Failure questions ("which jobs failed?") now count failures over the whole history too,
instead of over the window, so the answer cannot contradict the Jobs page either way.

## Tests

`apps/api/tests/test_pilot_job_counts.py`, `apps/web/src/lib/pilotNavigationGate.test.ts`,
`apps/api/tests/test_rag_grounded_answers.py` plus the AI/Copilot/Pilot suites:

```
pytest -q tests -k "rag or copilot or pilot or job"
920 passed, 2 skipped, 16539 deselected

apps/web: npm test → 583 passed, 0 failed · npm run build clean
```

Frontend citation rendering: `apps/web/src/lib/pilotSources.test.ts` and the Pilot page /
rail share one `PilotSources` component, so both surfaces cite identically.

## Not proven here

- No live OpenAI account was called; provider behaviours are exercised through a local
  OpenAI-compatible stub and unit doubles.
- Retrieval quality is measured against the shipped 58-chunk corpus only. Corpus drift is
  regenerated with `npx tsx scripts/export-help-corpus.ts`.
