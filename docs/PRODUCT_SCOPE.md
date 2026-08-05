# Datawrap — Product scope (honest)

Datawrap is a **governed migration / data-movement workbench**: Source → Destination → Map → Validate (G1–G9) → Run → Proof — not a general BI tool and not a Fivetran-class ELT fleet.

## In scope (shipped)

| Operation | Reality |
|-----------|---------|
| File → DB | CSV / JSON / Parquet (and related parsers) on PRODUCTION_SKU routes |
| DB → DB | Proven routes in `apps/api/src/transfer/registry.py` `PRODUCTION_SKU` |
| Map | BM25 + Hungarian + fidelity / risk ack (Studio `/map`) |
| Validate | Core preflight **G1–G9** (+ Studio policy gates / `constraint_fk` when dest FK metadata shows unmapped columns — not an extra marketed gate; RI not claimed from schema hints) |
| Run / Jobs | Checkpoint/resume; quarantine (no silent drop); Gate-8 reconcile |
| CDC | **At-least-once**; destinations must upsert with PK/LSN guards — not exactly-once |
| Schedules | Recurring sync UI (nav: Schedules) |
| Pilot / MCP | Same engine; cannot skip preflight when auth is required |

## Out of scope / not claimed

- Auditor **SOC 2 / HIPAA / PCI certification** (security *posture* and questionnaire packs only until letters exist)
- Exactly-once CDC / Qlik Replicate replacement
- Full iPaaS / ADF orchestration (no DAG “Pipelines” product yet)
- “Any file → any DB” including PDF/Word as transfer-ready without SKU proof
- RAG / sentence-transformers as the Studio column mapper (vector destinations and optional AI paths are separate)

## Guarantees (engine)

- **Fail-closed**: G1–G9 block bad jobs before write when Validate/Execute use the engine path
- **Mapping honesty**: lossy / mutate / cast require operator acknowledgement where required
- **Checkpoint fail-closed**: failed checkpoint persistence aborts the job
- **SKU honesty**: catalog tiles ≠ live; Planned connectors stay labeled until PRODUCTION_SKU

## AI vs gates

AI/Pilot/MCP may suggest mappings and transforms only. They **never** decide G1–G9
pass/fail. See `docs/AI_GATE_POLICY.md`.

## Rollback

Honest cutover rollback posture (quarantine, staging, warehouse restore — not
one-click undo): `docs/MIGRATION_ROLLBACK.md`.

## Buyer evidence

See `docs/BUYER_EVIDENCE_PACK.md` for how to cite gates, SKU routes, and test anchors in diligence.

## Compliance packets

- PCI scope (not AoC): `docs/PCI_SCOPE_PACKET.md`
- Redis TTL honesty: `docs/REDIS_TTL_SEMANTICS.md`
