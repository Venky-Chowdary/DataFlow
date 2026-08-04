# Migration confidence layer (product position)

**Do not sell Datawrap as “another AI ETL / 500-connector platform.”** That category is crowded and unwinnable against Fivetran, Informatica, Airbyte, and DMS.

## Winning position

> **AWS DMS (or your mover) moves it. Datawrap proves it.**

Datawrap is a **Migration Assurance / Migration Confidence** workbench:

| Buyer fear | Datawrap answer |
|------------|-----------------|
| Silent corruption (balances off by $0.01) | G1–G9 fail-closed + Gate-8 reconciliation + quarantine |
| Unexplained schema coercion | Map fidelity + risk ack (lossy/mutate) |
| No audit trail for cutover | Signed HMAC proof packs + mapping proof + audit chain |
| Agent/LLM bypasses gates | AI never decides G1–G9 (`docs/AI_GATE_POLICY.md`) |

## What we are / are not

| Are | Are not |
|-----|---------|
| Migration confidence report + Studio path | Connector-count empire |
| Map → Validate → Execute → Proof | ADF/Informatica DAG platform |
| At-least-once CDC capability (honest) | Exactly-once / Qlik Replicate replacement |
| Assurance adjacent to DMS/Airbyte | Fivetran MAR ELT fleet |

## Strongest MVP narrative

**Migration Validation Engine** — Confidence Report:

- Schema / mapping contract
- Row reconciliation (Gate-8)
- Constraint / uniqueness (G9, composite PK)
- Anomalies + business-rule failures
- Portable evidence for banks / healthcare / SI partners

## Buyers

Data Engineering Director · Cloud Migration PM · Enterprise Architecture · SI partners (Accenture, Deloitte, Capgemini).

## Moat

Trust + validation algorithms + migration evidence + enterprise compliance *acceptance* — not connectors.
