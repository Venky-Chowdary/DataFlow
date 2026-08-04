# PCI DSS scope packet (honest questionnaire)

**Status:** Security *posture* document for buyer diligence — **not** a PCI DSS
Attestation of Compliance (AoC). Do not claim PCI certified until a QSA letter exists.

## Product scope relative to cardholder data (CHD)

Datawrap is a **Migration Assurance Workbench**. Default product scope is
**out of CHD** when operators follow this packet.

| Question | Answer |
|----------|--------|
| Does Datawrap store PAN / full track data? | **No** by design. Do not configure sources that land raw PAN into Studio samples, quarantine stores, Gate-8 exports, Pilot/LLM prompts, or audit payloads. |
| Are samples / quarantine CHD-safe? | Operators must exclude CHD columns from Map selection or apply `hash_pii` / omit before Validate. LLM paths require PII masking (`PII_MASKING`). |
| Is the control plane in CHD CDE? | Deploy API/web **outside** the cardholder data environment. Use PrivateLink/VPC to reach in-scope warehouses if needed — bridge docs in `docs/ops/`. |
| Encryption at rest for secrets? | Connector secrets support Fernet + optional AWS KMS BYOK. Application DB audit is hash-chained; WORM/object-lock is roadmap. |
| Network | TLS to clients; configure custom domain/CORS per ops runbook. |
| Logging | Job logs and Gate-8 packs must not embed PAN. Redact before export. |

## Exclusion patterns (operator checklist)

1. **Do not** map columns named like `pan`, `card_number`, `cvv`, `track2` without omit/`hash_pii`.
2. **Do not** enable Pilot/LLM mapping on CHD-bearing samples.
3. Prefer **create-new staging** schemas outside CDE; cut over only after Gate-8 proof on non-CHD keys.
4. Quarantine CSV downloads are evidence — treat as sensitive; delete after remediation.

## What we will not claim

- PCI DSS Level 1/2 service provider certification
- Tokenization vault / payment switch replacement
- Guaranteed CHD discovery across all dialects

## Related

- `docs/BUYER_EVIDENCE_PACK.md`
- `docs/AI_GATE_POLICY.md`
- `apps/api/services/compliance_guard.py` (PII risk signals — not a PCI scanner)
