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

## How Validate flags card data (`compliance_guard.py`)

These are risk signals, not a PCI scanner.

- **Column name.** `pan` as a whole `_`-separated token with any prefix or suffix (`masked_pan`, `pan_encrypted`, `tokenized_pan`, `card_pan`…), `card_number`, `cc_number`, `credit_card`, `debit_card`, `payment_card`, `cvv`, `card_holder` and expiry fields. A masked, encrypted or tokenized PAN is not Luhn-valid, so the name is its only guard. Known non-card words are excluded: `pan_id`, regional `pan_african` / `pan_india` / …, `pan_region`, and the camera or chart sense (`pan_tilt`, `pan_x`, …).
- **Text column.** Any Luhn-valid 13-19 digit value is a PAN.
- **Numeric column (BIGINT, DECIMAL or int cells).** The column is card data when both of these hold:
  - at least **5** sampled values are card numbers: 13-19 digits, Luhn-valid, with a card network's issuer prefix (Visa, Mastercard, Amex, Discover, JCB, UnionPay, Diners, Maestro, Mir, RuPay, Troy, UATP);
  - those card numbers make up at least **80%** of the non-empty sample.

  One integer key in ten passes Luhn by chance, so a column of ids is not card data.
- **Small sample.** With 1-4 card hits at 80% or more of the non-empty sample, Validate shows a **non-blocking advisory** (`possible_pci_pan`, "small sample"), both in the preflight evidence (`proof_bundle.compliance.advisories`) and in the UI. It does not hold the transfer. Review the column, and rename or mask it if it holds PANs.

## What we will not claim

- PCI DSS Level 1/2 service provider certification
- Tokenization vault / payment switch replacement
- Guaranteed CHD discovery across all dialects

## Related

- `docs/BUYER_EVIDENCE_PACK.md`
- `docs/AI_GATE_POLICY.md`
- `apps/api/services/compliance_guard.py` (PII risk signals — not a PCI scanner)
