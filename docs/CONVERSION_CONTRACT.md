# Type Conversion Contract + DDL Identity (Module 12)

## Promise

Every mapping carries an explicit **ConversionClass**. Map → materialize DDL → Execute share one **DDL identity fingerprint**. Inventing precision, scale, FSP, or timezone without approval is never silent green.

## ConversionClass (charter 7)

| Class | Meaning |
|-------|---------|
| `lossless` | Round-trip without invent or declared loss |
| `lossy` | Lossy path under an approved Risk Contract |
| `unsupported` | Explicit domain jump (e.g. STRUCT→INTEGER) |
| `needs_transform` | Mutating transform rewrite |
| `needs_user_approval` | Lossy or invent without Risk Contract |
| `needs_quarantine` | Parse transform; bad cells quarantine |
| `needs_manual_mapping` | Unmapped / missing types |

## DDL identity

1. Validate stamps `proof_bundle.ddl_identity.ddl_identity_hash` via `materialize_dest_ddl` on approved Map stamps.
2. Execute calls `assert_ddl_identity` against current mappings.
3. Drift → job fails closed; operator must re-validate.

## Never invent

Bare `DECIMAL` → `DECIMAL(p,s)` / bare temporal → dialect FSP / TZ polarity loss → `needs_user_approval` until Risk Contract.

## Non-authoritative

`apps/api/src/ai/knowledge/type_conversions.py` is **assist-only** (`AUTHORITATIVE=False`). Do not use it for Map/Validate/Execute.

## Code SSOT

- `apps/api/services/conversion_contract.py`
- Wired: `mapping_proof.stamp_mapping_fidelity`, `preflight_service` proof stamp, `engine._enforce_ddl_identity`

## Related

- `docs/MIGRATION_RISK_CONTRACT.md`
- `docs/BUYER_EVIDENCE_PACK.md` Map≡CREATE / pair assurance
