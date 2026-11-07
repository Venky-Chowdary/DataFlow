# Type Conversion Contract + DDL Identity (Module 12)

## Promise

Every mapping carries an explicit **ConversionClass**. Map → materialize DDL → Execute share one **DDL identity fingerprint**. Inventing precision, scale, FSP, or timezone without approval is never silent green.

## ConversionClass (Phase C3 — full set + Module 12 gates)

Safe-path detail (non-lossy):

| Class | Meaning |
|-------|---------|
| `identity` | Identical source/target type stamps |
| `equivalent` | Same logical family, different native spelling |
| `widening` | Integer/float width increase (safe) |
| `representation` | Same string/text family, different representation |
| `normalization` | Canonical normalization without value loss |
| `lossless` | Other proven non-lossy cross-logical path |

Risk / fidelity:

| Class | Meaning |
|-------|---------|
| `narrowing` | Width/precision collapse (under Risk Contract when acknowledged) |
| `semantic` | Role/semantic reinterpretation |
| `potentially_lossy` | Ambiguous fidelity |
| `lossy` | Lossy path under an approved Risk Contract |
| `unsupported` | Explicit domain jump (e.g. STRUCT→INTEGER) |
| `manual` | Operator must map manually (preferred label) |

Gate / operator action (Module 12 — stable Execute blockers):

| Class | Meaning |
|-------|---------|
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

- **Import path:** `services.decision_kernel` (Type + Conversion + DDL identity + Execute gate)
- Implementation (until god-module split): `apps/api/services/conversion_contract.py`, `type_system.py`
- Wired: `preflight_service` stamps `decision_artifact` + DDL identity; `engine._enforce_ddl_identity` + `_enforce_decision_artifact`

## Pair assurance (Module 15)

Offline `pair_assurance.evaluate_type_cell` stamps both:

- Legacy `classification`: `lossless | lossy_ack_required | blocked | error`
- Charter `conversion_class`: from `classify_conversion` (7-class)

Proof artifacts include `conversion_class_counts`. Sample/fixture scope unchanged — never population proof.

## Related

- `docs/MIGRATION_RISK_CONTRACT.md`
- `docs/BUYER_EVIDENCE_PACK.md` Map≡CREATE / pair assurance
- `docs/EXECUTION_ENGINE_CONTRACT.md`
