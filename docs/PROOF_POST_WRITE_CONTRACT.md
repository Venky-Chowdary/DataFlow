# Proof Post-Write Contract (Module 8)

## Promise

**Never produce a migration-proven claim without post-write verification.**

Execute-ready (Validate `decision=approve`) is **not** migration proven.

## Claim levels

| `claim_level` | `post_write_verified` | `migration_proven` | Meaning |
|---------------|----------------------|--------------------|---------|
| `none` | false | false | No Gate-8 evidence |
| `pre_write_only` | false | false | Validate simulation — pending write |
| `writer_ack` | false | false | Writer said OK — not independent proof |
| `sample` | true | **false** | Keyed sample matched; not population |
| `full_checksum` | true | **true** | Row-count + checksum match |
| `failed` | false | false | Gate-8 failed |

`population_proof` and `referential_integrity_proven` remain **false** even for
`full_checksum` — checksum ≠ FK/orphan RI.

## Code SSOT

- `apps/api/services/signed_proof_pack.py`
  - `classify_post_write_assurance`
  - `assert_pack_may_claim_migration_proven`
  - `build_signed_proof_pack` always stamps `assurance`
- `apps/api/services/preflight_proof_bundle.py`
  - stamps `migration_proven`, `post_write_proof`, `proof_assurance`

## Guarantees

- Signed packs with `migration_proven=true` require `claim_level=full_checksum`
- Verify rejects packs that invent proven without full_checksum
- Preflight approve language clarified as Execute-ready only

## Non-guarantees

- Full checksum does not prove RI / orphan absence
- Sample post-write is assurance, not population proof
- Writer-ack is never migration proven
