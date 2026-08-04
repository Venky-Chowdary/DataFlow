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
- Hollow packs strip `migration_proven` when `ddl_hash` **and** `mapping_hash` are absent,
  matching independent checksums are missing, or `connector_versions` is empty
  (`proof_pack_evidence_completeness_errors` → `claim_level=incomplete_proof_evidence`)
- `connector_versions_honesty` stamps `format_or_kind_only` when values lack digits
- Preflight approve language clarified as Execute-ready only
- Job export (`export_proof_pack_for_job`) includes:
  - `accepted_risks` / `risk_contracts` harvested from mappings
  - `execution_policies[]` derived from those contracts
  - `rejected_rows_sample` + count
  - `rollback_plan` when stamped on the job
  - DDL / mapping / transformation hashes + Gate-8 checksums
  - CDC delivery honesty (`exactly_once: false`)

## Non-guarantees

- Full checksum does not prove RI / orphan absence
- Sample post-write is assurance, not population proof
- Writer-ack is never migration proven
- Checksum mismatch never greens Gate-8 via sample (Enterprise GA)
- Format/kind connector labels are not package version proof
