# Evidence chain — verification, retention, and pack anchoring

Every audit record is HMAC-SHA256 chained (`prev_hash` → `event_hash`, see
`apps/api/services/audit_log.py`). Until now nothing re-walked that chain, so an
altered or removed record was undetectable in practice, and a signed proof pack
stamped `prev_audit_hash` without ever being filed into the chain — a pointer at
a position it did not occupy.

`apps/api/services/evidence_chain.py` closes both gaps.

## Verification

`GET /api/v1/audit/verify?limit=5000` re-walks the stored chain oldest-first and
reports each record that fails, by index and event id:

| Finding | Meaning |
| --- | --- |
| `event_hash_missing` | Record carries no hash; nothing about it can be verified. |
| `event_hash_mismatch` | Recomputed HMAC (under the key the record names) ≠ stored hash: the record was altered after it was written. For a record with no `key_id` (written before D221) it may instead have been signed under a login secret that has since been rotated. |
| `key_mismatch` | The record names a signing key (`key_id`) this deployment does not hold, so it cannot be checked. A key configuration gap, not tampering. |
| `broken_link` | `prev_hash` does not name the previous record nor any earlier one: a record between them was removed, or the records were reordered. |
| `fork` | Two records claim one predecessor, or a record links one position back past a sibling branch — concurrent writers or a replayed segment; history is no longer a single line, but nothing was removed. |
| `unexplained_prefix` | The oldest record points at an absent predecessor and no signed retention checkpoint accounts for it. |

Each finding carries a `category` and the report totals them in `categories`:
`tampering` (`event_hash_missing`, `event_hash_mismatch`, `broken_link`,
`unexplained_prefix`), `fork`, `key_mismatch`, `legacy_hash_input` (records
broken by the pre-R7 hash input). `tampering_detected` is true only when the
`tampering` count is non-zero.

### Signing key (D221)

The chain has its own key, `DATAFLOW_AUDIT_HMAC_KEY` (or
`DATAWRAP_AUDIT_HMAC_KEY`); set the same value on every process that writes
audit records (API, worker, scheduler, CDC worker). Every record stores
`key_id`, a public fingerprint of the key that signed it. To rotate, set the new
key and move the old one into `DATAFLOW_AUDIT_HMAC_RETIRED_KEYS`
(comma-separated); records keep verifying under the key they name.

Records written before D221 have no `key_id` and verify under the login secret
(`DATAFLOW_AUTH_SECRET`), exactly as before, or under any retired key — so a
login secret that was rotated away can be listed there to make its records
verifiable again. The login secret is no longer used to sign anything new.

If `DATAFLOW_AUDIT_HMAC_KEY` is unset the login secret is still not used: the
process generates a random key once and keeps it at `<data dir>/audit_chain.key`
(mode 0600), shared by processes on the same volume, and logs a warning.
Processes on other hosts generate their own, and their records then show as
`key_mismatch` (not tampering) until the key is configured.

Appends are atomic across processes: in MongoDB the record's `_id` is its chain
position (`audit-chain:<seq>`), which MongoDB keeps unique on every collection
without any optional index, so a second writer to the same position gets a
duplicate-key error and re-links to the new tip. The file store takes an `flock`
across read-tip-and-append.

The owner's read-only production check is `apps/api/scripts/audit_chain_check.py`
(see `docs/AUDIT_CHAIN_PRODUCTION_CHECK.md`).

The **walk** is deliberately not workspace-filtered: the chain links every
record, so skipping another tenant's rows would invent broken links. When
`X-Workspace-Id` is set, findings that name another workspace's records are
**withheld after the walk**. `verified` stays the global verdict. Isolation on
and no header is a 400 (same as audit export).

Surface: **Settings → Audit Logs → Verify chain**.

## Retention is not tampering

Trimming the JSONL store (`DATAFLOW_AUDIT_MAX_EVENTS`, default 5000) deletes the
oldest records, which leaves the first survivor pointing at a hash that is no
longer present — indistinguishable from a deletion. `_trim_if_needed` therefore
writes a signed checkpoint to `audit_truncations.jsonl` recording
`removed_count`, the last removed hash, and the first kept hash. Verification
reads those checkpoints and reports retention as retention.

Checkpoints are themselves HMAC-signed, so an unsigned or edited checkpoint is
ignored and cannot be used to excuse a deletion. If the checkpoint write fails,
the trim still happens and the gap is reported as `unexplained_prefix` — the
honest outcome.

## Proof pack anchoring

`export_proof_pack_for_job` passes `anchor_in_chain=True`. The digest of the pack
body is filed into the chain as a `migration.evidence_sealed` record, and the
resulting record (`event_hash`, `prev_hash`, `sealed_at`) is embedded in the pack
*before* signing, so the pack and its chain record each commit to the other's
content. `build_signed_proof_pack` stays a pure function unless anchoring is
asked for.

Two independent checks:

* offline — `verify_signed_proof_pack` recomputes the anchored digest, so an
  altered pack, or an anchor lifted from a different pack, fails;
* against the store — `POST /api/v1/audit/verify-pack` also reports whether the
  chain still holds the record filed for that pack.

Anchoring is best-effort by design: if the audit store is unavailable the export
still succeeds and the pack carries `chain_anchor.anchored = false` with the
reason, so it can never silently claim a position it never got.

A run also seals a `migration.run_evidence` record on completion, so a migration
that nobody exported a pack for still leaves a tamper-evident trace (the lineage
ring buffer is bounded and process-local; the job document is mutable).

## What this does and does not prove

Proves: the records still in the store were not edited, reordered, or removed
since a process holding the platform HMAC secret wrote them, and that an exported
pack matches the record filed for it.

Does **not** prove:

* that the recorded facts are true, or that the actor was who they claimed;
* anything to a party that holds the signing secret — HMAC is tamper-evident
  only to those who do not have it. Public verifiability needs asymmetric
  signatures, which this does not implement;
* that records were never discarded *together with* their checkpoint by whoever
  controls the store. Only an external WORM / RFC-3161 timestamp anchor narrows
  that, and `services/audit_anchor.py` is a local stub unless
  `DATAFLOW_AUDIT_ANCHOR_PROVIDER` is configured;
* compliance with anything. A verified chain is diligence evidence, not a SOC 2,
  HIPAA, GDPR, SOX, or DORA attestation.

Also note the JSONL fallback store is not immutable storage; MongoDB is preferred
when connected.
