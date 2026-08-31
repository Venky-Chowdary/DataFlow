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
| `event_hash_mismatch` | Recomputed HMAC ≠ stored hash: the record was altered after it was written, or written under a different platform secret. |
| `broken_link` | `prev_hash` does not name the previous record: a record between them was removed, or the two were reordered. |
| `fork` | Two records claim one predecessor — concurrent writer or replayed segment; history is no longer a single line. |
| `unexplained_prefix` | The oldest record points at an absent predecessor and no signed retention checkpoint accounts for it. |

Verification is deliberately **not** workspace-scoped: the chain links every
record, so a filtered read would show gaps that are only filtering.

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
