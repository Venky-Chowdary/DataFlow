# Migration Certificate

The one artifact an operator hands a client after a run. Everything on it is
derived from evidence the engine already produced (Gate-8 reconciliation, the
signed proof pack, the quarantine DLQ) — the certificate re-states that evidence
honestly, it never re-derives correctness on its own.

Module: `apps/api/services/migration_certificate.py`
Tests: `apps/api/tests/test_migration_certificate.py`

## Endpoints

| Route | Purpose |
|---|---|
| `GET /transfer/{job_id}/certificate` | Signed JSON certificate |
| `GET /transfer/{job_id}/certificate?format=markdown` | Client-facing page (UI download) |
| `POST /transfer/certificate/verify` | Re-check hash, HMAC, and claim legitimacy |

Access uses the same fail-closed `_can_access_job` workspace/ACL gate as
`/proof-pack`, and every export appends a `migration_certificate.export` audit
event. Signing shares `signed_proof_pack.sign_body` — one HMAC implementation,
subject-bound to the job id so a certificate cannot be replayed for another run.

## Row conservation ledger

```
rows_read == rows_written + rows_quarantined + rows_skipped
```

`rows_read` comes **only** from Gate-8's measured `source_rows`. That is the
single figure measured against the source rather than reported by the writer.

- `unaccounted > 0` — rows were read and never explained. Reported as
  **potential silent loss**; blocks the proven claim.
- `unaccounted < 0` — more rows accounted for than read: duplicate writes or
  double-counted rejects. Also blocks the claim.
- `rows_read` absent — `rows_read_source: "unmeasured"`, `balanced: false`,
  `unaccounted: null`. An unmeasured source count can never be rendered as a
  clean ledger, and the Markdown prints `unmeasured`, never `0`.

## Verdict

`MIGRATION PROVEN` requires **all** of:

1. proof-pack `claim_level == "full_checksum"` (post-write, not sample or writer-ack);
2. a balanced row ledger;
3. no `proof_incomplete_reasons`;
4. job status completed/succeeded/success.

Otherwise `NOT PROVEN` (blockers listed) or `COMPLETED — NOT PROVEN` (run is
clean but assurance is below full checksum). `verify_migration_certificate`
re-checks these constraints, so re-signing a forged verdict still fails.

## Quarantine and burn-down

Reasons are grouped verbatim — the reason string is what the operator
remediates against, so it is never bucketed into invented categories.

Job documents cap embedded rejects, so when the embedded sample is shorter than
the reported quarantine count the breakdown is hydrated from the durable DLQ.
When only part of the population is available the certificate says so
("covers 40 of 1,000 rows") instead of implying the sample is the whole.

Burn-down reads DLQ events: `quarantined − replayed = open`. If the DLQ cannot
be read it returns `available: false` with a note — never zeros, because an
unreadable ledger must not render as a clean burn-down.

## What the certificate explicitly does not prove

- per-cell fidelity beyond the stated reconciliation scope;
- population referential integrity without an opt-in orphan scan;
- exactly-once delivery — CDC and resume are at-least-once with upsert;
- anything about rows the job never read.
