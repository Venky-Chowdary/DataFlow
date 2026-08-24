# Migration Certificate

The one artifact an operator hands a client after a run. Everything on it is
derived from evidence the engine already produced (Gate-8 reconciliation, the
signed proof pack, the quarantine DLQ) — the certificate re-states that evidence
honestly, it never re-derives correctness on its own.

Module: `apps/api/services/row_conservation.py` (identity) +
`apps/api/services/migration_certificate.py` (operator page)
Tests: `apps/api/tests/test_row_conservation.py`,
`apps/api/tests/test_property9_row_conservation.py`,
`apps/api/tests/test_migration_certificate.py`

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
reader_count == dest_COUNT(*) + hold_outs + skipped
```

`hold_outs = max(rejected − coerced_null, 0)` — coerced-null rows landed.

`rows_written` comes **only** from Gate-8's independent dest read-back
(`target_rows` when that figure is dest COUNT(*), not writer ack). Writer
`records_processed` is a diagnostic third number. Closing the ledger with it
is how AWS DMS reports Full Load success and later `MISSING_TARGET`.

- `unaccounted > 0` — dest COUNT(*) plus hold-outs plus skipped does not
  explain the read. Reported as **potential silent loss**; blocks the proven
  claim. Writer ack is not evidence those rows landed.
- `unaccounted < 0` — dest holds more than the identity allows (duplicate
  writes, leftover overwrite rows, or double-counted rejects).
- `rows_read` absent — `rows_read_source: "unmeasured"`, `balanced: false`.
- dest COUNT(*) absent or writer-ack-only — `rows_written_source: "unmeasured"`,
  `balanced: false`. An unmeasured dest can never be rendered as a clean ledger.
- append uses `COUNT(*)_after − COUNT(*)_before`, not the whole-table count.
- upsert/CDC into a non-empty dest has no COUNT(*) identity (updates do not
  change cardinality); the ledger stays unproven rather than inventing balance.

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
