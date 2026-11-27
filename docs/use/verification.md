# Verification

Datawrap checks a transfer twice: before it writes (Validate) and after it
writes (Gate-8). This page explains what each check proves, what it does not,
and how to read the words the product uses for the result.

## The short version

- **Validate** runs before any write. It looks at schemas, types, permissions
  and a sample of rows. A sample can find problems. It never proves that every
  row is correct.
- **Gate-8** runs after the write. It compares what was read from the source
  with what is now in the destination. This is the check that decides whether
  a run is verified.
- The result is only as strong as the comparison that actually ran. The
  product says which one ran. Read that before you rely on the result.

## Validate (before the write)

Validate runs a list of numbered gates. A gate can pass, warn or block. A
blocked gate stops the run until you fix the cause or record an approved risk.

Every Validate finding is stamped with the layer it belongs to: schema,
datatype, sample, population, execution or post-write. Only a population-level
check that ran to completion counts as proof about every row.

### Validation modes

The mode decides how strict Validate is. An unknown mode name is treated as
strict.

| Mode | Mapping confidence floor | Writes? | Behaviour |
|---|---|---|---|
| `strict` (default) | 0.85 | yes | Fidelity and schema failures block the run unless a signed risk contract covers the column. |
| `maximum` | 0.95 | yes | Same as strict with a higher confidence floor. |
| `balanced` | 0.75 | yes | Approved risks may unlock lossy columns. Unapproved risks still block. |
| `lenient` | 0.75 | yes | A named policy with the balanced floor. Not a licence to drop rows. |
| `migration` | 0.75 | yes | Recoverable issues become warnings. Risks are not accepted automatically. |
| `discovery` | 0 | no | Reports findings only. Never unlocks a run. |
| `audit` | 0.85 | no | Records blocks for the audit trail. Never writes. |

In every mode a checksum mismatch fails Gate-8. No mode turns a sample into
proof.

### Accepting a known loss

Sometimes a loss is intended, for example a column that is narrower in the
destination. A yes/no flag is not accepted for this. The approval is a signed
record (a migration risk contract) that states what can be lost (precision,
truncation, nulls), what the writer must do, the quarantine and rollback
posture, and who approved it, when and why. The default policy when a risk is
not approved is to fail the job, never to continue silently.

When a route's mapping confidence is below the mode's floor, review the
mapping in the Map step and run Validate again. Nothing is written until that
gate passes.

### Dropping fields on purpose

A source column must be mapped or declared omitted; otherwise the run is
refused. The field reduction ledger goes further and records why each dropped
field was dropped:

- every source field gets one disposition: carried, folded into a target, or
  reduced;
- a reason that makes a factual claim ("always empty", "always the same
  value") is checked against the sample. A claim the sample contradicts blocks
  the run;
- "kept elsewhere" needs a reference to where.

Limits: the check uses the Validate sample, not every row. A sample with no
values is not proof the column is empty. The ledger records who approved; it
does not verify that person's identity beyond their session.

## Gate-8 (after the write)

Gate-8 is the comparison of source and destination after the write. The
strongest form is a five-layer ladder:

| Layer | What it compares | What it tells you |
|---|---|---|
| L1 | Row counts | Whether rows are missing or extra. For appends, the change in the destination; for keyed upserts and CDC, inserts minus deletes. |
| L2 | Per-column null count, minimum, maximum and sum | Whether a column was nulled, truncated or lost precision. |
| L3 | A checksum over every row, independent of row order | Whether the whole population matches. This is the core of Gate-8. |
| L4 | A digest per column | Which column differs. |
| L5 | A search over the ordered key | Which row differs, and the values. |

### Limits of the ladder

- The ladder runs on five destination engines: SQLite, PostgreSQL (and
  Redshift), MySQL/MariaDB, SQL Server and Snowflake. On any other destination
  it declines and the report says so.
- The in-memory ladder compares every row only up to
  `DATAWRAP_VERIFICATION_LADDER_MAX_ROWS` rows (default 250,000). Above that
  it declines.
- Above that limit, the per-column statistics of L2 can be computed by the
  database engines themselves, so a nulled or truncated column is still
  caught. This runs only when each end is PostgreSQL or MySQL/MariaDB (same
  or mixed), and only for overwrite (full refresh) runs. Row-level localisation does not run
  at that size.
- On a table above the limit, re-running does not help: the product states
  that no existing check compares every row of a table that size.
- When only the whole-population checksum fails on an object-store (S3, GCS,
  ADLS) or SFTP destination, Gate-8 re-reads every written key, up to
  1,000,000 rows, to name the changed row. That keyed compare runs on that
  failure path only; it is not a general check.
- On some routes the full compare does not run at all. The job records the
  reason and the report says so.
- **Multi-table runs.** In a run that writes several tables, Gate-8 compares
  values for the last table only. Every other table has its row count, not
  its values, compared, so the Trust Report of a multi-table run is
  "unverifiable" and cannot be signed. Per-table value comparison is not
  built yet.
- A digest only proves fidelity when both sides cover the same rows. After an
  append or upsert into a table that already held rows, the destination
  digest covers rows this job never wrote. That difference is classified as a
  matter of scope, not reported as corruption.

### What a passing checksum does not prove

- It does not prove referential integrity between tables.
- It does not prove the source was right.
- It does not prove anything about rows written to the source after the read.

## Coverage: which comparison ran

Each table in a result carries the kind of check that ran.

| Check | Meaning | Strength |
|---|---|---|
| `full_checksum` | Every row compared by checksum | Proof for the rows compared |
| `key_diff` | Every row compared by key | Proof for the rows compared |
| `sampled` | Values compared on a sample of rows | An estimate with a stated bound, when the sample was a random draw. Nothing, about rows outside it, when it was not. |
| `readback` | Mirror or SCD2: a digest of every live destination row, re-read after the write, compared with the digest of the rows the run read from the source | Values of every live row were compared, but the source side is the run's own read, not an independent re-read, and a digest names no row. Not proof. |
| `count_only` | Only row counts compared | Says nothing about values |

### Which samples are random

A database source streamed to a database destination (the streaming route)
draws its 50-row Gate-8 sample uniformly at random, without replacement, from
every row the run offered the writer. The draw is seeded, the seed and the
sampled keys are recorded in the Gate-8 report (`sample_compare.sample_seed`,
method `uniform_random`), and only such a sample gives the bound below. The
draw is named random only when every drawn row was compared (a compare that
stopped early on a failure, or a drawn key the destination did not return, is
not reported as random).

Every other route still samples the first rows read, the lowest keys among
them, or a pick across one column's values among them. Those samples are not
random: the report states the rows checked and how they were chosen, gives no
bound, and sign-off waits for a full compare.

The Trust Report relies on the sample only when no full compare ran, for
example an append or upsert into a table that already held rows. With 50 rows
and no difference found, the bound is "at most 5.82% of rows hold an
unexplained difference" (Clopper-Pearson, one-sided, 95%).
| `none` | Nothing compared | No evidence |

## Reading the verdict

The Trust Report turns the evidence into one of five verdicts.

| Verdict | What it means |
|---|---|
| **proven** | Every row was compared and none differs. |
| **explained** | Every row was compared. Some differ, and every difference has a known cause. |
| **no difference found** | Values were compared and nothing unexplained was found, but not in a way that proves every row: on a sample, or (mirror and SCD2) by a read-back digest whose source side is the run's own read. With a random sample the report gives an upper bound at 95% confidence, for example "at most 5.82% of rows hold an unexplained difference". This is not "proven". |
| **unexplained** | At least one difference has no known cause. |
| **unverifiable** | The evidence cannot decide: the job is not finished, nothing was compared, or only row counts exist. |

So, in the product's words:

- **Proven** is used only when every row was compared.
- **Sampled** results are never called proven or explained. A random sample
  bounds a rate; any other sample bounds nothing.
- A mirror or SCD2 **read-back** is never called proven or explained.
- **Unverifiable** is not a pass and not a fail. It means there is nothing to
  sign.

## Known causes of a difference

A difference is "explained" when it matches one of these rules: whitespace,
letter case, Unicode normalisation, numeric scale, float precision, time zone
shift, null versus empty string, truncation, encoding, a transform you applied,
a quarantined row, a difference an operator accepted, a concurrent update on
the source, or rows outside the job's scope. Anything else is unexplained.

Two engine facts that often cause explained differences:

- **Timestamps.** The source value is rounded to the precision the destination
  column can hold before comparison.
- **Numbers inside JSON.** A native JSON column may store a number
  differently from how it was written. MySQL `JSON` keeps integers that fit 64
  bits and stores other numbers as doubles. MongoDB stores a non-integer as a
  double when that is exact, otherwise as a 34-digit decimal, otherwise as
  text. Snowflake keeps plain decimals up to 38 digits and turns the rest into
  doubles. PostgreSQL `jsonb` and MariaDB `JSON` keep the digits. Validate
  refuses a number the destination cannot store with the same value, so the
  failure appears before the write, not after.

## Evidence you can export

- **Proof pack**: `GET /api/v1/transfer/{job_id}/proof-pack`. A signed record
  of the Gate-8 and mapping evidence. It claims "migration proven" only when
  the post-write check was a full checksum. A pack built from a sample or
  before the write can be exported, and refuses that claim. Verify one with
  `POST /api/v1/transfer/proof-pack/verify`.

### How evidence is signed and verified

- Proof packs, Migration Certificates, signed Trust Reports, Exit Evidence
  Packs and field-reduction ledgers are signed with HMAC-SHA256 using the
  **workspace's evidence signing key**. Each workspace has its own key; work
  outside any workspace uses the deployment's key. The key is generated on
  first use, stored encrypted with the deployment's secrets key, and is never
  the login (token) secret.
- Every signature records the key id (`ev1:…`), its version and whose
  keyring holds it. A workspace admin can see the key ids at
  `GET /api/v1/workspace/evidence-signing-key` and start a new key with
  `POST /api/v1/workspace/evidence-signing-key/rotate`. Earlier keys are kept,
  so packs signed before a rotation keep verifying.
- Packs signed before evidence keys existed carry the key id
  `platform_auth_secret`: they were signed with the login secret. Anyone who
  holds that secret can still compute such a signature, so the signature
  alone proves nothing. What proves a legacy pack is the **legacy evidence
  register**: when the first evidence key is created (the cutover), the server
  records the content digest of every login-secret-signed pack it holds: proof
  packs and Migration Certificates from the audit log's export records and
  chain anchors (the whole audit store, not the newest 20,000 records), signed
  parity sign-off packs from the job store, and signed Exit Evidence Packs
  from the exit store. Each record keeps the digest, the evidence type, when
  it was made, the job or report id and the workspace. Records are only ever
  added, and the audit retention purge does not touch the register. A
  deployment that created its first evidence key on an earlier build takes
  the snapshot the first time a legacy pack is verified, counting only audit
  records filed before that key was created.
- A legacy pack verifies only when its digest is in the register; the answer
  names it as the legacy key. A legacy pack that is not on record returns
  `ok: false` with "signed with the retired key and not on record before the
  cutover; it cannot be told apart from a forgery". Nothing new is signed with
  the login secret.
- **What the register cannot recover.** It records what the server still
  held at the cutover. A pre-cutover pack that was never exported and never
  stored (for example a proof pack downloaded without an export record), or
  whose export record the audit retention purge or the file store's size cap
  (`DATAFLOW_AUDIT_MAX_EVENTS`, 5,000 records by default) had already removed
  before the snapshot ran, is not on record and is refused like a forgery.
  Back up the register (MongoDB collection `legacy_evidence_register`, or
  `legacy_evidence_register.json` in the data directory) with the rest of the
  evidence.
- Verifying is a read. `POST /api/v1/transfer/proof-pack/verify`,
  `POST /api/v1/transfer/certificate/verify` and
  `POST /api/v1/exit/evidence/verify` need `job.read`;
  `POST /api/v1/audit/verify-pack` needs `audit.read`. Anyone with access to
  the pack's workspace can verify it. A pack signed with another workspace's
  key is refused rather than confirmed.
- **Limitation.** The key is a shared secret held by this deployment, so only
  this deployment can verify a pack. There is no public or offline verifier
  and no third-party signature in this code line. Asymmetric signatures and a
  public verifier are planned work (the evidence ledger), not a feature.
- **Signed Trust Report**: see [parity-studio.md](parity-studio.md).
- **Trust score**: a per-job signal combining completeness, quarantine,
  reconciliation and freshness. It is a signal for operators, not a
  certificate.

These are engineering evidence. They are not an auditor's attestation.

## Not covered

- The full list of Validate gates and their numbers.
- Measured run times of Gate-8 at different sizes.
- Verification of unstructured documents (PDF, DOCX, HTML).
- How each connector reads back from the destination.

## Verified against

- `apps/api/services/evidence_signing_keys.py`, `apps/api/services/gate8_uniform_sample.py`
- `apps/api/src/transfer/reconcile_step.py` (`_stamp_uniform_draw`, the 1,000,000-row
  keyed compare, the engine-side L2 above the ladder limit)
- `apps/api/src/transfer/stream_multi.py`, `apps/api/services/reconcile_coverage.py`
  (the last-stream checksum)

- `apps/api/services/verification_ladder.py`, `apps/api/services/column_profile.py`
- `apps/api/services/validation_mode_contract.py`, `apps/api/services/validation_coverage.py`
- `apps/api/services/migration_risk_contract.py`, `apps/api/services/root_cause_engine.py`
- `apps/api/services/field_reduction_ledger.py`
- `apps/api/services/reconcile_coverage.py`, `apps/api/services/parity_evidence.py`
- `apps/api/services/parity_report.py`, `apps/api/services/parity_classifier.py`,
  `apps/api/services/parity_stats.py`, `apps/api/services/parity_fixes.py`
- `apps/api/services/json_number_capacity.py`, `apps/api/services/carrier_instant.py`
- `apps/api/services/signed_proof_pack.py`, `apps/api/services/job_trust.py`
