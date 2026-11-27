# Reversibility and exit readiness

Two features answer "could we get our data back out?". Neither moves data
back on its own.

- **Reversibility** is an analysis of a transfer's mapping: for each column,
  could the source value be recovered from the destination? It can block a
  plan or a run. Its "rollback" is a written runbook, not an executable
  rollback.
- **Exit readiness** is a register of the systems you would have to take out,
  plus **rehearsals**: real extracts of every declared table, checked for
  completeness, timed against your exit window, sealed and signed.

## Reversibility

### Where to find it

- In the web app: the Reversibility card in the plan step, with a "Require
  reversibility" switch, and the Reversibility tab of a job on the Jobs page.
- Through the API: `GET /api/v1/transfer/plans/{plan_id}/reversibility`
  (`job.plan`) and `GET /api/v1/jobs/{job_id}/reversibility` (`job.read`).
- Through MCP: the read-only `check_reversibility` tool.

### What it checks

Every mapped column is classed as one of:

| Class | Meaning | Examples |
|---|---|---|
| invertible | The source value can be recovered exactly | Same type, a rename, a wider column, a lossless cast |
| lossy | The source value cannot be recovered | `hash_pii`, `mask_pii`, `redact`, an omitted or dropped column, a narrower type, a date cut from a timestamp, rounding, a time zone dropped |
| conditional | Recoverable only if the values round-trip exactly | A cast out of text, a normaliser such as `trim` or `upper`, a default |

Conditional columns are checked against the rows Validate sampled. A column
that fits those rows is invertible "for the data seen": the report says it is
not proven for rows that were not sampled.

Type rules are engine-aware (bare spellings such as `INT` resolve per engine,
byte versus character widths, JSON normalising, `CHAR` padding). The rules for
SQL Server, Oracle and Snowflake come from vendor documentation and have no
live-engine tests in this code line.

A table is **reversible** when every column is invertible (or conditional and
fits), **not reversible** when every column it holds is lossy or a key column
is lossy, and **partly reversible** otherwise.

### Enforcement

When a plan's `require_reversibility` policy is on (or a run sets
`require_reversibility`), the Validate gate `g24_reversibility` blocks a plan
or run that is not reversible, before any row is read. The same rule refuses
a schedule that would run such a transfer (REST, MCP and GitOps) and a replay
of one. The writer enforces conditional columns as it writes.

### What "rollback" means here

Each table carries a rollback posture:

- **Restore from destination**: every mapping is lossless, so the destination
  holds what is needed to rebuild the source.
- **Keep source as fallback**: some mapping is lossy, so keep the source until
  sign-off.

This is advice. The rollback plan of a run is a `DOCUMENT_ONLY` runbook.
Datawrap does not undo committed rows, run a reverse transfer, or fail back
to the source. The Job Reversibility tab is recomputed from the job's stored
request; it does not show whether this run's data actually round-tripped.

## Exit readiness

### Where to find it

- In the web app: **Exit readiness** in the sidebar. The board lists your
  declared systems; open one to rehearse it and read its rehearsals.
- Through the API, under `/api/v1/exit/`:

| Route | Permission | What it does |
|---|---|---|
| `GET /exit/readiness` | `job.read` | The board: systems, which are proven, which are overdue, the longest measured exit, the next regulatory date |
| `GET`, `POST /exit/systems`; `GET`, `PATCH`, `DELETE /exit/systems/{system_id}` | `job.read` to read, `job.manage` to change | Declare the systems you would take out |
| `POST`, `DELETE /exit/systems/{system_id}/pii-acknowledgement` | `job.manage` | Acknowledge PII so the system's extracts can pass the PII gate |
| `POST /exit/systems/{system_id}/rehearsals` | `job.run` | Start a rehearsal |
| `GET /exit/systems/{system_id}/rehearsals`, `GET /exit/rehearsals/{rehearsal_id}` | `job.read` | Read rehearsals |
| `POST /exit/rehearsals/{rehearsal_id}/signoff` | `job.manage` | Sign a proven rehearsal for your role |
| `GET /exit/rehearsals/{rehearsal_id}/evidence?format=json` or `markdown` | `job.read` | The signed Exit Evidence Pack, or a readable report |
| `POST /exit/evidence/verify` | `job.read` | Verify a pack: signature, Merkle root, anchor |

- Through MCP: the read-only `get_exit_readiness` and `explain_exit_rehearsal`
  tools.

### Declaring a system

A system names a saved source connection, the tables to take out (with their
key columns, or the source's primary key), the exit window in hours and what
it is based on (for example a contract clause), any regulatory dates, how
often it must be rehearsed (default every 90 days), an optional rehearsal
schedule (cron and time zone), the extract format (`jsonl` or `csv`) and the
sign-off roles (default engineering, data owner and compliance).

### What a rehearsal does

A rehearsal runs one real extract per declared table on the normal transfer
engine, to a file in the private export bucket where the deployment has one,
else the exports folder. It then checks each table:

- row counts on both sides;
- a digest of the key set and a digest of every row, each over the whole
  population, source read in one pass and the sealed extract read back;
- the sum, minimum and maximum of every numeric column;
- the Trust Report of the extract job.

A table is **proven** when all of these were measured on both sides and are
equal and the extract file's SHA-256 was confirmed on read. It is
**incomplete** when something measured differs, **failed** when the extract
failed, and **not proven** when nothing differs but something could not be
measured in full (no key, a column whose values cannot be compared exactly,
a value check that was only sampled). The rehearsal is proven only when every
declared table is.

The extract files are sealed with a Merkle root (RFC 6962) over their
SHA-256 digests. The duration is measured from the job clocks and compared
with the exit window; after two measured rehearsals the next one is projected
and labelled as a projection. A system is overdue when its last rehearsal
started more than its rehearsal interval ago.

### Sign-off and the evidence pack

Sign-off uses the same rules as the Trust Report: each role is bound to real
authority in the system's workspace, one person signs one role, and only a
proven rehearsal can be signed. When the last role signs, the Exit Evidence
Pack is signed (HMAC-SHA256) with the workspace's evidence signing key and its
digest is anchored. Anyone with access to the workspace can verify the pack.

## Limits

- No executable rollback, reverse transfer or fail-back, for any engine.
- Rehearsal completeness is measured for SQL sources only. A rehearsal of a
  MongoDB, file, object-store or SaaS source cannot be proven: its tables are
  "not proven".
- The source digest and the extract are separate reads. On a source that is
  being written to, a rehearsal reports "incomplete"; rehearse a quiet source
  or a snapshot.
- The default anchor provider is a local stub: in a deployment that has not
  configured S3 Object Lock (`AUDIT_ANCHOR_PROVIDER=s3`), the pack's anchor
  reads "not anchored".
- The signing key is a shared secret this deployment holds, so only this
  deployment can verify a pack. There is no public or offline verifier yet.
- The time a rehearsal's verification may take is bounded only by
  `EXIT_VERIFY_MAX_SECONDS`, unset by default. No throughput figure has been
  measured.
- Live-engine tests cover PostgreSQL, MySQL, MongoDB and Redis (reversibility
  enforcement) and PostgreSQL and MySQL (rehearsals). SQL Server, Oracle,
  Snowflake and BigQuery have no live tests for these features.

## Verified against

- `apps/api/services/reversibility.py`, `apps/api/services/reversibility_types.py`,
  `apps/api/services/reversibility_enforce.py`, `apps/api/services/reversibility_schedule.py`,
  `apps/api/services/migration_rollback.py`
- `apps/api/src/routers/reversibility_router.py`, `apps/api/src/routers/exit_router.py`
- `apps/api/services/exit_inventory.py`, `apps/api/services/exit_rehearsal.py`,
  `apps/api/services/exit_completeness.py`, `apps/api/services/exit_evidence.py`,
  `apps/api/services/audit_anchor.py`, `apps/api/services/evidence_signing_keys.py`
- `apps/api/services/rbac.py` (route permissions)
- `apps/web/src/pages/ExitReadinessPage.tsx`, `apps/web/src/components/exit/`,
  `apps/web/src/components/reversibility/`
