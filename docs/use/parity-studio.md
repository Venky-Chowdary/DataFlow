# Parity Studio and the Trust Report

Parity Studio answers one question about a finished job: does the destination
match the source? The answer is a Trust Report that a person can sign.

## Where to find it

- In the web app: open a job on the Jobs page and choose the Trust Report tab.
  The Overview page shows a migration health summary across recent jobs.
- Through the API: `GET /api/v1/jobs/{job_id}/parity-report`.
- Across a workspace: `GET /api/v1/parity/summary`.

## How the report is built

The report uses the evidence the run already stored: the Gate-8 comparison,
the verification ladder, transform and protection records, the quarantine and
the trust score. It then:

1. re-reads the cells of the keys that evidence names, up to a bound;
2. classifies every difference with fixed rules;
3. for sampled checks, calculates how much the sample could have missed;
4. proposes fixes from a fixed menu;
5. merges the decisions people have recorded.

The verdict is deterministic. The same evidence and the same decisions always
give the same report. The AI explanation only words the result; it does not
decide it.

## Reading the report

| Part | What it shows |
|---|---|
| Verdict | One of proven, explained, no difference found, unexplained, unverifiable. See [verification.md](verification.md#reading-the-verdict). |
| Confidence | Whether every row was compared, a sample, a mirror/SCD2 read-back, or a mix. For a random sample: how many rows, how many differed, and the upper bound. For any other sample: the rows checked and how they were chosen, and no bound. |
| Tables | Per table: the check that ran, row counts on each side, rows that differ, how many are explained and unexplained. |
| Differences | Each difference with its cause and whether that cause explains it. |
| Fixes | Proposed actions and their state. |
| Sign-off | Required roles, who has signed, and why signing is blocked if it is. |

A table shows "unverifiable" when only row counts were compared, when nothing
was compared, or when the comparison stopped naming keys before the end.

A mirror or SCD2 table whose Gate-8 digests matched shows the check
`readback`: the digest of every live destination row, re-read after the write
(a streamed `SELECT`, not the write pass), matched the digest of the rows the
run read from the source. Its verdict is "no difference found", never
"proven": the source side is the run's own read and a digest names no row.

## Asking for an explanation

`POST /api/v1/jobs/{job_id}/parity-report/explain` (needs `job.run`) returns a
plain-language explanation of the report. It restates the deterministic
result. The report and its verdict do not depend on it.

## Fixes

Fixes come from a fixed menu. Nothing is applied without a person's approval.

| Fix | When it is offered | What it does |
|---|---|---|
| Widen column | A value was cut for scale or length, and the destination supports widening in place | Changes the destination column type |
| Re-sync keys | Rows are missing or wrong on a keyed table | Re-writes the named keys through the normal upsert path, up to a per-fix limit |
| Re-run | The differing keys cannot be named | Starts the job again |
| Accept | Any unexplained difference | Records the difference as accepted, with the approver's note |

Approve with `POST .../parity-report/fixes/{fix_id}/approve` and dismiss with
`.../dismiss`. Both need `job.manage`.

After a fix that writes, the affected keys are checked again on both sides and
the result is recorded as a round. A job can have at most three applied write
fixes.

An accepted difference still counts in a sample's difference rate. Accepting
covers the rows a person looked at, not the rows the sample stands for.

## Sign-off

Sign-off opens only when:

- no unexplained difference remains;
- the verdict is not "unverifiable";
- any sample used was a random draw that gives a bound (see
  [verification.md](verification.md#which-samples-are-random) for which
  routes draw one);
- no fix is in the middle of being applied.

Otherwise the report says why it is blocked.

A mirror or SCD2 report whose check is `readback` can be signed. What is
signed is the verdict "no difference found", never "proven", with the
statement that the source side is the run's own read.

### Who may sign

There are three sign-off roles. Each is tied to real authority in the job's
workspace, not chosen from a list.

| Sign-off role | Who may sign by default |
|---|---|
| `engineering` | operator, editor or admin |
| `data_owner` | editor or admin |
| `compliance` | admin |

Change the mapping with `DATAWRAP_PARITY_SIGNOFF_AUTHORITY` (a JSON object).
It can also name the people allowed to sign a role. A sign-off role missing
from the mapping cannot be signed. A mapping that does not parse refuses every
signature.

Rules:

- The route needs `job.manage`: `POST .../parity-report/signoff`.
- One person signs one role.
- An approval records the signer's authority, the report version and a hash of
  what was signed.
- An approval counts only while the report still has that content. If the
  verdict, the differences or a fix changes, the approval is shown as
  invalidated and the role signs again.

When every required role has signed the same content, the report is signed
(HMAC-SHA256) with the job workspace's evidence signing key and stored on the
job. The signature records the key id. A signed report is final. No later
decision changes it.

The signed report holds the verdict, the confidence statement, the tables,
every difference with its cause, the fixes proposed or applied (a Widen column
fix with the `ALTER TABLE` it previews), the approvals, and the evidence
*status*: the Gate-8 result, the assurance level, the trust score and whether a
transform or PII protection proof passed. It does not hold the transforms
themselves, the destination schema or the DDL the transfer ran.

## Exports

`GET /api/v1/jobs/{job_id}/parity-report/export` returns the report for
download. The Markdown export is rendered from the signed pack and includes
the verdict, confidence, tables, differences, approvals and signature.

The signed pack uses the same envelope that
`POST /api/v1/transfer/proof-pack/verify` checks. That route needs `job.read`,
so anyone with access to the job's workspace can verify the pack. See
[verification.md](verification.md#how-evidence-is-signed-and-verified).

## Limits

- The report can only be as strong as the comparison Gate-8 ran. A sampled run
  is never reported as proven.
- The re-read of cells is bounded. It covers the keys the evidence names.
- The signing key is the workspace's evidence key, a shared secret this
  deployment holds. The signature shows the report was not changed after
  signing. Only this deployment can check it: there is no public or offline
  verifier, and it is not an independent third party's signature. That is
  planned work (the evidence ledger), not a feature of this code line.
- Multi-table runs: Gate-8 compares values for the last table only, so every
  other table is "unverifiable" (count only) and the report cannot be signed.
- Only the streaming database route draws a random sample. A sample from any
  other route bounds nothing, so the report cannot be signed until a full
  compare runs.
- A busy source drifts: a key Gate-8 named that both sides now hold equal
  cannot be re-checked and stays unexplained, so a live system's report can
  be hard to sign. That is deliberate.
- Sign-off roles are bound to workspace roles, not to an external identity
  system.
- Internal bookkeeping fields are removed from every response and export.

## Not covered

- The exact per-fix key limit and the formats the export route accepts.
- MCP: only reading and explaining a report is available as a tool
  (`explain_parity`). Approving a fix and signing are not.
- Reversibility and exit rehearsals. They are part of this code line and
  described in [exit-readiness.md](exit-readiness.md). An executable rollback
  or fall-back run is not.

## Verified against

- `apps/api/src/routers/parity_router.py`
- `apps/api/services/evidence_signing_keys.py`, `apps/api/services/signed_proof_pack.py`
- `apps/api/services/parity_report.py`, `apps/api/services/parity_fixes.py`,
  `apps/api/services/parity_signoff.py`, `apps/api/services/parity_authority.py`,
  `apps/api/services/parity_state.py`, `apps/api/services/parity_stats.py`
- `apps/api/src/ai/copilot/tool_permissions.py` (the `explain_parity` tool)
- `apps/web/src/components/parity/` (Trust Report tab, sign-off panel, health section)
- `apps/api/tests/fixtures/rbac_route_permissions.json`
