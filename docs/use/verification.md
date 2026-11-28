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

Only the server signs a contract: the signature is keyed with the
deployment's signing key, so a contract written or edited by a client (for
example one naming someone else as the approver) is refused wherever it is
read: Validate, Execute, a schedule run and the proof pack. With a signed-in
user, the contract names that user as the approver, whatever the request
says. Each signing writes an audit event (who, workspace, column, table,
kind, reason and the signature; no data values). Contracts approved before
this release no longer count and must be re-approved: each one is listed in
the start log, at `GET /api/v1/ops/risk-contract-upgrade-report` and as a
`risk_contract_reapprove` warning in Validate (see
[backup and upgrade](../operate/backup-and-upgrade.md)).

**Oracle and empty strings.** Oracle stores `''` as NULL, so an empty string
cannot go into a `NOT NULL` column (`ORA-01400`). When the destination is
Oracle, Validate counts the empty strings in each source text column that will
be `NOT NULL` there, the same way in Studio, over the API, in Pilot and at
Execute. A SQL source counts them itself; otherwise Validate reads the rows the
run will read (with its filter, incremental window and transform). If there
are none, nothing is shown. If there are some, the run is blocked and the gate
names the column and the count. If Validate could not count them exactly (only
a preview could be read, the scan stopped, the time ran out, or the source or
its catalog could not be read), the column is blocked too, and the gate says
why empty strings could not be ruled out.

A column whose type cannot hold `''` is ruled out from the source catalog,
without reading a row, however large the table: numbers, dates and times,
booleans and uuids; on PostgreSQL also an enum whose labels do not include
`''`, `json`/`jsonb` (read as JSON text), arrays and `inet`/`cidr`/`macaddr`;
on MySQL also `JSON`. A type that can hold `''` is always checked: text and
char types, an enum with a `''` label, `tsvector`, `xml`, a MySQL `ENUM` or
`SET`, MariaDB's `JSON` (a text column), and every SQLite column (SQLite's
declared types do not restrict what is stored).

Validate's check has 60 seconds in total. The columns you asked to check now
go first, then the columns the preview already shows empty, then the rest,
each with an equal share of the time left (what one column does not use goes
to the next), so one slow column does not use up the others' time. A column
the time does not reach is blocked with the reason and the actions below.

Every block lists what you can do:

- **Map `default:<value>`** on the column: empty and blank values are written
  as `<value>`, everything else is unchanged; Validate then needs no read. A
  mapping applies one transform, so this is not offered on a column that
  already maps another transform (it would stop that transform). Change the
  transform in Map first.
- **Check this column now** (`risk_acceptance: {"option":
  "oracle_empty_check_now", "columns": [...]}`): a longer read, within a total
  of 5 minutes for the check, the asked columns first. Datawrap issues the
  request itself, signed and bound to the column pair, the destination
  (connector, schema and table), the workspace and your signed-in user; it
  lasts at most an hour from when it was issued. A request written by hand
  (or edited) in the API body is ignored. A column can be checked this way
  once every 10 minutes per destination table, unless the last check ran out
  of time; until then the gate says when it can be asked again. Not offered when the source catalog cannot be read: fix the
  connection. On BigQuery, Snowflake, Databricks, ClickHouse and Trino this
  is the only way Validate reads the column, because the warehouse bills the
  scan. There, the result is reused for 24 hours so a schedule does not pay
  for the scan again, and the gate says so (status "warn"): the column was
  checked at `<time>`, the result is being reused until `<time>`, and how to
  check again now.
- **Replace the empty values in the source**, then validate again.

In Studio these are listed under "Oracle empty strings" on the Validate
screen. Map `default:<value>` asks for the value; Create NULL-able asks for
a reason and is signed by the server in your name; Check now and Check again
are issued by the server; Fix the connection opens Connectors. Each then runs
Validate again. Replacing the source's values is something only you can do,
so it is listed as text, not as a button. Over MCP, Pilot and the API, send
the same `risk_acceptance`.

A found empty string is not forgotten: when a later, shorter check stops
before the row where an earlier check found it (within 24 hours, same
source, scope and mapping), the column stays blocked with that finding, the
gate says when it was checked, and Check again reads it afresh.

Execute checks again unless it is confirming the Validate you approved (same
workspace, destination, source and mapping, within an hour). An empty string
that reaches the source after the last check is refused by Oracle at write
time: that batch fails, and nothing is written as a different value.

And, per table:

- **New table:** one option, "Create `<col>` as NULL-able; empty strings land
  as NULL". Approving it signs a risk contract (over MCP:
  `risk_acceptance: {"option": "oracle_empty_as_null", "columns": [...],
  "approved_by": ..., "reason": ...}`). Only that column loses `NOT NULL`. After
  the write, Gate-8 lists the rows this run wrote under `explained_changes`
  with the contract's id, and the proof pack lists the contract. When the
  run's rows cannot be named (an append without a key list), Gate-8 reports
  "NULL cells in this column: N (table-wide)" and does not call them
  explained. A contract applies only to the column pair, the workspace, and the
  destination connector, schema and table it was signed for.
- **Existing table:** Datawrap never alters the column; use one of the
  actions above, then validate again.
- **Key column** (the run's key, or the destination's or source's primary
  key): cannot be made NULL-able; use one of the actions above.

When a route's mapping confidence is below the mode's floor, review the
mapping in the Map step and run Validate again. Nothing is written until that
gate passes.

### Dropping fields on purpose

A source column must be mapped or declared omitted; otherwise the run is
refused. That holds when the destination table already exists and lacks the
column, in every sync mode: the column is added when the schema policy approves
it (propagate columns, or backfill new fields) or the drift is acknowledged for
the run, and otherwise the run stops before the first write and names the
column. Under manual review the run never adds a column on its own to a table
that existed before the run, including a field a document source first shows
partway through the run: the run stops before that batch. A table the run
created is the run's own: a field first seen later is added to it and reported
as "added during the run" in the job summary and the Trust Report. A field you
declared omitted is never written, even if it first appears late.

In a job that moves several tables, each table is checked with the map it
writes with, at Validate and before it is written, and the reports name each
left-out column as `table.column`. A quarantine replay is checked the same way.

### Resuming with a decision

When a run stops on a field, the message ends with: "Nothing of that batch was
written; the batches before it are committed. Resume the job with schema_policy
set to propagate_columns (or backfill_new_fields true) to add the field, or with
the field in omit_columns to leave it out, and a reason; the resume continues
from that batch."

That is `POST /api/v1/connectors/jobs/{job_id}/resume` with a body such as
`{"schema_policy": "propagate_columns", "reason": "doc is a new product field"}`
or `{"omit_columns": ["doc"], "reason": "not needed downstream"}`. It needs the
job.manage permission (the one that approves destination changes). Who decided,
the reason and the decision are recorded on the job (`resume_decision`) and in
the audit log. `/rerun` takes the same decision as `schema_decision`.
`omit_columns` may name only the columns the stop named (the job's
`error_details.unaccounted_sources`); any other column is refused with 422,
because leaving out a column the job already writes would write the remaining
rows without its values. `/retry` runs the job as it was and refuses a
`schema_decision` with 422. A field met mid-run is added without a decision
only to a table this run created; a table whose existence could not be read
before the run counts as existing, so the run stops. In a multi-table job,
every SCD2, mirror and CDC table is judged with its own column map before the
first table is written.
A field flattened out of a document (for example `nested_city` from `nested`)
counts as carried when its parent object is written whole, and the reports say
"carried inside nested"; if the parent is transformed or not written, the field
needs its own decision. A run started without Validate
(quarantine replay, the SDK) gets the same check at Execute. In a job that moves
several tables, a table with more columns than the first is written with its
own columns.

A run that leaves a column out says so. Gate-8 compares the columns the run
wrote, so its message, the trust score's completeness note and the Trust
Report's confidence statement name each source column that was not migrated
and why (declared omitted, or left out under an acknowledged schema drift).

The field reduction ledger goes further and records why each dropped
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

### Verification after the write (experimental)

Behind `DATAWRAP_PARITY_SCALE=1`, a completed job whose table Gate-8 could
not compare in full (above the ladder limit) is verified after the write by
comparing every row inside both databases (see
[Parity at Scale](../reference/parity-canonical-encoding.md)).

- It runs only when the run read the whole table: a run that read an
  incremental window, a query or procedure, a limited number of rows, or
  rows its filter or recipe dropped or diverted is not compared as a whole
  table, and the report says why. Gate-8's own record then decides.
- It is its own state. While it runs the Trust Report reads
  **Verification running**; the job's write status is not changed, and
  sign-off and the "proven" verdict wait until it finishes. A verification
  whose worker stopped (a restart, a lost replica) does not stay "running":
  it reads as stopped, is resumed from its checkpoint when the app starts or
  on its next sweep, and **Re-verify** takes it over.
- The comparison inside the database runs on PostgreSQL 14 or later (it
  needs exact `numeric` results from `extract(epoch …)`). On an older
  server the table reads **Not verified at full scale**, with the server's
  version, and Gate-8's own result stands.
- **Proven** needs each side read from one snapshot for the whole comparison:
  PostgreSQL (one repeatable-read transaction) and MySQL / MariaDB on InnoDB
  (a consistent-snapshot transaction). Otherwise (SQL Server today, a
  resumed comparison, another storage engine) a clean result reads
  **no difference found**: the tables matched at scan time, but writes during
  the scan were not ruled out. An engine pair not certified for every column
  type reads the same way. Only a proven result becomes a full population
  proof in the signed proof pack, and only where Gate-8 itself passed with
  no fidelity veto.
- **What a held snapshot costs.** The setting "Consistent snapshot during
  verification" keeps one read-only transaction open on each database for
  the whole comparison, for at most "Longest snapshot hold". The
  deployment-wide setting (on, 30 minutes) is a platform admin's, under
  Settings, General, and it is a ceiling: off there is off for every
  workspace and schedule. Where it is on, each workspace can turn it off or
  on for its own runs and set a shorter hold (Settings, workspace enterprise
  settings), and a schedule can do the same. The hold used is the shortest
  one set. On MySQL / MariaDB the scan's login must be able to see other
  sessions (the PROCESS privilege — SUPER alone is not enough — or on
  MySQL 8 a readable `performance_schema` with the metadata-lock
  instrument on). This is probed from the watching connection before the
  snapshot is held; if it cannot be proven, nothing is pinned and the
  result says why. While it is open:
  - PostgreSQL cannot vacuum rows deleted or updated after the snapshot (the
    xmin horizon is held back), so tables with heavy writes grow until it
    ends;
  - MySQL / MariaDB InnoDB keeps undo history for the snapshot, so the undo
    log grows on a busy server;
  - on a PostgreSQL hot standby, replication can cancel a long snapshot
    (a recovery conflict), so a standby is never pinned;
  - the snapshot holds a lock on the table it has read: PostgreSQL an
    `AccessShareLock`, MySQL / MariaDB a shared metadata lock. A `TRUNCATE`,
    a table swap or rename, or DDL needs an exclusive lock and would wait
    for the snapshot (and everything queued behind it would wait too). So a
    second connection watches for any session waiting on those locks
    (`pg_blocking_pids`; MySQL `performance_schema.metadata_locks`, or on
    MariaDB the process list's "Waiting for table metadata lock"), about
    once a second, and the comparison lets its snapshot go as soon as one
    waits: the waiting statement proceeds within about a second, the
    comparison continues statement by statement, and the result reads
    **no difference found** with "released the snapshot so session … could
    proceed". Where no second connection can watch, nothing is pinned.
  Past the longest hold the comparison lets the snapshot go and continues
  statement by statement; a server whose `idle_in_transaction_session_timeout`
  (or `transaction_timeout`) is shorter than the hold is not pinned at all.
  Either way the result reads **no difference found**, with the reason. The
  Trust Report shows, per table, whether each side was read from one snapshot
  and for how long. The two snapshots (source and destination) are taken a
  few milliseconds apart; the report states the gap.
- What Gate-8 explained stays explained: quarantined rows, rows skipped by
  the duplicate policy and rows that were in the destination before an
  append are not turned into differences. A missing row is explained by
  quarantine only when its key is one the run recorded as quarantined; a
  missing row whose key the run did not record stays unexplained (the held
  row may be one the destination still has). Only missing rows beyond the
  keys the report keeps are matched to held rows by count, and the report
  says so.
- A text key is read in key ranges only when it orders the same way on
  both databases: a binary collation on both sides (PostgreSQL `C`,
  `POSIX`, `ucs_basic`, `pg_c_utf8`; MySQL `binary` or `utf8mb4_0900_bin`;
  MariaDB `utf8mb4_nopad_bin`), read from each catalog. Otherwise the table
  is compared by hash buckets, and its streamed columns (transformed,
  crosswalked, floats across engines, JSON) are read in one pass. On MySQL /
  MariaDB that one read is a single statement limited to the stream timeout
  (30 minutes), so a table too large to stream within it is reported as not
  fully compared, with that reason; the Trust Report's coverage says how the
  pass was read.
- Rows whose key is NULL fall in no key range: they are compared as whole
  rows (content), reported as "rows without key", and never attributed to a
  key. Before any match, each side's units must add up to its row count;
  if they do not, the table is not fully compared and the report says why.
- A table with no key is compared as content: the check reads
  **Content match (no key)**, and a difference cannot be attributed to a key.
- A verification that found differences makes the report **unexplained** and
  names the rows.
- On a schedule, the watermark moves only after verification passed (and
  only forward), and the next run waits while it runs (set
  `wait_for_verification` to false on the schedule to not wait).

The Trust Report's difference explorer shows, per verified table, each
column's differing rows (Columns), the missing, extra, duplicate and changed
keys (Keys), and the differing values read again from both sides (Values).
Protected columns never show a value, and their values are never stored with
the verification. **Re-verify this table** compares every row again.
**Copy repair SQL** shows statements that make the destination match the
source for the named keys, with the run's transforms applied; it leaves out
quarantined rows, rows outside the run and protected columns, and says so.
Each UPDATE and DELETE carries the destination row's values as read when
the statements were generated, so a row changed since is left alone; an
extra row is deleted only if the source still does not hold its key when
the statements are generated. A changed column of a type with no exact
equality (floats, JSON, XML, geometric, time-zone timestamps) is not set
by an UPDATE, since a newer value could not be detected: it is listed for
review by hand with its keys. MySQL / MariaDB string literals that hold a
quote or backslash are written in hex, so they mean the same with or
without `NO_BACKSLASH_ESCAPES`. Datawrap never runs them.

Tested end to end on PostgreSQL to PostgreSQL. Other engine pairs use the same
comparison but are not yet tested end to end in this mode.

### What a passing checksum does not prove

- It does not prove referential integrity between tables.
- It does not prove the source was right.
- It does not prove anything about rows written to the source after the read.
- It does not prove the mapping means the right thing. `first_name → last_name`
  copies perfectly and verifies perfectly. The Mapping Witness below checks that.

## Mapping Witness (before the write)

On the Map step, Datawrap checks each mapped pair against a bounded sample of
the data (at most 500 rows) and flags pairs to review:

- **Destination already holds the rows** (an existing table, read by key): a
  pair whose values sit in another column is flagged as a likely swap or wrong
  target. A pair that matches only after ×100 is flagged as a unit mismatch.
  Dates that match only when read in the order the run does not use are
  flagged as a format mismatch. The read names its columns and joins on the
  destination's key; a warehouse (BigQuery, Snowflake, …) is read only when you
  choose Check mapping.
- **Before load** (new table, or no key to join on): the values are checked
  against what the destination column promises. Examples: user names headed
  for an `email` column, fractional amounts headed for a `*_cents` column,
  surnames headed for `first_name` while given names go to `last_name`.

A flag is evidence with a reason; the witness never changes a mapping, and it
prefers silence to a wrong flag. "Witnessed" needs agreement beyond chance and
no other column that matches better. A column with one distinct value proves
nothing ("Consistent"). A pair the mapping transforms (mask, hash, an
expression) is "Not comparable". A destination column that was not read (not
in the table yet, a name the sampler cannot address as written, or past the
columns one check reads) is "Not read". A pair it cannot judge says "No
evidence", not "passed". Results carry column names and counts, never values.

When a job is submitted, the evidence for exactly the mapping and destination
it runs is frozen onto the job. The Trust Report and the AI decisions ledger
read only that copy.

A job takes evidence only from the Map session it was submitted from, for its
own source and destination, at most a day old; the Trust Report says who ran
the check and when. A flag counts as kept only when someone chose Keep this
mapping on it; continuing past it is recorded as not reviewed. A Keep stays
with its flag when the same inputs are checked again, and only then: a flag
that points at another column, gives another reason or other counts, or comes
from an edited source (same file name and row count, other contents) is a new
flag and shows again. A file or object is named by its whole path, letter case
included; a table or query by its text, where spacing and keyword case fold
outside quotes and a name's case folds only on engines that fold unquoted names
(PostgreSQL, Redshift, Oracle, Snowflake, SQL Server, SQLite). Map approvals and
changed flags count only for the source they were given on: switching the
file or table in one session starts them over. The session is only a key:
the people named are the ones who checked, approved and ran.

Evidence of a session still open is never pruned to make room for other
checks; older evidence goes first. If a workspace's hard limit ever removes
it, the job says "Evidence expired", not that no check matched. Editing the
pairs in an open session replaces that session's earlier check of the same
source and destination, unless a job was already submitted with it. A check
that ran past its time limit is not recorded; if it was recorded just before
the limit, you get its result rather than a time-out.

Measured on a synthetic benchmark written for the witness (120 rows per table,
at least 30 distinct values per column, the destination holding the same rows
with trailing spaces, other letter case, other number spellings, other time
zones and a few stale rows): no false flags on the correct mappings, and 20 of
20 injected swaps flagged on both sides. These are the CI floors.

The held-out mapping benchmark (206 matchable columns, 23 tables) gives no
usable witness figures: 203 of its 231 columns carry a single sample value,
and the destination there is an exact copy, so a pair can only come out
"consistent". Its false-flag count (0) and swap recall (19 of 399) are
reported, not claimed. Command:
`python apps/api/scripts/witness_holdout_eval.py <benchmark>`.

Until that held-out file (`tests/fixtures/mapping_holdout_v1.json`) is on the
branch, CI runs the held-out gate with a synthetic stand-in
(`tests/fixtures/witness/holdout_minimal_synthetic.json`) and the job summary
says "witness held-out: synthetic fixture — real held-out set not yet on this
branch". Once the real file is present the script uses it on its own.

## Coverage: which comparison ran

Each table in a result carries the kind of check that ran.

| Check | Meaning | Strength |
|---|---|---|
| `full_checksum` | Every row compared by checksum | Proof for the rows compared |
| `key_diff` | Every row compared by key | Proof for the rows compared |
| `segment_diff` | Every row compared inside both databases, by key (verification after the write) | Proof for the rows compared, on a certified engine pair |
| `multiset_digest` | Every row compared as content, for a table with no key ("Content match (no key)") | Proof the contents match; a difference cannot be attributed to a key |
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

- **Oracle zone columns.** Instants are written as UTC digits on a session
  pinned to UTC. The `WITH TIME ZONE` column then holds `+00:00`, not the
  source's offset; only a server-side Oracle-to-Oracle copy keeps the offset.
  Validate reports this as a loss it accepts on your behalf, not as a
  difference. Rows written by a release before the pin may hold shifted
  instants. Validate names such a table in one line, and a full re-run with
  overwrite corrects it; see
  [Backup and upgrade](../operate/backup-and-upgrade.md).
- **MySQL and MariaDB `DATETIME`.** An instant is stored as UTC digits in a
  column with no zone. Validate says so for that column; it is not a
  difference.

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
  it was made, the job or report id and the workspace (for an export record
  filed without one, the job's workspace). A stored parity or exit pack is
  recorded only when it was signed before the cutover, its body still hashes
  to its digest and its login-secret signature checks out. Records are only
  ever added, and the audit retention purge does not touch the register. A
  store that cannot be read during the snapshot is named on the snapshot
  record and read again at most three times, no more often than every 15
  minutes; its packs are refused until a retry reads it. After the third
  failed retry they stay refused for good, unless an operator reads the store
  again once it is back: `python -m services.legacy_evidence_register retry`
  (run from `apps/api` with the API's environment). That adds the packs it
  finds and prints what it could still not read. A
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
