# Re-sync into a non-empty destination — measured evidence

Harness: `apps/api/scripts/live_resync_matrix.py`
Raw artifact: `/home/ubuntu/repro/resync_matrix_results.json`
Engines exercised live: PostgreSQL 5433, MySQL 3307 (source is always PostgreSQL).
Path exercised: `UniversalTransferEngine.execute_tracked` — preflight gates, write, Gate-8.

Every scenario seeds the destination **before** the run, declares the contract it
expects, and the harness records whether the measurement met it. Nothing is
asserted into green: a scenario that disagrees with its contract is reported as a
gap.

## Result

| Scenario | Sync mode | Dest before → after | Verdict | Contract met |
| --- | --- | --- | --- | --- |
| empty destination + append | incremental_append | 0 → 3 | pass | yes |
| unrelated rows held + append | incremental_append | 2 → 5 | pass | yes |
| destination already holds these keys + append | full_refresh_append | 3 → 3 | refused pre-write | yes |
| upsert, identical values | incremental_deduped | 3 → 3 | pass | yes |
| upsert, changed values | incremental_deduped | 3 → 3, values replaced | pass | yes |
| upsert, new keys | incremental_deduped | 3 → 5 | pass | yes |
| upsert run twice | incremental_deduped | 3 → 3 | pass | yes |
| overwrite over unrelated rows | full_refresh_overwrite | 2 → 3, pre-existing gone | pass | yes |
| same batch appended twice, keyless dest | incremental_append | 0 → 6 | pass | yes |
| duplicate source identity keys + upsert | incremental_deduped | 0 → 0 | refused pre-write | yes |

**20 / 20 scenarios met contract on both engines** (10 scenarios × 2 destinations).
Oracle is available in the harness (`DESTS`) but not run here; a skipped engine is
honest, an invented green is not.

The three refusals are distinct verdicts, not one generic block:

* *destination already stores these keys* — `3 key value(s) in this batch are
  already at rest in the destination on id, which enforces uniqueness — a
  full_refresh_append insert aborts on the first one`. Destination collision.
* *duplicate identity keys* — two source rows share `id`; the run is refused
  before a write picks one of them at random. Source-side duplication.
* Neither is confused with the append delta, which is what proves the runs that
  do land.

## Defect this matrix found and fixed

**A correct append failed itself when the destination already held rows with the
same keys.** For an append/upsert into a non-empty table, Gate-8 re-reads the
destination `WHERE pk IN (written keys)` so the digest covers this run's batch
instead of the whole table. That scope is only comparable when the key
identifies *one* row: appending the same batch twice into a destination without
a unique constraint leaves two rows per key, the read-back returned 6 rows for a
3-row batch, and its digest could never equal the 3-row source digest. The run
failed with two hex strings on data that was exactly right — the same shape as
the customer's 710,000-row run.

Root cause: the key used to scope the digest could be an identity *inferred from
Map*, which says nothing about whether the destination allows a second row with
that value. The scope is now only adopted when the key identifies one row by
construction — a merge/upsert owns its conflict target, and a declared PK or
unique constraint rejects the duplicate. An append on an unenforced key keeps the
destination delta (`after − before == expected`) as its identity, with per-cell
fidelity explicitly not claimed.

Regression: `apps/api/tests/test_keyed_scope_requires_unique_keys.py` (4 tests);
the upsert routes that legitimately use the keyed scope (SQLite, MySQL/MariaDB,
MongoDB, Snowflake, declared-destination-key) stay green — 824 passed, 89 skipped
across the reconcile/checksum/append/upsert/merge selection.

## Honest limits

* Append proves **cardinality** (delta), not per-cell fidelity. Full-table
  checksum proof requires overwrite or a keyed upsert, and the report says so
  rather than implying more.
* The duplicate-source-identity refusal is correct but its wording still reads
  `impacts 3 gate check(s)` instead of naming the key and the colliding values.
  That is a message gap, tracked, not a verdict gap.
* Scenarios use small fixtures (2–6 rows) to isolate semantics. Volume behaviour
  (checkpointing, resume, million-row runs) is covered by separate artifacts.
