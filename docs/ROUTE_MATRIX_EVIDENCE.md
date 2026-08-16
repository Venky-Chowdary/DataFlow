# Route matrix evidence — PostgreSQL source × {PostgreSQL, MySQL, Oracle} destination

Harness: `apps/api/scripts/live_migration_scenario_matrix.py`
Artifact: `/home/ubuntu/repro/migration_scenario_matrix_results.json`
Services: local `df-pg` (5433), `df-mysql` (3307). Oracle was not reachable.

Every scenario creates real source rows, a real destination table, runs
`UniversalTransferEngine.execute_tracked`, and then counts the destination
independently. Nothing is asserted into green; a refusal is recorded as a
refusal and its message is kept verbatim.

## Counts — 45 runs

| Outcome | Count |
| --- | --- |
| Wrote and reconciled | 14 |
| Refused before the write (fail-closed, as designed) | 16 |
| Skipped — Oracle service unreachable (`DPY-6005`, connection refused) | 15 |
| Unexplained / silently wrong | 0 |

Oracle is **not** certified by this run. It is skipped, and stays skipped until
the service is up.

## Schema and type dimension (per destination)

| Scenario | PostgreSQL | MySQL |
| --- | --- | --- |
| 30 source columns → 20-column destination, extras unmapped | refused (G13 names all 10) | refused |
| Same, extras declared `intentional_omit` | wrote 5 | wrote 5 |
| Destination NOT NULL column with no source mapping | refused (G14 names `tenant_id`) | refused |
| Destination has an extra nullable column | wrote 5, untouched column NULL | wrote 5 |
| `TEXT` → `VARCHAR(64)` with longer values | refused (fidelity, names the path) | refused |
| `DECIMAL(18,6)` → `DECIMAL(10,2)` | refused (fidelity) | refused |
| Duplicate source keys | refused (identity) | refused |
| Boolean representation | wrote 3 | wrote 3 (`1`/`0`/NULL) |
| JSON column | wrote 3, structure intact | wrote 3 |
| `TIMESTAMPTZ` → naive timestamp | refused (fidelity) | refused |
| Zero-width / control characters in text | refused — **encoding** root | refused — fidelity root (the MySQL destination also narrows `TEXT → VARCHAR`, so both findings are real) |
| Case-sensitive quoted destination identifiers | wrote 3 | wrote 3 |

## Sync-mode dimension — the same batch run twice, destination counted each time

| Sync mode | Declared contract | PostgreSQL | MySQL |
| --- | --- | --- | --- |
| `full_refresh_overwrite` | converge on source cardinality | 3 → 3 | 3 → 3 |
| `incremental_deduped` | converge on source cardinality | 3 → 3 | 3 → 3 |
| `full_refresh_append` | second append of the same keys must be refused before the write | refused, destination stays 3 | refused, destination stays 3 |

No mode silently behaved like another, and no re-run left a half-applied batch.

## Defects this matrix found, and what was changed

1. **Zero-width characters were reported as a type-fidelity collapse.** The path
   was `TEXT → TEXT`, so the recommended action ("remap the type") could not fix
   the data. Encoding findings now raise their own `encoding_normalization` root
   naming the column, the character (`U+200B`) and the transform
   (`strip_controls`), with quarantine as the alternative.
2. **`incremental_deduped` refused every route where the operator had not typed
   the key into the stream contract**, even when the destination table declared
   a primary key. The declared key is catalog evidence, so upsert now keys on it
   when the write covers every key column — and still refuses when it does not,
   because keying on an unwritten column would insert duplicates.
3. **Cross-test authentication leakage** made unrelated API tests return 401/403.
   Auth enforcement is now resolved on the auth module per request instead of
   through copies bound at import time.
