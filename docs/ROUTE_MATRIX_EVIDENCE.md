# Route matrix evidence — PostgreSQL source × {PostgreSQL, MySQL, Oracle} destination

Harness: `apps/api/scripts/live_migration_scenario_matrix.py`
Artifact: `/home/ubuntu/repro/migration_scenario_matrix_results.json`
Services: local `df-pg` (5433), `df-mysql` (3307), `df-oracle` (1521, `FREEPDB1`).

Every scenario creates real source rows, a real destination table, runs
`UniversalTransferEngine.execute_tracked`, and then counts the destination
independently. Nothing is asserted into green; a refusal is recorded as a
refusal and its message is kept verbatim.

## Counts — 45 runs, all three destinations live

| Outcome | Count |
| --- | --- |
| Wrote and reconciled (dest COUNT(\*) == source rows) | 20 |
| Refused before the write (fail-closed, as designed) | 25 |
| Engine / SQL error | 0 |
| Unexplained / silently wrong | 0 |

Per destination: PostgreSQL 7 wrote / 8 refused, MySQL 7 / 8, Oracle 6 / 9.
Oracle refuses `json_column` because this matrix maps `JSONB → TEXT` there,
which is a real fidelity collapse and not a skip.

## Schema and type dimension (per destination)

| Scenario | PostgreSQL | MySQL | Oracle |
| --- | --- | --- | --- |
| 30 source columns → 20-column destination, extras unmapped | refused (G13 names all 10) | refused | refused |
| Same, extras declared `intentional_omit` | wrote 5 | wrote 5 | wrote 5 |
| Destination NOT NULL column with no source mapping | refused (G14 names `tenant_id`) | refused | refused |
| Destination has an extra nullable column | wrote 5, untouched column NULL | wrote 5 | wrote 5 |
| `TEXT` → `VARCHAR(64)` with longer values | refused (fidelity, names the path) | refused | refused |
| `DECIMAL(18,6)` → `DECIMAL(10,2)` | refused (fidelity) | refused | refused |
| Duplicate source keys | refused (identity) | refused | refused |
| Boolean representation | wrote 3 | wrote 3 (`1`/`0`/NULL) | wrote 3 |
| JSON column | wrote 3, structure intact | wrote 3 | refused (`JSONB → TEXT`) |
| `TIMESTAMPTZ` → naive timestamp | refused (fidelity) | refused | refused |
| Zero-width / control characters in text | refused — **encoding** root | refused — fidelity root (MySQL destination also narrows `TEXT → VARCHAR`) | refused — fidelity root (`VARCHAR2(400 BYTE)`) |
| Case-sensitive quoted destination identifiers | wrote 3 | wrote 3 | wrote 3 |

## Sync-mode dimension — the same batch run twice, destination counted each time

| Sync mode | Declared contract | PostgreSQL | MySQL | Oracle |
| --- | --- | --- | --- | --- |
| `full_refresh_overwrite` | converge on source cardinality | 3 → 3 | 3 → 3 | 3 → 3 |
| `incremental_deduped` | converge on source cardinality | 3 → 3 | 3 → 3 | 3 → 3 |
| `full_refresh_append` | second append of the same keys must be refused before the write | refused, destination stays 3 | refused, destination stays 3 | refused, destination stays 3 |

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
4. **`full_refresh_overwrite` silently behaved like an append on Oracle.**
   `DROP TABLE IF EXISTS` is a syntax error there, so the drop fell through to a
   fallback whose unquoted `Table()` name folded to `SCN_DST`; `checkfirst` read
   the folded name as absent and the drop became a **no-op that reported
   success**. The overwrite then loaded on top of the previous generation of
   rows — `ORA-00001` here because the table had a key, and silently doubled
   data on any table without one. The conditional drop is now emitted in each
   dialect's own spelling (Oracle PL/SQL `-942` guard, SQL Server `OBJECT_ID`),
   the fallback quotes the name, and every drop is **verified against the
   catalog**: a table still present after a "successful" drop raises instead of
   letting the caller treat the destination as cleared.
5. **The overwrite recreated the destination under a different name.** After the
   drop, a table that does not exist is created folded (`SCN_DST`), which is
   right for a first load and wrong for an overwrite: a quoted lower-case
   destination came back as a *different object* and anything reading the old
   identifier found nothing. The pre-drop spelling is now captured before the
   drop and carried to the writer through one owner
   (`connector_dispatch.writer_extra_kwargs`), so both the adapter and the
   streaming path recreate the same object. A genuine first load still folds.
6. **`ORA-00904` on destination columns that plainly exist.** Mapped targets are
   bound to the spelling the catalog stores, but that probe passed a bare
   lower-case *table* name, which Oracle folds — so a table created as
   `"scn_dst"` reported no columns, every target kept its Map spelling, and the
   write asked for `"email"` beside the stored `EMAIL`. The probe now retries
   with the quoted name. This defect blocked every Oracle sync-mode route.
