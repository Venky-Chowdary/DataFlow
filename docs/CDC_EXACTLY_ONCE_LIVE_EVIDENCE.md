# CDC exactly-once — live-engine evidence

What this document records: the dest-owned watermark exactly-once protocol, run
against **live PostgreSQL, MySQL, Oracle and SQL Server destinations** with
crashes injected inside the apply transaction. It replaces the previous position,
which was that the protocol was implemented and proven on SQLite only.

What it does **not** claim: platform-wide exactly-once.
`PLATFORM_EXACTLY_ONCE_CLAIMED` stays `False`, the delivery default stays
at-least-once upsert, and only a route that opts in *and* whose destination can
commit apply + watermark in one transaction is exactly-once. DuckDB,
`generic_sql` and Snowflake are listed as wired but are **not** measured here, so
they stay unproven rather than assumed.

## Harness and artifact

| Item | Path |
| --- | --- |
| Harness | `apps/api/scripts/live_cdc_exactly_once_proof.py` |
| Artifact | `/home/ubuntu/repro/cdc_exactly_once_live_results.json` |
| Regression tests | `apps/api/tests/test_cdc_exactly_once_live_engines.py` |

Services: `df-pg` PostgreSQL 5433, `df-mysql` MySQL 3307, `df-oracle`
Oracle Free 1521 (`FREEPDB1`), `df-mssql` SQL Server 2022 1433. Tests skip when a
port is unreachable; they never report green by absence.

## Measured results — 9 scenarios × 4 engines

Every row below is the destination's own state, read back after the apply with
the engine's `COUNT(*)` and the dest-owned watermark row, not a writer
acknowledgement.

All four engines returned the same measured state for all nine scenarios; the
"measured" column is that shared result.

| Scenario | Contract | Measured (PostgreSQL / MySQL / Oracle / SQL Server) |
| --- | --- | --- |
| `clean_apply_net_effect` | 3 batches reduce to the net effect, watermark at last LSN | 2 rows `[(1,a2),(3,c)]`, `0/300` |
| `redelivery_same_batch_noop` | Same LSN twice more writes nothing | `already_committed`, 0 rows written, 1 row |
| `stale_lsn_dropped` | Older LSN after a newer commit cannot resurrect a value | dropped, row stays `new`, watermark `0/200` |
| `crash_before_commit_rolls_back` | Crash after apply/watermark, before COMMIT leaves nothing | 1 row, `0/100`; retry → 2 rows, `0/200` |
| `crash_after_commit_replay_noop` | Crash before source ack; replay is a no-op | 1 row, value `b` |
| `zombie_fence_refused` | Stolen-lease writer cannot commit | refused `exactly_once_stale_writer_fence` |
| `same_lsn_different_payload_refused` | Same LSN, different payload is a conflict, never an overwrite | refused `exactly_once_checksum_mismatch`, dest unchanged |
| `bundle_atomic_across_streams` | N tables + one LSN in one txn; crash rolls back every stream | 1/1 after crash, 2/2 after retry |
| `open_raises_fence_without_apply` | Open raises the fence with no data write and returns the dest resume | fence 7 persisted, job cursor `0/900` rewound to `0/100`, count unchanged |

## Defects the live run exposed (all fixed, each with a regression test)

1. **Exactly-once into MySQL/MariaDB could never work.** Every identifier was
   quoted with ANSI double quotes, so the first statement of the protocol failed
   with `ER_PARSE_ERROR (1064)`. MySQL only accepts `"` for identifiers under
   `ANSI_QUOTES`. Quoting is now per engine family (`` ` `` MySQL, `[` T-SQL,
   `"` elsewhere) — the same rule the rest of the writers already followed.
2. **Exactly-once into PostgreSQL could never work either.** The watermark and
   destination tables were extended by issuing `ALTER TABLE ADD COLUMN` per
   column and swallowing the failure. On PostgreSQL a failed statement aborts
   the whole transaction, so the "already exists" error poisoned the apply
   transaction and every following statement raised
   `InFailedSqlTransaction` — including the `CREATE TABLE` for the destination.
   Missing columns are now read from the catalog and only genuinely absent ones
   are added; the one narrow SELECT retry that remains runs in its own savepoint.
3. **The multi-stream bundle was not atomic on MySQL.** DDL commits implicitly
   in MySQL, so the `CREATE TABLE IF NOT EXISTS` issued for the *second* stream
   committed the *first* stream's rows. A crash mid-bundle then left one stream
   applied and the rest rolled back — measured as 2/1 where the contract is 1/1.
   Schema preparation now runs in its own transaction before the apply
   transaction opens, on every engine.
4. **Ordinary recovery replay was refused as a payload conflict.** The
   dest-committed checksum describes the batch at the watermark, but it was
   compared against *any* already-committed LSN. A restart that replays a range
   of older batches — the normal at-least-once source behaviour this protocol
   exists to absorb — therefore failed the job with
   `exactly_once_checksum_mismatch`. The checksum guard now applies only when
   the incoming LSN equals the dest watermark; a strictly older LSN is dropped
   as superseded. Same-LSN tampering is still refused and quarantined.

5. **Exactly-once into Oracle could never work.** The watermark table is named
   `_df_cdc_eos_watermarks`, and Oracle rejects an unquoted identifier that
   starts with an underscore (`ORA-00911`). Since that DDL is the first statement
   of any apply, every CDC batch to an Oracle destination failed. Oracle now
   hosts the same table as `df_cdc_eos_watermarks` (`WATERMARK_TABLE_ORACLE`).
6. **Additive column detection was blind on Oracle.** Destination tables are
   created case-sensitively (`"eos_clean"`), but the catalog inspector was called
   with the bare lower-case name, which Oracle folds to upper case: it reported
   *no* columns, so every column was re-added and the apply died with
   `ORA-01430`. The lookup now retries with a quoted name before concluding a
   table has no columns.

Defects 1–3 and 5–6 mean the honest previous state was: exactly-once was
selectable on PostgreSQL, MySQL, Oracle and SQL Server, and would have failed on
the first batch everywhere except SQL Server. It is now measured working on all
four. SQL Server passed all nine scenarios on its first live run.

## Still unproven

- Azure SQL, DuckDB, `generic_sql` and Snowflake EOS routes: wired in code, no
  live run.
- Full source-side loop (real logical-replication/binlog reader → EOS apply →
  ack) is exercised per-batch here, not as a long-running stream with slot
  restarts.
- Snapshot→stream handoff and incremental-snapshot window closure are proven on
  SQLite fixtures; the live scenarios above cover streaming batches, fencing and
  bundle atomicity.
