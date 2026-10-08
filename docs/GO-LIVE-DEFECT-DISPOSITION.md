# Go-live defect disposition

Source register: QA snapshot Thu 2026-10-08, build prefix `6436aaa38583`, recommendation NO-GO. 125 defect rows.

This file is the register plus two columns: `root_cause` and `fixed_or_not`. The full row text is `docs/GO-LIVE-DEFECT-DISPOSITION.csv`.

A unit test is not a live matrix. Nothing here is a go-live. CDC delivery stays at-least-once upsert.

## Counts

- not fixed: 75
- QA had already marked fixed — not re-run here: 19
- already in tree before this wave — needs QA retest: 14
- fixed in code, unit-proven — not a live QA retest: 12
- partly fixed in code: 5

## Every row

| id | priority | QA status | fixed or not |
| --- | --- | --- | --- |
| DEF-B-031 | P1 | OPEN | already in tree at f125e5df. Unit-proven there. QA build 6436aaa38583 does not contain it. Needs a QA retest. Not re-proven live here. |
| DEF-B2-001 | P1 | OPEN | already in tree at f125e5df: heap warns, enforced key blocks before write, insert create-new withholds PK/UNIQUE. Unit-proven there. Needs QA retest. |
| DEF-B2-007 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B2-006 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-CDC-COUNT-ONLY-RECONCILE | P1 | OPEN | fixed in code, unit-proven (test_cdc_source_image_count_scope_does_not_claim_full_checksum). Count-only now fails the job. A finished source-row fingerprint scan can still pass. Not a live QA retest. |
| DEF-B2-008 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B2-010 | P1 | OPEN | partly fixed in code, unit-proven (test_materialize_widens_copied_nvarchar_stamp_for_sqlserver). A bound SQL Server source now materializes VARCHAR(n) CHARACTER SET utf8mb4. A MySQL source NVARCHAR and an unknown engine stay the utf8mb3 alias on purpose. Not a live MySQL retest. |
| DEF-B2-012 | P1 | OPEN | partly fixed. _records_after_failure keeps the larger committed count (test_records_after_failure_keeps_a_committed_prefix). Enforced-key refuse-before-insert was already in tree at f125e5df. Rows committed by an orphaned run are not deleted. Not a live QA retest. |
| DEF-B2-014 | P1 | OPEN | fixed in code, unit-proven (test_ensure_product_lsn_column_on_an_existing_table). The column is ALTER'd before the physical check. A snapshot already committed on build 6436aaa38583 is not rolled back here. Not a live SQL Server retest. |
| DEF-R1-001 | P1 | OPEN | DATETIME2(7) work is in an earlier commit. QA on 6436aaa38583 still truncated. Not re-proven on a live SQL Server here. Not marked fixed. |
| DEF-R1-002 | P1 | OPEN | fixed in code, unit-proven (test_unicode_source_into_sql_latin1_varchar_is_a_fidelity_collapse, test_latin1_varchar_is_not_safe_by_declaration). The pair is now a fidelity collapse and the population scan does not skip it. A SQL Server VARCHAR source into the same column is not a collapse. Writer quarantine of U+90CE / U+0141 was already in the tree. Not a live SQL Server retest. |
| DEF-B-027 | P1 | OPEN | pin-before-drop and MySQL rename-aside are in 059974d8. Postgres overwrite that already DROP'd rows is not restored by rename-aside. Needs QA retest. Not claimed live-green. |
| DEF-C-020 | P1 | OPEN | reader_population cap is in 059974d8. Unit-proven. The Postgres table that was already emptied was not restored. Needs QA retest. |
| DEF-C-024 | P1 | OPEN | algorithm changes are in 059974d8. The QA job was API-cancelled; statements on the old process were not killed from this VM. Needs QA retest. |
| DEF-C-017 | P1 | OPEN | resume accounting is in 059974d8. Needs QA retest. |
| DEF-C-036 | P1 | OPEN | already in tree at f125e5df: unproven RI is a warning; measured orphans still block. Unit-proven. Needs QA retest. Rollback of a real block is unchanged. |
| DEF-B-028 | P1 | OPEN | already in tree at f125e5df: page size 200 and tables_truncated is reported. Unit-proven. Needs QA retest on a database with more than 50 tables. |
| DEF-C-041 | P1 | OPEN | fixed in code, unit-proven (test_business_soft_delete_is_not_a_hard_delete, test_sqlite_upsert_tombstone_drops_dest_count, test_completed_job_does_not_report_the_validate_root_as_rejected). CDC __deleted/__op still delete. Not a live QA retest. |
| DEF-C-043 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-025 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-026 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-010 | P1 | OPEN | already in tree before this wave. Needs QA retest on a build after that commit. |
| DEF-B-009 | P1 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-010 | P1 | OPEN | partly fixed in code, unit-proven (test_mongo_timestamp_to_timestamp_is_not_a_false_polarity_collapse). Postgres, Oracle, and SQL Server identical TIMESTAMP pairs are not a polarity collapse when the source engine is mongodb. MySQL bare TIMESTAMP stays lossy; create-new stamps DATETIME(3). Not a live Mongo retest. |
| DEF-A-001 | P1 | OPEN | fixed in code, unit-proven (test_sftp_host_key_refusal_is_not_called_an_auth_failure, test_sftp_preauth_close_is_not_called_an_auth_failure). A real bad password is still an authentication failure. Not a live SSH retest. Not a go-live. |
| DEF-C-031 | P1 | UNCONFIRMED-ENV-DEPLOY | same long-writer path as 059974d8. Status on the register is UNCONFIRMED-ENV-DEPLOY. Not re-run live here. |
| DEF-B2-009 | P1 | NOT RETESTED | not retested in this session. Count-only completion is now a failure (DEF-CDC-COUNT-ONLY-RECONCILE), which stops a silent complete, but it does not by itself apply the missed changes. |
| DEF-B-018 | P1 | NOT RETESTED | refusal of uca1400 on MySQL is in 0bbb1ffe. Register status NOT RETESTED. Not re-run live here. |
| DEF-A-007 | P1 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-034 | P1 | PARTLY FIXED | cancel-before-batch is in 059974d8. QA still saw writers continue on the old process. Needs QA retest on a build that contains the commit. |
| DEF-B-004 | P1 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-001 | P1 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-019 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-003 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-005 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-006 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-015 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-008 | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-CDC-PG-MYSQL-TSTZ-BIND | P1 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-B-003 | P2 | OPEN | fixed in code for the coercion report: it binds the plan source engine before analyze_coercion. Unit-proven that a bound postgresql engine is not lossy (test_pg_varchar_to_sqlserver_nvarchar_is_preserve_when_the_source_engine_is_bound). Not a live preflight retest. |
| DEF-B-005 | P2 | OPEN | fixed in code, unit-proven (test_bare_five_field_cron_is_a_schedule). The runner's validate_cron is the acceptor. Not a live create_schedule retest. |
| DEF-B-013 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-014 | P2 | OPEN | already in tree before this wave (parse_cadence). Unit-proven in test_weekdays_and_hourly_minute_keep_their_anchor. QA build 6436aaa38583 predates it. Needs retest. |
| DEF-B-017 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-019 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-021 | P2 | OPEN | guards are already in tree (_primary_key_csv, incremental_append is not rewritten). Not re-executed against QA Postgres/MySQL in this session. Needs QA retest. Do not treat as live-green. |
| DEF-B-022 | P2 | OPEN | fixed in code, unit-proven (test_root_cause_identity_sentence_is_not_a_null_quarantine_row, test_completed_job_does_not_report_the_validate_root_as_rejected). A completed job no longer reports that sentence as a rejected row. Not a live QA retest. |
| DEF-B-029 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-032 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B2-002 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B2-004 | P2 | OPEN | fixed in code, unit-proven (test_cdc_prepare_does_not_stage_mysql_grants_for_other_engines). SQL Server, Oracle, and TimescaleDB are not staged. MariaDB stages the replication grant and says gtid_mode is not applied. Not a live QA retest. |
| DEF-B2-005 | P2 | OPEN | fixed in code, unit-proven (test_timescaledb_cdc_contract_blocks_even_when_format_was_aliased, test_capabilities_name_the_cdc_sources). g9 blocks timescaledb. Capabilities list cdc_capable_sources and state that timescaledb is not CDC-capable. Not a live QA retest. |
| DEF-B2-011 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-002 | P2 | OPEN | fixed in code, unit-proven (test_sftp_create_keeps_host_from_the_uri, test_sftp_uri_fills_empty_host_and_keeps_an_explicit_host, test_extract_url_credentials_reads_sftp_uri). An explicit host is not overwritten. Not a live connector retest. |
| DEF-A-018 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-011 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-003 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-009 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-010 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-011 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-013 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-015 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-022 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-023 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-025 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-026 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-027 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-033 | P2 | OPEN | fixed in code, unit-proven (test_base64_alphabet_text_stays_text_and_is_not_a_preserve). Hex named payload stays text. Real base64 named payload_b64 stays BINARY. Not a live PG retest. |
| DEF-C-039 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-040 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-044 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-045 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-047 | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-SCHEMALESS-DST-CONTRACT | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-DECIMAL-SAMPLE-INFER | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-TSTZ-NAME-INFER | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-REDIS-SRC-HEADERS | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-S3-ENDPOINT-FORM | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-ADLS-EMPTY-INCLUDE | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-GCS-DST-CREATE | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| UNNUMBERED (RETEST-R1 case 3.3) | P2 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-MCP-OUTAGE | P2 | UNCONFIRMED-ENV-DEPLOY | not fixed. Diagnose-only. No code change. |
| DEF-C-008 | P2 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-CDC-SLOT-LEAK | P2 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-001 | P2 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-C-006 | P2 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-A-014 | P3 | OPEN | partly fixed in code. describe_stored_cadence now says Every 5 minutes, and the create_schedule preview interval uses that label. The stored interval token remains a preset because the runner only accepts hourly/daily/weekly. Not a live retest. |
| DEF-B-006 | P3 | OPEN | partly fixed. The cadence label uses describe_stored_cadence while the stored interval stays a preset. The blocker is no longer copied onto a completed job as a __DF_SQL_NULL__ quarantine row (unit-proven with DEF-B-022). Not a live QA retest. |
| DEF-B-007 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-008 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-012 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-015 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-016 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-B-030 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-013 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-016 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-A-020 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-007 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-012 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-014 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-016 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-018 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-019 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-021 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-028 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-032 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-035 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-046 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-048 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-C-030 | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-EMBED-SILENT-FALLBACK | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-MINIO-ALIAS | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| GAP-XLS | P3 | OPEN | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-MCP-BQ-FORM | P3 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-SPECIALTY-SAMPLE | P3 | PARTLY FIXED | not fixed in this change. No live matrix was re-run. QA snapshot build is 6436aaa38583. |
| DEF-ES-PRICE-STRING | P3 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-CSV-TSTZ-OFFSET | P3 | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-ES-META-INDEX | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-PGVECTOR-TEXT | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-07 | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-08 | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-09 | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-OBJSTORE-TYPE-SPLIT | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-TIMESCALE-DST | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
| DEF-OVERWRITE-NOPK-IDENTITY | n/r | FIXED-verified | QA-verified on the recorded build. Not re-run in this session. |
