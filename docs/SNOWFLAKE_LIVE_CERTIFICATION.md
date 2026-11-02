# Snowflake live certification evidence

Evidence label: **live** (real Snowflake account, not fakesnow).

| Field | Value |
|---|---|
| Run date (UTC) | 2026-11-02T03:11:02Z |
| Code revision | `63838b35c` (`feature/fix-qa-bug-fixes`) |
| Suite | `apps/api/tests/test_live_snowflake_certification.py` |
| Result | **9 passed, 0 failed, 0 skipped** (1020 s) |
| Snowflake | trial account, Azure `AZURE_EASTUS2`, server 10.39.101 |
| Driver | `snowflake-connector-python` 4.8.0 |
| Principal | dedicated user `DATAWRAP_CERT_USER`, role `DATAWRAP_CERT` (owns only `DATAWRAP_CERT_DB`; no account-level CREATE DATABASE), warehouse `DATAWRAP_WH` (XSMALL) |

## Cells

| Test | Proves |
|---|---|
| `test_cert_connect_role_warehouse_and_ddl` | session lands on the configured role / warehouse / database / schema; DDL works under least privilege |
| `test_cert_wrong_password_is_reported_as_credentials_not_account` | bad password is classified as a credentials error |
| `test_cert_unknown_account_is_reported_as_account_not_found` | bad account is classified as account-not-found |
| `test_cert_sqlite_to_snowflake_typed_round_trip` | int64 edges, decimal, float, unicode/emoji, empty string vs NULL, dates, microsecond timestamps; independent read-back; Gate-8 count + checksum |
| `test_cert_snowflake_as_source_to_sqlite` | Snowflake as source; Gate-8 count + checksum |
| `test_cert_snowflake_to_snowflake_copy` | default in-warehouse copy reports **count-only** proof (no invented value match); with `DATAFLOW_ENGINE_DIGEST=1` value digests match; independent `MINUS` diff = 0 both ways |
| `test_cert_csv_to_snowflake_upsert_updates_and_inserts` | MERGE upsert updates existing keys and inserts new ones, no duplicates |
| `test_cert_incremental_append_second_run_lands_only_the_delta` | second incremental run lands only the 15 new rows (30 → 45), no duplicates |
| `test_cert_csv_exact_decimals_and_text_to_snowflake` | exact NUMBER(38,9) values (e.g. `99999999999999999999.999999999`, `-0.000000001`) and hostile text survive |

## Product defects found by this run (fixed before the green run)

1. Writer and file-manifest paths ran `CREATE DATABASE/SCHEMA IF NOT EXISTS` on existing namespaces; Snowflake checks the account privilege first, so least-privilege roles could not load. Now `USE` first, create only when missing (`use_snowflake_namespace`).
2. Gate-8 census dropped the configured role and referenced folded table/column names; now resolves stored names from the catalog.
3. Snowflake→Snowflake create-new stored a quoted lowercase table while other writers fold to upper case; now folds consistently.

## Limits (not claimed)

- One account, one region, password auth. Key-pair auth, other clouds/regions, and volumes beyond the fixture rows are not covered by this run.
- Same-account copy default proof is COUNT(*) only; value proof needs `DATAFLOW_ENGINE_DIGEST=1`.
- Incremental / upsert are at-least-once with idempotent MERGE, not exactly-once.

## Reproduce

```bash
cd apps/api
DATAFLOW_SNOWFLAKE_LIVE_ACCOUNT=... DATAFLOW_SNOWFLAKE_LIVE_USER=... \
DATAFLOW_SNOWFLAKE_LIVE_PASSWORD=... DATAFLOW_SNOWFLAKE_LIVE_ROLE=... \
DATAFLOW_SNOWFLAKE_LIVE_WAREHOUSE=... DATAFLOW_SNOWFLAKE_LIVE_DATABASE=... \
python -m pytest tests/test_live_snowflake_certification.py -v -p no:cacheprovider -rs
```
Without the variables every cell skips.
